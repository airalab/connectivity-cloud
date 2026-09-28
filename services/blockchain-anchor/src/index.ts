/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import {
  TELEMETRY_TOPICS,
  EnvelopeSchema,
  TelemetryBatchedPayloadSchema,
  type TelemetryBatchedPayload,
  installShutdownHandler,
} from '@scp/core';
import { fromBinary, type MessageShape } from '@bufbuild/protobuf';
import { Consumer, Producer } from '@platformatic/kafka';
import { ApiPromise, WsProvider, Keyring } from '@polkadot/api';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import {
  loadBlockchainAnchorConfig,
  type BlockchainAnchorConfig,
} from './config.js';
import { logInfo, logWarn, logDebug, logError } from './logger.js';

interface BlockchainAnchorMetrics {
  consumed: number;
  anchored: number;
  skippedDuplicate: number;
  failed: number;
  /** Payloads rejected because they exceed the defensive size invariant. */
  rejectedOversized: number;
}

export interface BlockchainAnchorService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<BlockchainAnchorMetrics>;
}

export interface BlockchainAnchorDeps {
  createConsumer?: () => Consumer;
  createProducer?: () => Producer;
  createApi?: () => Promise<ApiPromise>;
  createHealthServer?: (
    getMetrics: () => BlockchainAnchorMetrics,
    port: number
  ) => Server;
}

/**
 * Minimal shape of the `Option<BoundedVec<u8>>` codec returned by the CPS
 * pallet's `payload` storage query. Modeled loosely (rather than via
 * generated types) since this service talks to the chain dynamically.
 */
interface OptionBytesCodec {
  readonly isSome?: boolean;
  readonly isEmpty?: boolean;
  unwrap?: () => { toU8a: () => Uint8Array };
}

/**
 * Read the payload currently anchored on-chain for a CPS node, if any.
 *
 * This is the authoritative idempotency check: rather than relying solely on
 * a local/Redis marker (which can't observe whether an extrinsic actually
 * finalized before a crash), we ask the chain what payload is currently set
 * for the node and compare it byte-for-byte against the payload we are about
 * to submit. If they already match, the anchor operation is a no-op and is
 * skipped.
 */
async function getAnchoredPayload(
  api: ApiPromise,
  nodeId: number
): Promise<Uint8Array | null> {
  if (!api.query.cps?.payload) {
    return null;
  }

  try {
    const raw = (await api.query.cps.payload(
      nodeId
    )) as unknown as OptionBytesCodec;

    if (!raw || raw.isEmpty || raw.isSome === false || !raw.unwrap) {
      return null;
    }

    return raw.unwrap().toU8a();
  } catch (error) {
    logWarn('failed to read on-chain payload for idempotency check', {
      nodeId,
      reason: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

/**
 * Forward an oversized `telemetry.batched.v1` record to the DLQ topic,
 * preserving its original bytes. This is a permanent upstream contract
 * violation (the batcher is supposed to guarantee `payload <=
 * maxPayloadBytes`), so the record must be durably routed to the DLQ rather
 * than silently dropped: the offset is only committed after this publish is
 * confirmed.
 */
async function publishOversizedToDlq(
  raw: Uint8Array,
  partition: number,
  offset: bigint,
  reason: string,
  producer: Producer,
  config: BlockchainAnchorConfig
): Promise<void> {
  await producer.send({
    messages: [
      {
        topic: TELEMETRY_TOPICS.DLQ,
        value: Buffer.from(raw),
        headers: {
          source_topic: Buffer.from(TELEMETRY_TOPICS.BATCHED),
          source_service: Buffer.from(config.source),
          source_partition: Buffer.from(String(partition)),
          source_offset: Buffer.from(String(offset)),
          reason: Buffer.from(reason),
        },
      },
    ],
  });

  logWarn('oversized batch routed to DLQ', {
    partition,
    offset: offset.toString(),
    dlq_topic: TELEMETRY_TOPICS.DLQ,
  });
}

/**
 * Send CPS setPayload extrinsic to blockchain.
 *
 * `payload` is submitted exactly as received from `telemetry.batched.v1` —
 * this service must not reserialize, recompress, or otherwise change it.
 */
async function sendSetPayloadExtrinsic(
  api: ApiPromise,
  keyring: Keyring,
  suri: string,
  nodeId: number,
  payload: Uint8Array
): Promise<void> {
  logDebug('preparing set_payload extrinsic', {
    nodeId,
    payload_size: payload.length,
  });

  const account = keyring.addFromUri(suri);
  logDebug('account loaded', {
    address: account.address,
  });

  // Call cps.setPayload(node_id, Some(payload))
  if (!api.tx.cps?.setPayload) {
    throw new Error('cps.setPayload extrinsic not found');
  }
  const extrinsic = api.tx.cps.setPayload(nodeId, Array.from(payload));

  logInfo('submitting set_payload extrinsic', {
    nodeId,
    payload_size: payload.length,
    from: account.address,
  });

  return new Promise((resolve, reject) => {
    let unsub: (() => void) | null = null;

    extrinsic
      .signAndSend(account, (result) => {
        logDebug('extrinsic status update', {
          status: result.status.type,
          isFinalized: result.isFinalized,
          isInBlock: result.isInBlock,
        });

        if (result.status.isInBlock) {
          logInfo('extrinsic in block', {
            blockHash: result.status.asInBlock.toString(),
            nodeId,
            payload_size: payload.length,
          });
        }

        if (result.status.isFinalized) {
          logInfo('extrinsic finalized', {
            blockHash: result.status.asFinalized.toString(),
            nodeId,
            payload_size: payload.length,
          });

          if (unsub) {
            unsub();
          }

          // Check for errors in events
          if (!api.events.system?.ExtrinsicFailed) {
            resolve();
            return;
          }

          const hasError = result.events.some((record) => {
            const event = record.event;
            return api.events.system?.ExtrinsicFailed?.is(event) ?? false;
          });

          if (hasError) {
            const errorEvent = result.events.find((record) =>
              api.events.system?.ExtrinsicFailed?.is(record.event)
            );
            const errorData = errorEvent?.event.data;
            logError(
              'extrinsic failed in finalized block',
              new Error('ExtrinsicFailed'),
              {
                nodeId,
                payload_size: payload.length,
                errorData: errorData?.toString(),
              }
            );
            reject(
              new Error(
                `Extrinsic failed: ${errorData?.toString() ?? 'unknown error'}`
              )
            );
            return;
          }

          resolve();
        } else if (result.isError) {
          if (unsub) {
            unsub();
          }
          const error = new Error('Transaction failed with status error');
          logError('extrinsic submission error', error, {
            nodeId,
            payload_size: payload.length,
            status: result.status.type,
          });
          reject(error);
        }
      })
      .then((unsubscribe) => {
        unsub = unsubscribe;
      })
      .catch((error) => {
        logError('failed to submit extrinsic', error, {
          nodeId,
          payload_size: payload.length,
        });
        reject(error);
      });
  });
}

export function createBlockchainAnchorService(
  config: BlockchainAnchorConfig = loadBlockchainAnchorConfig(),
  deps: BlockchainAnchorDeps = {}
): BlockchainAnchorService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'blockchain-anchor',
      bootstrapBrokers: config.kafkaBrokers,
    });

  const producer =
    deps.createProducer?.() ??
    new Producer({
      clientId: 'blockchain-anchor',
      bootstrapBrokers: config.kafkaBrokers,
    });

  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let api: ApiPromise | null = null;
  let keyring: Keyring | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let shouldStop = false;

  const metrics: BlockchainAnchorMetrics = {
    consumed: 0,
    anchored: 0,
    skippedDuplicate: 0,
    failed: 0,
    rejectedOversized: 0,
  };

  const getMetrics = (): BlockchainAnchorMetrics => metrics;

  return {
    async start(): Promise<void> {
      if (started) {
        logInfo('start skipped; service already started');
        return;
      }

      started = true;
      shouldStop = false;
      logInfo('starting service', {
        consumerGroupId: config.consumerGroupId,
        kafkaBrokers: config.kafkaBrokers,
        substrateWsUrl: config.substrateWsUrl,
        nodeId: config.nodeId,
        healthPort: config.healthPort,
      });

      try {
        // Connect to blockchain
        if (deps.createApi) {
          api = await deps.createApi();
        } else {
          const provider = new WsProvider(config.substrateWsUrl);
          api = await ApiPromise.create({ provider: provider });
          await api.isReady;
        }
        logInfo('connected to substrate', {
          substrateWsUrl: config.substrateWsUrl,
        });

        // Initialize keyring
        keyring = new Keyring({ type: 'sr25519' });
        logInfo('keyring initialized');

        // Start Kafka consumer
        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.BATCHED],
          autocommit: false,
        });

        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = (async () => {
          try {
            for await (const message of consumerStream) {
              if (shouldStop) {
                break;
              }

              if (!message.value) {
                logWarn('received null message value; skipping');
                continue;
              }

              let envelope: MessageShape<typeof EnvelopeSchema>;
              let payload: TelemetryBatchedPayload;
              try {
                envelope = fromBinary(
                  EnvelopeSchema,
                  new Uint8Array(message.value)
                );

                if (envelope.eventType !== TELEMETRY_TOPICS.BATCHED) {
                  logDebug('non-batched envelope ignored', {
                    eventType: envelope.eventType,
                  });
                  continue;
                }

                payload = fromBinary(
                  TelemetryBatchedPayloadSchema,
                  envelope.payload
                ) as TelemetryBatchedPayload;
              } catch (error) {
                logWarn('envelope parse error', {
                  reason:
                    error instanceof Error ? error.message : String(error),
                });
                // Commit offset for invalid messages to skip them
                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.BATCHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });
                continue;
              }

              metrics.consumed += 1;

              // Defensive size invariant: the batcher owns payload fitting,
              // but blockchain-anchor must not submit a payload that
              // violates the CPS size limit. This is a permanent upstream
              // contract error, not a transient failure — do not split the
              // batch here, and do not retry. The record is still routed to
              // the DLQ (instead of being silently dropped) so it can be
              // inspected/recovered, and the offset is only committed once
              // that publish is confirmed.
              if (payload.payload.length > config.maxPayloadBytes) {
                metrics.rejectedOversized += 1;
                logError(
                  'rejecting batch: payload exceeds ANCHOR_MAX_PAYLOAD_BYTES',
                  new Error('ANCHOR_PAYLOAD_TOO_LARGE'),
                  {
                    batch_id: payload.batchId,
                    payload_size: payload.payload.length,
                    max_payload_bytes: config.maxPayloadBytes,
                    node_id: config.nodeId,
                  }
                );

                await publishOversizedToDlq(
                  new Uint8Array(message.value),
                  message.partition,
                  message.offset,
                  'ANCHOR_PAYLOAD_TOO_LARGE',
                  producer,
                  config
                );

                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.BATCHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });
                continue;
              }

              logInfo('anchoring batch to blockchain', {
                event_id: envelope.eventId,
                batch_id: payload.batchId,
                payload_size: payload.payload.length,
                event_count: payload.eventCount,
                node_id: config.nodeId,
              });

              try {
                // Idempotency check: ask the chain what is currently
                // anchored for this node before submitting a new
                // extrinsic. This covers the crash window between
                // extrinsic finalization and Kafka offset commit - on
                // redelivery the on-chain state already reflects the
                // anchor, so we skip re-submission instead of creating a
                // duplicate logical anchor operation.
                const anchoredPayload = await getAnchoredPayload(
                  api!,
                  config.nodeId
                );

                if (
                  anchoredPayload !== null &&
                  bytesEqual(anchoredPayload, payload.payload)
                ) {
                  metrics.skippedDuplicate += 1;

                  logInfo(
                    'batch already anchored on-chain; skipping duplicate submission',
                    {
                      batch_id: payload.batchId,
                      payload_size: payload.payload.length,
                      event_id: envelope.eventId,
                      node_id: config.nodeId,
                    }
                  );
                } else {
                  await sendSetPayloadExtrinsic(
                    api!,
                    keyring!,
                    config.suri,
                    config.nodeId,
                    payload.payload
                  );

                  metrics.anchored += 1;

                  logInfo('batch anchored successfully', {
                    batch_id: payload.batchId,
                    payload_size: payload.payload.length,
                    event_count: payload.eventCount,
                    node_id: config.nodeId,
                  });
                }

                // Commit offset only after successful anchoring (or a
                // confirmed no-op due to the idempotency check above)
                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.BATCHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });

                logDebug('kafka offset committed', {
                  partition: message.partition,
                  offset: (message.offset + 1n).toString(),
                });
              } catch (error) {
                metrics.failed += 1;
                logError('failed to anchor batch', error, {
                  batch_id: payload.batchId,
                  payload_size: payload.payload.length,
                  node_id: config.nodeId,
                });
                // Don't commit offset on failure. Stop consuming further
                // messages instead of continuing the loop: a later
                // message's commit would advance the committed offset past
                // this unretried one, silently losing it. Rethrowing exits
                // the loop (and the run promise) so the process can be
                // restarted and resume from the last committed offset.
                throw error;
              }
            }
          } catch (error) {
            logError('consumer stream error', error);
            throw error;
          }
        })();

        logInfo('service started');
      } catch (error) {
        logError('service failed to start', error);
        started = false;
        runPromise = null;

        if (healthServer) {
          await new Promise<void>((resolve) => {
            healthServer?.close(() => {
              resolve();
            });
          });
          healthServer = null;
        }

        await consumer.close().catch(() => undefined);
        await api?.disconnect();
        api = null;
        keyring = null;
        logInfo('startup rollback complete');
        throw error;
      }
    },
    async stop(): Promise<void> {
      if (!started) {
        logInfo('stop skipped; service not started');
        return;
      }

      started = false;
      shouldStop = true;
      logInfo('stopping service');

      // Stop pulling new messages, but do not disconnect the blockchain API
      // yet: the run loop only checks `shouldStop` before fetching the next
      // message, so a message whose extrinsic is currently in flight will
      // run to completion. Disconnecting the API here would race that
      // in-flight `signAndSend` and could drop/corrupt the submission.
      await consumer.close();

      logInfo(
        'waiting for in-flight processing to complete before disconnecting chain API'
      );
      await runPromise?.catch(() => undefined);
      runPromise = null;

      await api?.disconnect();
      api = null;
      keyring = null;

      await producer.close().catch(() => undefined);

      if (healthServer) {
        await new Promise<void>((resolve, reject) => {
          healthServer?.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
        healthServer = null;
      }

      logInfo('service stopped');
    },
    getMetrics(): Readonly<BlockchainAnchorMetrics> {
      return metrics;
    },
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => BlockchainAnchorMetrics,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      logDebug('metrics endpoint requested');
      const metrics = getMetrics();
      logInfo('metrics served', {
        consumed: metrics.consumed,
        anchored: metrics.anchored,
        failed: metrics.failed,
      });
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify(metrics));
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
  logInfo('HTTP server listening', { port, host: '0.0.0.0' });
  return server;
}

export async function startBlockchainAnchor(): Promise<BlockchainAnchorService> {
  const service = createBlockchainAnchorService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startBlockchainAnchor()
    .then((service) => {
      installShutdownHandler(() => service.stop(), {
        onSignal: (signal) => logInfo('received shutdown signal', { signal }),
        onShutdownError: (error) =>
          logError('error during graceful shutdown', error),
      });
    })
    .catch((error: unknown) => {
      logError('failed to start (direct run)', error);
      process.exitCode = 1;
    });
}
