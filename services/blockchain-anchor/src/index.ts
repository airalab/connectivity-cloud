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
  TelemetryIpfsPublishedPayloadSchema,
  type TelemetryIpfsPublishedPayload,
} from '@scp/core';
import { fromBinary } from '@bufbuild/protobuf';
import { Consumer } from '@platformatic/kafka';
import { ApiPromise, WsProvider, Keyring } from '@polkadot/api';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import { CID } from 'multiformats/cid';
import {
  loadBlockchainAnchorConfig,
  type BlockchainAnchorConfig,
} from './config.js';
import { logInfo, logWarn, logDebug, logError } from './logger.js';

interface BlockchainAnchorMetrics {
  consumed: number;
  anchored: number;
  failed: number;
}

export interface BlockchainAnchorService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<BlockchainAnchorMetrics>;
}

interface BlockchainAnchorDeps {
  createConsumer?: () => Consumer;
  createHealthServer?: (
    getMetrics: () => BlockchainAnchorMetrics,
    port: number
  ) => Server;
}

/**
 * Send CPS setPayload extrinsic to blockchain
 */
async function sendSetPayloadExtrinsic(
  api: ApiPromise,
  keyring: Keyring,
  suri: string,
  nodeId: number,
  cid: Uint8Array
): Promise<void> {
  const cidString = CID.decode(cid).toString();

  logDebug('preparing set_payload extrinsic', {
    nodeId,
    cid: cidString,
  });

  const account = keyring.addFromUri(suri);
  logDebug('account loaded', {
    address: account.address,
  });

  // Call cps.setPayload(node_id, Some(payload))
  if (!api.tx.cps?.setPayload) {
    throw new Error('cps.setPayload extrinsic not found');
  }
  const extrinsic = api.tx.cps.setPayload(nodeId, api.createType('Vec<u8>', cid));

  logInfo('submitting set_payload extrinsic', {
    nodeId,
    cid: cidString,
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
            cid: cidString,
          });
        }

        if (result.status.isFinalized) {
          logInfo('extrinsic finalized', {
            blockHash: result.status.asFinalized.toString(),
            nodeId,
            cid: cidString,
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
                cid: cidString,
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
            cid: cidString,
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
          cid: cidString,
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
    failed: 0,
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
        const provider = new WsProvider(config.substrateWsUrl);
        api = await ApiPromise.create({ provider: provider });
        await api.isReady;
        logInfo('connected to substrate', {
          substrateWsUrl: config.substrateWsUrl,
        });

        // Initialize keyring
        keyring = new Keyring({ type: 'sr25519' });
        logInfo('keyring initialized');

        // Start Kafka consumer
        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.IPFS_PUBLISHED],
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

              try {
                const envelope = fromBinary(
                  EnvelopeSchema,
                  new Uint8Array(message.value)
                );

                if (envelope.eventType !== TELEMETRY_TOPICS.IPFS_PUBLISHED) {
                  logDebug('non-ipfs-published envelope ignored', {
                    eventType: envelope.eventType,
                  });
                  continue;
                }

                const payload = fromBinary(
                  TelemetryIpfsPublishedPayloadSchema,
                  envelope.payload
                ) as TelemetryIpfsPublishedPayload;

                // Convert CID bytes to string
                const cidBytes = payload.cid;
                const cid = CID.decode(cidBytes);
                const cidString = cid.toString();

                metrics.consumed += 1;

                logInfo('anchoring IPFS CID to blockchain', {
                  event_id: envelope.eventId,
                  cid: cidString,
                  event_count: payload.eventCount,
                  compression: payload.compression,
                  node_id: config.nodeId,
                });

                try {
                  await sendSetPayloadExtrinsic(
                    api!,
                    keyring!,
                    config.suri,
                    config.nodeId,
                    cidBytes
                  );

                  metrics.anchored += 1;

                  logInfo('CID anchored successfully', {
                    cid: cidString,
                    event_count: payload.eventCount,
                    node_id: config.nodeId,
                  });

                  // Commit offset only after successful anchoring
                  await consumer.commit({
                    offsets: [
                      {
                        topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
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
                  logError('failed to anchor CID', error, {
                    cid: cidString,
                    node_id: config.nodeId,
                  });
                  // Don't commit offset on failure - message will be retried
                }
              } catch (error) {
                logWarn('envelope parse error', {
                  reason:
                    error instanceof Error ? error.message : String(error),
                });
                // Commit offset for invalid messages to skip them
                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });
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

      await consumer.close();
      await api?.disconnect();
      api = null;
      keyring = null;

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

      await runPromise?.catch(() => undefined);
      runPromise = null;
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
  startBlockchainAnchor().catch((error: unknown) => {
    logError('failed to start (direct run)', error);
    process.exitCode = 1;
  });
}
