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
  TelemetryAuthorizedPayloadSchema,
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryIpfsPublishedPayload_Compression as Compression,
  type TelemetryAuthorizedPayload,
  formatSensorId,
} from '@scp/core';
import { fromBinary, toBinary, create } from '@bufbuild/protobuf';
import {
  SignedEnvelopeSchema,
  SignedEnvelopeBatchSchema,
  type SignedEnvelope,
} from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { Consumer, Producer } from '@platformatic/kafka';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { CID } from 'multiformats/cid';
import { compress } from '@napi-rs/lzma/xz';
import {
  create as createKuboClient,
  type KuboRPCClient,
} from 'kubo-rpc-client';
import { loadIpfsPublisherConfig, type IpfsPublisherConfig } from './config.js';
import { logInfo, logWarn, logDebug, logError } from './logger.js';

interface IpfsPublisherMetrics {
  consumed: number;
  batchesPublished: number;
  eventsPublished: number;
  publishFailure: number;
}

export interface IpfsPublisherService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<IpfsPublisherMetrics>;
}

interface IpfsClient {
  start(): Promise<void>;
  stop(): Promise<void>;
  add(data: Uint8Array, compressed: boolean): Promise<string>;
}

interface IpfsPublisherDeps {
  createIpfsClient?: (apiUrl: string) => Promise<IpfsClient>;
  createConsumer?: () => Consumer;
  createProducer?: () => Producer;
  createHealthServer?: (
    getMetrics: () => IpfsPublisherMetrics,
    port: number
  ) => Server;
}

/**
 * Batch holds signed envelopes and their Kafka offset info
 */
interface BatchItem {
  signedEnvelope: SignedEnvelope;
  offset: bigint;
  partition: number;
  sensorId: Uint8Array;
  traceId?: string;
  eventId: string;
}

/**
 * Publish batch to IPFS and emit result event
 */
async function publishBatch(
  batch: BatchItem[],
  ipfsClient: IpfsClient,
  producer: Producer,
  config: IpfsPublisherConfig,
  metrics: IpfsPublisherMetrics
): Promise<void> {
  if (batch.length === 0) {
    return;
  }

  // Collect unique sensor IDs and trace IDs for logging
  const uniqueSensorIds = Array.from(
    new Set(batch.map((b) => formatSensorId(b.sensorId)))
  );
  const traceIds = Array.from(
    new Set(
      batch.map((b) => b.traceId).filter((id): id is string => id !== undefined)
    )
  );

  logInfo('publishing batch to IPFS', {
    batch_size: batch.length,
    unique_sensors: uniqueSensorIds.length,
    sensor_ids: uniqueSensorIds,
    trace_ids: traceIds.length > 0 ? traceIds : undefined,
  });

  try {
    // Serialize batch
    const batchData = create(SignedEnvelopeBatchSchema, {
      batch: batch.map((b) => b.signedEnvelope),
    });

    // Publish to IPFS
    const cid = await ipfsClient.add(
      toBinary(SignedEnvelopeBatchSchema, batchData),
      config.enableCompression
    );

    logInfo('batch published to IPFS', {
      cid,
      event_count: batch.length,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
      trace_ids: traceIds.length > 0 ? traceIds : undefined,
      compression: config.enableCompression,
    });

    // Create result envelope
    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(CID.parse(cid).bytes),
      eventCount: batch.length,
      compression: config.enableCompression ? Compression.XZ : Compression.NONE,
    });

    const resultEnvelope = create(EnvelopeSchema, {
      eventId: randomUUID(),
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: '1.0.0',
      occurredAt: new Date().toISOString(),
      source: config.source,
      payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
    });

    // Publish result to Kafka
    await producer.send({
      messages: [
        {
          topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
          value: Buffer.from(toBinary(EnvelopeSchema, resultEnvelope)),
        },
      ],
    });

    metrics.batchesPublished += 1;
    metrics.eventsPublished += batch.length;

    logInfo('batch result published to Kafka', {
      cid,
      event_count: batch.length,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
      trace_ids: traceIds.length > 0 ? traceIds : undefined,
      result_topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
    });
  } catch (error) {
    metrics.publishFailure += 1;
    logError('batch publish failed', error, {
      batch_size: batch.length,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
      trace_ids: traceIds.length > 0 ? traceIds : undefined,
    });
    throw error;
  }
}

/**
 * Get total lag (pending messages) across all partitions for given topics
 */
async function getTotalLag(
  consumer: Consumer,
  topics: string[]
): Promise<number> {
  try {
    const lagMap = await consumer.getLag({ topics });
    let total = 0;
    for (const partitionLags of lagMap.values()) {
      for (const lag of partitionLags) {
        total += Number(lag);
      }
    }
    return total;
  } catch (error) {
    logWarn('failed to get consumer lag', {
      error: error instanceof Error ? error.message : String(error),
    });
    return 0;
  }
}

export function createIpfsPublisherService(
  config: IpfsPublisherConfig = loadIpfsPublisherConfig(),
  deps: IpfsPublisherDeps = {}
): IpfsPublisherService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'ipfs-publisher',
      bootstrapBrokers: config.kafkaBrokers,
    });

  const producer =
    deps.createProducer?.() ??
    new Producer({
      clientId: 'ipfs-publisher',
      bootstrapBrokers: config.kafkaBrokers,
    });

  const createIpfsClient =
    deps.createIpfsClient ?? ((apiUrl: string) => createIpfsKuboClient(apiUrl));
  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let ipfsClient: IpfsClient | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let shouldStop = false;

  const metrics: IpfsPublisherMetrics = {
    consumed: 0,
    batchesPublished: 0,
    eventsPublished: 0,
    publishFailure: 0,
  };

  const getMetrics = (): IpfsPublisherMetrics => metrics;

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
        ipfsApiUrl: config.ipfsApiUrl,
        batchSize: config.batchSize,
        batchTimeoutMs: config.batchTimeoutMs,
        healthPort: config.healthPort,
      });

      try {
        ipfsClient = await createIpfsClient(config.ipfsApiUrl);
        await ipfsClient.start();

        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: false,
        });

        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = (async () => {
          let currentBatch: BatchItem[] = [];
          let batchTimer: NodeJS.Timeout | null = null;

          const resetBatchTimer = () => {
            if (batchTimer) {
              clearTimeout(batchTimer);
              batchTimer = null;
            }

            batchTimer = setTimeout(async () => {
              if (currentBatch.length > 0) {
                logDebug('batch timeout reached', {
                  batch_size: currentBatch.length,
                });
                await flushBatch();
              }
            }, config.batchTimeoutMs);
          };

          const flushBatch = async () => {
            if (currentBatch.length === 0) {
              return;
            }

            const batchToPublish = [...currentBatch];

            // Get last offset for each partition in batch
            const offsetsByPartition = new Map<number, bigint>();
            for (const item of batchToPublish) {
              const current = offsetsByPartition.get(item.partition);
              if (!current || item.offset > current) {
                offsetsByPartition.set(item.partition, item.offset);
              }
            }

            try {
              await publishBatch(
                batchToPublish,
                ipfsClient!,
                producer,
                config,
                metrics
              );

              // Commit offsets only after successful publish
              const offsets = Array.from(offsetsByPartition.entries()).map(
                ([partition, offset]) => ({
                  topic: TELEMETRY_TOPICS.AUTHORIZED,
                  partition,
                  offset,
                  leaderEpoch: -1,
                })
              );

              await consumer.commit({ offsets });

              logDebug('kafka offsets committed', {
                offsets: offsets.map((o) => ({
                  partition: o.partition,
                  offset: o.offset.toString(),
                })),
              });

              // Clear batch after successful publish and commit
              currentBatch = [];

              if (batchTimer) {
                clearTimeout(batchTimer);
                batchTimer = null;
              }
            } catch (error) {
              // On failure, keep messages in batch for retry
              const failedSensorIds = Array.from(
                new Set(batchToPublish.map((b) => formatSensorId(b.sensorId)))
              );
              const failedTraceIds = Array.from(
                new Set(
                  batchToPublish
                    .map((b) => b.traceId)
                    .filter((id): id is string => id !== undefined)
                )
              );
              logWarn('batch flush failed, will retry on next message', {
                batch_size: batchToPublish.length,
                unique_sensors: failedSensorIds.length,
                sensor_ids: failedSensorIds,
                trace_ids:
                  failedTraceIds.length > 0 ? failedTraceIds : undefined,
                error: error instanceof Error ? error.message : String(error),
              });
            }
          };

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

                if (envelope.eventType !== TELEMETRY_TOPICS.AUTHORIZED) {
                  logDebug('non-authorized envelope ignored', {
                    eventType: envelope.eventType,
                  });
                  continue;
                }

                // Extract SignedEnvelope from payload
                const payload = fromBinary(
                  TelemetryAuthorizedPayloadSchema,
                  envelope.payload
                ) as TelemetryAuthorizedPayload;

                const signedEnvelope = fromBinary(
                  SignedEnvelopeSchema,
                  payload.signedEnvelope
                );

                metrics.consumed += 1;

                const batchItem: BatchItem = {
                  signedEnvelope,
                  offset: message.offset + 1n, // Next offset to commit
                  partition: message.partition,
                  sensorId: payload.sensorId,
                  eventId: envelope.eventId,
                };
                if (envelope.traceId !== undefined) {
                  batchItem.traceId = envelope.traceId;
                }
                currentBatch.push(batchItem);

                logDebug('message added to batch', {
                  event_id: envelope.eventId,
                  trace_id: envelope.traceId,
                  sensor_id: formatSensorId(payload.sensorId),
                  batch_size: currentBatch.length,
                  batch_max: config.batchSize,
                });

                // Check if batch is full
                if (currentBatch.length >= config.batchSize) {
                  logDebug('batch size reached', {
                    batch_size: currentBatch.length,
                  });
                  await flushBatch();
                } else {
                  // Check lag to decide on batching strategy
                  const lag = await getTotalLag(consumer, [
                    TELEMETRY_TOPICS.AUTHORIZED,
                  ]);

                  logDebug('consumer lag check', {
                    lag,
                    batch_size: currentBatch.length,
                    batch_max: config.batchSize,
                  });

                  if (lag < config.batchSize && currentBatch.length > 0) {
                    // Not enough messages waiting, start/reset timer
                    resetBatchTimer();
                  } else if (lag >= config.batchSize) {
                    // Many messages waiting, flush current batch to catch up
                    logInfo('flushing batch early due to lag', {
                      batch_size: currentBatch.length,
                      lag,
                    });
                    await flushBatch();
                  }
                }
              } catch (error) {
                logWarn('envelope parse error', {
                  reason:
                    error instanceof Error ? error.message : String(error),
                });
              }
            }
          } finally {
            // Cleanup timer
            if (batchTimer) {
              clearTimeout(batchTimer);
            }

            // Flush remaining batch on shutdown
            if (currentBatch.length > 0 && !shouldStop) {
              logInfo('flushing remaining batch on shutdown', {
                batch_size: currentBatch.length,
              });
              await flushBatch().catch((error) => {
                logError('failed to flush final batch', error);
              });
            }
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
        await ipfsClient?.stop().catch(() => undefined);
        ipfsClient = null;
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
      await ipfsClient?.stop();
      ipfsClient = null;

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
    getMetrics(): Readonly<IpfsPublisherMetrics> {
      return metrics;
    },
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => IpfsPublisherMetrics,
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
        batchesPublished: metrics.batchesPublished,
        eventsPublished: metrics.eventsPublished,
        publishFailure: metrics.publishFailure,
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

/**
 * Create an IPFS Kubo RPC client for publishing binary data
 */
async function createIpfsKuboClient(apiUrl: string): Promise<IpfsClient> {
  const client: KuboRPCClient = createKuboClient({ url: apiUrl });
  let started = false;

  return {
    async start() {
      try {
        const version = await client.version();
        logInfo('connected to IPFS node', {
          version: version.version,
          apiUrl,
        });
        started = true;
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        const wrappedError = new Error(
          `Failed to connect to IPFS at ${apiUrl}: ${errorMessage}`,
          { cause: error }
        );
        throw wrappedError;
      }
    },
    async stop() {
      if (!started) {
        return;
      }
      started = false;
      logInfo('IPFS client stopped');
    },
    async add(data: Uint8Array, compressed: boolean): Promise<string> {
      if (!started) {
        throw new Error('IPFS client not started');
      }
      const result = await client.add(compressed ? await compress(data) : data);
      return result.cid.toString();
    },
  };
}

export async function startIpfsPublisher(): Promise<IpfsPublisherService> {
  const service = createIpfsPublisherService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startIpfsPublisher().catch((error: unknown) => {
    logError('failed to start (direct run)', error);
    process.exitCode = 1;
  });
}
