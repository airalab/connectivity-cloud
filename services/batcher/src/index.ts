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
  TelemetryBatchedPayloadSchema,
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
import { createHash } from 'node:crypto';
import { loadBatcherConfig, type BatcherConfig } from './config.js';
import { createBatchFlusher } from './batch-flusher.js';
import { logInfo, logWarn, logDebug, logError } from './logger.js';

interface BatcherMetrics {
  consumed: number;
  batchesProduced: number;
  eventsBatched: number;
  produceFailure: number;
}

export interface BatcherService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<BatcherMetrics>;
}

interface BatcherDeps {
  createConsumer?: () => Consumer;
  createProducer?: () => Producer;
  createHealthServer?: (
    getMetrics: () => BatcherMetrics,
    port: number
  ) => Server;
}

/**
 * Batch item holds a signed envelope and its Kafka offset info.
 *
 * A "poison" item (one whose envelope could not be parsed) carries no
 * `signedEnvelope`/`sensorId`/`eventId` but still occupies a slot so its
 * offset is included in the batch's commit range and it is routed to the
 * DLQ instead of being silently skipped past.
 */
interface BatchItem {
  signedEnvelope?: SignedEnvelope;
  offset: bigint;
  partition: number;
  sensorId?: Uint8Array;
  traceId?: string;
  eventId?: string;
  /** Reason the envelope failed to parse; set only for poison items. */
  parseError?: string;
  /** Original raw bytes; set only for poison items, for DLQ forwarding. */
  raw?: Uint8Array;
}

/** A batch item that parsed successfully and carries a valid envelope. */
type ValidBatchItem = BatchItem &
  Required<Pick<BatchItem, 'signedEnvelope' | 'sensorId' | 'eventId'>>;

function isValidItem(item: BatchItem): item is ValidBatchItem {
  return item.parseError === undefined;
}

/**
 * Derive a stable batch ID from the batch's source offsets so retries of the
 * same detached batch (e.g. after a failed offset commit) reuse the same ID.
 * This lets downstream consumers (e.g. ipfs-publisher) deduplicate by
 * `batch_id` even when a produce attempt is retried.
 */
function computeBatchId(batch: readonly BatchItem[]): string {
  const fingerprint = batch
    .map((b) => `${b.partition}:${b.offset}`)
    .sort()
    .join('|');
  return createHash('sha256').update(fingerprint).digest('hex');
}

/**
 * Publish poison (unparseable) records to the DLQ topic so they are handled
 * explicitly instead of being silently skipped past by a later commit.
 */
async function publishPoisonToDlq(
  poisonItems: readonly BatchItem[],
  producer: Producer,
  config: BatcherConfig
): Promise<void> {
  if (poisonItems.length === 0) {
    return;
  }

  await producer.send({
    messages: poisonItems.map((item) => ({
      topic: TELEMETRY_TOPICS.DLQ,
      value: Buffer.from(item.raw ?? new Uint8Array()),
      headers: {
        source_topic: Buffer.from(TELEMETRY_TOPICS.AUTHORIZED),
        source_service: Buffer.from(config.source),
        source_partition: Buffer.from(String(item.partition)),
        source_offset: Buffer.from(String(item.offset)),
        reason: Buffer.from(item.parseError ?? 'unknown parse error'),
      },
    })),
  });

  logWarn('poison records routed to DLQ', {
    count: poisonItems.length,
    dlq_topic: TELEMETRY_TOPICS.DLQ,
  });
}

/**
 * Serialize a batch of authorized telemetry into a `telemetry.batched.v1`
 * envelope and produce it to Kafka.
 */
async function produceBatch(
  batch: readonly ValidBatchItem[],
  producer: Producer,
  config: BatcherConfig,
  metrics: BatcherMetrics
): Promise<void> {
  if (batch.length === 0) {
    return;
  }

  const uniqueSensorIds = Array.from(
    new Set(batch.map((b) => formatSensorId(b.sensorId)))
  );
  const traceIds = Array.from(
    new Set(
      batch.map((b) => b.traceId).filter((id): id is string => id !== undefined)
    )
  );

  const batchId = computeBatchId(batch);

  logInfo('producing batch', {
    batch_id: batchId,
    batch_size: batch.length,
    unique_sensors: uniqueSensorIds.length,
    sensor_ids: uniqueSensorIds,
    trace_ids: traceIds.length > 0 ? traceIds : undefined,
  });

  try {
    const batchData = create(SignedEnvelopeBatchSchema, {
      batch: batch.map((b) => b.signedEnvelope),
    });

    const payload = create(TelemetryBatchedPayloadSchema, {
      batchId,
      signedEnvelopeBatch: toBinary(SignedEnvelopeBatchSchema, batchData),
      eventCount: batch.length,
      sensorIds: batch.map((b) => b.sensorId),
    });

    const resultEnvelope = create(EnvelopeSchema, {
      eventId: batchId,
      eventType: TELEMETRY_TOPICS.BATCHED,
      eventVersion: '1.0.0',
      occurredAt: new Date().toISOString(),
      source: config.source,
      payload: toBinary(TelemetryBatchedPayloadSchema, payload),
    });

    await producer.send({
      messages: [
        {
          topic: TELEMETRY_TOPICS.BATCHED,
          value: Buffer.from(toBinary(EnvelopeSchema, resultEnvelope)),
        },
      ],
    });

    metrics.batchesProduced += 1;
    metrics.eventsBatched += batch.length;

    logInfo('batch produced', {
      batch_id: batchId,
      event_count: batch.length,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
      trace_ids: traceIds.length > 0 ? traceIds : undefined,
      result_topic: TELEMETRY_TOPICS.BATCHED,
    });
  } catch (error) {
    metrics.produceFailure += 1;
    logError('batch produce failed', error, {
      batch_id: batchId,
      batch_size: batch.length,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
      trace_ids: traceIds.length > 0 ? traceIds : undefined,
    });
    throw error;
  }
}

/**
 * Get total lag (pending messages) across all partitions for given topics.
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

export function createBatcherService(
  config: BatcherConfig = loadBatcherConfig(),
  deps: BatcherDeps = {}
): BatcherService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'batcher',
      bootstrapBrokers: config.kafkaBrokers,
    });

  const producer =
    deps.createProducer?.() ??
    new Producer({
      clientId: 'batcher',
      bootstrapBrokers: config.kafkaBrokers,
    });

  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let shouldStop = false;

  const metrics: BatcherMetrics = {
    consumed: 0,
    batchesProduced: 0,
    eventsBatched: 0,
    produceFailure: 0,
  };

  const getMetrics = (): BatcherMetrics => metrics;

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
        batchSize: config.batchSize,
        batchTimeoutMs: config.batchTimeoutMs,
        healthPort: config.healthPort,
      });

      try {
        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: false,
        });

        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = (async () => {
          let batchTimer: NodeJS.Timeout | null = null;

          const flusher = createBatchFlusher<BatchItem>(
            async (batchToPublish) => {
              // Determine the last offset per partition before producing.
              const offsetsByPartition = new Map<number, bigint>();
              for (const item of batchToPublish) {
                const current = offsetsByPartition.get(item.partition);
                if (!current || item.offset > current) {
                  offsetsByPartition.set(item.partition, item.offset);
                }
              }

              const poisonItems = batchToPublish.filter(
                (item) => !isValidItem(item)
              );
              const validItems = batchToPublish.filter(isValidItem);

              try {
                // Route unparseable records to the DLQ before producing so
                // both are covered by the same offset commit below; if either
                // step fails the whole batch (poison + valid) is retried
                // together and no offset is committed.
                await publishPoisonToDlq(poisonItems, producer, config);
                await produceBatch(validItems, producer, config, metrics);

                // Commit authorized offsets only after the batch is durably
                // produced to telemetry.batched.v1 (and poison records, if
                // any, are durably routed to the DLQ).
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
              } catch (error) {
                const failedSensorIds = Array.from(
                  new Set(validItems.map((b) => formatSensorId(b.sensorId)))
                );
                logWarn('batch produce failed, will retry on next flush', {
                  batch_size: batchToPublish.length,
                  poison_count: poisonItems.length,
                  unique_sensors: failedSensorIds.length,
                  sensor_ids: failedSensorIds,
                  error: error instanceof Error ? error.message : String(error),
                });
                // Re-throw so the flusher re-attaches the batch for retry.
                throw error;
              }
            }
          );

          const flushBatch = async () => {
            if (batchTimer) {
              clearTimeout(batchTimer);
              batchTimer = null;
            }
            // The flusher serializes concurrent calls and re-attaches the batch
            // on failure; swallow the error here (already logged).
            await flusher.flush().catch(() => {
              if (!shouldStop && flusher.size() > 0) {
                resetBatchTimer();
              }
            });
          };

          const resetBatchTimer = () => {
            if (batchTimer) {
              clearTimeout(batchTimer);
              batchTimer = null;
            }

            batchTimer = setTimeout(async () => {
              if (flusher.size() > 0) {
                logDebug('batch timeout reached', {
                  batch_size: flusher.size(),
                });
                await flushBatch();
              }
            }, config.batchTimeoutMs);
          };

          const maybeFlush = async () => {
            // Check if batch is full.
            if (flusher.size() >= config.batchSize) {
              logDebug('batch size reached', {
                batch_size: flusher.size(),
              });
              await flushBatch();
            } else {
              const lag = await getTotalLag(consumer, [
                TELEMETRY_TOPICS.AUTHORIZED,
              ]);

              logDebug('consumer lag check', {
                lag,
                batch_size: flusher.size(),
                batch_max: config.batchSize,
              });

              if (lag < config.batchSize && flusher.size() > 0) {
                // Not enough messages waiting, start/reset timer.
                resetBatchTimer();
              } else if (lag >= config.batchSize) {
                // Many messages waiting, flush current batch to catch up.
                logInfo('flushing batch early due to lag', {
                  batch_size: flusher.size(),
                  lag,
                });
                await flushBatch();
              }
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
                flusher.add(batchItem);

                logDebug('message added to batch', {
                  event_id: envelope.eventId,
                  trace_id: envelope.traceId,
                  sensor_id: formatSensorId(payload.sensorId),
                  batch_size: flusher.size(),
                  batch_max: config.batchSize,
                });

                await maybeFlush();
              } catch (error) {
                // The record could not be parsed. Rather than silently
                // dropping it (which would let a later, valid record's
                // offset commit skip past it undetected), keep its offset in
                // the same batch as a poison item so it is explicitly routed
                // to the DLQ and only "skipped" once that is durable.
                const reason =
                  error instanceof Error ? error.message : String(error);
                logWarn('envelope parse error; routing to DLQ', {
                  partition: message.partition,
                  offset: message.offset.toString(),
                  reason,
                });

                flusher.add({
                  offset: message.offset + 1n,
                  partition: message.partition,
                  parseError: reason,
                  raw: new Uint8Array(message.value),
                });

                await maybeFlush();
              }
            }
          } finally {
            // Cleanup timer.
            if (batchTimer) {
              clearTimeout(batchTimer);
              batchTimer = null;
            }

            // Wait for any in-flight flush, then flush the remaining batch so
            // that graceful shutdown does not leave buffered work uncommitted.
            if (flusher.isFlushing()) {
              logInfo('waiting for in-flight flush before shutdown flush');
              await flusher.flush().catch(() => undefined);
            }

            if (flusher.size() > 0) {
              logInfo('flushing remaining batch on shutdown', {
                batch_size: flusher.size(),
              });
              await flusher.flush().catch((error) => {
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

      // Stop consuming first, then let the run loop drain and flush the
      // remaining batch before closing Kafka resources.
      await consumer.close();

      await runPromise?.catch(() => undefined);
      runPromise = null;

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
    getMetrics(): Readonly<BatcherMetrics> {
      return metrics;
    },
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => BatcherMetrics,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health' || request.url === '/healthz') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      logDebug('metrics endpoint requested');
      const metrics = getMetrics();
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

export async function startBatcher(): Promise<BatcherService> {
  const service = createBatcherService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startBatcher().catch((error: unknown) => {
    logError('failed to start (direct run)', error);
    process.exitCode = 1;
  });
}
