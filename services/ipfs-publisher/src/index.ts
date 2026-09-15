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
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryIpfsPublishedPayload_Compression as Compression,
  type TelemetryBatchedPayload,
  formatSensorId,
} from '@scp/core';
import { fromBinary, toBinary, create } from '@bufbuild/protobuf';
import { Consumer, Producer } from '@platformatic/kafka';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import { randomUUID } from 'node:crypto';
import { CID } from 'multiformats/cid';
import { compress } from '@napi-rs/lzma/xz';
import { loadIpfsPublisherConfig, type IpfsPublisherConfig } from './config.js';
import { logInfo, logWarn, logDebug, logError } from './logger.js';
import {
  MultiProviderIpfsClient,
  type ProviderStat,
} from './multi-provider-client.js';
import {
  createProvidersFromConfig,
  type IpfsProvider,
} from './providers/index.js';

interface IpfsPublisherMetrics {
  consumed: number;
  batchesPublished: number;
  eventsPublished: number;
  duplicatesSkipped: number;
  publishFailure: number;
}

interface IpfsPublisherMetricsSnapshot extends IpfsPublisherMetrics {
  durabilityFailures: number;
  pendingReplications: number;
  providers: Record<
    string,
    { success: number; failure: number; avgLatencyMs: number }
  >;
}

export interface IpfsPublisherService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<IpfsPublisherMetricsSnapshot>;
}

interface IpfsPublisherDeps {
  createProviders?: (config: IpfsPublisherConfig) => IpfsProvider[];
  createConsumer?: () => Consumer;
  createProducer?: () => Producer;
  createHealthServer?: (
    getMetrics: () => IpfsPublisherMetricsSnapshot,
    port: number
  ) => Server;
}

/**
 * Bounded set that remembers recently seen batch ids for in-process
 * deduplication, so a redelivered batch does not emit a duplicate
 * `ipfs.published.v1` result event.
 */
function createBoundedDedup(capacity: number): {
  has: (id: string) => boolean;
  add: (id: string) => void;
} {
  const seen = new Set<string>();
  const order: string[] = [];
  return {
    has: (id) => seen.has(id),
    add: (id) => {
      if (seen.has(id)) {
        return;
      }
      seen.add(id);
      order.push(id);
      if (order.length > capacity) {
        const evicted = order.shift();
        if (evicted !== undefined) {
          seen.delete(evicted);
        }
      }
    },
  };
}

/** Maximum attempts to process a single message before routing it to DLQ. */
const MAX_PROCESS_ATTEMPTS = 3;
/** Base delay between processing retries; grows linearly with attempt. */
const RETRY_BASE_DELAY_MS = 200;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Forward a message that exhausted its processing retries to the DLQ topic,
 * carrying the original bytes plus failure context in headers.
 */
async function publishToDlq(
  raw: Uint8Array,
  partition: number,
  offset: bigint,
  reason: string,
  producer: Producer,
  config: IpfsPublisherConfig
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
}

/**
 * Publish a single batched telemetry payload across all configured IPFS
 * providers and emit a `ipfs.published.v1` result event once the configured
 * durability policy is satisfied. Safe to call again for the same batch
 * (e.g. on Kafka redelivery): providers that already succeeded are not
 * re-uploaded, and the result event is emitted at most once.
 */
async function publishBatched(
  batched: TelemetryBatchedPayload,
  ipfsClient: MultiProviderIpfsClient,
  producer: Producer,
  config: IpfsPublisherConfig,
  metrics: IpfsPublisherMetrics,
  emittedDedup: { has: (id: string) => boolean; add: (id: string) => void }
): Promise<void> {
  const uniqueSensorIds = Array.from(
    new Set(batched.sensorIds.map((id) => formatSensorId(id)))
  );

  logInfo('publishing batch to IPFS', {
    batch_id: batched.batchId,
    event_count: batched.eventCount,
    unique_sensors: uniqueSensorIds.length,
    sensor_ids: uniqueSensorIds,
  });

  // Compress once (if enabled) so every provider receives identical bytes,
  // preserving content (and CID, where the providers use compatible
  // content-addressing) across providers.
  const dataToPublish = config.enableCompression
    ? await compress(batched.signedEnvelopeBatch)
    : batched.signedEnvelopeBatch;

  let result;
  try {
    result = await ipfsClient.publish(
      batched.batchId,
      dataToPublish,
      config.enableCompression
    );
  } catch (error) {
    metrics.publishFailure += 1;
    logError('batch publish failed on all providers', error, {
      batch_id: batched.batchId,
      event_count: batched.eventCount,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
    });
    throw error;
  }

  const providerSummary = Object.fromEntries(
    Array.from(result.providerOutcomes.entries()).map(([name, outcome]) => [
      name,
      outcome.success,
    ])
  );

  logInfo('batch publish attempt completed', {
    batch_id: batched.batchId,
    cid: result.cid,
    event_count: batched.eventCount,
    unique_sensors: uniqueSensorIds.length,
    sensor_ids: uniqueSensorIds,
    compression: config.enableCompression,
    durability_satisfied: result.durabilitySatisfied,
    providers: providerSummary,
  });

  if (!result.durabilitySatisfied) {
    metrics.publishFailure += 1;
    throw new Error(
      `durability policy "${config.durabilityPolicy}" not satisfied for batch ${batched.batchId} (providers: ${JSON.stringify(providerSummary)})`
    );
  }

  if (emittedDedup.has(batched.batchId)) {
    metrics.duplicatesSkipped += 1;
    logDebug('ipfs.published event already emitted for batch; skipping', {
      batch_id: batched.batchId,
    });
    return;
  }

  const payload = create(TelemetryIpfsPublishedPayloadSchema, {
    cid: Buffer.from(CID.parse(result.cid).bytes),
    eventCount: batched.eventCount,
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

  await producer.send({
    messages: [
      {
        topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
        value: Buffer.from(toBinary(EnvelopeSchema, resultEnvelope)),
      },
    ],
  });

  emittedDedup.add(batched.batchId);
  metrics.batchesPublished += 1;
  metrics.eventsPublished += batched.eventCount;

  logInfo('batch result published to Kafka', {
    batch_id: batched.batchId,
    cid: result.cid,
    event_count: batched.eventCount,
    result_topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
  });
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

  const createProviders =
    deps.createProviders ??
    ((cfg: IpfsPublisherConfig) => createProvidersFromConfig(cfg));
  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let ipfsClient: MultiProviderIpfsClient | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let shouldStop = false;

  const metrics: IpfsPublisherMetrics = {
    consumed: 0,
    batchesPublished: 0,
    eventsPublished: 0,
    duplicatesSkipped: 0,
    publishFailure: 0,
  };

  const buildMetricsSnapshot = (): IpfsPublisherMetricsSnapshot => {
    const providers: Record<
      string,
      { success: number; failure: number; avgLatencyMs: number }
    > = {};
    if (ipfsClient) {
      for (const [name, stat] of ipfsClient.getProviderStats().entries()) {
        const stats: ProviderStat = stat;
        const totalAttempts = stats.success + stats.failure;
        providers[name] = {
          success: stats.success,
          failure: stats.failure,
          avgLatencyMs:
            totalAttempts > 0 ? stats.totalLatencyMs / totalAttempts : 0,
        };
      }
    }

    return {
      ...metrics,
      durabilityFailures: ipfsClient?.getDurabilityFailureCount() ?? 0,
      pendingReplications: ipfsClient?.getPendingReplicationCount() ?? 0,
      providers,
    };
  };

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
        providers: config.providers,
        durabilityPolicy: config.durabilityPolicy,
        durabilityMinSuccessCount: config.durabilityMinSuccessCount,
        enableCompression: config.enableCompression,
        healthPort: config.healthPort,
      });

      try {
        ipfsClient = new MultiProviderIpfsClient(createProviders(config), {
          durability: {
            policy: config.durabilityPolicy,
            minSuccessCount: config.durabilityMinSuccessCount,
          },
          retryBaseDelayMs: config.providerRetryBaseDelayMs,
          retryMaxDelayMs: config.providerRetryMaxDelayMs,
          maxPendingReplications: config.providerReplicationMaxPending,
        });
        await ipfsClient.start();

        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.BATCHED],
          autocommit: false,
        });

        healthServer = createHealthServer(
          buildMetricsSnapshot,
          config.healthPort
        );

        runPromise = (async () => {
          const emittedDedup = createBoundedDedup(10000);

          messageLoop: for await (const message of consumerStream) {
            if (shouldStop) {
              break;
            }

            if (!message.value) {
              logWarn('received null message value; skipping');
              continue;
            }

            const raw = new Uint8Array(message.value);

            // Retry processing of this message in place before moving on to
            // the next one. Continuing to later messages on failure without
            // committing would let a later successful commit (a higher
            // offset) silently skip past this uncommitted one, breaking the
            // at-least-once contract.
            for (
              let attempt = 1;
              attempt <= MAX_PROCESS_ATTEMPTS;
              attempt += 1
            ) {
              try {
                const envelope = fromBinary(EnvelopeSchema, raw);

                if (envelope.eventType !== TELEMETRY_TOPICS.BATCHED) {
                  logDebug('non-batched envelope ignored', {
                    eventType: envelope.eventType,
                  });
                  continue messageLoop;
                }

                const batched = fromBinary(
                  TelemetryBatchedPayloadSchema,
                  envelope.payload
                ) as TelemetryBatchedPayload;

                if (attempt === 1) {
                  metrics.consumed += 1;
                }

                await publishBatched(
                  batched,
                  ipfsClient!,
                  producer,
                  config,
                  metrics,
                  emittedDedup
                );

                // Commit offset only once the configured durability policy
                // is satisfied (checked inside publishBatched, which throws
                // otherwise).
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
                  batch_id: batched.batchId,
                  partition: message.partition,
                  offset: (message.offset + 1n).toString(),
                });

                continue messageLoop;
              } catch (error) {
                const reason =
                  error instanceof Error ? error.message : String(error);

                if (attempt < MAX_PROCESS_ATTEMPTS) {
                  logWarn('batch processing failed, retrying', {
                    partition: message.partition,
                    offset: message.offset.toString(),
                    attempt,
                    max_attempts: MAX_PROCESS_ATTEMPTS,
                    error: reason,
                  });
                  await sleep(RETRY_BASE_DELAY_MS * attempt);
                  continue;
                }

                logError(
                  'batch processing exhausted retries, routing to DLQ',
                  error,
                  {
                    partition: message.partition,
                    offset: message.offset.toString(),
                  }
                );

                try {
                  await publishToDlq(
                    raw,
                    message.partition,
                    message.offset,
                    reason,
                    producer,
                    config
                  );

                  // Only commit past the poisoned message now that it is
                  // durably routed to the DLQ, so nothing is silently lost.
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

                  logWarn('poisoned batch routed to DLQ and committed', {
                    partition: message.partition,
                    offset: message.offset.toString(),
                  });
                } catch (dlqError) {
                  // Cannot safely commit past this message: stop consuming
                  // rather than let a later message's commit skip past it.
                  logError(
                    'failed to route batch to DLQ; stopping consumption to avoid silent offset skip',
                    dlqError,
                    {
                      partition: message.partition,
                      offset: message.offset.toString(),
                    }
                  );
                  break messageLoop;
                }
              }
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

      await runPromise?.catch(() => undefined);
      runPromise = null;

      await producer.close().catch(() => undefined);
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

      logInfo('service stopped');
    },
    getMetrics(): Readonly<IpfsPublisherMetricsSnapshot> {
      return buildMetricsSnapshot();
    },
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => IpfsPublisherMetricsSnapshot,
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
      logInfo('metrics served', {
        consumed: metrics.consumed,
        batchesPublished: metrics.batchesPublished,
        eventsPublished: metrics.eventsPublished,
        duplicatesSkipped: metrics.duplicatesSkipped,
        publishFailure: metrics.publishFailure,
        durabilityFailures: metrics.durabilityFailures,
        pendingReplications: metrics.pendingReplications,
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
