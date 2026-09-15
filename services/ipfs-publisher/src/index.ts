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
  duplicatesSkipped: number;
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
 * Bounded set that remembers recently seen batch ids for in-process
 * deduplication, so a redelivered batch is not published to IPFS twice.
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
 * Publish a single batched telemetry payload to IPFS and emit a
 * `ipfs.published.v1` result event.
 */
async function publishBatched(
  batched: TelemetryBatchedPayload,
  ipfsClient: IpfsClient,
  producer: Producer,
  config: IpfsPublisherConfig,
  metrics: IpfsPublisherMetrics
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

  try {
    // Publish the pre-serialized SignedEnvelopeBatch, compressing at publish
    // time so the wire format on the batched topic stays uncompressed.
    const cid = await ipfsClient.add(
      batched.signedEnvelopeBatch,
      config.enableCompression
    );

    logInfo('batch published to IPFS', {
      batch_id: batched.batchId,
      cid,
      event_count: batched.eventCount,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
      compression: config.enableCompression,
    });

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(CID.parse(cid).bytes),
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

    metrics.batchesPublished += 1;
    metrics.eventsPublished += batched.eventCount;

    logInfo('batch result published to Kafka', {
      batch_id: batched.batchId,
      cid,
      event_count: batched.eventCount,
      result_topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
    });
  } catch (error) {
    metrics.publishFailure += 1;
    logError('batch publish failed', error, {
      batch_id: batched.batchId,
      event_count: batched.eventCount,
      unique_sensors: uniqueSensorIds.length,
      sensor_ids: uniqueSensorIds,
    });
    throw error;
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
    duplicatesSkipped: 0,
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
        enableCompression: config.enableCompression,
        healthPort: config.healthPort,
      });

      try {
        ipfsClient = await createIpfsClient(config.ipfsApiUrl);
        await ipfsClient.start();

        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.BATCHED],
          autocommit: false,
        });

        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = (async () => {
          const dedup = createBoundedDedup(10000);

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

                metrics.consumed += 1;

                // Skip batches already published in this process; still commit
                // the offset so the duplicate is not redelivered forever.
                if (dedup.has(batched.batchId)) {
                  metrics.duplicatesSkipped += 1;
                  logDebug('duplicate batch skipped', {
                    batch_id: batched.batchId,
                  });
                } else {
                  await publishBatched(
                    batched,
                    ipfsClient!,
                    producer,
                    config,
                    metrics
                  );
                  dedup.add(batched.batchId);
                }

                // Commit offset only after successful publish (or dedup skip).
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
 * Create an IPFS Kubo RPC client for publishing binary data.
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
