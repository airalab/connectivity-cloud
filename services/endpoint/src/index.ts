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
import rateLimit from '@fastify/rate-limit';
import Fastify from 'fastify';
import type { FastifyInstance, FastifyRequest } from 'fastify';
import { fileURLToPath } from 'node:url';
import { create, fromBinary } from '@bufbuild/protobuf';
import { SignedEnvelope } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { MessageSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/core/v1/message_pb.js';
import type { NonceStore, TelemetryRejectedPayload } from '@scp/core';
import {
  formatSensorId,
  validateSignedEnvelope,
  REJECTION_CODES,
  TelemetryRejectedPayloadSchema,
  TelemetryAuthorizedPayloadSchema,
} from '@scp/core';
import { installShutdownHandler } from '@scp/core';
import {
  assertNonceTtl,
  closeRedisClient,
  createKafkaClientOptions,
  createRedisClient,
  createRedisNonceStore,
  loadKafkaSecurityConfig,
  loadNonceConfig,
  loadRedisConfig,
} from '@scp/infra';
import type { RegistryReader } from './registry-reader.js';
import {
  createEndpointEventProducer,
  type EndpointEventProducer,
} from './producer.js';
import { Producer } from '@platformatic/kafka';
import {
  loadEndpointConfig,
  type StatusMetricConfig,
  type StatusTargetConfig,
} from './config.js';
import {
  createSensorAuthProvider,
  loadSensorAuthConfig,
} from './sensor-auth-factory.js';
import { logDebug, logError, logInfo, logWarn } from './logger.js';
import { renderStatusPage } from './status-page.js';

export interface EndpointDeps {
  registryReader: RegistryReader;
  producer: EndpointEventProducer;
  /** Shared (cross-instance) replay protection, independent of the auth strategy. */
  nonceStore: NonceStore;
}

/** A dependency probe used by `/ready`; rejects when the dependency is unusable. */
export type ReadinessCheck = () => Promise<void>;

export interface EndpointAppOptions {
  timestampSkewSeconds?: number;
  /** Own listening port, used to probe `/health` for the status page. */
  statusPort?: number;
  /** Sibling services (with default ports) shown on the status page. */
  statusTargets?: StatusTargetConfig[];
  /** Simple headline metrics (sourced from sibling services) shown on the status page. */
  statusMetrics?: StatusMetricConfig[];
  /** Probes for required dependencies (e.g. Redis, Kafka) reported by `/ready`. */
  readinessChecks?: Record<string, ReadinessCheck>;
}

async function parseSignedEnvelope(request: FastifyRequest): Promise<{
  envelope: SignedEnvelope;
  rawBytes: Uint8Array;
  timestampMs: number;
}> {
  const contentType = request.headers['content-type']?.toLowerCase() ?? '';
  if (
    !contentType.includes('application/protobuf') &&
    !contentType.includes('application/x-protobuf')
  ) {
    throw new Error('Expected Content-Type application/protobuf');
  }
  if (!(request.body instanceof Uint8Array || Buffer.isBuffer(request.body))) {
    throw new Error('Expected binary protobuf request body');
  }
  const rawBytes = Uint8Array.from(request.body);
  const envelope = await validateSignedEnvelope(rawBytes, true);
  // The measurement timestamp now lives in `core.v1.Message.metadata.timestamp`
  // rather than on `SignedEnvelope` (moved so the message stays
  // self-contained; see connectivity-protocol v1-beta.2).
  const message = fromBinary(MessageSchema, envelope.message);
  const timestampMs = Number(message.metadata?.timestamp ?? 0n);
  return { envelope, rawBytes, timestampMs };
}

export function createEndpointApp(
  deps: EndpointDeps,
  options: EndpointAppOptions = {}
): FastifyInstance {
  const app = Fastify({ logger: false });
  const timestampSkewSeconds = options.timestampSkewSeconds ?? 300;
  const metrics = {
    accepted: 0,
    rejected: 0,
    kafkaErrors: 0,
    duplicateNonces: 0,
    nonceStoreErrors: 0,
  };

  app.addContentTypeParser(
    ['application/protobuf', 'application/x-protobuf'],
    { parseAs: 'buffer' },
    (_request, body, done) => done(null, body)
  );

  app.register(rateLimit, {
    max: 100,
    timeWindow: '1 minute',
  });

  app.get('/health', async (_request, reply) => {
    reply.header('access-control-allow-origin', '*');
    return { status: 'ok' };
  });
  // Readiness (as opposed to the `/health` liveness probe) fails whenever a
  // required dependency cannot be used, so traffic is not routed to an
  // instance that could only answer 503. Error details are never returned.
  app.get('/ready', async (_request, reply) => {
    const checks = Object.entries(options.readinessChecks ?? {});
    const failed: string[] = [];
    await Promise.all(
      checks.map(async ([name, check]) => {
        try {
          await check();
        } catch (error) {
          failed.push(name);
          logWarn('readiness check failed', {
            check: name,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      })
    );
    if (failed.length > 0) {
      return reply.code(503).send({ status: 'unavailable', failed });
    }
    return { status: 'ready' };
  });
  app.get('/metrics', async (_request, reply) => {
    reply.header('access-control-allow-origin', '*');
    return metrics;
  });

  // Static, minimalistic status page: the server renders identical markup
  // on every request (no server-side probing). A small inline script in
  // the page itself fetches each service's `/health` endpoint (ports
  // sourced from `.env`) using the browser's own hostname, and keeps the
  // table updated on an interval.
  const statusPageHtml = renderStatusPage({
    selfPort: options.statusPort ?? 3000,
    targets: options.statusTargets ?? [],
    metrics: options.statusMetrics ?? [],
  });
  app.get('/', async (_request, reply) =>
    reply.type('text/html; charset=utf-8').send(statusPageHtml)
  );

  // Rejection events are published fire-and-forget from request handlers so
  // that Kafka latency never delays the HTTP response. Track the in-flight
  // publishes here so `app.close()` can wait for them to settle before the
  // Kafka producer is disconnected during shutdown.
  const pendingRejectedPublishes = new Set<Promise<void>>();
  app.addHook('onClose', async () => {
    await Promise.allSettled(pendingRejectedPublishes);
  });

  app.post(
    '/v1/telemetry',
    {
      config: {
        rateLimit: {
          max: 100,
          timeWindow: '1 minute',
        },
      },
    },
    async (request, reply) => {
      const traceId = request.headers['x-request-id']?.toString() ?? request.id;
      let parsedEnvelope: SignedEnvelope;
      let rawEnvelopeBytes: Uint8Array;
      let timestampMs: number;

      try {
        const parsed = await parseSignedEnvelope(request);
        parsedEnvelope = parsed.envelope;
        rawEnvelopeBytes = parsed.rawBytes;
        timestampMs = parsed.timestampMs;
      } catch (error) {
        metrics.rejected += 1;
        logWarn(
          'telemetry rejected due to invalid envelope (corrupted or bad signature)',
          {
            trace_id: traceId,
            error: error instanceof Error ? error.message : String(error),
          }
        );
        return reply
          .code(400)
          .send({ status: 'rejected', error_code: 'invalid_envelope' });
      }

      const publishRejectedEvent = (payload: TelemetryRejectedPayload) => {
        const publishPromise = deps.producer
          .publishRejected(payload, traceId)
          .then((eventId) => {
            logInfo('telemetry rejected event published', {
              event_id: eventId,
              trace_id: traceId,
              sensor_id: payload.sensorId
                ? formatSensorId(payload.sensorId)
                : undefined,
              reason_code: payload.reasonCode,
            });
          })
          .catch((error) => {
            logError('failed to publish rejected event', error, {
              trace_id: traceId,
              sensor_id: payload.sensorId
                ? formatSensorId(payload.sensorId)
                : undefined,
              reason_code: payload.reasonCode,
            });
          });
        pendingRejectedPublishes.add(publishPromise);
        void publishPromise.finally(() =>
          pendingRejectedPublishes.delete(publishPromise)
        );
      };

      const skewMs = Math.abs(Date.now() - timestampMs);
      if (
        !Number.isSafeInteger(timestampMs) ||
        skewMs > timestampSkewSeconds * 1000
      ) {
        metrics.rejected += 1;
        publishRejectedEvent(
          create(TelemetryRejectedPayloadSchema, {
            sensorId: parsedEnvelope.sensorId,
            reasonCode: REJECTION_CODES.STALE_TIMESTAMP,
            reasonMessage: 'Timestamp outside allowed skew window',
          })
        );
        return reply
          .code(401)
          .send({ status: 'rejected', error_code: 'stale_timestamp' });
      }

      const record = await deps.registryReader.getSensorRecord(
        parsedEnvelope.sensorId
      );
      if (!record || !record.enabled) {
        metrics.rejected += 1;
        publishRejectedEvent(
          create(TelemetryRejectedPayloadSchema, {
            sensorId: parsedEnvelope.sensorId,
            reasonCode: REJECTION_CODES.SENSOR_FORBIDDEN,
            reasonMessage: 'Sensor is unknown or disabled',
          })
        );
        return reply
          .code(403)
          .send({ status: 'rejected', error_code: 'sensor_forbidden' });
      }

      // The claim is atomic and shared by all instances, and happens before
      // the Kafka publish. It is deliberately never released afterwards: a
      // failed or timed-out publish may still have been accepted by the broker.
      let claimed: boolean;
      try {
        claimed = await deps.nonceStore.claim(
          parsedEnvelope.sensorId,
          parsedEnvelope.nonce
        );
      } catch (error) {
        metrics.nonceStoreErrors += 1;
        metrics.rejected += 1;
        logError('nonce store unavailable', error, {
          trace_id: traceId,
          sensor_id: formatSensorId(parsedEnvelope.sensorId),
        });
        return reply
          .code(503)
          .send({ status: 'rejected', error_code: 'nonce_store_unavailable' });
      }
      if (!claimed) {
        metrics.rejected += 1;
        metrics.duplicateNonces += 1;
        publishRejectedEvent(
          create(TelemetryRejectedPayloadSchema, {
            sensorId: parsedEnvelope.sensorId,
            reasonCode: REJECTION_CODES.DUPLICATE_NONCE,
            reasonMessage: 'Nonce already used for this sensor',
          })
        );
        return reply
          .code(409)
          .send({ status: 'rejected', error_code: 'duplicate_nonce' });
      }

      logDebug('sensor message validation pass', {
        trace_id: traceId,
        sensor_id: formatSensorId(parsedEnvelope.sensorId),
      });

      try {
        await deps.producer.publishAuthorized(
          create(TelemetryAuthorizedPayloadSchema, {
            sensorId: parsedEnvelope.sensorId,
            signedEnvelope: rawEnvelopeBytes,
          }),
          traceId
        );
      } catch (error) {
        metrics.kafkaErrors += 1;
        metrics.rejected += 1;
        logError('failed to publish authorized telemetry event', error, {
          trace_id: traceId,
          sensor_id: formatSensorId(parsedEnvelope.sensorId),
        });
        return reply
          .code(503)
          .send({ status: 'rejected', error_code: 'kafka_unavailable' });
      }

      metrics.accepted += 1;
      return reply.code(202).send({ status: 'accepted' });
    }
  );

  return app;
}

export interface EndpointRuntime {
  app: FastifyInstance;
  /**
   * Graceful shutdown: stop accepting new connections and let in-flight
   * requests finish (Fastify's `close()`), then disconnect the Kafka
   * producer only once no more requests can produce to it.
   */
  stop(): Promise<void>;
}

/** Caches a probe's outcome briefly so frequent `/ready` polls do not hammer dependencies. */
function cacheCheck(check: ReadinessCheck, ttlMs: number): ReadinessCheck {
  let last: { at: number; error?: unknown } | undefined;
  return async () => {
    if (!last || Date.now() - last.at > ttlMs) {
      try {
        await check();
        last = { at: Date.now() };
      } catch (error) {
        last = { at: Date.now(), error };
      }
    }
    if (last.error !== undefined) throw last.error;
  };
}

const READINESS_CACHE_MS = 2000;

export async function startEndpoint(): Promise<EndpointRuntime> {
  const config = loadEndpointConfig();
  const authConfig = loadSensorAuthConfig();
  const nonceConfig = loadNonceConfig();
  assertNonceTtl(nonceConfig.ttlSeconds, config.timestampSkewSeconds);
  const redisConfig = loadRedisConfig();
  const kafkaOptions = createKafkaClientOptions({
    brokers: config.kafkaBrokers,
    security: config.kafkaSecurity ?? loadKafkaSecurityConfig(),
  });

  logInfo('starting endpoint service', {
    port: config.port,
    source: config.source,
    auth_strategy: authConfig.strategy,
    kafka_broker_count: config.kafkaBrokers.length,
    kafka_security_protocol: (config.kafkaSecurity ?? loadKafkaSecurityConfig())
      .protocol,
    timestamp_skew_seconds: config.timestampSkewSeconds,
    nonce_ttl_seconds: nonceConfig.ttlSeconds,
    nonce_prefix: nonceConfig.prefix,
  });

  // Startup fails (no local/unauthenticated fallback) when Redis is
  // unreachable or rejects TLS/AUTH.
  const redis = await createRedisClient(redisConfig);
  const nonceStore = createRedisNonceStore(
    redis,
    nonceConfig,
    closeRedisClient
  );

  let registryReader: RegistryReader | undefined;
  let kafkaProducer: Producer | undefined;
  try {
    registryReader = await createSensorAuthProvider(authConfig.strategy);

    // Idempotent producer; TLS/SASL come from the shared Kafka factory.
    kafkaProducer = new Producer({
      clientId: 'endpoint',
      ...kafkaOptions,
      idempotent: true,
      acks: -1, // 'all' - wait for all in-sync replicas
    });
    const producerForChecks = kafkaProducer;
    const checkKafka: ReadinessCheck = async () => {
      await producerForChecks.metadata({ topics: [] });
    };
    // Fails startup on broker authentication or trust errors.
    await checkKafka();

    const app = createEndpointApp(
      {
        registryReader,
        nonceStore,
        producer: createEndpointEventProducer(kafkaProducer, config.source),
      },
      {
        timestampSkewSeconds: config.timestampSkewSeconds,
        statusPort: config.port,
        statusTargets: config.statusTargets,
        statusMetrics: config.statusMetrics,
        readinessChecks: {
          redis: cacheCheck(async () => {
            await redis.ping();
          }, READINESS_CACHE_MS),
          kafka: cacheCheck(checkKafka, READINESS_CACHE_MS),
        },
      }
    );

    await app.listen({ host: '0.0.0.0', port: config.port });
    logInfo('listening', { port: config.port });

    const authProvider = registryReader;
    const producer = kafkaProducer;
    return {
      app,
      async stop(): Promise<void> {
        logInfo('stopping endpoint service');
        // Stop accepting new connections and wait for in-flight requests
        // (which may still use Redis and Kafka) to complete before
        // disconnecting either client.
        await app.close();
        await authProvider.close?.().catch((error: unknown) => {
          logError('error closing sensor auth provider during shutdown', error);
        });
        await nonceStore.close().catch((error: unknown) => {
          logError('error closing nonce store during shutdown', error);
        });
        await producer.close().catch((error: unknown) => {
          logError('error closing Kafka producer during shutdown', error);
        });
        logInfo('endpoint service stopped');
      },
    };
  } catch (error) {
    await registryReader?.close?.().catch(() => undefined);
    await kafkaProducer?.close().catch(() => undefined);
    await nonceStore.close().catch(() => undefined);
    throw error;
  }
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startEndpoint()
    .then((runtime) => {
      installShutdownHandler(() => runtime.stop(), {
        onSignal: (signal) => logInfo('received shutdown signal', { signal }),
        onShutdownError: (error) =>
          logError('error during graceful shutdown', error),
      });
    })
    .catch((error: unknown) => {
      logError('failed to start', error);
      process.exitCode = 1;
    });
}
