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
import { create } from '@bufbuild/protobuf';
import { SignedEnvelope } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import type { TelemetryRejectedPayload } from '@scp/core';
import {
  formatSensorId,
  validateSignedEnvelope,
  REJECTION_CODES,
  TelemetryRejectedPayloadSchema,
  TelemetryAuthorizedPayloadSchema,
} from '@scp/core';
import { installShutdownHandler } from '@scp/core';
import {
  createRegistryReaderFromEnv,
  type RegistryReader,
} from '@scp/registry-sync';
import {
  createEndpointEventProducer,
  type EndpointEventProducer,
} from './producer.js';
import { Producer } from '@platformatic/kafka';
import { loadEndpointConfig } from './config.js';
import {
  createSensorAuthProvider,
  loadSensorAuthConfig,
} from './sensor-auth-factory.js';
import { logDebug, logError, logInfo, logWarn } from './logger.js';

export interface EndpointDeps {
  registryReader: RegistryReader;
  producer: EndpointEventProducer;
}

export interface EndpointAppOptions {
  timestampSkewSeconds?: number;
}

async function parseSignedEnvelope(request: FastifyRequest): Promise<{
  envelope: SignedEnvelope;
  rawBytes: Uint8Array;
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
  return { envelope: await validateSignedEnvelope(rawBytes, true), rawBytes };
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

  app.get('/health', async () => ({ status: 'ok' }));
  app.get('/metrics', async () => metrics);

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

      try {
        const parsed = await parseSignedEnvelope(request);
        parsedEnvelope = parsed.envelope;
        rawEnvelopeBytes = parsed.rawBytes;
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

      const timestampMs = Number(parsedEnvelope.timestamp);
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

      const nonceSeen = await deps.registryReader.isNonceSeen(
        parsedEnvelope.sensorId,
        parsedEnvelope.nonce
      );
      if (nonceSeen) {
        metrics.rejected += 1;
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

      try {
        await deps.registryReader.rememberNonce(
          parsedEnvelope.sensorId,
          parsedEnvelope.nonce
        );
      } catch (error) {
        metrics.rejected += 1;
        logError('failed to remember sensor nonce', error, {
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

export async function startEndpoint(): Promise<EndpointRuntime> {
  const config = loadEndpointConfig();
  const authConfig = loadSensorAuthConfig();

  logInfo('starting endpoint service', {
    port: config.port,
    source: config.source,
    auth_strategy: authConfig.strategy,
    kafka_broker_count: config.kafkaBrokers.length,
    timestamp_skew_seconds: config.timestampSkewSeconds,
  });

  const registryReader = createSensorAuthProvider(
    authConfig.strategy,
    createRegistryReaderFromEnv
  );

  // Create Kafka producer with idempotence enabled
  const kafkaProducer = new Producer({
    clientId: 'endpoint',
    bootstrapBrokers: config.kafkaBrokers,
    idempotent: true,
    acks: -1, // 'all' - wait for all in-sync replicas
  });

  const app = createEndpointApp(
    {
      registryReader,
      producer: createEndpointEventProducer(kafkaProducer, config.source),
    },
    {
      timestampSkewSeconds: config.timestampSkewSeconds,
    }
  );

  await app.listen({ host: '0.0.0.0', port: config.port });
  logInfo('listening', { port: config.port });

  return {
    app,
    async stop(): Promise<void> {
      logInfo('stopping endpoint service');
      // Stop accepting new connections and wait for in-flight requests
      // (which may still be producing to Kafka) to complete before
      // disconnecting the producer.
      await app.close();
      await kafkaProducer.close().catch((error: unknown) => {
        logError('error closing Kafka producer during shutdown', error);
      });
      logInfo('endpoint service stopped');
    },
  };
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
