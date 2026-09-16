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
  formatSensorId,
  type TelemetryAuthorizedPayload,
  installShutdownHandler,
} from '@scp/core';
import { fromBinary } from '@bufbuild/protobuf';
import { Consumer } from '@platformatic/kafka';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import {
  loadPubsubBroadcasterConfig,
  type PubsubBroadcasterConfig,
} from './config.js';
import { createLibp2pPubsubClient } from './libp2p-node.js';
import { createConnectivityMonitor } from './connectivity.js';

interface PubsubBroadcasterMetrics {
  consumed: number;
  publishSuccess: number;
  publishFailure: number;
  /** Connected reserved peer ids (bounded by the configured reserved peer list). */
  connectedPeerIds: string[];
  connectedPeerCount: number;
  minConnectedPeers: number;
  kafkaPaused: boolean;
  pauseCount: number;
  resumeCount: number;
}

export interface PubsubBroadcasterService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<PubsubBroadcasterMetrics>;
  isReady(): boolean;
}

interface PubsubClient {
  start(): Promise<void>;
  stop(): Promise<void>;
  publish(topic: string, data: Uint8Array): Promise<void>;
  getConnectedPeerIds?: () => string[];
  getConnectedPeerCount?: () => number;
}

interface ConsumerMessagesStream extends AsyncIterable<{
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer | null;
}> {
  pause?: () => void;
  resume?: () => void;
}

interface PubsubBroadcasterDeps {
  createPubsubClient?: () => Promise<PubsubClient>;
  createConsumer?: () => Consumer;
  createHealthServer?: (
    getMetrics: () => PubsubBroadcasterMetrics,
    isReady: () => boolean,
    port: number
  ) => Server;
}

const logger = pino({
  name: 'pubsub-broadcaster',
  level:
    process.env.PUBSUB_BROADCASTER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info',
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
}

function logDebug(message: string, context?: Record<string, unknown>): void {
  logger.debug(context ?? {}, message);
}

function logError(
  message: string,
  error: unknown,
  context?: Record<string, unknown>
): void {
  logger.error(
    {
      ...(context ?? {}),
      error: error instanceof Error ? error.message : String(error),
    },
    message
  );
}

/**
 * Process and publish authorized telemetry envelope to PubSub
 * Simple handler without retry logic or result events (observability-only)
 */
export function handleTelemetryMessage(
  raw: Buffer,
  pubsub: PubsubClient,
  config: PubsubBroadcasterConfig,
  metrics: PubsubBroadcasterMetrics
): Promise<void> {
  try {
    const envelope = fromBinary(EnvelopeSchema, new Uint8Array(raw));

    if (envelope.eventType !== TELEMETRY_TOPICS.AUTHORIZED) {
      logDebug('non-authorized envelope ignored', {
        eventType: envelope.eventType,
      });
      return Promise.resolve();
    }

    const payload = fromBinary(
      TelemetryAuthorizedPayloadSchema,
      envelope.payload
    ) as TelemetryAuthorizedPayload;

    const sensorIdFormatted = formatSensorId(payload.sensorId);

    logDebug('authorized envelope received', {
      eventId: envelope.eventId,
      eventType: envelope.eventType,
      trace_id: envelope.traceId,
      sensor_id: sensorIdFormatted,
    });

    metrics.consumed += 1;

    // Forward the original SignedEnvelope to PubSub
    const signedEnvelopeBytes = payload.signedEnvelope;

    return pubsub
      .publish(config.pubsubTopic, signedEnvelopeBytes)
      .then(() => {
        metrics.publishSuccess += 1;
        logInfo('telemetry published to PubSub', {
          trace_id: envelope.traceId,
          sensor_id: sensorIdFormatted,
          pubsub_topic: config.pubsubTopic,
        });
      })
      .catch((error) => {
        metrics.publishFailure += 1;
        logWarn('PubSub publish failed (will not retry)', {
          trace_id: envelope.traceId,
          sensor_id: sensorIdFormatted,
          error: error instanceof Error ? error.message : String(error),
        });
      });
  } catch (error) {
    logWarn('envelope parse error', {
      reason: error instanceof Error ? error.message : String(error),
    });
    return Promise.resolve();
  }
}

/** Upper bound on how often connectivity is re-evaluated against the threshold. */
const MAX_CONNECTIVITY_POLL_INTERVAL_MS = 1000;

export function createPubsubBroadcasterService(
  config: PubsubBroadcasterConfig = loadPubsubBroadcasterConfig(),
  deps: PubsubBroadcasterDeps = {}
): PubsubBroadcasterService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'pubsub-broadcaster',
      bootstrapBrokers: config.kafkaBrokers,
    });
  const createPubsubClient =
    deps.createPubsubClient ??
    (() => createLibp2pPubsubClient(config, { logInfo, logWarn }));
  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let pubsubClient: PubsubClient | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let consumerStream: ConsumerMessagesStream | null = null;
  let connectivityPollTimer: ReturnType<typeof setInterval> | null = null;
  const monitoringEnabled = config.minConnectedPeers > 0;
  const metrics: PubsubBroadcasterMetrics = {
    consumed: 0,
    publishSuccess: 0,
    publishFailure: 0,
    connectedPeerIds: [],
    connectedPeerCount: 0,
    minConnectedPeers: config.minConnectedPeers,
    kafkaPaused: false,
    pauseCount: 0,
    resumeCount: 0,
  };

  const connectivityMonitor = monitoringEnabled
    ? createConnectivityMonitor({
        minConnectedPeers: config.minConnectedPeers,
        stabilizationIntervalMs: config.connectivityStabilizationIntervalMs,
        getConnectedPeerCount: () =>
          pubsubClient?.getConnectedPeerCount?.() ?? 0,
        onPause: () => {
          consumerStream?.pause?.();
          metrics.kafkaPaused = true;
          logWarn('pausing Kafka consumption; connected peers below minimum', {
            connectedPeerCount: metrics.connectedPeerCount,
            minConnectedPeers: config.minConnectedPeers,
          });
        },
        onResume: () => {
          consumerStream?.resume?.();
          metrics.kafkaPaused = false;
          logInfo('resuming Kafka consumption; connectivity recovered', {
            connectedPeerCount: metrics.connectedPeerCount,
            minConnectedPeers: config.minConnectedPeers,
          });
        },
      })
    : null;

  const getMetrics = (): PubsubBroadcasterMetrics => metrics;
  const isReady = (): boolean =>
    !monitoringEnabled ||
    metrics.connectedPeerCount >= config.minConnectedPeers;

  return {
    async start(): Promise<void> {
      if (started) {
        logInfo('start skipped; service already started');
        return;
      }

      started = true;
      logInfo('starting service', {
        consumerGroupId: config.consumerGroupId,
        kafkaBrokers: config.kafkaBrokers,
        pubsubTopic: config.pubsubTopic,
        reservedPeers: config.reservedPeers,
        minConnectedPeers: config.minConnectedPeers,
        healthPort: config.healthPort,
      });

      try {
        pubsubClient = await createPubsubClient();
        await pubsubClient.start();

        consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: true,
        });

        healthServer = createHealthServer(
          getMetrics,
          isReady,
          config.healthPort
        );

        if (connectivityMonitor) {
          metrics.kafkaPaused = connectivityMonitor.isPaused();
          if (metrics.kafkaPaused) {
            consumerStream.pause?.();
          }
          const pollIntervalMs = Math.min(
            MAX_CONNECTIVITY_POLL_INTERVAL_MS,
            config.connectivityStabilizationIntervalMs
          );
          connectivityPollTimer = setInterval(() => {
            metrics.connectedPeerIds =
              pubsubClient?.getConnectedPeerIds?.() ?? [];
            metrics.connectedPeerCount = metrics.connectedPeerIds.length;
            connectivityMonitor.notify();
            metrics.pauseCount = connectivityMonitor.getMetrics().pauseCount;
            metrics.resumeCount = connectivityMonitor.getMetrics().resumeCount;
          }, pollIntervalMs);
          connectivityPollTimer.unref?.();
        }

        runPromise = (async () => {
          for await (const message of consumerStream!) {
            if (!message.value) {
              logWarn('received null message value; skipping');
              continue;
            }
            await handleTelemetryMessage(
              message.value,
              pubsubClient!,
              config,
              metrics
            );
            logDebug('kafka message processed', {
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
              consumed: metrics.consumed,
            });
          }
        })();

        logInfo('service started');
      } catch (error) {
        logError('service failed to start', error);
        started = false;
        runPromise = null;

        if (connectivityPollTimer) {
          clearInterval(connectivityPollTimer);
          connectivityPollTimer = null;
        }

        if (healthServer) {
          await new Promise<void>((resolve) => {
            healthServer?.close(() => {
              resolve();
            });
          });
          healthServer = null;
        }

        await consumer.close().catch(() => undefined);
        await pubsubClient?.stop().catch(() => undefined);
        pubsubClient = null;
        consumerStream = null;
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
      logInfo('stopping service');
      if (connectivityPollTimer) {
        clearInterval(connectivityPollTimer);
        connectivityPollTimer = null;
      }
      connectivityMonitor?.stop();
      // Stop pulling new messages, then wait for any in-flight
      // `handleTelemetryMessage` call to finish before stopping the PubSub
      // client. Otherwise an autocommitted message could lose its publish
      // if the client is stopped while the publish is still pending.
      await consumer.close();
      await runPromise?.catch(() => undefined);
      runPromise = null;
      await pubsubClient?.stop();
      pubsubClient = null;
      consumerStream = null;
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
    getMetrics(): Readonly<PubsubBroadcasterMetrics> {
      return metrics;
    },
    isReady,
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => PubsubBroadcasterMetrics,
  isReady: () => boolean,
  port: number
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      // Liveness: the process is up. Peer connectivity does not affect this.
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/ready') {
      // Readiness: false when connected peers are below the configured minimum.
      const ready = isReady();
      logDebug('readiness check requested', { ready });
      response.statusCode = ready ? 200 : 503;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: ready ? 'ok' : 'unavailable' }));
      return;
    }

    if (request.url === '/metrics') {
      logDebug('metrics endpoint requested');
      const metrics = getMetrics();
      logInfo('metrics served', {
        consumed: metrics.consumed,
        publishSuccess: metrics.publishSuccess,
        publishFailure: metrics.publishFailure,
        connectedPeerCount: metrics.connectedPeerCount,
        minConnectedPeers: metrics.minConnectedPeers,
        kafkaPaused: metrics.kafkaPaused,
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

export async function startPubsubBroadcaster(): Promise<PubsubBroadcasterService> {
  const service = createPubsubBroadcasterService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startPubsubBroadcaster()
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
