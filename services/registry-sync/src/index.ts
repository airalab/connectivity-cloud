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
import Redis from 'ioredis';
import { installShutdownHandler } from '@scp/core';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import {
  SubstrateFinalizedRegistryEventSource,
  type FinalizedRegistryEventSource,
} from './chain-source.js';
import { loadRegistrySyncConfig, type RegistrySyncConfig } from './config.js';
import {
  createRedisKeyspace,
  mapRegistryEventToUpdate,
  toChainEventId,
  type RegistryEvent,
} from './keyspace.js';
import { logDebug, logError, logInfo, logWarn } from './logger.js';
import { RedisProjectionStore, type RedisLike } from './projection-store.js';

export * from './chain-source.js';
export * from './config.js';
export * from './keyspace.js';
export * from './projection-store.js';
export * from './reader.js';

interface RegistrySyncMetrics {
  syncHeight: number;
  latestFinalizedHeight: number;
  updateCount: number;
  failureCount: number;
}

export interface RegistrySyncService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<RegistrySyncMetrics>;
}

interface RegistrySyncDependencies {
  config?: RegistrySyncConfig;
  redis?: RedisLike;
  eventSource?: FinalizedRegistryEventSource;
  enableHealthServer?: boolean;
  sleep?: (ms: number) => Promise<void>;
}

type RedisConstructor = new (url: string) => RedisLike;

export function createRegistrySyncService(
  deps: RegistrySyncDependencies = {}
): RegistrySyncService {
  const config = deps.config ?? loadRegistrySyncConfig();
  const RedisClient = Redis as unknown as RedisConstructor;
  const redis = deps.redis ?? new RedisClient(config.redisUrl);
  const keyspace = createRedisKeyspace(config.redisKeyPrefix);
  const projectionStore = new RedisProjectionStore(redis, keyspace);
  const eventSource =
    deps.eventSource ??
    new SubstrateFinalizedRegistryEventSource(config.substrateWsUrl);
  const sleep = deps.sleep ?? defaultSleep;

  let started = false;
  let healthServer: Server | null = null;
  const metrics: RegistrySyncMetrics = {
    syncHeight: 0,
    latestFinalizedHeight: 0,
    updateCount: 0,
    failureCount: 0,
  };

  async function processEvent(event: RegistryEvent): Promise<void> {
    const eventId = toChainEventId(event);
    logDebug('processing registry event', {
      eventId,
      blockHeight: event.blockHeight,
      eventIndex: event.eventIndex,
      section: event.section,
      method: event.method,
      sensorId: event.sensorId,
    });
    logStructured('processing_registry_event', {
      eventId,
      blockHeight: event.blockHeight,
      eventIndex: event.eventIndex,
      section: event.section,
      method: event.method,
    });
    metrics.latestFinalizedHeight = Math.max(
      metrics.latestFinalizedHeight,
      event.blockHeight
    );

    // Check idempotency - skip if already processed
    if (await projectionStore.hasProcessed(eventId)) {
      logDebug('registry event already processed, skipping', { eventId });
      await projectionStore.commitCursorHeight(event.blockHeight);
      metrics.syncHeight = Math.max(metrics.syncHeight, event.blockHeight);
      return;
    }

    // Map event to projection update
    const projection = mapRegistryEventToUpdate(event);
    if (!projection) {
      logError('registry event missing sensorId for projection', {
        eventId,
        blockHeight: event.blockHeight,
        eventIndex: event.eventIndex,
        section: event.section,
        method: event.method,
      });

      await projectionStore.publishDlq({
        eventId,
        blockHeight: event.blockHeight,
        eventIndex: event.eventIndex,
        reason: 'Registry event missing sensorId for projection',
        section: event.section,
        method: event.method,
        rawData: event.rawData,
      });

      metrics.failureCount += 1;
      await projectionStore.markProcessed(eventId);
      await projectionStore.commitCursorHeight(event.blockHeight);
      metrics.syncHeight = Math.max(metrics.syncHeight, event.blockHeight);
      return;
    }

    // Apply projection with retry logic
    let attempt = 0;
    let lastError: unknown = null;
    while (attempt < config.maxRetries) {
      attempt += 1;
      try {
        await projectionStore.applyProjection(projection, {
          updatedAtBlock: event.blockHeight,
          updatedAtEvent: eventId,
        });

        await projectionStore.markProcessed(eventId);
        await projectionStore.commitCursorHeight(event.blockHeight);
        metrics.syncHeight = Math.max(metrics.syncHeight, event.blockHeight);
        metrics.updateCount += 1;

        logStructured('projection_updated', {
          eventId,
          blockHeight: event.blockHeight,
          eventIndex: event.eventIndex,
          section: event.section,
          method: event.method,
          sensorId: projection.sensorId,
          enabled: projection.enabled,
        });

        logDebug('registry event processing completed', {
          eventId,
          syncHeight: metrics.syncHeight,
          latestFinalizedHeight: metrics.latestFinalizedHeight,
        });
        return;
      } catch (error: unknown) {
        lastError = error;
        metrics.failureCount += 1;

        if (attempt < config.maxRetries) {
          logWarn('projection failed, will retry', {
            eventId,
            attempt,
            maxRetries: config.maxRetries,
            error: String(error),
          });
          await sleep(config.retryBackoffMs);
        }
      }
    }

    // Exhausted retries - publish to DLQ
    logError('projection exhausted retries, routing to DLQ', {
      eventId,
      blockHeight: event.blockHeight,
      eventIndex: event.eventIndex,
      error: lastError,
    });

    await projectionStore.publishDlq({
      eventId,
      blockHeight: event.blockHeight,
      eventIndex: event.eventIndex,
      reason: String(lastError),
      section: event.section,
      method: event.method,
      rawData: event.rawData,
    });

    await projectionStore.markProcessed(eventId);
    await projectionStore.commitCursorHeight(event.blockHeight);
    metrics.syncHeight = Math.max(metrics.syncHeight, event.blockHeight);
  }

  return {
    async start(): Promise<void> {
      if (started) {
        return;
      }

      started = true;
      logInfo('starting registry sync service', {
        substrateWsUrl: config.substrateWsUrl,
        redisUrl: config.redisUrl,
        redisKeyPrefix: config.redisKeyPrefix,
        maxRetries: config.maxRetries,
        retryBackoffMs: config.retryBackoffMs,
        healthPort: config.healthPort,
      });
      await eventSource.connect();
      logDebug('connected event source, loading cursor and finalized height');
      metrics.syncHeight = await projectionStore.loadCursorHeight();
      metrics.latestFinalizedHeight =
        await eventSource.getLatestFinalizedHeight();
      logInfo('registry sync cursor loaded', {
        syncHeight: metrics.syncHeight,
        latestFinalizedHeight: metrics.latestFinalizedHeight,
      });

      if (deps.enableHealthServer ?? true) {
        healthServer = startHealthAndMetricsServer(config.healthPort, metrics);
        logInfo('registry sync health server started', {
          healthPort: config.healthPort,
        });
      }

      logInfo('registry sync starting finalized event stream', {
        fromHeight: metrics.syncHeight + 1,
      });
      await eventSource.startFrom(
        metrics.syncHeight + 1,
        async (event) => {
          await processEvent(event);
        },
        async (height) => {
          await projectionStore.commitCursorHeight(height);
          metrics.syncHeight = Math.max(metrics.syncHeight, height);
          metrics.latestFinalizedHeight = Math.max(
            metrics.latestFinalizedHeight,
            height
          );
          logDebug('advanced sync cursor from finalized head', {
            height,
            syncHeight: metrics.syncHeight,
            latestFinalizedHeight: metrics.latestFinalizedHeight,
          });
        }
      );

      logStructured('registry_sync_started', {
        substrateWsUrl: config.substrateWsUrl,
        redisUrl: config.redisUrl,
        redisKeyPrefix: config.redisKeyPrefix,
        syncHeight: metrics.syncHeight,
      });
    },
    async stop(): Promise<void> {
      if (!started) {
        return;
      }

      started = false;
      logInfo('stopping registry sync service');
      await eventSource.stop();
      await eventSource.disconnect();

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
        logInfo('registry sync health server stopped');
      }

      if (typeof redis.quit === 'function') {
        await redis.quit();
      } else if (typeof redis.disconnect === 'function') {
        redis.disconnect();
      }

      logStructured('registry_sync_stopped', {
        syncHeight: metrics.syncHeight,
        updateCount: metrics.updateCount,
        failureCount: metrics.failureCount,
      });
    },
    getMetrics(): Readonly<RegistrySyncMetrics> {
      return metrics;
    },
  };
}

function startHealthAndMetricsServer(
  port: number,
  metrics: RegistrySyncMetrics
): Server {
  const server = createServer((request, response) => {
    if (request.url === '/health') {
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(
        JSON.stringify({
          status: 'ok',
          sync_height: metrics.syncHeight,
          latest_finalized_height: metrics.latestFinalizedHeight,
          lag: Math.max(metrics.latestFinalizedHeight - metrics.syncHeight, 0),
        })
      );
      return;
    }

    if (request.url === '/metrics') {
      response.statusCode = 200;
      response.setHeader('content-type', 'text/plain; version=0.0.4');
      response.end(
        [
          '# HELP registry_sync_height Current synced finalized block height.',
          '# TYPE registry_sync_height gauge',
          `registry_sync_height ${metrics.syncHeight}`,
          '# HELP registry_sync_lag Finalized height lag.',
          '# TYPE registry_sync_lag gauge',
          `registry_sync_lag ${Math.max(metrics.latestFinalizedHeight - metrics.syncHeight, 0)}`,
          '# HELP registry_sync_updates_total Projection updates applied.',
          '# TYPE registry_sync_updates_total counter',
          `registry_sync_updates_total ${metrics.updateCount}`,
          '# HELP registry_sync_failures_total Processing failures.',
          '# TYPE registry_sync_failures_total counter',
          `registry_sync_failures_total ${metrics.failureCount}`,
        ].join('\n')
      );
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });

  server.listen({ host: '0.0.0.0', port });
  return server;
}

function logStructured(message: string, fields: Record<string, unknown>): void {
  logInfo(message, fields);
}

async function defaultSleep(ms: number): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  const service = createRegistrySyncService();
  service
    .start()
    .then(() => {
      installShutdownHandler(() => service.stop(), {
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
