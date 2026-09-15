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
  type TelemetryAuthorizedPayload,
  formatSensorId,
} from '@scp/core';
import { fromBinary } from '@bufbuild/protobuf';
import Redis from 'ioredis';
import { Consumer } from '@platformatic/kafka';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import {
  loadHeartbeatTrackerConfig,
  type HeartbeatTrackerConfig,
} from './config.js';

interface HeartbeatTrackerMetrics {
  sensors_online: number;
  sensors_total_tracked: number;
  online_window_ms: number;
  consumed: number;
  sensor_uptime_seconds: Record<string, number>;
  sensors_uptime: Array<{
    sensor_id: string;
    online: boolean;
    first_seen: string;
    last_seen: string;
    uptime_seconds: number;
    seconds_since_last_seen: number;
  }>;
  max_uptime_seconds: number;
  avg_uptime_seconds: number;
}

export interface HeartbeatTrackerService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Promise<Readonly<HeartbeatTrackerMetrics>>;
}

export interface HeartbeatTrackerState {
  recordAuthorizedSensor(
    sensorId: string,
    eventTimestampMs: number
  ): Promise<void>;
  createMetrics(consumed: number): Promise<HeartbeatTrackerMetrics>;
}

interface HeartbeatTrackerDeps {
  createConsumer?: () => Consumer;
  createHealthServer?: (
    getMetrics: () => Promise<HeartbeatTrackerMetrics>,
    port: number
  ) => Server;
  redis?: RedisLike;
  now?: () => number;
}

interface RedisLike {
  hgetall(key: string): Promise<Record<string, string>>;
  hset(key: string, map: Record<string, string>): Promise<number>;
  sadd(key: string, member: string): Promise<number>;
  smembers(key: string): Promise<string[]>;
  srem(key: string, member: string): Promise<number>;
  del(key: string): Promise<number>;
  eval(
    script: string,
    numkeys: number,
    ...args: Array<string | number>
  ): Promise<unknown>;
  quit?(): Promise<'OK'>;
  disconnect?(): void;
}

/**
 * Atomically applies a heartbeat update for a sensor.
 *
 * The stored `lastSeen` is only advanced when the incoming event timestamp is
 * strictly newer than what is currently stored, making replayed/out-of-order
 * Kafka records unable to move heartbeat state backwards or re-advance it.
 * `onlineSince` resets to the event time whenever the gap since the previous
 * (stored) `lastSeen` exceeds `onlineWindowMs`.
 *
 * KEYS[1] = sensor hash key
 * KEYS[2] = sensors set key
 * ARGV[1] = sensor id
 * ARGV[2] = event timestamp (ms)
 * ARGV[3] = online window (ms)
 *
 * Returns [updated, isNew, firstSeen, lastSeen, onlineSince].
 */
const HEARTBEAT_CAS_SCRIPT = `
local key = KEYS[1]
local sensorsKey = KEYS[2]
local sensorId = ARGV[1]
local eventTime = tonumber(ARGV[2])
local onlineWindowMs = tonumber(ARGV[3])

redis.call('SADD', sensorsKey, sensorId)

local lastSeenRaw = redis.call('HGET', key, 'lastSeen')
local firstSeenRaw = redis.call('HGET', key, 'firstSeen')
local onlineSinceRaw = redis.call('HGET', key, 'onlineSince')

local lastSeen = lastSeenRaw and tonumber(lastSeenRaw) or nil
local firstSeen = firstSeenRaw and tonumber(firstSeenRaw) or nil
local onlineSince = onlineSinceRaw and tonumber(onlineSinceRaw) or nil

local isNew = 0
if firstSeen == nil then
  isNew = 1
end

if lastSeen == nil or eventTime > lastSeen then
  local newFirstSeen = firstSeen or eventTime
  local newOnlineSince = onlineSince or eventTime
  if lastSeen ~= nil and (eventTime - lastSeen) > onlineWindowMs then
    newOnlineSince = eventTime
  end
  redis.call('HSET', key, 'firstSeen', newFirstSeen, 'lastSeen', eventTime, 'onlineSince', newOnlineSince)
  return {1, isNew, newFirstSeen, eventTime, newOnlineSince}
end

return {0, isNew, firstSeen or eventTime, lastSeen, onlineSince or eventTime}
`;

const logger = pino({
  name: 'heartbeat-tracker',
  level:
    process.env.HEARTBEAT_TRACKER_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info',
});

function logInfo(message: string, context?: Record<string, unknown>): void {
  logger.info(context ?? {}, message);
}

function logDebug(message: string, context?: Record<string, unknown>): void {
  logger.debug(context ?? {}, message);
}

function logWarn(message: string, context?: Record<string, unknown>): void {
  logger.warn(context ?? {}, message);
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

type RedisConstructor = new (url: string) => RedisLike;

function createHeartbeatTrackerKeyspace(prefix: string) {
  return {
    sensors: `${prefix}:sensors`,
    sensor: (sensorId: string) => `${prefix}:sensor:${sensorId}`,
  };
}

export function createHeartbeatTrackerState(
  redis: RedisLike,
  redisKeyPrefix: string,
  onlineWindowMs: number,
  retentionWindowMs: number,
  now: () => number = Date.now
): HeartbeatTrackerState {
  const keyspace = createHeartbeatTrackerKeyspace(redisKeyPrefix);

  return {
    async recordAuthorizedSensor(
      sensorId: string,
      eventTimestampMs: number
    ): Promise<void> {
      const key = keyspace.sensor(sensorId);
      const result = (await redis.eval(
        HEARTBEAT_CAS_SCRIPT,
        2,
        key,
        keyspace.sensors,
        sensorId,
        eventTimestampMs,
        onlineWindowMs
      )) as [number, number, number, number, number];
      const [updatedFlag, isNewFlag, , lastSeen] = result;
      const updated = updatedFlag === 1;
      const isNewSensor = isNewFlag === 1;

      if (!updated) {
        logDebug('stale or replayed heartbeat ignored', {
          sensor_id: sensorId,
          event_timestamp_ms: eventTimestampMs,
          stored_last_seen_ms: lastSeen,
        });
        return;
      }

      if (isNewSensor) {
        logInfo('new sensor tracked', { sensor_id: sensorId });
      } else {
        logDebug('sensor heartbeat recorded', {
          sensor_id: sensorId,
          event_timestamp_ms: eventTimestampMs,
        });
      }
    },
    async createMetrics(consumed: number): Promise<HeartbeatTrackerMetrics> {
      const currentTime = now();
      const uptimeMap: Record<string, number> = {};
      const details: HeartbeatTrackerMetrics['sensors_uptime'] = [];
      const onlineUptimes: number[] = [];
      const sensorIds = await redis.smembers(keyspace.sensors);

      logDebug('computing metrics', {
        total_sensor_ids: sensorIds.length,
        retention_window_ms: retentionWindowMs,
        online_window_ms: onlineWindowMs,
      });

      const heartbeats = await Promise.all(
        sensorIds.map((sensorId) => redis.hgetall(keyspace.sensor(sensorId)))
      );

      const staleSensorIds: string[] = [];

      for (let i = 0; i < sensorIds.length; i++) {
        const sensorId = sensorIds[i];
        const heartbeat = heartbeats[i];
        if (sensorId === undefined || heartbeat === undefined) {
          continue;
        }
        const firstSeen = Number.parseInt(heartbeat.firstSeen ?? '', 10);
        const lastSeen = Number.parseInt(heartbeat.lastSeen ?? '', 10);
        const onlineSince = Number.parseInt(heartbeat.onlineSince ?? '', 10);
        if (
          !Number.isFinite(firstSeen) ||
          !Number.isFinite(lastSeen) ||
          !Number.isFinite(onlineSince)
        ) {
          logDebug('skipping sensor with invalid heartbeat data', {
            sensor_id: sensorId,
          });
          continue;
        }
        const secondsSinceLastSeen = Math.max(
          0,
          (currentTime - lastSeen) / 1000
        );
        const online = currentTime - lastSeen <= onlineWindowMs;
        const uptimeSeconds = online
          ? Math.max(0, (currentTime - onlineSince) / 1000)
          : 0;

        // Mark sensor as stale if not seen within retention window
        if (currentTime - lastSeen > retentionWindowMs) {
          staleSensorIds.push(sensorId);
          logDebug('sensor marked for pruning', {
            sensor_id: sensorId,
            seconds_since_last_seen: secondsSinceLastSeen,
            retention_window_seconds: retentionWindowMs / 1000,
          });
          continue;
        }

        if (online) {
          uptimeMap[sensorId] = uptimeSeconds;
          onlineUptimes.push(uptimeSeconds);
        }

        details.push({
          sensor_id: sensorId,
          online,
          first_seen: new Date(firstSeen).toISOString(),
          last_seen: new Date(lastSeen).toISOString(),
          uptime_seconds: uptimeSeconds,
          seconds_since_last_seen: secondsSinceLastSeen,
        });
      }

      // Prune stale sensors from Redis
      if (staleSensorIds.length > 0) {
        logInfo('pruning stale sensors from Redis', {
          stale_count: staleSensorIds.length,
          sensor_ids: staleSensorIds,
        });
        await Promise.all(
          staleSensorIds.map(async (sensorId) => {
            await redis.srem(keyspace.sensors, sensorId);
            await redis.del(keyspace.sensor(sensorId));
          })
        );
      }

      const sensorsOnline = onlineUptimes.length;
      const maxUptimeSeconds =
        sensorsOnline > 0 ? Math.max(...onlineUptimes) : 0;
      const avgUptimeSeconds =
        sensorsOnline > 0
          ? onlineUptimes.reduce((total, current) => total + current, 0) /
            sensorsOnline
          : 0;

      logDebug('metrics computed', {
        sensors_online: sensorsOnline,
        sensors_tracked: sensorIds.length - staleSensorIds.length,
        sensors_pruned: staleSensorIds.length,
        max_uptime_seconds: maxUptimeSeconds,
        avg_uptime_seconds: avgUptimeSeconds,
      });

      return {
        sensors_online: sensorsOnline,
        sensors_total_tracked: sensorIds.length - staleSensorIds.length,
        online_window_ms: onlineWindowMs,
        consumed,
        sensor_uptime_seconds: uptimeMap,
        sensors_uptime: details,
        max_uptime_seconds: maxUptimeSeconds,
        avg_uptime_seconds: avgUptimeSeconds,
      };
    },
  };
}

export function handleTelemetryMessage(
  raw: Buffer,
  tracker: HeartbeatTrackerState,
  consumed: { value: number }
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

    const sensorIdHex = Buffer.from(payload.sensorId).toString('hex');
    const eventTimestampMs = Date.parse(envelope.occurredAt);

    logDebug('authorized envelope received', {
      eventId: envelope.eventId,
      eventType: envelope.eventType,
      trace_id: envelope.traceId,
      sensor_id: formatSensorId(payload.sensorId),
      occurred_at: envelope.occurredAt,
    });

    if (!Number.isFinite(eventTimestampMs)) {
      logWarn('envelope has invalid occurred_at timestamp; skipping', {
        eventId: envelope.eventId,
        occurred_at: envelope.occurredAt,
        sensor_id: formatSensorId(payload.sensorId),
      });
      return Promise.resolve();
    }

    consumed.value += 1;
    return tracker.recordAuthorizedSensor(sensorIdHex, eventTimestampMs);
  } catch (error) {
    logWarn('envelope parse error', {
      reason: error instanceof Error ? error.message : String(error),
    });
    return Promise.resolve();
  }
}

export function createHeartbeatTrackerService(
  config: HeartbeatTrackerConfig = loadHeartbeatTrackerConfig(),
  deps: HeartbeatTrackerDeps = {}
): HeartbeatTrackerService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'heartbeat-tracker',
      bootstrapBrokers: config.kafkaBrokers,
    });
  const RedisClient = Redis as unknown as RedisConstructor;
  const redis = deps.redis ?? new RedisClient(config.redisUrl);
  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;
  const tracker = createHeartbeatTrackerState(
    redis,
    config.redisKeyPrefix,
    config.onlineWindowMs,
    config.retentionWindowMs,
    deps.now ?? Date.now
  );

  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let consumerStream: AsyncIterable<{
    topic: string;
    partition: number;
    offset: bigint;
    value: Buffer | null;
  }> | null = null;
  const consumed = { value: 0 };

  const getMetrics = async (): Promise<HeartbeatTrackerMetrics> =>
    tracker.createMetrics(consumed.value);

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
        healthPort: config.healthPort,
        onlineWindowMs: config.onlineWindowMs,
        retentionWindowMs: config.retentionWindowMs,
        redisUrl: config.redisUrl,
        redisKeyPrefix: config.redisKeyPrefix,
        source: config.source,
      });

      try {
        consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: true,
        });
        healthServer = createHealthServer(getMetrics, config.healthPort);

        runPromise = (async () => {
          for await (const message of consumerStream!) {
            if (!message.value) {
              logWarn('received null message value; skipping');
              continue;
            }
            await handleTelemetryMessage(message.value, tracker, consumed);
            logDebug('kafka message processed', {
              topic: message.topic,
              partition: message.partition,
              offset: message.offset,
              consumed: consumed.value,
            });
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
        if (typeof redis.quit === 'function') {
          await redis.quit();
        } else if (typeof redis.disconnect === 'function') {
          redis.disconnect();
        }
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
      await consumer.close();
      if (typeof redis.quit === 'function') {
        await redis.quit();
      } else if (typeof redis.disconnect === 'function') {
        redis.disconnect();
      }
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
    getMetrics(): Promise<Readonly<HeartbeatTrackerMetrics>> {
      return getMetrics();
    },
  };
}

export function startHealthAndMetricsServer(
  getMetrics: () => Promise<HeartbeatTrackerMetrics>,
  port: number
): Server {
  const server = createServer(async (request, response) => {
    if (request.url === '/health') {
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      try {
        logDebug('metrics endpoint requested');
        const metrics = await getMetrics();
        logInfo('metrics served', {
          sensors_online: metrics.sensors_online,
          sensors_total_tracked: metrics.sensors_total_tracked,
          consumed: metrics.consumed,
        });
        response.statusCode = 200;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(JSON.stringify(metrics));
      } catch (error) {
        response.statusCode = 500;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(JSON.stringify({ error: 'failed to collect metrics' }));
        logError('failed to collect metrics', error);
      }
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
  logInfo('HTTP server listening', { port, host: '0.0.0.0' });
  return server;
}

export async function startHeartbeatTracker(): Promise<HeartbeatTrackerService> {
  const service = createHeartbeatTrackerService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startHeartbeatTracker().catch((error: unknown) => {
    logError('failed to start (direct run)', error);
    process.exitCode = 1;
  });
}
