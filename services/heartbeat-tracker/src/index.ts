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
  installShutdownHandler,
} from '@scp/core';
import { fromBinary } from '@bufbuild/protobuf';
import { Consumer } from '@platformatic/kafka';
import {
  closeRedisClient,
  createKafkaClientOptions,
  createRedisClient,
  loadKafkaSecurityConfig,
  loadRedisConfig,
  redactRedisUrl,
} from '@scp/infra';
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

export interface HeartbeatSensorPageOptions {
  limit?: number;
  cursor?: string;
  filter?: string;
}

export interface HeartbeatSensorPage {
  observedAt: string;
  limit: number;
  total: number;
  nextCursor: string | null;
  /** True when uptime statistics were computed from a bounded sample of online sensors. */
  uptimeSummarySampled: boolean;
  summary: Pick<
    HeartbeatTrackerMetrics,
    | 'sensors_online'
    | 'sensors_total_tracked'
    | 'avg_uptime_seconds'
    | 'max_uptime_seconds'
    | 'online_window_ms'
  >;
  sensors: HeartbeatTrackerMetrics['sensors_uptime'];
}

/** Hard cap on sensors returned (and read from Redis) per page. */
export const MAX_SENSOR_PAGE_LIMIT = 100;
const DEFAULT_SENSOR_PAGE_LIMIT = 50;
/** Maximum online sensors read from Redis to compute uptime statistics. */
export const UPTIME_SUMMARY_SAMPLE_LIMIT = 500;
/** Maximum stale sensors pruned per page request. */
const PRUNE_BATCH_SIZE = 100;

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
  /**
   * Reads one page of sensors using the last-seen index. Redis work is bounded
   * by the page size (plus a fixed uptime sample), independent of fleet size.
   */
  getSensorPage(
    options?: HeartbeatSensorPageOptions
  ): Promise<HeartbeatSensorPage>;
}

interface HeartbeatTrackerDeps {
  createConsumer?: () => Consumer;
  createHealthServer?: (
    getMetrics: () => Promise<HeartbeatTrackerMetrics>,
    port: number,
    getSensorPage?: (
      options: HeartbeatSensorPageOptions
    ) => Promise<HeartbeatSensorPage>
  ) => Server;
  redis?: RedisLike;
  now?: () => number;
}

interface RedisLike {
  hgetall(key: string): Promise<Record<string, string>>;
  hset(key: string, map: Record<string, string>): Promise<number>;
  sadd(key: string, member: string): Promise<number>;
  smembers(key: string): Promise<string[]>;
  zcount(
    key: string,
    min: number | string,
    max: number | string
  ): Promise<number>;
  zrevrangebyscore(
    key: string,
    max: number | string,
    min: number | string,
    limitToken: 'LIMIT',
    offset: number,
    count: number
  ): Promise<string[]>;
  zrangebyscore(
    key: string,
    min: number | string,
    max: number | string,
    limitToken: 'LIMIT',
    offset: number,
    count: number
  ): Promise<string[]>;
  zrem(key: string, ...members: string[]): Promise<number>;
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
 * KEYS[3] = last-seen sorted set (score = lastSeen ms), used for paging
 * ARGV[1] = sensor id
 * ARGV[2] = event timestamp (ms)
 * ARGV[3] = online window (ms)
 *
 * Returns [updated, isNew, firstSeen, lastSeen, onlineSince].
 */
const HEARTBEAT_CAS_SCRIPT = `
local key = KEYS[1]
local sensorsKey = KEYS[2]
local lastSeenIndexKey = KEYS[3]
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
  redis.call('ZADD', lastSeenIndexKey, eventTime, sensorId)
  return {1, isNew, newFirstSeen, eventTime, newOnlineSince}
end

return {0, isNew, firstSeen or eventTime, lastSeen, onlineSince or eventTime}
`;

/**
 * Atomically prunes one sensor, but only if it is still stale.
 *
 * The last-seen timestamp is re-read inside the script, so a heartbeat that
 * landed after the sensor was selected for pruning keeps its state. The sensor
 * hash is authoritative; the index score is used when the hash is missing.
 *
 * KEYS[1] = sensor hash key
 * KEYS[2] = sensors set key
 * KEYS[3] = last-seen sorted set
 * ARGV[1] = sensor id
 * ARGV[2] = cutoff (ms); the sensor is pruned only when lastSeen < cutoff
 *
 * Returns 1 when the sensor was pruned, 0 when it was kept.
 */
export const PRUNE_STALE_SENSOR_SCRIPT = `
local key = KEYS[1]
local sensorsKey = KEYS[2]
local lastSeenIndexKey = KEYS[3]
local sensorId = ARGV[1]
local cutoff = tonumber(ARGV[2])

local lastSeen = tonumber(redis.call('HGET', key, 'lastSeen'))
if lastSeen == nil then
  lastSeen = tonumber(redis.call('ZSCORE', lastSeenIndexKey, sensorId))
end

if lastSeen ~= nil and lastSeen >= cutoff then
  return 0
end

redis.call('SREM', sensorsKey, sensorId)
redis.call('DEL', key)
redis.call('ZREM', lastSeenIndexKey, sensorId)
return 1
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

function createHeartbeatTrackerKeyspace(prefix: string) {
  return {
    sensors: `${prefix}:sensors`,
    lastSeenIndex: `${prefix}:sensors:last-seen`,
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

  const pruneStaleSensors = async (
    sensorIds: string[],
    cutoff: number
  ): Promise<void> => {
    await Promise.all(
      sensorIds.map((sensorId) =>
        redis.eval(
          PRUNE_STALE_SENSOR_SCRIPT,
          3,
          keyspace.sensor(sensorId),
          keyspace.sensors,
          keyspace.lastSeenIndex,
          sensorId,
          cutoff
        )
      )
    );
  };

  return {
    async recordAuthorizedSensor(
      sensorId: string,
      eventTimestampMs: number
    ): Promise<void> {
      const key = keyspace.sensor(sensorId);
      const result = (await redis.eval(
        HEARTBEAT_CAS_SCRIPT,
        3,
        key,
        keyspace.sensors,
        keyspace.lastSeenIndex,
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
    async getSensorPage(
      options: HeartbeatSensorPageOptions = {}
    ): Promise<HeartbeatSensorPage> {
      const currentTime = now();
      const limit = Math.min(
        Number.isSafeInteger(options.limit) && (options.limit ?? 0) > 0
          ? options.limit!
          : DEFAULT_SENSOR_PAGE_LIMIT,
        MAX_SENSOR_PAGE_LIMIT
      );
      const parsedCursor = Number.parseInt(options.cursor ?? '0', 10);
      const offset =
        Number.isSafeInteger(parsedCursor) && parsedCursor >= 0
          ? parsedCursor
          : 0;
      const filter =
        options.filter === 'online' || options.filter === 'offline'
          ? options.filter
          : 'all';
      const onlineCutoff = currentTime - onlineWindowMs;
      const retentionCutoff = Math.min(
        currentTime - retentionWindowMs,
        onlineCutoff
      );

      const staleIds = await redis.zrangebyscore(
        keyspace.lastSeenIndex,
        '-inf',
        `(${retentionCutoff}`,
        'LIMIT',
        0,
        PRUNE_BATCH_SIZE
      );
      if (staleIds.length > 0) {
        logInfo('pruning stale sensors from Redis', {
          stale_count: staleIds.length,
        });
        await pruneStaleSensors(staleIds, retentionCutoff);
      }

      const [onlineCount, visibleCount] = await Promise.all([
        redis.zcount(keyspace.lastSeenIndex, onlineCutoff, '+inf'),
        redis.zcount(keyspace.lastSeenIndex, retentionCutoff, '+inf'),
      ]);
      const total =
        filter === 'online'
          ? onlineCount
          : filter === 'offline'
            ? visibleCount - onlineCount
            : visibleCount;
      const max = filter === 'offline' ? `(${onlineCutoff}` : '+inf';
      const min = filter === 'online' ? onlineCutoff : retentionCutoff;

      const readSensors = async (
        ids: string[],
        onlyOnline: boolean
      ): Promise<HeartbeatTrackerMetrics['sensors_uptime']> => {
        const heartbeats = await Promise.all(
          ids.map((sensorId) => redis.hgetall(keyspace.sensor(sensorId)))
        );
        const details: HeartbeatTrackerMetrics['sensors_uptime'] = [];
        ids.forEach((sensorId, index) => {
          const heartbeat = heartbeats[index];
          const firstSeen = Number.parseInt(heartbeat?.firstSeen ?? '', 10);
          const lastSeen = Number.parseInt(heartbeat?.lastSeen ?? '', 10);
          const onlineSince = Number.parseInt(heartbeat?.onlineSince ?? '', 10);
          if (
            !Number.isFinite(firstSeen) ||
            !Number.isFinite(lastSeen) ||
            !Number.isFinite(onlineSince)
          ) {
            return;
          }
          const online = currentTime - lastSeen <= onlineWindowMs;
          if (onlyOnline && !online) return;
          details.push({
            sensor_id: sensorId,
            online,
            first_seen: new Date(firstSeen).toISOString(),
            last_seen: new Date(lastSeen).toISOString(),
            uptime_seconds: online
              ? Math.max(0, (currentTime - onlineSince) / 1000)
              : 0,
            seconds_since_last_seen: Math.max(
              0,
              (currentTime - lastSeen) / 1000
            ),
          });
        });
        return details;
      };

      const [pageIds, sampleIds] = await Promise.all([
        total > offset
          ? redis.zrevrangebyscore(
              keyspace.lastSeenIndex,
              max,
              min,
              'LIMIT',
              offset,
              limit
            )
          : Promise.resolve([] as string[]),
        onlineCount > 0
          ? redis.zrevrangebyscore(
              keyspace.lastSeenIndex,
              '+inf',
              onlineCutoff,
              'LIMIT',
              0,
              UPTIME_SUMMARY_SAMPLE_LIMIT
            )
          : Promise.resolve([] as string[]),
      ]);
      const [sensors, sample] = await Promise.all([
        readSensors(pageIds, false),
        readSensors(sampleIds, true),
      ]);
      const uptimes = sample.map((sensor) => sensor.uptime_seconds);
      const nextOffset = offset + pageIds.length;

      return {
        observedAt: new Date(currentTime).toISOString(),
        limit,
        total,
        nextCursor:
          pageIds.length > 0 && nextOffset < total ? String(nextOffset) : null,
        uptimeSummarySampled: onlineCount > sampleIds.length,
        summary: {
          sensors_online: onlineCount,
          sensors_total_tracked: visibleCount,
          avg_uptime_seconds:
            uptimes.length > 0
              ? uptimes.reduce((sum, value) => sum + value, 0) / uptimes.length
              : 0,
          max_uptime_seconds: uptimes.length > 0 ? Math.max(...uptimes) : 0,
          online_window_ms: onlineWindowMs,
        },
        sensors,
      };
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
        await pruneStaleSensors(
          staleSensorIds,
          currentTime - retentionWindowMs
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
      ...createKafkaClientOptions({
        brokers: config.kafkaBrokers,
        security: config.kafkaSecurity ?? loadKafkaSecurityConfig(),
      }),
    });
  // The protected Redis connection is established in start() so TLS/AUTH
  // failures surface at startup instead of constructing the service.
  let redisClient: RedisLike | undefined = deps.redis;
  const requireRedis = (): RedisLike => {
    if (!redisClient) {
      throw new Error('Redis client is not connected');
    }
    return redisClient;
  };
  const redis: RedisLike = {
    hgetall: (key) => requireRedis().hgetall(key),
    hset: (key, map) => requireRedis().hset(key, map),
    sadd: (key, member) => requireRedis().sadd(key, member),
    smembers: (key) => requireRedis().smembers(key),
    zcount: (key, min, max) => requireRedis().zcount(key, min, max),
    zrevrangebyscore: (key, max, min, limitToken, offset, count) =>
      requireRedis().zrevrangebyscore(key, max, min, limitToken, offset, count),
    zrangebyscore: (key, min, max, limitToken, offset, count) =>
      requireRedis().zrangebyscore(key, min, max, limitToken, offset, count),
    zrem: (key, ...members) => requireRedis().zrem(key, ...members),
    srem: (key, member) => requireRedis().srem(key, member),
    del: (key) => requireRedis().del(key),
    eval: (script, numkeys, ...args) =>
      requireRedis().eval(script, numkeys, ...args),
  };
  const closeRedis = async (): Promise<void> => {
    const client = redisClient;
    if (!client) return;
    if (!deps.redis) {
      await closeRedisClient(client as Parameters<typeof closeRedisClient>[0]);
    } else if (typeof client.quit === 'function') {
      await client.quit();
    } else if (typeof client.disconnect === 'function') {
      client.disconnect();
    }
    redisClient = undefined;
  };
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
        redisUrl: redactRedisUrl(config.redisUrl),
        redisKeyPrefix: config.redisKeyPrefix,
        source: config.source,
      });

      try {
        redisClient ??= (await createRedisClient(
          config.redis ?? loadRedisConfig()
        )) as unknown as RedisLike;
        consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.AUTHORIZED],
          autocommit: true,
        });
        healthServer = createHealthServer(
          getMetrics,
          config.healthPort,
          (options) => tracker.getSensorPage(options)
        );

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
        await closeRedis().catch(() => undefined);
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
      // Stop pulling new messages, then wait for any in-flight
      // `handleTelemetryMessage` call to finish before disconnecting Redis.
      // Otherwise an autocommitted message could fail to record its
      // heartbeat if Redis is quit while the write is still pending.
      await consumer.close();
      await runPromise?.catch(() => undefined);
      runPromise = null;
      await closeRedis();
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
    getMetrics(): Promise<Readonly<HeartbeatTrackerMetrics>> {
      return getMetrics();
    },
  };
}

export function startHealthAndMetricsServer(
  getMetrics: () => Promise<HeartbeatTrackerMetrics>,
  port: number,
  getSensorPage?: (
    options: HeartbeatSensorPageOptions
  ) => Promise<HeartbeatSensorPage>
): Server {
  const server = createServer(async (request, response) => {
    // The server starts listening only after the Kafka consumer is running,
    // so a successful response also proves the service is ready.
    if (request.url === '/health' || request.url === '/ready') {
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      // Allow the endpoint service's status page to poll this endpoint
      // directly from the browser.
      response.setHeader('access-control-allow-origin', '*');
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
        // Allow the endpoint service's status page to poll this endpoint
        // directly from the browser.
        response.setHeader('access-control-allow-origin', '*');
        response.end(JSON.stringify(metrics));
      } catch (error) {
        response.statusCode = 500;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(JSON.stringify({ error: 'failed to collect metrics' }));
        logError('failed to collect metrics', error);
      }
      return;
    }

    const url = new URL(request.url ?? '/', 'http://127.0.0.1');
    if (url.pathname === '/status/sensors' && getSensorPage) {
      try {
        const requestedLimit = Number(url.searchParams.get('limit'));
        const cursor = url.searchParams.get('cursor');
        const filter = url.searchParams.get('filter');
        const page = await getSensorPage({
          ...(Number.isSafeInteger(requestedLimit)
            ? { limit: requestedLimit }
            : {}),
          ...(cursor !== null ? { cursor } : {}),
          ...(filter !== null ? { filter } : {}),
        });
        response.statusCode = 200;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(JSON.stringify(page));
      } catch (error) {
        response.statusCode = 500;
        response.setHeader('content-type', 'application/json; charset=utf-8');
        response.end(
          JSON.stringify({ error: 'failed to collect sensor status' })
        );
        logError('failed to collect sensor status', error);
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
  startHeartbeatTracker()
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
