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
import { loadKafkaSecurityConfig, loadRedisConfig } from '@scp/infra';
import type { KafkaSecurityConfig, RedisConfig } from '@scp/infra';
export interface HeartbeatTrackerConfig {
  kafkaBrokers: string[];
  /** Kafka TLS/SASL settings shared by every producer and consumer. */
  kafkaSecurity?: KafkaSecurityConfig;
  consumerGroupId: string;
  source: string;
  healthPort: number;
  onlineWindowMs: number;
  retentionWindowMs: number;
  redisUrl: string;
  /** Full Redis connection settings (TLS CA, AUTH, timeouts). */
  redis?: RedisConfig;
  redisKeyPrefix: string;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

export function loadHeartbeatTrackerConfig(
  env: NodeJS.ProcessEnv = process.env
): HeartbeatTrackerConfig {
  const onlineWindowMs = parsePositiveInt(
    env.HEARTBEAT_TRACKER_ONLINE_WINDOW_MS,
    30000
  );
  const retentionWindowMs = parsePositiveInt(
    env.HEARTBEAT_TRACKER_RETENTION_WINDOW_MS,
    onlineWindowMs * 10
  );
  return {
    kafkaSecurity: loadKafkaSecurityConfig(env),
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.HEARTBEAT_TRACKER_GROUP_ID ?? 'heartbeat-tracker-v1',
    source: env.HEARTBEAT_TRACKER_SOURCE ?? 'heartbeat-tracker',
    healthPort: parsePositiveInt(env.HEARTBEAT_TRACKER_HEALTH_PORT, 3030),
    onlineWindowMs,
    retentionWindowMs,
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    redis: loadRedisConfig(env),
    redisKeyPrefix:
      env.HEARTBEAT_TRACKER_REDIS_PREFIX ?? 'heartbeat-tracker:v1',
  };
}
