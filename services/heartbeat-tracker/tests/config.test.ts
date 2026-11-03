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
import { describe, expect, it } from 'vitest';
import { loadHeartbeatTrackerConfig } from '../src/config.js';

describe('heartbeat tracker config', () => {
  it('uses 30000ms as default online window', () => {
    const config = loadHeartbeatTrackerConfig({});

    expect(config.onlineWindowMs).toBe(30000);
    expect(config.retentionWindowMs).toBe(300000); // 10x online window
    expect(config.redisUrl).toBe('redis://localhost:6379');
    expect(config.redisKeyPrefix).toBe('heartbeat-tracker:v1');
  });

  it('respects HEARTBEAT_TRACKER_ONLINE_WINDOW_MS override', () => {
    const config = loadHeartbeatTrackerConfig({
      HEARTBEAT_TRACKER_ONLINE_WINDOW_MS: '45000',
    });

    expect(config.onlineWindowMs).toBe(45000);
    expect(config.retentionWindowMs).toBe(450000); // 10x online window
  });

  it('respects HEARTBEAT_TRACKER_RETENTION_WINDOW_MS override', () => {
    const config = loadHeartbeatTrackerConfig({
      HEARTBEAT_TRACKER_ONLINE_WINDOW_MS: '30000',
      HEARTBEAT_TRACKER_RETENTION_WINDOW_MS: '600000',
    });

    expect(config.onlineWindowMs).toBe(30000);
    expect(config.retentionWindowMs).toBe(600000);
  });

  it('respects HEARTBEAT_TRACKER_REDIS_PREFIX override', () => {
    const config = loadHeartbeatTrackerConfig({
      REDIS_URL: 'redis://redis.internal:6379',
      HEARTBEAT_TRACKER_REDIS_PREFIX: 'heartbeat-tracker:prod',
    });

    expect(config.redisUrl).toBe('redis://redis.internal:6379');
    expect(config.redisKeyPrefix).toBe('heartbeat-tracker:prod');
  });

  it('exposes protected Redis and Kafka settings', () => {
    const config = loadHeartbeatTrackerConfig({
      REDIS_URL: 'rediss://10.0.0.5:6378',
      REDIS_AUTH_PASSWORD: 'pw',
      REDIS_TLS_CA_PATH: '/var/run/secrets/redis/ca.pem',
      KAFKA_SECURITY_PROTOCOL: 'SASL_SSL',
    });

    expect(config.redis).toMatchObject({
      url: 'rediss://10.0.0.5:6378',
      password: 'pw',
      tlsCaPath: '/var/run/secrets/redis/ca.pem',
    });
    expect(config.kafkaSecurity?.protocol).toBe('SASL_SSL');
    // Heartbeat keys keep their own namespace, separate from nonce:v1.
    expect(config.redisKeyPrefix).toBe('heartbeat-tracker:v1');
  });
});
