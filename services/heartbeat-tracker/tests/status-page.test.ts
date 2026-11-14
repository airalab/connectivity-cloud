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
import {
  createHeartbeatTrackerState,
  MAX_SENSOR_PAGE_LIMIT,
} from '../src/index.js';
import { FakeRedis } from './fake-redis.js';

const NOW = Date.parse('2026-11-11T09:00:00Z');
const ONLINE_WINDOW_MS = 30_000;
const RETENTION_MS = 300_000;

async function seed(
  sensors: Array<{ id: string; lastSeenAgoMs: number }>
): Promise<{ redis: FakeRedis; tracker: ReturnType<typeof create> }> {
  const redis = new FakeRedis();
  const tracker = create(redis);
  for (const sensor of sensors) {
    await tracker.recordAuthorizedSensor(sensor.id, NOW - sensor.lastSeenAgoMs);
  }
  return { redis, tracker };
}

function create(redis: FakeRedis) {
  return createHeartbeatTrackerState(
    redis,
    'heartbeat-tracker:test',
    ONLINE_WINDOW_MS,
    RETENTION_MS,
    () => NOW
  );
}

describe('heartbeat sensor pages', () => {
  it('sorts by last-seen time (online first) and paginates with a cursor', async () => {
    const { tracker } = await seed([
      { id: 'offline', lastSeenAgoMs: 120_000 },
      { id: 'online-old', lastSeenAgoMs: 10_000 },
      { id: 'online-new', lastSeenAgoMs: 1_000 },
    ]);
    const first = await tracker.getSensorPage({ limit: 2, cursor: '0' });
    expect(first.sensors.map((sensor) => sensor.sensor_id)).toEqual([
      'online-new',
      'online-old',
    ]);
    expect(first.total).toBe(3);
    expect(first.nextCursor).toBe('2');
    expect(first.summary).toMatchObject({
      sensors_online: 2,
      sensors_total_tracked: 3,
      online_window_ms: ONLINE_WINDOW_MS,
    });

    const next = await tracker.getSensorPage({
      limit: 2,
      cursor: first.nextCursor ?? undefined,
    });
    expect(next.sensors.map((sensor) => sensor.sensor_id)).toEqual(['offline']);
    expect(next.sensors[0]?.online).toBe(false);
    expect(next.nextCursor).toBeNull();
  });

  it('applies online/offline filters and caps requested page sizes', async () => {
    const { tracker } = await seed([
      { id: 'offline', lastSeenAgoMs: 120_000 },
      { id: 'online', lastSeenAgoMs: 1_000 },
    ]);
    const offline = await tracker.getSensorPage({
      limit: 500,
      filter: 'offline',
    });
    expect(offline.limit).toBe(MAX_SENSOR_PAGE_LIMIT);
    expect(offline.total).toBe(1);
    expect(offline.sensors.map((sensor) => sensor.sensor_id)).toEqual([
      'offline',
    ]);
    const online = await tracker.getSensorPage({ filter: 'online' });
    expect(online.total).toBe(1);
    expect(online.sensors.map((sensor) => sensor.sensor_id)).toEqual([
      'online',
    ]);
  });

  it('bounds Redis reads by the page size instead of the fleet size', async () => {
    const fleet = Array.from({ length: 2000 }, (_, index) => ({
      id: `sensor-${String(index).padStart(4, '0')}`,
      lastSeenAgoMs: 100 + index,
    }));
    const { redis, tracker } = await seed(fleet);
    for (const key of Object.keys(redis.commands)) redis.commands[key] = 0;
    redis.membersRead = 0;

    const page = await tracker.getSensorPage({ limit: 50 });

    expect(page.sensors).toHaveLength(50);
    expect(page.total).toBe(2000);
    expect(page.summary.sensors_online).toBe(2000);
    expect(page.uptimeSummarySampled).toBe(true);
    expect(redis.commands.smembers ?? 0).toBe(0);
    // 50 page sensors + the bounded uptime sample, never the 2000-sensor fleet.
    expect(redis.commands.hgetall).toBeLessThanOrEqual(50 + 500);
    expect(redis.membersRead).toBeLessThan(2000);
  });

  it('returns a deep page without materializing earlier pages', async () => {
    const fleet = Array.from({ length: 300 }, (_, index) => ({
      id: `sensor-${String(index).padStart(3, '0')}`,
      lastSeenAgoMs: 100 + index * 10,
    }));
    const { redis, tracker } = await seed(fleet);
    for (const key of Object.keys(redis.commands)) redis.commands[key] = 0;

    const page = await tracker.getSensorPage({ limit: 10, cursor: '250' });
    expect(page.sensors.map((sensor) => sensor.sensor_id)).toEqual(
      fleet.slice(250, 260).map((sensor) => sensor.id)
    );
    expect(page.nextCursor).toBe('260');
    expect(redis.commands.hgetall).toBeLessThanOrEqual(10 + 500);
  });

  it('prunes sensors beyond the retention window from the index', async () => {
    const { redis, tracker } = await seed([
      { id: 'expired', lastSeenAgoMs: RETENTION_MS + 1_000 },
      { id: 'recent', lastSeenAgoMs: 1_000 },
    ]);
    const page = await tracker.getSensorPage();
    expect(page.total).toBe(1);
    expect(page.summary.sensors_total_tracked).toBe(1);
    expect(
      await redis.hgetall('heartbeat-tracker:test:sensor:expired')
    ).toEqual({});
    expect(
      await redis.zcount(
        'heartbeat-tracker:test:sensors:last-seen',
        '-inf',
        '+inf'
      )
    ).toBe(1);
  });

  it('does not prune a sensor whose heartbeat arrives after it was selected for pruning', async () => {
    const redis = new FakeRedis();
    const tracker = create(redis);
    await tracker.recordAuthorizedSensor('revived', NOW - RETENTION_MS - 1_000);
    await tracker.recordAuthorizedSensor('expired', NOW - RETENTION_MS - 2_000);

    const selectStale = redis.zrangebyscore.bind(redis);
    redis.zrangebyscore = async (...args) => {
      const staleIds = await selectStale(...args);
      await tracker.recordAuthorizedSensor('revived', NOW - 500);
      return staleIds;
    };

    const page = await tracker.getSensorPage();
    expect(page.sensors.map((sensor) => sensor.sensor_id)).toEqual(['revived']);
    expect(page.total).toBe(1);
    expect(
      await redis.hgetall('heartbeat-tracker:test:sensor:revived')
    ).toMatchObject({ lastSeen: String(NOW - 500) });
    expect(
      await redis.hgetall('heartbeat-tracker:test:sensor:expired')
    ).toEqual({});
    expect(await redis.smembers('heartbeat-tracker:test:sensors')).toEqual([
      'revived',
    ]);
  });

  it('does not prune a revived sensor while computing metrics', async () => {
    const redis = new FakeRedis();
    const tracker = create(redis);
    await tracker.recordAuthorizedSensor('revived', NOW - RETENTION_MS - 1_000);

    const readHash = redis.hgetall.bind(redis);
    let revived = false;
    redis.hgetall = async (key) => {
      const hash = await readHash(key);
      if (!revived) {
        revived = true;
        await tracker.recordAuthorizedSensor('revived', NOW - 500);
      }
      return hash;
    };

    await tracker.createMetrics(0);
    expect(
      await redis.hgetall('heartbeat-tracker:test:sensor:revived')
    ).toMatchObject({ lastSeen: String(NOW - 500) });
    expect(await redis.smembers('heartbeat-tracker:test:sensors')).toEqual([
      'revived',
    ]);
  });

  it('keeps an empty healthy dataset distinct from unavailable data', async () => {
    const { tracker } = await seed([]);
    const empty = await tracker.getSensorPage();
    expect(empty.total).toBe(0);
    expect(empty.sensors).toEqual([]);
    expect(empty.summary.sensors_online).toBe(0);
    expect(empty.summary.sensors_total_tracked).toBe(0);
    expect(empty.nextCursor).toBeNull();
    expect(empty.uptimeSummarySampled).toBe(false);
    expect(typeof empty.observedAt).toBe('string');
  });
});
