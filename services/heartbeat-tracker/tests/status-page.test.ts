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
import { createHeartbeatSensorPage } from '../src/index.js';

const metrics = {
  sensors_online: 2,
  sensors_total_tracked: 3,
  online_window_ms: 30_000,
  consumed: 8,
  sensor_uptime_seconds: {},
  sensors_uptime: [
    {
      sensor_id: 'offline',
      online: false,
      first_seen: '2026-11-11T08:00:00Z',
      last_seen: '2026-11-11T08:50:00Z',
      uptime_seconds: 0,
      seconds_since_last_seen: 600,
    },
    {
      sensor_id: 'online-old',
      online: true,
      first_seen: '2026-11-11T08:00:00Z',
      last_seen: '2026-11-11T08:59:50Z',
      uptime_seconds: 30,
      seconds_since_last_seen: 10,
    },
    {
      sensor_id: 'online-new',
      online: true,
      first_seen: '2026-11-11T08:00:00Z',
      last_seen: '2026-11-11T08:59:59Z',
      uptime_seconds: 45,
      seconds_since_last_seen: 1,
    },
  ],
  max_uptime_seconds: 45,
  avg_uptime_seconds: 37.5,
};

describe('createHeartbeatSensorPage', () => {
  it('sorts online sensors first by last-seen time and paginates server-side', () => {
    const first = createHeartbeatSensorPage(
      metrics,
      { limit: 2, cursor: '0' },
      () => Date.parse('2026-11-11T09:00:00Z')
    );
    expect(first.sensors.map((sensor) => sensor.sensor_id)).toEqual([
      'online-new',
      'online-old',
    ]);
    expect(first.total).toBe(3);
    expect(first.nextCursor).toBe('2');
    expect(first.summary.sensors_online).toBe(2);

    const next = createHeartbeatSensorPage(metrics, {
      limit: 2,
      cursor: first.nextCursor ?? undefined,
    });
    expect(next.sensors.map((sensor) => sensor.sensor_id)).toEqual(['offline']);
    expect(next.nextCursor).toBeNull();
  });

  it('applies online/offline filters and caps requested page sizes', () => {
    const offline = createHeartbeatSensorPage(metrics, {
      limit: 500,
      filter: 'offline',
    });
    expect(offline.limit).toBe(100);
    expect(offline.total).toBe(1);
    expect(offline.sensors.map((sensor) => sensor.sensor_id)).toEqual([
      'offline',
    ]);
  });

  it('keeps an empty healthy dataset distinct from unavailable data', () => {
    const empty = createHeartbeatSensorPage({
      ...metrics,
      sensors_online: 0,
      sensors_total_tracked: 0,
      sensors_uptime: [],
    });
    expect(empty.total).toBe(0);
    expect(empty.sensors).toEqual([]);
    expect(empty.summary.sensors_online).toBe(0);
    expect(empty.nextCursor).toBeNull();
  });
});
