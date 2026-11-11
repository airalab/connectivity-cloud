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
  aggregateServiceStates,
  createStatusCollector,
  type StatusCollectorOptions,
} from '../src/status-collector.js';

function options(
  overrides: Partial<StatusCollectorOptions> = {}
): StatusCollectorOptions {
  return {
    targets: [],
    checkEndpointReadiness: async () => {},
    now: () => Date.parse('2026-11-11T09:00:00Z'),
    fetch: async () => new Response('{}', { status: 200 }),
    getIdToken: async () => 'Bearer test-token',
    ...overrides,
  };
}

describe('status aggregation', () => {
  it('uses the documented precedence and preserves unknown data', () => {
    expect(aggregateServiceStates(['operational', 'unknown'])).toBe('unknown');
    expect(aggregateServiceStates(['degraded', 'unknown', 'outage'])).toBe(
      'outage'
    );
    expect(aggregateServiceStates(['operational', 'degraded'])).toBe(
      'degraded'
    );
    expect(aggregateServiceStates(['operational'])).toBe('operational');
  });

  it('uses the configured Cloud Run URL as the ID-token audience', async () => {
    let audience = '';
    let authorization = '';
    const collector = createStatusCollector(
      options({
        targets: [
          {
            name: 'pubsub-broadcaster',
            url: 'https://pubsub-abc-uc.a.run.app',
          },
        ],
        getIdToken: async (target) => {
          audience = target;
          return 'Bearer mock';
        },
        fetch: async (_input, init) => {
          authorization = new Headers(init?.headers).get('authorization') ?? '';
          return new Response('{"status":"ready"}', { status: 200 });
        },
      })
    );

    const snapshot = await collector.collectStatus();
    expect(audience).toBe('https://pubsub-abc-uc.a.run.app');
    expect(authorization).toBe('Bearer mock');
    expect(snapshot.status).toBe('operational');
    expect(JSON.stringify(snapshot)).not.toContain('a.run.app');
  });

  it('deduplicates in-flight snapshots and caches them until expiry', async () => {
    let calls = 0;
    let currentTime = 1000;
    let release!: () => void;
    const gate = new Promise<void>((resolve) => {
      release = resolve;
    });
    const collector = createStatusCollector(
      options({
        targets: [{ name: 'batcher', url: 'https://batcher.example.run' }],
        cacheTtlMs: 100,
        now: () => currentTime,
        fetch: async () => {
          calls += 1;
          await gate;
          return new Response('{"status":"ready"}', { status: 200 });
        },
      })
    );

    const first = collector.collectStatus();
    const second = collector.collectStatus();
    release();
    await Promise.all([first, second]);
    await collector.collectStatus();
    expect(calls).toBe(1);

    currentTime += 101;
    await collector.collectStatus();
    expect(calls).toBe(2);
  });

  it('treats IAM failures as unknown and applies the transport failure threshold', async () => {
    let calls = 0;
    let currentTime = 1000;
    const forbidden = createStatusCollector(
      options({
        targets: [{ name: 'batcher', url: 'https://batcher.example.run' }],
        now: () => currentTime,
        fetch: async () => new Response('{}', { status: 403 }),
      })
    );
    expect((await forbidden.collectStatus()).services[1]?.state).toBe(
      'unknown'
    );

    const unavailable = createStatusCollector(
      options({
        targets: [{ name: 'batcher', url: 'https://batcher.example.run' }],
        cacheTtlMs: 0,
        failureThreshold: 3,
        now: () => currentTime,
        fetch: async () => {
          calls += 1;
          throw new Error('private upstream detail');
        },
      })
    );
    expect((await unavailable.collectStatus()).services[1]?.state).toBe(
      'unknown'
    );
    currentTime += 1;
    expect((await unavailable.collectStatus()).services[1]?.state).toBe(
      'unknown'
    );
    currentTime += 1;
    expect((await unavailable.collectStatus()).services[1]?.state).toBe(
      'outage'
    );
    expect(calls).toBe(3);
    expect(JSON.stringify(await unavailable.collectStatus())).not.toContain(
      'private upstream detail'
    );
  });

  it('recovers to operational after a transport failure', async () => {
    let calls = 0;
    let currentTime = 1000;
    const collector = createStatusCollector(
      options({
        targets: [{ name: 'batcher', url: 'https://batcher.example.run' }],
        cacheTtlMs: 0,
        now: () => currentTime,
        fetch: async () => {
          calls += 1;
          if (calls === 1) throw new Error('timeout');
          return new Response('{"status":"ready"}', { status: 200 });
        },
      })
    );

    expect((await collector.collectStatus()).services[1]?.state).toBe(
      'unknown'
    );
    currentTime += 1;
    expect((await collector.collectStatus()).services[1]?.state).toBe(
      'operational'
    );
    expect(calls).toBe(2);
  });

  it('returns full public hex sensor IDs without backend-generated presentation links', async () => {
    const id = 'ab'.repeat(32);
    const collector = createStatusCollector(
      options({
        targets: [
          {
            name: 'heartbeat-tracker',
            url: 'https://heartbeats.example.run',
          },
        ],
        fetch: async () =>
          new Response(
            JSON.stringify({
              sensors_online: 1,
              sensors_total_tracked: 1,
              avg_uptime_seconds: 45,
              max_uptime_seconds: 45,
              online_window_ms: 30000,
              sensors_uptime: [
                {
                  sensor_id: id,
                  online: true,
                  first_seen: '2026-11-11T08:00:00.000Z',
                  last_seen: '2026-11-11T09:00:00.000Z',
                  uptime_seconds: 45,
                },
              ],
            }),
            { status: 200 }
          ),
      })
    );

    const result = await collector.collectSensors();
    expect(result.summary).toMatchObject({
      sensorsOnline: 1,
      avgUptimeSeconds: 45,
    });

    expect(result.sensors[0]?.sensorId).toBe(id);
    expect(result).not.toHaveProperty('sensorUrl');
    expect(result.sensors[0]).not.toHaveProperty('sensorUrl');
  });

  it('requests bounded server-side sensor pages with the selected filter and cursor', async () => {
    let requestUrl = '';
    const collector = createStatusCollector(
      options({
        targets: [
          {
            name: 'heartbeat-tracker',
            url: 'https://heartbeats.example.run',
          },
        ],
        fetch: async (input) => {
          requestUrl = String(input);
          return new Response(
            JSON.stringify({
              observedAt: '2026-11-11T09:00:00Z',
              total: 70,
              nextCursor: '50',
              summary: { sensors_online: 50 },
              sensors: [],
            }),
            { status: 200 }
          );
        },
      })
    );

    const result = await collector.collectSensors({
      limit: 50,
      cursor: '20',
      filter: 'online',
    });
    expect(new URL(requestUrl).search).toBe(
      '?limit=50&cursor=20&filter=online'
    );
    expect(result).toMatchObject({
      total: 70,
      nextCursor: '50',
      filter: 'online',
      truncated: true,
    });
  });

  it('rejects unsafe or non-HTTPS production targets without exposing their URL', async () => {
    const collector = createStatusCollector(
      options({
        targets: [
          {
            name: 'batcher',
            url: 'https://user:pass@batcher.example.run/private',
          },
        ],
      })
    );
    const snapshot = await collector.collectStatus();
    expect(snapshot.services[1]?.state).toBe('unknown');
    expect(JSON.stringify(snapshot)).not.toContain('batcher.example.run');
  });
});
