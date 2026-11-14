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

const TARGETS = {
  heartbeat: {
    name: 'heartbeat-tracker',
    url: 'https://heartbeats-abc-uc.a.run.app',
  },
  pubsub: {
    name: 'pubsub-broadcaster',
    url: 'https://pubsub-abc-uc.a.run.app',
  },
  anchor: { name: 'blockchain-anchor', url: 'https://anchor-abc-uc.a.run.app' },
};

function sensorPage(
  sensors: unknown[],
  overrides: Record<string, unknown> = {}
): Record<string, unknown> {
  return {
    observedAt: '2026-11-11T08:59:30.000Z',
    limit: 50,
    total: sensors.length,
    nextCursor: null,
    summary: {
      sensors_online: sensors.length,
      sensors_total_tracked: sensors.length,
      avg_uptime_seconds: 45,
      max_uptime_seconds: 45,
      online_window_ms: 30000,
    },
    sensors,
    ...overrides,
  };
}

const validSensor = {
  sensor_id: 'ab'.repeat(32),
  online: true,
  first_seen: '2026-11-11T08:00:00.000Z',
  last_seen: '2026-11-11T09:00:00.000Z',
  uptime_seconds: 45,
};

const validPubsub = {
  connectedPeerCount: 4,
  minConnectedPeers: 1,
  kafkaPaused: false,
  publishSuccess: 10,
  publishFailure: 0,
  pauseCount: 0,
  resumeCount: 0,
};

const validAnchor = {
  observedAt: '2026-11-11T08:59:30.000Z',
  queuedBatches: 2,
  lastFinalizedAt: null,
  lastFinalizedBlock: null,
  lastBatchId: null,
  lastFinalizedObservationStale: null,
  freeBalance: null,
  tokenSymbol: null,
  tokenDecimals: null,
  bestBlock: '10',
  finalizedBlock: null,
  finalizationGap: null,
};

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
        targets: [{ name: 'batcher', url: 'https://batcher-abc-uc.a.run.app' }],
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
        targets: [{ name: 'batcher', url: 'https://batcher-abc-uc.a.run.app' }],
        now: () => currentTime,
        fetch: async () => new Response('{}', { status: 403 }),
      })
    );
    expect((await forbidden.collectStatus()).services[1]?.state).toBe(
      'unknown'
    );

    const unavailable = createStatusCollector(
      options({
        targets: [{ name: 'batcher', url: 'https://batcher-abc-uc.a.run.app' }],
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
        targets: [{ name: 'batcher', url: 'https://batcher-abc-uc.a.run.app' }],
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
            url: 'https://heartbeats-abc-uc.a.run.app',
          },
        ],
        fetch: async () =>
          new Response(
            JSON.stringify({
              ...sensorPage([
                {
                  sensor_id: id,
                  online: true,
                  first_seen: '2026-11-11T08:00:00.000Z',
                  last_seen: '2026-11-11T09:00:00.000Z',
                  uptime_seconds: 45,
                },
              ]),
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
            url: 'https://heartbeats-abc-uc.a.run.app',
          },
        ],
        fetch: async (input) => {
          requestUrl = String(input);
          return new Response(
            JSON.stringify(sensorPage([], { total: 70, nextCursor: '50' })),
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
            url: 'https://user:pass@batcher-abc-uc.a.run.app/private',
          },
        ],
      })
    );
    const snapshot = await collector.collectStatus();
    expect(snapshot.services[1]?.state).toBe('unknown');
    expect(JSON.stringify(snapshot)).not.toContain('batcher-abc-uc.a.run.app');
  });

  it('only sends ID tokens to allowlisted or Cloud Run HTTPS origins', async () => {
    const tokens: string[] = [];
    const requested: string[] = [];
    const run = async (url: string, allowedHosts?: string[]) => {
      const collector = createStatusCollector(
        options({
          targets: [{ name: 'batcher', url }],
          ...(allowedHosts ? { allowedHosts } : {}),
          getIdToken: async (audience) => {
            tokens.push(audience);
            return 'Bearer mock';
          },
          fetch: async (input) => {
            requested.push(String(input));
            return new Response('{"status":"ready"}', { status: 200 });
          },
        })
      );
      return (await collector.collectStatus()).services[1]?.state;
    };

    for (const url of [
      'https://attacker.example.com',
      'https://run.app.attacker.example',
      'https://batcher-abc-uc.a.run.app.attacker.example',
      'https://batcher-abc-uc.a.run.app:8443',
      'http://batcher-abc-uc.a.run.app',
    ]) {
      expect(await run(url)).toBe('unknown');
    }
    expect(tokens).toEqual([]);
    expect(requested).toEqual([]);

    expect(await run('https://batcher-abc-uc.a.run.app')).toBe('operational');
    expect(await run('https://batcher-123.us-central1.run.app')).toBe(
      'operational'
    );
    expect(tokens).toEqual([
      'https://batcher-abc-uc.a.run.app',
      'https://batcher-123.us-central1.run.app',
    ]);
  });

  it('uses an explicit host allowlist exclusively when configured', async () => {
    const tokens: string[] = [];
    const run = async (url: string) => {
      const collector = createStatusCollector(
        options({
          targets: [{ name: 'batcher', url }],
          allowedHosts: ['internal.example.com', '*.svc.example.org'],
          getIdToken: async (audience) => {
            tokens.push(audience);
            return 'Bearer mock';
          },
          fetch: async () => new Response('{"status":"ok"}', { status: 200 }),
        })
      );
      return (await collector.collectStatus()).services[1]?.state;
    };
    expect(await run('https://internal.example.com')).toBe('operational');
    expect(await run('https://batcher.svc.example.org')).toBe('operational');
    expect(await run('https://svc.example.org')).toBe('unknown');
    expect(await run('https://evilsvc.example.org')).toBe('unknown');
    expect(await run('https://batcher-abc-uc.a.run.app')).toBe('unknown');
    expect(tokens).toEqual([
      'https://internal.example.com',
      'https://batcher.svc.example.org',
    ]);
  });

  it('keeps the localhost exception without credentials outside production', async () => {
    let authorization: string | null = 'unset';
    let tokenRequests = 0;
    const collector = createStatusCollector(
      options({
        targets: [{ name: 'batcher', url: 'http://127.0.0.1:3041' }],
        getIdToken: async () => {
          tokenRequests += 1;
          return 'Bearer mock';
        },
        fetch: async (_input, init) => {
          authorization = new Headers(init?.headers).get('authorization');
          return new Response('{"status":"ok"}', { status: 200 });
        },
      })
    );
    expect((await collector.collectStatus()).services[1]?.state).toBe(
      'operational'
    );
    expect(authorization).toBeNull();
    expect(tokenRequests).toBe(0);
  });

  it('uses configured local health ports for fallback targets', async () => {
    const requested: string[] = [];
    const collector = createStatusCollector(
      options({
        targets: [{ name: 'batcher' }, { name: 'heartbeat-tracker' }],
        localPorts: { batcher: 4041 },
        fetch: async (input) => {
          requested.push(String(input));
          return new Response('{"status":"ok"}', { status: 200 });
        },
      })
    );
    await collector.collectStatus();
    expect(requested.sort()).toEqual([
      'http://127.0.0.1:3030/ready',
      'http://127.0.0.1:4041/ready',
    ]);
  });

  it('treats malformed or incomplete sensor responses as unavailable, not as an empty list', async () => {
    const bodies: unknown[] = [
      {},
      { sensors: [] },
      { ...sensorPage([]), summary: undefined },
      { ...sensorPage([]), total: 'many' },
      { ...sensorPage([]), observedAt: 'yesterday-ish' },
      { ...sensorPage([]), nextCursor: undefined },
      sensorPage([{ ...validSensor, online: 'yes' }]),
      sensorPage([{ ...validSensor, last_seen: null }]),
      sensorPage([validSensor, 'not-an-object']),
      sensorPage([], { sensors: 'none' }),
    ];
    for (const body of bodies) {
      const collector = createStatusCollector(
        options({
          targets: [TARGETS.heartbeat],
          fetch: async () =>
            new Response(JSON.stringify(body), { status: 200 }),
        })
      );
      const result = await collector.collectSensors();
      expect(result.available).toBe(false);
      expect(result.sensors).toEqual([]);
      expect(result.summary).toBeNull();
    }
  });

  it('keeps a legitimate empty sensor list available', async () => {
    const collector = createStatusCollector(
      options({
        targets: [TARGETS.heartbeat],
        fetch: async () =>
          new Response(JSON.stringify(sensorPage([])), { status: 200 }),
      })
    );
    const result = await collector.collectSensors();
    expect(result).toMatchObject({
      available: true,
      stale: false,
      total: 0,
      sensors: [],
      observedAt: '2026-11-11T08:59:30.000Z',
      truncated: false,
    });
  });

  it('separates observation and fetch timestamps and marks retained data stale after a failed refresh', async () => {
    let currentTime = Date.parse('2026-11-11T09:00:00Z');
    let failing = false;
    const collector = createStatusCollector(
      options({
        targets: [TARGETS.heartbeat, TARGETS.pubsub],
        cacheTtlMs: 1000,
        staleMaxAgeMs: 120_000,
        now: () => currentTime,
        fetch: async (input) => {
          if (failing) throw new Error('upstream down');
          return new Response(
            String(input).includes('/metrics')
              ? JSON.stringify({ ...validPubsub, connectedPeerCount: 3 })
              : JSON.stringify(sensorPage([validSensor])),
            { status: 200 }
          );
        },
      })
    );

    const fresh = await collector.collectSensors();
    expect(fresh).toMatchObject({
      available: true,
      stale: false,
      observedAt: '2026-11-11T08:59:30.000Z',
      fetchedAt: '2026-11-11T09:00:00.000Z',
    });

    failing = true;
    currentTime += 30_000;
    const stale = await collector.collectSensors();
    expect(stale).toMatchObject({
      available: true,
      stale: true,
      observedAt: '2026-11-11T08:59:30.000Z',
      fetchedAt: '2026-11-11T09:00:00.000Z',
    });
    expect(stale.sensors).toHaveLength(1);

    currentTime += 120_000;
    const expired = await collector.collectSensors();
    expect(expired).toMatchObject({ available: false, stale: false });
    expect(expired.observedAt).toBeNull();
  });

  it('marks network sections stale independently', async () => {
    let currentTime = Date.parse('2026-11-11T09:00:00Z');
    let anchorFailing = false;
    const collector = createStatusCollector(
      options({
        targets: [TARGETS.pubsub, TARGETS.anchor],
        cacheTtlMs: 1000,
        now: () => currentTime,
        fetch: async (input) => {
          if (String(input).includes('anchor') && anchorFailing) {
            return new Response('{}', { status: 503 });
          }
          return new Response(
            JSON.stringify(
              String(input).includes('anchor') ? validAnchor : validPubsub
            ),
            { status: 200 }
          );
        },
      })
    );
    const first = await collector.collectNetwork();
    expect(first.stale).toBe(false);

    anchorFailing = true;
    currentTime += 30_000;
    const second = await collector.collectNetwork();
    expect(second.stale).toBe(true);
    expect(second.pubsub).toMatchObject({
      available: true,
      stale: false,
      fetchedAt: '2026-11-11T09:00:30.000Z',
    });
    expect(second.blockchain).toMatchObject({
      available: true,
      stale: true,
      fetchedAt: '2026-11-11T09:00:00.000Z',
      queuedBatches: 2,
    });
  });

  it('treats malformed or incomplete network responses as unavailable', async () => {
    const bodies: Array<[unknown, unknown]> = [
      [{}, {}],
      [
        { ...validPubsub, connectedPeerCount: undefined },
        { ...validAnchor, observedAt: undefined },
      ],
      [
        { ...validPubsub, kafkaPaused: 'no' },
        { ...validAnchor, queuedBatches: 'many' },
      ],
      [
        { ...validPubsub, publishFailure: -1 },
        { ...validAnchor, bestBlock: undefined },
      ],
    ];
    for (const [pubsubBody, anchorBody] of bodies) {
      const collector = createStatusCollector(
        options({
          targets: [TARGETS.pubsub, TARGETS.anchor],
          fetch: async (input) =>
            new Response(
              JSON.stringify(
                String(input).includes('anchor') ? anchorBody : pubsubBody
              ),
              { status: 200 }
            ),
        })
      );
      const network = await collector.collectNetwork();
      expect(network.pubsub).toEqual({ available: false, stale: false });
      expect(network.blockchain).toEqual({ available: false, stale: false });
      expect(network.observedAt).toBeNull();
    }
  });

  it('accepts a complete anchor status whose observations are all null', async () => {
    const collector = createStatusCollector(
      options({
        targets: [TARGETS.anchor],
        fetch: async () =>
          new Response(
            JSON.stringify({
              ...validAnchor,
              queuedBatches: null,
              bestBlock: null,
            }),
            { status: 200 }
          ),
      })
    );
    const network = await collector.collectNetwork();
    expect(network.blockchain).toMatchObject({
      available: true,
      stale: false,
      observedAt: '2026-11-11T08:59:30.000Z',
      queuedBatches: null,
    });
  });

  it('preserves the last valid network observation when a refresh is malformed', async () => {
    let currentTime = Date.parse('2026-11-11T09:00:00Z');
    let malformed = false;
    const collector = createStatusCollector(
      options({
        targets: [TARGETS.pubsub],
        cacheTtlMs: 1000,
        now: () => currentTime,
        fetch: async () =>
          new Response(JSON.stringify(malformed ? {} : validPubsub), {
            status: 200,
          }),
      })
    );
    expect((await collector.collectNetwork()).pubsub).toMatchObject({
      available: true,
      stale: false,
      connectedPeerCount: 4,
    });

    malformed = true;
    currentTime += 30_000;
    expect((await collector.collectNetwork()).pubsub).toMatchObject({
      available: true,
      stale: true,
      connectedPeerCount: 4,
      fetchedAt: '2026-11-11T09:00:00.000Z',
    });
  });

  it('enforces the maximum stale age on cached snapshots independently of the cache TTL', async () => {
    let currentTime = Date.parse('2026-11-11T09:00:00Z');
    let failing = false;
    let requests = 0;
    const collector = createStatusCollector(
      options({
        targets: [TARGETS.heartbeat],
        cacheTtlMs: 30_000,
        staleMaxAgeMs: 300_000,
        now: () => currentTime,
        fetch: async () => {
          requests += 1;
          if (failing) throw new Error('upstream down');
          return new Response(JSON.stringify(sensorPage([validSensor])), {
            status: 200,
          });
        },
      })
    );
    expect(await collector.collectSensors()).toMatchObject({
      available: true,
      stale: false,
    });

    failing = true;
    currentTime += 299_000;
    expect(await collector.collectSensors()).toMatchObject({
      available: true,
      stale: true,
    });
    const requestsWhenStale = requests;

    currentTime += 500;
    expect(await collector.collectSensors()).toMatchObject({
      available: true,
      stale: true,
    });
    expect(requests).toBe(requestsWhenStale);

    // 300.5s after the last good fetch: beyond staleMaxAgeMs but still inside
    // the 30s cache TTL of the stale snapshot.
    currentTime += 1_000;
    const expired = await collector.collectSensors();
    expect(expired).toMatchObject({ available: false, stale: false });
    expect(requests).toBeGreaterThan(requestsWhenStale);
  });
});
