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
} from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import {
  createHeartbeatTrackerState,
  handleTelemetryMessage,
} from '../src/index.js';

function createClock(start = Date.parse('2026-01-01T00:00:00Z')) {
  let current = start;
  return {
    now: () => current,
    set: (next: number) => {
      current = next;
    },
    advance: (ms: number) => {
      current += ms;
    },
  };
}

function authorizedEnvelope(
  sensorId: string,
  occurredAtMs: number = Date.parse('2026-01-01T00:00:00Z')
) {
  const sensorBytes = Buffer.from(sensorId.padEnd(32, '_').slice(0, 32));
  const signedEnvelope = Buffer.alloc(100, 0); // Fake signed envelope bytes

  const payload = create(TelemetryAuthorizedPayloadSchema, {
    sensorId: sensorBytes,
    signedEnvelope,
  });

  const envelope = create(EnvelopeSchema, {
    eventId: `evt-${sensorId}-${occurredAtMs}`,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: new Date(occurredAtMs).toISOString(),
    source: 'endpoint',
    payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
  });

  return Buffer.from(toBinary(EnvelopeSchema, envelope));
}

function encodeSensorId(sensorId: string): string {
  return Buffer.from(sensorId.padEnd(32, '_').slice(0, 32)).toString('hex');
}

class FakeRedis {
  private readonly hashes = new Map<string, Record<string, string>>();
  private readonly sets = new Map<string, Set<string>>();

  async hgetall(key: string): Promise<Record<string, string>> {
    return { ...(this.hashes.get(key) ?? {}) };
  }

  async hset(key: string, map: Record<string, string>): Promise<number> {
    const current = this.hashes.get(key) ?? {};
    this.hashes.set(key, { ...current, ...map });
    return Object.keys(map).length;
  }

  async sadd(key: string, member: string): Promise<number> {
    const set = this.sets.get(key) ?? new Set<string>();
    const before = set.size;
    set.add(member);
    this.sets.set(key, set);
    return set.size > before ? 1 : 0;
  }

  async smembers(key: string): Promise<string[]> {
    return [...(this.sets.get(key) ?? new Set<string>())];
  }

  async srem(key: string, member: string): Promise<number> {
    const set = this.sets.get(key);
    if (!set) return 0;
    const deleted = set.delete(member) ? 1 : 0;
    if (set.size === 0) {
      this.sets.delete(key);
    }
    return deleted;
  }

  async del(key: string): Promise<number> {
    const existed = this.hashes.has(key) ? 1 : 0;
    this.hashes.delete(key);
    return existed;
  }

  /**
   * Test double for the heartbeat compare-and-set Lua script used in
   * production. Mirrors its semantics: `lastSeen` only advances when the
   * incoming event timestamp is strictly newer than what is stored.
   */
  async eval(
    _script: string,
    numkeys: number,
    ...rest: Array<string | number>
  ): Promise<[number, number, number, number, number]> {
    const keys = rest.slice(0, numkeys) as string[];
    const args = rest.slice(numkeys);
    const [key, sensorsKey] = keys;
    const [sensorId, eventTimeRaw, onlineWindowMsRaw] = args;
    const eventTime = Number(eventTimeRaw);
    const onlineWindowMs = Number(onlineWindowMsRaw);

    if (key === undefined || sensorsKey === undefined) {
      throw new Error('missing keys for eval');
    }
    if (typeof sensorId !== 'string') {
      throw new Error('missing sensorId arg for eval');
    }

    await this.sadd(sensorsKey, sensorId);

    const existing = this.hashes.get(key) ?? {};
    const lastSeen = Number.parseInt(existing.lastSeen ?? '', 10);
    const firstSeen = Number.parseInt(existing.firstSeen ?? '', 10);
    const onlineSince = Number.parseInt(existing.onlineSince ?? '', 10);
    const hasLastSeen = Number.isFinite(lastSeen);
    const hasFirstSeen = Number.isFinite(firstSeen);
    const hasOnlineSince = Number.isFinite(onlineSince);
    const isNew = hasFirstSeen ? 0 : 1;

    if (!hasLastSeen || eventTime > lastSeen) {
      const newFirstSeen = hasFirstSeen ? firstSeen : eventTime;
      let newOnlineSince = hasOnlineSince ? onlineSince : eventTime;
      if (hasLastSeen && eventTime - lastSeen > onlineWindowMs) {
        newOnlineSince = eventTime;
      }
      this.hashes.set(key, {
        firstSeen: String(newFirstSeen),
        lastSeen: String(eventTime),
        onlineSince: String(newOnlineSince),
      });
      return [1, isNew, newFirstSeen, eventTime, newOnlineSince];
    }

    return [
      0,
      isNew,
      hasFirstSeen ? firstSeen : eventTime,
      lastSeen,
      hasOnlineSince ? onlineSince : eventTime,
    ];
  }
}

describe('heartbeat tracker state', () => {
  it('counts a sensor online right after an authorized message', async () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', clock.now()),
      tracker,
      consumed
    );

    const metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(1);
    expect(metrics.sensors_total_tracked).toBe(1);
    expect(consumed.value).toBe(1);
  });

  it('drops sensor from online count once time passes the window', async () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', clock.now()),
      tracker,
      consumed
    );
    clock.advance(30001);

    const metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(0);
    expect(metrics.sensors_total_tracked).toBe(1);
  });

  it('recomputes online count correctly across multiple sensors', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    await handleTelemetryMessage(
      authorizedEnvelope('sensor-a', clock.now()),
      tracker,
      consumed
    );
    clock.set(base + 10000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-b', clock.now()),
      tracker,
      consumed
    );
    clock.set(base + 39000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-c', clock.now()),
      tracker,
      consumed
    );

    const metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(3);
    expect(metrics.sensors_online).toBe(2);
    expect(metrics.sensor_uptime_seconds).toMatchObject({
      [encodeSensorId('sensor-b')]: 29,
      [encodeSensorId('sensor-c')]: 0,
    });
    expect(
      metrics.sensor_uptime_seconds[encodeSensorId('sensor-a')]
    ).toBeUndefined();
  });

  it('keeps onlineSince stable within window and resets after large gap', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', clock.now()),
      tracker,
      consumed
    );
    clock.advance(10000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', clock.now()),
      tracker,
      consumed
    );
    clock.advance(5000);

    let metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensor_uptime_seconds[encodeSensorId('sensor-1')]).toBe(15);

    clock.advance(35000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', clock.now()),
      tracker,
      consumed
    );

    metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensor_uptime_seconds[encodeSensorId('sensor-1')]).toBe(0);
  });

  it('computes uptime aggregates and returns avg 0 when none are online', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    await handleTelemetryMessage(
      authorizedEnvelope('sensor-a', clock.now()),
      tracker,
      consumed
    );
    clock.set(base + 10000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-b', clock.now()),
      tracker,
      consumed
    );
    clock.set(base + 25000);

    let metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.max_uptime_seconds).toBe(25);
    expect(metrics.avg_uptime_seconds).toBe(20);

    clock.set(base + 80001);
    metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_online).toBe(0);
    expect(metrics.max_uptime_seconds).toBe(0);
    expect(metrics.avg_uptime_seconds).toBe(0);
  });

  it('ignores invalid and non-authorized envelopes', async () => {
    const clock = createClock();
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    await handleTelemetryMessage('not-json', tracker, consumed);
    await handleTelemetryMessage(
      JSON.stringify({ event_type: TELEMETRY_TOPICS.AUTHORIZED }),
      tracker,
      consumed
    );
    await handleTelemetryMessage(
      JSON.stringify({
        event_id: 'evt-rejected',
        event_type: TELEMETRY_TOPICS.REJECTED,
        event_version: 'v1',
        occurred_at: '2026-01-01T00:00:00Z',
        source: 'endpoint',
        payload: {
          reason_code: 'forbidden',
        },
      }),
      tracker,
      consumed
    );

    const metrics = await tracker.createMetrics(consumed.value);
    expect(consumed.value).toBe(0);
    expect(metrics.sensors_total_tracked).toBe(0);
    expect(metrics.sensors_online).toBe(0);
  });

  it('does not move lastSeen backwards when replaying an older event', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    // Newest event arrives first.
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', base + 20000),
      tracker,
      consumed
    );
    // A delayed/replayed older event arrives afterwards.
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-1', base + 5000),
      tracker,
      consumed
    );

    clock.set(base + 20000);
    const metrics = await tracker.createMetrics(consumed.value);
    const detail = metrics.sensors_uptime.find(
      (entry) => entry.sensor_id === encodeSensorId('sensor-1')
    );
    expect(detail?.last_seen).toBe(new Date(base + 20000).toISOString());
    expect(consumed.value).toBe(2);
  });

  it('produces monotonically increasing heartbeat state for out-of-order events', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      30000,
      300000,
      clock.now
    );
    const consumed = { value: 0 };

    const timestamps = [base, base + 5000, base + 2000, base + 15000];
    for (const ts of timestamps) {
      await handleTelemetryMessage(
        authorizedEnvelope('sensor-1', ts),
        tracker,
        consumed
      );
    }

    clock.set(base + 15000);
    const metrics = await tracker.createMetrics(consumed.value);
    const detail = metrics.sensors_uptime.find(
      (entry) => entry.sensor_id === encodeSensorId('sensor-1')
    );
    // lastSeen must reflect the highest event timestamp seen so far,
    // regardless of the arrival order of the underlying Kafka records.
    expect(detail?.last_seen).toBe(new Date(base + 15000).toISOString());
    expect(detail?.first_seen).toBe(new Date(base).toISOString());
  });

  it('prunes sensors not seen within retention window', async () => {
    const base = Date.parse('2026-01-01T00:00:00Z');
    const clock = createClock(base);
    const onlineWindowMs = 30000;
    const retentionWindowMs = 300000; // 5 minutes
    const tracker = createHeartbeatTrackerState(
      new FakeRedis(),
      'heartbeat-tracker:test',
      onlineWindowMs,
      retentionWindowMs,
      clock.now
    );
    const consumed = { value: 0 };

    // Add two sensors
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-old', clock.now()),
      tracker,
      consumed
    );
    clock.set(base + 10000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-recent', clock.now()),
      tracker,
      consumed
    );

    // Verify both are tracked
    let metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(2);
    expect(metrics.sensors_online).toBe(2);

    // Move time beyond retention window for sensor-old, but keep sensor-recent within retention
    clock.set(base + retentionWindowMs + 1000);
    await handleTelemetryMessage(
      authorizedEnvelope('sensor-recent', clock.now()),
      tracker,
      consumed
    );

    // Check metrics - sensor-old should be pruned
    metrics = await tracker.createMetrics(consumed.value);
    expect(metrics.sensors_total_tracked).toBe(1);
    expect(metrics.sensors_online).toBe(1);
    expect(metrics.sensors_uptime.length).toBe(1);
    expect(metrics.sensors_uptime.at(0)?.sensor_id).toBe(
      encodeSensorId('sensor-recent')
    );
  });
});
