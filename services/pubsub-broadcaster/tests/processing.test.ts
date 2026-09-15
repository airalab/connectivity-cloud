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
import { TELEMETRY_TOPICS } from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import { EnvelopeSchema, TelemetryAuthorizedPayloadSchema } from '@scp/core';
import { describe, expect, it } from 'vitest';
import { handleTelemetryMessage } from '../src/index.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

function makeAuthorizedEnvelope(): Buffer {
  const payload = create(TelemetryAuthorizedPayloadSchema, {
    sensorId: Buffer.alloc(32, 1),
    timestamp: BigInt(Date.parse('2026-01-01T00:00:00Z')),
    nonce: Buffer.alloc(16, 2),
    message: Buffer.from(JSON.stringify({ temp: 21 })),
    signature: Buffer.alloc(64, 3),
    signedEnvelope: Buffer.alloc(100, 4),
  });

  const envelope = create(EnvelopeSchema, {
    eventId: 'evt-1',
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: new Date().toISOString(),
    source: 'test',
    traceId: 'trace-1',
    payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
  });

  return Buffer.from(toBinary(EnvelopeSchema, envelope));
}

function makeConfig(
  overrides: Partial<PubsubBroadcasterConfig> = {}
): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'pubsub-broadcaster-v1',
    source: 'pubsub-broadcaster',
    healthPort: 3020,
    pubsubTopic: 'telemetry/authorized/v1',
    reservedPeers: [],
    minConnectedPeers: 0,
    libp2pPrivateKeySeedHex: undefined,
    libp2pListenAddresses: ['/ip4/0.0.0.0/tcp/0'],
    connectivityStabilizationIntervalMs: 5000,
    reconnectIntervalMs: 10000,
    ...overrides,
  };
}

describe('pubsub broadcaster processing', () => {
  it('publishes authorized telemetry to configured pubsub topic', async () => {
    const publishedTopics: string[] = [];
    const publishedData: Uint8Array[] = [];
    const metrics = {
      consumed: 0,
      publishSuccess: 0,
      publishFailure: 0,
    };

    const pubsub = {
      async start() {},
      async stop() {},
      async publish(topic: string, data: Uint8Array) {
        publishedTopics.push(topic);
        publishedData.push(data);
      },
    };

    await handleTelemetryMessage(
      makeAuthorizedEnvelope(),
      pubsub,
      makeConfig(),
      metrics
    );

    expect(metrics.consumed).toBe(1);
    expect(metrics.publishSuccess).toBe(1);
    expect(metrics.publishFailure).toBe(0);
    expect(publishedTopics).toEqual(['telemetry/authorized/v1']);
    expect(publishedData.length).toBe(1);
  });

  it('logs failure but does not throw on publish error', async () => {
    const metrics = {
      consumed: 0,
      publishSuccess: 0,
      publishFailure: 0,
    };

    const pubsub = {
      async start() {},
      async stop() {},
      async publish() {
        throw new Error('network timeout');
      },
    };

    await handleTelemetryMessage(
      makeAuthorizedEnvelope(),
      pubsub,
      makeConfig(),
      metrics
    );

    expect(metrics.consumed).toBe(1);
    expect(metrics.publishSuccess).toBe(0);
    expect(metrics.publishFailure).toBe(1);
  });

  it('ignores non-authorized envelopes', async () => {
    const metrics = {
      consumed: 0,
      publishSuccess: 0,
      publishFailure: 0,
    };

    const pubsub = {
      async start() {},
      async stop() {},
      async publish() {
        throw new Error('should not be called');
      },
    };

    const rejectedEnvelope = create(EnvelopeSchema, {
      eventId: 'evt-rejected',
      eventType: TELEMETRY_TOPICS.REJECTED,
      eventVersion: 'v1',
      occurredAt: new Date().toISOString(),
      source: 'test',
      payload: Buffer.alloc(0),
    });

    await handleTelemetryMessage(
      Buffer.from(toBinary(EnvelopeSchema, rejectedEnvelope)),
      pubsub,
      makeConfig(),
      metrics
    );

    expect(metrics.consumed).toBe(0);
    expect(metrics.publishSuccess).toBe(0);
    expect(metrics.publishFailure).toBe(0);
  });

  it('handles malformed envelopes gracefully', async () => {
    const metrics = {
      consumed: 0,
      publishSuccess: 0,
      publishFailure: 0,
    };

    const pubsub = {
      async start() {},
      async stop() {},
      async publish() {
        throw new Error('should not be called');
      },
    };

    await handleTelemetryMessage(
      Buffer.from('invalid protobuf data'),
      pubsub,
      makeConfig(),
      metrics
    );

    expect(metrics.consumed).toBe(0);
    expect(metrics.publishSuccess).toBe(0);
    expect(metrics.publishFailure).toBe(0);
  });
});
