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
  TelemetryAuthorizedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import type { Consumer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createPubsubBroadcasterService } from '../src/index.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

function testConfig(
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

function createAuthorizedMessage() {
  const payload = create(TelemetryAuthorizedPayloadSchema, {
    sensorId: Buffer.alloc(32, 1),
    timestamp: BigInt(Date.parse('2026-01-01T00:00:00Z')),
    nonce: Buffer.alloc(16, 2),
    message: Buffer.from(JSON.stringify({ temp: 25 })),
    signature: Buffer.alloc(64, 3),
    signedEnvelope: Buffer.alloc(100, 4),
  });

  const envelope = create(EnvelopeSchema, {
    eventId: 'evt-int-1',
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'endpoint',
    payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
  });

  return Buffer.from(toBinary(EnvelopeSchema, envelope));
}

const authorizedMessage = createAuthorizedMessage();

describe('pubsub broadcaster connectivity-based Kafka pause/resume', () => {
  it('pauses Kafka consumption when connected peers drop below the minimum and resumes on recovery', async () => {
    let connectedPeerCount = 2;
    const pauseSpy: string[] = [];

    async function* emptyStream() {
      // No messages; the test only exercises pause()/resume() calls via the
      // connectivity monitor, not actual message consumption.
    }

    const stream = emptyStream() as AsyncIterable<{
      topic: string;
      partition: number;
      offset: bigint;
      value: Buffer | null;
    }> & { pause?: () => void; resume?: () => void };
    stream.pause = () => pauseSpy.push('pause');
    stream.resume = () => pauseSpy.push('resume');

    const fakeConsumer = {
      async consume() {
        return stream;
      },
      async close() {},
    };

    const service = createPubsubBroadcasterService(
      testConfig({
        minConnectedPeers: 1,
        connectivityStabilizationIntervalMs: 50,
      }),
      {
        createConsumer: () => fakeConsumer as unknown as Consumer,
        createPubsubClient: async () => ({
          async start() {},
          async stop() {},
          async publish() {},
          getConnectedPeerIds: () => (connectedPeerCount > 0 ? ['peer-1'] : []),
          getConnectedPeerCount: () => connectedPeerCount,
        }),
        createHealthServer: () =>
          ({
            close(callback: (error?: Error) => void) {
              callback();
            },
          }) as unknown as import('node:http').Server,
      }
    );

    await service.start();
    // Allow the connectivity poll loop to observe sufficient connectivity and resume.
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(service.getMetrics().kafkaPaused).toBe(false);
    expect(service.isReady()).toBe(true);

    // Simulate peer loss.
    connectedPeerCount = 0;
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(service.getMetrics().kafkaPaused).toBe(true);
    expect(service.isReady()).toBe(false);
    expect(pauseSpy).toContain('pause');

    // Simulate recovery.
    connectedPeerCount = 1;
    await new Promise((resolve) => setTimeout(resolve, 200));
    expect(service.getMetrics().kafkaPaused).toBe(false);
    expect(service.isReady()).toBe(true);
    expect(pauseSpy).toContain('resume');

    await service.stop();
  });
});

describe('pubsub broadcaster integration flow (mock harness)', () => {
  it('processes consume -> publish flow with autocommit', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async consume() {
        // Return an async iterable that yields messages
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            offset: 1n,
            value: authorizedMessage,
          };
        })();
      },
      async close() {},
    };

    const service = createPubsubBroadcasterService(testConfig({}), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
      createPubsubClient: async () => ({
        async start() {},
        async stop() {},
        async publish() {
          callOrder.push('publish');
        },
      }),
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    await service.start();
    // Give it time to process the message
    await new Promise((resolve) => setTimeout(resolve, 100));
    await service.stop();

    expect(callOrder).toEqual(['publish']);
    const metrics = service.getMetrics();
    expect(metrics.consumed).toBe(1);
    expect(metrics.publishSuccess).toBe(1);
    expect(metrics.publishFailure).toBe(0);
  });

  it('handles publish failures gracefully without retry', async () => {
    const callOrder: string[] = [];

    const fakeConsumer = {
      async consume() {
        return (async function* () {
          yield {
            topic: TELEMETRY_TOPICS.AUTHORIZED,
            partition: 0,
            offset: 0n,
            value: authorizedMessage,
          };
        })();
      },
      async close() {},
    };

    const service = createPubsubBroadcasterService(testConfig({}), {
      createConsumer: () => fakeConsumer as unknown as Consumer,
      createPubsubClient: async () => ({
        async start() {},
        async stop() {},
        async publish() {
          callOrder.push('publish-attempt');
          throw new Error('network timeout');
        },
      }),
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    await service.start();
    // Give it time to process the message
    await new Promise((resolve) => setTimeout(resolve, 100));
    await service.stop();

    expect(callOrder).toEqual(['publish-attempt']);
    const metrics = service.getMetrics();
    expect(metrics.consumed).toBe(1);
    expect(metrics.publishSuccess).toBe(0);
    expect(metrics.publishFailure).toBe(1);
  });
});
