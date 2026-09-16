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
  TelemetryBatchedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import type { Consumer, Producer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createIpfsPublisherService } from '../src/index.js';
import type { IpfsPublisherConfig } from '../src/config.js';
import type { IpfsProvider } from '../src/providers/types.js';

function testConfig(
  overrides: Partial<IpfsPublisherConfig> = {}
): IpfsPublisherConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'ipfs-publisher-v1',
    source: 'ipfs-publisher',
    healthPort: 3061,
    ipfsApiUrl: 'http://localhost:5001',
    enableCompression: false,
    providers: ['fake'],
    pinataApiUrl: 'https://api.pinata.cloud',
    pinataJwt: '',
    durabilityPolicy: 'any',
    durabilityMinSuccessCount: 1,
    providerRetryBaseDelayMs: 5000,
    providerRetryMaxDelayMs: 60000,
    providerReplicationMaxPending: 1000,
    ...overrides,
  };
}

interface FakeMessage {
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer;
}

function createBatchedMessage(batchId: string): FakeMessage {
  const payload = create(TelemetryBatchedPayloadSchema, {
    batchId,
    signedEnvelopeBatch: new Uint8Array([1, 2, 3]),
    eventCount: 1,
    sensorIds: [Buffer.alloc(32, 1)],
  });

  const envelope = create(EnvelopeSchema, {
    eventId: batchId,
    eventType: TELEMETRY_TOPICS.BATCHED,
    eventVersion: '1.0.0',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'batcher',
    payload: toBinary(TelemetryBatchedPayloadSchema, payload),
  });

  return {
    topic: TELEMETRY_TOPICS.BATCHED,
    partition: 0,
    offset: 0n,
    value: Buffer.from(toBinary(EnvelopeSchema, envelope)),
  };
}

/** A fake consumer that yields a single message then stays open until closed. */
function createFakeConsumer(message: FakeMessage): { consumer: Consumer } {
  let releaseClose!: () => void;
  const closed = new Promise<void>((resolve) => {
    releaseClose = resolve;
  });

  const fake = {
    async consume() {
      return (async function* () {
        yield message;
        await closed;
      })();
    },
    async commit() {},
    async close() {
      releaseClose();
    },
  };

  return { consumer: fake as unknown as Consumer };
}

/** A fake producer that records every published `ipfs.published.v1` event id. */
function createFakeProducer(): {
  producer: Producer;
  publishedEventIds: string[];
} {
  const publishedEventIds: string[] = [];

  const fake = {
    async send({ messages }: { messages: { topic: string; value: Buffer }[] }) {
      for (const message of messages) {
        if (message.topic === TELEMETRY_TOPICS.IPFS_PUBLISHED) {
          const envelope = fromBinary(EnvelopeSchema, message.value);
          publishedEventIds.push(envelope.eventId);
        }
      }
    },
    async close() {},
  };

  return { producer: fake as unknown as Producer, publishedEventIds };
}

function createFakeProvider(): IpfsProvider {
  return {
    name: 'fake',
    async start() {},
    async stop() {},
    async add() {
      return 'QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG';
    },
  };
}

describe('ipfs-publisher durable event identity (issue #32)', () => {
  it('emits the same ipfs.published event_id when a batch is redelivered after a restart', async () => {
    const batchId = 'batch-durable-1';

    // Simulate two independent process lifetimes (e.g. crash + restart)
    // both processing the same redelivered batch. In-memory dedup state
    // does not survive between them, so the durability guarantee must come
    // from the emitted event_id being derived from the batch id.
    const runOnce = async (): Promise<string> => {
      const { consumer } = createFakeConsumer(createBatchedMessage(batchId));
      const { producer, publishedEventIds } = createFakeProducer();

      const service = createIpfsPublisherService(testConfig(), {
        createConsumer: () => consumer,
        createProducer: () => producer,
        createProviders: () => [createFakeProvider()],
        createHealthServer: () =>
          ({
            close(callback: (error?: Error) => void) {
              callback();
            },
          }) as unknown as import('node:http').Server,
      });

      await service.start();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await service.stop();

      expect(publishedEventIds).toHaveLength(1);
      return publishedEventIds[0]!;
    };

    const firstEventId = await runOnce();
    const secondEventId = await runOnce();

    expect(firstEventId).toBe(batchId);
    expect(secondEventId).toBe(batchId);
    expect(firstEventId).toBe(secondEventId);
  });
});
