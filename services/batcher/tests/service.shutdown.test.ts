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
  TelemetryBatchedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import type { Consumer, Producer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createBatcherService } from '../src/index.js';
import type { BatcherConfig } from '../src/config.js';

function testConfig(overrides: Partial<BatcherConfig> = {}): BatcherConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'batcher-v1',
    source: 'batcher',
    healthPort: 3041,
    batchSize: 10,
    // Large timeout so the flush timer never fires during the test; the only
    // flush should be the one triggered by graceful shutdown.
    batchTimeoutMs: 60000,
    ...overrides,
  };
}

interface FakeMessage {
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer;
}

function createAuthorizedMessage(
  partition: number,
  offset: bigint
): FakeMessage {
  const signedEnvelope = create(SignedEnvelopeSchema, {
    sensorId: Buffer.alloc(32, 1),
    nonce: Buffer.alloc(16, 2),
    message: Buffer.from(JSON.stringify({ temp: 25 })),
    signature: Buffer.alloc(64, 3),
  });

  const payload = create(TelemetryAuthorizedPayloadSchema, {
    sensorId: Buffer.alloc(32, 1),
    signedEnvelope: toBinary(SignedEnvelopeSchema, signedEnvelope),
  });

  const envelope = create(EnvelopeSchema, {
    eventId: `evt-${partition}-${offset}`,
    eventType: TELEMETRY_TOPICS.AUTHORIZED,
    eventVersion: 'v1',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'endpoint',
    payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
  });

  return {
    topic: TELEMETRY_TOPICS.AUTHORIZED,
    partition,
    offset,
    value: Buffer.from(toBinary(EnvelopeSchema, envelope)),
  };
}

/**
 * A fake consumer that yields the given messages and then keeps the stream
 * open (as a live consumer would) until `close()` is called, so the pending
 * batch stays partially filled until shutdown.
 */
function createFakeConsumer(messages: FakeMessage[]): {
  consumer: Consumer;
  commits: { partition: number; offset: bigint }[];
} {
  let releaseClose!: () => void;
  const closed = new Promise<void>((resolve) => {
    releaseClose = resolve;
  });
  const commits: { partition: number; offset: bigint }[] = [];

  const fake = {
    async consume() {
      return (async function* () {
        for (const message of messages) {
          yield message;
        }
        await closed;
      })();
    },
    async getLag() {
      return new Map<string, bigint[]>();
    },
    async commit({
      offsets,
    }: {
      offsets: { partition: number; offset: bigint }[];
    }) {
      for (const o of offsets) {
        commits.push({ partition: o.partition, offset: o.offset });
      }
    },
    async close() {
      releaseClose();
    },
  };

  return { consumer: fake as unknown as Consumer, commits };
}

function createFakeProducer(sent: Buffer[]): Producer {
  const fake = {
    async send({ messages }: { messages: { topic: string; value: Buffer }[] }) {
      for (const m of messages) {
        sent.push(m.value);
      }
    },
    async close() {},
  };
  return fake as unknown as Producer;
}

describe('batcher graceful shutdown', () => {
  it('flushes a partially filled batch and commits offsets on shutdown', async () => {
    const messages = [
      createAuthorizedMessage(0, 0n),
      createAuthorizedMessage(0, 1n),
    ];
    const { consumer, commits } = createFakeConsumer(messages);
    const sent: Buffer[] = [];
    const producer = createFakeProducer(sent);

    const service = createBatcherService(testConfig(), {
      createConsumer: () => consumer,
      createProducer: () => producer,
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    await service.start();
    // Let the two messages be consumed and added to the pending batch.
    await new Promise((resolve) => setTimeout(resolve, 50));

    // Nothing should have been produced yet: batch (2) is below batchSize (10)
    // and the flush timer has not fired.
    expect(sent).toHaveLength(0);
    expect(service.getMetrics().batchesProduced).toBe(0);

    // Graceful shutdown must flush the pending batch.
    await service.stop();

    expect(sent).toHaveLength(1);

    const metrics = service.getMetrics();
    expect(metrics.consumed).toBe(2);
    expect(metrics.batchesProduced).toBe(1);
    expect(metrics.eventsBatched).toBe(2);

    // The produced envelope carries both events on the batched topic.
    const envelope = fromBinary(EnvelopeSchema, new Uint8Array(sent[0]!));
    expect(envelope.eventType).toBe(TELEMETRY_TOPICS.BATCHED);
    const batched = fromBinary(TelemetryBatchedPayloadSchema, envelope.payload);
    expect(batched.eventCount).toBe(2);

    // Offsets are committed only after a successful produce, using the next
    // offset (max consumed offset + 1) for the partition.
    expect(commits).toEqual([{ partition: 0, offset: 2n }]);
  });
});
