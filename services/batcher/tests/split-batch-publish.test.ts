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
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { create, toBinary } from '@bufbuild/protobuf';
import { randomBytes } from 'node:crypto';
import type { Consumer, Producer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createBatcherService } from '../src/index.js';
import type { BatcherConfig } from '../src/config.js';

const MAX_PAYLOAD_BYTES = 2000;

function testConfig(overrides: Partial<BatcherConfig> = {}): BatcherConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'batcher-v1',
    source: 'batcher',
    healthPort: 3043,
    batchSize: 20,
    batchTimeoutMs: 60000,
    maxPayloadBytes: MAX_PAYLOAD_BYTES,
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
    // High-entropy payload so zstd cannot meaningfully compress it, forcing the
    // combined batch above the payload limit and requiring a split.
    message: randomBytes(1000),
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

describe('batcher split-batch publishing', () => {
  it('publishes all fitted sub-batches in a single producer.send call', async () => {
    const messages = Array.from({ length: 20 }, (_, i) =>
      createAuthorizedMessage(0, BigInt(i))
    );
    const { consumer, commits } = createFakeConsumer(messages);

    let sendCalls = 0;
    let totalMessages = 0;
    const producer = {
      async send({ messages: sent }: { messages: unknown[] }) {
        sendCalls += 1;
        totalMessages += sent.length;
      },
      async close() {},
    } as unknown as Producer;

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
    await new Promise((resolve) => setTimeout(resolve, 100));
    await service.stop();

    const metrics = service.getMetrics();
    // The batch had to be split into more than one sub-batch to fit.
    expect(metrics.batchesProduced).toBeGreaterThan(1);

    // All sub-batches for this attempt are published in a single Kafka
    // request: sending them one at a time would let an earlier split
    // succeed while a later one fails, and a retry of the whole batch would
    // then republish the already-sent sub-batch.
    expect(sendCalls).toBe(1);
    expect(totalMessages).toBe(metrics.batchesProduced);

    expect(commits).toEqual([{ partition: 0, offset: 20n }]);
  });
});
