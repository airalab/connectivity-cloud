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
import type { Consumer, Producer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createBatcherService } from '../src/index.js';
import type { BatcherConfig } from '../src/config.js';

function testConfig(overrides: Partial<BatcherConfig> = {}): BatcherConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'batcher-v1',
    source: 'batcher',
    healthPort: 3042,
    batchSize: 10,
    batchTimeoutMs: 60000,
    maxPayloadBytes: 8192,
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
  offset: bigint,
  messageSize: number
): FakeMessage {
  const signedEnvelope = create(SignedEnvelopeSchema, {
    sensorId: Buffer.alloc(32, 1),
    nonce: Buffer.alloc(16, 2),
    message: Buffer.alloc(messageSize, 9),
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

function createFakeProducer(
  sent: { topic: string; value: Buffer; headers?: Record<string, Buffer> }[]
): Producer {
  const fake = {
    async send({
      messages,
    }: {
      messages: {
        topic: string;
        value: Buffer;
        headers?: Record<string, Buffer>;
      }[];
    }) {
      sent.push(...messages);
    },
    async close() {},
  };
  return fake as unknown as Producer;
}

describe('batcher oversized-event DLQ forwarding', () => {
  it('forwards the original authorized-envelope bytes for an oversized event', async () => {
    // A single event whose serialized+compressed size exceeds the tiny
    // maxPayloadBytes below, forcing it into the `oversized` path.
    const message = createAuthorizedMessage(0, 0n, 5000);
    const { consumer, commits } = createFakeConsumer([message]);
    const sent: {
      topic: string;
      value: Buffer;
      headers?: Record<string, Buffer>;
    }[] = [];
    const producer = createFakeProducer(sent);

    const service = createBatcherService(
      testConfig({ maxPayloadBytes: 100, batchSize: 1 }),
      {
        createConsumer: () => consumer,
        createProducer: () => producer,
        createHealthServer: () =>
          ({
            close(callback: (error?: Error) => void) {
              callback();
            },
          }) as unknown as import('node:http').Server,
      }
    );

    await service.start();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await service.stop();

    expect(commits).toEqual([{ partition: 0, offset: 1n }]);

    const dlqRecords = sent.filter((m) => m.topic === TELEMETRY_TOPICS.DLQ);
    expect(dlqRecords).toHaveLength(1);
    // The DLQ record must be the original authorized Envelope bytes (decodable
    // the same way as any other DLQ record), not just the inner SignedEnvelope.
    expect(dlqRecords[0]?.value).toEqual(message.value);
    expect(dlqRecords[0]?.headers?.reason?.toString()).toContain(
      'ANCHOR_PAYLOAD_TOO_LARGE'
    );

    const metrics = service.getMetrics();
    expect(metrics.oversizedEvents).toBe(1);
    expect(metrics.batchesProduced).toBe(0);
  });
});
