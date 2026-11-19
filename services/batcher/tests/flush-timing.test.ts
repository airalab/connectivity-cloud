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
    healthPort: 3041,
    batchSize: 10,
    // Large timeout so the flush timer never fires during the test; the only
    // flush should be the one triggered by graceful shutdown.
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
 * A fake consumer whose stream is fed on demand via `push()`. `getLag`
 * reports a configurable (high) lag to prove lag no longer drives flushing.
 */
function createFakeConsumer(lag: bigint): {
  consumer: Consumer;
  commits: { partition: number; offset: bigint }[];
  push: (message: FakeMessage) => void;
} {
  const queue: FakeMessage[] = [];
  let wake: (() => void) | null = null;
  let closedFlag = false;
  const commits: { partition: number; offset: bigint }[] = [];

  const fake = {
    async consume() {
      return (async function* () {
        while (!closedFlag) {
          const next = queue.shift();
          if (next) {
            yield next;
          } else {
            await new Promise<void>((resolve) => {
              wake = resolve;
            });
          }
        }
      })();
    },
    async getLag() {
      return new Map<string, bigint[]>([['t', [lag]]]);
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
      closedFlag = true;
      wake?.();
    },
  };

  return {
    consumer: fake as unknown as Consumer,
    commits,
    push(message) {
      queue.push(message);
      wake?.();
    },
  };
}

function createFakeProducer(
  sent: Buffer[],
  failures = { remaining: 0 }
): Producer {
  const fake = {
    async send({ messages }: { messages: { topic: string; value: Buffer }[] }) {
      if (failures.remaining > 0) {
        failures.remaining -= 1;
        throw new Error('kafka unavailable');
      }
      for (const m of messages) {
        sent.push(m.value);
      }
    },
    async close() {},
  };
  return fake as unknown as Producer;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

function setup(
  overrides: Partial<BatcherConfig>,
  options: { lag?: bigint; failures?: number } = {}
) {
  const { consumer, commits, push } = createFakeConsumer(options.lag ?? 0n);
  const sent: Buffer[] = [];
  const failures = { remaining: options.failures ?? 0 };
  const service = createBatcherService(testConfig(overrides), {
    createConsumer: () => consumer,
    createProducer: () => createFakeProducer(sent, failures),
    createHealthServer: () =>
      ({
        close(callback: (error?: Error) => void) {
          callback();
        },
      }) as unknown as import('node:http').Server,
  });
  return { service, commits, push, sent };
}

describe('batcher flush timing', () => {
  it('does not flush an undersized batch under high Kafka lag', async () => {
    const { service, commits, push, sent } = setup(
      { batchSize: 10, batchTimeoutMs: 60000 },
      { lag: 1000n }
    );
    await service.start();
    push(createAuthorizedMessage(0, 0n));
    push(createAuthorizedMessage(0, 1n));
    await sleep(50);

    expect(sent).toHaveLength(0);
    expect(commits).toHaveLength(0);

    await service.stop();
    expect(sent).toHaveLength(1);
  });

  it('flushes immediately once the batch is full', async () => {
    const { service, commits, push, sent } = setup({
      batchSize: 3,
      batchTimeoutMs: 60000,
    });
    await service.start();
    for (let i = 0n; i < 3n; i++) push(createAuthorizedMessage(0, i));
    await sleep(50);

    expect(sent).toHaveLength(1);
    expect(commits).toEqual([{ partition: 0, offset: 3n }]);
    await service.stop();
  });

  it('flushes a partial batch within the timeout of the first event despite continuous traffic', async () => {
    const { service, push, sent } = setup({
      batchSize: 100,
      batchTimeoutMs: 200,
    });
    await service.start();

    // Events keep arriving more often than the timeout; a timer reset on each
    // event would postpone the flush forever.
    for (let i = 0n; i < 6n; i++) {
      push(createAuthorizedMessage(0, i));
      await sleep(60);
    }

    expect(sent.length).toBeGreaterThanOrEqual(1);
    await service.stop();
  });

  it('keeps a failed batch for retry and commits only after a successful publish', async () => {
    const { service, commits, push, sent } = setup(
      { batchSize: 2, batchTimeoutMs: 100 },
      { failures: 1 }
    );
    await service.start();
    push(createAuthorizedMessage(0, 0n));
    push(createAuthorizedMessage(0, 1n));
    await sleep(50);

    // First publish failed: nothing produced or committed yet.
    expect(sent).toHaveLength(0);
    expect(commits).toHaveLength(0);
    expect(service.getMetrics().produceFailure).toBe(1);

    // The re-attached batch is retried by the timer.
    await sleep(250);
    expect(sent).toHaveLength(1);
    expect(commits).toEqual([{ partition: 0, offset: 2n }]);
    await service.stop();
  });
});
