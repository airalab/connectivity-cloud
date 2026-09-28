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
import { create, toBinary } from '@bufbuild/protobuf';
import type { ApiPromise } from '@polkadot/api';
import type { Consumer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { createBlockchainAnchorService } from '../src/index.js';
import type { BlockchainAnchorConfig } from '../src/config.js';

function testConfig(
  overrides: Partial<BlockchainAnchorConfig> = {}
): BlockchainAnchorConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'blockchain-anchor-v1',
    substrateWsUrl: 'ws://localhost:9944',
    suri: '//Alice',
    nodeId: 0,
    healthPort: 3053,
    maxPayloadBytes: 8192,
    source: 'blockchain-anchor',
    ...overrides,
  };
}

interface FakeMessage {
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer;
}

function createBatchedMessage(
  eventId: string,
  batchId: string,
  payload: Uint8Array,
  partition: number,
  offset: bigint
): FakeMessage {
  const batchedPayload = create(TelemetryBatchedPayloadSchema, {
    batchId,
    payload,
    eventCount: 5,
    sensorIds: [Buffer.alloc(32, 1)],
    uncompressedSize: payload.length * 2,
    compressedSize: payload.length,
    payloadHash: Buffer.alloc(32, 3),
  });

  const envelope = create(EnvelopeSchema, {
    eventId,
    eventType: TELEMETRY_TOPICS.BATCHED,
    eventVersion: '1.0.0',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'batcher',
    payload: toBinary(TelemetryBatchedPayloadSchema, batchedPayload),
  });

  return {
    topic: TELEMETRY_TOPICS.BATCHED,
    partition,
    offset,
    value: Buffer.from(toBinary(EnvelopeSchema, envelope)),
  };
}

/**
 * A fake consumer that yields the given messages and then keeps the stream
 * open until `close()` is called.
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

/** A fake ApiPromise whose extrinsic submission always fails. */
function createFailingApi(): ApiPromise {
  return {
    isReady: Promise.resolve(),
    query: {
      cps: {
        async payload(_nodeId: number) {
          return { isEmpty: true, isSome: false };
        },
      },
    },
    tx: {
      cps: {
        setPayload(_nodeId: number, _payload: number[]) {
          return {
            signAndSend(
              _account: unknown,
              callback: (result: unknown) => void
            ) {
              queueMicrotask(() => {
                callback({
                  status: { type: 'Invalid' },
                  isFinalized: false,
                  isError: true,
                  events: [],
                });
              });
              return Promise.resolve(() => {});
            },
          };
        },
      },
    },
    events: {},
    async disconnect() {},
  } as unknown as ApiPromise;
}

describe('blockchain-anchor anchor failure handling', () => {
  it('does not commit a later offset when an earlier anchor attempt fails', async () => {
    const payloadA = Buffer.from('compressed-batch-bytes-a');
    const payloadB = Buffer.from('compressed-batch-bytes-b');

    // Two distinct batches on the same partition; the first anchor attempt
    // fails permanently in this fake. A prior bug continued the consume loop
    // and let the second message's commit silently advance past the first
    // (unretried) offset.
    const messages = [
      createBatchedMessage('evt-a', 'batch-a', payloadA, 0, 0n),
      createBatchedMessage('evt-b', 'batch-b', payloadB, 0, 1n),
    ];

    const { consumer, commits } = createFakeConsumer(messages);
    const api = createFailingApi();

    const service = createBlockchainAnchorService(testConfig(), {
      createConsumer: () => consumer,
      createApi: () => Promise.resolve(api),
      createHealthServer: () =>
        ({
          close(callback: (error?: Error) => void) {
            callback();
          },
        }) as unknown as import('node:http').Server,
    });

    // The anchor failure intentionally propagates out of the internal run
    // loop (so the process can crash and be restarted by the orchestrator,
    // resuming from the last committed offset). Swallow that expected
    // rejection here rather than letting it surface as an unhandled one -
    // the assertions below directly verify the resulting behavior.
    const onUnhandledRejection = (): void => {};
    process.on('unhandledRejection', onUnhandledRejection);

    try {
      await service.start();
      await new Promise((resolve) => setTimeout(resolve, 50));
      await service.stop();
    } finally {
      process.off('unhandledRejection', onUnhandledRejection);
    }

    // Neither offset is committed: the first anchor failure must stop
    // processing rather than let the second message's commit skip past it.
    expect(commits).toEqual([]);

    const metrics = service.getMetrics();
    expect(metrics.failed).toBe(1);
    expect(metrics.anchored).toBe(0);
    // Only the first message was consumed before processing halted.
    expect(metrics.consumed).toBe(1);
  });
});
