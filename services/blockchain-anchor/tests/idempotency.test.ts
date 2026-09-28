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
    healthPort: 3051,
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

/**
 * A fake ApiPromise whose `cps.payload` storage mirrors the last payload
 * submitted via `cps.setPayload`, simulating authoritative on-chain state.
 */
function createFakeApi(): {
  api: ApiPromise;
  setPayloadCalls: { nodeId: number; payload: number[] }[];
} {
  const setPayloadCalls: { nodeId: number; payload: number[] }[] = [];
  let anchored: Uint8Array | null = null;

  const api = {
    isReady: Promise.resolve(),
    query: {
      cps: {
        async payload(_nodeId: number) {
          if (!anchored) {
            return { isEmpty: true, isSome: false };
          }
          const bytes = anchored;
          return {
            isEmpty: false,
            isSome: true,
            unwrap: () => ({ toU8a: () => bytes }),
          };
        },
      },
    },
    tx: {
      cps: {
        setPayload(nodeId: number, payload: number[]) {
          setPayloadCalls.push({ nodeId, payload });
          return {
            signAndSend(
              _account: unknown,
              callback: (result: unknown) => void
            ) {
              queueMicrotask(() => {
                callback({
                  status: {
                    type: 'Finalized',
                    isInBlock: false,
                    isFinalized: true,
                    asFinalized: { toString: () => '0xfinalized' },
                  },
                  isFinalized: true,
                  isError: false,
                  events: [],
                });
                anchored = new Uint8Array(payload);
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

  return { api, setPayloadCalls };
}

describe('blockchain-anchor idempotency', () => {
  it('does not resubmit a duplicate Kafka delivery once the batch is anchored on-chain', async () => {
    const payload = Buffer.from('compressed-batch-bytes-1');

    // Same batch delivered twice (e.g. after a crash before the Kafka
    // commit landed): both messages carry the same batch_id and payload.
    const messages = [
      createBatchedMessage('evt-1', 'batch-1', payload, 0, 0n),
      createBatchedMessage('evt-1', 'batch-1', payload, 0, 1n),
    ];

    const { consumer, commits } = createFakeConsumer(messages);
    const { api, setPayloadCalls } = createFakeApi();

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

    await service.start();

    // Allow both messages to be processed.
    await new Promise((resolve) => setTimeout(resolve, 50));

    await service.stop();

    // Only one extrinsic should ever be submitted on-chain.
    expect(setPayloadCalls).toHaveLength(1);

    const metrics = service.getMetrics();
    expect(metrics.consumed).toBe(2);
    expect(metrics.anchored).toBe(1);
    expect(metrics.skippedDuplicate).toBe(1);
    expect(metrics.failed).toBe(0);

    // Both offsets are committed, since the duplicate is safely a no-op.
    expect(commits).toEqual([
      { partition: 0, offset: 1n },
      { partition: 0, offset: 2n },
    ]);
  });

  it('anchors distinct batches independently without treating them as duplicates', async () => {
    const payloadA = Buffer.from('compressed-batch-bytes-a');
    const payloadB = Buffer.from('compressed-batch-bytes-b');

    const messages = [
      createBatchedMessage('evt-a', 'batch-a', payloadA, 0, 0n),
      createBatchedMessage('evt-b', 'batch-b', payloadB, 0, 1n),
    ];

    const { consumer } = createFakeConsumer(messages);
    const { api, setPayloadCalls } = createFakeApi();

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

    await service.start();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await service.stop();

    expect(setPayloadCalls).toHaveLength(2);

    const metrics = service.getMetrics();
    expect(metrics.anchored).toBe(2);
    expect(metrics.skippedDuplicate).toBe(0);
  });

  it('rejects a payload larger than maxPayloadBytes without submitting or splitting it', async () => {
    const oversizedPayload = Buffer.alloc(200, 9);

    const messages = [
      createBatchedMessage(
        'evt-oversized',
        'batch-oversized',
        oversizedPayload,
        0,
        0n
      ),
    ];

    const { consumer, commits } = createFakeConsumer(messages);
    const { api, setPayloadCalls } = createFakeApi();

    const service = createBlockchainAnchorService(
      testConfig({ maxPayloadBytes: 100 }),
      {
        createConsumer: () => consumer,
        createApi: () => Promise.resolve(api),
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

    expect(setPayloadCalls).toHaveLength(0);
    expect(commits).toEqual([{ partition: 0, offset: 1n }]);

    const metrics = service.getMetrics();
    expect(metrics.rejectedOversized).toBe(1);
    expect(metrics.anchored).toBe(0);
  });
});
