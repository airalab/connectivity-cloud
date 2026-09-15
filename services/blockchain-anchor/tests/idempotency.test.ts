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
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryIpfsPublishedPayload_Compression,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import type { ApiPromise } from '@polkadot/api';
import type { Consumer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import { CID } from 'multiformats/cid';
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
    ...overrides,
  };
}

interface FakeMessage {
  topic: string;
  partition: number;
  offset: bigint;
  value: Buffer;
}

function createIpfsPublishedMessage(
  eventId: string,
  cid: CID,
  partition: number,
  offset: bigint
): FakeMessage {
  const payload = create(TelemetryIpfsPublishedPayloadSchema, {
    cid: Buffer.from(cid.bytes),
    eventCount: 5,
    compression: TelemetryIpfsPublishedPayload_Compression.NONE,
  });

  const envelope = create(EnvelopeSchema, {
    eventId,
    eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
    eventVersion: '1.0.0',
    occurredAt: '2026-01-01T00:00:00Z',
    source: 'ipfs-publisher',
    payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
  });

  return {
    topic: TELEMETRY_TOPICS.IPFS_PUBLISHED,
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
 * A fake ApiPromise whose `cps.payload` storage mirrors the last CID
 * submitted via `cps.setPayload`, simulating authoritative on-chain state.
 */
function createFakeApi(): {
  api: ApiPromise;
  setPayloadCalls: { nodeId: number; cid: number[] }[];
} {
  const setPayloadCalls: { nodeId: number; cid: number[] }[] = [];
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
        setPayload(nodeId: number, cid: number[]) {
          setPayloadCalls.push({ nodeId, cid });
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
                anchored = new Uint8Array(cid);
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

describe('blockchain-anchor idempotency (issue #27)', () => {
  it('does not resubmit a duplicate Kafka delivery once the CID is anchored on-chain', async () => {
    const cid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

    // Same event delivered twice (e.g. after a crash before the Kafka
    // commit landed): both messages carry the same CID.
    const messages = [
      createIpfsPublishedMessage('evt-1', cid, 0, 0n),
      createIpfsPublishedMessage('evt-1', cid, 0, 1n),
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

  it('anchors distinct CIDs independently without treating them as duplicates', async () => {
    const cidA = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');
    const cidB = CID.parse(
      'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi'
    );

    const messages = [
      createIpfsPublishedMessage('evt-a', cidA, 0, 0n),
      createIpfsPublishedMessage('evt-b', cidB, 0, 1n),
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
});
