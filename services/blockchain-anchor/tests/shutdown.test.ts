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
    healthPort: 3052,
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

/** A fake consumer that yields the given messages then stays open until closed. */
function createFakeConsumer(messages: FakeMessage[]): { consumer: Consumer } {
  let releaseClose!: () => void;
  const closed = new Promise<void>((resolve) => {
    releaseClose = resolve;
  });

  const fake = {
    async consume() {
      return (async function* () {
        for (const message of messages) {
          yield message;
        }
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

describe('blockchain-anchor graceful shutdown (issue #32)', () => {
  it('does not disconnect the chain API until the in-flight extrinsic finishes', async () => {
    const cid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');
    const messages = [createIpfsPublishedMessage('evt-1', cid, 0, 0n)];
    const { consumer } = createFakeConsumer(messages);

    const events: string[] = [];
    let releaseExtrinsic!: () => void;
    const extrinsicGate = new Promise<void>((resolve) => {
      releaseExtrinsic = resolve;
    });

    const api = {
      isReady: Promise.resolve(),
      query: {
        cps: {
          async payload() {
            return { isEmpty: true, isSome: false };
          },
        },
      },
      tx: {
        cps: {
          setPayload() {
            return {
              async signAndSend(
                _account: unknown,
                callback: (result: unknown) => void
              ) {
                events.push('extrinsic:submitted');
                // Hold the extrinsic "in flight" until the test releases it,
                // simulating a SIGTERM arriving mid-submission.
                await extrinsicGate;
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
                events.push('extrinsic:finalized');
                return () => {};
              },
            };
          },
        },
      },
      events: {},
      async disconnect() {
        events.push('api:disconnect');
      },
    } as unknown as ApiPromise;

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

    // Let the message reach the point where the extrinsic is submitted and
    // is being held open by `extrinsicGate`.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(events).toEqual(['extrinsic:submitted']);

    // Trigger shutdown while the extrinsic is still in flight. `stop()`
    // must wait for the extrinsic to finish before disconnecting the API.
    const stopPromise = service.stop();

    // Give stop() a chance to (incorrectly) race ahead if the bug regresses.
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(events).toEqual(['extrinsic:submitted']);

    releaseExtrinsic();
    await stopPromise;

    expect(events).toEqual([
      'extrinsic:submitted',
      'extrinsic:finalized',
      'api:disconnect',
    ]);
  });
});
