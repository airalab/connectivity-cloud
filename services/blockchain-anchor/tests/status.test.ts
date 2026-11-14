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
import type { ApiPromise } from '@polkadot/api';
import type { Admin, Consumer, Producer } from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import {
  createBlockchainAnchorService,
  getQueuedBatchCount,
} from '../src/index.js';
import type { BlockchainAnchorConfig } from '../src/config.js';

const config: BlockchainAnchorConfig = {
  kafkaBrokers: ['localhost:9092'],
  consumerGroupId: 'blockchain-anchor-v1',
  substrateWsUrl: 'ws://localhost:9944',
  suri: '//Alice',
  nodeId: 0,
  healthPort: 3050,
  maxPayloadBytes: 8192,
  source: 'blockchain-anchor',
};

describe('blockchain status observation', () => {
  it('calculates read-only Kafka backlog across partitions and preserves missing offsets', async () => {
    let queries = 0;
    const admin = {
      async metadata() {
        return {
          topics: new Map([
            ['telemetry.batched.v1', { partitions: [{}, {}, {}] }],
          ]),
        };
      },
      async listOffsets() {
        queries += 1;
        return [
          {
            name: 'telemetry.batched.v1',
            partitions: [
              { partitionIndex: 0, offset: 10n },
              { partitionIndex: 1, offset: 20n },
              { partitionIndex: 2, offset: 7n },
            ],
          },
        ];
      },
      async listConsumerGroupOffsets() {
        queries += 1;
        return [
          {
            groupId: 'anchor-group',
            topics: [
              {
                name: 'telemetry.batched.v1',
                partitions: [
                  { partitionIndex: 0, committedOffset: 8n },
                  { partitionIndex: 1, committedOffset: 25n },
                  { partitionIndex: 2, committedOffset: 2n },
                ],
              },
            ],
          },
        ];
      },
    };

    expect(
      await getQueuedBatchCount(admin, 'anchor-group', 'telemetry.batched.v1')
    ).toBe(7);
    expect(queries).toBe(2);
    expect(
      await getQueuedBatchCount(
        {
          ...admin,
          async listConsumerGroupOffsets() {
            return [];
          },
        },
        'anchor-group',
        'telemetry.batched.v1'
      )
    ).toBeNull();
  });

  it('preserves balance precision and reports best/finalized heights independently', async () => {
    let releaseClose!: () => void;
    const closed = new Promise<void>((resolve) => {
      releaseClose = resolve;
    });
    const consumer = {
      async consume() {
        return (async function* () {
          await closed;
        })();
      },
      async close() {
        releaseClose();
      },
    } as unknown as Consumer;
    let getStatus:
      | (() => Promise<{
          freeBalance: string | null;
          tokenSymbol: string | null;
          tokenDecimals: number | null;
          bestBlock: string | null;
          finalizedBlock: string | null;
          finalizationGap: number | null;
        }>)
      | undefined;
    const api = {
      query: {
        system: {
          async account() {
            return { data: { free: { toString: () => '1234567890123456' } } };
          },
        },
      },
      rpc: {
        chain: {
          async getHeader(hash?: string) {
            return { number: BigInt(hash ? '998' : '1000') };
          },
          async getFinalizedHead() {
            return '0xfinalized';
          },
        },
      },
      registry: { chainDecimals: [12], chainTokens: ['XRT'] },
      async disconnect() {},
    } as unknown as ApiPromise;
    const service = createBlockchainAnchorService(config, {
      createConsumer: () => consumer,
      createProducer: () =>
        ({ async close() {}, async send() {} }) as unknown as Producer,
      createAdmin: () =>
        ({
          async close() {},
          async metadata() {
            return {
              topics: new Map([
                ['telemetry.batched.v1', { partitions: [{}, {}] }],
              ]),
            };
          },
          async listOffsets() {
            return [
              {
                name: 'telemetry.batched.v1',
                partitions: [
                  { partitionIndex: 0, offset: 10n },
                  { partitionIndex: 1, offset: 10n },
                ],
              },
            ];
          },
          async listConsumerGroupOffsets() {
            return [
              {
                groupId: 'blockchain-anchor-v1',
                topics: [
                  {
                    name: 'telemetry.batched.v1',
                    partitions: [
                      { partitionIndex: 0, committedOffset: 10n },
                      { partitionIndex: 1, committedOffset: 10n },
                    ],
                  },
                ],
              },
            ];
          },
        }) as unknown as Admin,
      createStatusRedis: async () =>
        ({
          async hgetall() {
            return {};
          },
          async eval() {
            return 1;
          },
          async quit() {
            return 'OK';
          },
        }) as unknown as Awaited<
          ReturnType<typeof import('@scp/infra').createRedisClient>
        >,
      createApi: async () => api,
      createHealthServer: (_getMetrics, _port, status) => {
        getStatus = status;
        return {
          close(callback: (error?: Error) => void) {
            callback();
          },
        } as unknown as import('node:http').Server;
      },
    });

    await service.start();
    const snapshot = await getStatus!();
    expect(snapshot).toMatchObject({
      freeBalance: '1234.567890123456',
      tokenSymbol: 'XRT',
      tokenDecimals: 12,
      bestBlock: '1000',
      finalizedBlock: '998',
      finalizationGap: 2,
    });
    await service.stop();
  });
});
