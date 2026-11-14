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
import { closeRedisClient, createRedisClient } from '@scp/infra';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { STORE_FINALIZED_OBSERVATION_SCRIPT } from '../src/index.js';

// Executes the real Lua script, so it needs a Redis server:
// REDIS_TEST_URL=redis://127.0.0.1:6379 pnpm --filter @scp/blockchain-anchor test
const redisUrl = process.env.REDIS_TEST_URL;
const key = 'blockchain-anchor:test:last-finalized';

describe.skipIf(!redisUrl)('finalized observation Redis script', () => {
  let redis: Awaited<ReturnType<typeof createRedisClient>>;

  beforeAll(async () => {
    redis = await createRedisClient({
      url: redisUrl!,
      connectTimeoutMs: 2000,
      commandTimeoutMs: 2000,
      allowInsecure: true,
    });
  });

  beforeEach(async () => {
    await redis.del(key);
  });

  afterAll(async () => {
    await redis.del(key);
    await closeRedisClient(redis);
  });

  const store = (block: number, batchId: string, extrinsicIndex: number) =>
    redis.eval(
      STORE_FINALIZED_OBSERVATION_SCRIPT,
      1,
      key,
      String(block),
      String(block),
      `0x${block}`,
      batchId,
      '2026-11-11T09:00:00.000Z',
      String(extrinsicIndex)
    );

  it('keeps the later of two batches finalized in the same block', async () => {
    expect(await store(100, 'first', 1)).toBe(1);
    expect(await store(100, 'second', 2)).toBe(1);
    expect((await redis.hgetall(key)).lastBatchId).toBe('second');
  });

  it('does not let an earlier extrinsic in the same block overwrite a later one', async () => {
    await store(100, 'second', 2);
    expect(await store(100, 'first', 1)).toBe(0);
    expect((await redis.hgetall(key)).lastBatchId).toBe('second');
  });

  it('never overwrites a newer block with an older observation', async () => {
    await store(1000, 'newer', 0);
    expect(await store(999, 'older', 5)).toBe(0);
    expect(await store(99, 'much-older', 5)).toBe(0);
    expect((await redis.hgetall(key)).lastBatchId).toBe('newer');
  });

  it('advances to a higher block and tolerates legacy records without an index', async () => {
    await redis.hset(key, { lastFinalizedBlockNumber: '100' });
    expect(await store(100, 'same-block', -1)).toBe(1);
    expect(await store(101, 'next', 0)).toBe(1);
    expect((await redis.hgetall(key)).lastBatchId).toBe('next');
  });
});
