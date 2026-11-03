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
import { describe, expect, it } from 'vitest';
import { NonceStoreUnavailableError } from '@scp/core';
import {
  RedisNonceStore,
  assertNonceTtl,
  deriveNonceKey,
  loadNonceConfig,
  minimumNonceTtlSeconds,
  type NonceRedis,
} from '../src/index.js';

/** Fake with real `SET NX EX` semantics, including expiry on a controllable clock. */
class FakeRedis implements NonceRedis {
  readonly entries = new Map<string, { value: string; expiresAt: number }>();
  now = 0;
  failWith: Error | undefined;
  reply: unknown;

  async set(
    key: string,
    value: string,
    _mode: 'EX',
    seconds: number,
    _nx: 'NX'
  ): Promise<'OK' | null> {
    await Promise.resolve();
    if (this.failWith) throw this.failWith;
    if (this.reply !== undefined) return this.reply as 'OK' | null;
    const existing = this.entries.get(key);
    if (existing && existing.expiresAt > this.now) return null;
    this.entries.set(key, { value, expiresAt: this.now + seconds * 1000 });
    return 'OK';
  }
}

const config = { prefix: 'nonce:v1', ttlSeconds: 900 };
const sensor = Uint8Array.from(Buffer.alloc(32, 7));
const nonce = Uint8Array.from(Buffer.alloc(16, 1));

describe('deriveNonceKey', () => {
  it('is deterministic, prefixed and never contains raw nonce bytes', () => {
    const key = deriveNonceKey('nonce:v1', sensor, nonce);
    expect(key).toBe(deriveNonceKey('nonce:v1', sensor, nonce));
    expect(key).toMatch(/^nonce:v1:[0-9a-f]{64}$/);
    expect(key).not.toContain(Buffer.from(nonce).toString('hex'));
  });

  it('separates sensors and nonces and avoids concatenation ambiguity', () => {
    const a = deriveNonceKey('p', Buffer.from('ab'), Buffer.from('c'));
    const b = deriveNonceKey('p', Buffer.from('a'), Buffer.from('bc'));
    expect(a).not.toBe(b);
    expect(deriveNonceKey('p', sensor, nonce)).not.toBe(
      deriveNonceKey('p', Uint8Array.from(Buffer.alloc(32, 8)), nonce)
    );
  });
});

describe('RedisNonceStore', () => {
  it('claims once and rejects the replay', async () => {
    const store = new RedisNonceStore(new FakeRedis(), config);
    expect(await store.claim(sensor, nonce)).toBe(true);
    expect(await store.claim(sensor, nonce)).toBe(false);
  });

  it('keeps different nonces and different sensors independent', async () => {
    const store = new RedisNonceStore(new FakeRedis(), config);
    expect(await store.claim(sensor, nonce)).toBe(true);
    expect(await store.claim(sensor, Uint8Array.from([9]))).toBe(true);
    expect(await store.claim(Uint8Array.from([5]), nonce)).toBe(true);
  });

  it('grants exactly one claim to simultaneous requests', async () => {
    const store = new RedisNonceStore(new FakeRedis(), config);
    const results = await Promise.all(
      Array.from({ length: 20 }, () => store.claim(sensor, nonce))
    );
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('shares claims between store instances using the same Redis', async () => {
    const redis = new FakeRedis();
    const first = new RedisNonceStore(redis, config);
    const second = new RedisNonceStore(redis, config);
    expect(await first.claim(sensor, nonce)).toBe(true);
    expect(await second.claim(sensor, nonce)).toBe(false);
  });

  it('stores a random token with the configured TTL', async () => {
    const redis = new FakeRedis();
    const store = new RedisNonceStore(redis, config);
    await store.claim(sensor, nonce);
    await store.claim(sensor, Uint8Array.from([2]));
    const [one, two] = [...redis.entries.values()];
    expect(one?.expiresAt).toBe(900_000);
    expect(one?.value).toMatch(/^[0-9a-f]{32}$/);
    expect(one?.value).not.toBe(two?.value);
  });

  it('fails closed with NonceStoreUnavailableError on Redis errors', async () => {
    const redis = new FakeRedis();
    redis.failWith = new Error('ECONNREFUSED');
    const store = new RedisNonceStore(redis, config);
    await expect(store.claim(sensor, nonce)).rejects.toBeInstanceOf(
      NonceStoreUnavailableError
    );
  });

  it('fails closed on an unexpected SET reply', async () => {
    const redis = new FakeRedis();
    redis.reply = 'QUEUED';
    const store = new RedisNonceStore(redis, config);
    await expect(store.claim(sensor, nonce)).rejects.toBeInstanceOf(
      NonceStoreUnavailableError
    );
  });

  it('never exposes the raw nonce in errors', async () => {
    const redis = new FakeRedis();
    redis.failWith = new Error('boom');
    const store = new RedisNonceStore(redis, config);
    const error = await store.claim(sensor, nonce).catch((e: Error) => e);
    expect(String((error as Error).message)).not.toContain('01010101');
  });

  it('runs the close callback', async () => {
    let closed = false;
    const store = new RedisNonceStore(new FakeRedis(), config, async () => {
      closed = true;
    });
    await store.close();
    expect(closed).toBe(true);
  });
});

describe('nonce TTL configuration', () => {
  it('defaults to nonce:v1 and 900 seconds', () => {
    expect(loadNonceConfig({})).toEqual({
      prefix: 'nonce:v1',
      ttlSeconds: 900,
    });
  });

  it('requires the TTL to cover twice the skew plus a margin', () => {
    expect(minimumNonceTtlSeconds(300)).toBe(660);
    expect(() => assertNonceTtl(659, 300)).toThrow(/at least 660/);
    expect(() => assertNonceTtl(660, 300)).not.toThrow();
  });

  it('rejects a TTL shorter than the timestamp acceptance window', () => {
    expect(() =>
      loadNonceConfig({
        NONCE_TTL_SECONDS: '300',
        ENDPOINT_TIMESTAMP_SKEW_SECONDS: '300',
      })
    ).toThrow(/NONCE_TTL_SECONDS/);
  });

  it('honours a custom prefix and a larger skew', () => {
    expect(
      loadNonceConfig({
        NONCE_REDIS_PREFIX: 'replay:v2',
        NONCE_TTL_SECONDS: '2000',
        ENDPOINT_TIMESTAMP_SKEW_SECONDS: '900',
      })
    ).toEqual({ prefix: 'replay:v2', ttlSeconds: 2000 });
  });
});
