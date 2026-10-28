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

import { createHash, randomBytes } from 'node:crypto';
import type { Redis } from 'ioredis';
import { NonceStoreUnavailableError } from '@scp/core';
import type { NonceStore } from '@scp/core';
import { parsePositiveInt } from './env.js';

export const DEFAULT_NONCE_PREFIX = 'nonce:v1';
export const DEFAULT_NONCE_TTL_SECONDS = 900;
export const DEFAULT_TIMESTAMP_SKEW_SECONDS = 300;
/** Safety margin for clock drift and processing time beyond the accepted window. */
export const NONCE_TTL_MARGIN_SECONDS = 60;

export interface NonceConfig {
  prefix: string;
  ttlSeconds: number;
}

/**
 * Minimal Redis surface required by {@link RedisNonceStore}.
 */
export interface NonceRedis {
  set(
    key: string,
    value: string,
    expiryMode: 'EX',
    seconds: number,
    setMode: 'NX'
  ): Promise<'OK' | null>;
}

/**
 * Minimum TTL that covers every moment at which the same signed envelope
 * could still pass timestamp validation: the accepted window is
 * `[ts - skew, ts + skew]`, i.e. twice the skew, plus a safety margin.
 */
export function minimumNonceTtlSeconds(timestampSkewSeconds: number): number {
  return 2 * timestampSkewSeconds + NONCE_TTL_MARGIN_SECONDS;
}

/**
 * Loads and validates nonce settings.
 *
 * Environment variables:
 * - NONCE_REDIS_PREFIX (default `nonce:v1`)
 * - NONCE_TTL_SECONDS (default 900)
 * - ENDPOINT_TIMESTAMP_SKEW_SECONDS (default 300), used for validation
 *
 * @throws Error when the TTL does not cover the timestamp acceptance window
 */
export function loadNonceConfig(
  env: NodeJS.ProcessEnv = process.env
): NonceConfig {
  const prefix = env.NONCE_REDIS_PREFIX?.trim() || DEFAULT_NONCE_PREFIX;
  const ttlSeconds = parsePositiveInt(
    env.NONCE_TTL_SECONDS,
    DEFAULT_NONCE_TTL_SECONDS
  );
  const skew = parsePositiveInt(
    env.ENDPOINT_TIMESTAMP_SKEW_SECONDS,
    DEFAULT_TIMESTAMP_SKEW_SECONDS
  );
  assertNonceTtl(ttlSeconds, skew);
  return { prefix, ttlSeconds };
}

/**
 * @throws Error if `ttlSeconds` is shorter than the timestamp acceptance window
 */
export function assertNonceTtl(
  ttlSeconds: number,
  timestampSkewSeconds: number
): void {
  const minimum = minimumNonceTtlSeconds(timestampSkewSeconds);
  if (ttlSeconds < minimum) {
    throw new Error(
      `NONCE_TTL_SECONDS (${ttlSeconds}) must be at least ${minimum} (2 x ENDPOINT_TIMESTAMP_SKEW_SECONDS + ${NONCE_TTL_MARGIN_SECONDS}s margin)`
    );
  }
}

function lengthPrefixed(bytes: Uint8Array): Buffer {
  const length = Buffer.alloc(4);
  length.writeUInt32BE(bytes.length);
  return Buffer.concat([length, bytes]);
}

/**
 * Derives the Redis key for a claim. The hash input is domain separated and
 * length-prefixed so `(sensor, nonce)` pairs can never collide through
 * concatenation ambiguity, and raw nonces never appear in Redis keys or logs.
 */
export function deriveNonceKey(
  prefix: string,
  sensorId: Uint8Array,
  nonce: Uint8Array
): string {
  const digest = createHash('sha256')
    .update('scp.nonce.v1')
    .update(lengthPrefixed(sensorId))
    .update(lengthPrefixed(nonce))
    .digest('hex');
  return `${prefix}:${digest}`;
}

/**
 * Redis-backed nonce store shared by all endpoint instances.
 *
 * Uses `SET key token NX EX ttl`, so exactly one concurrent caller across all
 * instances obtains a claim. Claims are never deleted: after an ambiguous
 * Kafka outcome the claim must survive, and expiry is handled by the TTL.
 */
export class RedisNonceStore implements NonceStore {
  constructor(
    private readonly redis: NonceRedis,
    private readonly config: NonceConfig,
    private readonly onClose: () => Promise<void> = async () => undefined
  ) {}

  async claim(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    const key = deriveNonceKey(this.config.prefix, sensorId, nonce);
    const token = randomBytes(16).toString('hex');
    let result: unknown;
    try {
      result = await this.redis.set(
        key,
        token,
        'EX',
        this.config.ttlSeconds,
        'NX'
      );
    } catch (error) {
      throw new NonceStoreUnavailableError('nonce store request failed', {
        cause: error,
      });
    }
    if (result === 'OK') return true;
    if (result === null) return false;
    throw new NonceStoreUnavailableError('unexpected nonce store reply');
  }

  async close(): Promise<void> {
    await this.onClose();
  }
}

/** Creates a {@link RedisNonceStore} that closes the given client on `close()`. */
export function createRedisNonceStore(
  redis: Redis,
  config: NonceConfig,
  closeClient: (client: Redis) => Promise<void>
): RedisNonceStore {
  return new RedisNonceStore(redis as unknown as NonceRedis, config, () =>
    closeClient(redis)
  );
}
