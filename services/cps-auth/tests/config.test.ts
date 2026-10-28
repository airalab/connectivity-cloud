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
import { describe, it, expect } from 'vitest';
import { randomBytes } from 'node:crypto';
import { encodeAddress } from '@polkadot/util-crypto';
import { loadCpsAuthConfig } from '../src/config.js';

const address = encodeAddress(randomBytes(32), 32);

describe('loadCpsAuthConfig', () => {
  it('applies defaults', () => {
    expect(loadCpsAuthConfig({ CPS_ROOT_ADDRESS: address })).toEqual({
      substrateWsUrl: 'ws://localhost:9944',
      rootAddress: address,
      cacheMaxSize: 100_000,
      negativeTtlSeconds: 60,
    });
  });

  it('reads overrides', () => {
    const config = loadCpsAuthConfig({
      CPS_ROOT_ADDRESS: address,
      SUBSTRATE_WS_URL: 'wss://node.example',
      CPS_AUTH_CACHE_MAX_SIZE: '5',
      CPS_AUTH_NEGATIVE_TTL_SECONDS: '10',
    });
    expect(config.substrateWsUrl).toBe('wss://node.example');
    expect(config.cacheMaxSize).toBe(5);
    expect(config.negativeTtlSeconds).toBe(10);
  });

  it('requires a valid root address', () => {
    expect(() => loadCpsAuthConfig({})).toThrow(/CPS_ROOT_ADDRESS is required/);
    expect(() => loadCpsAuthConfig({ CPS_ROOT_ADDRESS: 'nope' })).toThrow(
      /not a valid SS58/
    );
  });

  it('rejects non-positive numbers', () => {
    for (const value of ['0', '-1', 'abc']) {
      expect(() =>
        loadCpsAuthConfig({
          CPS_ROOT_ADDRESS: address,
          CPS_AUTH_CACHE_MAX_SIZE: value,
        })
      ).toThrow(/CPS_AUTH_CACHE_MAX_SIZE/);
    }
  });
});
