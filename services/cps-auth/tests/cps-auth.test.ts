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
import { describe, it, expect, vi } from 'vitest';
import { randomBytes } from 'node:crypto';
import { encodeAddress } from '@polkadot/util-crypto';
import {
  CpsAuth,
  resolveRootNode,
  type CpsChainReader,
  type CpsNodeId,
  type CpsNodeInfo,
} from '../src/index.js';

const ROOT = 1n;

class FakeChain implements CpsChainReader {
  readonly identities = new Map<string, CpsNodeId>();
  readonly nodes = new Map<CpsNodeId, CpsNodeInfo>();
  failing = false;
  identityCalls = 0;
  nodeCalls = 0;

  addNode(id: CpsNodeId, parent: CpsNodeId | null): void {
    this.nodes.set(id, { parent });
  }

  bind(account: Uint8Array, id: CpsNodeId): void {
    this.identities.set(Buffer.from(account).toString('hex'), id);
  }

  async getNodeByIdentity(account: Uint8Array): Promise<CpsNodeId | null> {
    this.identityCalls++;
    if (this.failing) throw new Error('rpc down');
    return this.identities.get(Buffer.from(account).toString('hex')) ?? null;
  }

  async getNodeInfo(nodeId: CpsNodeId): Promise<CpsNodeInfo | null> {
    this.nodeCalls++;
    if (this.failing) throw new Error('rpc down');
    return this.nodes.get(nodeId) ?? null;
  }
}

function setup(options: { cacheMaxSize?: number } = {}) {
  const chain = new FakeChain();
  chain.addNode(ROOT, null);
  let now = 1_000_000;
  const auth = new CpsAuth(chain, {
    rootNodeId: ROOT,
    cacheMaxSize: options.cacheMaxSize ?? 100,
    negativeTtlSeconds: 60,
    now: () => now,
  });
  return {
    chain,
    auth,
    advance: (ms: number) => {
      now += ms;
    },
  };
}

function sensorAt(chain: FakeChain, id: CpsNodeId, parent: CpsNodeId) {
  chain.addNode(id, parent);
  const key = randomBytes(32);
  chain.bind(key, id);
  return key;
}

describe('CpsAuth traversal', () => {
  it('authorizes a direct child of the root', async () => {
    const { chain, auth } = setup();
    expect(await auth.authenticate(sensorAt(chain, 2n, ROOT))).toBe(true);
  });

  it('authorizes descendants at any depth', async () => {
    const { chain, auth } = setup();
    chain.addNode(2n, ROOT);
    for (let id = 3n; id <= 40n; id++) chain.addNode(id, id - 1n);
    const key = randomBytes(32);
    chain.bind(key, 40n);
    expect(await auth.authenticate(key)).toBe(true);
  });

  it('rejects nodes outside the sensors.social subtree', async () => {
    const { chain, auth } = setup();
    chain.addNode(10n, null);
    expect(await auth.authenticate(sensorAt(chain, 11n, 10n))).toBe(false);
  });

  it('rejects the root node itself', async () => {
    const { chain, auth } = setup();
    const key = randomBytes(32);
    chain.bind(key, ROOT);
    expect(await auth.authenticate(key)).toBe(false);
  });

  it('rejects keys with no CPS identity node', async () => {
    const { auth } = setup();
    expect(await auth.authenticate(randomBytes(32))).toBe(false);
  });

  it('terminates on parent cycles', async () => {
    const { chain, auth } = setup();
    chain.addNode(20n, 21n);
    chain.addNode(21n, 20n);
    const key = randomBytes(32);
    chain.bind(key, 20n);
    expect(await auth.authenticate(key)).toBe(false);
  });
});

describe('CpsAuth caching', () => {
  it('serves positive results from cache without TTL', async () => {
    const { chain, auth, advance } = setup();
    const key = sensorAt(chain, 2n, ROOT);
    expect(await auth.authenticate(key)).toBe(true);
    const calls = chain.identityCalls + chain.nodeCalls;

    advance(24 * 3600 * 1000);
    expect(await auth.authenticate(key)).toBe(true);
    expect(chain.identityCalls + chain.nodeCalls).toBe(calls);
  });

  it('caches negative results for 60 seconds, then re-checks', async () => {
    const { chain, auth, advance } = setup();
    const key = randomBytes(32);
    expect(await auth.authenticate(key)).toBe(false);
    const calls = chain.identityCalls;

    chain.addNode(2n, ROOT);
    chain.bind(key, 2n);

    advance(59_000);
    expect(await auth.authenticate(key)).toBe(false);
    expect(chain.identityCalls).toBe(calls);

    advance(1_000);
    expect(await auth.authenticate(key)).toBe(true);
    expect(chain.identityCalls).toBe(calls + 1);
  });

  it('evicts least recently used entries beyond the bound', async () => {
    const { chain, auth } = setup({ cacheMaxSize: 2 });
    const a = sensorAt(chain, 2n, ROOT);
    const b = sensorAt(chain, 3n, ROOT);
    const c = sensorAt(chain, 4n, ROOT);

    await auth.authenticate(a);
    await auth.authenticate(b);
    await auth.authenticate(a);
    await auth.authenticate(c);

    let calls = chain.identityCalls;
    await auth.authenticate(a);
    expect(chain.identityCalls).toBe(calls);

    calls = chain.identityCalls;
    await auth.authenticate(b);
    expect(chain.identityCalls).toBe(calls + 1);
  });

  it('coalesces concurrent lookups for the same sensor', async () => {
    const { chain, auth } = setup();
    const key = sensorAt(chain, 2n, ROOT);
    const results = await Promise.all([
      auth.authenticate(key),
      auth.authenticate(key),
      auth.authenticate(key),
    ]);
    expect(results).toEqual([true, true, true]);
    expect(chain.identityCalls).toBe(1);
  });
});

describe('CpsAuth failures', () => {
  it('denies and does not cache on RPC errors', async () => {
    const { chain, auth } = setup();
    const key = sensorAt(chain, 2n, ROOT);

    chain.failing = true;
    expect(await auth.authenticate(key)).toBe(false);

    chain.failing = false;
    expect(await auth.authenticate(key)).toBe(true);
  });

  it('does not downgrade a cached positive result on later RPC errors', async () => {
    const { chain, auth } = setup();
    const key = sensorAt(chain, 2n, ROOT);
    expect(await auth.authenticate(key)).toBe(true);

    chain.failing = true;
    expect(await auth.authenticate(key)).toBe(true);
  });
});

describe('CpsAuth nonces', () => {
  it('tracks nonces and bounds the store', async () => {
    const chain = new FakeChain();
    const auth = new CpsAuth(chain, {
      rootNodeId: ROOT,
      cacheMaxSize: 10,
      negativeTtlSeconds: 60,
      maxNonces: 2,
    });
    const sensor = randomBytes(32);
    const [n1, n2, n3] = [randomBytes(8), randomBytes(8), randomBytes(8)];

    await auth.rememberNonce(sensor, n1);
    expect(await auth.isNonceSeen(sensor, n1)).toBe(true);
    await auth.rememberNonce(sensor, n2);
    await auth.rememberNonce(sensor, n3);
    expect(await auth.isNonceSeen(sensor, n1)).toBe(false);
    expect(await auth.isNonceSeen(sensor, n3)).toBe(true);
  });
});

describe('resolveRootNode', () => {
  const address = encodeAddress(randomBytes(32), 32);

  it('returns the node id of a valid root', async () => {
    const chain = new FakeChain();
    vi.spyOn(chain, 'getNodeByIdentity').mockResolvedValue(7n);
    chain.addNode(7n, null);
    expect(await resolveRootNode(chain, address)).toBe(7n);
  });

  it('fails when the address has no node', async () => {
    const chain = new FakeChain();
    await expect(resolveRootNode(chain, address)).rejects.toThrow(
      /not the identity of any CPS node/
    );
  });

  it('fails when the node is not a root', async () => {
    const chain = new FakeChain();
    vi.spyOn(chain, 'getNodeByIdentity').mockResolvedValue(7n);
    chain.addNode(7n, 3n);
    await expect(resolveRootNode(chain, address)).rejects.toThrow(
      /expected a tree root/
    );
  });
});
