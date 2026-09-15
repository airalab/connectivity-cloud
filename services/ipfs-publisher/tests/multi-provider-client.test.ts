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
import { describe, expect, it, vi, beforeEach, afterEach } from 'vitest';
import { MultiProviderIpfsClient } from '../src/multi-provider-client.js';
import type { IpfsProvider } from '../src/providers/types.js';

function createFakeProvider(
  name: string,
  behavior: {
    failFirstNCalls?: number;
    alwaysFail?: boolean;
    cid?: string;
  } = {}
): IpfsProvider & { addCalls: number } {
  const state = {
    addCalls: 0,
  };
  const failFirstNCalls = behavior.failFirstNCalls ?? 0;

  return {
    name,
    get addCalls() {
      return state.addCalls;
    },
    async start() {},
    async stop() {},
    async add(): Promise<string> {
      state.addCalls += 1;
      if (behavior.alwaysFail || state.addCalls <= failFirstNCalls) {
        throw new Error(`${name} unavailable`);
      }
      return behavior.cid ?? `cid-from-${name}`;
    },
  };
}

describe('MultiProviderIpfsClient', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('throws when constructed with no providers', () => {
    expect(
      () =>
        new MultiProviderIpfsClient([], {
          durability: { policy: 'any', minSuccessCount: 1 },
          retryBaseDelayMs: 1000,
          retryMaxDelayMs: 5000,
          maxPendingReplications: 10,
        })
    ).toThrow();
  });

  it('publishes to primary provider and reports durability satisfied for "any"', async () => {
    const primary = createFakeProvider('primary');
    const fallback = createFakeProvider('fallback');
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'any', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    const result = await client.publish(
      'batch-1',
      new Uint8Array([1, 2, 3]),
      false
    );

    expect(result.durabilitySatisfied).toBe(true);
    expect(result.cid).toBe('cid-from-primary');
    expect(primary.addCalls).toBe(1);
    expect(fallback.addCalls).toBe(1);

    await client.stop();
  });

  it('falls back when primary is down, satisfying "any" durability', async () => {
    const primary = createFakeProvider('primary', { alwaysFail: true });
    const fallback = createFakeProvider('fallback');
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'any', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    const result = await client.publish('batch-2', new Uint8Array([1]), false);

    expect(result.durabilitySatisfied).toBe(true);
    expect(result.cid).toBe('cid-from-fallback');
    expect(result.providerOutcomes.get('primary')?.success).toBe(false);
    expect(result.providerOutcomes.get('fallback')?.success).toBe(true);

    await client.stop();
  });

  it('throws when all providers fail', async () => {
    const primary = createFakeProvider('primary', { alwaysFail: true });
    const fallback = createFakeProvider('fallback', { alwaysFail: true });
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'any', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    await expect(
      client.publish('batch-3', new Uint8Array([1]), false)
    ).rejects.toThrow(/all IPFS providers failed/);

    await client.stop();
  });

  it('"all" policy is not satisfied when a fallback fails, but does not throw', async () => {
    const primary = createFakeProvider('primary');
    const fallback = createFakeProvider('fallback', { alwaysFail: true });
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'all', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    const result = await client.publish('batch-4', new Uint8Array([1]), false);

    expect(result.durabilitySatisfied).toBe(false);
    expect(client.getDurabilityFailureCount()).toBe(1);
    expect(client.getPendingReplicationCount()).toBe(1);

    await client.stop();
  });

  it('does not re-upload to providers that already succeeded on a retried publish call', async () => {
    const primary = createFakeProvider('primary');
    const fallback = createFakeProvider('fallback', { failFirstNCalls: 1 });
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'all', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    const first = await client.publish('batch-5', new Uint8Array([1]), false);
    expect(first.durabilitySatisfied).toBe(false);
    expect(primary.addCalls).toBe(1);
    expect(fallback.addCalls).toBe(1);

    // Simulate Kafka redelivery: publish is called again for the same batch.
    const second = await client.publish('batch-5', new Uint8Array([1]), false);
    expect(second.durabilitySatisfied).toBe(true);
    // Primary already succeeded, so it must not be called again.
    expect(primary.addCalls).toBe(1);
    // Fallback had failed, so it is retried and now succeeds.
    expect(fallback.addCalls).toBe(2);

    await client.stop();
  });

  it('background retry loop eventually replicates to a recovered fallback', async () => {
    const primary = createFakeProvider('primary');
    const fallback = createFakeProvider('fallback', { failFirstNCalls: 1 });
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'any', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    const result = await client.publish('batch-6', new Uint8Array([1]), false);
    expect(result.durabilitySatisfied).toBe(true);
    expect(client.getPendingReplicationCount()).toBe(1);
    expect(fallback.addCalls).toBe(1);

    // Advance past the retry interval so the background loop retries the
    // failed fallback provider independently of any Kafka redelivery.
    await vi.advanceTimersByTimeAsync(1100);

    expect(fallback.addCalls).toBe(2);
    expect(client.getPendingReplicationCount()).toBe(0);

    await client.stop();
  });

  it('tracks per-provider success/failure stats', async () => {
    const primary = createFakeProvider('primary');
    const fallback = createFakeProvider('fallback', { alwaysFail: true });
    const client = new MultiProviderIpfsClient([primary, fallback], {
      durability: { policy: 'any', minSuccessCount: 1 },
      retryBaseDelayMs: 1000,
      retryMaxDelayMs: 5000,
      maxPendingReplications: 10,
    });
    await client.start();

    await client.publish('batch-7', new Uint8Array([1]), false);

    const stats = client.getProviderStats();
    expect(stats.get('primary')?.success).toBe(1);
    expect(stats.get('primary')?.failure).toBe(0);
    expect(stats.get('fallback')?.success).toBe(0);
    expect(stats.get('fallback')?.failure).toBe(1);

    await client.stop();
  });
});
