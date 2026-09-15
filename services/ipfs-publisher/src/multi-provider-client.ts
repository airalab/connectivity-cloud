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
import { logDebug, logError, logInfo, logWarn } from './logger.js';
import { isDurabilitySatisfied, type DurabilityConfig } from './durability.js';
import type { IpfsProvider } from './providers/types.js';

export interface ProviderOutcome {
  success: boolean;
  cid?: string;
  lastError?: string;
  latencyMs?: number;
}

export interface PublishResult {
  /** Canonical CID for the batch (from the first provider that succeeded). */
  cid: string;
  /** Whether the configured durability policy is currently satisfied. */
  durabilitySatisfied: boolean;
  /** Per-provider outcome as of this call. */
  providerOutcomes: ReadonlyMap<string, ProviderOutcome>;
}

export interface ProviderStat {
  success: number;
  failure: number;
  totalLatencyMs: number;
}

export interface MultiProviderClientOptions {
  durability: DurabilityConfig;
  retryBaseDelayMs: number;
  retryMaxDelayMs: number;
  maxPendingReplications: number;
}

/** Per-batch replication state, tracked so retries never re-upload to a
 * provider that already succeeded. */
interface BatchReplicationState {
  cid?: string;
  providerOutcomes: Map<string, ProviderOutcome>;
}

interface PendingReplication {
  batchId: string;
  data: Uint8Array;
  compressed: boolean;
  attempts: number;
  nextAttemptAt: number;
}

/**
 * Orchestrates publishing a batch to an ordered list of IPFS providers
 * (primary first, then fallbacks), evaluates a configurable durability
 * policy, and independently retries only the providers that failed without
 * re-uploading to providers that already succeeded.
 */
export class MultiProviderIpfsClient {
  private readonly providers: IpfsProvider[];
  private readonly options: MultiProviderClientOptions;
  private readonly batchStates = new Map<string, BatchReplicationState>();
  private readonly batchStateOrder: string[] = [];
  private readonly pending = new Map<string, PendingReplication>();
  private readonly providerStats = new Map<string, ProviderStat>();
  private durabilityFailures = 0;
  private retryTimer: NodeJS.Timeout | null = null;

  constructor(providers: IpfsProvider[], options: MultiProviderClientOptions) {
    if (providers.length === 0) {
      throw new Error('at least one IPFS provider must be configured');
    }
    this.providers = providers;
    this.options = options;
    for (const provider of providers) {
      this.providerStats.set(provider.name, {
        success: 0,
        failure: 0,
        totalLatencyMs: 0,
      });
    }
  }

  async start(): Promise<void> {
    const failures: string[] = [];
    for (const provider of this.providers) {
      try {
        await provider.start();
      } catch (error) {
        failures.push(provider.name);
        logError('IPFS provider failed to start', error, {
          provider: provider.name,
        });
      }
    }

    if (failures.length === this.providers.length) {
      throw new Error(
        `all configured IPFS providers failed to start: ${failures.join(', ')}`
      );
    }

    this.retryTimer = setInterval(() => {
      this.processPendingReplications().catch((error: unknown) => {
        logError('replication retry loop failed', error);
      });
    }, this.options.retryBaseDelayMs);
    this.retryTimer.unref?.();
  }

  async stop(): Promise<void> {
    if (this.retryTimer) {
      clearInterval(this.retryTimer);
      this.retryTimer = null;
    }
    for (const provider of this.providers) {
      await provider.stop().catch(() => undefined);
    }
  }

  /**
   * Publish `data` for `batchId`, trying the primary provider first and then
   * replicating to fallback providers. Safe to call again for the same
   * `batchId` (e.g. on redelivery): providers that already succeeded are not
   * retried here, only providers still pending.
   */
  async publish(
    batchId: string,
    data: Uint8Array,
    compressed: boolean
  ): Promise<PublishResult> {
    const state = this.getOrCreateBatchState(batchId);

    const remaining = this.providers.filter(
      (provider) => !state.providerOutcomes.get(provider.name)?.success
    );

    for (const provider of remaining) {
      await this.tryProvider(provider, batchId, data, compressed, state);
    }

    const successCount = this.countSuccesses(state);
    const durabilitySatisfied = isDurabilitySatisfied(
      this.options.durability,
      successCount,
      this.providers.length
    );

    if (successCount === 0) {
      throw new Error(
        `all IPFS providers failed for batch ${batchId}: ${this.providers
          .map(
            (p) =>
              `${p.name}=${state.providerOutcomes.get(p.name)?.lastError ?? 'unknown error'}`
          )
          .join('; ')}`
      );
    }

    if (!durabilitySatisfied) {
      this.durabilityFailures += 1;
      logWarn('durability policy not yet satisfied for batch', {
        batch_id: batchId,
        policy: this.options.durability.policy,
        success_count: successCount,
        total_providers: this.providers.length,
      });
    }

    this.schedulePendingReplication(batchId, data, compressed, state);

    return {
      cid: state.cid!,
      durabilitySatisfied,
      providerOutcomes: state.providerOutcomes,
    };
  }

  getProviderStats(): ReadonlyMap<string, ProviderStat> {
    return this.providerStats;
  }

  getPendingReplicationCount(): number {
    return this.pending.size;
  }

  getDurabilityFailureCount(): number {
    return this.durabilityFailures;
  }

  private getOrCreateBatchState(batchId: string): BatchReplicationState {
    let state = this.batchStates.get(batchId);
    if (!state) {
      state = { providerOutcomes: new Map() };
      this.batchStates.set(batchId, state);
      this.batchStateOrder.push(batchId);
      // Bound memory usage: evict oldest batch state once capacity exceeded.
      const capacity = 10000;
      if (this.batchStateOrder.length > capacity) {
        const evicted = this.batchStateOrder.shift();
        if (evicted !== undefined) {
          this.batchStates.delete(evicted);
        }
      }
    }
    return state;
  }

  private countSuccesses(state: BatchReplicationState): number {
    let count = 0;
    for (const outcome of state.providerOutcomes.values()) {
      if (outcome.success) {
        count += 1;
      }
    }
    return count;
  }

  private async tryProvider(
    provider: IpfsProvider,
    batchId: string,
    data: Uint8Array,
    compressed: boolean,
    state: BatchReplicationState
  ): Promise<void> {
    const startedAt = Date.now();
    const stats = this.providerStats.get(provider.name)!;
    try {
      const cid = await provider.add(data, compressed);
      const latencyMs = Date.now() - startedAt;
      state.providerOutcomes.set(provider.name, {
        success: true,
        cid,
        latencyMs,
      });
      if (!state.cid) {
        state.cid = cid;
      }
      stats.success += 1;
      stats.totalLatencyMs += latencyMs;
      logInfo('provider publish succeeded', {
        batch_id: batchId,
        provider: provider.name,
        cid,
        latency_ms: latencyMs,
      });
    } catch (error) {
      const latencyMs = Date.now() - startedAt;
      const message = error instanceof Error ? error.message : String(error);
      state.providerOutcomes.set(provider.name, {
        success: false,
        lastError: message,
        latencyMs,
      });
      stats.failure += 1;
      logError('provider publish failed', error, {
        batch_id: batchId,
        provider: provider.name,
        latency_ms: latencyMs,
      });
    }
  }

  private schedulePendingReplication(
    batchId: string,
    data: Uint8Array,
    compressed: boolean,
    state: BatchReplicationState
  ): void {
    const allSucceeded = this.providers.every(
      (provider) => state.providerOutcomes.get(provider.name)?.success
    );

    if (allSucceeded) {
      this.pending.delete(batchId);
      return;
    }

    if (this.pending.has(batchId)) {
      return;
    }

    if (this.pending.size >= this.options.maxPendingReplications) {
      logWarn('replication retry queue full; dropping oldest entry', {
        max_pending: this.options.maxPendingReplications,
      });
      const oldestKey = this.pending.keys().next().value;
      if (oldestKey !== undefined) {
        this.pending.delete(oldestKey);
      }
    }

    this.pending.set(batchId, {
      batchId,
      data,
      compressed,
      attempts: 0,
      nextAttemptAt: Date.now() + this.options.retryBaseDelayMs,
    });
  }

  private async processPendingReplications(): Promise<void> {
    const now = Date.now();
    for (const entry of this.pending.values()) {
      if (entry.nextAttemptAt > now) {
        continue;
      }

      const state = this.batchStates.get(entry.batchId);
      if (!state) {
        this.pending.delete(entry.batchId);
        continue;
      }

      const remaining = this.providers.filter(
        (provider) => !state.providerOutcomes.get(provider.name)?.success
      );

      if (remaining.length === 0) {
        this.pending.delete(entry.batchId);
        continue;
      }

      logDebug('retrying replication for pending providers', {
        batch_id: entry.batchId,
        providers: remaining.map((p) => p.name),
        attempt: entry.attempts + 1,
      });

      for (const provider of remaining) {
        await this.tryProvider(
          provider,
          entry.batchId,
          entry.data,
          entry.compressed,
          state
        );
      }

      const stillRemaining = this.providers.some(
        (provider) => !state.providerOutcomes.get(provider.name)?.success
      );

      if (!stillRemaining) {
        this.pending.delete(entry.batchId);
        continue;
      }

      entry.attempts += 1;
      const backoff = Math.min(
        this.options.retryBaseDelayMs * 2 ** entry.attempts,
        this.options.retryMaxDelayMs
      );
      entry.nextAttemptAt = Date.now() + backoff;
    }
  }
}
