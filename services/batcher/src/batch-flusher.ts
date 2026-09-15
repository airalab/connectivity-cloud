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

/**
 * Callback invoked with the detached batch when a flush runs.
 *
 * It must fully complete the external work (e.g. publish to IPFS and commit
 * Kafka offsets) before resolving. If it rejects, the flusher re-attaches the
 * batch so it is not lost and can be retried on a later flush.
 */
export type FlushHandler<T> = (batch: readonly T[]) => Promise<void>;

/**
 * Serializes batch flushing so that at most one flush is active at a time.
 *
 * This guards against the race where multiple execution paths (batch-size / lag
 * based consumer path and the flush timer) call flush concurrently and end up
 * publishing the same batch more than once. The current batch is atomically
 * detached before any asynchronous work begins, and concurrent flush requests
 * join the in-flight flush instead of starting a new one.
 */
export interface BatchFlusher<T> {
  /** Append an item to the pending batch. */
  add(item: T): void;
  /** Number of items currently pending (not yet detached for flushing). */
  size(): number;
  /** Whether a flush is currently in progress. */
  isFlushing(): boolean;
  /**
   * Flush the pending batch.
   *
   * If a flush is already in progress, the returned promise resolves/rejects
   * with that in-flight flush rather than starting a new one. If the batch is
   * empty, resolves immediately. On handler failure the batch is re-attached
   * (preserving any items added while the flush was running) and the error is
   * re-thrown.
   */
  flush(): Promise<void>;
}

/**
 * Create a {@link BatchFlusher} that serializes flushes via a single-flight
 * promise and atomically detaches the batch before asynchronous work.
 *
 * @param onFlush Handler that performs the external work for a detached batch.
 */
export function createBatchFlusher<T>(
  onFlush: FlushHandler<T>
): BatchFlusher<T> {
  let currentBatch: T[] = [];
  let flushPromise: Promise<void> | null = null;

  return {
    add(item: T): void {
      currentBatch.push(item);
    },
    size(): number {
      return currentBatch.length;
    },
    isFlushing(): boolean {
      return flushPromise !== null;
    },
    flush(): Promise<void> {
      // Single-flight: join the in-progress flush instead of starting a new one.
      if (flushPromise !== null) {
        return flushPromise;
      }

      if (currentBatch.length === 0) {
        return Promise.resolve();
      }

      // Atomically detach the batch before any asynchronous work so that a
      // concurrent flush cannot observe and publish the same items.
      const batchToPublish = currentBatch;
      currentBatch = [];

      flushPromise = (async () => {
        try {
          await onFlush(batchToPublish);
        } catch (error) {
          // Re-attach the failed batch ahead of any items that arrived while the
          // flush was running, so nothing is silently lost.
          currentBatch = [...batchToPublish, ...currentBatch];
          throw error;
        } finally {
          flushPromise = null;
        }
      })();

      return flushPromise;
    },
  };
}
