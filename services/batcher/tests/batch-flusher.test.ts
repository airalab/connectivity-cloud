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
import { describe, expect, it, vi } from 'vitest';
import { createBatchFlusher } from '../src/batch-flusher.js';

/** Deferred promise helper for controlling async timing in tests. */
function deferred<T = void>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
} {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('createBatchFlusher', () => {
  it('does nothing when flushing an empty batch', async () => {
    const onFlush = vi.fn(async () => undefined);
    const flusher = createBatchFlusher<number>(onFlush);

    await flusher.flush();

    expect(onFlush).not.toHaveBeenCalled();
    expect(flusher.size()).toBe(0);
  });

  it('detaches the batch before async work so new items form a new batch', async () => {
    const gate = deferred();
    const flushed: number[][] = [];
    const flusher = createBatchFlusher<number>(async (batch) => {
      await gate.promise;
      flushed.push([...batch]);
    });

    flusher.add(1);
    flusher.add(2);

    const flushPromise = flusher.flush();
    // Batch was detached synchronously.
    expect(flusher.size()).toBe(0);
    expect(flusher.isFlushing()).toBe(true);

    // Items added during the flush accumulate into the next batch.
    flusher.add(3);
    expect(flusher.size()).toBe(1);

    gate.resolve();
    await flushPromise;

    expect(flushed).toEqual([[1, 2]]);
    expect(flusher.isFlushing()).toBe(false);
    expect(flusher.size()).toBe(1);
  });

  it('serializes overlapping timer and consumer triggered flushes onto one run', async () => {
    const gate = deferred();
    const onFlush = vi.fn(async (batch: readonly number[]) => {
      await gate.promise;
      void batch;
    });
    const flusher = createBatchFlusher<number>(onFlush);

    flusher.add(1);
    flusher.add(2);

    // Two concurrent flush triggers (e.g. size-based + timer-based).
    const first = flusher.flush();
    const second = flusher.flush();

    // Both callers observe the same in-flight flush; handler runs once.
    expect(onFlush).toHaveBeenCalledTimes(1);

    gate.resolve();
    await Promise.all([first, second]);

    expect(onFlush).toHaveBeenCalledTimes(1);
    expect(onFlush).toHaveBeenCalledWith([1, 2]);
  });

  it('does not publish the same batch twice across overlapping flushes', async () => {
    const gate = deferred();
    const published: number[][] = [];
    const flusher = createBatchFlusher<number>(async (batch) => {
      await gate.promise;
      published.push([...batch]);
    });

    flusher.add(10);
    flusher.add(20);

    const timerFlush = flusher.flush();
    const consumerFlush = flusher.flush();

    gate.resolve();
    await Promise.all([timerFlush, consumerFlush]);

    // The batch [10, 20] must be published exactly once.
    expect(published).toEqual([[10, 20]]);
  });

  it('re-attaches the batch on failure without losing newly added items', async () => {
    const gate = deferred();
    const attempts: number[][] = [];
    let shouldFail = true;
    const flusher = createBatchFlusher<number>(async (batch) => {
      await gate.promise;
      attempts.push([...batch]);
      if (shouldFail) {
        throw new Error('publish failed');
      }
    });

    flusher.add(1);
    flusher.add(2);

    const failing = flusher.flush();
    // Item arrives while the (failing) flush is in progress.
    flusher.add(3);

    gate.resolve();
    await expect(failing).rejects.toThrow('publish failed');

    // Failed batch is re-attached ahead of the newly added item.
    expect(flusher.size()).toBe(3);
    expect(flusher.isFlushing()).toBe(false);

    // Next flush retries the full batch and succeeds.
    shouldFail = false;
    await flusher.flush();

    expect(attempts).toEqual([
      [1, 2],
      [1, 2, 3],
    ]);
    expect(flusher.size()).toBe(0);
  });

  it('propagates handler errors to all joined callers', async () => {
    const flusher = createBatchFlusher<number>(async () => {
      throw new Error('boom');
    });

    flusher.add(1);

    const a = flusher.flush();
    const b = flusher.flush();

    await expect(a).rejects.toThrow('boom');
    await expect(b).rejects.toThrow('boom');
  });
});
