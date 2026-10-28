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
interface Entry<V> {
  value: V;
  /** Absolute expiry in clock milliseconds; `undefined` means never. */
  expiresAt: number | undefined;
}

/**
 * Bounded least-recently-used cache with optional per-entry expiry.
 *
 * Memory usage is capped at `maxSize` entries; the least recently used entry
 * is evicted on overflow.
 */
export class LruCache<K, V> {
  private readonly entries = new Map<K, Entry<V>>();

  constructor(
    private readonly maxSize: number,
    private readonly now: () => number = Date.now
  ) {
    if (!Number.isInteger(maxSize) || maxSize <= 0) {
      throw new Error('LruCache maxSize must be a positive integer');
    }
  }

  get size(): number {
    return this.entries.size;
  }

  /** Returns the live value for `key` (marking it recently used), if any. */
  get(key: K): V | undefined {
    const entry = this.entries.get(key);
    if (entry === undefined) {
      return undefined;
    }
    if (entry.expiresAt !== undefined && entry.expiresAt <= this.now()) {
      this.entries.delete(key);
      return undefined;
    }
    this.entries.delete(key);
    this.entries.set(key, entry);
    return entry.value;
  }

  /** Stores `value`; `ttlMs` omitted means the entry never expires. */
  set(key: K, value: V, ttlMs?: number): void {
    this.entries.delete(key);
    this.entries.set(key, {
      value,
      expiresAt: ttlMs === undefined ? undefined : this.now() + ttlMs,
    });
    if (this.entries.size > this.maxSize) {
      const oldest = this.entries.keys().next();
      if (!oldest.done) {
        this.entries.delete(oldest.value);
      }
    }
  }
}
