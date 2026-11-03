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
import type { NonceStore } from '@scp/core';

/** In-memory NonceStore for tests; `claim` is atomic within a single process. */
export class InMemoryNonceStore implements NonceStore {
  private readonly claimed = new Set<string>();

  async claim(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    const key = `${Buffer.from(sensorId).toString('hex')}:${Buffer.from(nonce).toString('hex')}`;
    if (this.claimed.has(key)) return false;
    this.claimed.add(key);
    return true;
  }

  async close(): Promise<void> {
    this.claimed.clear();
  }
}
