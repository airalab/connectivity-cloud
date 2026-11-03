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
 * Shared replay-protection store.
 *
 * Authorization (who may send) and replay protection (has this exact signed
 * message been seen) are deliberately separate: every sensor authentication
 * strategy shares one `NonceStore` implementation.
 */
export interface NonceStore {
  /**
   * Atomically claims a `(sensorId, nonce)` pair.
   *
   * @param sensorId - Raw sensor public key bytes
   * @param nonce - Raw nonce bytes from the signed envelope
   * @returns true iff this call newly claimed the pair; false if it was already claimed
   * @throws {NonceStoreUnavailableError} when the outcome cannot be determined
   */
  claim(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean>;
  /** Releases underlying connections. */
  close(): Promise<void>;
}

/**
 * Raised when the nonce store cannot give a definite answer (connection
 * failure, timeout, unexpected reply). Callers must fail closed.
 */
export class NonceStoreUnavailableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'NonceStoreUnavailableError';
  }
}
