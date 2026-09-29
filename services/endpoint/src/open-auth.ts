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
import type { SensorAuth } from '@scp/core';

const DEFAULT_MAX_NONCES = 100_000;

function toHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

/**
 * "None" sensor authentication strategy: every sensor is authorized, so only
 * the envelope signature and timestamp checks gate ingestion.
 *
 * Any signer can submit, so the replay-protection nonce store is bounded and
 * evicts the oldest entries first to keep memory usage constant.
 */
export class OpenAuth implements SensorAuth {
  private readonly nonces = new Set<string>();

  constructor(private readonly maxNonces: number = DEFAULT_MAX_NONCES) {}

  async authenticate(_sensorId: Uint8Array): Promise<boolean> {
    return true;
  }

  async isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    return this.nonces.has(`${toHex(sensorId)}:${toHex(nonce)}`);
  }

  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    this.nonces.add(`${toHex(sensorId)}:${toHex(nonce)}`);
    if (this.nonces.size > this.maxNonces) {
      const oldest = this.nonces.values().next().value;
      if (oldest !== undefined) {
        this.nonces.delete(oldest);
      }
    }
  }
}
