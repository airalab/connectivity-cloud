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
import type {
  RegistryReader,
  SensorRegistryRecord,
} from '../src/registry-reader.js';

function toHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

/** In-memory RegistryReader for tests, seeded with known sensor records. */
export class InMemoryRegistryReader implements RegistryReader {
  private readonly sensors = new Map<string, SensorRegistryRecord>();
  private readonly seenNonces = new Set<string>();

  constructor(seed: SensorRegistryRecord[] = []) {
    seed.forEach((record) => {
      this.sensors.set(toHex(record.sensorId), record);
    });
  }

  async authenticate(sensorId: Uint8Array): Promise<boolean> {
    const record = await this.getSensorRecord(sensorId);
    return record !== null && record.enabled;
  }

  async getSensorRecord(
    sensorId: Uint8Array
  ): Promise<SensorRegistryRecord | null> {
    return this.sensors.get(toHex(sensorId)) ?? null;
  }

  async isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    return this.seenNonces.has(`${toHex(sensorId)}:${toHex(nonce)}`);
  }

  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    this.seenNonces.add(`${toHex(sensorId)}:${toHex(nonce)}`);
  }
}
