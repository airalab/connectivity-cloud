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

/** Authorization record for a sensor as seen by the endpoint. */
export interface SensorRegistryRecord {
  sensorId: Uint8Array;
  enabled: boolean;
}

/** Sensor authorization plus replay-protection (nonce) storage. */
export interface RegistryReader extends SensorAuth {
  getSensorRecord(sensorId: Uint8Array): Promise<SensorRegistryRecord | null>;
  isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean>;
  rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void>;
}
