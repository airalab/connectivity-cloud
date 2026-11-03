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

/**
 * "None" sensor authentication strategy: every sensor is authorized, so only
 * the envelope signature, timestamp and shared nonce checks gate ingestion.
 */
export class OpenAuth implements SensorAuth {
  async authenticate(_sensorId: Uint8Array): Promise<boolean> {
    return true;
  }
}
