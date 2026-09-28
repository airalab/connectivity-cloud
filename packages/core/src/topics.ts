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
export const TELEMETRY_TOPICS = Object.freeze({
  AUTHORIZED: 'telemetry.authorized.v1',
  REJECTED: 'telemetry.rejected.v1',
  BATCHED: 'telemetry.batched.v1',
  DLQ: 'telemetry.dlq.v1',
} as const);

export type TelemetryTopic =
  (typeof TELEMETRY_TOPICS)[keyof typeof TELEMETRY_TOPICS];
