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
 * Common interface that every IPFS storage backend must implement so the
 * multi-provider publisher can treat them interchangeably.
 */
export interface IpfsProvider {
  /** Stable identifier used in metrics, logs and configuration. */
  readonly name: string;
  start(): Promise<void>;
  stop(): Promise<void>;
  /**
   * Persist `data` to the provider's storage, returning the resulting CID.
   * `compressed` indicates whether the bytes have already been XZ compressed
   * by the caller (providers must not re-compress).
   */
  add(data: Uint8Array, compressed: boolean): Promise<string>;
}
