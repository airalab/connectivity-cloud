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
 * `any`: at least one provider must succeed.
 * `all`: every configured provider must succeed.
 * `quorum`: at least `minSuccessCount` providers must succeed.
 */
export type DurabilityPolicy = 'any' | 'all' | 'quorum';

export interface DurabilityConfig {
  policy: DurabilityPolicy;
  /** Only used when `policy === 'quorum'`. */
  minSuccessCount: number;
}

/**
 * Evaluate whether the durability requirement is satisfied given how many
 * providers succeeded out of the total configured.
 */
export function isDurabilitySatisfied(
  config: DurabilityConfig,
  successCount: number,
  totalProviders: number
): boolean {
  if (totalProviders === 0) {
    return false;
  }

  switch (config.policy) {
    case 'any':
      return successCount >= 1;
    case 'all':
      return successCount >= totalProviders;
    case 'quorum':
      return successCount >= Math.min(config.minSuccessCount, totalProviders);
    default:
      return false;
  }
}
