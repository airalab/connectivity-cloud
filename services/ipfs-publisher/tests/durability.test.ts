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
import { describe, expect, it } from 'vitest';
import { isDurabilitySatisfied } from '../src/durability.js';

describe('isDurabilitySatisfied', () => {
  it('"any" is satisfied by a single success', () => {
    expect(
      isDurabilitySatisfied({ policy: 'any', minSuccessCount: 1 }, 1, 3)
    ).toBe(true);
    expect(
      isDurabilitySatisfied({ policy: 'any', minSuccessCount: 1 }, 0, 3)
    ).toBe(false);
  });

  it('"all" requires every provider to succeed', () => {
    expect(
      isDurabilitySatisfied({ policy: 'all', minSuccessCount: 1 }, 2, 3)
    ).toBe(false);
    expect(
      isDurabilitySatisfied({ policy: 'all', minSuccessCount: 1 }, 3, 3)
    ).toBe(true);
  });

  it('"quorum" requires the configured minimum successes', () => {
    const config = { policy: 'quorum' as const, minSuccessCount: 2 };
    expect(isDurabilitySatisfied(config, 1, 3)).toBe(false);
    expect(isDurabilitySatisfied(config, 2, 3)).toBe(true);
    expect(isDurabilitySatisfied(config, 3, 3)).toBe(true);
  });

  it('"quorum" clamps the minimum to the total number of providers', () => {
    const config = { policy: 'quorum' as const, minSuccessCount: 5 };
    expect(isDurabilitySatisfied(config, 2, 2)).toBe(true);
    expect(isDurabilitySatisfied(config, 1, 2)).toBe(false);
  });

  it('is never satisfied with zero configured providers', () => {
    expect(
      isDurabilitySatisfied({ policy: 'any', minSuccessCount: 1 }, 0, 0)
    ).toBe(false);
  });
});
