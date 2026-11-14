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
import { formatSensorId } from '@scp/core';
import { sensorIdToSs58 } from '../src/sensor-address.js';
import { renderStatusPage } from '../src/status-page.js';

describe('sensorIdToSs58', () => {
  it.each([0, 1, 32, 127, 255])(
    'matches the shared SS58 formatter for 32-byte keys filled with %i',
    (byte) => {
      const sensorId = new Uint8Array(32).fill(byte);
      expect(sensorIdToSs58(Buffer.from(sensorId).toString('hex'))).toBe(
        formatSensorId(sensorId)
      );
    }
  );

  it('rejects malformed or wrong-length hex IDs', () => {
    expect(() => sensorIdToSs58('01'.repeat(31))).toThrow(/32 bytes/);
    expect(() => sensorIdToSs58('gg'.repeat(32))).toThrow(/32 bytes/);
  });

  it('builds a fixed-origin sensor link with safe new-tab attributes in the client', () => {
    const html = renderStatusPage();
    expect(html).toContain("new URL('https://sensors.social/')");
    expect(html).toContain("externalUrl.searchParams.set('sensor', ss58)");
    expect(html).toContain("link.target = '_blank'");
    expect(html).toContain("link.rel = 'noopener noreferrer'");
    expect(html).toContain('sensorIdToSs58(sourceId)');
  });
});
