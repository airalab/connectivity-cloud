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
import { loadEndpointConfig } from '../src/config.js';

describe('loadEndpointConfig', () => {
  it('defaults to port 3000 when neither PORT nor ENDPOINT_PORT is set', () => {
    const config = loadEndpointConfig({});
    expect(config.port).toBe(3000);
  });

  it('uses ENDPOINT_PORT when PORT is not set', () => {
    const config = loadEndpointConfig({ ENDPOINT_PORT: '4001' });
    expect(config.port).toBe(4001);
  });

  it('prefers Cloud Run PORT over ENDPOINT_PORT', () => {
    const config = loadEndpointConfig({ PORT: '8080', ENDPOINT_PORT: '4001' });
    expect(config.port).toBe(8080);
  });

  it('falls back to the default when PORT is not a positive integer', () => {
    const config = loadEndpointConfig({ PORT: 'not-a-number' });
    expect(config.port).toBe(3000);
  });
});
