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
import { loadHeartbeatTrackerConfig } from '../src/config.js';

describe('heartbeat-tracker health port', () => {
  const base: NodeJS.ProcessEnv = {};

  it('defaults to 3030', () => {
    expect(loadHeartbeatTrackerConfig(base).healthPort).toBe(3030);
  });

  it('uses HEARTBEAT_TRACKER_HEALTH_PORT when PORT is not set', () => {
    expect(
      loadHeartbeatTrackerConfig({
        ...base,
        HEARTBEAT_TRACKER_HEALTH_PORT: '4001',
      }).healthPort
    ).toBe(4001);
  });

  it('prefers Cloud Run PORT over HEARTBEAT_TRACKER_HEALTH_PORT', () => {
    expect(
      loadHeartbeatTrackerConfig({
        ...base,
        PORT: '8080',
        HEARTBEAT_TRACKER_HEALTH_PORT: '4001',
      }).healthPort
    ).toBe(8080);
  });

  it('falls back to the default when PORT is not a positive integer', () => {
    expect(
      loadHeartbeatTrackerConfig({ ...base, PORT: 'not-a-number' }).healthPort
    ).toBe(3030);
  });
});
