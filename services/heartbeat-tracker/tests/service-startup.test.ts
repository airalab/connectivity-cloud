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
import type { Consumer } from '@platformatic/kafka';
import { describe, expect, it, vi } from 'vitest';
import { loadHeartbeatTrackerConfig } from '../src/config.js';
import { createHeartbeatTrackerService } from '../src/index.js';

function fakeConsumer() {
  const consume = vi.fn(async () => (async function* () {})());
  const close = vi.fn(async () => undefined);
  return {
    consumer: { consume, close } as unknown as Consumer,
    consume,
    close,
  };
}

describe('heartbeat tracker startup with protected Redis', () => {
  it('refuses to start against plaintext Redis unless local mode is explicit', async () => {
    const { consumer, consume, close } = fakeConsumer();
    const service = createHeartbeatTrackerService(
      loadHeartbeatTrackerConfig({ REDIS_URL: 'redis://localhost:6379' }),
      { createConsumer: () => consumer }
    );

    await expect(service.start()).rejects.toThrow(/Redis TLS is required/);
    expect(consume).not.toHaveBeenCalled();
    expect(close).toHaveBeenCalled();
  });

  it('refuses to start without Redis AUTH', async () => {
    const { consumer } = fakeConsumer();
    const service = createHeartbeatTrackerService(
      loadHeartbeatTrackerConfig({
        REDIS_URL: 'rediss://redis.internal:6378',
        REDIS_TLS_CA_PATH: '/tmp/ca.pem',
      }),
      { createConsumer: () => consumer }
    );

    await expect(service.start()).rejects.toThrow(/Redis AUTH is required/);
  });
});
