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
import { randomBytes } from 'node:crypto';
import { readFileSync, writeFileSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { afterAll, describe, expect, it } from 'vitest';
import type { Redis } from 'ioredis';
import {
  closeRedisClient,
  createRedisClient,
  createRedisNonceStore,
  loadRedisConfig,
} from '../src/index.js';
import {
  REDIS_PASSWORD,
  REDIS_PLAIN_URL,
  REDIS_TLS_URL,
  certPath,
  integrationEnabled,
} from './integration-env.js';

const tlsEnv = (overrides: NodeJS.ProcessEnv = {}): NodeJS.ProcessEnv => ({
  REDIS_URL: REDIS_TLS_URL,
  REDIS_AUTH_PASSWORD: REDIS_PASSWORD,
  REDIS_TLS_CA_PATH: certPath('ca.pem'),
  REDIS_CONNECT_TIMEOUT_MS: '2000',
  ...overrides,
});

describe.skipIf(!integrationEnabled)('Redis TLS + AUTH (real server)', () => {
  const clients: Redis[] = [];
  const connect = async (env: NodeJS.ProcessEnv): Promise<Redis> => {
    const client = await createRedisClient(loadRedisConfig(env));
    clients.push(client);
    return client;
  };
  afterAll(async () => {
    await Promise.all(clients.map((client) => closeRedisClient(client)));
  });

  it('connects with verified TLS, a trusted CA and AUTH', async () => {
    const client = await connect(tlsEnv());
    expect(await client.ping()).toBe('PONG');
  });

  it('fails with a wrong AUTH password without leaking it', async () => {
    const error = await connect(
      tlsEnv({ REDIS_AUTH_PASSWORD: 'wrong-password' })
    ).catch((e: Error) => e);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).not.toContain('wrong-password');
  });

  it('fails when AUTH is missing', async () => {
    const config = loadRedisConfig(tlsEnv({ REDIS_AUTH_PASSWORD: '' }));
    // Production validation rejects it before any connection is attempted.
    await expect(createRedisClient(config)).rejects.toThrow(/AUTH/);
  });

  it('fails for an untrusted CA', async () => {
    await expect(
      connect(tlsEnv({ REDIS_TLS_CA_PATH: certPath('other-ca.pem') }))
    ).rejects.toThrow(/failed to connect/);
  });

  it('fails for a hostname that the certificate does not cover', async () => {
    await expect(
      connect(tlsEnv({ REDIS_TLS_SERVERNAME: 'not-the-server.example' }))
    ).rejects.toThrow(/failed to connect/);
  });

  it('fails when a plaintext client talks to the TLS port', async () => {
    await expect(
      connect({
        REDIS_URL: 'redis://localhost:6380',
        REDIS_AUTH_PASSWORD: REDIS_PASSWORD,
        SCP_ALLOW_INSECURE_LOCAL: 'true',
        REDIS_CONNECT_TIMEOUT_MS: '2000',
        REDIS_COMMAND_TIMEOUT_MS: '1000',
      })
    ).rejects.toThrow(/failed to connect/);
  });

  it('accepts a CA bundle with old and new CAs (rotation overlap)', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'scp-ca-'));
    const bundle = join(dir, 'bundle.pem');
    writeFileSync(
      bundle,
      readFileSync(certPath('other-ca.pem'), 'utf8') +
        readFileSync(certPath('ca.pem'), 'utf8')
    );
    const client = await connect(tlsEnv({ REDIS_TLS_CA_PATH: bundle }));
    expect(await client.ping()).toBe('PONG');
  });

  it('shares one protected instance between nonce claims and heartbeat keys', async () => {
    const client = await connect(tlsEnv());
    const nonceStore = createRedisNonceStore(
      client,
      { prefix: 'nonce:v1', ttlSeconds: 900 },
      async () => undefined
    );
    const sensor = randomBytes(32);
    const nonce = randomBytes(16);
    expect(await nonceStore.claim(sensor, nonce)).toBe(true);
    expect(await nonceStore.claim(sensor, nonce)).toBe(false);

    const heartbeatKey = `heartbeat-tracker:v1:sensor:${randomBytes(4).toString('hex')}`;
    await client.hset(heartbeatKey, { lastSeen: '1' });
    expect(await client.hget(heartbeatKey, 'lastSeen')).toBe('1');
    expect(await client.keys('nonce:v1:*')).not.toContain(heartbeatKey);
    await client.del(heartbeatKey);
  });

  it('grants one claim to simultaneous requests across separate connections', async () => {
    const [a, b] = await Promise.all([connect(tlsEnv()), connect(tlsEnv())]);
    const config = { prefix: 'nonce:v1', ttlSeconds: 60 };
    const stores = [a, b].map((client) =>
      createRedisNonceStore(client, config, async () => undefined)
    );
    const sensor = randomBytes(32);
    const nonce = randomBytes(16);
    const results = await Promise.all(
      Array.from({ length: 20 }, (_, i) => stores[i % 2]!.claim(sensor, nonce))
    );
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('expires claims after the TTL', async () => {
    const client = await connect(tlsEnv());
    const store = createRedisNonceStore(
      client,
      { prefix: 'nonce:v1', ttlSeconds: 1 },
      async () => undefined
    );
    const sensor = randomBytes(32);
    const nonce = randomBytes(16);
    expect(await store.claim(sensor, nonce)).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 1500));
    expect(await store.claim(sensor, nonce)).toBe(true);
  });
});

describe.skipIf(!integrationEnabled)('Redis plain (local mode)', () => {
  it('works only through the explicit insecure local mode', async () => {
    await expect(
      createRedisClient(loadRedisConfig({ REDIS_URL: REDIS_PLAIN_URL }))
    ).rejects.toThrow(/TLS is required/);
    const client = await createRedisClient(
      loadRedisConfig({
        REDIS_URL: REDIS_PLAIN_URL,
        SCP_ALLOW_INSECURE_LOCAL: 'true',
      })
    );
    expect(await client.ping()).toBe('PONG');
    await closeRedisClient(client);
  });
});
