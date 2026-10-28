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
import { mkdtempSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  assertSecureRedisConfig,
  buildRedisOptions,
  createRedisClient,
  loadRedisConfig,
  redactRedisUrl,
  redactSecrets,
} from '../src/index.js';

const caPath = (() => {
  const dir = mkdtempSync(join(tmpdir(), 'scp-redis-'));
  const path = join(dir, 'ca.pem');
  writeFileSync(path, 'CA-BUNDLE-PLACEHOLDER');
  return path;
})();

const secure = {
  REDIS_URL: 'rediss://10.0.0.5:6378',
  REDIS_AUTH_PASSWORD: 's3cret',
  REDIS_TLS_CA_PATH: caPath,
};

describe('loadRedisConfig', () => {
  it('defaults to the local URL with safe timeouts and no insecure opt-in', () => {
    expect(loadRedisConfig({})).toEqual({
      url: 'redis://localhost:6379',
      connectTimeoutMs: 5000,
      commandTimeoutMs: 2000,
      allowInsecure: false,
    });
  });

  it('reads TLS, AUTH and the insecure opt-in', () => {
    const config = loadRedisConfig({
      ...secure,
      REDIS_TLS_SERVERNAME: 'redis.internal',
      SCP_ALLOW_INSECURE_LOCAL: 'true',
    });
    expect(config).toMatchObject({
      url: 'rediss://10.0.0.5:6378',
      password: 's3cret',
      tlsCaPath: caPath,
      tlsServerName: 'redis.internal',
      allowInsecure: true,
    });
  });
});

describe('assertSecureRedisConfig', () => {
  it('rejects plaintext, missing AUTH and missing CA in production', () => {
    expect(() => assertSecureRedisConfig(loadRedisConfig({}))).toThrow(
      /rediss/
    );
    expect(() =>
      assertSecureRedisConfig(
        loadRedisConfig({ ...secure, REDIS_AUTH_PASSWORD: '' })
      )
    ).toThrow(/AUTH/);
    expect(() =>
      assertSecureRedisConfig(
        loadRedisConfig({ ...secure, REDIS_TLS_CA_PATH: '' })
      )
    ).toThrow(/CA/);
  });

  it('accepts TLS + AUTH + CA, and plaintext only in explicit local mode', () => {
    expect(() =>
      assertSecureRedisConfig(loadRedisConfig(secure))
    ).not.toThrow();
    expect(() =>
      assertSecureRedisConfig(
        loadRedisConfig({ SCP_ALLOW_INSECURE_LOCAL: 'true' })
      )
    ).not.toThrow();
  });
});

describe('buildRedisOptions', () => {
  it('enables verified TLS with the provided CA and AUTH', () => {
    const options = buildRedisOptions(loadRedisConfig(secure));
    expect(options.host).toBe('10.0.0.5');
    expect(options.port).toBe(6378);
    expect(options.password).toBe('s3cret');
    expect(options.tls).toMatchObject({ rejectUnauthorized: true });
    expect(options.tls?.ca?.toString()).toBe('CA-BUNDLE-PLACEHOLDER');
    expect(options.commandTimeout).toBe(2000);
    expect(options.connectTimeout).toBe(5000);
    expect(options.lazyConnect).toBe(true);
  });

  it('passes an explicit TLS server name', () => {
    const options = buildRedisOptions(
      loadRedisConfig({ ...secure, REDIS_TLS_SERVERNAME: 'redis.internal' })
    );
    expect(options.tls).toMatchObject({ servername: 'redis.internal' });
  });

  it('prefers REDIS_AUTH_PASSWORD over a password embedded in the URL', () => {
    const options = buildRedisOptions(
      loadRedisConfig({ ...secure, REDIS_URL: 'rediss://:old@10.0.0.5:6378' })
    );
    expect(options.password).toBe('s3cret');
  });

  it('uses no TLS only in explicit insecure local mode', () => {
    const options = buildRedisOptions(
      loadRedisConfig({ SCP_ALLOW_INSECURE_LOCAL: 'true' })
    );
    expect(options.tls).toBeUndefined();
    expect(options.host).toBe('localhost');
    expect(options.port).toBe(6379);
  });

  it('refuses insecure configuration by default', () => {
    expect(() => buildRedisOptions(loadRedisConfig({}))).toThrow();
  });
});

describe('redaction', () => {
  it('removes credentials from URLs and messages', () => {
    expect(redactRedisUrl('rediss://user:pw@host:6378')).not.toContain('pw');
    expect(redactRedisUrl('not a url')).toBe('<invalid redis url>');
    expect(redactSecrets('AUTH s3cret failed s3cret', ['s3cret'])).toBe(
      'AUTH *** failed ***'
    );
  });

  it('does not leak the password when startup fails', async () => {
    const config = loadRedisConfig({
      REDIS_URL: 'redis://127.0.0.1:1',
      REDIS_AUTH_PASSWORD: 'topsecret',
      SCP_ALLOW_INSECURE_LOCAL: 'true',
      REDIS_CONNECT_TIMEOUT_MS: '500',
    });
    const error = await createRedisClient(config).catch((e: Error) => e);
    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toMatch(/failed to connect to Redis/);
    expect((error as Error).message).not.toContain('topsecret');
  });
});
