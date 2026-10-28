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
  buildGoogleKafkaToken,
  createGoogleKafkaTokenProvider,
  createKafkaClientOptions,
  loadKafkaSecurityConfig,
  type GoogleCredentialSource,
} from '../src/index.js';

const brokers = ['broker-1.internal:9092'];

function decodeToken(token: string) {
  const [header, claims, signature] = token.split('.') as [
    string,
    string,
    string,
  ];
  const parse = (value: string) =>
    JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as unknown;
  return {
    header: parse(header),
    claims: parse(claims),
    accessToken: Buffer.from(signature, 'base64url').toString('utf8'),
  };
}

function fakeSource(
  overrides: Partial<{ token: string; expiresAtMs: number }> = {}
): GoogleCredentialSource & { calls: number } {
  const source = {
    calls: 0,
    async getAccessToken() {
      source.calls += 1;
      return {
        token: overrides.token ?? `ya29.token-${source.calls}`,
        expiresAtMs: overrides.expiresAtMs ?? Date.now() + 3_600_000,
      };
    },
    async getPrincipal() {
      return 'runtime@project.iam.gserviceaccount.com';
    },
  };
  return source;
}

describe('loadKafkaSecurityConfig', () => {
  it('defaults to SASL_SSL with OAUTHBEARER and google-adc', () => {
    expect(loadKafkaSecurityConfig({})).toEqual({
      protocol: 'SASL_SSL',
      saslMechanism: 'OAUTHBEARER',
      authProvider: 'google-adc',
      allowInsecure: false,
    });
  });

  it('rejects unknown values', () => {
    expect(() =>
      loadKafkaSecurityConfig({ KAFKA_SECURITY_PROTOCOL: 'TLS' })
    ).toThrow(/KAFKA_SECURITY_PROTOCOL/);
    expect(() =>
      loadKafkaSecurityConfig({ KAFKA_SASL_MECHANISM: 'GSSAPI' })
    ).toThrow(/KAFKA_SASL_MECHANISM/);
  });
});

describe('buildGoogleKafkaToken', () => {
  it('produces the Google header.claims.access_token envelope', () => {
    const token = buildGoogleKafkaToken(
      'ya29.abc',
      'sa@p.iam.gserviceaccount.com',
      2_000_000,
      1_000_000
    );
    const decoded = decodeToken(token);
    expect(decoded.header).toEqual({ typ: 'JWT', alg: 'GOOG_OAUTH2_TOKEN' });
    expect(decoded.claims).toMatchObject({
      exp: 2000,
      iat: 1000,
      iss: 'Google',
      scope: 'kafka',
      sub: 'sa@p.iam.gserviceaccount.com',
    });
    expect(decoded.accessToken).toBe('ya29.abc');
  });
});

describe('createGoogleKafkaTokenProvider', () => {
  it('asks the credential source on every call so reconnects get fresh tokens', async () => {
    const source = fakeSource();
    const provider = createGoogleKafkaTokenProvider(source);
    const first = decodeToken(await provider()).accessToken;
    const second = decodeToken(await provider()).accessToken;
    expect(first).not.toBe(second);
    expect(source.calls).toBe(2);
  });

  it('refuses to present an expired token', async () => {
    const provider = createGoogleKafkaTokenProvider(
      fakeSource({ expiresAtMs: 1000 }),
      () => 2000
    );
    await expect(provider()).rejects.toThrow(/expired/);
  });

  it('propagates credential failures instead of looping', async () => {
    const provider = createGoogleKafkaTokenProvider({
      async getAccessToken() {
        throw new Error('metadata server unavailable');
      },
      async getPrincipal() {
        return 'sa@p';
      },
    });
    await expect(provider()).rejects.toThrow(/metadata server/);
  });
});

describe('createKafkaClientOptions', () => {
  it('refuses plaintext unless local mode is explicitly enabled', () => {
    expect(() =>
      createKafkaClientOptions({
        brokers,
        security: loadKafkaSecurityConfig({
          KAFKA_SECURITY_PROTOCOL: 'PLAINTEXT',
        }),
        env: {},
      })
    ).toThrow(/TLS and SASL are required/);
    expect(() =>
      createKafkaClientOptions({
        brokers,
        security: loadKafkaSecurityConfig({
          KAFKA_SECURITY_PROTOCOL: 'SSL',
        }),
        env: {},
      })
    ).toThrow(/TLS and SASL are required/);
  });

  it('returns only brokers in explicit insecure local mode', () => {
    const options = createKafkaClientOptions({
      brokers,
      security: loadKafkaSecurityConfig({
        KAFKA_SECURITY_PROTOCOL: 'PLAINTEXT',
        SCP_ALLOW_INSECURE_LOCAL: 'true',
      }),
      env: {},
    });
    expect(options).toEqual({ bootstrapBrokers: brokers });
  });

  it('builds verified TLS + OAUTHBEARER token provider for google-adc', async () => {
    const options = createKafkaClientOptions({
      brokers,
      security: loadKafkaSecurityConfig({}),
      googleCredentials: fakeSource(),
      env: {},
    });
    expect(options.tls).toEqual({ rejectUnauthorized: true });
    expect(options.tlsServerName).toBe(true);
    expect(options.sasl?.mechanism).toBe('OAUTHBEARER');
    const token = await options.sasl?.token?.();
    expect(decodeToken(token ?? '').claims).toMatchObject({
      sub: 'runtime@project.iam.gserviceaccount.com',
    });
  });

  it('loads an optional CA bundle and SNI override', () => {
    const dir = mkdtempSync(join(tmpdir(), 'scp-kafka-'));
    const ca = join(dir, 'ca.pem');
    writeFileSync(ca, 'KAFKA-CA');
    const options = createKafkaClientOptions({
      brokers,
      security: loadKafkaSecurityConfig({
        KAFKA_TLS_CA_PATH: ca,
        KAFKA_TLS_SERVERNAME: 'bootstrap.kafka.internal',
      }),
      googleCredentials: fakeSource(),
      env: {},
    });
    expect(options.tls?.ca?.toString()).toBe('KAFKA-CA');
    expect(options.tlsServerName).toBe('bootstrap.kafka.internal');
  });

  it('supports the PLAIN fallback with a short-lived access token', async () => {
    const options = createKafkaClientOptions({
      brokers,
      security: loadKafkaSecurityConfig({ KAFKA_SASL_MECHANISM: 'PLAIN' }),
      googleCredentials: fakeSource({ token: 'ya29.plain' }),
      env: {},
    });
    expect(options.sasl?.mechanism).toBe('PLAIN');
    const username = options.sasl?.username;
    const password = options.sasl?.password;
    expect(typeof username === 'function' ? await username() : username).toBe(
      'runtime@project.iam.gserviceaccount.com'
    );
    expect(typeof password === 'function' ? await password() : password).toBe(
      'ya29.plain'
    );
  });

  it('supports static PLAIN credentials over TLS', () => {
    const options = createKafkaClientOptions({
      brokers,
      security: loadKafkaSecurityConfig({
        KAFKA_SASL_MECHANISM: 'PLAIN',
        KAFKA_AUTH_PROVIDER: 'static',
        KAFKA_SASL_USERNAME: 'user',
        KAFKA_SASL_PASSWORD: 'pass',
      }),
      env: {},
    });
    expect(options.sasl).toEqual({
      mechanism: 'PLAIN',
      username: 'user',
      password: 'pass',
    });
    expect(options.tls?.rejectUnauthorized).toBe(true);
  });

  it('requires static credentials for the static provider', () => {
    expect(() =>
      createKafkaClientOptions({
        brokers,
        security: loadKafkaSecurityConfig({ KAFKA_AUTH_PROVIDER: 'static' }),
        env: {},
      })
    ).toThrow(/static/);
  });

  it('rejects long-lived service-account key files in production', () => {
    expect(() =>
      createKafkaClientOptions({
        brokers,
        security: loadKafkaSecurityConfig({}),
        googleCredentials: fakeSource(),
        env: { GOOGLE_APPLICATION_CREDENTIALS: '/tmp/key.json' },
      })
    ).toThrow(/GOOGLE_APPLICATION_CREDENTIALS/);
  });
});
