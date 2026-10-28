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

import { readFileSync } from 'node:fs';
import { GoogleAuth } from 'google-auth-library';
import { isInsecureLocalAllowed, optionalEnv } from './env.js';

export type KafkaSecurityProtocol = 'PLAINTEXT' | 'SSL' | 'SASL_SSL';
export type KafkaSaslMechanism = 'OAUTHBEARER' | 'PLAIN';
/** `google-adc`: short-lived Google access tokens from Application Default Credentials. `static`: PLAIN with fixed username/password. */
export type KafkaAuthProvider = 'google-adc' | 'static';

/** Kafka transport security settings shared by every producer and consumer. */
export interface KafkaSecurityConfig {
  protocol: KafkaSecurityProtocol;
  saslMechanism?: KafkaSaslMechanism;
  authProvider?: KafkaAuthProvider;
  /** Optional PEM CA bundle, only for brokers that do not chain to a system CA. */
  tlsCaPath?: string;
  /** Overrides the SNI/verification hostname; defaults to each broker's own host. */
  tlsServerName?: string;
  /** Principal e-mail override for `google-adc` (otherwise taken from ADC). */
  googlePrincipal?: string;
  saslUsername?: string;
  saslPassword?: string;
  /** Plaintext/unauthenticated connections permitted (local development only). */
  allowInsecure: boolean;
}

/** The subset of Platformatic client options produced by this module. */
export interface KafkaClientSecurityOptions {
  bootstrapBrokers: string[];
  tls?: { rejectUnauthorized: true; ca?: Buffer };
  tlsServerName?: string | boolean;
  sasl?: {
    mechanism: KafkaSaslMechanism;
    token?: () => Promise<string>;
    username?: string | (() => Promise<string>);
    password?: string | (() => Promise<string>);
  };
}

function parseEnum<T extends string>(
  name: string,
  value: string | undefined,
  allowed: readonly T[],
  fallback: T | undefined
): T | undefined {
  const normalized = optionalEnv(value);
  if (normalized === undefined) return fallback;
  const match = allowed.find((candidate) => candidate === normalized);
  if (match === undefined) {
    throw new Error(`${name} must be one of: ${allowed.join(', ')}`);
  }
  return match;
}

/**
 * Loads Kafka security settings from the environment.
 *
 * Environment variables:
 * - KAFKA_SECURITY_PROTOCOL (PLAINTEXT | SSL | SASL_SSL, default SASL_SSL)
 * - KAFKA_SASL_MECHANISM (OAUTHBEARER | PLAIN, default OAUTHBEARER)
 * - KAFKA_AUTH_PROVIDER (google-adc | static, default google-adc)
 * - KAFKA_TLS_CA_PATH, KAFKA_TLS_SERVERNAME
 * - KAFKA_GOOGLE_PRINCIPAL
 * - KAFKA_SASL_USERNAME, KAFKA_SASL_PASSWORD (static PLAIN)
 * - SCP_ALLOW_INSECURE_LOCAL (default false)
 */
export function loadKafkaSecurityConfig(
  env: NodeJS.ProcessEnv = process.env
): KafkaSecurityConfig {
  const protocol =
    parseEnum(
      'KAFKA_SECURITY_PROTOCOL',
      env.KAFKA_SECURITY_PROTOCOL,
      ['PLAINTEXT', 'SSL', 'SASL_SSL'] as const,
      'SASL_SSL'
    ) ?? 'SASL_SSL';
  const usesSasl = protocol === 'SASL_SSL';
  const saslMechanism = usesSasl
    ? parseEnum(
        'KAFKA_SASL_MECHANISM',
        env.KAFKA_SASL_MECHANISM,
        ['OAUTHBEARER', 'PLAIN'] as const,
        'OAUTHBEARER'
      )
    : undefined;
  const authProvider = usesSasl
    ? parseEnum(
        'KAFKA_AUTH_PROVIDER',
        env.KAFKA_AUTH_PROVIDER,
        ['google-adc', 'static'] as const,
        'google-adc'
      )
    : undefined;
  const tlsCaPath = optionalEnv(env.KAFKA_TLS_CA_PATH);
  const tlsServerName = optionalEnv(env.KAFKA_TLS_SERVERNAME);
  const googlePrincipal = optionalEnv(env.KAFKA_GOOGLE_PRINCIPAL);
  const saslUsername = optionalEnv(env.KAFKA_SASL_USERNAME);
  const saslPassword = optionalEnv(env.KAFKA_SASL_PASSWORD);
  return {
    protocol,
    ...(saslMechanism ? { saslMechanism } : {}),
    ...(authProvider ? { authProvider } : {}),
    ...(tlsCaPath ? { tlsCaPath } : {}),
    ...(tlsServerName ? { tlsServerName } : {}),
    ...(googlePrincipal ? { googlePrincipal } : {}),
    ...(saslUsername ? { saslUsername } : {}),
    ...(saslPassword ? { saslPassword } : {}),
    allowInsecure: isInsecureLocalAllowed(env),
  };
}

/**
 * Validates that the settings meet the production requirements (verified TLS
 * plus SASL authentication, no long-lived service-account keys).
 *
 * @throws Error describing the first violated requirement
 */
export function assertSecureKafkaConfig(
  config: KafkaSecurityConfig,
  env: NodeJS.ProcessEnv = process.env
): void {
  if (config.allowInsecure) return;
  if (config.protocol !== 'SASL_SSL') {
    throw new Error(
      'Kafka TLS and SASL are required: set KAFKA_SECURITY_PROTOCOL=SASL_SSL (set SCP_ALLOW_INSECURE_LOCAL=true for local development only)'
    );
  }
  if (config.authProvider === 'static') {
    if (
      config.saslMechanism !== 'PLAIN' ||
      !config.saslUsername ||
      !config.saslPassword
    ) {
      throw new Error(
        'KAFKA_AUTH_PROVIDER=static requires KAFKA_SASL_MECHANISM=PLAIN, KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD'
      );
    }
  }
  if (
    config.authProvider === 'google-adc' &&
    optionalEnv(env.GOOGLE_APPLICATION_CREDENTIALS)
  ) {
    throw new Error(
      'GOOGLE_APPLICATION_CREDENTIALS (service-account key file) is not allowed in production; use the Cloud Run service account identity'
    );
  }
}

function base64Url(value: string | Buffer): string {
  return Buffer.from(value).toString('base64url');
}

/**
 * Builds the Google Managed Kafka OAUTHBEARER token: three base64url segments
 * `header.claims.access_token` that the broker validates against Google IAM.
 */
export function buildGoogleKafkaToken(
  accessToken: string,
  principal: string,
  expiresAtMs: number,
  nowMs: number = Date.now()
): string {
  const header = JSON.stringify({ typ: 'JWT', alg: 'GOOG_OAUTH2_TOKEN' });
  const claims = JSON.stringify({
    exp: Math.floor(expiresAtMs / 1000),
    iat: Math.floor(nowMs / 1000),
    iss: 'Google',
    scope: 'kafka',
    sub: principal,
  });
  return [header, claims, accessToken].map(base64Url).join('.');
}

/** Source of Google access tokens and the principal they belong to. */
export interface GoogleCredentialSource {
  getAccessToken(): Promise<{ token: string; expiresAtMs: number }>;
  getPrincipal(): Promise<string>;
}

const DEFAULT_TOKEN_LIFETIME_MS = 55 * 60 * 1000;

/** Creates a credential source backed by Application Default Credentials. */
export function createGoogleAdcCredentialSource(
  principalOverride?: string
): GoogleCredentialSource {
  const auth = new GoogleAuth({
    scopes: ['https://www.googleapis.com/auth/cloud-platform'],
  });
  return {
    async getAccessToken() {
      const client = await auth.getClient();
      const { token } = await client.getAccessToken();
      if (!token) {
        throw new Error('Application Default Credentials returned no token');
      }
      const expiry = client.credentials.expiry_date;
      return {
        token,
        expiresAtMs: expiry ?? Date.now() + DEFAULT_TOKEN_LIFETIME_MS,
      };
    },
    async getPrincipal() {
      if (principalOverride) return principalOverride;
      const credentials = await auth.getCredentials();
      if (!credentials.client_email) {
        throw new Error(
          'Unable to determine the service account e-mail from ADC; set KAFKA_GOOGLE_PRINCIPAL'
        );
      }
      return credentials.client_email;
    },
  };
}

/**
 * Returns an async provider evaluated on every broker authentication, so
 * reconnects always present a fresh, unexpired token. The credential source
 * is responsible for caching and refreshing the underlying access token.
 */
export function createGoogleKafkaTokenProvider(
  source: GoogleCredentialSource,
  now: () => number = Date.now
): () => Promise<string> {
  return async () => {
    const [{ token, expiresAtMs }, principal] = await Promise.all([
      source.getAccessToken(),
      source.getPrincipal(),
    ]);
    if (expiresAtMs <= now()) {
      throw new Error('Google access token is expired');
    }
    return buildGoogleKafkaToken(token, principal, expiresAtMs, now());
  };
}

export interface CreateKafkaClientOptionsInput {
  brokers: string[];
  security: KafkaSecurityConfig;
  /** Overrides ADC lookup (tests). */
  googleCredentials?: GoogleCredentialSource;
  env?: NodeJS.ProcessEnv;
}

/**
 * Builds the TLS/SASL options to spread into every `@platformatic/kafka`
 * `Producer` / `Consumer` constructor.
 *
 * @throws Error when the configuration is insecure and local mode was not explicitly enabled
 */
export function createKafkaClientOptions(
  input: CreateKafkaClientOptionsInput
): KafkaClientSecurityOptions {
  const { brokers, security } = input;
  assertSecureKafkaConfig(security, input.env ?? process.env);

  if (security.protocol === 'PLAINTEXT') {
    return { bootstrapBrokers: brokers };
  }

  const options: KafkaClientSecurityOptions = {
    bootstrapBrokers: brokers,
    tls: {
      rejectUnauthorized: true,
      ...(security.tlsCaPath ? { ca: readFileSync(security.tlsCaPath) } : {}),
    },
    tlsServerName: security.tlsServerName ?? true,
  };

  if (security.protocol === 'SSL') return options;

  const mechanism = security.saslMechanism ?? 'OAUTHBEARER';
  if (security.authProvider === 'static') {
    if (!security.saslUsername || !security.saslPassword) {
      throw new Error('static SASL requires KAFKA_SASL_USERNAME/PASSWORD');
    }
    options.sasl = {
      mechanism,
      username: security.saslUsername,
      password: security.saslPassword,
    };
    return options;
  }

  const source =
    input.googleCredentials ??
    createGoogleAdcCredentialSource(security.googlePrincipal);
  const tokenProvider = createGoogleKafkaTokenProvider(source);
  if (mechanism === 'OAUTHBEARER') {
    options.sasl = { mechanism, token: tokenProvider };
  } else {
    // PLAIN fallback: principal as username and the raw short-lived access token as password.
    options.sasl = {
      mechanism,
      username: () => source.getPrincipal(),
      password: async () => (await source.getAccessToken()).token,
    };
  }
  return options;
}
