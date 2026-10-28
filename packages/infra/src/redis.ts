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
import { Redis } from 'ioredis';
import type { RedisOptions } from 'ioredis';
import {
  isInsecureLocalAllowed,
  optionalEnv,
  parsePositiveInt,
} from './env.js';

/** Connection settings shared by every service that talks to Redis. */
export interface RedisConfig {
  /** `rediss://host:port` in production; `redis://` only in explicit insecure local mode. */
  url: string;
  /** Redis AUTH password, kept out of the URL so it never lands in logs. */
  password?: string;
  /** Path to a PEM file holding one or more trusted CA certificates. */
  tlsCaPath?: string;
  /** Overrides the TLS server name used for certificate verification. */
  tlsServerName?: string;
  connectTimeoutMs: number;
  commandTimeoutMs: number;
  /** Plaintext/unauthenticated connections permitted (local development only). */
  allowInsecure: boolean;
}

/**
 * Loads Redis connection settings from the environment.
 *
 * Environment variables:
 * - REDIS_URL (default redis://localhost:6379)
 * - REDIS_AUTH_PASSWORD
 * - REDIS_TLS_CA_PATH
 * - REDIS_TLS_SERVERNAME
 * - REDIS_CONNECT_TIMEOUT_MS (default 5000)
 * - REDIS_COMMAND_TIMEOUT_MS (default 2000)
 * - SCP_ALLOW_INSECURE_LOCAL (default false)
 */
export function loadRedisConfig(
  env: NodeJS.ProcessEnv = process.env
): RedisConfig {
  const password = optionalEnv(env.REDIS_AUTH_PASSWORD);
  const tlsCaPath = optionalEnv(env.REDIS_TLS_CA_PATH);
  const tlsServerName = optionalEnv(env.REDIS_TLS_SERVERNAME);
  return {
    url: optionalEnv(env.REDIS_URL) ?? 'redis://localhost:6379',
    ...(password ? { password } : {}),
    ...(tlsCaPath ? { tlsCaPath } : {}),
    ...(tlsServerName ? { tlsServerName } : {}),
    connectTimeoutMs: parsePositiveInt(env.REDIS_CONNECT_TIMEOUT_MS, 5000),
    commandTimeoutMs: parsePositiveInt(env.REDIS_COMMAND_TIMEOUT_MS, 2000),
    allowInsecure: isInsecureLocalAllowed(env),
  };
}

/** Removes credentials from a Redis URL so it is safe to log. */
export function redactRedisUrl(url: string): string {
  try {
    const parsed = new URL(url);
    if (parsed.username) parsed.username = '***';
    if (parsed.password) parsed.password = '***';
    return parsed.toString();
  } catch {
    return '<invalid redis url>';
  }
}

/** Replaces any occurrence of secrets in an error message. */
export function redactSecrets(message: string, secrets: string[]): string {
  return secrets
    .filter((secret) => secret.length > 0)
    .reduce((text, secret) => text.split(secret).join('***'), message);
}

/**
 * Validates that the configuration satisfies the production security
 * requirements: verified TLS and AUTH. Insecure settings are rejected unless
 * insecure local mode was explicitly enabled.
 *
 * @throws Error describing the first violated requirement (never including secrets)
 */
export function assertSecureRedisConfig(config: RedisConfig): void {
  if (config.allowInsecure) return;
  let protocol: string;
  try {
    protocol = new URL(config.url).protocol;
  } catch {
    throw new Error('REDIS_URL is not a valid URL');
  }
  if (protocol !== 'rediss:') {
    throw new Error(
      'Redis TLS is required: REDIS_URL must use rediss:// (set SCP_ALLOW_INSECURE_LOCAL=true for local development only)'
    );
  }
  if (!config.password && !new URL(config.url).password) {
    throw new Error(
      'Redis AUTH is required: set REDIS_AUTH_PASSWORD (set SCP_ALLOW_INSECURE_LOCAL=true for local development only)'
    );
  }
  if (!config.tlsCaPath) {
    throw new Error(
      'Redis TLS CA is required: set REDIS_TLS_CA_PATH to the instance CA bundle'
    );
  }
}

/**
 * Builds ioredis options from the configuration. Server certificate
 * verification is always enabled.
 */
export function buildRedisOptions(config: RedisConfig): RedisOptions {
  assertSecureRedisConfig(config);
  const url = new URL(config.url);
  const useTls = url.protocol === 'rediss:';
  const password =
    config.password ??
    (url.password ? decodeURIComponent(url.password) : undefined);
  const username = url.username ? decodeURIComponent(url.username) : undefined;
  const db = url.pathname.length > 1 ? Number(url.pathname.slice(1)) : 0;

  return {
    host: url.hostname.replace(/^\[|\]$/g, ''),
    port: url.port ? Number(url.port) : 6379,
    db: Number.isInteger(db) ? db : 0,
    ...(username ? { username } : {}),
    ...(password ? { password } : {}),
    ...(useTls
      ? {
          tls: {
            rejectUnauthorized: true,
            ...(config.tlsCaPath ? { ca: readFileSync(config.tlsCaPath) } : {}),
            ...(config.tlsServerName
              ? { servername: config.tlsServerName }
              : {}),
          },
        }
      : {}),
    connectTimeout: config.connectTimeoutMs,
    commandTimeout: config.commandTimeoutMs,
    maxRetriesPerRequest: 1,
    // Bounded exponential backoff, retried indefinitely so a restarted Redis
    // is picked up; commands fail fast via commandTimeout in the meantime.
    retryStrategy: (attempt: number) => Math.min(attempt * 200, 5000),
    lazyConnect: true,
  };
}

/**
 * Creates an ioredis client and waits until it is connected and
 * authenticated. Startup fails (rejects) rather than falling back to a local
 * or unauthenticated connection.
 */
export async function createRedisClient(config: RedisConfig): Promise<Redis> {
  // ioredis' overloads reject optional-undefined under exactOptionalPropertyTypes.
  const RedisClient = Redis as unknown as new (options: RedisOptions) => Redis;
  const client = new RedisClient(buildRedisOptions(config));
  const secrets = [config.password ?? ''];
  // Without a listener ioredis logs every reconnect error to stderr.
  client.on('error', () => undefined);
  let failure: string | undefined;
  try {
    await client.connect();
    await client.ping();
  } catch (error) {
    client.disconnect();
    // Not chained as `cause`: the original error could carry credentials.
    failure = redactSecrets(
      error instanceof Error ? error.message : String(error),
      secrets
    );
  }
  if (failure !== undefined) {
    throw new Error(
      `failed to connect to Redis ${redactRedisUrl(config.url)}: ${failure}`
    );
  }
  return client;
}

/** Gracefully closes a Redis client, forcing a disconnect if QUIT fails. */
export async function closeRedisClient(client: Redis): Promise<void> {
  try {
    await client.quit();
  } catch {
    client.disconnect();
  }
}
