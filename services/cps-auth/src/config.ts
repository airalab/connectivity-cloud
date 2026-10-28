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
import { decodeAddress } from '@polkadot/util-crypto';

/** Configuration for the CPS subtree authentication strategy. */
export interface CpsAuthConfig {
  substrateWsUrl: string;
  /** SS58 address of the identity account of the `sensors.social` CPS root node. */
  rootAddress: string;
  /** Maximum number of cached authorization decisions. */
  cacheMaxSize: number;
  /** How long a negative (unauthorized) decision is cached. */
  negativeTtlSeconds: number;
}

const DEFAULT_CACHE_MAX_SIZE = 100_000;
const DEFAULT_NEGATIVE_TTL_SECONDS = 60;

function parsePositiveInt(
  name: string,
  value: string | undefined,
  fallback: number
): number {
  if (value === undefined || value.trim() === '') {
    return fallback;
  }
  if (!/^\d+$/.test(value.trim()) || Number.parseInt(value, 10) <= 0) {
    throw new Error(`${name} must be a positive integer, got: ${value}`);
  }
  return Number.parseInt(value, 10);
}

/**
 * Loads CPS authentication configuration from environment variables.
 *
 * Environment variables:
 * - SUBSTRATE_WS_URL: Substrate WebSocket endpoint (default: ws://localhost:9944)
 * - CPS_ROOT_ADDRESS: identity address of the `sensors.social` root node (required)
 * - CPS_AUTH_CACHE_MAX_SIZE: max cached decisions (default: 100000)
 * - CPS_AUTH_NEGATIVE_TTL_SECONDS: negative cache TTL (default: 60)
 *
 * @throws If `CPS_ROOT_ADDRESS` is missing or invalid, or a number is malformed
 */
export function loadCpsAuthConfig(
  env: NodeJS.ProcessEnv = process.env
): CpsAuthConfig {
  const rootAddress = env.CPS_ROOT_ADDRESS?.trim();
  if (!rootAddress) {
    throw new Error(
      'CPS_ROOT_ADDRESS is required when SENSOR_AUTH_STRATEGY=cps'
    );
  }
  try {
    decodeAddress(rootAddress);
  } catch (error) {
    throw new Error(
      `CPS_ROOT_ADDRESS is not a valid SS58 address: ${
        error instanceof Error ? error.message : String(error)
      }`,
      { cause: error }
    );
  }

  return {
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    rootAddress,
    cacheMaxSize: parsePositiveInt(
      'CPS_AUTH_CACHE_MAX_SIZE',
      env.CPS_AUTH_CACHE_MAX_SIZE,
      DEFAULT_CACHE_MAX_SIZE
    ),
    negativeTtlSeconds: parsePositiveInt(
      'CPS_AUTH_NEGATIVE_TTL_SECONDS',
      env.CPS_AUTH_NEGATIVE_TTL_SECONDS,
      DEFAULT_NEGATIVE_TTL_SECONDS
    ),
  };
}
