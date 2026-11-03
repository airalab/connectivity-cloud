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
import type { SensorAuth } from '@scp/core';
import { CpsAuth, loadCpsAuthConfig } from '@scp/cps-auth';
import { WhitelistAuth, loadWhitelistConfig } from '@scp/whitelist';
import { OpenAuth } from './open-auth.js';
import type { RegistryReader } from './registry-reader.js';
import { logInfo, logWarn } from './logger.js';

/**
 * Sensor authentication strategy types.
 *
 * - `whitelist`: only sensors listed in `WHITELIST_SENSOR_IDS` are authorized.
 * - `none`: every validly-signed sensor is authorized (no allowlist).
 * - `cps`: sensors whose CPS node descends from the `sensors.social` root.
 */
export type SensorAuthStrategy = 'whitelist' | 'none' | 'cps';

/**
 * Configuration for sensor authentication.
 */
export interface SensorAuthConfig {
  strategy: SensorAuthStrategy;
}

/**
 * Loads sensor authentication configuration from environment variables.
 *
 * Environment variables:
 * - SENSOR_AUTH_STRATEGY: Authentication strategy to use (whitelist, none or
 *   cps, default: whitelist)
 */
export function loadSensorAuthConfig(
  env: NodeJS.ProcessEnv = process.env
): SensorAuthConfig {
  const strategyStr = env.SENSOR_AUTH_STRATEGY ?? 'whitelist';
  const strategy: SensorAuthStrategy =
    strategyStr === 'none' || strategyStr === 'cps' ? strategyStr : 'whitelist';

  if (strategyStr !== strategy) {
    logWarn('invalid SENSOR_AUTH_STRATEGY, defaulting to whitelist', {
      provided: strategyStr,
      using: strategy,
    });
  }

  return { strategy };
}

function toRegistryReader(
  auth: SensorAuth,
  close?: () => Promise<void>
): RegistryReader {
  return {
    authenticate: (sensorId) => auth.authenticate(sensorId),
    async getSensorRecord(sensorId) {
      const isAuthenticated = await auth.authenticate(sensorId);
      return isAuthenticated ? { sensorId, enabled: true } : null;
    },
    ...(close ? { close } : {}),
  };
}

/**
 * Creates a sensor authentication provider based on the configured strategy.
 *
 * @param strategy - The authentication strategy to use
 * @returns A RegistryReader adapter over the selected strategy
 * @throws For `cps`, if the chain is unreachable or the CPS root is invalid
 */
export async function createSensorAuthProvider(
  strategy: SensorAuthStrategy
): Promise<RegistryReader> {
  logInfo('creating sensor auth provider', { strategy });

  if (strategy === 'none') {
    logWarn('sensor authentication disabled; any signed sensor is accepted');
    return toRegistryReader(new OpenAuth());
  }

  if (strategy === 'cps') {
    const cpsAuth = await CpsAuth.connect(loadCpsAuthConfig());
    return toRegistryReader(cpsAuth, () => cpsAuth.close());
  }

  const whitelistConfig = loadWhitelistConfig();
  return toRegistryReader(new WhitelistAuth(whitelistConfig.allowedSensorIds));
}
