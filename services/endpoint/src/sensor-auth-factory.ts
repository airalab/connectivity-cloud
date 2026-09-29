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
import { WhitelistAuth, loadWhitelistConfig } from '@scp/whitelist';
import { OpenAuth } from './open-auth.js';
import type { RegistryReader } from './registry-reader.js';
import { logInfo, logWarn } from './logger.js';

/**
 * Sensor authentication strategy types.
 *
 * - `whitelist`: only sensors listed in `WHITELIST_SENSOR_IDS` are authorized.
 * - `none`: every validly-signed sensor is authorized (no allowlist).
 */
export type SensorAuthStrategy = 'whitelist' | 'none';

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
 * - SENSOR_AUTH_STRATEGY: Authentication strategy to use (whitelist or none,
 *   default: whitelist)
 */
export function loadSensorAuthConfig(
  env: NodeJS.ProcessEnv = process.env
): SensorAuthConfig {
  const strategyStr = env.SENSOR_AUTH_STRATEGY ?? 'whitelist';
  const strategy = strategyStr === 'none' ? 'none' : 'whitelist';

  if (strategyStr !== strategy) {
    logWarn('invalid SENSOR_AUTH_STRATEGY, defaulting to whitelist', {
      provided: strategyStr,
      using: strategy,
    });
  }

  return { strategy };
}

interface NonceAwareSensorAuth {
  authenticate(sensorId: Uint8Array): Promise<boolean>;
  isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean>;
  rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void>;
}

function toRegistryReader(auth: NonceAwareSensorAuth): RegistryReader {
  return {
    authenticate: (sensorId) => auth.authenticate(sensorId),
    async getSensorRecord(sensorId) {
      const isAuthenticated = await auth.authenticate(sensorId);
      return isAuthenticated ? { sensorId, enabled: true } : null;
    },
    isNonceSeen: (sensorId, nonce) => auth.isNonceSeen(sensorId, nonce),
    rememberNonce: (sensorId, nonce) => auth.rememberNonce(sensorId, nonce),
  };
}

/**
 * Creates a sensor authentication provider based on the configured strategy.
 *
 * @param strategy - The authentication strategy to use
 * @returns A RegistryReader adapter over the selected strategy
 */
export function createSensorAuthProvider(
  strategy: SensorAuthStrategy
): RegistryReader {
  logInfo('creating sensor auth provider', { strategy });

  if (strategy === 'none') {
    logWarn('sensor authentication disabled; any signed sensor is accepted');
    return toRegistryReader(new OpenAuth());
  }

  const whitelistConfig = loadWhitelistConfig();
  return toRegistryReader(new WhitelistAuth(whitelistConfig.allowedSensorIds));
}
