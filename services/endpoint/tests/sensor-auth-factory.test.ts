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
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { randomBytes } from 'node:crypto';
import { encodeAddress } from '@polkadot/util-crypto';
import {
  loadSensorAuthConfig,
  createSensorAuthProvider,
} from '../src/sensor-auth-factory.js';
import { OpenAuth } from '../src/open-auth.js';

// Helper to create SS58 test addresses (using Robonomics prefix 32)
function createTestAddress(): { address: string; pubkey: Uint8Array } {
  const pubkey = randomBytes(32);
  const address = encodeAddress(pubkey, 32);
  return { address, pubkey };
}

describe('sensor auth factory', () => {
  const originalEnv = process.env;

  beforeEach(() => {
    process.env = { ...originalEnv };
  });

  afterEach(() => {
    process.env = originalEnv;
  });

  describe('loadSensorAuthConfig', () => {
    it('should default to whitelist strategy', () => {
      delete process.env.SENSOR_AUTH_STRATEGY;

      expect(loadSensorAuthConfig().strategy).toBe('whitelist');
    });

    it('should load none strategy from env', () => {
      process.env.SENSOR_AUTH_STRATEGY = 'none';

      expect(loadSensorAuthConfig().strategy).toBe('none');
    });

    it('should default to whitelist for invalid strategy (including removed registry-sync)', () => {
      process.env.SENSOR_AUTH_STRATEGY = 'registry-sync';
      expect(loadSensorAuthConfig().strategy).toBe('whitelist');

      process.env.SENSOR_AUTH_STRATEGY = 'invalid-strategy';
      expect(loadSensorAuthConfig().strategy).toBe('whitelist');
    });
  });

  describe('createSensorAuthProvider', () => {
    it('should create whitelist provider', async () => {
      const sensorA = createTestAddress();
      const sensorB = createTestAddress();
      const sensorC = createTestAddress();
      const sensorD = createTestAddress();

      process.env.WHITELIST_SENSOR_IDS = `${sensorA.address},${sensorB.address},${sensorC.address}`;

      const provider = createSensorAuthProvider('whitelist');

      expect(await provider.authenticate(sensorA.pubkey)).toBe(true);
      expect(await provider.authenticate(sensorB.pubkey)).toBe(true);
      expect(await provider.authenticate(sensorC.pubkey)).toBe(true);
      expect(await provider.authenticate(sensorD.pubkey)).toBe(false);
    });

    it('should authorize nobody with an empty whitelist', async () => {
      delete process.env.WHITELIST_SENSOR_IDS;

      const provider = createSensorAuthProvider('whitelist');

      expect(await provider.authenticate(createTestAddress().pubkey)).toBe(
        false
      );
    });

    it('should handle nonce management for whitelist provider', async () => {
      const sensor1 = createTestAddress();
      const nonce1 = randomBytes(32);

      process.env.WHITELIST_SENSOR_IDS = sensor1.address;

      const provider = createSensorAuthProvider('whitelist');

      expect(await provider.isNonceSeen(sensor1.pubkey, nonce1)).toBe(false);
      await provider.rememberNonce(sensor1.pubkey, nonce1);
      expect(await provider.isNonceSeen(sensor1.pubkey, nonce1)).toBe(true);
    });

    it('should return sensor record for whitelist provider', async () => {
      const sensor1 = createTestAddress();
      const sensor2 = createTestAddress();
      const unknown = createTestAddress();

      process.env.WHITELIST_SENSOR_IDS = `${sensor1.address},${sensor2.address}`;

      const provider = createSensorAuthProvider('whitelist');

      expect(await provider.getSensorRecord(sensor1.pubkey)).toEqual({
        sensorId: sensor1.pubkey,
        enabled: true,
      });
      expect(await provider.getSensorRecord(unknown.pubkey)).toBeNull();
    });

    it('should authorize any sensor with the none provider', async () => {
      process.env.WHITELIST_SENSOR_IDS = '';
      const sensor = createTestAddress();

      const provider = createSensorAuthProvider('none');

      expect(await provider.authenticate(sensor.pubkey)).toBe(true);
      expect(await provider.getSensorRecord(sensor.pubkey)).toEqual({
        sensorId: sensor.pubkey,
        enabled: true,
      });
    });

    it('should track nonces for the none provider', async () => {
      const sensor = createTestAddress();
      const nonce = randomBytes(16);

      const provider = createSensorAuthProvider('none');

      expect(await provider.isNonceSeen(sensor.pubkey, nonce)).toBe(false);
      await provider.rememberNonce(sensor.pubkey, nonce);
      expect(await provider.isNonceSeen(sensor.pubkey, nonce)).toBe(true);
    });
  });

  describe('OpenAuth', () => {
    it('should evict the oldest nonce once the bound is exceeded', async () => {
      const auth = new OpenAuth(2);
      const sensor = randomBytes(32);
      const [n1, n2, n3] = [randomBytes(16), randomBytes(16), randomBytes(16)];

      await auth.rememberNonce(sensor, n1);
      await auth.rememberNonce(sensor, n2);
      await auth.rememberNonce(sensor, n3);

      expect(await auth.isNonceSeen(sensor, n1)).toBe(false);
      expect(await auth.isNonceSeen(sensor, n2)).toBe(true);
      expect(await auth.isNonceSeen(sensor, n3)).toBe(true);
    });
  });
});
