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
import { describe, expect, it } from 'vitest';
import {
  assertSecureKafkaConfig,
  assertSecureRedisConfig,
  isInsecureLocalAllowed,
  loadKafkaSecurityConfig,
  loadRedisConfig,
} from '../src/index.js';

describe('insecure local opt-in', () => {
  it('is disabled by default', () => {
    expect(isInsecureLocalAllowed({})).toBe(false);
    expect(isInsecureLocalAllowed({ NODE_ENV: 'production' })).toBe(false);
  });

  it('is allowed outside production', () => {
    expect(isInsecureLocalAllowed({ SCP_ALLOW_INSECURE_LOCAL: 'true' })).toBe(
      true
    );
    expect(
      isInsecureLocalAllowed({
        SCP_ALLOW_INSECURE_LOCAL: 'true',
        NODE_ENV: 'development',
      })
    ).toBe(true);
  });

  it.each(['production', ' Production '])(
    'is rejected when NODE_ENV=%s',
    (nodeEnv) => {
      expect(() =>
        isInsecureLocalAllowed({
          SCP_ALLOW_INSECURE_LOCAL: 'TRUE',
          NODE_ENV: nodeEnv,
        })
      ).toThrow(/not permitted when NODE_ENV=production/);
    }
  );

  it('makes the Redis and Kafka loaders refuse insecure mode in production', () => {
    const env = { SCP_ALLOW_INSECURE_LOCAL: 'true', NODE_ENV: 'production' };
    expect(() => loadRedisConfig(env)).toThrow(/NODE_ENV=production/);
    expect(() =>
      loadKafkaSecurityConfig({ ...env, KAFKA_SECURITY_PROTOCOL: 'PLAINTEXT' })
    ).toThrow(/NODE_ENV=production/);
  });

  it('still enforces TLS and AUTH in production without the opt-in', () => {
    const redis = loadRedisConfig({ NODE_ENV: 'production' });
    expect(() => assertSecureRedisConfig(redis)).toThrow(/TLS is required/);
    const kafka = loadKafkaSecurityConfig({
      NODE_ENV: 'production',
      KAFKA_SECURITY_PROTOCOL: 'PLAINTEXT',
    });
    expect(() => assertSecureKafkaConfig(kafka, {})).toThrow(
      /TLS and SASL are required/
    );
  });
});
