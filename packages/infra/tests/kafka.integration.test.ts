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
import { randomUUID } from 'node:crypto';
import {
  Consumer,
  Producer,
  stringDeserializers,
  stringSerializers,
} from '@platformatic/kafka';
import { describe, expect, it } from 'vitest';
import {
  createKafkaClientOptions,
  loadKafkaSecurityConfig,
} from '../src/index.js';
import {
  KAFKA_PASSWORD,
  KAFKA_SECURE_BROKER,
  KAFKA_USER,
  certPath,
  integrationEnabled,
} from './integration-env.js';

function options(overrides: NodeJS.ProcessEnv = {}) {
  return createKafkaClientOptions({
    brokers: [KAFKA_SECURE_BROKER],
    security: loadKafkaSecurityConfig({
      KAFKA_AUTH_PROVIDER: 'static',
      KAFKA_SASL_MECHANISM: 'PLAIN',
      KAFKA_SASL_USERNAME: KAFKA_USER,
      KAFKA_SASL_PASSWORD: KAFKA_PASSWORD,
      KAFKA_TLS_CA_PATH: certPath('ca.pem'),
      ...overrides,
    }),
    env: {},
  });
}

function newProducer(overrides: NodeJS.ProcessEnv = {}) {
  return new Producer({
    clientId: 'integration-test',
    ...options(overrides),
    serializers: stringSerializers,
    retries: 0,
    connectTimeout: 3000,
    requestTimeout: 5000,
  });
}

describe.skipIf(!integrationEnabled)('Kafka TLS + SASL (real broker)', () => {
  it('produces and consumes through the shared factory options', async () => {
    const topic = `scp-it-${randomUUID()}`;
    const producer = newProducer();
    const consumer = new Consumer({
      clientId: 'integration-test',
      groupId: `group-${randomUUID()}`,
      ...options(),
      deserializers: stringDeserializers,
    });
    try {
      // Auto-created topics are reported as unknown until the leader election finishes.
      for (let attempt = 0; ; attempt++) {
        try {
          await producer.metadata({ topics: [topic], autocreateTopics: true });
          break;
        } catch (error) {
          if (attempt >= 10) throw error;
          await new Promise((resolve) => setTimeout(resolve, 500));
        }
      }
      await producer.send({
        messages: [{ topic, key: 'k', value: 'hello' }],
      });
      const stream = await consumer.consume({
        topics: [topic],
        mode: 'earliest',
        autocommit: true,
      });
      const received = await new Promise<string>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('timeout')), 20_000);
        stream.once('data', (message) => {
          clearTimeout(timer);
          resolve(String(message.value));
        });
      });
      expect(received).toBe('hello');
    } finally {
      await consumer.close(true).catch(() => undefined);
      await producer.close().catch(() => undefined);
    }
  }, 60_000);

  it('fails authentication with a wrong password', async () => {
    const producer = newProducer({ KAFKA_SASL_PASSWORD: 'wrong' });
    await expect(producer.metadata({ topics: [] })).rejects.toThrow();
    await producer.close().catch(() => undefined);
  });

  it('fails TLS verification for an untrusted CA', async () => {
    const producer = newProducer({
      KAFKA_TLS_CA_PATH: certPath('other-ca.pem'),
    });
    await expect(producer.metadata({ topics: [] })).rejects.toThrow();
    await producer.close().catch(() => undefined);
  });

  it('fails when the server name does not match the certificate', async () => {
    const producer = newProducer({
      KAFKA_TLS_SERVERNAME: 'not-the-broker.example',
    });
    await expect(producer.metadata({ topics: [] })).rejects.toThrow();
    await producer.close().catch(() => undefined);
  });
});
