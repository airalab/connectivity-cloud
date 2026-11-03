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
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import {
  closeRedisClient,
  createRedisClient,
  createRedisNonceStore,
  loadRedisConfig,
} from '@scp/infra';
import {
  cryptoWaitReady,
  ed25519PairFromSeed,
  ed25519Sign,
} from '@polkadot/util-crypto';
import {
  buildEnvelopeSigningBytes,
  NonceStoreUnavailableError,
  REJECTION_CODES,
} from '@scp/core';
import type { NonceStore } from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { MessageSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/core/v1/message_pb.js';
import { createEndpointApp, type EndpointDeps } from '../src/index.js';
import { InMemoryNonceStore } from './in-memory-nonce-store.js';
import { InMemoryRegistryReader } from './in-memory-registry-reader.js';

async function signedEnvelope(seedByte: number, nonceByte: number) {
  await cryptoWaitReady();
  const pair = ed25519PairFromSeed(
    Uint8Array.from(Array.from({ length: 32 }, () => seedByte))
  );
  const nonce = Uint8Array.from(Buffer.alloc(16, nonceByte));
  const message = toBinary(
    MessageSchema,
    create(MessageSchema, { metadata: { timestamp: BigInt(Date.now()) } })
  );
  const signature = ed25519Sign(
    buildEnvelopeSigningBytes({ sensorId: pair.publicKey, nonce, message }),
    pair
  );
  return {
    sensorId: pair.publicKey,
    bytes: toBinary(
      SignedEnvelopeSchema,
      create(SignedEnvelopeSchema, {
        sensorId: pair.publicKey,
        nonce,
        message,
        signature,
      })
    ),
  };
}

function buildApp(
  sensorId: Uint8Array,
  nonceStore: NonceStore,
  overrides: Partial<EndpointDeps['producer']> = {}
) {
  const authorized: unknown[] = [];
  const rejected: number[] = [];
  const app = createEndpointApp({
    registryReader: new InMemoryRegistryReader([{ sensorId, enabled: true }]),
    nonceStore,
    producer: {
      async publishAuthorized(payload) {
        // Yield so concurrent requests interleave across the publish.
        await new Promise((resolve) => setImmediate(resolve));
        authorized.push(payload);
        return 'event-1';
      },
      async publishRejected(payload) {
        rejected.push(payload.reasonCode);
        return 'event-2';
      },
      ...overrides,
    },
  });
  return { app, authorized, rejected };
}

function post(app: ReturnType<typeof createEndpointApp>, bytes: Uint8Array) {
  return app.inject({
    method: 'POST',
    url: '/v1/telemetry',
    headers: { 'content-type': 'application/protobuf' },
    payload: Buffer.from(bytes),
  });
}

describe('endpoint nonce replay protection', () => {
  it('publishes once for simultaneous identical envelopes', async () => {
    const envelope = await signedEnvelope(3, 1);
    const { app, authorized } = buildApp(
      envelope.sensorId,
      new InMemoryNonceStore()
    );
    const responses = await Promise.all(
      Array.from({ length: 10 }, () => post(app, envelope.bytes))
    );
    expect(responses.filter((r) => r.statusCode === 202)).toHaveLength(1);
    expect(responses.filter((r) => r.statusCode === 409)).toHaveLength(9);
    expect(authorized).toHaveLength(1);
    await app.close();
  });

  it('rejects cross-instance replays through the shared store', async () => {
    const envelope = await signedEnvelope(3, 2);
    const shared = new InMemoryNonceStore();
    const first = buildApp(envelope.sensorId, shared);
    const second = buildApp(envelope.sensorId, shared);

    expect((await post(first.app, envelope.bytes)).statusCode).toBe(202);
    const replay = await post(second.app, envelope.bytes);
    expect(replay.statusCode).toBe(409);
    expect(replay.json()).toEqual({
      status: 'rejected',
      error_code: 'duplicate_nonce',
    });
    expect(second.authorized).toHaveLength(0);
    await Promise.all([first.app.close(), second.app.close()]);
  });

  it('emits a duplicate-nonce rejected event and counts it', async () => {
    const envelope = await signedEnvelope(3, 3);
    const { app, rejected } = buildApp(
      envelope.sensorId,
      new InMemoryNonceStore()
    );
    await post(app, envelope.bytes);
    await post(app, envelope.bytes);
    await app.close();
    expect(rejected).toEqual([REJECTION_CODES.DUPLICATE_NONCE]);
  });

  it('keeps different nonces independent', async () => {
    const one = await signedEnvelope(3, 4);
    const two = await signedEnvelope(3, 5);
    const { app, authorized } = buildApp(
      one.sensorId,
      new InMemoryNonceStore()
    );
    expect((await post(app, one.bytes)).statusCode).toBe(202);
    expect((await post(app, two.bytes)).statusCode).toBe(202);
    expect(authorized).toHaveLength(2);
    await app.close();
  });

  it('keeps the same nonce independent across sensors', async () => {
    const a = await signedEnvelope(3, 6);
    const b = await signedEnvelope(4, 6);
    const store = new InMemoryNonceStore();
    const first = buildApp(a.sensorId, store);
    const second = buildApp(b.sensorId, store);
    expect((await post(first.app, a.bytes)).statusCode).toBe(202);
    expect((await post(second.app, b.bytes)).statusCode).toBe(202);
    await Promise.all([first.app.close(), second.app.close()]);
  });

  it('fails closed with 503 and no publish when the nonce store is unavailable', async () => {
    const envelope = await signedEnvelope(3, 7);
    const failing: NonceStore = {
      async claim() {
        throw new NonceStoreUnavailableError('redis down');
      },
      async close() {},
    };
    const { app, authorized } = buildApp(envelope.sensorId, failing);
    const response = await post(app, envelope.bytes);
    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({
      status: 'rejected',
      error_code: 'nonce_store_unavailable',
    });
    expect(authorized).toHaveLength(0);
    await app.close();
  });

  it('retains the claim when the Kafka publish fails', async () => {
    const envelope = await signedEnvelope(3, 8);
    const claims: string[] = [];
    const store: NonceStore = {
      async claim() {
        claims.push('claim');
        return claims.length === 1;
      },
      async close() {},
    };
    const { app } = buildApp(envelope.sensorId, store, {
      async publishAuthorized() {
        throw new Error('ack timeout');
      },
    });
    const first = await post(app, envelope.bytes);
    expect(first.statusCode).toBe(503);
    expect(first.json()).toEqual({
      status: 'rejected',
      error_code: 'kafka_unavailable',
    });
    // The retry is rejected because the claim was kept, never released.
    expect((await post(app, envelope.bytes)).statusCode).toBe(409);
    await app.close();
  });

  it('claims the nonce before publishing', async () => {
    const envelope = await signedEnvelope(3, 9);
    const order: string[] = [];
    const store: NonceStore = {
      async claim() {
        order.push('claim');
        return true;
      },
      async close() {},
    };
    const { app } = buildApp(envelope.sensorId, store, {
      async publishAuthorized() {
        order.push('publish');
        return 'event';
      },
    });
    await post(app, envelope.bytes);
    expect(order).toEqual(['claim', 'publish']);
    await app.close();
  });
});

describe('endpoint /ready', () => {
  function readyApp(checks: Record<string, () => Promise<void>>) {
    return createEndpointApp(
      {
        registryReader: new InMemoryRegistryReader([]),
        nonceStore: new InMemoryNonceStore(),
        producer: {
          async publishAuthorized() {
            return 'e';
          },
          async publishRejected() {
            return 'e';
          },
        },
      },
      { readinessChecks: checks }
    );
  }

  it('reports ready when every dependency check passes', async () => {
    const app = readyApp({ redis: async () => {}, kafka: async () => {} });
    const response = await app.inject({ method: 'GET', url: '/ready' });
    expect(response.statusCode).toBe(200);
    expect(response.json()).toEqual({ status: 'ready' });
    await app.close();
  });

  it('reports 503 without leaking error details when a dependency fails', async () => {
    const app = readyApp({
      redis: async () => {
        throw new Error('AUTH failed for password hunter2');
      },
      kafka: async () => {},
    });
    const response = await app.inject({ method: 'GET', url: '/ready' });
    expect(response.statusCode).toBe(503);
    expect(response.json()).toEqual({
      status: 'unavailable',
      failed: ['redis'],
    });
    expect(response.body).not.toContain('hunter2');
    await app.close();
  });

  it('keeps /health as a dependency-free liveness probe', async () => {
    const app = readyApp({
      redis: async () => {
        throw new Error('down');
      },
    });
    const response = await app.inject({ method: 'GET', url: '/health' });
    expect(response.statusCode).toBe(200);
    await app.close();
  });
});

const integrationEnabled = process.env.SCP_INTEGRATION === 'true';

describe.skipIf(!integrationEnabled)(
  'endpoint replay protection with protected Redis (real server)',
  () => {
    it('rejects replays across two instances and a restart, publishing once', async () => {
      const certDir = new URL('../../../docker/certs/', import.meta.url);
      const redisEnv = {
        REDIS_URL:
          process.env.SCP_TEST_REDIS_TLS_URL ?? 'rediss://localhost:6380',
        REDIS_AUTH_PASSWORD:
          process.env.SCP_TEST_REDIS_PASSWORD ?? 'test-redis-password',
        REDIS_TLS_CA_PATH: fileURLToPath(new URL('ca.pem', certDir)),
      };
      const nonceConfig = { prefix: `nonce-it:${Date.now()}`, ttlSeconds: 60 };
      const newStore = async () =>
        createRedisNonceStore(
          await createRedisClient(loadRedisConfig(redisEnv)),
          nonceConfig,
          closeRedisClient
        );

      const envelope = await signedEnvelope(6, 1);
      const [storeA, storeB] = await Promise.all([newStore(), newStore()]);
      const a = buildApp(envelope.sensorId, storeA);
      const b = buildApp(envelope.sensorId, storeB);

      const responses = await Promise.all([
        post(a.app, envelope.bytes),
        post(b.app, envelope.bytes),
        post(a.app, envelope.bytes),
        post(b.app, envelope.bytes),
      ]);
      expect(responses.filter((r) => r.statusCode === 202)).toHaveLength(1);
      expect(responses.filter((r) => r.statusCode === 409)).toHaveLength(3);
      expect(a.authorized.length + b.authorized.length).toBe(1);

      // "Restart": brand-new store and app instance, same Redis.
      await Promise.all([a.app.close(), b.app.close()]);
      await Promise.all([storeA.close(), storeB.close()]);
      const restartedStore = await newStore();
      const restarted = buildApp(envelope.sensorId, restartedStore);
      expect((await post(restarted.app, envelope.bytes)).statusCode).toBe(409);
      expect(restarted.authorized).toHaveLength(0);
      await restarted.app.close();
      await restartedStore.close();
    });
  }
);
