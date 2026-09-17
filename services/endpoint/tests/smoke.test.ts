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
  cryptoWaitReady,
  ed25519PairFromSeed,
  ed25519Sign,
} from '@polkadot/util-crypto';
import { buildEnvelopeSigningBytes, REJECTION_CODES } from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { MessageSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/core/v1/message_pb.js';
import { createEndpointApp } from '../src/index.js';
import { InMemoryRegistryReader } from '@scp/registry-sync';

async function buildSignedEnvelopeBytes(
  seedByte: number,
  timestamp: bigint = BigInt(Date.now())
) {
  await cryptoWaitReady();
  const seed = Uint8Array.from(Array.from({ length: 32 }, () => seedByte));
  const pair = ed25519PairFromSeed(seed);
  const nonce = Uint8Array.from(Buffer.alloc(16, 1));
  const message = toBinary(
    MessageSchema,
    create(MessageSchema, {
      metadata: { timestamp },
    })
  );

  const signingBytes = buildEnvelopeSigningBytes({
    sensorId: pair.publicKey,
    nonce,
    message,
  });
  const signature = ed25519Sign(signingBytes, pair);

  const envelope = create(SignedEnvelopeSchema, {
    sensorId: pair.publicKey,
    nonce,
    message,
    signature,
  });

  return {
    bytes: toBinary(SignedEnvelopeSchema, envelope),
    sensorId: pair.publicKey,
  };
}

describe('endpoint smoke', () => {
  it('returns 403 for unknown sensor, emits rejected event, and exposes health/metrics counters', async () => {
    const rejectedEvents: Array<{
      reason_code: string;
      sensor_id?: string | undefined;
    }> = [];
    const app = createEndpointApp({
      registryReader: new InMemoryRegistryReader([]),
      producer: {
        async publishAuthorized() {
          return 'event-1';
        },
        async publishRejected(payload) {
          rejectedEvents.push(payload);
          return 'event-2';
        },
      },
    });
    const envelope = await buildSignedEnvelopeBytes(1);
    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      headers: {
        'content-type': 'application/protobuf',
      },
      payload: Buffer.from(envelope.bytes),
    });

    expect(response.statusCode).toBe(403);
    expect(rejectedEvents[0]?.reasonCode).toBe(
      REJECTION_CODES.SENSOR_FORBIDDEN
    );
    await app.close();
  });

  it('returns 401 for stale timestamps', async () => {
    const rejectedEvents: Array<{
      reason_code: string;
      sensor_id?: string | undefined;
    }> = [];
    const app = createEndpointApp(
      {
        registryReader: new InMemoryRegistryReader([]),
        producer: {
          async publishAuthorized() {
            return 'event-1';
          },
          async publishRejected(payload) {
            rejectedEvents.push(payload);
            return 'event-2';
          },
        },
      },
      { timestampSkewSeconds: 300 }
    );
    const envelope = await buildSignedEnvelopeBytes(1, BigInt(1));
    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      headers: {
        'content-type': 'application/protobuf',
      },
      payload: Buffer.from(envelope.bytes),
    });

    expect(response.statusCode).toBe(401);
    expect(response.json()).toEqual({
      status: 'rejected',
      error_code: 'stale_timestamp',
    });
    expect(rejectedEvents[0]?.reasonCode).toBe(REJECTION_CODES.STALE_TIMESTAMP);
    await app.close();
  });

  it('accepts valid envelope for enabled sensor', async () => {
    const envelope = await buildSignedEnvelopeBytes(2);
    const authorized: unknown[] = [];
    const app = createEndpointApp({
      registryReader: new InMemoryRegistryReader([
        { sensorId: envelope.sensorId, enabled: true },
      ]),
      producer: {
        async publishAuthorized(payload) {
          authorized.push(payload);
          return 'event-1';
        },
        async publishRejected() {
          return 'event-2';
        },
      },
    });

    const response = await app.inject({
      method: 'POST',
      url: '/v1/telemetry',
      headers: {
        'content-type': 'application/protobuf',
      },
      payload: Buffer.from(envelope.bytes),
    });

    expect(response.statusCode).toBe(202);
    expect(authorized).toHaveLength(1);
    await app.close();
  });
});
