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
import {
  cryptoWaitReady,
  ed25519PairFromSeed,
  ed25519Sign,
} from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { validateSignedEnvelope, buildEnvelopeSigningBytes } from '@scp/core';
import { create, toBinary } from '@bufbuild/protobuf';

describe('verifyTelemetrySignature', () => {
  it('verifies protobuf envelope signature', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const payload = {
      sensorId: pair.publicKey,
      timestamp: BigInt(Date.now()),
      nonce: Uint8Array.from(Buffer.alloc(16, 8)),
      message: Uint8Array.from(Buffer.from('test-message')),
    };
    const signature = ed25519Sign(buildEnvelopeSigningBytes(payload), pair);
    const envelope = create(SignedEnvelopeSchema, {
      ...payload,
      signature,
    });
    await expect(
      validateSignedEnvelope(toBinary(SignedEnvelopeSchema, envelope), true)
    ).resolves.toStrictEqual(envelope);
  });

  it('returns false when envelope payload changes', async () => {
    await cryptoWaitReady();
    const seed = Uint8Array.from(Array.from({ length: 32 }, (_, i) => i + 1));
    const pair = ed25519PairFromSeed(seed);
    const payload = {
      sensorId: pair.publicKey,
      timestamp: BigInt(Date.now()),
      nonce: Uint8Array.from(Buffer.alloc(16, 8)),
      message: Uint8Array.from(Buffer.from('test-message')),
    };
    const signature = ed25519Sign(buildEnvelopeSigningBytes(payload), pair);
    const envelope = create(SignedEnvelopeSchema, {
      ...payload,
      message: Uint8Array.from(Buffer.from('mutated')),
      signature,
    });

    await expect(
      validateSignedEnvelope(toBinary(SignedEnvelopeSchema, envelope), true)
    ).rejects.toThrow('bad signature');
  });
});
