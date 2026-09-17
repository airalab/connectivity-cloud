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
  encodeAddress,
  ed25519Verify,
  cryptoWaitReady,
} from '@polkadot/util-crypto';
import { fromBinary } from '@bufbuild/protobuf';
import {
  SignedEnvelopeSchema,
  type SignedEnvelope,
} from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';

export const SENSOR_ID_LENGTH = 32;
export const SIGNATURE_LENGTH = 64;
export const MIN_NONCE_LENGTH = 16;
export const MAX_NONCE_LENGTH = 32;

export function timestampToLeBytes(timestamp: bigint): Uint8Array {
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, timestamp, true);
  return out;
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, p) => sum + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

export function buildEnvelopeSigningBytes(
  envelope: Pick<SignedEnvelope, 'sensorId' | 'nonce' | 'message'>
): Uint8Array {
  return concatBytes([envelope.sensorId, envelope.nonce, envelope.message]);
}

/**
 * Parse and validate SignedEnvelope from protobuf bytes.
 * Uses generated @bufbuild/protobuf code for parsing.
 */
export async function validateSignedEnvelope(
  bytes: Uint8Array,
  checkSignature?: boolean | undefined
): Promise<SignedEnvelope> {
  const envelope = fromBinary(SignedEnvelopeSchema, bytes);
  if (envelope.sensorId.length !== SENSOR_ID_LENGTH) {
    throw new Error(`sensor_id must be ${SENSOR_ID_LENGTH} bytes`);
  }
  if (envelope.signature.length !== SIGNATURE_LENGTH) {
    throw new Error(`signature must be ${SIGNATURE_LENGTH} bytes`);
  }
  if (
    envelope.nonce.length < MIN_NONCE_LENGTH ||
    envelope.nonce.length > MAX_NONCE_LENGTH
  ) {
    throw new Error(
      `nonce must be ${MIN_NONCE_LENGTH}-${MAX_NONCE_LENGTH} bytes`
    );
  }
  if (envelope.message.length === 0) {
    throw new Error('message must be non-empty');
  }

  if (checkSignature) {
    await cryptoWaitReady();
    const isSignatureValid = ed25519Verify(
      buildEnvelopeSigningBytes(envelope),
      envelope.signature,
      envelope.sensorId
    );
    if (!isSignatureValid) {
      throw new Error('bad signature');
    }
  }

  return envelope;
}

/**
 * Format sensor ID bytes as SS58 address.
 */
export function formatSensorId(sensorId: Uint8Array): string {
  return encodeAddress(sensorId, 32);
}
