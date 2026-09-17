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
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { TELEMETRY_TOPICS } from '../src/topics.js';
import { validateSignedEnvelope } from '../src/utils.js';
import { EnvelopeSchema } from '../src/generated/connectivity/v1/envelope_pb.js';
import { TelemetryAuthorizedPayloadSchema } from '../src/generated/connectivity/v1/payload_pb.js';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';

describe('core', () => {
  it('creates and parses well-formed envelope + authorized payload', () => {
    const signedEnvelopeBytes = Buffer.alloc(100, 1);
    const payload = create(TelemetryAuthorizedPayloadSchema, {
      sensorId: Buffer.alloc(32, 1),
      signedEnvelope: signedEnvelopeBytes,
    });
    const payloadBytes = toBinary(TelemetryAuthorizedPayloadSchema, payload);

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-1',
      eventType: TELEMETRY_TOPICS.AUTHORIZED,
      eventVersion: 'v1',
      occurredAt: new Date().toISOString(),
      source: 'endpoint',
      payload: payloadBytes,
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.AUTHORIZED);
    expect(parsed.eventId).toBe('evt-1');
  });

  it('exposes exact stable topic constants', () => {
    expect(TELEMETRY_TOPICS.AUTHORIZED).toBe('telemetry.authorized.v1');
    expect(TELEMETRY_TOPICS.REJECTED).toBe('telemetry.rejected.v1');
    expect(TELEMETRY_TOPICS.BATCHED).toBe('telemetry.batched.v1');
    expect(TELEMETRY_TOPICS.IPFS_PUBLISHED).toBe('ipfs.published.v1');
    expect(TELEMETRY_TOPICS.DLQ).toBe('telemetry.dlq.v1');
    expect(Object.isFrozen(TELEMETRY_TOPICS)).toBe(true);
  });

  it('validates signed envelope protobuf bytes', async () => {
    const envelope = create(SignedEnvelopeSchema, {
      sensorId: Buffer.alloc(32, 1),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from('abc'),
      signature: Buffer.alloc(64, 4),
    });
    const envelopeBytes = toBinary(SignedEnvelopeSchema, envelope);
    const parsed = await validateSignedEnvelope(envelopeBytes);
    expect(parsed.sensorId.length).toBe(32);
    expect(parsed.signature.length).toBe(64);
  });
});
