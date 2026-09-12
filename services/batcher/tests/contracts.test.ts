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
  TELEMETRY_TOPICS,
  TelemetryAuthorizedPayloadSchema,
  TelemetryBatchedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import {
  SignedEnvelopeSchema,
  SignedEnvelopeBatchSchema,
} from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

describe('batcher contract compatibility', () => {
  it('accepts telemetry.authorized.v1 envelope/payload as input', () => {
    const payload = create(TelemetryAuthorizedPayloadSchema, {
      sensorId: Buffer.alloc(32, 1),
      signedEnvelope: Buffer.alloc(100, 4),
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-contract-1',
      eventType: TELEMETRY_TOPICS.AUTHORIZED,
      eventVersion: 'v1',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'endpoint',
      payload: toBinary(TelemetryAuthorizedPayloadSchema, payload),
    });

    const parsed = fromBinary(
      EnvelopeSchema,
      toBinary(EnvelopeSchema, envelope)
    );

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.AUTHORIZED);
    const payloadParsed = fromBinary(
      TelemetryAuthorizedPayloadSchema,
      parsed.payload
    );
    expect(Buffer.from(payloadParsed.sensorId)).toEqual(Buffer.alloc(32, 1));
  });

  it('produces valid telemetry.batched.v1 envelope carrying a SignedEnvelopeBatch', () => {
    const signedEnvelope1 = create(SignedEnvelopeSchema, {
      sensorId: Buffer.alloc(32, 1),
      timestamp: BigInt(Date.now()),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from(JSON.stringify({ temp: 20 })),
      signature: Buffer.alloc(64, 3),
    });
    const signedEnvelope2 = create(SignedEnvelopeSchema, {
      sensorId: Buffer.alloc(32, 4),
      timestamp: BigInt(Date.now()),
      nonce: Buffer.alloc(16, 5),
      message: Buffer.from(JSON.stringify({ temp: 22 })),
      signature: Buffer.alloc(64, 6),
    });

    const batch = create(SignedEnvelopeBatchSchema, {
      batch: [signedEnvelope1, signedEnvelope2],
    });

    const batchedPayload = create(TelemetryBatchedPayloadSchema, {
      batchId: 'batch-1',
      signedEnvelopeBatch: toBinary(SignedEnvelopeBatchSchema, batch),
      eventCount: 2,
      sensorIds: [Buffer.alloc(32, 1), Buffer.alloc(32, 4)],
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'batch-1',
      eventType: TELEMETRY_TOPICS.BATCHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'batcher',
      payload: toBinary(TelemetryBatchedPayloadSchema, batchedPayload),
    });

    const parsed = fromBinary(
      EnvelopeSchema,
      toBinary(EnvelopeSchema, envelope)
    );

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.BATCHED);

    const payloadParsed = fromBinary(
      TelemetryBatchedPayloadSchema,
      parsed.payload
    );
    expect(payloadParsed.batchId).toBe('batch-1');
    expect(payloadParsed.eventCount).toBe(2);
    expect(payloadParsed.sensorIds).toHaveLength(2);

    // The carried SignedEnvelopeBatch round-trips intact.
    const innerBatch = fromBinary(
      SignedEnvelopeBatchSchema,
      payloadParsed.signedEnvelopeBatch
    );
    expect(innerBatch.batch).toHaveLength(2);
    expect(Buffer.from(innerBatch.batch[0]?.sensorId ?? [])).toEqual(
      Buffer.alloc(32, 1)
    );
    expect(Buffer.from(innerBatch.batch[1]?.sensorId ?? [])).toEqual(
      Buffer.alloc(32, 4)
    );
  });

  it('topic constant matches expected wire value', () => {
    expect(TELEMETRY_TOPICS.BATCHED).toBe('telemetry.batched.v1');
  });
});
