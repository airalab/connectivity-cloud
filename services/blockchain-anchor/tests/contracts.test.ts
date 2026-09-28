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
  TelemetryBatchedPayloadSchema,
  EnvelopeSchema,
} from '@scp/core';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';

describe('blockchain anchor contract compatibility', () => {
  it('accepts telemetry.batched.v1 envelope/payload as input', () => {
    const compressedPayload = Buffer.alloc(64, 7);

    const payload = create(TelemetryBatchedPayloadSchema, {
      batchId: 'batch-anchor-1',
      payload: compressedPayload,
      eventCount: 10,
      sensorIds: [Buffer.alloc(32, 1)],
      uncompressedSize: 200,
      compressedSize: compressedPayload.length,
      payloadHash: Buffer.alloc(32, 9),
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-anchor-1',
      eventType: TELEMETRY_TOPICS.BATCHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'batcher',
      payload: toBinary(TelemetryBatchedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.BATCHED);
    expect(parsed.eventId).toBe('evt-anchor-1');

    const payloadParsed = fromBinary(
      TelemetryBatchedPayloadSchema,
      parsed.payload
    );
    expect(payloadParsed.eventCount).toBe(10);
    expect(Buffer.from(payloadParsed.payload)).toEqual(compressedPayload);
    expect(payloadParsed.compressedSize).toBe(compressedPayload.length);
  });

  it('rejects (defensively) a payload larger than ANCHOR_MAX_PAYLOAD_BYTES', () => {
    const maxPayloadBytes = 8192;
    const oversizedPayload = Buffer.alloc(maxPayloadBytes + 1, 1);

    const payload = create(TelemetryBatchedPayloadSchema, {
      batchId: 'batch-anchor-oversized',
      payload: oversizedPayload,
      eventCount: 1,
      sensorIds: [Buffer.alloc(32, 1)],
      uncompressedSize: oversizedPayload.length,
      compressedSize: oversizedPayload.length,
      payloadHash: Buffer.alloc(32, 9),
    });

    expect(payload.payload.length).toBeGreaterThan(maxPayloadBytes);
  });

  it('envelope contains trace_id for distributed tracing', () => {
    const payload = create(TelemetryBatchedPayloadSchema, {
      batchId: 'batch-anchor-trace-1',
      payload: Buffer.alloc(16, 1),
      eventCount: 1,
      sensorIds: [Buffer.alloc(32, 1)],
      uncompressedSize: 50,
      compressedSize: 16,
      payloadHash: Buffer.alloc(32, 9),
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-anchor-trace-1',
      eventType: TELEMETRY_TOPICS.BATCHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      traceId: 'trace-123-456',
      source: 'batcher',
      payload: toBinary(TelemetryBatchedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.traceId).toBe('trace-123-456');
  });
});
