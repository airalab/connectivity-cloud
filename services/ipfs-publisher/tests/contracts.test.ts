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
  TelemetryIpfsPublishedPayloadSchema,
  TelemetryIpfsPublishedPayload_Compression,
  EnvelopeSchema,
} from '@scp/core';
import {
  SignedEnvelopeSchema,
  SignedEnvelopeBatchSchema,
} from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { create, toBinary, fromBinary } from '@bufbuild/protobuf';
import { describe, expect, it } from 'vitest';
import { CID } from 'multiformats/cid';

describe('ipfs publisher contract compatibility', () => {
  it('accepts telemetry.batched.v1 envelope/payload as input', () => {
    const signedEnvelope = create(SignedEnvelopeSchema, {
      sensorId: Buffer.alloc(32, 1),
      timestamp: BigInt(Date.now()),
      nonce: Buffer.alloc(16, 2),
      message: Buffer.from(JSON.stringify({ temp: 20 })),
      signature: Buffer.alloc(64, 3),
    });
    const innerBatch = create(SignedEnvelopeBatchSchema, {
      batch: [signedEnvelope],
    });

    const payload = create(TelemetryBatchedPayloadSchema, {
      batchId: 'batch-contract-1',
      signedEnvelopeBatch: toBinary(SignedEnvelopeBatchSchema, innerBatch),
      eventCount: 1,
      sensorIds: [Buffer.alloc(32, 1)],
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'batch-contract-1',
      eventType: TELEMETRY_TOPICS.BATCHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'batcher',
      payload: toBinary(TelemetryBatchedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.BATCHED);
    expect(parsed.eventId).toBe('batch-contract-1');

    const payloadParsed = fromBinary(
      TelemetryBatchedPayloadSchema,
      parsed.payload
    );
    expect(payloadParsed.batchId).toBe('batch-contract-1');
    expect(payloadParsed.eventCount).toBe(1);
    const parsedInner = fromBinary(
      SignedEnvelopeBatchSchema,
      payloadParsed.signedEnvelopeBatch
    );
    expect(Buffer.from(parsedInner.batch[0]?.sensorId ?? [])).toEqual(
      Buffer.alloc(32, 1)
    );
  });

  it('produces valid SignedEnvelopeBatch for IPFS', () => {
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

    const batchBytes = toBinary(SignedEnvelopeBatchSchema, batch);
    const parsed = fromBinary(SignedEnvelopeBatchSchema, batchBytes);

    expect(parsed.batch).toHaveLength(2);
    expect(Buffer.from(parsed.batch[0]?.sensorId ?? [])).toEqual(
      Buffer.alloc(32, 1)
    );
    expect(Buffer.from(parsed.batch[1]?.sensorId ?? [])).toEqual(
      Buffer.alloc(32, 4)
    );
  });

  it('produces valid telemetry.ipfs.published.v1 result with compression NONE', () => {
    const fakeCid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(fakeCid.bytes),
      eventCount: 10,
      compression: TelemetryIpfsPublishedPayload_Compression.NONE,
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-ipfs-1',
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'ipfs-publisher',
      payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    expect(parsed.eventType).toBe(TELEMETRY_TOPICS.IPFS_PUBLISHED);
    expect(parsed.eventId).toBe('evt-ipfs-1');

    const payloadParsed = fromBinary(
      TelemetryIpfsPublishedPayloadSchema,
      parsed.payload
    );
    expect(payloadParsed.eventCount).toBe(10);
    expect(payloadParsed.compression).toBe(
      TelemetryIpfsPublishedPayload_Compression.NONE
    );
    expect(CID.decode(payloadParsed.cid).toString()).toBe(fakeCid.toString());
  });

  it('produces valid telemetry.ipfs.published.v1 result with compression XZ', () => {
    const fakeCid = CID.parse('QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG');

    const payload = create(TelemetryIpfsPublishedPayloadSchema, {
      cid: Buffer.from(fakeCid.bytes),
      eventCount: 50,
      compression: TelemetryIpfsPublishedPayload_Compression.XZ,
    });

    const envelope = create(EnvelopeSchema, {
      eventId: 'evt-ipfs-2',
      eventType: TELEMETRY_TOPICS.IPFS_PUBLISHED,
      eventVersion: '1.0.0',
      occurredAt: '2026-01-01T00:00:00Z',
      source: 'ipfs-publisher',
      payload: toBinary(TelemetryIpfsPublishedPayloadSchema, payload),
    });

    const envelopeBytes = toBinary(EnvelopeSchema, envelope);
    const parsed = fromBinary(EnvelopeSchema, envelopeBytes);

    const payloadParsed = fromBinary(
      TelemetryIpfsPublishedPayloadSchema,
      parsed.payload
    );
    expect(payloadParsed.eventCount).toBe(50);
    expect(payloadParsed.compression).toBe(
      TelemetryIpfsPublishedPayload_Compression.XZ
    );
  });

  it('compression enum values match protobuf definition', () => {
    expect(TelemetryIpfsPublishedPayload_Compression.NONE).toBe(0);
    expect(TelemetryIpfsPublishedPayload_Compression.XZ).toBe(1);
  });
});
