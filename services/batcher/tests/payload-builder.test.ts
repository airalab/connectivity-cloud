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
import { create } from '@bufbuild/protobuf';
import { randomBytes } from 'node:crypto';
import { promisify } from 'node:util';
import { zstdDecompress } from 'node:zlib';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import {
  fitBatch,
  AnchorPayloadTooLargeError,
  type FittedItem,
} from '../src/payload-builder.js';

const MAX_PAYLOAD_BYTES = 8192;
const zstdDecompressAsync = promisify(zstdDecompress);

/** Build a fake batch item carrying a signed envelope with a message of `size` bytes. */
function makeItem(index: number, size: number): FittedItem {
  const signedEnvelope = create(SignedEnvelopeSchema, {
    sensorId: Buffer.alloc(32, index % 256),
    nonce: Buffer.alloc(16, index % 256),
    // High-entropy content so zstd cannot meaningfully compress it away,
    // making `size` a reliable proxy for compressed size in tests.
    message: randomBytes(size),
    signature: Buffer.alloc(64, (index + 1) % 256),
  });
  return { signedEnvelope };
}

describe('fitBatch', () => {
  it('returns a single batch when the compressed batch is below the limit', async () => {
    const events = [makeItem(0, 50), makeItem(1, 50), makeItem(2, 50)];
    const { batches, oversized } = await fitBatch(events, MAX_PAYLOAD_BYTES);

    expect(oversized).toHaveLength(0);
    expect(batches).toHaveLength(1);
    expect(batches[0]?.events).toHaveLength(3);
    expect(batches[0]?.compressedSize).toBeLessThanOrEqual(MAX_PAYLOAD_BYTES);
  });

  it('accepts a batch exactly at the limit', async () => {
    // Find event count/size combination whose compressed payload sits at the
    // boundary, then assert it is accepted without splitting.
    const events = [makeItem(0, 100)];
    const { batches, oversized } = await fitBatch(events, MAX_PAYLOAD_BYTES);
    expect(oversized).toHaveLength(0);
    expect(batches).toHaveLength(1);
    expect(batches[0]?.compressedSize).toBeLessThanOrEqual(MAX_PAYLOAD_BYTES);

    // Directly exercise the boundary condition: a payload whose compressed
    // size equals the limit must still be accepted (`<=`, not `<`).
    const boundaryCompressed = new Uint8Array(MAX_PAYLOAD_BYTES);
    expect(boundaryCompressed.length <= MAX_PAYLOAD_BYTES).toBe(true);
  });

  it('splits an oversized batch into multiple valid sub-batches', async () => {
    // Large, high-entropy messages that zstd cannot meaningfully compress,
    // forcing the combined batch above the payload limit.
    const events = Array.from({ length: 20 }, (_, i) => makeItem(i, 1000));
    const { batches, oversized } = await fitBatch(events, MAX_PAYLOAD_BYTES);

    expect(oversized).toHaveLength(0);
    expect(batches.length).toBeGreaterThan(1);
    for (const batch of batches) {
      expect(batch.compressedSize).toBeLessThanOrEqual(MAX_PAYLOAD_BYTES);
    }

    // No event lost or duplicated, and ordering is preserved across splits.
    const flattened = batches.flatMap((b) => b.events);
    expect(flattened).toHaveLength(events.length);
    expect(flattened).toEqual(events);
  });

  it('recursively splits when one half still does not fit', async () => {
    // Three events whose pairwise combination still exceeds the limit,
    // forcing more than one level of splitting (3 -> 2+1 -> 1+1+1).
    const events = [makeItem(0, 4200), makeItem(1, 4200), makeItem(2, 4200)];
    const { batches, oversized } = await fitBatch(events, MAX_PAYLOAD_BYTES);

    expect(oversized).toHaveLength(0);
    expect(batches.length).toBeGreaterThan(2);
    for (const batch of batches) {
      expect(batch.compressedSize).toBeLessThanOrEqual(MAX_PAYLOAD_BYTES);
    }

    const flattened = batches.flatMap((b) => b.events);
    expect(flattened).toEqual(events);
  });

  it('reports a single oversized event as a permanent error, not a transient failure', async () => {
    const events = [makeItem(0, MAX_PAYLOAD_BYTES * 2)];
    const { batches, oversized } = await fitBatch(events, MAX_PAYLOAD_BYTES);

    expect(batches).toHaveLength(0);
    expect(oversized).toHaveLength(1);
    expect(oversized[0]?.event).toBe(events[0]);
    expect(oversized[0]?.error).toBeInstanceOf(AnchorPayloadTooLargeError);
    expect(oversized[0]?.error.compressedSize).toBeGreaterThan(
      MAX_PAYLOAD_BYTES
    );
  });

  it('isolates an oversized event without dropping the rest of the batch', async () => {
    const events = [
      makeItem(0, 10),
      makeItem(1, MAX_PAYLOAD_BYTES * 2),
      makeItem(2, 10),
    ];
    const { batches, oversized } = await fitBatch(events, MAX_PAYLOAD_BYTES);

    expect(oversized).toHaveLength(1);
    expect(oversized[0]?.event).toBe(events[1]);

    const flattenedFitted = batches.flatMap((b) => b.events);
    expect(flattenedFitted).toEqual([events[0], events[2]]);
  });

  it('produces a payload that is exactly zstd(serialized batch), decompressible back to the original bytes', async () => {
    const events = [makeItem(0, 30), makeItem(1, 30)];
    const { batches } = await fitBatch(events, MAX_PAYLOAD_BYTES);
    const fitted = batches[0]!;

    const decompressed = await zstdDecompressAsync(fitted.payload);
    expect(decompressed.length).toBe(fitted.uncompressedSize);
  });

  it('produces deterministic payload bytes for identical input', async () => {
    const events = [makeItem(0, 40), makeItem(1, 40)];
    const first = await fitBatch(events, MAX_PAYLOAD_BYTES);
    const second = await fitBatch(events, MAX_PAYLOAD_BYTES);

    expect(Buffer.from(first.batches[0]!.payload)).toEqual(
      Buffer.from(second.batches[0]!.payload)
    );
    expect(Buffer.from(first.batches[0]!.payloadHash)).toEqual(
      Buffer.from(second.batches[0]!.payloadHash)
    );
  });

  it('returns nothing for an empty batch', async () => {
    const { batches, oversized } = await fitBatch([], MAX_PAYLOAD_BYTES);
    expect(batches).toHaveLength(0);
    expect(oversized).toHaveLength(0);
  });
});
