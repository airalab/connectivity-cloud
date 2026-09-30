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
import { toBinary, create } from '@bufbuild/protobuf';
import { promisify } from 'node:util';
import { constants, zstdCompress } from 'node:zlib';
import { blake2AsU8a } from '@polkadot/util-crypto';
import {
  SignedEnvelopeBatchSchema,
  type SignedEnvelope,
} from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';

/**
 * Error describing a single event that does not fit within
 * `ANCHOR_MAX_PAYLOAD_BYTES` even after serialization and zstd compression on
 * its own. This cannot be resolved by further splitting and must be treated
 * as a permanent validation error (e.g. routed to the DLQ), not retried as a
 * transient failure.
 */
export class AnchorPayloadTooLargeError extends Error {
  constructor(
    public readonly compressedSize: number,
    public readonly maxPayloadBytes: number
  ) {
    super(
      `single event payload (${compressedSize} bytes compressed) exceeds ` +
        `ANCHOR_MAX_PAYLOAD_BYTES (${maxPayloadBytes} bytes) and cannot be split`
    );
    this.name = 'AnchorPayloadTooLargeError';
  }
}

/** A generic item that can be serialized into a `SignedEnvelopeBatch`. */
export interface FittedItem {
  signedEnvelope: SignedEnvelope;
}

/** A chain-ready sub-batch produced by {@link fitBatch}. */
export interface FittedBatch<T extends FittedItem> {
  /** The events contained in this sub-batch, in original order. */
  events: readonly T[];
  /** zstd(serialized SignedEnvelopeBatch) — exactly the bytes for `set_payload`. */
  payload: Uint8Array;
  /** Size of the serialized batch before compression. */
  uncompressedSize: number;
  /** Size of `payload`; always `<= maxPayloadBytes`. */
  compressedSize: number;
  /** blake2_256 hash of `payload`. */
  payloadHash: Uint8Array;
}

/** A single event that does not fit even on its own, after splitting bottomed out. */
export interface OversizedItem<T extends FittedItem> {
  event: T;
  error: AnchorPayloadTooLargeError;
}

export interface FitBatchOutcome<T extends FittedItem> {
  /** Chain-ready sub-batches, in original event order. */
  batches: FittedBatch<T>[];
  /** Events that could not fit on their own; a permanent error, not a transient one. */
  oversized: OversizedItem<T>[];
}

const zstdCompressAsync = promisify(zstdCompress);

/** Zstandard compression level; higher is smaller, output remains a standard zstd frame. */
const ZSTD_COMPRESSION_LEVEL = 19;

/** Compress bytes into a single standard zstd frame. */
async function compressZstd(input: Uint8Array): Promise<Uint8Array> {
  return zstdCompressAsync(input, {
    params: { [constants.ZSTD_c_compressionLevel]: ZSTD_COMPRESSION_LEVEL },
  });
}

/** Serialize a set of signed envelopes into a `SignedEnvelopeBatch` and zstd compress it. */
async function serializeAndCompress<T extends FittedItem>(
  events: readonly T[]
): Promise<{ uncompressed: Uint8Array; compressed: Uint8Array }> {
  const batch = create(SignedEnvelopeBatchSchema, {
    batch: events.map((e) => e.signedEnvelope),
  });
  const uncompressed = toBinary(SignedEnvelopeBatchSchema, batch);
  const compressed = await compressZstd(uncompressed);
  return { uncompressed, compressed };
}

/**
 * Recursively fit `events` into one or more chain-ready payloads that each
 * satisfy `compressed_payload.len <= maxPayloadBytes`.
 *
 * If the compressed batch does not fit, it is split in half and each half is
 * fitted independently (recursive binary splitting). If a single event still
 * does not fit after serialization and compression, it is reported in
 * `oversized` rather than aborting the rest of the batch — this preserves
 * every other event and mirrors the existing poison-item / DLQ handling
 * pattern used for unparseable envelopes.
 *
 * Event order is preserved across the returned sub-batches and no event is
 * lost or duplicated between `batches` and `oversized`.
 */
export async function fitBatch<T extends FittedItem>(
  events: readonly T[],
  maxPayloadBytes: number
): Promise<FitBatchOutcome<T>> {
  if (events.length === 0) {
    return { batches: [], oversized: [] };
  }

  const { uncompressed, compressed } = await serializeAndCompress(events);

  if (compressed.length <= maxPayloadBytes) {
    return {
      batches: [
        {
          events,
          payload: compressed,
          uncompressedSize: uncompressed.length,
          compressedSize: compressed.length,
          payloadHash: blake2AsU8a(compressed, 256),
        },
      ],
      oversized: [],
    };
  }

  if (events.length === 1) {
    return {
      batches: [],
      oversized: [
        {
          event: events[0]!,
          error: new AnchorPayloadTooLargeError(
            compressed.length,
            maxPayloadBytes
          ),
        },
      ],
    };
  }

  const middle = Math.ceil(events.length / 2);
  const [first, second] = await Promise.all([
    fitBatch(events.slice(0, middle), maxPayloadBytes),
    fitBatch(events.slice(middle), maxPayloadBytes),
  ]);

  return {
    batches: [...first.batches, ...second.batches],
    oversized: [...first.oversized, ...second.oversized],
  };
}
