# `@scp/batcher`

Groups authorized telemetry into deterministic batches, compresses and
size-fits them, and emits chain-ready payloads for direct blockchain
anchoring.

## Architecture

- **Pattern**: Standard consumer with manual commit
- **Input**: `telemetry.authorized.v1` from Kafka
- **Output**: `telemetry.batched.v1` (carries a zstd-compressed, chain-ready payload)
- **Autocommit**: Disabled (commit only after the batch is durably produced)
- **Concurrency**: Single active flush per instance (single-flight); a timer-triggered flush cannot publish the same batch as a size/lag-triggered flush
- **Shutdown**: Waits for any in-flight flush and flushes the remaining batch before closing resources

The batcher serializes each detached batch as a `SignedEnvelopeBatch`
protobuf, zstd-compresses it, and — if the compressed result exceeds
`ANCHOR_MAX_PAYLOAD_BYTES` — recursively splits it into smaller sub-batches
until every emitted payload fits. Each sub-batch becomes its own
`telemetry.batched.v1` message with its own `batch_id`. A single event whose
compressed size alone exceeds the limit cannot be split further and is
routed to the dead-letter queue instead of blocking the rest of the batch.

This keeps `blockchain-anchor` simple and stateless: it only needs to submit
already-compressed, already-size-checked bytes via `cps.setPayload`, with no
IPFS/CID indirection in between.

## Batching Strategy

- **Size-based flush**: Flushes when `BATCHER_BATCH_SIZE` messages are accumulated
- **Lag-based flush**: When consumer lag is high (>= batch size), flushes early to catch up
- **Time-based flush**: When lag is low, a bounded timer (`BATCHER_BATCH_TIMEOUT_MS`) flushes partial batches
- **Shutdown flush**: Any pending batch is flushed during graceful shutdown to avoid replay

The current batch is atomically detached before any asynchronous work, so
concurrent flush paths can never publish the same batch twice.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `BATCHER_GROUP_ID` (default: `batcher-v1`)
- `BATCHER_SOURCE` (default: `batcher`)
- `BATCHER_HEALTH_PORT` (default: `3041`)
- `BATCHER_BATCH_SIZE` (default: `10`) - Maximum messages per batch
- `BATCHER_BATCH_TIMEOUT_MS` (default: `30000`) - Timeout for partial batches under low load
- `ANCHOR_MAX_PAYLOAD_BYTES` (default: `8192`) - Maximum compressed payload size per emitted batch; larger batches are recursively split, shared default defined in `@scp/core`

## Metrics

Available at `http://localhost:3041/metrics`:

- `consumed`: Total authorized telemetry messages consumed
- `batchesProduced`: Total batches produced to `telemetry.batched.v1`
- `eventsBatched`: Total individual telemetry events batched
- `batchesSplit`: Total detached batches that required splitting into multiple sub-batches
- `oversizedEvents`: Total individual events that could not fit even alone and were routed to the DLQ
- `produceFailure`: Failed batch produce attempts (re-attached for retry)

## Data Flow

1. Consume `telemetry.authorized.v1` event from Kafka
2. Extract `SignedEnvelope` from payload and add to the current batch
3. When the batch is full, lag is high, or the flush timer fires:
   - Serialize the batch as a `SignedEnvelopeBatch` protobuf and zstd-compress it
   - If the compressed payload exceeds `ANCHOR_MAX_PAYLOAD_BYTES`, recursively split the batch until every sub-batch fits (oversized single events go to the DLQ)
   - Wrap each fitted sub-batch in a `TelemetryBatchedPayload` (`batch_id`, `payload`, `uncompressed_size`, `compressed_size`, `payload_hash`, `event_count`, `sensor_ids`)
   - Produce one `telemetry.batched.v1` envelope per sub-batch to Kafka
   - Commit `telemetry.authorized.v1` offsets (only after all sub-batches are produced)

## Result Event Schema

Published to `telemetry.batched.v1`:

```typescript
{
  batchId: string;              // Unique batch identifier (per sub-batch)
  payload: Uint8Array;          // zstd-compressed, chain-ready crypto.v1.SignedEnvelopeBatch
  uncompressedSize: number;     // Size of the serialized batch before compression
  compressedSize: number;       // Size of `payload` (<= ANCHOR_MAX_PAYLOAD_BYTES)
  payloadHash: Uint8Array;      // blake2_256 hash of `payload`
  eventCount: number;           // Number of telemetry events in this sub-batch
  sensorIds: Uint8Array[];      // Sensor IDs present in this sub-batch (observability)
}
```

## Development

```bash
# Start batcher
pnpm --filter @scp/batcher dev

# Build
pnpm --filter @scp/batcher build

# Run tests
pnpm --filter @scp/batcher test

# Type check
pnpm --filter @scp/batcher typecheck

# Lint
pnpm --filter @scp/batcher lint
```

## Dependencies

- **Kafka**: For consuming authorized telemetry and producing batched events

## Commit Safety

The batcher commits `telemetry.authorized.v1` offsets only after the batch is
durably produced to `telemetry.batched.v1`. If producing fails, the batch is
re-attached and retried on the next flush, and offsets stay uncommitted so no
telemetry is lost.
