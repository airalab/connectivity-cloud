# `@scp/ipfs-publisher`

Publishes batched telemetry to IPFS for permanent archival storage.

## Architecture

- **Pattern**: Standard consumer with manual commit (durable archival)
- **Input**: `telemetry.batched.v1` from Kafka (produced by `@scp/batcher`)
- **Output**: Batches published to IPFS + result events to `ipfs.published.v1`
- **Autocommit**: Disabled (commit only after IPFS confirmation)
- **Failure handling**: On failure the offset is not committed and the batch is redelivered; in-process `batch_id` deduplication prevents double publication of redelivered batches

Batching is handled upstream by `@scp/batcher`. This service consumes each
`telemetry.batched.v1` event, which carries a serialized `SignedEnvelopeBatch`,
optionally compresses it with XZ, publishes it to IPFS, and emits the resulting
Content-ID (CID) for downstream blockchain anchoring.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `IPFS_PUBLISHER_GROUP_ID` (default: `ipfs-publisher-v1`)
- `IPFS_PUBLISHER_SOURCE` (default: `ipfs-publisher`)
- `IPFS_PUBLISHER_HEALTH_PORT` (default: `3040`)
- `IPFS_API_URL` (default: `http://localhost:5001`) - Kubo RPC endpoint
- `IPFS_PUBLISHER_ENABLE_COMPRESSION` (default: `true`) - XZ compression before IPFS upload

## Metrics

Available at `http://localhost:3040/metrics`:

- `consumed`: Total batched events consumed
- `batchesPublished`: Total batches successfully published to IPFS
- `eventsPublished`: Total individual telemetry events archived to IPFS
- `duplicatesSkipped`: Redelivered batches skipped via `batch_id` deduplication
- `publishFailure`: Failed IPFS publish attempts (offset not committed, redelivered)

## Data Flow

1. Consume `telemetry.batched.v1` event from Kafka
2. Decode `TelemetryBatchedPayload` (carries a serialized `SignedEnvelopeBatch`)
3. Skip if `batch_id` was already published in this process
4. Otherwise:
   - Optionally compress the batch bytes with XZ (LZMA2)
   - Upload to IPFS via Kubo RPC
   - Receive CID from IPFS
   - Emit `ipfs.published.v1` result event to Kafka
5. Commit Kafka offset (only after successful upload + publish, or dedup skip)

## Result Event Schema

Published to `ipfs.published.v1`:

```typescript
{
  cid: Uint8Array;          // CID bytes (use multiformats/cid to parse)
  eventCount: number;        // Number of telemetry events in batch
  compression: Compression;  // NONE or XZ
}
```

## Development

```bash
# Start ipfs-publisher
pnpm --filter @scp/ipfs-publisher dev

# Build
pnpm --filter @scp/ipfs-publisher build

# Run tests
pnpm --filter @scp/ipfs-publisher test

# Type check
pnpm --filter @scp/ipfs-publisher typecheck

# Lint
pnpm --filter @scp/ipfs-publisher lint
```

## Dependencies

- **Kafka**: For consuming batched telemetry and publishing results
- **IPFS Kubo**: For permanent content-addressed storage (RPC API on port 5001)

## Commit Safety

The service uses manual offset commits to ensure at-least-once delivery semantics:

1. Batch is published to IPFS
2. CID is received from IPFS
3. Result event is published to Kafka
4. Kafka offset is committed

If any step fails, the offset remains uncommitted and the batch is reprocessed.
This ensures no telemetry is lost. In-process `batch_id` deduplication avoids
republishing a batch that is redelivered after a successful publish.

## Compression

XZ compression (enabled by default) significantly reduces IPFS storage and bandwidth:

- **Compression level**: Default (level 6, LZMA2 algorithm)
- **Typical savings**: 60-80% for telemetry batches
- **Trade-off**: Slight CPU overhead during publish

Disable with `IPFS_PUBLISHER_ENABLE_COMPRESSION=false` for low-latency scenarios where storage is not a constraint.
