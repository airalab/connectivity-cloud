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
optionally compresses it with XZ, publishes it to one or more IPFS providers,
and emits the resulting Content-ID (CID) for downstream blockchain anchoring.

## Multi-Provider Publishing

`ipfs-publisher` supports publishing the same content to multiple IPFS-compatible
storage providers so that an outage of one provider does not block the pipeline.

- **Providers**: `kubo` (Kubo/go-ipfs RPC API) and `pinata` (Pinata pinning REST
  API) are supported today; more providers can be added behind the same
  `IpfsProvider` interface (`src/providers/types.ts`).
- **Ordering**: `IPFS_PROVIDERS` is an ordered, comma-separated list. The first
  entry is the **primary** provider and is always attempted first; the rest are
  fallbacks used for replication and durability.
- **Durability policy**: configurable via `IPFS_DURABILITY_POLICY`:
  - `any` (default) - at least one provider must succeed
  - `all` - every configured provider must succeed
  - `quorum` - at least `IPFS_DURABILITY_MIN_SUCCESS_COUNT` providers must succeed
- **Commit behavior**: the Kafka offset is only committed once the durability
  policy is satisfied. If it isn't, the message is retried (and eventually
  routed to the DLQ after exhausting retries), while a **background retry
  loop** keeps independently retrying only the providers that failed -
  providers that already succeeded are never re-uploaded.
- **Identical content**: compression happens once, before publishing, so every
  provider receives byte-identical input (and therefore the same CID, where
  providers use compatible content-addressing).

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `IPFS_PUBLISHER_GROUP_ID` (default: `ipfs-publisher-v1`)
- `IPFS_PUBLISHER_SOURCE` (default: `ipfs-publisher`)
- `IPFS_PUBLISHER_HEALTH_PORT` (default: `3040`)
- `IPFS_PUBLISHER_ENABLE_COMPRESSION` (default: `true`) - XZ compression before IPFS upload
- `IPFS_PROVIDERS` (default: `kubo`) - ordered, comma-separated provider list; first is primary (e.g. `pinata,kubo`)
- `IPFS_API_URL` (default: `http://localhost:5001`) - Kubo RPC endpoint (used by the `kubo` provider)
- `PINATA_API_URL` (default: `https://api.pinata.cloud`) - Pinata API base URL (used by the `pinata` provider)
- `PINATA_JWT` (default: none) - Pinata JWT bearer token (required if `pinata` is configured)
- `IPFS_DURABILITY_POLICY` (default: `any`) - `any` | `all` | `quorum`
- `IPFS_DURABILITY_MIN_SUCCESS_COUNT` (default: `1`) - minimum successful providers required for `quorum`
- `IPFS_PROVIDER_RETRY_BASE_DELAY_MS` (default: `5000`) - initial delay between independent per-provider replication retries
- `IPFS_PROVIDER_RETRY_MAX_DELAY_MS` (default: `60000`) - cap for the exponential backoff between replication retries
- `IPFS_PROVIDER_REPLICATION_MAX_PENDING` (default: `1000`) - max number of batches awaiting replication retry; oldest entries are dropped once exceeded

## Metrics

Available at `http://localhost:3040/metrics`:

- `consumed`: Total batched events consumed
- `batchesPublished`: Total batches successfully published (durability policy satisfied)
- `eventsPublished`: Total individual telemetry events archived to IPFS
- `duplicatesSkipped`: Redelivered batches skipped via `batch_id` result-event deduplication
- `publishFailure`: Failed publish attempts (all providers failed, or durability policy not satisfied)
- `durabilityFailures`: Number of publish attempts where at least one provider succeeded but the durability policy was not satisfied
- `pendingReplications`: Number of batches with at least one provider still pending replication retry
- `providers`: Per-provider `{ success, failure, avgLatencyMs }` counters

## Data Flow

1. Consume `telemetry.batched.v1` event from Kafka
2. Decode `TelemetryBatchedPayload` (carries a serialized `SignedEnvelopeBatch`)
3. Optionally compress the batch bytes with XZ (LZMA2), once, before publishing
4. Publish to the primary provider, then replicate to configured fallback providers
   - Providers that already succeeded for this `batch_id` (e.g. on redelivery) are skipped
5. Evaluate the configured durability policy against the per-provider outcomes
6. If satisfied, emit `ipfs.published.v1` (once per `batch_id`, deduplicated) and commit the Kafka offset
7. If not satisfied, the offset is left uncommitted (message is retried/DLQ'd), while a background
   loop continues retrying only the still-failing providers independently

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
- **IPFS Kubo** and/or **Pinata**: For permanent content-addressed storage, depending on `IPFS_PROVIDERS`

## Commit Safety

The service uses manual offset commits to ensure at-least-once delivery semantics:

1. Batch is published to the primary provider, then replicated to fallback providers
2. The configured durability policy is evaluated against per-provider outcomes
3. If satisfied, the result event (with the canonical CID) is published to Kafka
4. Kafka offset is committed

If durability is not satisfied, the offset remains uncommitted and the batch is
reprocessed; providers that already succeeded are tracked per `batch_id` and are
not re-uploaded on the next attempt. This ensures no telemetry is lost. In-process
`batch_id` deduplication avoids emitting a duplicate result event for a batch that
is redelivered after it already satisfied the durability policy.

## Compression

XZ compression (enabled by default) significantly reduces IPFS storage and bandwidth:

- **Compression level**: Default (level 6, LZMA2 algorithm)
- **Typical savings**: 60-80% for telemetry batches
- **Trade-off**: Slight CPU overhead during publish

Disable with `IPFS_PUBLISHER_ENABLE_COMPRESSION=false` for low-latency scenarios where storage is not a constraint.
