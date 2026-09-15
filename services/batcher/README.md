# `@scp/batcher`

Groups authorized telemetry into deterministic batches and emits them for
downstream publication.

## Architecture

- **Pattern**: Standard consumer with manual commit
- **Input**: `telemetry.authorized.v1` from Kafka
- **Output**: `telemetry.batched.v1` (carries a serialized `SignedEnvelopeBatch`)
- **Autocommit**: Disabled (commit only after the batch is durably produced)
- **Concurrency**: Single active flush per instance (single-flight); a timer-triggered flush cannot publish the same batch as a size/lag-triggered flush
- **Shutdown**: Waits for any in-flight flush and flushes the remaining batch before closing resources

Separating batching from IPFS publishing keeps the publisher stateless and
lets each side scale and fail independently. The batcher owns buffering,
flush timing, and offset commits for the authorized topic; the
`@scp/ipfs-publisher` owns durable IPFS publication of `telemetry.batched.v1`.

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

## Metrics

Available at `http://localhost:3041/metrics`:

- `consumed`: Total authorized telemetry messages consumed
- `batchesProduced`: Total batches produced to `telemetry.batched.v1`
- `eventsBatched`: Total individual telemetry events batched
- `produceFailure`: Failed batch produce attempts (re-attached for retry)

## Data Flow

1. Consume `telemetry.authorized.v1` event from Kafka
2. Extract `SignedEnvelope` from payload and add to the current batch
3. When the batch is full, lag is high, or the flush timer fires:
   - Serialize the batch as a `SignedEnvelopeBatch` protobuf
   - Wrap it in a `TelemetryBatchedPayload` (`batch_id`, `event_count`, `sensor_ids`)
   - Produce a `telemetry.batched.v1` envelope to Kafka
   - Commit `telemetry.authorized.v1` offsets (only after the batch is produced)

## Result Event Schema

Published to `telemetry.batched.v1`:

```typescript
{
  batchId: string;               // Unique batch identifier
  signedEnvelopeBatch: Uint8Array; // Serialized crypto.v1.SignedEnvelopeBatch
  eventCount: number;            // Number of telemetry events in batch
  sensorIds: Uint8Array[];       // Sensor IDs present in the batch (observability)
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
