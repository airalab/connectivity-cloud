# Blockchain Anchor Service

Consumes chain-ready compressed telemetry batches from Kafka and anchors them to the Robonomics blockchain via the CPS (Cyber-Physical Systems) pallet.

## Overview

This service:
1. Subscribes to `telemetry.batched.v1` Kafka topic
2. Extracts the zstd-compressed batch payload bytes from each event
3. Defensively rejects (as a permanent error, offset committed) any payload
   larger than `ANCHOR_MAX_PAYLOAD_BYTES` — the batcher is expected to have
   already split batches to fit this limit, so this should only trigger on a
   producer bug or contract violation
4. Checks the current on-chain payload for the configured CPS node; if it
   already matches the batch payload bytes, the anchor operation is skipped
   as a no-op (see [Idempotency](#idempotency) below)
5. Otherwise calls `cps.setPayload(node_id, payload)` to anchor the
   compressed payload on-chain
6. Only commits Kafka offset after successful blockchain finalization (or a
   confirmed idempotent skip/rejection)

## Idempotency

`blockchain-anchor` uses manual Kafka commits and only commits the consumed
offset after the blockchain operation succeeds, which gives at-least-once
delivery of `telemetry.batched.v1` events. However, a process crash between
extrinsic finalization and the Kafka commit would redeliver the same event,
and Kafka offset management alone cannot make the blockchain side effect
exactly-once.

To close that gap, before submitting a `setPayload` extrinsic the service
queries the **current on-chain payload** for the configured CPS node
(`api.query.cps.payload(node_id)`) and compares it byte-for-byte against the
compressed payload it is about to anchor:

- If the on-chain payload **already matches**, the extrinsic is **not**
  resubmitted. The event is treated as already anchored, the
  `skippedDuplicate` metric is incremented, and the Kafka offset is
  committed as normal.
- If the on-chain payload differs (or is unset), the extrinsic is submitted
  as usual and the offset is committed only after finalization succeeds.

This makes the idempotency check authoritative: it relies on the actual
chain state rather than a local/Redis marker, so it also covers the crash
window between extrinsic finalization and the Kafka commit — on redelivery
the chain already reflects the anchor, so the duplicate is safely skipped.

The idempotency key is the compressed payload bytes themselves (deterministic
per batch, so stable across retries and redeliveries of the same event),
scoped to the configured `BLOCKCHAIN_ANCHOR_NODE_ID`.

## Configuration

Environment variables:

- `KAFKA_BROKERS` - Kafka broker addresses (default: `localhost:9092`)
- `BLOCKCHAIN_ANCHOR_GROUP_ID` - Kafka consumer group ID (default: `blockchain-anchor-v1`)
- `BLOCKCHAIN_ANCHOR_NODE_ID` - **Required** - CPS node ID to update with payload
- `BLOCKCHAIN_ANCHOR_SURI` - **Required** - Account seed/mnemonic for signing transactions
- `SUBSTRATE_WS_URL` - Substrate WebSocket endpoint (default: `ws://localhost:9944`)
- `BLOCKCHAIN_ANCHOR_HEALTH_PORT` - Health check server port (default: `3050`); Cloud Run `PORT` takes precedence
- `BLOCKCHAIN_ANCHOR_STATUS_REDIS_KEY` - Redis hash used for the last finalized batch observation (default: `blockchain-anchor:v1:last-finalized`)
- `BLOCKCHAIN_ANCHOR_OBSERVATION_STALE_MS` - Age after which the last finalized batch is labeled stale (default: 24 hours)
- `ANCHOR_MAX_PAYLOAD_BYTES` - Maximum accepted compressed payload size in bytes (default: `8192`, shared default defined in `@scp/core`)
- `LOG_LEVEL` - Logging level (default: `info`)

## CPS Pallet Integration

The service interacts with the Robonomics CPS pallet, which provides hierarchical Cyber-Physical Systems management. Each CPS node can have:

- **Metadata**: Configuration data (set once, rarely changed)
- **Payload**: Operational data (updated frequently)

This service uses `setPayload` to update a specific node's operational data with the zstd-compressed telemetry batch bytes.

### Extrinsic Format

```typescript
api.tx.cps.setPayload(node_id: u64, payload: Option<BoundedVec<u8>>)
```

The payload bytes are the raw zstd-compressed batch produced by the batcher
service, submitted unmodified (no reserialization or recompression).

## Usage

### Development

```bash
pnpm --filter @scp/blockchain-anchor dev
```

### Build

```bash
pnpm --filter @scp/blockchain-anchor build
```

### Run

```bash
# Set required environment variables
export BLOCKCHAIN_ANCHOR_NODE_ID=0
export BLOCKCHAIN_ANCHOR_SURI="//Alice"
export SUBSTRATE_WS_URL="ws://localhost:9944"

pnpm --filter @scp/blockchain-anchor build
node services/blockchain-anchor/dist/index.mjs
```

## Health Check

```bash
curl http://localhost:3050/health
```

## Metrics

```bash
curl http://localhost:3050/metrics
```

## Private status endpoint

`GET /status` is a private, read-only monitoring route. It reports real Kafka
consumer-group lag for `telemetry.batched.v1` using the configured anchor group,
free balance for the transaction signer using the chain's token metadata, best
and finalized block heights, finalization gap, and the persisted last finalized
batch observation. Kafka offset reads use the Admin API and never consume
records or commit offsets. Missing consumer offsets and unavailable components
are returned as `null`; status reads are cached for 15 seconds and do not affect
service readiness.

After a successful on-chain finalization, the service writes the observation to
Redis asynchronously with a monotonic block-height check. A Redis or RPC
observation failure is logged but does not fail the transaction or delay its
Kafka offset commit. Duplicate skips do not update this observation. Set the
shared `REDIS_*` connection variables used by the endpoint/heartbeat tracker
for persistence across restarts; without Redis, the last-batch fields remain
unavailable.

Returns:
```json
{
  "consumed": 42,
  "anchored": 40,
  "skippedDuplicate": 1,
  "rejectedOversized": 0,
  "failed": 1
}
```

- `consumed`: Total batched telemetry events consumed
- `anchored`: Successfully anchored payloads
- `skippedDuplicate`: Redelivered events skipped because the payload was already anchored on-chain
- `rejectedOversized`: Batches rejected because the compressed payload exceeded `ANCHOR_MAX_PAYLOAD_BYTES`
- `failed`: Failed anchoring attempts

## Error Handling

- **Invalid messages**: Skipped (offset committed)
- **Oversized payloads**: Rejected as a permanent contract violation (offset committed, not retried)
- **Blockchain errors**: Message not committed, will be retried
- **Connection loss**: Service stops, must be restarted

## Integration

This service is part of the telemetry pipeline:

```
[batcher] → telemetry.batched.v1 → [blockchain-anchor] → Robonomics CPS pallet
```

The anchored payloads provide an immutable on-chain audit trail of published telemetry batches.
