# Blockchain Anchor Service

Consumes IPFS publish events from Kafka and anchors them to the Robonomics blockchain via the CPS (Cyber-Physical Systems) pallet.

## Overview

This service:
1. Subscribes to `ipfs.published.v1` Kafka topic
2. Extracts IPFS CID from each event
3. Checks the current on-chain payload for the configured CPS node; if it
   already matches the CID, the anchor operation is skipped as a no-op (see
   [Idempotency](#idempotency) below)
4. Otherwise calls `cps.setPayload(node_id, cid)` to anchor the CID on-chain
5. Only commits Kafka offset after successful blockchain finalization (or a
   confirmed idempotent skip)

## Idempotency

`blockchain-anchor` uses manual Kafka commits and only commits the consumed
offset after the blockchain operation succeeds, which gives at-least-once
delivery of `ipfs.published.v1` events. However, a process crash between
extrinsic finalization and the Kafka commit would redeliver the same event,
and Kafka offset management alone cannot make the blockchain side effect
exactly-once.

To close that gap, before submitting a `setPayload` extrinsic the service
queries the **current on-chain payload** for the configured CPS node
(`api.query.cps.payload(node_id)`) and compares it against the CID it is
about to anchor:

- If the on-chain payload **already matches** the CID, the extrinsic is
  **not** resubmitted. The event is treated as already anchored, the
  `skippedDuplicate` metric is incremented, and the Kafka offset is
  committed as normal.
- If the on-chain payload differs (or is unset), the extrinsic is submitted
  as usual and the offset is committed only after finalization succeeds.

This makes the idempotency check authoritative: it relies on the actual
chain state rather than a local/Redis marker, so it also covers the crash
window between extrinsic finalization and the Kafka commit — on redelivery
the chain already reflects the anchor, so the duplicate is safely skipped.

The idempotency key is the CID itself (derived from IPFS content, so it is
stable across retries and redeliveries of the same event), scoped to the
configured `BLOCKCHAIN_ANCHOR_NODE_ID`.

## Configuration

Environment variables:

- `KAFKA_BROKERS` - Kafka broker addresses (default: `localhost:9092`)
- `BLOCKCHAIN_ANCHOR_GROUP_ID` - Kafka consumer group ID (default: `blockchain-anchor-v1`)
- `BLOCKCHAIN_ANCHOR_NODE_ID` - **Required** - CPS node ID to update with payload
- `BLOCKCHAIN_ANCHOR_SURI` - **Required** - Account seed/mnemonic for signing transactions
- `SUBSTRATE_WS_URL` - Substrate WebSocket endpoint (default: `ws://localhost:9944`)
- `BLOCKCHAIN_ANCHOR_HEALTH_PORT` - Health check server port (default: `3050`)
- `LOG_LEVEL` - Logging level (default: `info`)

## CPS Pallet Integration

The service interacts with the Robonomics CPS pallet, which provides hierarchical Cyber-Physical Systems management. Each CPS node can have:

- **Metadata**: Configuration data (set once, rarely changed)
- **Payload**: Operational data (updated frequently)

This service uses `setPayload` to update a specific node's operational data with the IPFS CID of the published telemetry batch.

### Extrinsic Format

```typescript
api.tx.cps.setPayload(node_id: u64, payload: Option<BoundedVec<u8>>)
```

The CID is encoded as a UTF-8 string in the payload bytes.

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

Returns:
```json
{
  "consumed": 42,
  "anchored": 40,
  "skippedDuplicate": 1,
  "failed": 1
}
```

- `consumed`: Total IPFS publish events consumed
- `anchored`: Successfully anchored CIDs
- `skippedDuplicate`: Redelivered events skipped because the CID was already anchored on-chain
- `failed`: Failed anchoring attempts

## Error Handling

- **Invalid messages**: Skipped (offset committed)
- **Blockchain errors**: Message not committed, will be retried
- **Connection loss**: Service stops, must be restarted

## Integration

This service is part of the telemetry pipeline:

```
[ipfs-publisher] → ipfs.published.v1 → [blockchain-anchor] → Robonomics CPS pallet
```

The anchored CIDs provide an immutable on-chain audit trail of published telemetry batches.
