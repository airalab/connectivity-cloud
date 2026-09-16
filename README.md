# Connectivity Service Monorepo

TypeScript monorepo for the event-driven telemetry pipeline described in:

- [`project-architecture.md`](./docs/architecture/project-architecture.md)
- [`integration-guide.md`](./docs/architecture/integration-guide.md)
- [`cloud-run.md`](./docs/deployment/cloud-run.md) — Cloud Run deployment runbook

## Prerequisites

- Node.js 22+ (`.nvmrc`)
- pnpm 11+
- Docker + Docker Compose

## Quick start

Get the entire platform running with a single command:

```bash
make all
```

This will:
1. Install dependencies with pnpm
2. Build all packages and services
3. Start infrastructure (Kafka, Redis, IPFS, Robonomics)
4. Initialize Kafka topics
5. Start all services

## Workspace layout

```text
packages/contracts         # @scp/core - shared schemas, types, validation, consumer runtime
services/endpoint          # POST /v1/telemetry ingress - protobuf validation, signature verification
services/registry-sync     # Robonomics blockchain→Redis projection sync service
services/whitelist         # Whitelist-based sensor authentication provider
services/pubsub-broadcaster # Kafka→libp2p GossipSub bridge for real-time web UI
services/heartbeat-tracker  # Observability: sensor liveness & uptime metrics
services/batcher           # Kafka: batches authorized telemetry into telemetry.batched.v1
services/ipfs-publisher    # Kafka→IPFS publisher (publishes batches, produces CIDs)
services/blockchain-anchor # IPFS CID→Robonomics CPS pallet anchoring
tools/fake-sensor-cli      # Generate test telemetry with Ed25519 signatures
```

## Fake sensor telemetry CLI

Use this helper CLI to post fake telemetry messages for local debugging and integration testing.

Run from repo root:

```bash
pnpm fake-sensor -- \
  --endpoint http://localhost:3000/v1/telemetry \
  --signer-seed-hex 0x0101010101010101010101010101010101010101010101010101010101010101 \
  --count 5 \
  --interval-ms 1000 \
  --sensor-zone eu-west
```

Run directly in tools workspace:

```bash
pnpm --filter @scp/fake-sensor-cli fake-sensor -- --count 3
```

Available options:

- `--endpoint <url>` (env: `SENSOR_FAKE_ENDPOINT_URL`, default: `http://localhost:3000/v1/telemetry`)
- `--sensor-id <ss58>` (env: `SENSOR_FAKE_SENSOR_ID`; defaults to address derived from signer seed)
- `--signer-seed-hex <hex>` (env: `SENSOR_FAKE_SIGNER_SEED_HEX`, default: deterministic debug seed `0x00...01`)
- `--sensor-zone <zone>` (env: `SENSOR_FAKE_SENSOR_ZONE`, optional; sent as `X-Sensor-Zone`)
- `--count <n>` (env: `SENSOR_FAKE_COUNT`, default: `1`)
- `--interval-ms <ms>` (env: `SENSOR_FAKE_INTERVAL_MS`, default: `1000`)

The CLI now sends binary protobuf `crypto.v1.SignedEnvelope` (`Content-Type: application/protobuf`) and includes `X-Request-Id` on every request. It exits with a non-zero code on invalid options, request failures, or non-2xx responses.

### Rejection Reason Codes

When telemetry validation fails, the endpoint emits `telemetry.rejected.v1` events with one of these reason codes:

| Code | Constant | Description |
|------|----------|-------------|
| 1 | `STALE_TIMESTAMP` | Timestamp is outside the allowed clock skew window |
| 2 | `SENSOR_FORBIDDEN` | Sensor is not registered or has been disabled |
| 3 | `DUPLICATE_NONCE` | Nonce has already been used (replay attack prevention) |
| 4 | `INVALID_SIGNATURE` | Ed25519 signature verification failed |
| 999 | `KAFKA_PUBLISH_FAILED` | Internal error: Kafka publish failed after retries |

Use `REJECTION_CODES` from `@scp/core` to reference these codes in your code:

## Service overview

- **endpoint**: Validates `POST /v1/telemetry` (protobuf `crypto.v1.SignedEnvelope`), verifies Ed25519 signatures, checks sensor authorization via Redis projection, publishes `telemetry.authorized.v1` and `telemetry.rejected.v1`, returns `202` only after Kafka ACK. Supports pluggable authentication strategies (registry-sync or whitelist).
- **registry-sync**: Consumes finalized Robonomics blockchain events, projects sensor/key authorization state to Redis for endpoint lookups.
- **whitelist**: Alternative authentication provider - maintains static sensor whitelist in Redis, bypassing blockchain dependency for simpler deployments.
- **pubsub-broadcaster**: Consumes `telemetry.authorized.v1`, publishes to libp2p/GossipSub for real-time web UI, emits `telemetry.pubsub.result.v1`, routes exhausted failures to DLQ.
- **heartbeat-tracker**: Observability-only consumer of `telemetry.authorized.v1`, tracks sensor liveness (`firstSeen`, `lastSeen`, `onlineSince`) in Redis, exposes `sensors_online` count and per-sensor/aggregate uptime metrics over configurable online window (default 30s). Does not emit result events or participate in retry/DLQ.
- **batcher**: Consumes `telemetry.authorized.v1`, groups events into deterministic batches (by size, lag, and a bounded flush timer), and emits `telemetry.batched.v1`. Serializes flushes (single active flush per instance) and flushes pending batches on graceful shutdown.
- **ipfs-publisher**: Consumes `telemetry.batched.v1`, publishes/pins each batch to IPFS (optionally XZ-compressed), deduplicates by `batch_id`, emits `ipfs.published.v1`.
- **blockchain-anchor**: Consumes `ipfs.published.v1` and anchors CIDs to the Robonomics blockchain.
