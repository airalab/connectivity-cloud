# General Architecture

## Purpose

The system accepts Ed25519-signed environmental sensor telemetry (Altruist-series sensors), verifies authenticity and authorization, and distributes trusted events to downstream channels with Kafka as the central durable bus.

## High-level architecture

`Sensor -> Endpoint -> Message Bus (Kafka) -> Services (PubSub, Batcher, Blockchain Anchor)`

> Authorization source: `SENSOR_AUTH_STRATEGY` selects how the Endpoint authorizes sensors, in memory (`whitelist`, `none`) or via cached read-only CPS lookups (`cps`):
> - `whitelist` (default): `WHITELIST_SENSOR_IDS` allowlist; an empty list authorizes nobody.
> - `cps`: sensor must be a descendant of the `sensors.social` CPS root node; positive results are cached in a bounded LRU, negative results for 60s, RPC errors fail closed.
> - `none`: any validly-signed sensor is authorized (signature, timestamp and nonce checks still apply).

### PubSub Broadcast

`Trusted Messages (Kafka) -> PubSub Broadcaster -> libp2p GossipSub -> Web UI (sensors.social)`

### Heartbeat observability

`Trusted Messages (Kafka) -> Heartbeat Tracker -> /metrics (liveness + uptime)`

### Blockchain anchoring

`Trusted Messages (Kafka) -> Batcher -> Message Bus (Kafka) -> Blockchain Anchor -> Robonomics Blockchain`

### Status dashboard flow

`Browser -> Endpoint GET / (static HTML) -> same-origin /api/status* -> endpoint Status Collector -> IAM-authenticated private service URLs`

## Module responsibilities

### Sensor
- Out of scope for this project phase (designed and implemented by third party).
- Sends telemetry via `POST /v1/telemetry`.
- Wire format: protobuf `crypto.v1.SignedEnvelope` (`Content-Type: application/protobuf`).
- Includes Ed25519 signature over `sensor_id || timestamp_le || nonce || message` and anti-replay fields.
- Retries same payload safely when delivery fails.

### Endpoint
- Validates protobuf `crypto.v1.SignedEnvelope` schema and field constraints.
- Verifies Ed25519 signature over raw envelope bytes.
- Enforces timestamp window policy and replay protection via an atomic Redis nonce claim (`SET ... NX EX`, shared by all instances and strategies) made before the Kafka publish.
- Checks sensor authorization using a pluggable strategy (`whitelist` allowlist or `none`, which accepts any validly-signed sensor).
- Publishes authorized events to `telemetry.authorized.v1` and rejected events to `telemetry.rejected.v1`.
- Returns `202` only after Kafka ACK.
- Serves the status page at `GET /` (see [Status page](#status-page)), plus `GET /health` and `GET /metrics`.

### Whitelist
- Sensor authentication provider used by the Endpoint's `whitelist` strategy.
- Maintains a static, in-memory sensor allowlist from `WHITELIST_SENSOR_IDS` (SS58 addresses, decoded to public keys).
- No external dependencies (no Redis, no blockchain RPC in the hot path).
- Replay protection is **not** part of the whitelist; the Endpoint injects a shared Redis `NonceStore`, so multiple Endpoint instances are safe.

### Kafka (central bus)
- Durable event log and decoupling point for all processing modules.
- Enables replay, independent scaling, and fault isolation per consumer group.

### PubSub Broadcaster
- Consumes authorized events from Kafka.
- Publishes to libp2p/GossipSub topics.
- Commits offset only after publish confirmation policy.

### Heartbeat Tracker
- Observability-only consumer of trusted `telemetry.authorized.v1` events from Kafka.
- Uses `fromBeginning: false` and tracks `firstSeen`, `lastSeen`, and `onlineSince` per sensor in Redis.
- Exposes JSON metrics on `/metrics`: `sensors_online` count, per-sensor uptime, and aggregate (max/avg) uptime over a configurable online window (default 30s).
- Does not emit result events and does not participate in retry/DLQ commit-result semantics.
- Fault isolation: failures in heartbeat tracking do not block telemetry pipeline.

### Batcher
- Consumes authorized events from `telemetry.authorized.v1`.
- Groups events into deterministic batches by size, consumer lag, and a bounded flush timer.
- Serializes each batch as a `crypto.v1.SignedEnvelopeBatch`, zstd-compresses it, and recursively splits it into smaller sub-batches if the compressed result exceeds `ANCHOR_MAX_PAYLOAD_BYTES`.
- Emits one `telemetry.batched.v1` message per fitted sub-batch, each carrying the compressed payload, its size/hash fields, and its own `batch_id`.
- Routes single events that cannot fit even alone to `telemetry.dlq.v1` without blocking the rest of the batch.
- Serializes flushes (single active flush per instance) so a timer-triggered flush cannot publish the same batch as a size/lag-triggered flush.
- Flushes any pending batch during graceful shutdown before closing resources.
- Commits `telemetry.authorized.v1` offsets only after all sub-batches are durably produced.

### Blockchain Anchor
- Consumes batched events from `telemetry.batched.v1`.
- Defensively rejects (permanent error, offset committed) any payload exceeding `ANCHOR_MAX_PAYLOAD_BYTES`.
- Submits the compressed payload bytes directly into the substrate-based Robonomics blockchain via `cps.setPayload` to make the batch immutable.
- Deduplicates by comparing the current on-chain payload bytes against the incoming payload before submission.
- Commits offset only after blockchain submission confirmation (or a confirmed idempotent skip/rejection).
- Emitting an anchoring result event (`telemetry.blockchain.result.v1`) is deferred to a future phase.

### Status dashboard
- The Endpoint serves one self-contained static HTML page at `GET /`, with accessible Overview, Sensors, and Network tabs, hash deep links, and light/dark Robonomics branding.
- The browser calls only same-origin `/api/status`, `/api/status/sensors`, and `/api/status/network`; it lazy-loads detail tabs and refreshes the active tab every 30 seconds.
- The Endpoint Status Collector calls operator-configured private Cloud Run URLs using ADC-signed OIDC ID tokens. It uses bounded probes, per-instance TTL caches, single-flight requests, and safe public DTOs. Caches are not synchronized across Cloud Run instances.
- Readiness (`/ready`), not liveness (`/health`), determines service state. Transport failures require repeated observations before outage; IAM failures and missing configuration are unknown.
- Sensor IDs are public hex values. The Sensors API returns bounded cursor-based pages and no SS58 address or sensors.social URL; the browser validates the ID, converts it to SS58 prefix 32, and creates the external link.
- Network reports reserved peer counts and process-local counters separately from blockchain Kafka lag, persisted last-finalized-batch observation, signer balance, and chain block heads. Kafka lag uses read-only Admin offset queries; finalized observations are persisted best-effort in Redis after finality.
- Status collection does not require CORS or public access to private service ports.

| Service | Display name | Default port | Env var |
| --- | --- | --- | --- |
| endpoint | Telemetry Ingress API | 3000 | `ENDPOINT_PORT` / `PORT` |
| pubsub-broadcaster | Live Telemetry Broadcast (libp2p) | 3020 | `PUBSUB_BROADCASTER_HEALTH_PORT` / `PORT` |
| heartbeat-tracker | Sensor Heartbeat Tracker | 3030 | `HEARTBEAT_TRACKER_HEALTH_PORT` / `PORT` |
| batcher | Telemetry Batcher | 3041 | `BATCHER_HEALTH_PORT` / `PORT` |
| blockchain-anchor | Robonomics Blockchain Anchor | 3050 | `BLOCKCHAIN_ANCHOR_HEALTH_PORT` / `PORT` |

## Core Kafka topics
- `telemetry.authorized.v1`
- `telemetry.rejected.v1`
- `telemetry.batched.v1`
- `telemetry.dlq.v1`

## Error handling baseline
- Bounded retries for transient failures.
- Route exhausted failures to DLQ.
- Keep retries/DLQ isolated per module to prevent cross-module blocking.

## Explicit architectural constraints
- No synchronous dependency between processing modules (all flow through Kafka).
- Allowed flow: `Endpoint -> Kafka -> Consumers`.
- Disallowed direct couplings:
  - Endpoint -> PubSub
  - Endpoint -> Blockchain Anchor
  - PubSub -> Batcher
  - Batcher -> Blockchain Anchor (must flow through Kafka)
- Authentication is pluggable via the `SensorAuth` interface (`whitelist` or `none`) and must stay in-process: no network or blockchain lookup on the ingest hot path.
