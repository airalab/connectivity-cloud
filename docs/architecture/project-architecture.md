# General Architecture

## Purpose

The system accepts Ed25519-signed environmental sensor telemetry (Altruist-series sensors), verifies authenticity and authorization, and distributes trusted events to downstream channels with Kafka as the central durable bus.

## High-level architecture

`Sensor -> Endpoint -> Message Bus (Kafka) -> Services (PubSub, Batcher, Blockchain Anchor)`

> Authorization source: `SENSOR_AUTH_STRATEGY` selects how the Endpoint authorizes sensors, entirely in memory (no blockchain or Redis lookup on the hot path):
> - `whitelist` (default): `WHITELIST_SENSOR_IDS` allowlist; an empty list authorizes nobody.
> - `none`: any validly-signed sensor is authorized (signature, timestamp and nonce checks still apply).

### PubSub Broadcast

`Trusted Messages (Kafka) -> PubSub Broadcaster -> libp2p GossipSub -> Web UI (sensors.social)`

### Heartbeat observability

`Trusted Messages (Kafka) -> Heartbeat Tracker -> /metrics (liveness + uptime)`

### Blockchain anchoring

`Trusted Messages (Kafka) -> Batcher -> Message Bus (Kafka) -> Blockchain Anchor -> Robonomics Blockchain`

### Status page flow

`Browser -> Endpoint GET / (static HTML) -> browser polls each service's /health and /metrics directly`

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
- Enforces timestamp window policy and replay protection via nonce deduplication (in-memory; bounded for `none`).
- Checks sensor authorization using a pluggable strategy (`whitelist` allowlist or `none`, which accepts any validly-signed sensor).
- Publishes authorized events to `telemetry.authorized.v1` and rejected events to `telemetry.rejected.v1`.
- Returns `202` only after Kafka ACK.
- Serves the status page at `GET /` (see [Status page](#status-page)), plus `GET /health` and `GET /metrics`.

### Whitelist
- Sensor authentication provider used by the Endpoint's `whitelist` strategy.
- Maintains a static, in-memory sensor allowlist from `WHITELIST_SENSOR_IDS` (SS58 addresses, decoded to public keys).
- No external dependencies (no Redis, no blockchain RPC in the hot path).
- Nonce replay protection is per-process (in memory); run a single Endpoint instance or accept that replay windows are per instance.

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
- Serializes each batch as a `crypto.v1.SignedEnvelopeBatch`, XZ-compresses it, and recursively splits it into smaller sub-batches if the compressed result exceeds `ANCHOR_MAX_PAYLOAD_BYTES`.
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

### Status page
- Served by the Endpoint at `GET /` as a static, dependency-free HTML page; the server performs no probing and renders identical markup on every request.
- Branded with the official Robonomics Network logo, inlined as SVG (black variant in light mode, white variant in dark mode via `prefers-color-scheme`), so the page needs no external assets.
- An inline script polls each service's `/health` directly from the browser every 5s and shows each service's status (Operational / Unreachable).
- Service URLs are built from `window.location.hostname` (the host the page was loaded from) plus the per-service health port, so no host configuration is needed.
- A metrics section polls `/metrics` for three headline numbers:

| Metric | Source service | Field |
| --- | --- | --- |
| Online sensors | heartbeat-tracker | `sensors_online` |
| libp2p peers | pubsub-broadcaster | `connectedPeerCount` |
| Anchored messages | blockchain-anchor | `anchored` |

- Because the browser calls services cross-origin, `/health` (all services) and `/metrics` (endpoint, heartbeat-tracker, pubsub-broadcaster, blockchain-anchor) send `access-control-allow-origin: *`. Health ports must therefore be reachable from the browser.

| Service | Display name | Default port | Env var |
| --- | --- | --- | --- |
| endpoint | Telemetry Ingress API | 3000 | `ENDPOINT_PORT` / `PORT` |
| pubsub-broadcaster | Live Telemetry Broadcast (libp2p) | 3020 | `PUBSUB_BROADCASTER_HEALTH_PORT` |
| heartbeat-tracker | Sensor Heartbeat Tracker | 3030 | `HEARTBEAT_TRACKER_HEALTH_PORT` |
| batcher | Telemetry Batcher | 3041 | `BATCHER_HEALTH_PORT` |
| blockchain-anchor | Robonomics Blockchain Anchor | 3050 | `BLOCKCHAIN_ANCHOR_HEALTH_PORT` |

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
