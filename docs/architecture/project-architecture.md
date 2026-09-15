# General Architecture

## Purpose

The system accepts Ed25519-signed environmental sensor telemetry (Altruist-series sensors), verifies authenticity and authorization, and distributes trusted events to downstream channels with Kafka as the central durable bus.

## High-level architecture

`Sensor -> Endpoint -> Message Bus (Kafka) -> Services (IPFS, PubSub, Blockchain)`

> Authorization source: `Robonomics Blockchain -> Registry Sync -> Redis -> Endpoint` or `Whitelist -> Redis -> Endpoint`

### PubSub Broadcast

`Trusted Messages (Kafka) -> PubSub Broadcaster -> IPFS PubSub -> Web UI (sensors.social)`

### Heartbeat observability

`Trusted Messages (Kafka) -> Heartbeat Tracker -> /metrics (liveness + uptime)`

### Blockchain anchoring

`Trusted Messages (Kafka) -> Batcher -> Message Bus (Kafka) -> IPFS Publisher -> Message Bus (Kafka) -> Robonomics Blockchain`

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
- Enforces timestamp window policy and replay protection via nonce deduplication.
- Checks sensor/key status from Redis projection using pluggable authentication (registry-sync or whitelist).
- Publishes authorized events to `telemetry.authorized.v1` and rejected events to `telemetry.rejected.v1`.
- Returns `202` only after Kafka ACK.

### Registry Sync
- Consumes finalized Robonomics blockchain events.
- Maintains local Redis projection of sensor/key status.
- Serves low-latency reads to Endpoint module (no blockchain RPC in hot path).

### Whitelist
- Alternative authentication provider for simplified deployments.
- Maintains static sensor whitelist in Redis.
- Bypasses blockchain dependency while preserving same authentication contract.

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
- Exposes Prometheus metrics: `sensors_online` count, per-sensor uptime percentage, and aggregate uptime over a configurable online window (default 30s).
- Does not emit result events and does not participate in retry/DLQ commit-result semantics.
- Fault isolation: failures in heartbeat tracking do not block telemetry pipeline.

### Batcher
- Consumes authorized events from `telemetry.authorized.v1`.
- Groups events into deterministic batches by size, consumer lag, and a bounded flush timer.
- Serializes each batch as a `crypto.v1.SignedEnvelopeBatch` and emits `telemetry.batched.v1`.
- Serializes flushes (single active flush per instance) so a timer-triggered flush cannot publish the same batch as a size/lag-triggered flush.
- Flushes any pending batch during graceful shutdown before closing resources.
- Commits `telemetry.authorized.v1` offsets only after the batch is durably produced.

### IPFS Publisher
- Consumes batched events from `telemetry.batched.v1`.
- Publishes/pins each batch artifact to IPFS (optionally XZ-compressed) and captures the CID.
- Deduplicates by `batch_id` to avoid duplicate publication of redelivered batches.
- Emits `ipfs.published.v1` with `cid` and `event_count`.
- Commits `telemetry.batched.v1` offsets only after publish success and result emission.

### Robonomics Blockchain
- Consumes IPFS-published events (`ipfs.published.v1`) from Kafka.
- Publishes the CID into the substrate-based Robonomics blockchain to make the CID immutable.
- Deduplicates by CID before submission.
- Emits anchoring result events (`telemetry.blockchain.result.v1`).
- Commits offset only after blockchain submission confirmation.

## Core Kafka topics
- `telemetry.authorized.v1`
- `telemetry.rejected.v1`
- `telemetry.batched.v1`
- `ipfs.published.v1`
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
  - Endpoint -> IPFS
  - PubSub -> IPFS
  - Batcher -> IPFS (must flow through Kafka)
  - IPFS -> Blockchain (must flow through Kafka)
- Authentication is pluggable via `SensorAuth` interface but must use Redis for low-latency lookups.
