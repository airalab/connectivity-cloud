# WP-02 — `endpoint`

## Summary
WP-02 implements trusted telemetry ingress at `POST /v1/telemetry`. The service validates protobuf `SignedEnvelope` payloads, verifies Ed25519 signatures over raw envelope bytes (`sensor_id || timestamp_le || nonce || message`), and authorizes sensors with a pluggable in-memory strategy (`whitelist` or `none`). Authorized events are published to Kafka, and `202` is returned only after broker acknowledgment.

## Current status snapshot
- Implemented: ingress route scaffold, schema validation, whitelist/none sensor auth with in-memory nonce checks, signature verification path, `401/403/409/503/202` response mapping.
- Implemented in this increment: timestamp skew enforcement, rejection-event publish path (`telemetry.rejected.v1`), bounded producer retry with DLQ fallback, and ACK-gated Kafka publish for `telemetry.authorized.v1`.
- Remaining for full WP-02 DoD: broader docker-compose integration coverage and formal DoD sign-off.

## Depends on
- WP-00

## Scope / Goal
Deliver a working Authorizer ingress path that:
- Accepts signed sensor telemetry.
- Applies schema/limits/timestamp window/nonce/signature checks.
- Checks sensor authorization via the configured strategy (`SENSOR_AUTH_STRATEGY`: `whitelist` or `none`).
- Publishes `telemetry.authorized.v1` (and `telemetry.rejected.v1` on rejection conditions).
- Maps outcomes to the required HTTP response codes.

## Out of scope / Deferred
- Direct downstream delivery to PubSub/IPFS (must flow through Kafka only).
- Blockchain access in hot path.
- Changes to `v1` envelope/topic contracts.

## Inputs & Outputs
### Inputs
- HTTP endpoint: `POST /v1/telemetry`.
- Request body:
  - protobuf `crypto.v1.SignedEnvelope` (`application/protobuf`)
  - optional JSON wrapper with base64 `envelope`
- Whitelist (`WHITELIST_SENSOR_IDS`) or `none` sensor authentication strategy (`SENSOR_AUTH_STRATEGY`).

### Outputs
- Kafka produced events:
  - `telemetry.authorized.v1` on accepted telemetry.
  - `telemetry.rejected.v1` on rejected telemetry with reason information.
- HTTP responses:
  - `202 Accepted` (only after Kafka ACK on `telemetry.authorized.v1`)
  - `401 Unauthorized`
  - `403 Forbidden`
  - `409 Conflict`
  - `503 Service Unavailable`

## Detailed tasks / Implementation checklist
- [x] Replace Authorizer stubs/TODOs with real handler implementation for `POST /v1/telemetry`.
- [x] Validate protobuf envelope fields (`sensor_id`, `timestamp`, `nonce`, `message`, `signature`).
- [x] Implement binary signature verification:
  - `sensor_id || timestamp_le || nonce || message`
  - verify Ed25519 signature against resolved public key.
- [x] Enforce timestamp window policy and replay protection using binary-safe dedup key (`hex(sensor_id):hex(nonce)`).
- [x] Authorize sensors via the whitelist/none strategy; reject sensors not on the whitelist.
- [x] Publish accepted requests to `telemetry.authorized.v1` with canonical envelope/payload from WP-00.
- [x] Publish rejections to `telemetry.rejected.v1` with reason code/message.
- [x] Gate `202` response on Kafka ACK success for `telemetry.authorized.v1` publish.
- [x] Map failures to `401/403/409/503` deterministically and return minimal response body (`status`, `error_code`).
- [x] Add health/metrics endpoint and structured logging with `event_id`/`trace_id` correlation.

## Idempotency & error handling
- Ingest dedup key: `hex(sensor_id)` + `hex(nonce)`.
- Duplicate nonce for same sensor must return `409 Conflict`.
- Signature mismatch must return `401 Unauthorized`.
- A sensor not on the whitelist must return `403 Forbidden`.
- Kafka unavailability/ACK failure must return `503 Service Unavailable`.
- Rejected-event publication should be best-effort with bounded retry; failures must not incorrectly return `202`.

## Testing
- Unit tests:
  - request schema validation,
  - canonicalization and signature verification,
  - timestamp window handling,
  - nonce replay detection,
  - HTTP status mapping.
- Integration tests:
  - local `docker-compose` Kafka,
  - `202` only when `telemetry.authorized.v1` ACK is received,
  - rejection path publication to `telemetry.rejected.v1`.
- Contract tests:
  - envelope/payload compatibility with WP-00 schemas for both produced topics.

## Definition of Done
- All `TODO` markers in `endpoint` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry + DLQ behavior is wired via shared runtime where applicable.
- Baseline structured logging and health/metrics endpoint are implemented.
- `POST /v1/telemetry` behavior matches contract: required fields, signature flow, nonce replay handling, and ACK-gated `202` semantics.
