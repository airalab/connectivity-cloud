# WP-04 — `ipfs-publisher`

## Summary
WP-04 implements deterministic batching and IPFS publication for authorized telemetry events.

> **Update (batcher/publisher split):** batching and IPFS publication are now separate services. The `@scp/batcher` service consumes `telemetry.authorized.v1`, builds deterministic batches, and emits `telemetry.batched.v1`. The `@scp/ipfs-publisher` service consumes `telemetry.batched.v1`, publishes/pins the batch artifact to IPFS, and emits `ipfs.published.v1` (`cid`, `event_count`). The sections below describe the combined responsibility; the batching tasks now belong to the batcher and the publication tasks to the ipfs-publisher.

## Depends on
- WP-00
- WP-02

## Scope / Goal
Deliver `ipfs-publisher` as an idempotent Kafka consumer that deterministically batches authorized telemetry, publishes/pins to IPFS, and emits `telemetry.ipfs.result.v1` events containing the CID and batch event count.

## Out of scope / Deferred
- Blockchain anchoring submission logic (handled by WP-05).
- Sensor ingress authorization.
- Multi-format archival strategy beyond the selected deterministic object/CAR output.

## Inputs & Outputs
### Inputs
- Kafka topic consumed: `telemetry.authorized.v1`.
- Envelope/payload contracts from WP-00 (protobuf envelope fields serialized as base64 strings).
- External system: IPFS node/gateway/pinning service.

### Outputs
- External side effect: deterministic batch object/CAR publication and pin success.
- Kafka topic produced: `telemetry.ipfs.result.v1` payload with:
  - `cid`
  - `event_count`

## Detailed tasks / Implementation checklist
- [ ] Replace service stubs/TODOs with a real `telemetry.authorized.v1` consumer.
- [ ] Validate consumed events with WP-00 schemas.
- [ ] Implement deterministic batching strategy and `batch_id` derivation.
- [ ] Build deterministic IPFS object/CAR bytes from batch contents.
- [ ] Publish and pin batch artifact to IPFS; capture resulting CID.
- [ ] Deduplicate batch processing by `batch_id` to avoid duplicate IPFS publication.
- [ ] Emit `telemetry.ipfs.result.v1` with canonical envelope and payload (`cid`, `event_count`).
- [ ] Apply bounded retry for transient IPFS/pinning failures.
- [ ] Route exhausted failures to `telemetry.dlq.v1`.
- [ ] Commit Kafka offset only after pin success and result-event emission succeed.
- [ ] Add structured logging and health/metrics endpoint (batch count, pin latency, retry/DLQ counts).

## Idempotency & error handling
- Primary dedup key: `batch_id`.
- Replayed inputs that map to same `batch_id` must not create inconsistent duplicate publication records.
- Only commit offset after successful pin + `telemetry.ipfs.result.v1` emission.
- Use bounded retry and DLQ for exhausted transient failures.

## Testing
- Unit tests:
  - deterministic batch formation,
  - `batch_id` derivation stability,
  - `event_count` calculation,
  - retry/error classification.
- Integration tests:
  - local `docker-compose` Kafka + IPFS,
  - validate consume → batch → publish/pin → emit result → commit order,
  - verify DLQ behavior on exhausted failures.
- Contract tests:
  - consumed `telemetry.authorized.v1` schema and produced `telemetry.ipfs.result.v1` payload compatibility.

## Definition of Done
- All `TODO` markers in `ipfs-publisher` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry + DLQ via shared consumer runtime is implemented.
- Baseline structured logging and health/metrics endpoint are implemented.
- Published `telemetry.ipfs.result.v1` events consistently contain valid `cid` and `event_count` after successful pin.
