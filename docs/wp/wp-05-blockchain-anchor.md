# WP-05 — `blockchain-anchor`

> **Updated (issue #34):** The IPFS anchoring stage has been removed.
> `blockchain-anchor` now consumes `telemetry.batched.v1` directly from
> `@scp/batcher` and submits the already XZ-compressed, size-fitted payload
> bytes on-chain via `cps.setPayload`. There is no CID or IPFS publication
> step; idempotency is based on comparing the current on-chain payload
> bytes against the incoming payload.

## Summary
WP-05 anchors chain-ready compressed telemetry batches into the substrate-based Robonomics blockchain. It consumes batched telemetry events, deduplicates by comparing on-chain payload bytes, submits anchoring transactions, and commits offsets only after finalization (or a confirmed idempotent skip). Emitting a `telemetry.blockchain.result.v1` result event is deferred to a future phase.

## Depends on
- WP-00
- `@scp/batcher` (formerly WP-04)

## Scope / Goal
Implement `blockchain-anchor` to consume `telemetry.batched.v1`, submit the compressed batch payload to Robonomics via `cps.setPayload`, and commit offsets only after submission confirmation (or a confirmed idempotent skip).

## Out of scope / Deferred
- Advanced finality/reorg handling.
- Attestation/proof frameworks.
- Multi-chain adapters/abstraction.
- Advanced fee/relay optimization strategies.
- Emitting `telemetry.blockchain.result.v1` (deferred; not implemented yet).

## Inputs & Outputs
### Inputs
- Kafka topic consumed: `telemetry.batched.v1` payload fields:
  - `batch_id`
  - `payload` (XZ-compressed, chain-ready bytes; must be `<= ANCHOR_MAX_PAYLOAD_BYTES`)
  - `uncompressed_size`, `compressed_size`, `payload_hash`
  - `event_count`, `sensor_ids`
- Shared envelope/contracts from WP-00.
- External system: substrate-based Robonomics blockchain.

### Outputs
- External side effect: `cps.setPayload(node_id, payload)` blockchain transaction submission.
- No result event is currently emitted (see Deferred).

## Detailed tasks / Implementation checklist
- [x] Consume `telemetry.batched.v1` and parse `TelemetryBatchedPayload`.
- [x] Validate consumed events against WP-00 schemas.
- [x] Defensively reject (permanent error, offset committed) any payload exceeding `ANCHOR_MAX_PAYLOAD_BYTES`.
- [x] Implement byte-for-byte payload dedup guard (`api.query.cps.payload(node_id)`) before blockchain submission.
- [x] Implement substrate/Robonomics client submission for `cps.setPayload`.
- [x] Wait for extrinsic finalization before committing the offset.
- [ ] Emit `telemetry.blockchain.result.v1` with success/failure status and transaction/error details (deferred).
- [ ] Apply bounded retry for transient submission failures (currently retried via redelivery/offset-not-committed only).
- [ ] Route exhausted failures to `telemetry.dlq.v1`.
- [x] Commit Kafka offset only after submission confirmation (or idempotent skip, or permanent oversized-payload rejection).
- [x] Add structured logging and health/metrics endpoint (`consumed`, `anchored`, `skippedDuplicate`, `rejectedOversized`, `failed`).

## Idempotency & error handling
- Dedup key: the compressed payload bytes themselves, scoped to the configured `BLOCKCHAIN_ANCHOR_NODE_ID`.
- Replayed messages with the same payload bytes must not resubmit a duplicate anchoring transaction; the on-chain state is queried and compared byte-for-byte before submission.
- Consumer rule: consume → check size limit → check on-chain payload → submit (if needed) → wait confirmation → commit offset.
- Oversized payloads (a producer bug or contract violation, since the batcher is expected to have already split to fit) are rejected permanently and the offset is committed without submission.

## Testing
- Unit tests:
  - byte-for-byte payload dedup logic,
  - oversized-payload rejection,
  - config parsing (`maxPayloadBytes`, node ID, etc.).
- Integration tests:
  - local `docker-compose` Kafka + substrate/Robonomics test environment/mock,
  - verify consume → submit → commit order,
  - verify graceful shutdown waits for an in-flight extrinsic before disconnecting.
- Contract tests:
  - consumed `telemetry.batched.v1` payload compatibility (`batch_id`, `payload`, size/hash fields).

## Definition of Done
- All `TODO` markers in `blockchain-anchor` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Baseline structured logging and health/metrics endpoint are implemented.
- Compressed-payload anchoring works end-to-end with offset-commit-after-confirmation policy.
- Deferred items (result event emission, finality/reorg, attestation, multi-chain, bounded retry/DLQ) are explicitly documented as not in this phase.
