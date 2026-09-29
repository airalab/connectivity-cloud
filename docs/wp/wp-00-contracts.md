# WP-00 — `@scp/contracts`

## Summary
WP-00 establishes the shared integration foundation for the telemetry pipeline: canonical event envelope, protobuf-aware payload schemas, versioned topic constants, and reusable consumer runtime behavior. Every downstream service depends on these contracts to avoid drift and incompatible message handling. Completing this WP freezes `v1` boundaries so other WPs can implement logic against stable interfaces.

## Depends on
- —

## Scope / Goal
Deliver a production-ready `@scp/contracts` package that provides:
- Canonical envelope schema and validators.
- Topic-name constants for all core `telemetry.*.v1` topics.
- Payload schemas for pipeline events.
- Shared consumer runtime primitives for bounded retry, DLQ routing, idempotency hooks, and offset-commit-after-side-effect policy.

## Out of scope / Deferred
- Introducing `v2` contracts or breaking changes to `v1`.
- Business logic for any concrete service (Authorizer, IPFS, Blockchain, PubSub).
- Advanced workflow/SLA orchestration beyond bounded retry + DLQ.

## Inputs & Outputs
### Inputs
- Source-of-truth documents:
  - `docs/architecture/project-architecture.md`
  - `docs/architecture/integration-guide.md`

### Outputs
- Shared envelope schema using fields:
  - `event_id`, `event_type`, `event_version`, `occurred_at`, `trace_id` (optional), `source`, `payload`.
- Topic constants for:
  - `telemetry.authorized.v1`
  - `telemetry.rejected.v1`
  - `telemetry.pubsub.result.v1`
  - `telemetry.ipfs.result.v1`
  - `telemetry.blockchain.result.v1`
  - `telemetry.retry.v1`
  - `telemetry.dlq.v1`
- Payload schemas aligned with integration contracts draft for:
  - `telemetry.authorized.v1`
  - `telemetry.rejected.v1`
  - `telemetry.ipfs.result.v1`
  - `telemetry.blockchain.result.v1`
- Runtime interfaces/helpers used by consumers in later WPs.

## Detailed tasks / Implementation checklist
- [ ] Inventory existing contract stubs/TODOs in `packages/contracts` and replace all placeholders with concrete `v1` schema definitions.
- [ ] Define and export canonical envelope schema with strict required/optional field handling.
- [ ] Define and export topic constants exactly matching architecture docs.
- [ ] Implement/export payload schemas for all currently defined telemetry contracts from the integration draft.
- [ ] Freeze explicit placeholders or typed extension points for topics whose payloads are not yet fully specified by draft docs (without inventing contradictory fields).
- [ ] Implement shared consumer runtime policy primitives:
  - bounded retry policy,
  - DLQ routing to `telemetry.dlq.v1`,
  - idempotency hook interface,
  - commit-after-side-effect/result policy guardrails.
- [ ] Add reusable envelope + payload validation helpers for producers/consumers.
- [ ] Add contract tests to verify schema compatibility and topic constant stability.
- [ ] Add documentation/examples for importing topics, validating envelopes, and wiring retry/DLQ.

## Idempotency & error handling
- Event-level dedup key support must be based on `event_id`.
- Consumer runtime must enforce: consume → side effect → confirmation → result event → offset commit.
- Retries must be bounded and exhausted messages must be routed to `telemetry.dlq.v1` with error context.

## Testing
- Unit tests:
  - envelope schema validation,
  - payload schema validation,
  - topic constant exports,
  - retry/DLQ runtime behavior,
  - offset commit guard behavior.
- Contract tests:
  - compatibility for all `telemetry.*.v1` exports,
  - explicit assertions for canonical field names and requiredness.
- Integration test:
  - run against local `docker-compose` Kafka stack to validate retry and DLQ routing behavior via shared runtime.

## Definition of Done
- All `TODO` markers in `@scp/contracts` are replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra are green.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` are green.
- Bounded retry + DLQ to `telemetry.dlq.v1` is implemented in shared consumer runtime primitives.
- Baseline structured logging hooks and health/metrics integration points expected by services are documented and usable.
- `v1` envelope field names and topic constants are frozen and used as the single source for downstream WPs.
