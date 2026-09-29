# Work Packages (WPs)

Implementation work packages for the Sensors.social Connectivity telemetry pipeline.
Each WP takes a scaffold/stub service to a working, tested implementation.

## Source of truth

- [`../architecture/project-architecture.md`](../architecture/project-architecture.md)
- [`../architecture/integration-guide.md`](../architecture/integration-guide.md)

## Pipeline

```text
Sensor -> Endpoint -> Kafka -> {PubSub Broadcaster, Batcher -> Blockchain Anchor}
```

## Work packages

| WP | Service / Package | Depends on | Status |
|----|-------------------|------------|--------|
| [WP-00](./wp-00-contracts.md) | `@scp/contracts` (shared schemas, envelope, topics, consumer runtime) | — | Done |
| WP-01 | `registry-sync` (substrate → Redis projection) | WP-00 | Removed (superseded by whitelist/none auth in `endpoint`) |
| [WP-02](./wp-02-endpoint.md) | `endpoint` (`POST /v1/telemetry` ingress) | WP-00 | Implemented (pending formal DoD sign-off) |
| [WP-03](./wp-03-pubsub-broadcaster.md) | `pubsub-broadcaster` (GossipSub fan-out) | WP-00, WP-02 | Implemented |
| WP-03A | `heartbeat-tracker` (trusted-event liveness & uptime observability) | WP-00, WP-02 | Implemented |
| [WP-04](./wp-04-ipfs-publisher.md) | `@scp/batcher` (batch, XZ-compress, size-fit) | WP-00, WP-02 | Implemented (superseded `ipfs-publisher`, see note in doc) |
| [WP-05](./wp-05-blockchain-anchor.md) | `blockchain-anchor` (anchors compressed batch payloads via `cps.setPayload`) | WP-00, WP-04 | Implemented |

## Recommended sequencing

1. **WP-00** first — all services import shared contracts, so freeze schemas/envelope/topics before wiring services.
2. **WP-02** — enables end-to-end producing onto Kafka.
3. **WP-03 / WP-04** in parallel — both consume `telemetry.authorized.v1`.
4. **WP-05** last — consumes `telemetry.batched.v1` from WP-04.

## Definition of done (applies to every WP)

- All `TODO` markers in the service replaced with real logic.
- Unit tests + integration test against local `docker-compose` infra.
- `pnpm build`, `pnpm typecheck`, `pnpm lint`, `pnpm test` green.
- Bounded retry + DLQ wired via the shared consumer runtime.
- Baseline structured logging and health/metrics endpoint.
