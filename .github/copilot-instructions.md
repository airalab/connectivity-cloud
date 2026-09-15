## Build, Test, and Development Commands

### Monorepo-wide
- `pnpm build` - Build all packages and services (uses Turborepo)
- `pnpm test` - Run all tests across the monorepo
- `pnpm lint` - Lint all workspaces (eslint)
- `pnpm lint:fix` - Auto-fix linting issues
- `pnpm typecheck` - Type-check all TypeScript files
- `pnpm dev` - Run all services in parallel (development mode)

### Per-service/package
- `pnpm --filter @scp/<name> build` - Build specific workspace
- `pnpm --filter @scp/<name> test` - Run tests for specific workspace
- `pnpm --filter @scp/<name> dev` - Run specific service in watch mode
- Example: `pnpm --filter @scp/endpoint dev`

### Testing single files
- `pnpm --filter @scp/<name> test <pattern>` - Run specific test file
- Example: `pnpm --filter @scp/core test contracts.test.ts`

### Fake sensor CLI (for debugging/integration testing)
```bash
pnpm fake-sensor -- \
  --endpoint http://localhost:3000/v1/telemetry \
  --signer-seed-hex 0x0101010101010101010101010101010101010101010101010101010101010101 \
  --count 5 \
  --interval-ms 1000 \
  --sensor-zone eu-west
```

## High-Level Architecture

This is an **event-driven telemetry pipeline** with Kafka as the central durable message bus.

### Data Flow
1. **Sensors** → POST `/v1/telemetry` (protobuf `SignedEnvelope`) → **endpoint** service
2. **endpoint** validates signature/timestamp/nonce, checks sensor authorization via Redis projection
3. **endpoint** publishes to Kafka `telemetry.authorized.v1` topic (returns 202 only after Kafka ACK)
4. Multiple **downstream consumers** process authorized events independently:
   - **pubsub-broadcaster**: Publishes to libp2p/GossipSub for real-time web UI
   - **heartbeat-tracker**: Tracks sensor liveness/uptime metrics (observability-only, no DLQ)
   - **batcher**: Batches authorized telemetry and emits `telemetry.batched.v1`
5. **batcher** → **ipfs-publisher** → **blockchain-anchor** publication chain:
   - **ipfs-publisher**: Consumes `telemetry.batched.v1`, publishes batches to IPFS, emits CIDs
   - **blockchain-anchor**: Anchors IPFS CIDs to Robonomics blockchain

### Key Architectural Constraints
- **No direct coupling** between processing modules (all flow through Kafka)
- **At-least-once delivery** + idempotent consumers = effectively-once behavior
- **Commit after confirmation**: Consumers must complete external actions before committing offsets
- **Bounded retries + DLQ**: Transient failures retry up to `maxAttempts`, then route to dead-letter queue

### Authorization Source
```
Robonomics Blockchain → registry-sync → Redis → endpoint (lookup during validation)
```
- **registry-sync** subscribes to finalized blockchain events, projects sensor/key state to Redis
- **endpoint** reads from Redis (no blockchain RPC in hot path)

### Core Kafka Topics
- `telemetry.authorized.v1` - Successfully validated telemetry
- `telemetry.rejected.v1` - Failed validation (signature/timestamp/auth)
- `telemetry.batched.v1` - Batched authorized telemetry ready for publication
- `ipfs.published.v1` - IPFS publish results (includes CID)
- `telemetry.blockchain.result.v1` - Blockchain anchoring results
- `telemetry.retry.v1` - Transient failures for retry
- `telemetry.dlq.v1` - Exhausted retries (dead letters)

### Workspace Structure
- `packages/contracts` - Shared schemas, types, validation, and consumer runtime logic (`@scp/core`)
- `services/endpoint` - HTTP ingress (Fastify, validates protobuf `SignedEnvelope`)
- `services/registry-sync` - Blockchain→Redis sync (`@polkadot/api` → ioredis)
- `services/whitelist` - Whitelist-based sensor auth provider
- `services/pubsub-broadcaster` - Kafka→libp2p GossipSub bridge
- `services/heartbeat-tracker` - Observability metrics (online sensors, uptime)
- `services/batcher` - Kafka batcher (authorized → `telemetry.batched.v1`)
- `services/ipfs-publisher` - Kafka→IPFS publisher (Kubo RPC)
- `services/blockchain-anchor` - IPFS CID→blockchain anchoring
- `tools/fake-sensor-cli` - Generate test telemetry with Ed25519 signatures

## Key Conventions and Patterns

### TypeScript Configuration
- **Strict mode enabled** with extra-strict flags:
  - `noUncheckedIndexedAccess: true`
  - `exactOptionalPropertyTypes: true`
- **Target: ES2025**, module resolution: `bundler`
- All files use `.js` extensions in import paths (ESM requirement)
- Workspace references via `@scp/*` path aliases

### Protobuf and Crypto
- Wire format: `crypto.v1.SignedEnvelope` from `@buf/airalab_sensors-social-proto.bufbuild_es`
- Signature verification: Ed25519 over `sensor_id || timestamp_le || nonce || message`
- Use `@polkadot/util-crypto` for Ed25519 operations
- Binary data encoded as base64 when serialized to Kafka JSON

### Event Envelope Pattern
All Kafka events follow a strict envelope schema (see `packages/contracts/src/envelope.ts`):
```typescript
{
  event_id: string;        // Unique event identifier
  event_type: string;      // e.g., "telemetry.authorized"
  event_version: string;   // Schema version
  occurred_at: string;     // RFC3339 timestamp with offset
  trace_id?: string;       // Optional distributed tracing ID
  source: string;          // Originating service
  payload: Record<string, unknown>; // Event-specific data
}
```

### Consumer Processing Pattern
All Kafka consumers follow the same processing rule (see `packages/contracts/src/consumer-runtime.ts`):
1. Consume event
2. Check deduplication (if applicable)
3. Perform external action (e.g., publish to PubSub/IPFS)
4. Wait for confirmation
5. Emit result event
6. Commit offset

Use `runConsumerProcessingRule()` helper with:
- `dedup`: Deduplication by `event_id`, `batch_id`, or `cid`
- `retryPolicy`: Bounded retries with configurable `maxAttempts`
- `idempotency`: Event-level duplicate detection
- `retryDlqPublisher`: Automatic retry/DLQ routing

### Configuration
- Services load config from environment variables with defaults
- Common pattern: `load<Service>Config()` function validates and returns typed config
- Use zod schemas for validation where possible
- Required infrastructure endpoints: Kafka, Redis, IPFS Kubo RPC, Robonomics node

### Naming Conventions
- Services: `@scp/<name>` (e.g., `@scp/endpoint`)
- Kafka topics: `telemetry.<domain>.<version>` (e.g., `telemetry.authorized.v1`)
- Functions: camelCase, avoid abbreviations
- Types/interfaces: PascalCase
- Files: kebab-case

### Logging
- Use structured logging with `pino` (see `services/endpoint/src/logger.ts`)
- Standard log functions: `logInfo`, `logWarn`, `logError`, `logDebug`
- Include contextual metadata (sensor_id, event_id, trace_id)

### Code Style
- **No semicolons** (consistent with prettier config)
- Always add JSDoc comments for public APIs and exported functions
- Prefer explicit return types on exported functions
- Use `async/await` over raw promises

## Local Development Setup

1. Prerequisites: Node.js 20 (`.nvmrc`), pnpm 9+, Docker Compose
2. `pnpm install`
3. `cp .env.example .env` and configure as needed
4. `docker compose up -d` (starts Kafka, Redis, IPFS, Robonomics node)
5. `pnpm dev` (starts all services) or `pnpm --filter @scp/<name> dev` (single service)

## Testing Philosophy
- Integration tests focus on end-to-end flows (sensor → endpoint → Kafka → consumers)
- Use `fake-sensor-cli` to generate realistic signed telemetry for testing
- Vitest for unit tests (`vitest run` or watch mode)

## Workflow

- Run `pnpm lint && pnpm test` after making changes
- Commit messages follow conventional commits format
- Create feature branches from `main`
