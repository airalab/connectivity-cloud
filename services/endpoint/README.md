# Endpoint Service

The endpoint service provides the telemetry ingress endpoint (`POST /v1/telemetry`) and validates sensor authentication using pluggable authentication strategies.

## Authentication Strategies

The endpoint supports two authentication strategies that can be selected at runtime:

### 1. Registry-Sync Strategy (Default)

Uses the registry-sync service to validate sensors against the on-chain registry projection stored in Redis.

**Features:**
- Validates sensors against on-chain registry state
- Real-time updates from blockchain events
- Checks if sensor is enabled in the registry
- Nonce replay protection with Redis

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=registry-sync
```

**Requirements:**
- Redis instance running
- Registry-sync service running and populating Redis projection

### 2. Whitelist Strategy

Uses an in-memory allowlist of sensor IDs for authentication.

**Features:**
- Simple allowlist-based authorization
- No external dependencies (no Redis, no registry-sync)
- Fast in-memory lookups
- Nonce replay protection in memory

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=whitelist
WHITELIST_SENSOR_IDS=sensor-1,sensor-2,sensor-3
```

**Use Cases:**
- Testing and development
- Small deployments with static sensor lists
- Scenarios where blockchain integration is not needed

## Environment Variables

### Endpoint Configuration

- `ENDPOINT_PORT` - HTTP server port (default: `3000`)
- `ENDPOINT_SOURCE` - Event source identifier (default: `endpoint`)
- `ENDPOINT_TIMESTAMP_SKEW_SECONDS` - Maximum allowed timestamp skew (default: `300`)
- `ENDPOINT_PRODUCER_MAX_ATTEMPTS` - Kafka producer retry attempts (default: `3`)
- `ENDPOINT_PRODUCER_RETRY_BACKOFF_MS` - Kafka producer retry backoff (default: `100`)
- `ENDPOINT_LOG_LEVEL` - Log level (default: `info`)

### Authentication Strategy

- `SENSOR_AUTH_STRATEGY` - Authentication strategy: `registry-sync` or `whitelist` (default: `registry-sync`)

### Whitelist Strategy Configuration

- `WHITELIST_SENSOR_IDS` - Comma-separated list of allowed sensor IDs (only used when `SENSOR_AUTH_STRATEGY=whitelist`)

### Kafka Configuration

- `KAFKA_BROKERS` - Comma-separated list of Kafka broker addresses (default: `localhost:9092`)

## Development

```bash
# Start endpoint with registry-sync (default)
pnpm --filter @scp/endpoint dev

# Start endpoint with whitelist strategy
SENSOR_AUTH_STRATEGY=whitelist WHITELIST_SENSOR_IDS=sensor-1,sensor-2 pnpm --filter @scp/endpoint dev
```

## Testing

```bash
pnpm --filter @scp/endpoint test
pnpm --filter @scp/endpoint build
pnpm --filter @scp/endpoint typecheck
pnpm --filter @scp/endpoint lint
```

## API

### POST /v1/telemetry

Submit sensor telemetry data.

**Request Body:**
```json
{
  "sensor_id": "5GrwvaEF5zXb26Fz9rcQpDWS57CtERHpNehXCPcNoHGKutQY",
  "timestamp": "2024-01-15T10:30:00Z",
  "nonce": "unique-nonce-value",
  "measurements": {
    "temperature": 22.5,
    "humidity": 65.0
  },
  "signature": "0x..."
}
```

**Response:**
- `202 Accepted` - Telemetry accepted and published to Kafka
- `401 Unauthorized` - Invalid timestamp or signature
- `403 Forbidden` - Sensor not authorized (not in registry/whitelist or disabled)
- `409 Conflict` - Duplicate nonce (replay attack detected)
- `503 Service Unavailable` - Kafka unavailable

### GET /

Static, minimalistic status page. Server-rendered markup is identical on
every request (no server-side probing); an inline script in the page polls
this service's `/health` plus each sibling service's `/health` endpoint
(ports read from `.env`, reachable at `STATUS_PAGE_HOST`, default
`localhost`) directly from the browser and refreshes the table every 5
seconds.

### GET /health

Health check endpoint.

**Response:**
```json
{
  "status": "ok"
}
```

### GET /metrics

Metrics endpoint.

**Response:**
```json
{
  "accepted": 123,
  "rejected": 45,
  "kafkaErrors": 2
}
```
