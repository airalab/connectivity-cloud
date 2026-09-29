# Endpoint Service

The endpoint service provides the telemetry ingress endpoint (`POST /v1/telemetry`) and validates sensor authentication using pluggable authentication strategies.

## Authentication Strategies

The endpoint supports two authentication strategies that can be selected at runtime with `SENSOR_AUTH_STRATEGY`. Both run in memory with no external dependencies. The envelope signature and timestamp checks always apply.

### 1. Whitelist Strategy (Default)

Only sensors listed in `WHITELIST_SENSOR_IDS` are authorized. An empty list authorizes nobody.

**Features:**
- Simple allowlist-based authorization
- Fast in-memory lookups
- Nonce replay protection in memory

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=whitelist
WHITELIST_SENSOR_IDS=<ss58-address-1>,<ss58-address-2>
```

### 2. None Strategy

Every validly-signed sensor is authorized; there is no allowlist.

**Features:**
- Open ingestion gated only by signature and timestamp validation
- Nonce replay protection in memory, bounded (oldest nonces are evicted)

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=none
```

**Use Cases:**
- Local development and testing
- Deployments where authorization is enforced elsewhere

## Environment Variables

### Endpoint Configuration

- `ENDPOINT_PORT` - HTTP server port (default: `3000`)
- `ENDPOINT_SOURCE` - Event source identifier (default: `endpoint`)
- `ENDPOINT_TIMESTAMP_SKEW_SECONDS` - Maximum allowed timestamp skew (default: `300`)
- `ENDPOINT_PRODUCER_MAX_ATTEMPTS` - Kafka producer retry attempts (default: `3`)
- `ENDPOINT_PRODUCER_RETRY_BACKOFF_MS` - Kafka producer retry backoff (default: `100`)
- `ENDPOINT_LOG_LEVEL` - Log level (default: `info`)

### Authentication Strategy

- `SENSOR_AUTH_STRATEGY` - Authentication strategy: `whitelist` or `none` (default: `whitelist`)

### Whitelist Strategy Configuration

- `WHITELIST_SENSOR_IDS` - Comma-separated list of allowed sensor IDs (only used when `SENSOR_AUTH_STRATEGY=whitelist`)

### Kafka Configuration

- `KAFKA_BROKERS` - Comma-separated list of Kafka broker addresses (default: `localhost:9092`)

## Development

```bash
# Start endpoint with the whitelist strategy (default)
WHITELIST_SENSOR_IDS=<ss58-address> pnpm --filter @scp/endpoint dev

# Start endpoint accepting any validly-signed sensor
SENSOR_AUTH_STRATEGY=none pnpm --filter @scp/endpoint dev
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

Static, minimalistic status page branded with the Robonomics Network logo
(inlined SVG; the black variant is shown in light mode and the white variant
in dark mode). Server-rendered markup is identical on
every request (no server-side probing); an inline script in the page polls
this service's `/health` plus each sibling service's `/health` endpoint
(ports read from `.env`) using `window.location.hostname` — the same host
the browser used to load the page — directly from the browser, and
refreshes the table every 5 seconds. Services are listed by descriptive
name (e.g. "Telemetry Batcher") with the technical service name shown
underneath. Below the service table, a small
metrics section polls each sibling's `/metrics` endpoint for a few
headline numbers: online sensors (heartbeat-tracker), connected libp2p
peers (pubsub-broadcaster), and anchored messages (blockchain-anchor).

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
