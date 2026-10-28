# Endpoint Service

The endpoint service provides the telemetry ingress endpoint (`POST /v1/telemetry`) and validates sensor authentication using pluggable authentication strategies.

## Authentication Strategies

The endpoint supports three authentication strategies that can be selected at runtime with `SENSOR_AUTH_STRATEGY`. `whitelist` and `none` run in memory with no external dependencies; `cps` reads the Robonomics chain. The envelope signature and timestamp checks always apply.

### 1. Whitelist Strategy (Default)

Only sensors listed in `WHITELIST_SENSOR_IDS` are authorized. An empty list authorizes nobody.

**Features:**
- Simple allowlist-based authorization
- Fast in-memory lookups
- Nonce replay protection via the shared Redis `NonceStore`

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=whitelist
WHITELIST_SENSOR_IDS=<ss58-address-1>,<ss58-address-2>
```

### 2. None Strategy

Every validly-signed sensor is authorized; there is no allowlist.

**Features:**
- Open ingestion gated only by signature and timestamp validation
- Nonce replay protection via the shared Redis `NonceStore`

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=none
```

**Use Cases:**
- Local development and testing
- Deployments where authorization is enforced elsewhere

### 3. CPS Strategy

A sensor is authorized when its public key is the identity of a CPS node that is a strict descendant (any depth) of the `sensors.social` root node. The root node itself is not authorized.

**Features:**
- Read-only lookups via `cps.identityNode` and `cps.nodes` on the Robonomics chain
- Positive decisions cached in a bounded LRU (no TTL); negative decisions cached for `CPS_AUTH_NEGATIVE_TTL_SECONDS`
- RPC errors are never cached and deny access (fail closed)
- At startup `CPS_ROOT_ADDRESS` must resolve to an existing CPS root node (no parent), otherwise the endpoint fails to start
- Nonce replay protection via the shared Redis `NonceStore`

**Configuration:**
```bash
SENSOR_AUTH_STRATEGY=cps
SUBSTRATE_WS_URL=wss://...
CPS_ROOT_ADDRESS=<sensors.social-root-address>
CPS_AUTH_CACHE_MAX_SIZE=100000
CPS_AUTH_NEGATIVE_TTL_SECONDS=60
```

**Limitation:** changes to the ancestry or removal of a previously authorized node are not seen until its cache entry is evicted or the service restarts.

## Replay protection (shared Redis nonce store)

Replay protection is independent of the authentication strategy and shared by every endpoint instance, so the service is safe to run on multiple Cloud Run instances.

1. After signature, timestamp and authorization checks pass, the endpoint atomically claims the pair `(sensor public key, nonce)` in Redis with `SET nonce:v1:<sha256(...)> <random token> NX EX <ttl>`. The key hashes a domain-separated, length-prefixed encoding of the raw sensor key and nonce bytes, so raw nonces never appear in Redis keys or logs.
2. The claim happens **before** the Kafka publish. A duplicate returns `409 duplicate_nonce` (and emits a rejected event) without publishing authorized telemetry.
3. If Redis fails, times out or returns an unexpected reply, the request fails closed with `503 nonce_store_unavailable` and nothing is published. There is no process-local fallback.
4. If the Kafka publish fails or its ACK is uncertain (`503 kafka_unavailable`), the claim is **kept**: the broker may have accepted the event, so a release could cause a duplicate. A client retry gets `409` until the claim expires.

Trade-offs: a crash after the Redis claim but before the Kafka publish suppresses the sensor's retry of that exact envelope until the TTL expires. Exactly-once delivery across Redis and Kafka is **not** provided; that would need a separate outbox design.

`NONCE_TTL_SECONDS` must cover the whole interval in which the same signed envelope could still pass timestamp validation (`[ts - skew, ts + skew]`, i.e. twice the skew) plus a 60 second margin. Startup fails when it is shorter: with the default 300 second skew the minimum is 660 and the default is 900.

## Environment Variables

### Endpoint Configuration

- `ENDPOINT_PORT` - HTTP server port (default: `3000`)
- `ENDPOINT_SOURCE` - Event source identifier (default: `endpoint`)
- `ENDPOINT_TIMESTAMP_SKEW_SECONDS` - Maximum allowed timestamp skew (default: `300`)
- `ENDPOINT_PRODUCER_MAX_ATTEMPTS` - Kafka producer retry attempts (default: `3`)
- `ENDPOINT_PRODUCER_RETRY_BACKOFF_MS` - Kafka producer retry backoff (default: `100`)
- `ENDPOINT_LOG_LEVEL` - Log level (default: `info`)
- `PORT` - Cloud Run listening port; takes precedence over `ENDPOINT_PORT`

### Authentication Strategy

- `SENSOR_AUTH_STRATEGY` - Authentication strategy: `whitelist`, `none` or `cps` (default: `whitelist`)

### Whitelist Strategy Configuration

- `WHITELIST_SENSOR_IDS` - Comma-separated list of allowed sensor IDs (only used when `SENSOR_AUTH_STRATEGY=whitelist`)

### CPS Strategy Configuration

Only used when `SENSOR_AUTH_STRATEGY=cps`.

- `SUBSTRATE_WS_URL` - Robonomics WebSocket endpoint (default: `ws://localhost:9944`)
- `CPS_ROOT_ADDRESS` - Identity address of the `sensors.social` root CPS node (required)
- `CPS_AUTH_CACHE_MAX_SIZE` - Maximum cached decisions (default: `100000`)
- `CPS_AUTH_NEGATIVE_TTL_SECONDS` - Negative cache TTL in seconds (default: `60`)

### Redis Configuration (nonce store)

- `REDIS_URL` - `rediss://host:port` (Memorystore with in-transit encryption typically uses port `6378`); default `redis://localhost:6379` works only with `SCP_ALLOW_INSECURE_LOCAL=true`
- `REDIS_AUTH_PASSWORD` - Redis AUTH string (kept out of the URL so it cannot leak into logs); inject from Secret Manager
- `REDIS_TLS_CA_PATH` - PEM file with the trusted CA certificate(s), e.g. the Memorystore instance CA mounted from Secret Manager. Put old and new CA certificates in the same file for a rotation overlap. Server certificate verification is always on
- `REDIS_TLS_SERVERNAME` - Optional TLS server name used for certificate verification
- `REDIS_CONNECT_TIMEOUT_MS` / `REDIS_COMMAND_TIMEOUT_MS` - Timeouts (defaults `5000` / `2000`)
- `NONCE_REDIS_PREFIX` - Key prefix (default: `nonce:v1`; heartbeat-tracker uses `heartbeat-tracker:v1`)
- `NONCE_TTL_SECONDS` - Claim lifetime (default: `900`, validated against `ENDPOINT_TIMESTAMP_SKEW_SECONDS`)

In production the service refuses to start unless Redis uses TLS, a CA file and AUTH, and it fails at startup if Redis cannot be reached or rejects the credentials.

### Kafka Configuration

- `KAFKA_BROKERS` - Comma-separated list of Kafka broker addresses (default: `localhost:9092`)
- `KAFKA_SECURITY_PROTOCOL` - `SASL_SSL` (default), `SSL` or `PLAINTEXT`; only `SASL_SSL` is accepted unless `SCP_ALLOW_INSECURE_LOCAL=true`
- `KAFKA_SASL_MECHANISM` - `OAUTHBEARER` (default) or `PLAIN`
- `KAFKA_AUTH_PROVIDER` - `google-adc` (default; short-lived tokens from the Cloud Run service account via Application Default Credentials) or `static` (PLAIN with `KAFKA_SASL_USERNAME`/`KAFKA_SASL_PASSWORD`, e.g. for self-hosted brokers)
- `KAFKA_TLS_CA_PATH`, `KAFKA_TLS_SERVERNAME` - Optional custom CA bundle / SNI override. Certificate and hostname verification are never disabled
- `KAFKA_GOOGLE_PRINCIPAL` - Optional service-account e-mail override if it cannot be derived from ADC

All Kafka producers and consumers in this repository share the same factory (`@scp/infra`). `GOOGLE_APPLICATION_CREDENTIALS` (a long-lived key file) is rejected in production mode.

### Local development mode

`SCP_ALLOW_INSECURE_LOCAL=true` explicitly opts in to the plaintext, unauthenticated Docker Compose Redis and Kafka (`KAFKA_SECURITY_PROTOCOL=PLAINTEXT`). It is set by `.env.example` and must never be set in production.

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
- `409 Conflict` - Duplicate nonce (replay attack detected; `duplicate_nonce`)
- `503 Service Unavailable` - `nonce_store_unavailable` (Redis failure, nothing published) or `kafka_unavailable` (the nonce claim is retained)

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

### GET /ready

Readiness probe for Cloud Run/load balancers. Returns `200 {"status":"ready"}` only when Redis (`PING`) and Kafka (authenticated metadata request) are usable; otherwise `503 {"status":"unavailable","failed":["redis"]}`. Error details and credentials are never returned (they are logged server-side). Results are cached for ~2 seconds.

### GET /health

Liveness probe; it does not check dependencies.

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
