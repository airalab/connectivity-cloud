# Sensors Integration Guide (Protobuf)

## `POST /v1/telemetry`

- Primary transport: `Content-Type: application/protobuf`

## Wire format

Request body is serialized `crypto.v1.SignedEnvelope`:

```protobuf
message SignedEnvelope {
  bytes sensor_id = 1;   // 32-byte Ed25519 public key
  uint64 timestamp = 2;  // unix ms UTC
  bytes nonce = 3;       // 16-32 bytes
  bytes message = 4;     // serialized core.v1.Message
  bytes signature = 5;   // 64-byte Ed25519 signature
}
```

Protocol and schema references:

- Buf Schema Registry docs: https://buf.build/docs
- Sensors Social module: https://buf.build/airalab/sensors-social-proto

`message` contains:

```protobuf
message Message {
  Meta metadata = 1;
  oneof payload {
    device.v1.Urban urban = 2;
    device.v1.Insight insight = 3;
  }
}
```

## Signature (normative)

```text
timestamp_le = uint64(timestamp) encoded as 8 bytes little-endian
signing_bytes = sensor_id || timestamp_le || nonce || message
signature = Ed25519.sign(private_key, signing_bytes)
verify    = Ed25519.verify(public_key, signing_bytes, signature)
```

## Routing and zones

- `X-Request-Id` is passed through for tracing/auditing.
- `X-Sensor-Zone` remains available for upstream routing policies.

Supported zones:

- `ru`
- `eu-west`
- `us-east`
- `ap-southeast`

Endpoint matrix:

| Environment | Zone         | Base URL                                     |
| ----------- | ------------ | -------------------------------------------- |
| production  | global       | `https://ingest.sensors.social`              |
| production  | ru           | `https://ru.ingest.sensors.social`           |
| production  | eu-west      | `https://eu-west.ingest.sensors.social`      |
| production  | us-east      | `https://us-east.ingest.sensors.social`      |
| production  | ap-southeast | `https://ap-southeast.ingest.sensors.social` |
| staging     | global       | `https://ingest.staging.sensors.social`      |
| staging     | ru           | `https://ru.ingest.staging.sensors.social`   |
| staging     | eu-west      | `https://eu-west.ingest.staging.sensors.social` |
| staging     | us-east      | `https://us-east.ingest.staging.sensors.social` |
| staging     | ap-southeast | `https://ap-southeast.ingest.staging.sensors.social` |

Primary path in every zone: `POST /v1/telemetry`.

Global endpoint routing behavior:

1. Route by `X-Sensor-Zone` when present and valid.
2. If header is absent, route by sender IP geolocation policy.
3. Return `307 Temporary Redirect` to a zone endpoint so clients preserve method and body.

Connectivity validates signature and envelope constraints; it does not decode inner measurements.

## Validation rules

- `sensor_id` MUST be 32 bytes
- `signature` MUST be 64 bytes
- `nonce` MUST be 16..32 bytes
- `message` MUST be non-empty
- timestamp skew policy: reject outside configured window (default `±300s`)
- replay scope: `(sensor_id, nonce)`

## Responses

- `202` accepted and published to Kafka (`telemetry.authorized.v1`)
- `401` invalid signature or stale timestamp
- `403` sensor not authorized (not in the endpoint's whitelist; never returned when the endpoint runs with `SENSOR_AUTH_STRATEGY=none`)
- `409` duplicate nonce
- `503` Kafka/infra unavailable

## Authorization

The endpoint authorizes sensors with one of two strategies, chosen by the operator via `SENSOR_AUTH_STRATEGY`:

- `whitelist` (default): only sensors whose public key is listed in `WHITELIST_SENSOR_IDS` are accepted. Other sensors receive `403`. Ask the operator to add your sensor's SS58 address.
- `none`: any sensor with a valid signature and timestamp is accepted.

Signature, timestamp and nonce validation apply in both modes.

## Monitoring and status

- `GET /health` on every service returns `{"status":"ok"}` (CORS-enabled).
- `GET /` on the endpoint serves a status page listing each service's health, plus online sensors, libp2p peers and anchored messages.
- The status page resolves service URLs from the hostname in the browser's address bar, so open it using the same host name that exposes the services' health ports (default ports: endpoint 3000, pubsub-broadcaster 3020, heartbeat-tracker 3030, batcher 3041, blockchain-anchor 3050).
- `GET /metrics` returns JSON counters on endpoint, heartbeat-tracker, pubsub-broadcaster, batcher and blockchain-anchor.

## Sensors payload proto 

- Buf module: `buf.build/airalab/sensors-social-proto`
