# `@scp/heartbeat-tracker`

Observability consumer for trusted `telemetry.authorized.v1` events. It tracks sensor liveness and uptime in Redis and exposes metrics over HTTP.

## Online and uptime definitions

- **Online**: `now - lastSeen <= HEARTBEAT_TRACKER_ONLINE_WINDOW_MS` (default: `30000` ms / 30s).
- **firstSeen**: first time a sensor is observed, based on the telemetry event's `occurred_at` timestamp.
- **lastSeen**: the highest `occurred_at` event timestamp seen for the sensor so far (not Kafka processing time).
- **onlineSince**: start of the current continuous online streak. If the gap between two event timestamps is greater than the window, streak uptime resets at the new event.

## Event-time based, monotonic heartbeat state

Heartbeat timing is derived from the `Envelope.occurred_at` timestamp of the telemetry event, not from when the Kafka record is processed. Redis is updated with a compare-and-set Lua script so that `lastSeen`/`onlineSince`/`firstSeen` only advance when the incoming event timestamp is strictly newer than what is stored:

- Replaying an older Kafka record cannot move `lastSeen` backwards (or re-advance it).
- The read-modify-write is atomic, so concurrent consumer instances (e.g. during a rebalance) cannot race and corrupt state.
- Horizontal scaling is safe as long as records for a given sensor are partitioned consistently by sensor ID.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_AUTH_PROVIDER`, `KAFKA_TLS_CA_PATH`, `KAFKA_TLS_SERVERNAME` (verified TLS + SASL, shared by all services; see the endpoint README)
- `HEARTBEAT_TRACKER_GROUP_ID` (default: `heartbeat-tracker-v1`)
- `HEARTBEAT_TRACKER_SOURCE` (default: `heartbeat-tracker`)
- `HEARTBEAT_TRACKER_HEALTH_PORT` (default: `3030`); Cloud Run `PORT` takes precedence
- `HEARTBEAT_TRACKER_ONLINE_WINDOW_MS` (default: `30000`)
- `REDIS_URL` (default: `redis://localhost:6379`; use `rediss://host:6378` for Memorystore)
- `REDIS_AUTH_PASSWORD` (Redis AUTH string, injected from Secret Manager)
- `REDIS_TLS_CA_PATH` (PEM with the Memorystore instance CA; may contain several certificates for rotation)
- `REDIS_TLS_SERVERNAME`, `REDIS_CONNECT_TIMEOUT_MS`, `REDIS_COMMAND_TIMEOUT_MS`
- `SCP_ALLOW_INSECURE_LOCAL` (`true` only for local Docker Compose; otherwise TLS + AUTH are mandatory and startup fails without them)
- `HEARTBEAT_TRACKER_REDIS_PREFIX` (default: `heartbeat-tracker:v1`; the endpoint's replay claims use the separate `nonce:v1` prefix on the same Redis instance)

## Endpoints

### `GET /health`

```json
{ "status": "ok" }
```

### `GET /metrics`

Example:

```json
{
  "sensors_online": 2,
  "sensors_total_tracked": 3,
  "online_window_ms": 30000,
  "consumed": 125,
  "sensor_uptime_seconds": {
    "sensor-a": 42,
    "sensor-b": 7
  },
  "sensors_uptime": [
    {
      "sensor_id": "sensor-a",
      "online": true,
      "first_seen": "2026-01-01T00:00:00.000Z",
      "last_seen": "2026-01-01T00:01:02.000Z",
      "uptime_seconds": 42,
      "seconds_since_last_seen": 1
    },
    {
      "sensor_id": "sensor-c",
      "online": false,
      "first_seen": "2026-01-01T00:00:10.000Z",
      "last_seen": "2026-01-01T00:00:20.000Z",
      "uptime_seconds": 0,
      "seconds_since_last_seen": 120
    }
  ],
  "max_uptime_seconds": 42,
  "avg_uptime_seconds": 24.5
}
```

### `GET /status/sensors`

Private read-only status endpoint used by the endpoint collector. It returns
a heartbeat summary with a bounded sensor list, sorted online-first and
most-recently-seen within each group. `limit` defaults to 50 and is capped at
100; `cursor` is a server-side numeric offset and `filter` accepts `all`,
`online`, or `offline`. The response contains `total`, `nextCursor`,
`observedAt`, `summary`, `uptimeSummarySampled`, and the selected `sensors`
only. It does not create SS58 addresses or external URLs.

Pagination limits Redis work, not just response size. Each heartbeat also
updates the `<prefix>:sensors:last-seen` sorted set (score = last-seen
milliseconds); because a sensor is online exactly when its last-seen time is
within the online window, online/offline filtering, ordering, and counts are
`ZCOUNT`/`ZREVRANGEBYSCORE ... LIMIT` range reads. A request reads at most one
page of sensor hashes plus a sample of up to 500 online sensors used for the
average/maximum uptime (`uptimeSummarySampled` is `true` when more sensors are
online than the sample covers), and prunes at most 100 expired sensors. The index
is populated as sensors send heartbeats, so after upgrading, sensors that are
already offline appear in `/status/sensors` only after their next heartbeat
(`/metrics` still prunes them from the legacy set).
