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
- `HEARTBEAT_TRACKER_GROUP_ID` (default: `heartbeat-tracker-v1`)
- `HEARTBEAT_TRACKER_SOURCE` (default: `heartbeat-tracker`)
- `HEARTBEAT_TRACKER_HEALTH_PORT` (default: `3030`)
- `HEARTBEAT_TRACKER_ONLINE_WINDOW_MS` (default: `30000`)
- `REDIS_URL` (default: `redis://localhost:6379`)
- `HEARTBEAT_TRACKER_REDIS_PREFIX` (default: `heartbeat-tracker:v1`)

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
