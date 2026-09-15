# `@scp/pubsub-broadcaster`

Simple consumer that publishes authorized telemetry over a native libp2p
GossipSub node for real-time web UI updates.

## Architecture

- **Pattern**: Simple consumer (observability-only, no retry/DLQ/result events)
- **Input**: `telemetry.authorized.v1` from Kafka
- **Output**: SignedEnvelope bytes published to a libp2p GossipSub topic
- **Autocommit**: Enabled (best-effort delivery)
- **Failure handling**: Logs failures but does not retry or emit result events
- **Connectivity gating**: Kafka consumption is paused (without leaving the
  consumer group) whenever fewer than the configured minimum number of
  reserved peers are connected, and resumed once connectivity recovers

This service forwards telemetry to real-time subscribers. Failures are acceptable since telemetry is also archived via `ipfs-publisher` and `blockchain-anchor` services.

## libp2p GossipSub

The service runs an embedded libp2p node (TCP + WebSocket/WSS transports,
Noise encryption, Yamux muxing, GossipSub pubsub) instead of talking to the
Kubo PubSub HTTP API. It dials a configurable list of reserved peers on
startup and automatically redials any that disconnect. Because
`@libp2p/websockets` is included as a transport, reserved peers may be
specified as `wss://` multiaddrs (e.g. behind a TLS-terminating relay),
in addition to plain `tcp`/`ws` addresses.

## Environment

- `KAFKA_BROKERS` (default: `localhost:9092`)
- `PUBSUB_BROADCASTER_GROUP_ID` (default: `pubsub-broadcaster-v1`)
- `PUBSUB_BROADCASTER_SOURCE` (default: `pubsub-broadcaster`)
- `PUBSUB_BROADCASTER_HEALTH_PORT` (default: `3020`)
- `PUBSUB_TOPIC` (default: `sensors.social/telemetry/v1`)
- `PUBSUB_RESERVED_PEERS` (comma-separated multiaddrs, e.g.
  `/dns4/relay.example.com/tcp/443/wss/p2p/12D3Koo...`; default: none)
- `PUBSUB_MIN_CONNECTED_PEERS` (minimum connected reserved peers required to
  consume Kafka; `0` disables connectivity gating; default: `0`)
- `PUBSUB_LIBP2P_PRIVATE_KEY_SEED_HEX` (32-byte hex seed for a stable Ed25519
  libp2p identity; if unset, an ephemeral identity is generated on each start
  — only use this default outside of production)
- `PUBSUB_LIBP2P_LISTEN_ADDRESSES` (comma-separated multiaddrs; default:
  `/ip4/0.0.0.0/tcp/0,/ip4/0.0.0.0/tcp/0/ws`)
- `PUBSUB_CONNECTIVITY_STABILIZATION_INTERVAL_MS` (how long connectivity must
  remain below/above the threshold before pausing/resuming Kafka, to avoid
  flapping; default: `5000`)
- `PUBSUB_RECONNECT_INTERVAL_MS` (interval between reconnect attempts for
  disconnected reserved peers; default: `10000`)

## Health

- `GET /health`: liveness. Always `200` while the process is running;
  temporary peer unavailability does not affect liveness.
- `GET /ready`: readiness. Returns `503` when the number of connected
  reserved peers is below `PUBSUB_MIN_CONNECTED_PEERS`, `200` otherwise.

## Metrics

Available at `http://localhost:3020/metrics`:

- `consumed`: Total messages consumed
- `publishSuccess`: Successful GossipSub publishes
- `publishFailure`: Failed GossipSub publishes (not retried)
- `connectedPeerIds`: Ids of currently connected reserved peers (bounded by
  the configured reserved peer list, so cardinality stays small)
- `connectedPeerCount`: Number of currently connected reserved peers
- `minConnectedPeers`: Configured minimum required peers
- `kafkaPaused`: Whether Kafka consumption is currently paused
- `pauseCount` / `resumeCount`: Number of times consumption has been paused
  or resumed due to connectivity changes

