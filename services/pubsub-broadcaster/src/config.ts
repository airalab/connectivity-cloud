/**
 * Copyright 2026 Robonomics Network
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
export interface PubsubBroadcasterConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  pubsubTopic: string;
  /** Multiaddrs of reserved libp2p peers to dial and keep connected. */
  reservedPeers: string[];
  /** Minimum number of connected reserved peers required to consume Kafka. */
  minConnectedPeers: number;
  /** Hex-encoded 32-byte Ed25519 seed for a stable libp2p identity. */
  libp2pPrivateKeySeedHex: string | undefined;
  /** Multiaddrs the embedded libp2p node listens on. */
  libp2pListenAddresses: string[];
  /** How long connectivity must remain below/above the threshold before pausing/resuming Kafka. */
  connectivityStabilizationIntervalMs: number;
  /** Interval between reconnect attempts for disconnected reserved peers. */
  reconnectIntervalMs: number;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
}

function parseList(value: string | undefined): string[] {
  return (value ?? '')
    .split(',')
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

export function loadPubsubBroadcasterConfig(
  env: NodeJS.ProcessEnv = process.env
): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.PUBSUB_BROADCASTER_GROUP_ID ?? 'pubsub-broadcaster-v1',
    source: env.PUBSUB_BROADCASTER_SOURCE ?? 'pubsub-broadcaster',
    healthPort: parsePositiveInt(env.PUBSUB_BROADCASTER_HEALTH_PORT, 3020),
    pubsubTopic: env.PUBSUB_TOPIC ?? 'sensors.social/telemetry/v1',
    reservedPeers: parseList(env.PUBSUB_RESERVED_PEERS),
    minConnectedPeers: parsePositiveInt(env.PUBSUB_MIN_CONNECTED_PEERS, 0),
    libp2pPrivateKeySeedHex: env.PUBSUB_LIBP2P_PRIVATE_KEY_SEED_HEX,
    libp2pListenAddresses: parseList(env.PUBSUB_LIBP2P_LISTEN_ADDRESSES).length
      ? parseList(env.PUBSUB_LIBP2P_LISTEN_ADDRESSES)
      : ['/ip4/0.0.0.0/tcp/0', '/ip4/0.0.0.0/tcp/0/ws'],
    connectivityStabilizationIntervalMs: parsePositiveInt(
      env.PUBSUB_CONNECTIVITY_STABILIZATION_INTERVAL_MS,
      5000
    ),
    reconnectIntervalMs: parsePositiveInt(
      env.PUBSUB_RECONNECT_INTERVAL_MS,
      10000
    ),
  };
}
