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
export interface StatusTargetConfig {
  /** Technical service identifier (e.g. `batcher`). */
  name: string;
  /** Descriptive name shown on the status page; defaults to `name`. */
  label?: string;
  port: number;
}

export interface StatusMetricConfig {
  /** Human-readable label shown on the status page. */
  label: string;
  /** Service name, used for display and to match a `statusTargets` port. */
  service: string;
  /** Port to fetch `/metrics` from. */
  port: number;
  /** Field name to read from the service's `/metrics` JSON response. */
  field: string;
}

export interface EndpointConfig {
  port: number;
  source: string;
  kafkaBrokers: string[];
  timestampSkewSeconds: number;
  /** Sibling services (with their default ports) shown on the status page. */
  statusTargets: StatusTargetConfig[];
  /** Simple headline metrics (sourced from sibling services) shown on the status page. */
  statusMetrics: StatusMetricConfig[];
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseCsv(value: string | undefined, fallback: string): string[] {
  return (value ?? fallback)
    .split(',')
    .map((v) => v.trim())
    .filter(Boolean);
}

export function loadEndpointConfig(
  env: NodeJS.ProcessEnv = process.env
): EndpointConfig {
  const pubsubBroadcasterPort = parsePositiveInt(
    env.PUBSUB_BROADCASTER_HEALTH_PORT,
    3020
  );
  const heartbeatTrackerPort = parsePositiveInt(
    env.HEARTBEAT_TRACKER_HEALTH_PORT,
    3030
  );
  const blockchainAnchorPort = parsePositiveInt(
    env.BLOCKCHAIN_ANCHOR_HEALTH_PORT,
    3050
  );

  return {
    // Cloud Run injects the listening port via `PORT`; prefer it over the
    // service-specific `ENDPOINT_PORT`, which remains as a fallback for
    // local/non-Cloud-Run deployments.
    port: parsePositiveInt(env.PORT ?? env.ENDPOINT_PORT, 3000),
    source: env.ENDPOINT_SOURCE ?? 'endpoint',
    kafkaBrokers: parseCsv(env.KAFKA_BROKERS, 'localhost:9092'),
    timestampSkewSeconds: parsePositiveInt(
      env.ENDPOINT_TIMESTAMP_SKEW_SECONDS,
      300
    ),
    statusTargets: [
      {
        name: 'pubsub-broadcaster',
        label: 'Live Telemetry Broadcast (libp2p)',
        port: pubsubBroadcasterPort,
      },
      {
        name: 'heartbeat-tracker',
        label: 'Sensor Heartbeat Tracker',
        port: heartbeatTrackerPort,
      },
      {
        name: 'batcher',
        label: 'Telemetry Batcher',
        port: parsePositiveInt(env.BATCHER_HEALTH_PORT, 3041),
      },
      {
        name: 'blockchain-anchor',
        label: 'Robonomics Blockchain Anchor',
        port: blockchainAnchorPort,
      },
    ],
    statusMetrics: [
      {
        label: 'Online sensors',
        service: 'heartbeat-tracker',
        port: heartbeatTrackerPort,
        field: 'sensors_online',
      },
      {
        label: 'libp2p peers',
        service: 'pubsub-broadcaster',
        port: pubsubBroadcasterPort,
        field: 'connectedPeerCount',
      },
      {
        label: 'Anchored messages',
        service: 'blockchain-anchor',
        port: blockchainAnchorPort,
        field: 'anchored',
      },
    ],
  };
}
