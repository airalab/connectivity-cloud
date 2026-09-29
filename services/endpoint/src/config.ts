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
  name: string;
  port: number;
}

export interface EndpointConfig {
  port: number;
  source: string;
  kafkaBrokers: string[];
  timestampSkewSeconds: number;
  /** Host used to reach sibling services' health ports for the status page. */
  statusHost: string;
  /** Sibling services (with their default ports) shown on the status page. */
  statusTargets: StatusTargetConfig[];
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
    statusHost: env.STATUS_PAGE_HOST ?? 'localhost',
    statusTargets: [
      {
        name: 'registry-sync',
        port: parsePositiveInt(env.REGISTRY_SYNC_HEALTH_PORT, 3011),
      },
      {
        name: 'pubsub-broadcaster',
        port: parsePositiveInt(env.PUBSUB_BROADCASTER_HEALTH_PORT, 3020),
      },
      {
        name: 'heartbeat-tracker',
        port: parsePositiveInt(env.HEARTBEAT_TRACKER_HEALTH_PORT, 3030),
      },
      {
        name: 'batcher',
        port: parsePositiveInt(env.BATCHER_HEALTH_PORT, 3041),
      },
      {
        name: 'blockchain-anchor',
        port: parsePositiveInt(env.BLOCKCHAIN_ANCHOR_HEALTH_PORT, 3050),
      },
    ],
  };
}
