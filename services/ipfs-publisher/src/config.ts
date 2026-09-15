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
import type { DurabilityPolicy } from './durability.js';

export interface IpfsPublisherConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  ipfsApiUrl: string;
  enableCompression: boolean;
  /** Ordered list of provider names; the first entry is the primary. */
  providers: string[];
  pinataApiUrl: string;
  pinataJwt: string;
  durabilityPolicy: DurabilityPolicy;
  durabilityMinSuccessCount: number;
  providerRetryBaseDelayMs: number;
  providerRetryMaxDelayMs: number;
  providerReplicationMaxPending: number;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function parseDurabilityPolicy(value: string | undefined): DurabilityPolicy {
  return value === 'all' || value === 'quorum' ? value : 'any';
}

export function loadIpfsPublisherConfig(
  env: NodeJS.ProcessEnv = process.env
): IpfsPublisherConfig {
  const providers = (env.IPFS_PROVIDERS ?? 'kubo')
    .split(',')
    .map((provider) => provider.trim())
    .filter((provider) => provider.length > 0);

  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.IPFS_PUBLISHER_GROUP_ID ?? 'ipfs-publisher-v1',
    source: env.IPFS_PUBLISHER_SOURCE ?? 'ipfs-publisher',
    healthPort: parsePositiveInt(env.IPFS_PUBLISHER_HEALTH_PORT, 3040),
    ipfsApiUrl: env.IPFS_API_URL ?? 'http://localhost:5001',
    enableCompression: env.IPFS_PUBLISHER_ENABLE_COMPRESSION !== 'false',
    providers: providers.length > 0 ? providers : ['kubo'],
    pinataApiUrl: env.PINATA_API_URL ?? 'https://api.pinata.cloud',
    pinataJwt: env.PINATA_JWT ?? '',
    durabilityPolicy: parseDurabilityPolicy(env.IPFS_DURABILITY_POLICY),
    durabilityMinSuccessCount: parsePositiveInt(
      env.IPFS_DURABILITY_MIN_SUCCESS_COUNT,
      1
    ),
    providerRetryBaseDelayMs: parsePositiveInt(
      env.IPFS_PROVIDER_RETRY_BASE_DELAY_MS,
      5000
    ),
    providerRetryMaxDelayMs: parsePositiveInt(
      env.IPFS_PROVIDER_RETRY_MAX_DELAY_MS,
      60000
    ),
    providerReplicationMaxPending: parsePositiveInt(
      env.IPFS_PROVIDER_REPLICATION_MAX_PENDING,
      1000
    ),
  };
}
