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
import { readFileSync } from 'node:fs';
import { Keyring } from '@polkadot/api';
import { cryptoWaitReady } from '@polkadot/util-crypto';
import { loadKafkaSecurityConfig } from '@scp/infra';
import type { KafkaSecurityConfig } from '@scp/infra';
import { DEFAULT_ANCHOR_MAX_PAYLOAD_BYTES } from '@scp/core';

/** Signing account built once from the seed file. */
export type AnchorSigner = ReturnType<Keyring['addFromUri']>;

export interface BlockchainAnchorConfig {
  kafkaBrokers: string[];
  /** Kafka TLS/SASL settings shared by every producer and consumer. */
  kafkaSecurity?: KafkaSecurityConfig;
  consumerGroupId: string;
  substrateWsUrl: string;
  /** Account used to sign `cps.setPayload` extrinsics. */
  signer: AnchorSigner;
  nodeId: number;
  healthPort: number;
  /** Maximum size, in bytes, of a chain-ready compressed payload (defensive check). */
  maxPayloadBytes: number;
  /** Value used as the `source_service` DLQ header when forwarding records. */
  source: string;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

function readSuriFile(path: string): string {
  let content: string;
  try {
    content = readFileSync(path, 'utf8');
  } catch (error) {
    throw new Error(
      `BLOCKCHAIN_ANCHOR_SURI_FILE could not be read: ${
        error instanceof Error ? error.message : 'unknown error'
      }`,
      { cause: error }
    );
  }
  const suri = content.trim();
  if (!suri) {
    throw new Error(`BLOCKCHAIN_ANCHOR_SURI_FILE is empty: ${path}`);
  }
  return suri;
}

/**
 * Load the service configuration, including the signing account read from
 * the file named by `BLOCKCHAIN_ANCHOR_SURI_FILE`.
 */
export async function loadBlockchainAnchorConfig(
  env: NodeJS.ProcessEnv = process.env
): Promise<BlockchainAnchorConfig> {
  const nodeId = env.BLOCKCHAIN_ANCHOR_NODE_ID;
  if (!nodeId) {
    throw new Error(
      'BLOCKCHAIN_ANCHOR_NODE_ID is required (the CPS node ID to update)'
    );
  }

  const nodeIdParsed = Number.parseInt(nodeId, 10);
  if (!Number.isFinite(nodeIdParsed) || nodeIdParsed < 0) {
    throw new Error(
      `BLOCKCHAIN_ANCHOR_NODE_ID must be a non-negative integer, got: ${nodeId}`
    );
  }

  const suriFile = env.BLOCKCHAIN_ANCHOR_SURI_FILE;
  if (!suriFile) {
    throw new Error(
      'BLOCKCHAIN_ANCHOR_SURI_FILE is required (path to a file with the account seed/mnemonic for signing)'
    );
  }
  const suri = readSuriFile(suriFile);

  await cryptoWaitReady();
  const signer = new Keyring({ type: 'sr25519' }).addFromUri(suri);

  return {
    kafkaSecurity: loadKafkaSecurityConfig(env),
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.BLOCKCHAIN_ANCHOR_GROUP_ID ?? 'blockchain-anchor-v1',
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    signer,
    nodeId: nodeIdParsed,
    healthPort: parsePositiveInt(
      env.PORT ?? env.BLOCKCHAIN_ANCHOR_HEALTH_PORT,
      3050
    ),
    maxPayloadBytes: parsePositiveInt(
      env.ANCHOR_MAX_PAYLOAD_BYTES,
      DEFAULT_ANCHOR_MAX_PAYLOAD_BYTES
    ),
    source: env.BLOCKCHAIN_ANCHOR_SOURCE ?? 'blockchain-anchor',
  };
}
