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
import { DEFAULT_ANCHOR_MAX_PAYLOAD_BYTES } from '@scp/core';

export interface BlockchainAnchorConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  substrateWsUrl: string;
  suri: string;
  nodeId: number;
  healthPort: number;
  /** Maximum size, in bytes, of a chain-ready compressed payload (defensive check). */
  maxPayloadBytes: number;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function loadBlockchainAnchorConfig(
  env: NodeJS.ProcessEnv = process.env
): BlockchainAnchorConfig {
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

  const suri = env.BLOCKCHAIN_ANCHOR_SURI;
  if (!suri) {
    throw new Error(
      'BLOCKCHAIN_ANCHOR_SURI is required (account seed/mnemonic for signing)'
    );
  }

  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.BLOCKCHAIN_ANCHOR_GROUP_ID ?? 'blockchain-anchor-v1',
    substrateWsUrl: env.SUBSTRATE_WS_URL ?? 'ws://localhost:9944',
    suri,
    nodeId: nodeIdParsed,
    healthPort: parsePositiveInt(env.BLOCKCHAIN_ANCHOR_HEALTH_PORT, 3050),
    maxPayloadBytes: parsePositiveInt(
      env.ANCHOR_MAX_PAYLOAD_BYTES,
      DEFAULT_ANCHOR_MAX_PAYLOAD_BYTES
    ),
  };
}
