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
import { describe, expect, it } from 'vitest';
import { loadBlockchainAnchorConfig } from '../src/config.js';

describe('loadBlockchainAnchorConfig', () => {
  it('throws when BLOCKCHAIN_ANCHOR_NODE_ID is missing', () => {
    expect(() => {
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      });
    }).toThrow('BLOCKCHAIN_ANCHOR_NODE_ID is required');
  });

  it('throws when BLOCKCHAIN_ANCHOR_SURI is missing', () => {
    expect(() => {
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      });
    }).toThrow('BLOCKCHAIN_ANCHOR_SURI is required');
  });

  it('throws when node_id is not a valid integer', () => {
    expect(() => {
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_NODE_ID: 'not-a-number',
        BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      });
    }).toThrow('BLOCKCHAIN_ANCHOR_NODE_ID must be a non-negative integer');
  });

  it('throws when node_id is negative', () => {
    expect(() => {
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_NODE_ID: '-5',
        BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      });
    }).toThrow('BLOCKCHAIN_ANCHOR_NODE_ID must be a non-negative integer');
  });

  it('loads minimal valid config with required fields', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '42',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
    });

    expect(config.nodeId).toBe(42);
    expect(config.suri).toBe('//Alice');
    expect(config.kafkaBrokers).toEqual(['localhost:9092']);
    expect(config.consumerGroupId).toBe('blockchain-anchor-v1');
    expect(config.substrateWsUrl).toBe('ws://localhost:9944');
    expect(config.healthPort).toBe(3050);
    expect(config.maxPayloadBytes).toBe(8192);
  });

  it('parses node_id as zero', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
    });

    expect(config.nodeId).toBe(0);
  });

  it('parses large node_id', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '999999',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
    });

    expect(config.nodeId).toBe(999999);
  });

  it('parses kafka brokers from comma-separated string', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      KAFKA_BROKERS: 'broker1:9092, broker2:9093 ,broker3:9094',
    });

    expect(config.kafkaBrokers).toEqual([
      'broker1:9092',
      'broker2:9093',
      'broker3:9094',
    ]);
  });

  it('filters empty broker strings', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      KAFKA_BROKERS: 'broker1:9092,,,broker2:9093',
    });

    expect(config.kafkaBrokers).toEqual(['broker1:9092', 'broker2:9093']);
  });

  it('parses custom consumer group id', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      BLOCKCHAIN_ANCHOR_GROUP_ID: 'my-custom-group',
    });

    expect(config.consumerGroupId).toBe('my-custom-group');
  });

  it('parses custom substrate ws url', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      SUBSTRATE_WS_URL: 'wss://kusama.example.com:443',
    });

    expect(config.substrateWsUrl).toBe('wss://kusama.example.com:443');
  });

  it('parses seed phrase as suri', () => {
    const seedPhrase =
      'bottom drive obey lake curtain smoke basket hold race lonely fit walk';
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: seedPhrase,
    });

    expect(config.suri).toBe(seedPhrase);
  });

  it('parses hex seed as suri', () => {
    const hexSeed =
      '0xe5be9a5092b81bca64be81d212e7f2f9eba183bb7a90954f7b76361f6edb5c0a';
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: hexSeed,
    });

    expect(config.suri).toBe(hexSeed);
  });

  it('parses positive integer health port', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: '8080',
    });

    expect(config.healthPort).toBe(8080);
  });

  it('falls back to default on invalid health port', () => {
    const config1 = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: '-1',
    });
    expect(config1.healthPort).toBe(3050);

    const config2 = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '0',
      BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: 'not-a-number',
    });
    expect(config2.healthPort).toBe(3050);
  });

  it('loads all custom config values', () => {
    const config = loadBlockchainAnchorConfig({
      BLOCKCHAIN_ANCHOR_NODE_ID: '5',
      BLOCKCHAIN_ANCHOR_SURI: '//Bob',
      KAFKA_BROKERS: 'kafka1:9092,kafka2:9092',
      BLOCKCHAIN_ANCHOR_GROUP_ID: 'anchor-prod',
      SUBSTRATE_WS_URL: 'wss://robonomics.network:443',
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: '9000',
    });

    expect(config.nodeId).toBe(5);
    expect(config.suri).toBe('//Bob');
    expect(config.kafkaBrokers).toEqual(['kafka1:9092', 'kafka2:9092']);
    expect(config.consumerGroupId).toBe('anchor-prod');
    expect(config.substrateWsUrl).toBe('wss://robonomics.network:443');
    expect(config.healthPort).toBe(9000);
  });
});
