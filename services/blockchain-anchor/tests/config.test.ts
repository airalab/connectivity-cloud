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
import { aliceSigner } from './signer.js';
import { writeSeedFile } from './seed-file.js';

const ALICE_ADDRESS = aliceSigner.address;

function baseEnv(seed = '//Alice'): NodeJS.ProcessEnv {
  return {
    BLOCKCHAIN_ANCHOR_NODE_ID: '0',
    BLOCKCHAIN_ANCHOR_SURI_FILE: writeSeedFile(seed),
  };
}

describe('loadBlockchainAnchorConfig', () => {
  it('throws when BLOCKCHAIN_ANCHOR_NODE_ID is missing', async () => {
    await expect(
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_SURI_FILE: writeSeedFile('//Alice'),
      })
    ).rejects.toThrow('BLOCKCHAIN_ANCHOR_NODE_ID is required');
  });

  it('throws when BLOCKCHAIN_ANCHOR_SURI_FILE is missing', async () => {
    await expect(
      loadBlockchainAnchorConfig({ BLOCKCHAIN_ANCHOR_NODE_ID: '0' })
    ).rejects.toThrow('BLOCKCHAIN_ANCHOR_SURI_FILE is required');
  });

  it('does not accept the removed BLOCKCHAIN_ANCHOR_SURI env var', async () => {
    await expect(
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_NODE_ID: '0',
        BLOCKCHAIN_ANCHOR_SURI: '//Alice',
      })
    ).rejects.toThrow('BLOCKCHAIN_ANCHOR_SURI_FILE is required');
  });

  it('throws when the seed file does not exist', async () => {
    await expect(
      loadBlockchainAnchorConfig({
        BLOCKCHAIN_ANCHOR_NODE_ID: '0',
        BLOCKCHAIN_ANCHOR_SURI_FILE: '/nonexistent/anchor-suri',
      })
    ).rejects.toThrow('BLOCKCHAIN_ANCHOR_SURI_FILE could not be read');
  });

  it('throws when the seed file is empty', async () => {
    await expect(loadBlockchainAnchorConfig(baseEnv(' \n'))).rejects.toThrow(
      'BLOCKCHAIN_ANCHOR_SURI_FILE is empty'
    );
  });

  it('throws when node_id is not a valid integer', async () => {
    await expect(
      loadBlockchainAnchorConfig({
        ...baseEnv(),
        BLOCKCHAIN_ANCHOR_NODE_ID: 'not-a-number',
      })
    ).rejects.toThrow(
      'BLOCKCHAIN_ANCHOR_NODE_ID must be a non-negative integer'
    );
  });

  it('throws when node_id is negative', async () => {
    await expect(
      loadBlockchainAnchorConfig({
        ...baseEnv(),
        BLOCKCHAIN_ANCHOR_NODE_ID: '-5',
      })
    ).rejects.toThrow(
      'BLOCKCHAIN_ANCHOR_NODE_ID must be a non-negative integer'
    );
  });

  it('loads minimal valid config with required fields', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      BLOCKCHAIN_ANCHOR_NODE_ID: '42',
    });

    expect(config.nodeId).toBe(42);
    expect(config.signer.address).toBe(ALICE_ADDRESS);
    expect(config.kafkaBrokers).toEqual(['localhost:9092']);
    expect(config.consumerGroupId).toBe('blockchain-anchor-v1');
    expect(config.substrateWsUrl).toBe('ws://localhost:9944');
    expect(config.healthPort).toBe(3050);
    expect(config.maxPayloadBytes).toBe(8192);
  });

  it('parses node_id as zero', async () => {
    const config = await loadBlockchainAnchorConfig(baseEnv());
    expect(config.nodeId).toBe(0);
  });

  it('parses large node_id', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      BLOCKCHAIN_ANCHOR_NODE_ID: '999999',
    });
    expect(config.nodeId).toBe(999999);
  });

  it('parses kafka brokers from comma-separated string', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      KAFKA_BROKERS: 'broker1:9092, broker2:9093 ,broker3:9094',
    });

    expect(config.kafkaBrokers).toEqual([
      'broker1:9092',
      'broker2:9093',
      'broker3:9094',
    ]);
  });

  it('filters empty broker strings', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      KAFKA_BROKERS: 'broker1:9092,,,broker2:9093',
    });

    expect(config.kafkaBrokers).toEqual(['broker1:9092', 'broker2:9093']);
  });

  it('parses custom consumer group id', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      BLOCKCHAIN_ANCHOR_GROUP_ID: 'my-custom-group',
    });

    expect(config.consumerGroupId).toBe('my-custom-group');
  });

  it('parses custom substrate ws url', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      SUBSTRATE_WS_URL: 'wss://kusama.example.com:443',
    });

    expect(config.substrateWsUrl).toBe('wss://kusama.example.com:443');
  });

  it('trims surrounding whitespace in the seed file', async () => {
    const config = await loadBlockchainAnchorConfig(baseEnv('  //Alice\n'));
    expect(config.signer.address).toBe(ALICE_ADDRESS);
  });

  it('derives the signer from a seed phrase', async () => {
    const config = await loadBlockchainAnchorConfig(
      baseEnv(
        'bottom drive obey lake curtain smoke basket hold race lonely fit walk'
      )
    );

    expect(config.signer.address).toBeTruthy();
    expect(config.signer.address).not.toBe(ALICE_ADDRESS);
  });

  it('derives the signer from a hex seed', async () => {
    const config = await loadBlockchainAnchorConfig(
      baseEnv(
        '0x0101010101010101010101010101010101010101010101010101010101010101'
      )
    );

    expect(config.signer.address).toBeTruthy();
    expect(config.signer.address).not.toBe(ALICE_ADDRESS);
  });

  it('parses positive integer health port', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: '8080',
    });

    expect(config.healthPort).toBe(8080);
  });

  it('falls back to default on invalid health port', async () => {
    const config1 = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: '-1',
    });
    expect(config1.healthPort).toBe(3050);

    const config2 = await loadBlockchainAnchorConfig({
      ...baseEnv(),
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: 'not-a-number',
    });
    expect(config2.healthPort).toBe(3050);
  });

  it('loads all custom config values', async () => {
    const config = await loadBlockchainAnchorConfig({
      ...baseEnv('//Bob'),
      BLOCKCHAIN_ANCHOR_NODE_ID: '5',
      KAFKA_BROKERS: 'kafka1:9092,kafka2:9092',
      BLOCKCHAIN_ANCHOR_GROUP_ID: 'anchor-prod',
      SUBSTRATE_WS_URL: 'wss://robonomics.network:443',
      BLOCKCHAIN_ANCHOR_HEALTH_PORT: '9000',
    });

    expect(config.nodeId).toBe(5);
    expect(config.signer.address).not.toBe(ALICE_ADDRESS);
    expect(config.kafkaBrokers).toEqual(['kafka1:9092', 'kafka2:9092']);
    expect(config.consumerGroupId).toBe('anchor-prod');
    expect(config.substrateWsUrl).toBe('wss://robonomics.network:443');
    expect(config.healthPort).toBe(9000);
  });
});
