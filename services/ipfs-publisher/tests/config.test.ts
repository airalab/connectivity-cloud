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
import { loadIpfsPublisherConfig } from '../src/config.js';

describe('loadIpfsPublisherConfig', () => {
  it('uses defaults when no env vars set', () => {
    const config = loadIpfsPublisherConfig({});

    expect(config.kafkaBrokers).toEqual(['localhost:9092']);
    expect(config.consumerGroupId).toBe('ipfs-publisher-v1');
    expect(config.source).toBe('ipfs-publisher');
    expect(config.healthPort).toBe(3040);
    expect(config.ipfsApiUrl).toBe('http://localhost:5001');
    expect(config.enableCompression).toBe(true);
  });

  it('parses kafka brokers from comma-separated string', () => {
    const config = loadIpfsPublisherConfig({
      KAFKA_BROKERS: 'broker1:9092, broker2:9093 ,broker3:9094',
    });

    expect(config.kafkaBrokers).toEqual([
      'broker1:9092',
      'broker2:9093',
      'broker3:9094',
    ]);
  });

  it('filters empty broker strings', () => {
    const config = loadIpfsPublisherConfig({
      KAFKA_BROKERS: 'broker1:9092,,,broker2:9093',
    });

    expect(config.kafkaBrokers).toEqual(['broker1:9092', 'broker2:9093']);
  });

  it('parses custom consumer group id', () => {
    const config = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_GROUP_ID: 'my-custom-group',
    });

    expect(config.consumerGroupId).toBe('my-custom-group');
  });

  it('parses custom source', () => {
    const config = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_SOURCE: 'ipfs-prod',
    });

    expect(config.source).toBe('ipfs-prod');
  });

  it('parses positive integer health port', () => {
    const config = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_HEALTH_PORT: '8080',
    });

    expect(config.healthPort).toBe(8080);
  });

  it('falls back to default on invalid health port', () => {
    const config1 = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_HEALTH_PORT: '-1',
    });
    expect(config1.healthPort).toBe(3040);

    const config2 = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_HEALTH_PORT: 'not-a-number',
    });
    expect(config2.healthPort).toBe(3040);
  });

  it('parses custom IPFS API URL', () => {
    const config = loadIpfsPublisherConfig({
      IPFS_API_URL: 'http://ipfs.example.com:5001',
    });

    expect(config.ipfsApiUrl).toBe('http://ipfs.example.com:5001');
  });

  it('disables compression when explicitly set to false', () => {
    const config = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_ENABLE_COMPRESSION: 'false',
    });

    expect(config.enableCompression).toBe(false);
  });

  it('enables compression by default', () => {
    const config = loadIpfsPublisherConfig({});

    expect(config.enableCompression).toBe(true);
  });

  it('enables compression for any non-false value', () => {
    const config1 = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_ENABLE_COMPRESSION: 'true',
    });
    expect(config1.enableCompression).toBe(true);

    const config2 = loadIpfsPublisherConfig({
      IPFS_PUBLISHER_ENABLE_COMPRESSION: '1',
    });
    expect(config2.enableCompression).toBe(true);
  });
});
