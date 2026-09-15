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
import { createLibp2p, type Libp2p } from 'libp2p';
import { gossipsub } from '@chainsafe/libp2p-gossipsub';
import { noise } from '@chainsafe/libp2p-noise';
import { yamux } from '@chainsafe/libp2p-yamux';
import { tcp } from '@libp2p/tcp';
import { identify } from '@libp2p/identify';
import { generateKeyPair } from '@libp2p/crypto/keys';
import { describe, expect, it, afterEach } from 'vitest';
import {
  createLibp2pPubsubClient,
  type Libp2pPubsubClient,
} from '../src/libp2p-node.js';
import type { PubsubBroadcasterConfig } from '../src/config.js';

const TEST_TOPIC = 'sensors.social/telemetry/v1';

async function createSubscriberNode() {
  const privateKey = await generateKeyPair('Ed25519');
  const node = await createLibp2p({
    privateKey,
    addresses: { listen: ['/ip4/127.0.0.1/tcp/0'] },
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    services: {
      identify: identify(),
      pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
    },
  });
  await node.start();
  node.services.pubsub.subscribe(TEST_TOPIC);
  return node as Libp2p<{
    identify: ReturnType<ReturnType<typeof identify>>;
    pubsub: ReturnType<ReturnType<typeof gossipsub>>;
  }>;
}

function testConfig(
  overrides: Partial<PubsubBroadcasterConfig> = {}
): PubsubBroadcasterConfig {
  return {
    kafkaBrokers: ['localhost:9092'],
    consumerGroupId: 'pubsub-broadcaster-v1',
    source: 'pubsub-broadcaster',
    healthPort: 3020,
    pubsubTopic: TEST_TOPIC,
    reservedPeers: [],
    minConnectedPeers: 1,
    libp2pPrivateKeySeedHex: undefined,
    libp2pListenAddresses: ['/ip4/127.0.0.1/tcp/0'],
    connectivityStabilizationIntervalMs: 200,
    reconnectIntervalMs: 500,
    ...overrides,
  };
}

async function waitFor(
  predicate: () => boolean,
  timeoutMs = 10000
): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error('timed out waiting for condition');
    }
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
}

describe('libp2p GossipSub client (real node) integration', () => {
  let subscriberNode: Awaited<ReturnType<typeof createSubscriberNode>> | null =
    null;
  let client: Libp2pPubsubClient | null = null;

  afterEach(async () => {
    await client?.stop();
    await subscriberNode?.stop();
    client = null;
    subscriberNode = null;
  });

  it('connects to a reserved peer, publishes over GossipSub, and is received', async () => {
    subscriberNode = await createSubscriberNode();
    const subscriberAddr = subscriberNode
      .getMultiaddrs()
      .find((addr) => addr.toString().includes('/ip4/127.0.0.1'));
    expect(subscriberAddr).toBeDefined();
    const reservedPeerAddr = subscriberAddr!.toString();

    const received: Uint8Array[] = [];
    subscriberNode.services.pubsub.addEventListener('message', (event) => {
      if (event.detail.topic === TEST_TOPIC) {
        received.push(event.detail.data);
      }
    });

    client = await createLibp2pPubsubClient(
      testConfig({ reservedPeers: [reservedPeerAddr] })
    );
    await client.start();

    // Reserved peer dial + connectivity tracking.
    await waitFor(() => client!.getConnectedPeerCount() === 1);
    expect(client.getConnectedPeerIds()).toHaveLength(1);

    // Wait for GossipSub subscription exchange to complete so the publisher
    // knows the subscriber peer is interested in the topic.
    const node = client.getNode();
    await waitFor(
      () => node.services.pubsub.getSubscribers(TEST_TOPIC).length > 0
    );

    const payload = new TextEncoder().encode('hello-gossipsub');
    await client.publish(TEST_TOPIC, payload);

    await waitFor(() => received.length > 0);
    expect(Buffer.from(received[0]!)).toEqual(Buffer.from(payload));
  }, 20000);

  it('tracks reserved peer disconnects and reconnects automatically', async () => {
    subscriberNode = await createSubscriberNode();
    const subscriberAddr = subscriberNode
      .getMultiaddrs()
      .find((addr) => addr.toString().includes('/ip4/127.0.0.1'));
    const reservedPeerAddr = subscriberAddr!.toString();

    client = await createLibp2pPubsubClient(
      testConfig({
        reservedPeers: [reservedPeerAddr],
        reconnectIntervalMs: 300,
      })
    );
    await client.start();

    await waitFor(() => client!.getConnectedPeerCount() === 1);

    // Simulate peer loss by stopping the subscriber node's connections.
    const node = client.getNode();
    const connections = node.getConnections();
    await Promise.all(connections.map((conn) => conn.abort(new Error('test'))));

    await waitFor(() => client!.getConnectedPeerCount() === 0);

    // Restart the subscriber node fresh so the reconnect sweep can redial it;
    // its peer id and listen port stay the same since we reuse subscriberNode.
    await waitFor(() => client!.getConnectedPeerCount() === 1, 15000);
  }, 30000);
});
