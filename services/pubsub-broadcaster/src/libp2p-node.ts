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
import { webSockets } from '@libp2p/websockets';
import { identify } from '@libp2p/identify';
import { generateKeyPair, generateKeyPairFromSeed } from '@libp2p/crypto/keys';
import type { PrivateKey } from '@libp2p/interface';
import { multiaddr, type Multiaddr } from '@multiformats/multiaddr';
import type { PubsubBroadcasterConfig } from './config.js';

const SEED_LENGTH_BYTES = 32;

type GossipsubServices = {
  identify: ReturnType<ReturnType<typeof identify>>;
  pubsub: ReturnType<ReturnType<typeof gossipsub>>;
};

export type GossipsubNode = Libp2p<GossipsubServices>;

export interface Libp2pPubsubClient {
  start(): Promise<void>;
  stop(): Promise<void>;
  publish(topic: string, data: Uint8Array): Promise<void>;
  /** Reserved peer ids that are currently connected (bounded by reserved peer list size). */
  getConnectedPeerIds(): string[];
  getConnectedPeerCount(): number;
  getNode(): GossipsubNode;
}

/**
 * Parse a hex-encoded 32-byte Ed25519 seed into raw bytes.
 */
function parseSeedHex(seedHex: string): Uint8Array {
  const normalized = seedHex.startsWith('0x') ? seedHex.slice(2) : seedHex;
  if (!/^[0-9a-fA-F]{64}$/.test(normalized)) {
    throw new Error('libp2p private key seed must be 32-byte hex');
  }
  return Uint8Array.from(Buffer.from(normalized, 'hex'));
}

/**
 * Load a stable Ed25519 libp2p identity from a configured hex seed, or
 * generate an ephemeral one if none is configured (suitable for local dev only).
 */
export async function loadLibp2pIdentity(
  seedHex: string | undefined,
  onEphemeral?: () => void
): Promise<PrivateKey> {
  if (seedHex) {
    const seed = parseSeedHex(seedHex);
    if (seed.length !== SEED_LENGTH_BYTES) {
      throw new Error(
        `libp2p private key seed must be ${SEED_LENGTH_BYTES} bytes`
      );
    }
    return generateKeyPairFromSeed('Ed25519', seed);
  }
  onEphemeral?.();
  return generateKeyPair('Ed25519');
}

function parseReservedPeers(reservedPeers: string[]): Multiaddr[] {
  return reservedPeers.map((peer) => multiaddr(peer));
}

function peerIdFromMultiaddr(addr: Multiaddr): string | null {
  const peerId = addr.getPeerId();
  return peerId ?? null;
}

export interface CreateLibp2pPubsubClientDeps {
  createNode?: (config: PubsubBroadcasterConfig) => Promise<GossipsubNode>;
  logInfo?: (message: string, context?: Record<string, unknown>) => void;
  logWarn?: (message: string, context?: Record<string, unknown>) => void;
}

/**
 * Create a libp2p-backed pubsub client with an embedded GossipSub node.
 * Reserved peers are dialed on start and automatically redialed after
 * disconnects; connected reserved peer ids are tracked for connectivity
 * monitoring.
 */
export async function createLibp2pPubsubClient(
  config: PubsubBroadcasterConfig,
  deps: CreateLibp2pPubsubClientDeps = {}
): Promise<Libp2pPubsubClient> {
  const logInfo = deps.logInfo ?? (() => undefined);
  const logWarn = deps.logWarn ?? (() => undefined);

  const createNode =
    deps.createNode ??
    (async (cfg: PubsubBroadcasterConfig): Promise<GossipsubNode> => {
      const privateKey = await loadLibp2pIdentity(
        cfg.libp2pPrivateKeySeedHex,
        () =>
          logWarn(
            'no libp2p private key seed configured; using an ephemeral identity (not suitable for production)'
          )
      );

      return createLibp2p({
        privateKey,
        addresses: { listen: cfg.libp2pListenAddresses },
        transports: [tcp(), webSockets()],
        connectionEncrypters: [noise()],
        streamMuxers: [yamux()],
        services: {
          identify: identify(),
          pubsub: gossipsub({ allowPublishToZeroTopicPeers: true }),
        },
      });
    });

  const node = await createNode(config);
  const reservedPeerAddrs = parseReservedPeers(config.reservedPeers);
  const reservedPeerIds = new Set(
    reservedPeerAddrs
      .map((addr) => peerIdFromMultiaddr(addr))
      .filter((id): id is string => id !== null)
  );
  const connectedReservedPeers = new Set<string>();
  let reconnectTimer: ReturnType<typeof setInterval> | null = null;

  function handlePeerConnect(peerId: string): void {
    if (reservedPeerIds.has(peerId)) {
      connectedReservedPeers.add(peerId);
      logInfo('reserved peer connected', {
        peerId,
        connectedCount: connectedReservedPeers.size,
      });
    }
  }

  function handlePeerDisconnect(peerId: string): void {
    if (reservedPeerIds.has(peerId)) {
      connectedReservedPeers.delete(peerId);
      logWarn('reserved peer disconnected', {
        peerId,
        connectedCount: connectedReservedPeers.size,
      });
    }
  }

  node.addEventListener('peer:connect', (event) => {
    handlePeerConnect(event.detail.toString());
  });
  node.addEventListener('peer:disconnect', (event) => {
    handlePeerDisconnect(event.detail.toString());
  });

  async function dialReservedPeers(): Promise<void> {
    for (const addr of reservedPeerAddrs) {
      const peerId = peerIdFromMultiaddr(addr);
      if (peerId && connectedReservedPeers.has(peerId)) {
        continue;
      }
      try {
        await node.dial(addr);
      } catch (error) {
        logWarn('failed to dial reserved peer', {
          addr: addr.toString(),
          error: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  let started = false;

  return {
    async start(): Promise<void> {
      await node.start();
      node.services.pubsub.subscribe(config.pubsubTopic);
      started = true;

      // Dial reserved peers immediately, then keep retrying disconnected ones.
      await dialReservedPeers();
      reconnectTimer = setInterval(() => {
        dialReservedPeers().catch((error: unknown) => {
          logWarn('reserved peer reconnect sweep failed', {
            error: error instanceof Error ? error.message : String(error),
          });
        });
      }, config.reconnectIntervalMs);
      reconnectTimer.unref?.();

      logInfo('libp2p node started', {
        peerId: node.peerId.toString(),
        listenAddresses: node.getMultiaddrs().map((addr) => addr.toString()),
        reservedPeers: config.reservedPeers,
      });
    },
    async stop(): Promise<void> {
      if (!started) {
        return;
      }
      started = false;
      if (reconnectTimer) {
        clearInterval(reconnectTimer);
        reconnectTimer = null;
      }
      await node.stop();
      logInfo('libp2p node stopped');
    },
    async publish(topic: string, data: Uint8Array): Promise<void> {
      if (!started) {
        throw new Error('libp2p pubsub client not started');
      }
      await node.services.pubsub.publish(topic, data);
    },
    getConnectedPeerIds(): string[] {
      return Array.from(connectedReservedPeers);
    },
    getConnectedPeerCount(): number {
      return connectedReservedPeers.size;
    },
    getNode(): GossipsubNode {
      return node;
    },
  };
}
