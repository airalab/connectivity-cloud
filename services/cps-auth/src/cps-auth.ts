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
import type { SensorAuth } from '@scp/core';
import { formatSensorId } from '@scp/core';
import { ApiPromise, WsProvider } from '@polkadot/api';
import { decodeAddress } from '@polkadot/util-crypto';
import type { CpsAuthConfig } from './config.js';
import {
  PolkadotCpsChainReader,
  type CpsChainReader,
  type CpsNodeId,
} from './cps-chain-reader.js';
import { LruCache } from './lru-cache.js';
import { logDebug, logError, logInfo } from './logger.js';

/**
 * Upper bound on parent links followed per lookup. CPS bounds depth per Scope
 * only, so the walk carries its own cap to stay finite on any input.
 */
const MAX_TRAVERSAL_STEPS = 1024;
const DEFAULT_MAX_NONCES = 100_000;

/** Options for {@link CpsAuth}. */
export interface CpsAuthOptions {
  /** Node id of the `sensors.social` root. */
  rootNodeId: CpsNodeId;
  cacheMaxSize: number;
  negativeTtlSeconds: number;
  maxNonces?: number;
  /** Clock in milliseconds; injectable for tests. */
  now?: () => number;
}

function toHex(bytes: Uint8Array): string {
  return Buffer.from(bytes).toString('hex');
}

/**
 * Resolves and validates the `sensors.social` root node.
 *
 * @throws If the address has no CPS node or the node is not a tree root
 */
export async function resolveRootNode(
  reader: CpsChainReader,
  rootAddress: string
): Promise<CpsNodeId> {
  const nodeId = await reader.getNodeByIdentity(decodeAddress(rootAddress));
  if (nodeId === null) {
    throw new Error(
      `CPS_ROOT_ADDRESS ${rootAddress} is not the identity of any CPS node on this chain`
    );
  }
  const info = await reader.getNodeInfo(nodeId);
  if (info === null) {
    throw new Error(`CPS root node ${nodeId} does not exist on this chain`);
  }
  if (info.parent !== null) {
    throw new Error(
      `CPS root node ${nodeId} has a parent (${info.parent}); expected a tree root`
    );
  }
  return nodeId;
}

/**
 * Read-only CPS subtree sensor authentication.
 *
 * A sensor is authorized when the CPS node identified by its public key is a
 * strict descendant of the configured root node. Positive decisions are cached
 * without expiry in a bounded LRU; negative decisions expire after a TTL; RPC
 * failures are never cached and deny access (fail closed).
 */
export class CpsAuth implements SensorAuth {
  private readonly cache: LruCache<string, boolean>;
  private readonly inFlight = new Map<string, Promise<boolean>>();
  private readonly nonces = new Set<string>();
  private readonly negativeTtlMs: number;
  private readonly maxNonces: number;
  private disconnect: (() => Promise<void>) | undefined;

  constructor(
    private readonly reader: CpsChainReader,
    private readonly options: CpsAuthOptions
  ) {
    this.cache = new LruCache(options.cacheMaxSize, options.now);
    this.negativeTtlMs = options.negativeTtlSeconds * 1000;
    this.maxNonces = options.maxNonces ?? DEFAULT_MAX_NONCES;
  }

  /**
   * Connects to the chain, validates the configured root and returns a ready
   * provider.
   *
   * @throws If the node is unreachable or the root fails validation
   */
  static async connect(config: CpsAuthConfig): Promise<CpsAuth> {
    const api = await ApiPromise.create({
      provider: new WsProvider(config.substrateWsUrl),
    });
    try {
      const reader = new PolkadotCpsChainReader(api);
      const rootNodeId = await resolveRootNode(reader, config.rootAddress);
      logInfo('cps auth initialized', {
        root_node_id: rootNodeId.toString(),
        cache_max_size: config.cacheMaxSize,
        negative_ttl_seconds: config.negativeTtlSeconds,
      });
      const auth = new CpsAuth(reader, {
        rootNodeId,
        cacheMaxSize: config.cacheMaxSize,
        negativeTtlSeconds: config.negativeTtlSeconds,
      });
      auth.disconnect = () => api.disconnect();
      return auth;
    } catch (error) {
      await api.disconnect();
      throw error;
    }
  }

  /** Closes the chain connection, if this instance owns one. */
  async close(): Promise<void> {
    await this.disconnect?.();
  }

  async authenticate(sensorId: Uint8Array): Promise<boolean> {
    const key = toHex(sensorId);
    const cached = this.cache.get(key);
    if (cached !== undefined) {
      logDebug('cps authentication cache hit', {
        sensor_id: formatSensorId(sensorId),
        allowed: cached,
      });
      return cached;
    }

    let pending = this.inFlight.get(key);
    if (pending === undefined) {
      pending = this.lookup(sensorId, key).finally(() => {
        this.inFlight.delete(key);
      });
      this.inFlight.set(key, pending);
    }
    return pending;
  }

  private async lookup(sensorId: Uint8Array, key: string): Promise<boolean> {
    try {
      const allowed = await this.isRootDescendant(sensorId);
      if (allowed) {
        this.cache.set(key, true);
      } else {
        this.cache.set(key, false, this.negativeTtlMs);
      }
      logDebug('cps authentication resolved', {
        sensor_id: formatSensorId(sensorId),
        allowed,
      });
      return allowed;
    } catch (error) {
      logError('cps authentication lookup failed; denying', error, {
        sensor_id: formatSensorId(sensorId),
      });
      return false;
    }
  }

  private async isRootDescendant(sensorId: Uint8Array): Promise<boolean> {
    const nodeId = await this.reader.getNodeByIdentity(sensorId);
    if (nodeId === null) {
      return false;
    }

    const visited = new Set<CpsNodeId>([nodeId]);
    let info = await this.reader.getNodeInfo(nodeId);
    for (let step = 0; step < MAX_TRAVERSAL_STEPS; step++) {
      if (info === null || info.parent === null) {
        return false;
      }
      if (info.parent === this.options.rootNodeId) {
        return true;
      }
      if (visited.has(info.parent)) {
        return false;
      }
      visited.add(info.parent);
      info = await this.reader.getNodeInfo(info.parent);
    }
    return false;
  }

  async isNonceSeen(sensorId: Uint8Array, nonce: Uint8Array): Promise<boolean> {
    return this.nonces.has(`${toHex(sensorId)}:${toHex(nonce)}`);
  }

  async rememberNonce(sensorId: Uint8Array, nonce: Uint8Array): Promise<void> {
    this.nonces.add(`${toHex(sensorId)}:${toHex(nonce)}`);
    if (this.nonces.size > this.maxNonces) {
      const oldest = this.nonces.values().next();
      if (!oldest.done) {
        this.nonces.delete(oldest.value);
      }
    }
  }
}
