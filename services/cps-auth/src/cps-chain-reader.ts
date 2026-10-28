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
import type { ApiPromise } from '@polkadot/api';

/** CPS node identifier. */
export type CpsNodeId = bigint;

/** Topology record of a CPS node. */
export interface CpsNodeInfo {
  /** `null` for a root node. */
  parent: CpsNodeId | null;
}

/** Read-only view of the CPS pallet needed for subtree authorization. */
export interface CpsChainReader {
  /** Node whose identity account is `account` (32-byte public key), if any. */
  getNodeByIdentity(account: Uint8Array): Promise<CpsNodeId | null>;
  /** Topology record of `nodeId`, or `null` if the node does not exist. */
  getNodeInfo(nodeId: CpsNodeId): Promise<CpsNodeInfo | null>;
}

interface OptionCodec {
  readonly isSome: boolean;
  unwrap(): { toString(): string };
}

interface NodeInfoCodec {
  parent: OptionCodec;
}

interface CpsQueries {
  identityNode?: (account: Uint8Array) => Promise<unknown>;
  nodes?: (nodeId: bigint) => Promise<unknown>;
}

/** CPS chain reader backed by a connected `@polkadot/api` instance. */
export class PolkadotCpsChainReader implements CpsChainReader {
  constructor(private readonly api: ApiPromise) {}

  private queries(): Required<CpsQueries> {
    const cps = (this.api.query as unknown as { cps?: CpsQueries }).cps;
    if (!cps?.identityNode || !cps.nodes) {
      throw new Error(
        'CPS pallet storage (identityNode, nodes) is not available on this chain'
      );
    }
    return cps as Required<CpsQueries>;
  }

  async getNodeByIdentity(account: Uint8Array): Promise<CpsNodeId | null> {
    const raw = (await this.queries().identityNode(account)) as OptionCodec;
    return raw.isSome ? BigInt(raw.unwrap().toString()) : null;
  }

  async getNodeInfo(nodeId: CpsNodeId): Promise<CpsNodeInfo | null> {
    const raw = (await this.queries().nodes(nodeId)) as unknown as {
      isSome: boolean;
      unwrap(): NodeInfoCodec;
    };
    if (!raw.isSome) {
      return null;
    }
    const parent = raw.unwrap().parent;
    return {
      parent: parent.isSome ? BigInt(parent.unwrap().toString()) : null,
    };
  }
}
