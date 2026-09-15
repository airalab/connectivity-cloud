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
import {
  create as createKuboClient,
  type KuboRPCClient,
} from 'kubo-rpc-client';
import { logInfo } from '../logger.js';
import type { IpfsProvider } from './types.js';

/**
 * Create an IPFS provider backed by a Kubo (go-ipfs) RPC endpoint.
 */
export function createKuboProvider(apiUrl: string): IpfsProvider {
  const client: KuboRPCClient = createKuboClient({ url: apiUrl });
  let started = false;

  return {
    name: 'kubo',
    async start() {
      try {
        const version = await client.version();
        logInfo('connected to IPFS node', {
          provider: 'kubo',
          version: version.version,
          apiUrl,
        });
        started = true;
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : String(error);
        throw new Error(
          `Failed to connect to IPFS (kubo) at ${apiUrl}: ${errorMessage}`,
          { cause: error }
        );
      }
    },
    async stop() {
      if (!started) {
        return;
      }
      started = false;
      logInfo('IPFS provider stopped', { provider: 'kubo' });
    },
    async add(data: Uint8Array): Promise<string> {
      if (!started) {
        throw new Error('kubo provider not started');
      }
      const result = await client.add(data);
      return result.cid.toString();
    },
  };
}
