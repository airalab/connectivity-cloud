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
import { logInfo } from '../logger.js';
import type { IpfsProvider } from './types.js';

export interface PinataProviderConfig {
  apiUrl: string;
  jwt: string;
}

interface PinataPinResponse {
  IpfsHash: string;
}

/**
 * Create an IPFS provider backed by Pinata's pinning REST API.
 * https://docs.pinata.cloud/api-reference/endpoint/pin-file-to-ipfs
 */
export function createPinataProvider(
  config: PinataProviderConfig,
  fetchImpl: typeof fetch = fetch
): IpfsProvider {
  let started = false;

  return {
    name: 'pinata',
    async start() {
      if (!config.jwt) {
        throw new Error('Pinata provider requires a JWT credential');
      }
      started = true;
      logInfo('pinata provider ready', {
        provider: 'pinata',
        apiUrl: config.apiUrl,
      });
    },
    async stop() {
      if (!started) {
        return;
      }
      started = false;
      logInfo('IPFS provider stopped', { provider: 'pinata' });
    },
    async add(data: Uint8Array): Promise<string> {
      if (!started) {
        throw new Error('pinata provider not started');
      }

      const formData = new FormData();
      formData.append(
        'file',
        new Blob([data as BlobPart]),
        'telemetry-batch.bin'
      );

      const response = await fetchImpl(
        `${config.apiUrl}/pinning/pinFileToIPFS`,
        {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${config.jwt}`,
          },
          body: formData,
        }
      );

      if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(
          `Pinata publish failed with status ${response.status}: ${body}`
        );
      }

      const result = (await response.json()) as PinataPinResponse;
      if (!result.IpfsHash) {
        throw new Error('Pinata response did not include an IpfsHash');
      }

      return result.IpfsHash;
    },
  };
}
