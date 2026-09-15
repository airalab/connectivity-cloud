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
import type { IpfsPublisherConfig } from '../config.js';
import { createKuboProvider } from './kubo.js';
import { createPinataProvider } from './pinata.js';
import type { IpfsProvider } from './types.js';

export type { IpfsProvider } from './types.js';

/**
 * Build the ordered list of IPFS providers from configuration. The first
 * entry is the primary provider; the rest are fallbacks used for
 * replication and durability.
 */
export function createProvidersFromConfig(
  config: IpfsPublisherConfig
): IpfsProvider[] {
  return config.providers.map((name) => {
    switch (name) {
      case 'kubo':
        return createKuboProvider(config.ipfsApiUrl);
      case 'pinata':
        return createPinataProvider({
          apiUrl: config.pinataApiUrl,
          jwt: config.pinataJwt,
        });
      default:
        throw new Error(`unknown IPFS provider "${name}"`);
    }
  });
}
