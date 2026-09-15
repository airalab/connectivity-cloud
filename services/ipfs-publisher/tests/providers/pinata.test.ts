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
import { describe, expect, it, vi } from 'vitest';
import { createPinataProvider } from '../../src/providers/pinata.js';

function jsonResponse(body: unknown, ok = true, status = 200): Response {
  return {
    ok,
    status,
    json: async () => body,
    text: async () => JSON.stringify(body),
  } as Response;
}

describe('createPinataProvider', () => {
  it('requires a JWT to start', async () => {
    const provider = createPinataProvider({
      apiUrl: 'https://api.pinata.cloud',
      jwt: '',
    });

    await expect(provider.start()).rejects.toThrow(/JWT/);
  });

  it('uploads data and returns the CID from IpfsHash', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ IpfsHash: 'Qmfakecid' }));

    const provider = createPinataProvider(
      { apiUrl: 'https://api.pinata.cloud', jwt: 'test-jwt' },
      fetchMock as unknown as typeof fetch
    );

    await provider.start();
    const cid = await provider.add(new Uint8Array([1, 2, 3]), false);

    expect(cid).toBe('Qmfakecid');
    expect(fetchMock).toHaveBeenCalledWith(
      'https://api.pinata.cloud/pinning/pinFileToIPFS',
      expect.objectContaining({
        method: 'POST',
        headers: expect.objectContaining({
          Authorization: 'Bearer test-jwt',
        }),
      })
    );
  });

  it('throws on a non-ok response', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ error: 'nope' }, false, 500));

    const provider = createPinataProvider(
      { apiUrl: 'https://api.pinata.cloud', jwt: 'test-jwt' },
      fetchMock as unknown as typeof fetch
    );

    await provider.start();
    await expect(
      provider.add(new Uint8Array([1, 2, 3]), false)
    ).rejects.toThrow(/status 500/);
  });

  it('throws when the response has no IpfsHash', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({}));

    const provider = createPinataProvider(
      { apiUrl: 'https://api.pinata.cloud', jwt: 'test-jwt' },
      fetchMock as unknown as typeof fetch
    );

    await provider.start();
    await expect(
      provider.add(new Uint8Array([1, 2, 3]), false)
    ).rejects.toThrow(/IpfsHash/);
  });

  it('rejects add() calls before start()', async () => {
    const provider = createPinataProvider({
      apiUrl: 'https://api.pinata.cloud',
      jwt: 'test-jwt',
    });

    await expect(
      provider.add(new Uint8Array([1, 2, 3]), false)
    ).rejects.toThrow(/not started/);
  });
});
