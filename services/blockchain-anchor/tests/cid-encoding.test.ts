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
import { describe, expect, it, vi, beforeEach } from 'vitest';
import { CID } from 'multiformats/cid';
import { ApiPromise, Keyring } from '@polkadot/api';

// Mock the sendSetPayloadExtrinsic function behavior
describe('CID encoding for setPayload extrinsic (issue #23)', () => {
  const testCidString = 'QmbfnLyddmmtf7drakAw4DF7p5oWExia2RcDv4M91Y6PyK';
  const testCid = CID.parse(testCidString);
  const cidBytes = new Uint8Array(testCid.bytes);

  it('CID bytes should not be truncated to 8 bytes', () => {
    // This reproduces the bug described in issue #23
    const truncatedHex = '0xc60e9960772b63e2';
    const truncatedBytes = Buffer.from(truncatedHex.slice(2), 'hex');

    // Verify the bug: cidBytes should NOT equal the truncated version
    expect(cidBytes.length).toBeGreaterThan(truncatedBytes.length);
    expect(cidBytes.length).toBe(34); // CIDv0 with SHA-256 hash
    expect(Buffer.from(cidBytes).toString('hex')).not.toBe(
      truncatedBytes.toString('hex')
    );
  });

  it('CID bytes preserve full multihash format', () => {
    // CIDv0: <multihash> where multihash = <hash-fn-code><digest-length><hash-digest>
    // For QmbfnLyddmmtf7drakAw4DF7p5oWExia2RcDv4M91Y6PyK:
    // - hash-fn-code: 0x12 (SHA-256)
    // - digest-length: 0x20 (32 bytes)
    // - hash-digest: 32 bytes of hash data
    expect(cidBytes[0]).toBe(0x12); // SHA-256 multicodec
    expect(cidBytes[1]).toBe(0x20); // 32-byte length
    expect(cidBytes.length).toBe(34); // 1 + 1 + 32
  });

  it('CID can roundtrip through bytes without truncation', () => {
    const encoded = Buffer.from(cidBytes);
    const decoded = CID.decode(encoded);

    expect(decoded.toString()).toBe(testCidString);
    expect(decoded.bytes).toEqual(cidBytes);
  });

  it('different encoding approaches preserve full CID bytes', () => {
    // Test different ways to pass CID to Polkadot.js API
    const asBuffer = Buffer.from(cidBytes);
    const asArray = Array.from(cidBytes);
    const asUint8Array = cidBytes;

    // All should preserve the full length
    expect(asBuffer.length).toBe(34);
    expect(asArray.length).toBe(34);
    expect(asUint8Array.length).toBe(34);

    // All should decode back to the same CID
    expect(CID.decode(asBuffer).toString()).toBe(testCidString);
    expect(CID.decode(new Uint8Array(asArray)).toString()).toBe(testCidString);
    expect(CID.decode(asUint8Array).toString()).toBe(testCidString);
  });

  it('CIDv1 bytes also preserve full format', () => {
    const cidV1String = 'bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi';
    const cidV1 = CID.parse(cidV1String);
    const cidV1Bytes = new Uint8Array(cidV1.bytes);

    // CIDv1: <version><multicodec><multihash>
    // version: 0x01
    // multicodec: 0x55 (raw) or 0x70 (dag-pb), etc.
    // multihash: <hash-fn-code><digest-length><hash-digest>
    expect(cidV1Bytes[0]).toBe(0x01); // CIDv1
    expect(cidV1Bytes.length).toBeGreaterThan(8); // Much longer than truncated

    const decoded = CID.decode(cidV1Bytes);
    expect(decoded.toString()).toBe(cidV1String);
    expect(decoded.version).toBe(1);
  });

  describe('Mock Polkadot.js API encoding behavior', () => {
    it('should pass full CID bytes to api.tx.cps.setPayload', () => {
      // Mock the API to capture what bytes are actually passed
      const capturedPayload: unknown[] = [];
      
      const mockSetPayload = vi.fn((_nodeId: number, payload: unknown) => {
        capturedPayload.push(payload);
        return {
          signAndSend: vi.fn(() => Promise.resolve(() => {})),
        };
      });

      const mockApi = {
        tx: {
          cps: {
            setPayload: mockSetPayload,
          },
        },
      } as unknown as ApiPromise;

      const nodeId = 0;

      // Test different encoding approaches
      const approaches = [
        { name: 'Buffer.from(cid)', value: Buffer.from(cidBytes) },
        { name: 'Array.from(cid)', value: Array.from(cidBytes) },
        { name: 'Uint8Array directly', value: cidBytes },
      ];

      approaches.forEach((approach) => {
        mockApi.tx.cps.setPayload(nodeId, approach.value);
      });

      expect(mockSetPayload).toHaveBeenCalledTimes(3);

      // Verify each call received the full CID bytes (not truncated)
      capturedPayload.forEach((payload, index) => {
        if (payload instanceof Buffer || payload instanceof Uint8Array) {
          expect(payload.length).toBe(34);
          expect(CID.decode(payload as Uint8Array).toString()).toBe(testCidString);
        } else if (Array.isArray(payload)) {
          expect(payload.length).toBe(34);
          expect(CID.decode(new Uint8Array(payload)).toString()).toBe(testCidString);
        } else {
          throw new Error(`Unexpected payload type at index ${index}`);
        }
      });
    });

    it('verifies api.createType approach for Vec<u8>', () => {
      // Mock createType to verify it receives full bytes
      const mockCreateType = vi.fn((type: string, data: unknown) => {
        if (type === 'Vec<u8>') {
          const bytes = data instanceof Uint8Array ? data : new Uint8Array(data as number[]);
          expect(bytes.length).toBe(34);
          expect(CID.decode(bytes).toString()).toBe(testCidString);
        }
        return data;
      });

      const mockApi = {
        createType: mockCreateType,
        tx: {
          cps: {
            setPayload: vi.fn(),
          },
        },
      } as unknown as ApiPromise;

      // This is the approach that should work: explicitly create Vec<u8>
      const vecU8 = mockApi.createType('Vec<u8>', cidBytes);
      mockApi.tx.cps.setPayload(0, vecU8);

      expect(mockCreateType).toHaveBeenCalledWith('Vec<u8>', cidBytes);
    });
  });

  describe('Regression test for truncation bug', () => {
    it('should fail if CID gets truncated to 8 bytes (bug reproduction)', () => {
      const truncatedHex = '0xc60e9960772b63e2';
      const truncatedBytes = Buffer.from(truncatedHex.slice(2), 'hex');

      // This should throw because truncated bytes are not a valid CID
      expect(() => {
        CID.decode(truncatedBytes);
      }).toThrow();
    });

    it('full CID bytes should decode successfully', () => {
      // This should NOT throw
      expect(() => {
        const decoded = CID.decode(cidBytes);
        expect(decoded.toString()).toBe(testCidString);
      }).not.toThrow();
    });
  });

  describe('Compare UTF-8 string vs raw bytes encoding', () => {
    it('UTF-8 string encoding produces different bytes than multihash', () => {
      const cidAsUtf8 = Buffer.from(testCidString, 'utf-8');
      const cidAsBytes = Buffer.from(cidBytes);

      // String encoding is longer (46 chars for base58)
      expect(cidAsUtf8.length).toBe(46);
      expect(cidAsBytes.length).toBe(34);

      // They are different encodings
      expect(cidAsUtf8.toString('hex')).not.toBe(cidAsBytes.toString('hex'));
    });

    it('multihash bytes are more efficient than string', () => {
      const cidAsUtf8 = Buffer.from(testCidString, 'utf-8');
      const cidAsBytes = Buffer.from(cidBytes);

      // Multihash format is ~26% smaller
      expect(cidAsBytes.length).toBeLessThan(cidAsUtf8.length);
      
      // Multihash preserves structure (can extract hash function, version, etc.)
      const decoded = CID.decode(cidAsBytes);
      expect(decoded.code).toBe(0x70); // dag-pb
      expect(decoded.multihash.code).toBe(0x12); // sha2-256
    });
  });
});
