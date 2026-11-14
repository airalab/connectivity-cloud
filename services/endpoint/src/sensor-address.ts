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

/** Browser-compatible SS58 conversion for 32-byte public keys (network prefix 32). */
export function sensorIdToSs58(hex: string): string {
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
    throw new Error('sensor ID must be exactly 32 bytes of hexadecimal');
  }

  const alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz';
  const input = new Uint8Array(35);
  input[0] = 32;
  for (let i = 0; i < 32; i += 1) {
    input[i + 1] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  const prefix = new TextEncoder().encode('SS58PRE');
  const message = new Uint8Array(prefix.length + 33);
  message.set(prefix);
  message.set(input.subarray(0, 33), prefix.length);

  const mask = (1n << 64n) - 1n;
  const iv = [
    0x6a09e667f3bcc908n,
    0xbb67ae8584caa73bn,
    0x3c6ef372fe94f82bn,
    0xa54ff53a5f1d36f1n,
    0x510e527fade682d1n,
    0x9b05688c2b3e6c1fn,
    0x1f83d9abfb41bd6bn,
    0x5be0cd19137e2179n,
  ];
  const sigma = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
  ];
  const h = iv.slice();
  h[0] = h[0]! ^ 0x01010040n;
  const block = new Uint8Array(128);
  block.set(message);
  const m = Array.from({ length: 16 }, (_, i) => {
    let word = 0n;
    for (let j = 0; j < 8; j += 1) {
      word |= BigInt(block[i * 8 + j]!) << BigInt(j * 8);
    }
    return word;
  });
  const v = [...h, ...iv];
  v[12] = v[12]! ^ BigInt(message.length);
  v[14] = v[14]! ^ mask;

  const rotateRight = (x: bigint, bits: bigint): bigint =>
    ((x >> bits) | (x << (64n - bits))) & mask;
  const mix = (
    a: number,
    b: number,
    c: number,
    d: number,
    x: bigint,
    y: bigint
  ) => {
    v[a] = (v[a]! + v[b]! + x) & mask;
    v[d] = rotateRight(v[d]! ^ v[a]!, 32n);
    v[c] = (v[c]! + v[d]!) & mask;
    v[b] = rotateRight(v[b]! ^ v[c]!, 24n);
    v[a] = (v[a]! + v[b]! + y) & mask;
    v[d] = rotateRight(v[d]! ^ v[a]!, 16n);
    v[c] = (v[c]! + v[d]!) & mask;
    v[b] = rotateRight(v[b]! ^ v[c]!, 63n);
  };
  for (let round = 0; round < 12; round += 1) {
    const s = sigma[round]!;
    mix(0, 4, 8, 12, m[s[0]!]!, m[s[1]!]!);
    mix(1, 5, 9, 13, m[s[2]!]!, m[s[3]!]!);
    mix(2, 6, 10, 14, m[s[4]!]!, m[s[5]!]!);
    mix(3, 7, 11, 15, m[s[6]!]!, m[s[7]!]!);
    mix(0, 5, 10, 15, m[s[8]!]!, m[s[9]!]!);
    mix(1, 6, 11, 12, m[s[10]!]!, m[s[11]!]!);
    mix(2, 7, 8, 13, m[s[12]!]!, m[s[13]!]!);
    mix(3, 4, 9, 14, m[s[14]!]!, m[s[15]!]!);
  }
  for (let i = 0; i < 8; i += 1) h[i] = h[i]! ^ v[i]! ^ v[i + 8]!;
  for (let i = 0; i < 2; i += 1) {
    input[33 + i] = Number((h[0]! >> BigInt(i * 8)) & 255n);
  }

  let value = 0n;
  for (const byte of input) value = (value << 8n) | BigInt(byte);
  let encoded = '';
  while (value > 0n) {
    const remainder = Number(value % 58n);
    encoded = alphabet[remainder]! + encoded;
    value /= 58n;
  }
  let leadingZeroes = 0;
  while (leadingZeroes < input.length && input[leadingZeroes] === 0) {
    leadingZeroes += 1;
  }
  return '1'.repeat(leadingZeroes) + encoded;
}
