import { Keyring } from '@polkadot/api';
import { cryptoWaitReady } from '@polkadot/util-crypto';
import type { AnchorSigner } from '../src/config.js';

await cryptoWaitReady();

/** Well-known dev account used wherever a test needs a signer. */
export const aliceSigner: AnchorSigner = new Keyring({
  type: 'sr25519',
}).addFromUri('//Alice');
