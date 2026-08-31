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
  cryptoWaitReady,
  ed25519PairFromSeed,
  encodeAddress,
} from '@polkadot/util-crypto';
import { describe, expect, it } from 'vitest';
import {
  createFakeEnvelopePayload,
  parseFakeSensorCliOptions,
} from '../src/index.js';
import { SignedEnvelopeSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/crypto/v1/envelope_pb.js';
import { MessageSchema } from '@buf/airalab_connectivity-protocol.bufbuild_es/core/v1/message_pb.js';
import { fromBinary } from '@bufbuild/protobuf';

describe('fake sensor CLI', () => {
  it('parses args with defaults', async () => {
    await cryptoWaitReady();
    const signerSeedHex =
      '0x0000000000000000000000000000000000000000000000000000000000000001';
    const seed = Buffer.from(signerSeedHex.slice(2), 'hex');
    const expectedSensorId = encodeAddress(
      ed25519PairFromSeed(seed).publicKey,
      32
    );

    const options = parseFakeSensorCliOptions([], {});
    expect(options).toEqual({
      endpointUrl: 'http://localhost:3000/v1/telemetry',
      sensorId: expectedSensorId,
      count: 1,
      intervalMs: 1000,
      signerSeedHex,
    });
  });

  it('accepts cli args and env overrides', () => {
    const signerSeedHex =
      '0x0101010101010101010101010101010101010101010101010101010101010101';
    const seed = Buffer.from(signerSeedHex.slice(2), 'hex');
    const expectedSensorId = encodeAddress(
      ed25519PairFromSeed(seed).publicKey,
      32
    );

    const options = parseFakeSensorCliOptions(
      [
        '--endpoint',
        'http://localhost:4000/v1/telemetry',
        '--count=3',
        '--sensor-zone',
        'eu-west',
        '--signer-seed-hex',
        signerSeedHex,
      ],
      {
        SENSOR_FAKE_INTERVAL_MS: '250',
      }
    );

    expect(options).toEqual({
      endpointUrl: 'http://localhost:4000/v1/telemetry',
      sensorId: expectedSensorId,
      count: 3,
      intervalMs: 250,
      signerSeedHex,
      sensorZone: 'eu-west',
    });
  });

  it('creates fake signed protobuf envelope', () => {
    const signerSeedHex =
      '0x0101010101010101010101010101010101010101010101010101010101010101';
    const payload = createFakeEnvelopePayload(signerSeedHex);
    expect(payload.envelopeBytes.length).toBeGreaterThan(0);
    expect(payload.sensorAddress).toBe(
      encodeAddress(
        ed25519PairFromSeed(Buffer.from(signerSeedHex.slice(2), 'hex'))
          .publicKey,
        32
      )
    );
    expect(payload.nonce.length).toBe(16);

    const envelope = fromBinary(SignedEnvelopeSchema, payload.envelopeBytes);
    const message = fromBinary(MessageSchema, envelope.message);
    expect(message.metadata).toBeDefined();
    expect(message.metadata?.owner).toHaveLength(32);
    expect(message.payload?.case).toBe('urban');
    expect(message.payload?.value?.public).toHaveLength(2);

    const publicSensors = message.payload?.value?.public;
    expect(publicSensors?.[0]?.sensor?.case).toBe('bme280');
    expect(publicSensors?.[0]?.sensor?.value?.measurement?.case).toBe(
      'temperature'
    );
    expect(
      publicSensors?.[0]?.sensor?.value?.measurement?.value?.celsius
    ).toBeTypeOf('number');
    expect(publicSensors?.[1]?.sensor?.case).toBe('bme280');
    expect(publicSensors?.[1]?.sensor?.value?.measurement?.case).toBe(
      'humidity'
    );
    expect(
      publicSensors?.[1]?.sensor?.value?.measurement?.value?.percent
    ).toBeTypeOf('number');
  });

  it('throws for invalid numeric options', () => {
    expect(() => parseFakeSensorCliOptions(['--count', '0'], {})).toThrow(
      'Invalid positive integer value: 0'
    );
  });

  it('throws when sensor id does not match signer seed', () => {
    expect(() =>
      parseFakeSensorCliOptions(
        ['--sensor-id', '5DoHTsjp9DN9KEabruKS8p8wAAVHgrEiJ9vtyjAuGEMZqpWt'],
        {
          SENSOR_FAKE_SIGNER_SEED_HEX:
            '0x0202020202020202020202020202020202020202020202020202020202020202',
        }
      )
    ).toThrow('sensor id does not match signer seed');
  });
});
