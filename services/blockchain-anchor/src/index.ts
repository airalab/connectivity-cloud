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
  TELEMETRY_TOPICS,
  EnvelopeSchema,
  TelemetryBatchedPayloadSchema,
  type TelemetryBatchedPayload,
  installShutdownHandler,
} from '@scp/core';
import { fromBinary, type MessageShape } from '@bufbuild/protobuf';
import { Admin, Consumer, Producer } from '@platformatic/kafka';
import {
  closeRedisClient,
  createKafkaClientOptions,
  createRedisClient,
  loadKafkaSecurityConfig,
  loadRedisConfig,
} from '@scp/infra';
import { ApiPromise, WsProvider, Keyring } from '@polkadot/api';
import { blake2AsU8a } from '@polkadot/util-crypto';
import { createServer, type Server } from 'node:http';
import { fileURLToPath } from 'node:url';
import {
  loadBlockchainAnchorConfig,
  type BlockchainAnchorConfig,
} from './config.js';
import { logInfo, logWarn, logDebug, logError } from './logger.js';

interface BlockchainAnchorMetrics {
  consumed: number;
  anchored: number;
  skippedDuplicate: number;
  failed: number;
  /** Payloads rejected because they exceed the defensive size invariant. */
  rejectedOversized: number;
}

interface BlockchainAnchorStatus {
  observedAt: string;
  queuedBatches: number | null;
  lastFinalizedAt: string | null;
  lastFinalizedBlock: string | null;
  lastBatchId: string | null;
  lastFinalizedObservationStale: boolean | null;
  freeBalance: string | null;
  tokenSymbol: string | null;
  tokenDecimals: number | null;
  bestBlock: string | null;
  finalizedBlock: string | null;
  finalizationGap: number | null;
}

export interface BlockchainAnchorService {
  start(): Promise<void>;
  stop(): Promise<void>;
  getMetrics(): Readonly<BlockchainAnchorMetrics>;
}

export interface BlockchainAnchorDeps {
  createConsumer?: () => Consumer;
  createProducer?: () => Producer;
  createAdmin?: () => Admin;
  createStatusRedis?: () => Promise<
    Awaited<ReturnType<typeof createRedisClient>>
  >;
  createApi?: () => Promise<ApiPromise>;
  createHealthServer?: (
    getMetrics: () => BlockchainAnchorMetrics,
    port: number,
    getStatus?: () => Promise<BlockchainAnchorStatus>
  ) => Server;
}

/**
 * Minimal shape of the `Option<BoundedVec<u8>>` codec returned by the CPS
 * pallet's `payload` storage query. Modeled loosely (rather than via
 * generated types) since this service talks to the chain dynamically.
 */
interface OptionBytesCodec {
  readonly isSome?: boolean;
  readonly isEmpty?: boolean;
  unwrap?: () => { toU8a: (isBare?: boolean) => Uint8Array };
}

const STORE_FINALIZED_OBSERVATION_SCRIPT = `
local key = KEYS[1]
local incoming = ARGV[1]
local current = redis.call('HGET', key, 'lastFinalizedBlockNumber')
if current and (#current > #incoming or (#current == #incoming and current >= incoming)) then
  return 0
end
redis.call('HSET', key,
  'lastFinalizedBlockNumber', incoming,
  'lastFinalizedBlock', ARGV[2],
  'lastFinalizedBlockHash', ARGV[3],
  'lastBatchId', ARGV[4],
  'lastFinalizedAt', ARGV[5])
return 1
`;

interface KafkaOffsetAdmin {
  metadata(options: { topics: string[] }): Promise<{
    topics: Map<string, { partitions: unknown[] }>;
  }>;
  listOffsets(options: {
    topics: Array<{
      name: string;
      partitions: Array<{ partitionIndex: number; timestamp: bigint }>;
    }>;
  }): Promise<
    Array<{
      name: string;
      partitions: Array<{ partitionIndex: number; offset: bigint }>;
    }>
  >;
  listConsumerGroupOffsets(options: {
    groups: Array<{
      groupId: string;
      topics: Array<{ name: string; partitionIndexes: number[] }>;
    }>;
  }): Promise<
    Array<{
      groupId: string;
      topics: Array<{
        name: string;
        partitions: Array<{ partitionIndex: number; committedOffset: bigint }>;
      }>;
    }>
  >;
}

export async function getQueuedBatchCount(
  admin: KafkaOffsetAdmin,
  groupId: string,
  topic: string
): Promise<number | null> {
  const metadata = await admin.metadata({
    topics: [topic],
    autocreateTopics: false,
  });
  const partitionCount = metadata.topics.get(topic)?.partitions.length ?? 0;
  if (partitionCount === 0) return null;
  const partitionIndexes = Array.from(
    { length: partitionCount },
    (_, index) => index
  );
  const [endTopics, groups] = await Promise.all([
    admin.listOffsets({
      topics: [
        {
          name: topic,
          partitions: partitionIndexes.map((partitionIndex) => ({
            partitionIndex,
            timestamp: -1n,
          })),
        },
      ],
    }),
    admin.listConsumerGroupOffsets({
      groups: [{ groupId, topics: [{ name: topic, partitionIndexes }] }],
    }),
  ]);
  const endByPartition = new Map(
    endTopics
      .find((entry) => entry.name === topic)
      ?.partitions.map((partition) => [
        partition.partitionIndex,
        partition.offset,
      ])
  );
  const committedByPartition = new Map(
    groups
      .find((group) => group.groupId === groupId)
      ?.topics.find((entry) => entry.name === topic)
      ?.partitions.map((partition) => [
        partition.partitionIndex,
        partition.committedOffset,
      ])
  );
  let total = 0n;
  for (const partition of partitionIndexes) {
    const end = endByPartition.get(partition);
    const committed = committedByPartition.get(partition);
    if (end === undefined || committed === undefined || committed < 0n) {
      return null;
    }
    if (end > committed) total += end - committed;
  }
  return total <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(total) : null;
}

function emptyStatus(now: () => number = Date.now): BlockchainAnchorStatus {
  return {
    observedAt: new Date(now()).toISOString(),
    queuedBatches: null,
    lastFinalizedAt: null,
    lastFinalizedBlock: null,
    lastBatchId: null,
    lastFinalizedObservationStale: null,
    freeBalance: null,
    tokenSymbol: null,
    tokenDecimals: null,
    bestBlock: null,
    finalizedBlock: null,
    finalizationGap: null,
  };
}

/**
 * Read the payload currently anchored on-chain for a CPS node, if any.
 *
 * This is the authoritative idempotency check: rather than relying on local
 * process state (which can't observe whether an extrinsic actually
 * finalized before a crash), we ask the chain what payload is currently set
 * for the node and compare its content hash against the batch we are about
 * to submit. If they already match, the anchor operation is a no-op and is
 * skipped. The Kafka offset for `telemetry.batched.v1` is only committed
 * once this check (and, if needed, the extrinsic) has completed, so an
 * uncommitted offset itself durably marks a batch as "not yet confirmed
 * anchored" — no separate persisted queue is required.
 */
async function getAnchoredPayload(
  api: ApiPromise,
  nodeId: number
): Promise<Uint8Array | null> {
  if (!api.query.cps?.payload) {
    return null;
  }

  try {
    const raw = (await api.query.cps.payload(
      nodeId
    )) as unknown as OptionBytesCodec;

    if (!raw || raw.isEmpty || raw.isSome === false || !raw.unwrap) {
      return null;
    }

    // `toU8a()` defaults to including the SCALE compact-length prefix used
    // to encode `BoundedVec<u8>`, but `payload.payload` is just the bare zstd
    // bytes. Pass `true` to strip the length prefix so the comparison is
    // byte-for-byte against the same shape we submit.
    return raw.unwrap().toU8a(true);
  } catch (error) {
    logWarn('failed to read on-chain payload for idempotency check', {
      nodeId,
      reason: error instanceof Error ? error.message : String(error),
    });
    return null;
  }
}

/**
 * Compare a batch's content hash (`payload_hash`, computed by the batcher as
 * `blake2b-256` of the compressed payload) against the same hash computed
 * over the payload currently anchored on-chain.
 *
 * Comparing hashes rather than raw bytes is cheaper and, more importantly,
 * keys the comparison on the same content-addressed identity the batcher
 * assigns to the batch, rather than depending on decoding the exact codec
 * shape returned by the chain.
 */
function anchoredPayloadMatches(
  anchoredPayload: Uint8Array,
  expectedPayloadHash: Uint8Array
): boolean {
  const anchoredHash = blake2AsU8a(anchoredPayload, 256);
  return bytesEqual(anchoredHash, expectedPayloadHash);
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) {
    return false;
  }
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) {
      return false;
    }
  }
  return true;
}

function formatBalance(value: string, decimals: number): string | null {
  if (!/^-?\d+$/.test(value) || !Number.isInteger(decimals) || decimals < 0) {
    return null;
  }
  const amount = BigInt(value);
  const negative = amount < 0n;
  const digits = (negative ? -amount : amount)
    .toString()
    .padStart(decimals + 1, '0');
  if (decimals === 0) return `${negative ? '-' : ''}${digits}`;
  const integer = digits.slice(0, -decimals);
  const fraction = digits.slice(-decimals).replace(/0+$/, '');
  return `${negative ? '-' : ''}${integer}${fraction ? `.${fraction}` : ''}`;
}

function timeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error('status observation timed out')),
      timeoutMs
    );
    timer.unref?.();
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error: unknown) => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

async function observeBlockchain(
  api: ApiPromise,
  keyring: Keyring,
  suri: string,
  admin: Admin | null,
  redis: Awaited<ReturnType<typeof createRedisClient>> | null,
  groupId: string,
  now: () => number = Date.now
): Promise<BlockchainAnchorStatus> {
  const result = emptyStatus(now);
  const address = keyring.addFromUri(suri).address;
  const key =
    process.env.BLOCKCHAIN_ANCHOR_STATUS_REDIS_KEY ??
    'blockchain-anchor:v1:last-finalized';
  const [account, best, finalized, backlog, observation] =
    await Promise.allSettled([
      timeout(api.query.system.account(address), 3000),
      timeout(api.rpc.chain.getHeader(), 3000),
      timeout(
        api.rpc.chain
          .getFinalizedHead()
          .then((hash) => api.rpc.chain.getHeader(hash)),
        3000
      ),
      admin
        ? timeout(
            getQueuedBatchCount(admin, groupId, TELEMETRY_TOPICS.BATCHED),
            3000
          )
        : Promise.resolve(null),
      redis ? timeout(redis.hgetall(key), 2000) : Promise.resolve(null),
    ]);

  if (backlog.status === 'fulfilled') result.queuedBatches = backlog.value;
  if (
    observation.status === 'fulfilled' &&
    observation.value &&
    observation.value.lastFinalizedAt &&
    /^\d+$/.test(observation.value.lastFinalizedBlock ?? '')
  ) {
    const observedAt = Date.parse(observation.value.lastFinalizedAt);
    if (Number.isFinite(observedAt)) {
      result.lastFinalizedAt = observation.value.lastFinalizedAt;
      result.lastFinalizedBlock = observation.value.lastFinalizedBlock ?? null;
      result.lastBatchId = observation.value.lastBatchId ?? null;
      const staleAfterMs = Number.parseInt(
        process.env.BLOCKCHAIN_ANCHOR_OBSERVATION_STALE_MS ?? '',
        10
      );
      result.lastFinalizedObservationStale =
        now() - observedAt >
        (Number.isSafeInteger(staleAfterMs) && staleAfterMs > 0
          ? staleAfterMs
          : 86_400_000);
    }
  }
  if (account.status === 'fulfilled') {
    const data = account.value.data;
    const decimals = api.registry.chainDecimals[0];
    const symbol = api.registry.chainTokens[0];
    if (typeof decimals === 'number' && typeof symbol === 'string') {
      result.tokenDecimals = decimals;
      result.tokenSymbol = symbol;
      result.freeBalance = formatBalance(data.free.toString(), decimals);
    }
  }
  if (best.status === 'fulfilled') {
    result.bestBlock = best.value.number.toString();
  }
  if (finalized.status === 'fulfilled') {
    result.finalizedBlock = finalized.value.number.toString();
  }
  if (result.bestBlock !== null && result.finalizedBlock !== null) {
    const bestHeight = BigInt(result.bestBlock);
    const finalizedHeight = BigInt(result.finalizedBlock);
    if (bestHeight >= finalizedHeight) {
      const gap = bestHeight - finalizedHeight;
      result.finalizationGap =
        gap <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(gap) : null;
    }
  }
  return result;
}

/**
 * Forward an oversized `telemetry.batched.v1` record to the DLQ topic,
 * preserving its original bytes. This is a permanent upstream contract
 * violation (the batcher is supposed to guarantee `payload <=
 * maxPayloadBytes`), so the record must be durably routed to the DLQ rather
 * than silently dropped: the offset is only committed after this publish is
 * confirmed.
 */
async function publishOversizedToDlq(
  raw: Uint8Array,
  partition: number,
  offset: bigint,
  reason: string,
  producer: Producer,
  config: BlockchainAnchorConfig
): Promise<void> {
  await producer.send({
    messages: [
      {
        topic: TELEMETRY_TOPICS.DLQ,
        value: Buffer.from(raw),
        headers: {
          source_topic: Buffer.from(TELEMETRY_TOPICS.BATCHED),
          source_service: Buffer.from(config.source),
          source_partition: Buffer.from(String(partition)),
          source_offset: Buffer.from(String(offset)),
          reason: Buffer.from(reason),
        },
      },
    ],
  });

  logWarn('oversized batch routed to DLQ', {
    partition,
    offset: offset.toString(),
    dlq_topic: TELEMETRY_TOPICS.DLQ,
  });
}

/**
 * Send CPS setPayload extrinsic to blockchain.
 *
 * `payload` is submitted exactly as received from `telemetry.batched.v1` —
 * this service must not reserialize, recompress, or otherwise change it.
 */
async function sendSetPayloadExtrinsic(
  api: ApiPromise,
  keyring: Keyring,
  suri: string,
  nodeId: number,
  payload: Uint8Array
): Promise<string> {
  logDebug('preparing set_payload extrinsic', {
    nodeId,
    payload_size: payload.length,
  });

  const account = keyring.addFromUri(suri);
  logDebug('account loaded', {
    address: account.address,
  });

  // Call cps.setPayload(node_id, Some(payload))
  if (!api.tx.cps?.setPayload) {
    throw new Error('cps.setPayload extrinsic not found');
  }
  const extrinsic = api.tx.cps.setPayload(nodeId, Array.from(payload));

  logInfo('submitting set_payload extrinsic', {
    nodeId,
    payload_size: payload.length,
    from: account.address,
  });

  return new Promise((resolve, reject) => {
    let unsub: (() => void) | null = null;

    extrinsic
      .signAndSend(account, (result) => {
        logDebug('extrinsic status update', {
          status: result.status.type,
          isFinalized: result.isFinalized,
          isInBlock: result.isInBlock,
        });

        if (result.status.isInBlock) {
          logInfo('extrinsic in block', {
            blockHash: result.status.asInBlock.toString(),
            nodeId,
            payload_size: payload.length,
          });
        }

        if (result.status.isFinalized) {
          logInfo('extrinsic finalized', {
            blockHash: result.status.asFinalized.toString(),
            nodeId,
            payload_size: payload.length,
          });

          if (unsub) {
            unsub();
          }

          // Check for errors in events
          if (!api.events.system?.ExtrinsicFailed) {
            resolve(result.status.asFinalized.toString());
            return;
          }

          const hasError = result.events.some((record) => {
            const event = record.event;
            return api.events.system?.ExtrinsicFailed?.is(event) ?? false;
          });

          if (hasError) {
            const errorEvent = result.events.find((record) =>
              api.events.system?.ExtrinsicFailed?.is(record.event)
            );
            const errorData = errorEvent?.event.data;
            logError(
              'extrinsic failed in finalized block',
              new Error('ExtrinsicFailed'),
              {
                nodeId,
                payload_size: payload.length,
                errorData: errorData?.toString(),
              }
            );
            reject(
              new Error(
                `Extrinsic failed: ${errorData?.toString() ?? 'unknown error'}`
              )
            );
            return;
          }

          resolve(result.status.asFinalized.toString());
        } else if (result.isError) {
          if (unsub) {
            unsub();
          }
          const error = new Error('Transaction failed with status error');
          logError('extrinsic submission error', error, {
            nodeId,
            payload_size: payload.length,
            status: result.status.type,
          });
          reject(error);
        }
      })
      .then((unsubscribe) => {
        unsub = unsubscribe;
      })
      .catch((error) => {
        logError('failed to submit extrinsic', error, {
          nodeId,
          payload_size: payload.length,
        });
        reject(error);
      });
  });
}

export function createBlockchainAnchorService(
  config: BlockchainAnchorConfig = loadBlockchainAnchorConfig(),
  deps: BlockchainAnchorDeps = {}
): BlockchainAnchorService {
  const consumer =
    deps.createConsumer?.() ??
    new Consumer({
      groupId: config.consumerGroupId,
      clientId: 'blockchain-anchor',
      ...createKafkaClientOptions({
        brokers: config.kafkaBrokers,
        security: config.kafkaSecurity ?? loadKafkaSecurityConfig(),
      }),
    });

  const producer =
    deps.createProducer?.() ??
    new Producer({
      clientId: 'blockchain-anchor',
      ...createKafkaClientOptions({
        brokers: config.kafkaBrokers,
        security: config.kafkaSecurity ?? loadKafkaSecurityConfig(),
      }),
    });
  let admin: Admin | null = null;

  const createHealthServer =
    deps.createHealthServer ?? startHealthAndMetricsServer;

  let api: ApiPromise | null = null;
  let keyring: Keyring | null = null;
  let started = false;
  let runPromise: Promise<void> | null = null;
  let healthServer: Server | null = null;
  let shouldStop = false;

  const metrics: BlockchainAnchorMetrics = {
    consumed: 0,
    anchored: 0,
    skippedDuplicate: 0,
    failed: 0,
    rejectedOversized: 0,
  };

  const getMetrics = (): BlockchainAnchorMetrics => metrics;
  let statusCache: { at: number; value: BlockchainAnchorStatus } | undefined;
  let statusInFlight: Promise<BlockchainAnchorStatus> | undefined;
  type StatusRedisClient = Awaited<ReturnType<typeof createRedisClient>>;
  let statusRedisPromise: Promise<StatusRedisClient> | undefined;
  const pendingStatusWrites = new Set<Promise<void>>();
  const statusRedisKey =
    process.env.BLOCKCHAIN_ANCHOR_STATUS_REDIS_KEY ??
    'blockchain-anchor:v1:last-finalized';
  const getStatusRedis = async (): Promise<StatusRedisClient> => {
    statusRedisPromise ??= (
      deps.createStatusRedis?.() ?? createRedisClient(loadRedisConfig())
    ).catch((error: unknown) => {
      statusRedisPromise = undefined;
      throw error;
    });
    return statusRedisPromise;
  };
  const closeStatusRedis = async (): Promise<void> => {
    const client = await statusRedisPromise?.catch(() => undefined);
    statusRedisPromise = undefined;
    if (client) await closeRedisClient(client);
  };
  const recordFinalizedObservation = (
    blockHash: string,
    batchId: string
  ): void => {
    const currentApi = api;
    if (!currentApi) return;
    const observedAt = new Date().toISOString();
    const write = (async () => {
      const header = await timeout(
        currentApi.rpc.chain.getHeader(blockHash),
        3000
      );
      const client = await getStatusRedis();
      await timeout(
        client.eval(
          STORE_FINALIZED_OBSERVATION_SCRIPT,
          1,
          statusRedisKey,
          header.number.toString(),
          header.number.toString(),
          blockHash,
          batchId,
          observedAt
        ),
        2000
      );
      statusCache = undefined;
    })().catch((error: unknown) => {
      logWarn('failed to persist finalized batch observation', {
        reason: error instanceof Error ? error.message : 'unknown error',
      });
    });
    pendingStatusWrites.add(write);
    void write.finally(() => pendingStatusWrites.delete(write));
  };
  const getStatus = async (): Promise<BlockchainAnchorStatus> => {
    if (statusCache && Date.now() - statusCache.at < 15_000) {
      return statusCache.value;
    }
    if (statusInFlight) return statusInFlight;
    const currentApi = api;
    const currentKeyring = keyring;
    if (!currentApi || !currentKeyring) {
      return emptyStatus();
    }
    const statusRedis = await timeout(getStatusRedis(), 1500).catch(() => null);
    statusInFlight = observeBlockchain(
      currentApi,
      currentKeyring,
      config.suri,
      admin,
      statusRedis,
      config.consumerGroupId
    ).then((value) => {
      statusCache = { at: Date.now(), value };
      return value;
    });
    try {
      return await statusInFlight;
    } finally {
      statusInFlight = undefined;
    }
  };

  return {
    async start(): Promise<void> {
      if (started) {
        logInfo('start skipped; service already started');
        return;
      }

      started = true;
      shouldStop = false;
      logInfo('starting service', {
        consumerGroupId: config.consumerGroupId,
        kafkaBrokers: config.kafkaBrokers,
        substrateWsUrl: config.substrateWsUrl,
        nodeId: config.nodeId,
        healthPort: config.healthPort,
      });

      try {
        // Connect to blockchain
        if (deps.createApi) {
          api = await deps.createApi();
        } else {
          const provider = new WsProvider(config.substrateWsUrl);
          api = await ApiPromise.create({ provider: provider });
          await api.isReady;
        }
        logInfo('connected to substrate', {
          substrateWsUrl: config.substrateWsUrl,
        });

        // Initialize keyring
        keyring = new Keyring({ type: 'sr25519' });
        logInfo('keyring initialized');
        admin =
          deps.createAdmin?.() ??
          new Admin({
            clientId: 'blockchain-anchor-status',
            autocreateTopics: false,
            ...createKafkaClientOptions({
              brokers: config.kafkaBrokers,
              security: config.kafkaSecurity ?? loadKafkaSecurityConfig(),
            }),
          });

        // Start Kafka consumer
        const consumerStream = await consumer.consume({
          topics: [TELEMETRY_TOPICS.BATCHED],
          autocommit: false,
        });

        healthServer = createHealthServer(
          getMetrics,
          config.healthPort,
          getStatus
        );

        runPromise = (async () => {
          try {
            for await (const message of consumerStream) {
              if (shouldStop) {
                break;
              }

              if (!message.value) {
                logWarn('received null message value; skipping');
                continue;
              }

              let envelope: MessageShape<typeof EnvelopeSchema>;
              let payload: TelemetryBatchedPayload;
              try {
                envelope = fromBinary(
                  EnvelopeSchema,
                  new Uint8Array(message.value)
                );

                if (envelope.eventType !== TELEMETRY_TOPICS.BATCHED) {
                  logDebug('non-batched envelope ignored', {
                    eventType: envelope.eventType,
                  });
                  continue;
                }

                payload = fromBinary(
                  TelemetryBatchedPayloadSchema,
                  envelope.payload
                ) as TelemetryBatchedPayload;
              } catch (error) {
                logWarn('envelope parse error', {
                  reason:
                    error instanceof Error ? error.message : String(error),
                });
                // Commit offset for invalid messages to skip them
                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.BATCHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });
                continue;
              }

              metrics.consumed += 1;

              // Defensive size invariant: the batcher owns payload fitting,
              // but blockchain-anchor must not submit a payload that
              // violates the CPS size limit. This is a permanent upstream
              // contract error, not a transient failure — do not split the
              // batch here, and do not retry. The record is still routed to
              // the DLQ (instead of being silently dropped) so it can be
              // inspected/recovered, and the offset is only committed once
              // that publish is confirmed.
              if (payload.payload.length > config.maxPayloadBytes) {
                metrics.rejectedOversized += 1;
                logError(
                  'rejecting batch: payload exceeds ANCHOR_MAX_PAYLOAD_BYTES',
                  new Error('ANCHOR_PAYLOAD_TOO_LARGE'),
                  {
                    batch_id: payload.batchId,
                    payload_size: payload.payload.length,
                    max_payload_bytes: config.maxPayloadBytes,
                    node_id: config.nodeId,
                  }
                );

                await publishOversizedToDlq(
                  new Uint8Array(message.value),
                  message.partition,
                  message.offset,
                  'ANCHOR_PAYLOAD_TOO_LARGE',
                  producer,
                  config
                );

                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.BATCHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });
                continue;
              }

              logInfo('anchoring batch to blockchain', {
                event_id: envelope.eventId,
                batch_id: payload.batchId,
                payload_size: payload.payload.length,
                event_count: payload.eventCount,
                node_id: config.nodeId,
              });

              try {
                // Idempotency check: ask the chain what is currently
                // anchored for this node before submitting a new
                // extrinsic. This covers the crash window between
                // extrinsic finalization and Kafka offset commit - on
                // redelivery the on-chain state already reflects the
                // anchor (same content hash), so we skip re-submission
                // instead of creating a duplicate logical anchor operation.
                const anchoredPayload = await getAnchoredPayload(
                  api!,
                  config.nodeId
                );

                if (
                  anchoredPayload !== null &&
                  anchoredPayloadMatches(anchoredPayload, payload.payloadHash)
                ) {
                  metrics.skippedDuplicate += 1;

                  logInfo(
                    'batch already anchored on-chain; skipping duplicate submission',
                    {
                      batch_id: payload.batchId,
                      payload_size: payload.payload.length,
                      event_id: envelope.eventId,
                      node_id: config.nodeId,
                    }
                  );
                } else {
                  const finalizedBlockHash = await sendSetPayloadExtrinsic(
                    api!,
                    keyring!,
                    config.suri,
                    config.nodeId,
                    payload.payload
                  );

                  metrics.anchored += 1;
                  recordFinalizedObservation(
                    finalizedBlockHash,
                    payload.batchId
                  );

                  logInfo('batch anchored successfully', {
                    batch_id: payload.batchId,
                    payload_size: payload.payload.length,
                    event_count: payload.eventCount,
                    node_id: config.nodeId,
                  });
                }

                // Commit offset only after successful anchoring (or a
                // confirmed no-op due to the idempotency check above)
                await consumer.commit({
                  offsets: [
                    {
                      topic: TELEMETRY_TOPICS.BATCHED,
                      partition: message.partition,
                      offset: message.offset + 1n,
                      leaderEpoch: -1,
                    },
                  ],
                });

                logDebug('kafka offset committed', {
                  partition: message.partition,
                  offset: (message.offset + 1n).toString(),
                });
              } catch (error) {
                metrics.failed += 1;
                logError('failed to anchor batch', error, {
                  batch_id: payload.batchId,
                  payload_size: payload.payload.length,
                  node_id: config.nodeId,
                });
                // Don't commit offset on failure. Stop consuming further
                // messages instead of continuing the loop: a later
                // message's commit would advance the committed offset past
                // this unretried one, silently losing it. Rethrowing exits
                // the loop (and the run promise) so the process can be
                // restarted and resume from the last committed offset.
                throw error;
              }
            }
          } catch (error) {
            logError('consumer stream error', error);
            throw error;
          }
        })();

        logInfo('service started');
      } catch (error) {
        logError('service failed to start', error);
        started = false;
        runPromise = null;

        if (healthServer) {
          await new Promise<void>((resolve) => {
            healthServer?.close(() => {
              resolve();
            });
          });
          healthServer = null;
        }

        await consumer.close().catch(() => undefined);
        await admin?.close().catch(() => undefined);
        admin = null;
        await closeStatusRedis().catch(() => undefined);
        await api?.disconnect();
        api = null;
        keyring = null;
        logInfo('startup rollback complete');
        throw error;
      }
    },
    async stop(): Promise<void> {
      if (!started) {
        logInfo('stop skipped; service not started');
        return;
      }

      started = false;
      shouldStop = true;
      logInfo('stopping service');

      // Stop pulling new messages, but do not disconnect the blockchain API
      // yet: the run loop only checks `shouldStop` before fetching the next
      // message, so a message whose extrinsic is currently in flight will
      // run to completion. Disconnecting the API here would race that
      // in-flight `signAndSend` and could drop/corrupt the submission.
      await consumer.close();

      logInfo(
        'waiting for in-flight processing to complete before disconnecting chain API'
      );
      await runPromise?.catch(() => undefined);
      runPromise = null;
      await Promise.allSettled([...pendingStatusWrites]);

      await api?.disconnect();
      api = null;
      keyring = null;

      await producer.close().catch(() => undefined);
      await admin?.close().catch(() => undefined);
      admin = null;
      await closeStatusRedis().catch((error: unknown) => {
        logWarn('error closing status Redis client', {
          reason: error instanceof Error ? error.message : 'unknown error',
        });
      });

      if (healthServer) {
        await new Promise<void>((resolve, reject) => {
          healthServer?.close((error) => {
            if (error) {
              reject(error);
              return;
            }
            resolve();
          });
        });
        healthServer = null;
      }

      logInfo('service stopped');
    },
    getMetrics(): Readonly<BlockchainAnchorMetrics> {
      return metrics;
    },
  };
}

function startHealthAndMetricsServer(
  getMetrics: () => BlockchainAnchorMetrics,
  port: number,
  getStatus: () => Promise<BlockchainAnchorStatus> = async () => emptyStatus()
): Server {
  const server = createServer((request, response) => {
    // The server starts listening only after the Kafka consumer is running,
    // so a successful response also proves the service is ready.
    if (request.url === '/health' || request.url === '/ready') {
      logDebug('health check requested');
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      // Allow the endpoint service's status page to poll this endpoint
      // directly from the browser.
      response.setHeader('access-control-allow-origin', '*');
      response.end(JSON.stringify({ status: 'ok' }));
      return;
    }

    if (request.url === '/metrics') {
      logDebug('metrics endpoint requested');
      const metrics = getMetrics();
      logInfo('metrics served', {
        consumed: metrics.consumed,
        anchored: metrics.anchored,
        failed: metrics.failed,
      });
      response.statusCode = 200;
      response.setHeader('content-type', 'application/json; charset=utf-8');
      // Allow the endpoint service's status page to poll this endpoint
      // directly from the browser.
      response.setHeader('access-control-allow-origin', '*');
      response.end(JSON.stringify(metrics));
      return;
    }

    if (request.url === '/status') {
      void getStatus()
        .then((status) => {
          response.statusCode = 200;
          response.setHeader('content-type', 'application/json; charset=utf-8');
          response.end(JSON.stringify(status));
        })
        .catch((error: unknown) => {
          logWarn('blockchain status observation failed', {
            reason: error instanceof Error ? error.message : 'unknown error',
          });
          response.statusCode = 200;
          response.setHeader('content-type', 'application/json; charset=utf-8');
          response.end(JSON.stringify(emptyStatus()));
        });
      return;
    }

    response.statusCode = 404;
    response.end('not found');
  });
  server.listen({ host: '0.0.0.0', port });
  logInfo('HTTP server listening', { port, host: '0.0.0.0' });
  return server;
}

export async function startBlockchainAnchor(): Promise<BlockchainAnchorService> {
  const service = createBlockchainAnchorService();
  await service.start();
  logInfo('service started (direct run)');
  return service;
}

const isDirectRun = process.argv[1] === fileURLToPath(import.meta.url);
if (isDirectRun) {
  startBlockchainAnchor()
    .then((service) => {
      installShutdownHandler(() => service.stop(), {
        onSignal: (signal) => logInfo('received shutdown signal', { signal }),
        onShutdownError: (error) =>
          logError('error during graceful shutdown', error),
      });
    })
    .catch((error: unknown) => {
      logError('failed to start (direct run)', error);
      process.exitCode = 1;
    });
}
