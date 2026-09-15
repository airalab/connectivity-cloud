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
export interface BatcherConfig {
  kafkaBrokers: string[];
  consumerGroupId: string;
  source: string;
  healthPort: number;
  batchSize: number;
  batchTimeoutMs: number;
}

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

export function loadBatcherConfig(
  env: NodeJS.ProcessEnv = process.env
): BatcherConfig {
  return {
    kafkaBrokers: (env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter((broker) => broker.length > 0),
    consumerGroupId: env.BATCHER_GROUP_ID ?? 'batcher-v1',
    source: env.BATCHER_SOURCE ?? 'batcher',
    healthPort: parsePositiveInt(env.BATCHER_HEALTH_PORT, 3041),
    batchSize: parsePositiveInt(env.BATCHER_BATCH_SIZE, 10),
    batchTimeoutMs: parsePositiveInt(env.BATCHER_BATCH_TIMEOUT_MS, 30000),
  };
}
