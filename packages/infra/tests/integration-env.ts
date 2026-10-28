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
import { fileURLToPath } from 'node:url';

/** Integration tests need `docker compose --profile secure up -d` and `docker/generate-test-certs.sh`. */
export const integrationEnabled = process.env.SCP_INTEGRATION === 'true';

export function certPath(name: string): string {
  return fileURLToPath(
    new URL(`../../../docker/certs/${name}`, import.meta.url)
  );
}

export const REDIS_PLAIN_URL =
  process.env.SCP_TEST_REDIS_URL ?? 'redis://localhost:6379';
export const REDIS_TLS_URL =
  process.env.SCP_TEST_REDIS_TLS_URL ?? 'rediss://localhost:6380';
export const REDIS_PASSWORD =
  process.env.SCP_TEST_REDIS_PASSWORD ?? 'test-redis-password';
export const KAFKA_SECURE_BROKER =
  process.env.SCP_TEST_KAFKA_SECURE_BROKER ?? 'localhost:9094';
export const KAFKA_USER = process.env.SCP_TEST_KAFKA_USER ?? 'scp';
export const KAFKA_PASSWORD =
  process.env.SCP_TEST_KAFKA_PASSWORD ?? 'scp-secret';
