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

/** Environment variable that explicitly opts in to plaintext/unauthenticated local development. */
export const ALLOW_INSECURE_LOCAL_ENV = 'SCP_ALLOW_INSECURE_LOCAL';

/**
 * Whether insecure (plaintext, unauthenticated) Redis/Kafka connections are
 * explicitly allowed. Defaults to false so production never silently falls
 * back to an insecure connection.
 */
export function isInsecureLocalAllowed(
  env: NodeJS.ProcessEnv = process.env
): boolean {
  return env[ALLOW_INSECURE_LOCAL_ENV]?.trim().toLowerCase() === 'true';
}

/** Parses a positive integer environment value, falling back when unset or invalid. */
export function parsePositiveInt(
  value: string | undefined,
  fallback: number
): number {
  const parsed = Number.parseInt(value ?? '', 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

/** Returns a trimmed environment value, or undefined when unset or blank. */
export function optionalEnv(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed ? trimmed : undefined;
}
