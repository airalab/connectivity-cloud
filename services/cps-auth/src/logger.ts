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
import pino from 'pino';

const logger = pino({
  name: 'cps-auth',
  level: process.env.CPS_AUTH_LOG_LEVEL ?? process.env.LOG_LEVEL ?? 'info',
});

export function logInfo(
  message: string,
  context?: Record<string, unknown>
): void {
  logger.info(context ?? {}, message);
}

export function logDebug(
  message: string,
  context?: Record<string, unknown>
): void {
  logger.debug(context ?? {}, message);
}

export function logWarn(
  message: string,
  context?: Record<string, unknown>
): void {
  logger.warn(context ?? {}, message);
}

export function logError(
  message: string,
  error: unknown,
  context?: Record<string, unknown>
): void {
  const normalizedError =
    error instanceof Error
      ? { name: error.name, message: error.message, stack: error.stack }
      : { message: String(error) };
  logger.error(
    {
      ...(context ?? {}),
      error: normalizedError,
    },
    message
  );
}
