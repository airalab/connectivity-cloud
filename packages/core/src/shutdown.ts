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

export interface ShutdownHooks {
  /** Called once, synchronously, as soon as a shutdown signal is received. */
  onSignal?(signal: NodeJS.Signals): void;
  /** Called if the provided `stop` function rejects during shutdown. */
  onShutdownError?(error: unknown): void;
  /** Overrideable for testing; defaults to `process.exit`. */
  exit?(code: number): void;
  /** Overrideable for testing; defaults to `process`. */
  process?: NodeJS.Process;
}

/**
 * Wires `SIGTERM`/`SIGINT` to a graceful shutdown flow, as required for
 * long-running services on Cloud Run (which sends `SIGTERM` before
 * terminating an instance).
 *
 * The provided `stop` function is expected to encapsulate the full graceful
 * shutdown sequence for the service: stop consuming new work, let in-flight
 * processing complete/flush, commit offsets only after durable side effects,
 * and only then disconnect external clients. This helper only guarantees
 * that `stop` is invoked exactly once (repeated signals are ignored while a
 * shutdown is already in progress) and that the process exits with a
 * non-zero code if shutdown itself fails.
 */
export function installShutdownHandler(
  stop: () => Promise<void>,
  hooks: ShutdownHooks = {}
): void {
  const proc = hooks.process ?? process;
  const exit = hooks.exit ?? ((code: number) => proc.exit(code));

  let shuttingDown = false;
  const shutdown = (signal: NodeJS.Signals): void => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    hooks.onSignal?.(signal);
    stop()
      .then(() => exit(0))
      .catch((error: unknown) => {
        hooks.onShutdownError?.(error);
        exit(1);
      });
  };

  proc.once('SIGTERM', () => shutdown('SIGTERM'));
  proc.once('SIGINT', () => shutdown('SIGINT'));
}
