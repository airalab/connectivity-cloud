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
import { EventEmitter } from 'node:events';
import { describe, expect, it, vi } from 'vitest';
import { installShutdownHandler } from '../src/shutdown.js';

function createFakeProcess(): NodeJS.Process {
  const emitter = new EventEmitter();
  return emitter as unknown as NodeJS.Process;
}

describe('installShutdownHandler', () => {
  it('invokes stop() and exits 0 on SIGTERM', async () => {
    const proc = createFakeProcess();
    const stop = vi.fn().mockResolvedValue(undefined);
    const exit = vi.fn();
    const onSignal = vi.fn();

    installShutdownHandler(stop, { process: proc, exit, onSignal });

    proc.emit('SIGTERM');
    // Allow the stop() promise chain to resolve.
    await Promise.resolve();
    await Promise.resolve();

    expect(onSignal).toHaveBeenCalledWith('SIGTERM');
    expect(stop).toHaveBeenCalledTimes(1);
    expect(exit).toHaveBeenCalledWith(0);
  });

  it('invokes stop() and exits 0 on SIGINT', async () => {
    const proc = createFakeProcess();
    const stop = vi.fn().mockResolvedValue(undefined);
    const exit = vi.fn();

    installShutdownHandler(stop, { process: proc, exit });

    proc.emit('SIGINT');
    await Promise.resolve();
    await Promise.resolve();

    expect(stop).toHaveBeenCalledTimes(1);
    expect(exit).toHaveBeenCalledWith(0);
  });

  it('only runs the shutdown sequence once for repeated signals', async () => {
    const proc = createFakeProcess();
    const stop = vi.fn().mockResolvedValue(undefined);
    const exit = vi.fn();

    installShutdownHandler(stop, { process: proc, exit });

    proc.emit('SIGTERM');
    proc.emit('SIGTERM');
    proc.emit('SIGINT');
    await Promise.resolve();
    await Promise.resolve();

    expect(stop).toHaveBeenCalledTimes(1);
    expect(exit).toHaveBeenCalledTimes(1);
  });

  it('exits with code 1 and reports the error if stop() rejects', async () => {
    const proc = createFakeProcess();
    const error = new Error('flush failed');
    const stop = vi.fn().mockRejectedValue(error);
    const exit = vi.fn();
    const onShutdownError = vi.fn();

    installShutdownHandler(stop, { process: proc, exit, onShutdownError });

    proc.emit('SIGTERM');
    await Promise.resolve();
    await Promise.resolve();
    await Promise.resolve();

    expect(onShutdownError).toHaveBeenCalledWith(error);
    expect(exit).toHaveBeenCalledWith(1);
  });
});
