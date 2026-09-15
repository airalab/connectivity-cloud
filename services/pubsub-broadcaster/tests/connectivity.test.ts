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
import { describe, expect, it, vi } from 'vitest';
import { createConnectivityMonitor } from '../src/connectivity.js';

describe('connectivity monitor', () => {
  it('pauses once connectivity stabilizes below the minimum', () => {
    vi.useFakeTimers();
    let count = 2;
    const onPause = vi.fn();
    const onResume = vi.fn();

    const monitor = createConnectivityMonitor({
      minConnectedPeers: 1,
      stabilizationIntervalMs: 5000,
      getConnectedPeerCount: () => count,
      onPause,
      onResume,
    });

    // Establish a resumed baseline (sufficient peers already connected).
    monitor.notify();
    vi.advanceTimersByTime(5000);
    expect(monitor.isPaused()).toBe(false);

    count = 0;
    monitor.notify();
    expect(onPause).not.toHaveBeenCalled();

    vi.advanceTimersByTime(5000);
    expect(onPause).toHaveBeenCalledTimes(1);
    expect(monitor.isPaused()).toBe(true);
    expect(monitor.getMetrics().pauseCount).toBe(1);

    vi.useRealTimers();
  });

  it('resumes once connectivity stabilizes at or above the minimum', () => {
    vi.useFakeTimers();
    let count = 0;
    const onPause = vi.fn();
    const onResume = vi.fn();

    const monitor = createConnectivityMonitor({
      minConnectedPeers: 1,
      stabilizationIntervalMs: 5000,
      getConnectedPeerCount: () => count,
      onPause,
      onResume,
    });

    // Initial state assumes paused since minConnectedPeers > 0.
    expect(monitor.isPaused()).toBe(true);

    count = 1;
    monitor.notify();
    vi.advanceTimersByTime(5000);

    expect(onResume).toHaveBeenCalledTimes(1);
    expect(monitor.isPaused()).toBe(false);
    expect(monitor.getMetrics().resumeCount).toBe(1);

    vi.useRealTimers();
  });

  it('does not flap when connectivity briefly recovers before the stabilization interval elapses', () => {
    vi.useFakeTimers();
    let count = 2;
    const onPause = vi.fn();
    const onResume = vi.fn();

    const monitor = createConnectivityMonitor({
      minConnectedPeers: 1,
      stabilizationIntervalMs: 5000,
      getConnectedPeerCount: () => count,
      onPause,
      onResume,
    });

    // Drop below minimum, then recover before the timer fires.
    count = 0;
    monitor.notify();
    vi.advanceTimersByTime(2000);
    count = 2;
    monitor.notify();
    vi.advanceTimersByTime(5000);

    expect(onPause).not.toHaveBeenCalled();
    expect(monitor.isPaused()).toBe(false);
    expect(monitor.getMetrics().pauseCount).toBe(0);

    vi.useRealTimers();
  });

  it('does not re-trigger pause while already pending the same transition', () => {
    vi.useFakeTimers();
    let count = 2;
    const onPause = vi.fn();
    const onResume = vi.fn();

    const monitor = createConnectivityMonitor({
      minConnectedPeers: 1,
      stabilizationIntervalMs: 5000,
      getConnectedPeerCount: () => count,
      onPause,
      onResume,
    });

    // Establish a resumed baseline (sufficient peers already connected).
    monitor.notify();
    vi.advanceTimersByTime(5000);
    expect(monitor.isPaused()).toBe(false);

    count = 0;
    monitor.notify();
    monitor.notify();
    monitor.notify();
    vi.advanceTimersByTime(5000);

    expect(onPause).toHaveBeenCalledTimes(1);

    vi.useRealTimers();
  });

  it('starts unpaused when no minimum is configured', () => {
    const monitor = createConnectivityMonitor({
      minConnectedPeers: 0,
      stabilizationIntervalMs: 5000,
      getConnectedPeerCount: () => 0,
      onPause: vi.fn(),
      onResume: vi.fn(),
    });

    expect(monitor.isPaused()).toBe(false);
  });
});
