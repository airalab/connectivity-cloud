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

export interface ConnectivityMonitorMetrics {
  pauseCount: number;
  resumeCount: number;
}

export interface ConnectivityMonitorOptions {
  /** Minimum number of connected peers required to consider connectivity sufficient. */
  minConnectedPeers: number;
  /** How long connectivity must remain on one side of the threshold before acting on it. */
  stabilizationIntervalMs: number;
  /** Poll the current connected peer count. */
  getConnectedPeerCount: () => number;
  /** Called when consumption should pause (debounced). */
  onPause: () => void;
  /** Called when consumption should resume (debounced). */
  onResume: () => void;
  setTimeout?: typeof setTimeout;
  clearTimeout?: typeof clearTimeout;
}

export interface ConnectivityMonitor {
  /** Re-evaluate connectivity now (e.g. after a peer connect/disconnect event). */
  notify(): void;
  isPaused(): boolean;
  getMetrics(): Readonly<ConnectivityMonitorMetrics>;
  stop(): void;
}

/**
 * Debounces peer connectivity changes so that Kafka consumption is only
 * paused/resumed once connectivity has stabilized above or below the
 * configured threshold, avoiding rapid pause/resume flapping.
 */
export function createConnectivityMonitor(
  options: ConnectivityMonitorOptions
): ConnectivityMonitor {
  const scheduleTimeout = options.setTimeout ?? setTimeout;
  const cancelTimeout = options.clearTimeout ?? clearTimeout;

  const metrics: ConnectivityMonitorMetrics = {
    pauseCount: 0,
    resumeCount: 0,
  };

  // Start paused only if a minimum is configured; the caller applies the
  // initial state explicitly once peer count is known.
  let paused = options.minConnectedPeers > 0;
  let pendingDesiredPaused: boolean | null = null;
  let timer: ReturnType<typeof setTimeout> | null = null;

  function clearPending(): void {
    if (timer) {
      cancelTimeout(timer);
      timer = null;
    }
    pendingDesiredPaused = null;
  }

  function applyState(desiredPaused: boolean): void {
    if (desiredPaused === paused) {
      return;
    }
    paused = desiredPaused;
    if (paused) {
      metrics.pauseCount += 1;
      options.onPause();
    } else {
      metrics.resumeCount += 1;
      options.onResume();
    }
  }

  function notify(): void {
    const count = options.getConnectedPeerCount();
    const desiredPaused = count < options.minConnectedPeers;

    if (desiredPaused === paused) {
      // Connectivity is back in line with the currently applied state
      // before the debounce timer fired; cancel any pending flip.
      clearPending();
      return;
    }

    if (pendingDesiredPaused === desiredPaused) {
      // Already waiting to apply this exact change.
      return;
    }

    clearPending();
    pendingDesiredPaused = desiredPaused;
    timer = scheduleTimeout(() => {
      timer = null;
      const finalCount = options.getConnectedPeerCount();
      const stillDesired = finalCount < options.minConnectedPeers;
      pendingDesiredPaused = null;
      if (stillDesired === desiredPaused) {
        applyState(desiredPaused);
      }
    }, options.stabilizationIntervalMs);
    timer.unref?.();
  }

  return {
    notify,
    isPaused(): boolean {
      return paused;
    },
    getMetrics(): Readonly<ConnectivityMonitorMetrics> {
      return metrics;
    },
    stop(): void {
      clearPending();
    },
  };
}
