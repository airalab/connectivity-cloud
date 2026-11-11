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
import { createGoogleAdcIdTokenProvider } from '@scp/infra';

export type ServiceState = 'operational' | 'degraded' | 'outage' | 'unknown';

export interface StatusTarget {
  name: string;
  url?: string;
}

export interface StatusCollectorOptions {
  targets: StatusTarget[];
  checkEndpointReadiness: () => Promise<void>;
  cacheTtlMs?: number;
  probeTimeoutMs?: number;
  failureThreshold?: number;
  now?: () => number;
  fetch?: typeof globalThis.fetch;
  getIdToken?: (audience: string) => Promise<string>;
}

interface ProbeResult {
  state: ServiceState;
  observedAt: string | null;
  detail?: Record<string, unknown>;
}

interface Cached<T> {
  value: T;
  expiresAt: number;
}

const SERVICE_LABELS: Record<string, string> = {
  endpoint: 'Telemetry Ingress API',
  'pubsub-broadcaster': 'Live Telemetry Broadcast',
  'heartbeat-tracker': 'Sensor Heartbeat Tracker',
  batcher: 'Telemetry Batcher',
  'blockchain-anchor': 'Robonomics Blockchain Anchor',
};

const DEFAULT_PORTS: Record<string, number> = {
  'pubsub-broadcaster': 3020,
  'heartbeat-tracker': 3030,
  batcher: 3041,
  'blockchain-anchor': 3050,
};

function safeTargetUrl(value: string | undefined): URL | undefined {
  if (!value) return undefined;
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    return undefined;
  }
  const isLocal =
    (url.hostname === 'localhost' || url.hostname === '127.0.0.1') &&
    url.protocol === 'http:' &&
    process.env.NODE_ENV !== 'production';
  if (
    (!isLocal && url.protocol !== 'https:') ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.pathname !== '/' && url.pathname !== '')
  ) {
    return undefined;
  }
  return url;
}

function localDefaultUrl(name: string): string | undefined {
  const port = DEFAULT_PORTS[name];
  return port ? `http://127.0.0.1:${port}` : undefined;
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object'
    ? (value as Record<string, unknown>)
    : undefined;
}

function finiteNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

export function aggregateServiceStates(states: ServiceState[]): ServiceState {
  if (states.includes('outage')) return 'outage';
  if (states.includes('degraded')) return 'degraded';
  if (states.includes('unknown')) return 'unknown';
  return 'operational';
}

export function createStatusCollector(options: StatusCollectorOptions) {
  const now = options.now ?? Date.now;
  const fetchImpl = options.fetch ?? globalThis.fetch;
  const positiveEnv = (name: string, fallback: number): number => {
    const parsed = Number.parseInt(process.env[name] ?? '', 10);
    return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : fallback;
  };
  const cacheTtlMs =
    options.cacheTtlMs ?? positiveEnv('STATUS_CACHE_TTL_MS', 30_000);
  const probeTimeoutMs =
    options.probeTimeoutMs ?? positiveEnv('STATUS_PROBE_TIMEOUT_MS', 5_000);
  const failureThreshold =
    options.failureThreshold ?? positiveEnv('STATUS_FAILURE_THRESHOLD', 3);
  const configuredTargets = new Map(
    options.targets.map((target) => [target.name, target.url])
  );
  const targetUrlFor = (name: string): URL | undefined => {
    const configured = configuredTargets.get(name);
    const value =
      configured ??
      (process.env.NODE_ENV === 'production'
        ? undefined
        : localDefaultUrl(name));
    return safeTargetUrl(value);
  };
  const failures = new Map<string, number>();
  const caches = new Map<string, Cached<unknown>>();
  const inFlight = new Map<string, Promise<unknown>>();
  const getIdToken = options.getIdToken ?? createGoogleAdcIdTokenProvider();

  async function cached<T>(key: string, load: () => Promise<T>): Promise<T> {
    const entry = caches.get(key);
    if (entry && entry.expiresAt > now()) return entry.value as T;
    const pending = inFlight.get(key);
    if (pending) return pending as Promise<T>;
    const request = load().then((value) => {
      caches.set(key, { value, expiresAt: now() + cacheTtlMs });
      if (caches.size > 100) {
        const oldest = [...caches.entries()].sort(
          (a, b) => a[1].expiresAt - b[1].expiresAt
        )[0]?.[0];
        if (oldest) caches.delete(oldest);
      }
      return value;
    });
    inFlight.set(key, request);
    try {
      return await request;
    } finally {
      inFlight.delete(key);
    }
  }

  async function requestJson(
    targetUrl: URL,
    path: string
  ): Promise<{ status: number; body: Record<string, unknown> }> {
    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    const operation = async () => {
      const local = targetUrl.protocol === 'http:';
      const headers: Record<string, string> = { accept: 'application/json' };
      if (!local) {
        headers.authorization = await getIdToken(targetUrl.origin);
      } else if (process.env.NODE_ENV === 'production') {
        throw new Error('unauthenticated local target is not allowed');
      }
      const response = await fetchImpl(new URL(path, targetUrl), {
        headers,
        signal: controller.signal,
        redirect: 'manual',
      });
      if (!response.ok) return { status: response.status, body: {} };
      const body = asObject(await response.json()) ?? {};
      return { status: response.status, body };
    };
    const deadline = new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => {
        controller.abort();
        reject(new Error('status request timed out'));
      }, probeTimeoutMs);
    });
    try {
      return await Promise.race([operation(), deadline]);
    } finally {
      clearTimeout(timer!);
    }
  }

  async function probe(name: string): Promise<ProbeResult> {
    if (name === 'endpoint') {
      try {
        await options.checkEndpointReadiness();
        return {
          state: 'operational',
          observedAt: new Date(now()).toISOString(),
        };
      } catch {
        return { state: 'degraded', observedAt: new Date(now()).toISOString() };
      }
    }

    const targetUrl = targetUrlFor(name);
    if (!targetUrl) return { state: 'unknown', observedAt: null };
    try {
      const result = await requestJson(targetUrl, '/ready');
      failures.set(name, 0);
      if (result.status === 401 || result.status === 403) {
        return { state: 'unknown', observedAt: new Date(now()).toISOString() };
      }
      if (
        result.status === 200 &&
        result.body.status !== 'ok' &&
        result.body.status !== 'ready'
      ) {
        return { state: 'unknown', observedAt: new Date(now()).toISOString() };
      }
      return {
        state: result.status === 200 ? 'operational' : 'degraded',
        observedAt: new Date(now()).toISOString(),
      };
    } catch {
      const count = (failures.get(name) ?? 0) + 1;
      failures.set(name, count);
      return {
        state: count >= failureThreshold ? 'outage' : 'unknown',
        observedAt: new Date(now()).toISOString(),
      };
    }
  }

  async function collectStatus() {
    return cached('status', async () => {
      const names = ['endpoint', ...configuredTargets.keys()];
      const settled = await Promise.allSettled(
        names.map((name) => probe(name))
      );
      const services = settled.map((result, index) => {
        const name = names[index]!;
        const outcome: ProbeResult =
          result.status === 'fulfilled'
            ? result.value
            : { state: 'unknown', observedAt: null };
        return {
          name,
          label: SERVICE_LABELS[name] ?? name,
          ...outcome,
        };
      });
      return {
        status: aggregateServiceStates(
          services.map((service) => service.state)
        ),
        observedAt: new Date(now()).toISOString(),
        services,
      };
    });
  }

  async function fetchTargetJson(
    name: string,
    path: string,
    cacheKey: string
  ): Promise<Record<string, unknown> | null> {
    return cached(cacheKey, async () => {
      const targetUrl = targetUrlFor(name);
      if (!targetUrl) return null;
      try {
        const result = await requestJson(targetUrl, path);
        if (result.status !== 200) return null;
        return result.body;
      } catch {
        return null;
      }
    });
  }

  async function collectSensors(
    params: { limit?: number; cursor?: string; filter?: string } = {}
  ) {
    const limit =
      typeof params.limit === 'number' &&
      Number.isSafeInteger(params.limit) &&
      params.limit > 0
        ? Math.min(params.limit, 100)
        : 50;
    const cursor =
      params.cursor && /^\d{1,15}$/.test(params.cursor) ? params.cursor : '0';
    const filter =
      params.filter === 'online' || params.filter === 'offline'
        ? params.filter
        : 'all';
    const query = new URLSearchParams({ limit: String(limit), cursor, filter });
    const body = await fetchTargetJson(
      'heartbeat-tracker',
      `/status/sensors?${query}`,
      `sensors:${limit}:${cursor}:${filter}`
    );
    if (!body) {
      return {
        observedAt: new Date(now()).toISOString(),
        available: false,
        summary: null,
        sensors: [],
        truncated: false,
      };
    }
    const source = Array.isArray(body.sensors)
      ? body.sensors
      : Array.isArray(body.sensors_uptime)
        ? body.sensors_uptime
        : [];
    const sensors = source
      .map((item) => asObject(item))
      .filter((item): item is Record<string, unknown> => !!item)
      .map((item) => ({
        sensorId: typeof item.sensor_id === 'string' ? item.sensor_id : null,
        online: typeof item.online === 'boolean' ? item.online : null,
        firstSeen: typeof item.first_seen === 'string' ? item.first_seen : null,
        lastSeen: typeof item.last_seen === 'string' ? item.last_seen : null,
        uptimeSeconds: finiteNumber(item.uptime_seconds),
      }))
      .sort((a, b) => {
        if (a.online !== b.online) return a.online ? -1 : 1;
        return (
          (Date.parse(b.lastSeen ?? '') || 0) -
          (Date.parse(a.lastSeen ?? '') || 0)
        );
      });
    const summary = asObject(body.summary) ?? body;
    const total = finiteNumber(body.total);
    const nextCursor =
      typeof body.nextCursor === 'string' && /^\d{1,15}$/.test(body.nextCursor)
        ? body.nextCursor
        : null;
    return {
      observedAt:
        typeof body.observedAt === 'string'
          ? body.observedAt
          : new Date(now()).toISOString(),
      available: true,
      summary: {
        sensorsOnline: finiteNumber(summary.sensors_online),
        sensorsTotalTracked: finiteNumber(summary.sensors_total_tracked),
        avgUptimeSeconds: finiteNumber(summary.avg_uptime_seconds),
        maxUptimeSeconds: finiteNumber(summary.max_uptime_seconds),
        onlineWindowMs: finiteNumber(summary.online_window_ms),
      },
      sensors: sensors.slice(0, limit),
      total: total ?? sensors.length,
      totalReturned: Math.min(sensors.length, limit),
      limit,
      filter,
      nextCursor,
      truncated:
        nextCursor !== null || (total !== null && sensors.length < total),
    };
  }

  async function collectNetwork() {
    const [pubsub, anchor] = await Promise.all([
      fetchTargetJson('pubsub-broadcaster', '/metrics', 'pubsub-metrics'),
      fetchTargetJson('blockchain-anchor', '/status', 'anchor-status'),
    ]);
    return {
      observedAt: new Date(now()).toISOString(),
      pubsub: pubsub
        ? {
            available: true,
            connectedPeerCount: finiteNumber(pubsub.connectedPeerCount),
            minConnectedPeers: finiteNumber(pubsub.minConnectedPeers),
            kafkaPaused:
              typeof pubsub.kafkaPaused === 'boolean'
                ? pubsub.kafkaPaused
                : null,
            publishSuccess: finiteNumber(pubsub.publishSuccess),
            publishFailure: finiteNumber(pubsub.publishFailure),
            pauseCount: finiteNumber(pubsub.pauseCount),
            resumeCount: finiteNumber(pubsub.resumeCount),
            scope: 'process-local',
          }
        : { available: false },
      blockchain: anchor
        ? {
            available: true,
            queuedBatches: finiteNumber(anchor.queuedBatches),
            lastFinalizedAt:
              typeof anchor.lastFinalizedAt === 'string'
                ? anchor.lastFinalizedAt
                : null,
            lastFinalizedObservationStale:
              typeof anchor.lastFinalizedObservationStale === 'boolean'
                ? anchor.lastFinalizedObservationStale
                : null,
            lastFinalizedBlock:
              typeof anchor.lastFinalizedBlock === 'string'
                ? anchor.lastFinalizedBlock
                : null,
            lastBatchId:
              typeof anchor.lastBatchId === 'string'
                ? anchor.lastBatchId
                : null,
            freeBalance:
              typeof anchor.freeBalance === 'string'
                ? anchor.freeBalance
                : null,
            tokenSymbol:
              typeof anchor.tokenSymbol === 'string'
                ? anchor.tokenSymbol
                : null,
            tokenDecimals: finiteNumber(anchor.tokenDecimals),
            bestBlock:
              typeof anchor.bestBlock === 'string' ? anchor.bestBlock : null,
            finalizedBlock:
              typeof anchor.finalizedBlock === 'string'
                ? anchor.finalizedBlock
                : null,
            finalizationGap: finiteNumber(anchor.finalizationGap),
          }
        : { available: false },
    };
  }

  return { collectStatus, collectSensors, collectNetwork };
}
