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
  /**
   * Hostnames (exact, or `*.suffix`) that may receive Google-signed ID tokens.
   * Defaults to `STATUS_ALLOWED_UPSTREAM_HOSTS`; when empty, only Cloud Run
   * `*.run.app` hostnames are accepted.
   */
  allowedHosts?: string[];
  /** Health ports of local (non-production) fallback targets, by service name. */
  localPorts?: Record<string, number>;
  /** Maximum age of a retained last-good observation served as stale. */
  staleMaxAgeMs?: number;
}

interface Snapshot<T> {
  value: T;
  /** When the upstream was actually read. */
  fetchedAt: number;
  /** True when a failed refresh is being covered by an older observation. */
  stale: boolean;
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

const CLOUD_RUN_HOSTNAME =
  /^[a-z0-9]([a-z0-9-]*[a-z0-9])?(\.[a-z0-9]([a-z0-9-]*[a-z0-9])?)*\.run\.app$/;

function hostnameAllowed(hostname: string, allowedHosts: string[]): boolean {
  if (allowedHosts.length === 0) return CLOUD_RUN_HOSTNAME.test(hostname);
  return allowedHosts.some((pattern) =>
    pattern.startsWith('*.')
      ? hostname.endsWith(pattern.slice(1)) &&
        hostname.length > pattern.length - 1
      : hostname === pattern
  );
}

function parseHostList(value: string | undefined): string[] {
  return (value ?? '')
    .split(',')
    .map((host) => host.trim().toLowerCase())
    .filter(Boolean);
}

/**
 * Accepts only origins that may receive a Google-signed ID token: allowlisted
 * (or Cloud Run) HTTPS origins, or plain-HTTP localhost outside production.
 */
function safeTargetUrl(
  value: string | undefined,
  allowedHosts: string[]
): URL | undefined {
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
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.pathname !== '/' && url.pathname !== '')
  ) {
    return undefined;
  }
  if (isLocal) return url;
  if (
    url.protocol !== 'https:' ||
    url.port !== '' ||
    !hostnameAllowed(url.hostname, allowedHosts)
  ) {
    return undefined;
  }
  return url;
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value !== null && typeof value === 'object'
    ? (value as Record<string, unknown>)
    : undefined;
}

function finiteNumber(value: unknown): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null;
}

function nonNegativeNumber(value: unknown): number | null {
  const number = finiteNumber(value);
  return number !== null && number >= 0 ? number : null;
}

function validTimestamp(value: unknown): value is string {
  return typeof value === 'string' && Number.isFinite(Date.parse(value));
}

interface ParsedSensorPage {
  observedAt: string;
  summary: {
    sensorsOnline: number;
    sensorsTotalTracked: number;
    avgUptimeSeconds: number;
    maxUptimeSeconds: number;
    onlineWindowMs: number;
  };
  sensors: Array<{
    sensorId: string;
    online: boolean;
    firstSeen: string;
    lastSeen: string;
    uptimeSeconds: number;
  }>;
  total: number;
  nextCursor: string | null;
  uptimeSummarySampled: boolean;
}

/**
 * Validates a heartbeat-tracker `/status/sensors` snapshot. Returns null for
 * anything malformed or incomplete so it can never pass for an empty list.
 */
function parseSensorPage(
  body: Record<string, unknown>,
  limit: number
): ParsedSensorPage | null {
  const summary = asObject(body.summary);
  if (!summary || !Array.isArray(body.sensors)) return null;
  const sensorsOnline = nonNegativeNumber(summary.sensors_online);
  const sensorsTotalTracked = nonNegativeNumber(summary.sensors_total_tracked);
  const avgUptimeSeconds = nonNegativeNumber(summary.avg_uptime_seconds);
  const maxUptimeSeconds = nonNegativeNumber(summary.max_uptime_seconds);
  const onlineWindowMs = nonNegativeNumber(summary.online_window_ms);
  const total = nonNegativeNumber(body.total);
  if (
    sensorsOnline === null ||
    sensorsTotalTracked === null ||
    avgUptimeSeconds === null ||
    maxUptimeSeconds === null ||
    onlineWindowMs === null ||
    total === null ||
    !validTimestamp(body.observedAt) ||
    body.sensors.length > limit
  ) {
    return null;
  }
  let nextCursor: string | null;
  if (body.nextCursor === null) {
    nextCursor = null;
  } else if (
    typeof body.nextCursor === 'string' &&
    /^\d{1,15}$/.test(body.nextCursor)
  ) {
    nextCursor = body.nextCursor;
  } else {
    return null;
  }
  const sensors: ParsedSensorPage['sensors'] = [];
  for (const entry of body.sensors) {
    const item = asObject(entry);
    const uptimeSeconds = item ? nonNegativeNumber(item.uptime_seconds) : null;
    if (
      !item ||
      typeof item.sensor_id !== 'string' ||
      item.sensor_id.length === 0 ||
      typeof item.online !== 'boolean' ||
      !validTimestamp(item.first_seen) ||
      !validTimestamp(item.last_seen) ||
      uptimeSeconds === null
    ) {
      return null;
    }
    sensors.push({
      sensorId: item.sensor_id,
      online: item.online,
      firstSeen: item.first_seen,
      lastSeen: item.last_seen,
      uptimeSeconds,
    });
  }
  return {
    observedAt: body.observedAt,
    summary: {
      sensorsOnline,
      sensorsTotalTracked,
      avgUptimeSeconds,
      maxUptimeSeconds,
      onlineWindowMs,
    },
    sensors,
    total,
    nextCursor,
    uptimeSummarySampled: body.uptimeSummarySampled === true,
  };
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
  const staleMaxAgeMs =
    options.staleMaxAgeMs ?? positiveEnv('STATUS_STALE_MAX_AGE_MS', 300_000);
  const allowedHosts = (
    options.allowedHosts ??
    parseHostList(process.env.STATUS_ALLOWED_UPSTREAM_HOSTS)
  ).map((host) => host.toLowerCase());
  const configuredTargets = new Map(
    options.targets.map((target) => [target.name, target.url])
  );
  const localDefaultUrl = (name: string): string | undefined => {
    const port = options.localPorts?.[name] ?? DEFAULT_PORTS[name];
    return port ? `http://127.0.0.1:${port}` : undefined;
  };
  const targetUrlFor = (name: string): URL | undefined => {
    const configured = configuredTargets.get(name);
    const value =
      configured ??
      (process.env.NODE_ENV === 'production'
        ? undefined
        : localDefaultUrl(name));
    return safeTargetUrl(value, allowedHosts);
  };
  const failures = new Map<string, number>();
  const caches = new Map<string, Cached<unknown>>();
  const lastGood = new Map<string, { value: unknown; fetchedAt: number }>();
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
      const observedAt = new Date(now()).toISOString();
      return {
        status: aggregateServiceStates(
          services.map((service) => service.state)
        ),
        observedAt,
        fetchedAt: observedAt,
        services,
      };
    });
  }

  /**
   * Reads and validates one upstream JSON document. A failed or invalid refresh
   * falls back to the last valid observation (marked stale) until it exceeds
   * `staleMaxAgeMs`; after that the data is reported as unavailable.
   */
  async function fetchTarget<T>(
    name: string,
    path: string,
    cacheKey: string,
    parse: (body: Record<string, unknown>) => T | null
  ): Promise<Snapshot<T> | null> {
    return cached(cacheKey, async () => {
      const targetUrl = targetUrlFor(name);
      if (!targetUrl) return null;
      let parsed: T | null = null;
      try {
        const result = await requestJson(targetUrl, path);
        if (result.status === 200) parsed = parse(result.body);
      } catch {
        parsed = null;
      }
      const fetchedAt = now();
      if (parsed !== null) {
        lastGood.set(cacheKey, { value: parsed, fetchedAt });
        if (lastGood.size > 100) {
          const oldest = [...lastGood.entries()].sort(
            (a, b) => a[1].fetchedAt - b[1].fetchedAt
          )[0]?.[0];
          if (oldest) lastGood.delete(oldest);
        }
        return { value: parsed, fetchedAt, stale: false };
      }
      const previous = lastGood.get(cacheKey);
      if (previous && fetchedAt - previous.fetchedAt <= staleMaxAgeMs) {
        return {
          value: previous.value as T,
          fetchedAt: previous.fetchedAt,
          stale: true,
        };
      }
      lastGood.delete(cacheKey);
      return null;
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
    const snapshot = await fetchTarget(
      'heartbeat-tracker',
      `/status/sensors?${query}`,
      `sensors:${limit}:${cursor}:${filter}`,
      (body) => parseSensorPage(body, limit)
    );
    if (!snapshot) {
      return {
        available: false,
        stale: false,
        observedAt: null,
        fetchedAt: new Date(now()).toISOString(),
        summary: null,
        sensors: [],
        truncated: false,
      };
    }
    const page = snapshot.value;
    const sensors = page.sensors.map((sensor) => ({ ...sensor }));
    return {
      available: true,
      stale: snapshot.stale,
      observedAt: page.observedAt,
      fetchedAt: new Date(snapshot.fetchedAt).toISOString(),
      summary: page.summary,
      uptimeSummarySampled: page.uptimeSummarySampled,
      sensors,
      total: page.total,
      totalReturned: sensors.length,
      limit,
      filter,
      nextCursor: page.nextCursor,
      truncated: page.nextCursor !== null || sensors.length < page.total,
    };
  }

  function section<T extends object>(
    snapshot: Snapshot<Record<string, unknown>> | null,
    map: (body: Record<string, unknown>) => T
  ) {
    if (!snapshot) return { available: false as const, stale: false };
    const upstreamObservedAt = snapshot.value.observedAt;
    return {
      available: true as const,
      stale: snapshot.stale,
      observedAt: validTimestamp(upstreamObservedAt)
        ? upstreamObservedAt
        : new Date(snapshot.fetchedAt).toISOString(),
      fetchedAt: new Date(snapshot.fetchedAt).toISOString(),
      ...map(snapshot.value),
    };
  }

  async function collectNetwork() {
    const [pubsubSnapshot, anchorSnapshot] = await Promise.all([
      fetchTarget(
        'pubsub-broadcaster',
        '/metrics',
        'pubsub-metrics',
        (body) => body
      ),
      fetchTarget(
        'blockchain-anchor',
        '/status',
        'anchor-status',
        (body) => body
      ),
    ]);
    const pubsub = section(pubsubSnapshot, (body) => ({
      connectedPeerCount: finiteNumber(body.connectedPeerCount),
      minConnectedPeers: finiteNumber(body.minConnectedPeers),
      kafkaPaused:
        typeof body.kafkaPaused === 'boolean' ? body.kafkaPaused : null,
      publishSuccess: finiteNumber(body.publishSuccess),
      publishFailure: finiteNumber(body.publishFailure),
      pauseCount: finiteNumber(body.pauseCount),
      resumeCount: finiteNumber(body.resumeCount),
      scope: 'process-local',
    }));
    const text = (value: unknown): string | null =>
      typeof value === 'string' ? value : null;
    const blockchain = section(anchorSnapshot, (anchor) => ({
      queuedBatches: finiteNumber(anchor.queuedBatches),
      lastFinalizedAt: text(anchor.lastFinalizedAt),
      lastFinalizedObservationStale:
        typeof anchor.lastFinalizedObservationStale === 'boolean'
          ? anchor.lastFinalizedObservationStale
          : null,
      lastFinalizedBlock: text(anchor.lastFinalizedBlock),
      lastBatchId: text(anchor.lastBatchId),
      freeBalance: text(anchor.freeBalance),
      tokenSymbol: text(anchor.tokenSymbol),
      tokenDecimals: finiteNumber(anchor.tokenDecimals),
      bestBlock: text(anchor.bestBlock),
      finalizedBlock: text(anchor.finalizedBlock),
      finalizationGap: finiteNumber(anchor.finalizationGap),
    }));
    const observations = [pubsub, blockchain]
      .map((entry) => ('observedAt' in entry ? entry.observedAt : null))
      .filter((value): value is string => value !== null)
      .sort();
    return {
      observedAt: observations[0] ?? null,
      fetchedAt: new Date(now()).toISOString(),
      stale: pubsub.stale || blockchain.stale,
      pubsub,
      blockchain,
    };
  }

  return { collectStatus, collectSensors, collectNetwork };
}
