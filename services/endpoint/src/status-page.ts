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
import type { StatusMetricConfig, StatusTargetConfig } from './config.js';
import { STATUS_LOGO_DARK_SVG, STATUS_LOGO_LIGHT_SVG } from './status-logo.js';

export interface StatusPageOptions {
  /** This service's own listening port. */
  selfPort: number;
  /** Sibling services (name, descriptive label, and port) shown on the status page. */
  targets: StatusTargetConfig[];
  /** Simple headline metrics polled from sibling services' `/metrics`. */
  metrics?: StatusMetricConfig[];
  /** Poll interval, in milliseconds, used by the page's client-side script. */
  refreshIntervalMs?: number;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

/**
 * Renders a fully static, dependency-free HTML status page.
 *
 * The server does no probing itself: the returned markup is identical on
 * every request. A small inline script embedded in the page performs the
 * `/health` checks directly from the browser and refreshes the table on an
 * interval, so the page keeps itself up to date without any server work.
 *
 * The script targets each service via `window.location.hostname`, i.e. the
 * same host the browser used to load this page, so the page works no matter
 * which domain/IP the service is reached through (localhost, LAN, or a
 * public hostname) without any server-side host configuration.
 */
export function renderStatusPage(options: StatusPageOptions): string {
  const refreshIntervalMs = options.refreshIntervalMs ?? 5000;
  const services: StatusTargetConfig[] = [
    {
      name: 'endpoint',
      label: 'Telemetry Ingress API',
      port: options.selfPort,
    },
    ...options.targets,
  ];

  const rows = services
    .map(
      (service) => `
        <tr data-service="${escapeHtml(service.name)}" data-port="${service.port}">
          <td><span class="dot"></span>${escapeHtml(service.label ?? service.name)}<span class="service-id">${escapeHtml(service.name)}</span></td>
          <td class="state">Checking…</td>
        </tr>`
    )
    .join('');

  const metrics = options.metrics ?? [];
  const metricCards = metrics
    .map(
      (metric) => `
      <div class="metric" data-service="${escapeHtml(metric.service)}" data-port="${metric.port}" data-field="${escapeHtml(metric.field)}">
        <div class="metric-value">—</div>
        <div class="metric-label">${escapeHtml(metric.label)}</div>
      </div>`
    )
    .join('');

  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Connectivity Status</title>
<style>
  :root { color-scheme: light dark; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    max-width: 640px;
    margin: 3rem auto;
    padding: 0 1rem;
    color: #1a1a1a;
    background: #fff;
  }
  h1 { font-size: 1.25rem; font-weight: 600; margin-bottom: 0.25rem; }
  .logo { display: block; width: 176px; height: 28px; margin-bottom: 1.5rem; }
  .logo-dark { display: none; }
  p.subtitle { color: #666; margin-top: 0; margin-bottom: 1.5rem; font-size: 0.9rem; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #eee; font-size: 0.9rem; }
  th { color: #666; font-weight: 500; }
  .service-id { display: block; color: #666; font-size: 0.75rem; margin-left: calc(8px + 0.5rem); }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.5rem; background: #999; }
  .dot.up { background: #22c55e; }
  .dot.down { background: #ef4444; }
  .state.up { color: #22c55e; }
  .state.down { color: #ef4444; }
  h2 { font-size: 1rem; font-weight: 600; margin: 2rem 0 0.75rem; }
  .metrics { display: flex; gap: 1rem; flex-wrap: wrap; }
  .metric {
    flex: 1 1 140px;
    border: 1px solid #eee;
    border-radius: 8px;
    padding: 0.75rem 1rem;
  }
  .metric-value { font-size: 1.5rem; font-weight: 600; }
  .metric-label { color: #666; font-size: 0.85rem; margin-top: 0.15rem; }
  @media (prefers-color-scheme: dark) {
    body { color: #eee; background: #111; }
    .logo-light { display: none; }
    .logo-dark { display: block; }
    p.subtitle { color: #999; }
    th { color: #999; }
    .service-id { color: #999; }
    th, td { border-bottom-color: #333; }
    .metric { border-color: #333; }
    .metric-label { color: #999; }
  }
</style>
</head>
<body>
  ${STATUS_LOGO_LIGHT_SVG}
  ${STATUS_LOGO_DARK_SVG}
  <h1>Connectivity Status</h1>
  <p class="subtitle" id="subtitle">Checking services…</p>
  <table>
    <thead>
      <tr><th>Service</th><th>Status</th></tr>
    </thead>
    <tbody id="services">${rows}
    </tbody>
  </table>
  ${
    metricCards
      ? `<h2>Metrics</h2>
  <div class="metrics" id="metrics">${metricCards}
  </div>`
      : ''
  }
<script>
(function () {
  // Use the hostname the browser used to load this page, so the status
  // checks work correctly regardless of which host/domain the service is
  // being accessed through (localhost, LAN IP, or a public hostname).
  var HOST = window.location.hostname;
  var REFRESH_MS = ${refreshIntervalMs};

  function setRowState(row, healthy) {
    var dot = row.querySelector('.dot');
    var state = row.querySelector('.state');
    dot.className = 'dot ' + (healthy ? 'up' : 'down');
    state.className = 'state ' + (healthy ? 'up' : 'down');
    state.textContent = healthy ? 'Operational' : 'Unreachable';
  }

  function checkRow(row) {
    var port = row.getAttribute('data-port');
    var controller = new AbortController();
    var timeout = setTimeout(function () { controller.abort(); }, 1500);
    return fetch('http://' + HOST + ':' + port + '/health', { signal: controller.signal })
      .then(function (response) {
        setRowState(row, response.ok);
        return response.ok;
      })
      .catch(function () {
        setRowState(row, false);
        return false;
      })
      .finally(function () { clearTimeout(timeout); });
  }

  function refresh() {
    var rows = Array.prototype.slice.call(document.querySelectorAll('#services tr'));
    Promise.all(rows.map(checkRow)).then(function (results) {
      var subtitle = document.getElementById('subtitle');
      var allHealthy = results.every(Boolean);
      subtitle.textContent = (allHealthy ? 'All systems operational' : 'Some services are unreachable')
        + ' · updated ' + new Date().toLocaleTimeString();
    });
    refreshMetrics();
  }

  function checkMetric(card) {
    var port = card.getAttribute('data-port');
    var field = card.getAttribute('data-field');
    var valueEl = card.querySelector('.metric-value');
    var controller = new AbortController();
    var timeout = setTimeout(function () { controller.abort(); }, 1500);
    return fetch('http://' + HOST + ':' + port + '/metrics', { signal: controller.signal })
      .then(function (response) { return response.json(); })
      .then(function (body) {
        var value = body ? body[field] : undefined;
        valueEl.textContent = (value === undefined || value === null) ? '—' : String(value);
      })
      .catch(function () {
        valueEl.textContent = '—';
      })
      .finally(function () { clearTimeout(timeout); });
  }

  function refreshMetrics() {
    var cards = Array.prototype.slice.call(document.querySelectorAll('#metrics .metric'));
    cards.forEach(checkMetric);
  }

  refresh();
  setInterval(refresh, REFRESH_MS);
})();
</script>
</body>
</html>
`;
}
