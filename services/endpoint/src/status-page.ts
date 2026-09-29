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
import type { StatusTargetConfig } from './config.js';

export interface StatusPageOptions {
  /** This service's own listening port. */
  selfPort: number;
  /** Host used by the browser to reach every service's health port. */
  host: string;
  /** Sibling services (name + default port) shown on the status page. */
  targets: StatusTargetConfig[];
  /** Poll interval, in milliseconds, used by the page's client-side script. */
  refreshIntervalMs?: number;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * Renders a fully static, dependency-free HTML status page.
 *
 * The server does no probing itself: the returned markup is identical on
 * every request. A small inline script embedded in the page performs the
 * `/health` checks directly from the browser and refreshes the table on an
 * interval, so the page keeps itself up to date without any server work.
 */
export function renderStatusPage(options: StatusPageOptions): string {
  const refreshIntervalMs = options.refreshIntervalMs ?? 5000;
  const services = [
    { name: 'endpoint', port: options.selfPort },
    ...options.targets,
  ];

  const rows = services
    .map(
      (service) => `
        <tr data-service="${escapeHtml(service.name)}" data-port="${service.port}">
          <td><span class="dot"></span>${escapeHtml(service.name)}</td>
          <td>${service.port}</td>
          <td class="state">Checking…</td>
          <td class="latency">—</td>
        </tr>`
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
  p.subtitle { color: #666; margin-top: 0; margin-bottom: 1.5rem; font-size: 0.9rem; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 0.5rem 0.75rem; border-bottom: 1px solid #eee; font-size: 0.9rem; }
  th { color: #666; font-weight: 500; }
  .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 0.5rem; background: #999; }
  .dot.up { background: #22c55e; }
  .dot.down { background: #ef4444; }
  .state.up { color: #22c55e; }
  .state.down { color: #ef4444; }
  @media (prefers-color-scheme: dark) {
    body { color: #eee; background: #111; }
    p.subtitle { color: #999; }
    th { color: #999; }
    th, td { border-bottom-color: #333; }
  }
</style>
</head>
<body>
  <h1>Connectivity Status</h1>
  <p class="subtitle" id="subtitle">Checking services…</p>
  <table>
    <thead>
      <tr><th>Service</th><th>Port</th><th>Status</th><th>Latency</th></tr>
    </thead>
    <tbody id="services">${rows}
    </tbody>
  </table>
<script>
(function () {
  var HOST = ${JSON.stringify(options.host)};
  var REFRESH_MS = ${refreshIntervalMs};

  function setRowState(row, healthy, latencyMs) {
    var dot = row.querySelector('.dot');
    var state = row.querySelector('.state');
    var latency = row.querySelector('.latency');
    dot.className = 'dot ' + (healthy ? 'up' : 'down');
    state.className = 'state ' + (healthy ? 'up' : 'down');
    state.textContent = healthy ? 'Operational' : 'Unreachable';
    latency.textContent = healthy && latencyMs !== null ? latencyMs + ' ms' : '—';
  }

  function checkRow(row) {
    var port = row.getAttribute('data-port');
    var startedAt = Date.now();
    var controller = new AbortController();
    var timeout = setTimeout(function () { controller.abort(); }, 1500);
    return fetch('http://' + HOST + ':' + port + '/health', { signal: controller.signal })
      .then(function (response) {
        setRowState(row, response.ok, Date.now() - startedAt);
        return response.ok;
      })
      .catch(function () {
        setRowState(row, false, null);
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
  }

  refresh();
  setInterval(refresh, REFRESH_MS);
})();
</script>
</body>
</html>
`;
}
