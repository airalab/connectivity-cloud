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
import { STATUS_LOGO_DARK_SVG, STATUS_LOGO_LIGHT_SVG } from './status-logo.js';
import { sensorIdToSs58 } from './sensor-address.js';

const DASHBOARD_SCRIPT = String.raw`
${sensorIdToSs58.toString()}
(function () {
  var activeTab = 'overview';
  var timer = null;
  var request = null;
  var sensorCursor = '0';
  var sensorNextCursor = null;
  var appendNextSensorPage = false;
  var names = ['overview', 'sensors', 'network'];
  var labels = { operational: 'Operational', degraded: 'Degraded', outage: 'Outage', unknown: 'Unknown' };

  function setTab(name, updateHash) {
    if (names.indexOf(name) < 0) name = 'overview';
    activeTab = name;
    document.querySelectorAll('[role="tab"]').forEach(function (tab) {
      var selected = tab.getAttribute('data-tab') === name;
      tab.setAttribute('aria-selected', String(selected));
      tab.tabIndex = selected ? 0 : -1;
    });
    document.querySelectorAll('[role="tabpanel"]').forEach(function (panel) {
      panel.hidden = panel.id !== name + '-panel';
    });
    if (updateHash && location.hash !== '#' + name) history.pushState(null, '', '#' + name);
    if (request) request.abort();
    refresh();
    clearInterval(timer);
    timer = setInterval(function () {
      if (activeTab === 'sensors') {
        sensorCursor = '0';
        sensorNextCursor = null;
      }
      refresh();
    }, 30000);
  }

  function text(id, value) {
    var node = document.getElementById(id);
    if (node) node.textContent = value == null ? 'Unavailable' : String(value);
  }

  function duration(seconds) {
    if (typeof seconds !== 'number' || !Number.isFinite(seconds) || seconds < 0) return 'Unavailable';
    if (seconds < 60) return Math.floor(seconds) + 's';
    var days = Math.floor(seconds / 86400);
    var hours = Math.floor(seconds % 86400 / 3600);
    var minutes = Math.floor(seconds % 3600 / 60);
    return (days ? days + 'd ' : '') + (hours ? hours + 'h ' : '') + minutes + 'm';
  }

  function relativeTime(value) {
    if (!value) return 'Unavailable';
    var timestamp = Date.parse(value);
    if (!Number.isFinite(timestamp)) return 'Unavailable';
    var seconds = Math.round((timestamp - Date.now()) / 1000);
    var direction = seconds < 0 ? ' ago' : ' from now';
    var absolute = Math.abs(seconds);
    if (absolute < 60) return absolute + ' seconds' + direction;
    if (absolute < 3600) return Math.round(absolute / 60) + ' minutes' + direction;
    if (absolute < 86400) return Math.round(absolute / 3600) + ' hours' + direction;
    return Math.round(absolute / 86400) + ' days' + direction;
  }

  function stateBadge(state) {
    return '<span class="badge ' + (labels[state] ? state : 'unknown') + '">' +
      (labels[state] || labels.unknown) + '</span>';
  }

  function renderOverview(data) {
    text('overall-state', labels[data.status] || labels.unknown);
    text('overview-time', data.observedAt ? new Date(data.observedAt).toLocaleString() : 'Unavailable');
    var list = document.getElementById('service-list');
    list.replaceChildren();
    (data.services || []).forEach(function (service) {
      var row = document.createElement('div');
      row.className = 'service';
      var title = document.createElement('div');
      title.innerHTML = '<strong></strong><small></small>';
      title.querySelector('strong').textContent = service.label || service.name;
      title.querySelector('small').textContent = service.name;
      var observed = document.createElement('small');
      observed.textContent = service.observedAt
        ? 'Observed ' + new Date(service.observedAt).toLocaleTimeString()
        : 'No reliable observation';
      title.append(observed);
      var state = document.createElement('span');
      state.innerHTML = stateBadge(service.state);
      row.append(title, state);
      list.append(row);
    });
  }

  function renderSensors(data, append) {
    var summary = data.summary;
    ['sensorsOnline', 'sensorsTotalTracked', 'avgUptimeSeconds', 'maxUptimeSeconds', 'onlineWindowMs'].forEach(function (key) {
      var value = summary && summary[key];
      text('sensor-' + key, key.indexOf('Uptime') >= 0 ? duration(value) : (key === 'onlineWindowMs' && value != null ? duration(value / 1000) : value));
    });
    var body = document.getElementById('sensor-rows');
    if (!append) body.replaceChildren();
    var filter = document.getElementById('sensor-filter').value;
    var pageSensors = (data.sensors || []).filter(function (sensor) {
      return filter === 'all' || (filter === 'online' ? sensor.online === true : sensor.online === false);
    });
    pageSensors.forEach(function (sensor) {
      var row = document.createElement('tr');
      var address = document.createElement('td');
      address.className = 'address';
      var sourceId = typeof sensor.sensorId === 'string' ? sensor.sensorId : '';
      try {
        var ss58 = sensorIdToSs58(sourceId);
        var externalUrl = new URL('https://sensors.social/');
        externalUrl.searchParams.set('sensor', ss58);
        var link = document.createElement('a');
        link.href = externalUrl.href;
        link.textContent = ss58;
        link.target = '_blank';
        link.rel = 'noopener noreferrer';
        link.setAttribute('aria-label', 'View sensor ' + ss58 + ' on sensors.social (opens in a new tab)');
        address.append(link);
      } catch {
        address.textContent = sourceId || 'Invalid sensor ID';
      }
      var state = document.createElement('td');
      state.textContent = sensor.online == null ? 'Unknown' : (sensor.online ? 'Online' : 'Offline');
      var lastSeen = document.createElement('td');
      lastSeen.textContent = relativeTime(sensor.lastSeen);
      lastSeen.title = sensor.lastSeen || '';
      var uptime = document.createElement('td');
      uptime.textContent = duration(sensor.uptimeSeconds);
      var firstSeen = document.createElement('td');
      firstSeen.textContent = sensor.firstSeen ? new Date(sensor.firstSeen).toLocaleString() : 'Unavailable';
      row.append(address, state, lastSeen, uptime, firstSeen);
      body.append(row);
    });
    if (!append && data.available && pageSensors.length === 0) {
      var emptyRow = document.createElement('tr');
      var emptyCell = document.createElement('td');
      emptyCell.colSpan = 5;
      emptyCell.textContent = 'No sensors match this filter.';
      emptyRow.append(emptyCell);
      body.append(emptyRow);
    }
    text('sensor-time', data.observedAt ? new Date(data.observedAt).toLocaleString() : 'Unavailable');
    text('sensor-note', data.available ? (data.total + ' sensors match the ' + filter + ' filter; each page contains up to ' + data.limit + '.') : 'Sensor data is unavailable.');
    sensorNextCursor = data.nextCursor || null;
    document.getElementById('sensor-more').hidden = !sensorNextCursor;
  }

  function renderNetwork(data) {
    text('network-time', data.observedAt ? new Date(data.observedAt).toLocaleString() : 'Unavailable');
    var p = data.pubsub || {};
    ['connectedPeerCount', 'minConnectedPeers', 'publishSuccess', 'publishFailure', 'pauseCount', 'resumeCount'].forEach(function (key) {
      text('network-' + key, p[key]);
    });
    text('network-kafkaPaused', p.kafkaPaused == null ? null : (p.kafkaPaused ? 'Paused' : 'Running'));
    text('network-scope', p.scope === 'process-local' ? 'Process-local counters (not globally aggregated)' : 'Unavailable');
    var b = data.blockchain || {};
    ['queuedBatches', 'lastFinalizedAt', 'lastFinalizedBlock', 'lastBatchId', 'freeBalance', 'tokenSymbol', 'tokenDecimals', 'bestBlock', 'finalizedBlock', 'finalizationGap'].forEach(function (key) {
      text('chain-' + key, b[key]);
    });
    text(
      'chain-lastFinalizedAt',
      b.lastFinalizedAt
        ? new Date(b.lastFinalizedAt).toLocaleString() +
            (b.lastFinalizedObservationStale ? ' (stale)' : '')
        : null
    );
  }

  function refresh() {
    if (request) request.abort();
    request = new AbortController();
    var endpoint = '/api/status' + (activeTab === 'overview' ? '' : '/' + activeTab);
    var status = document.getElementById(activeTab + '-message');
    if (status) status.textContent = 'Loading…';
    var appendSensors = activeTab === 'sensors' && appendNextSensorPage;
    appendNextSensorPage = false;
    if (activeTab === 'sensors') {
      endpoint += '?limit=50&cursor=' + encodeURIComponent(sensorCursor) +
        '&filter=' + encodeURIComponent(document.getElementById('sensor-filter').value);
    }
    fetch(endpoint, { headers: { accept: 'application/json' }, signal: request.signal })
      .then(function (response) {
        if (!response.ok) throw new Error('Status request failed');
        return response.json();
      })
      .then(function (data) {
        if (activeTab === 'overview') renderOverview(data);
        if (activeTab === 'sensors') renderSensors(data, appendSensors);
        if (activeTab === 'network') renderNetwork(data);
        if (status) status.textContent = 'Updated ' + new Date(data.observedAt).toLocaleTimeString();
      })
      .catch(function (error) {
        if (error.name !== 'AbortError' && status) status.textContent = 'Data unavailable. ' + error.message;
      });
  }

  document.querySelectorAll('[role="tab"]').forEach(function (tab) {
    tab.addEventListener('click', function () { setTab(tab.getAttribute('data-tab'), true); });
    tab.addEventListener('keydown', function (event) {
      if (event.key !== 'ArrowLeft' && event.key !== 'ArrowRight' && event.key !== 'Home' && event.key !== 'End') return;
      event.preventDefault();
      var current = names.indexOf(activeTab);
      var next = event.key === 'Home' ? 0 : event.key === 'End' ? names.length - 1 :
        (current + (event.key === 'ArrowRight' ? 1 : names.length - 1)) % names.length;
      setTab(names[next], true);
      document.querySelector('[data-tab="' + names[next] + '"]').focus();
    });
  });
  document.getElementById('sensor-filter').addEventListener('change', function () {
    sensorCursor = '0';
    refresh();
  });
  document.getElementById('sensor-more').addEventListener('click', function () {
    if (!sensorNextCursor) return;
    sensorCursor = sensorNextCursor;
    appendNextSensorPage = true;
    refresh();
  });
  window.addEventListener('popstate', function () { setTab(location.hash.slice(1), false); });
  window.addEventListener('hashchange', function () { setTab(location.hash.slice(1), false); });
  setTab(location.hash.slice(1), false);
})();
`;

export function renderStatusPage(): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Connectivity Cloud Status</title>
<style>
  :root { color-scheme: light dark; --fg:#1a1a1a; --muted:#666; --bg:#fff; --line:#e7e7e7; --card:#fafafa; }
  * { box-sizing:border-box; }
  body { max-width:900px; margin:2rem auto; padding:0 1rem; color:var(--fg); background:var(--bg); font:15px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }
  .logo { display:block; width:176px; height:28px; margin:0 0 1.5rem; } .logo-dark { display:none; }
  h1 { font-size:1.55rem; margin:.2rem 0 .25rem; } h2 { font-size:1.1rem; margin:1.5rem 0 .5rem; }
  .subtitle,.muted,small { color:var(--muted); } .subtitle { margin:0 0 1.2rem; }
  [role=tablist] { display:flex; gap:.3rem; border-bottom:1px solid var(--line); margin-bottom:1rem; }
  [role=tab] { border:0; border-bottom:3px solid transparent; background:transparent; color:var(--muted); padding:.65rem .9rem; font:inherit; cursor:pointer; }
  [role=tab][aria-selected=true] { border-color:#1677ff; color:var(--fg); font-weight:600; }
  :focus-visible { outline:2px solid #1677ff; outline-offset:2px; }
  .panel { min-height:18rem; } .summary,.grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(155px,1fr)); gap:.7rem; }
  .card,.service { border:1px solid var(--line); border-radius:8px; padding:.8rem 1rem; background:var(--card); }
  .card strong { display:block; font-size:1.25rem; overflow-wrap:anywhere; } .card span { color:var(--muted); font-size:.85rem; }
  .service-list { display:grid; gap:.5rem; } .service { display:flex; align-items:center; justify-content:space-between; gap:1rem; }
  .service small { display:block; } .badge { display:inline-block; border-radius:999px; padding:.15rem .6rem; font-size:.8rem; font-weight:600; background:#ddd; }
  .badge.operational { color:#087443; background:#d8f6e8; } .badge.degraded { color:#875d00; background:#fff0c2; }
  .badge.outage { color:#a51d1d; background:#ffe0e0; } .badge.unknown { color:#555; background:#e9e9e9; }
  .status-line { min-height:1.5em; margin:.5rem 0; color:var(--muted); font-size:.85rem; }
  .table-wrap { overflow-x:auto; } table { width:100%; border-collapse:collapse; } th,td { text-align:left; padding:.55rem; border-bottom:1px solid var(--line); vertical-align:top; }
  th { color:var(--muted); font-weight:600; } td.address { overflow-wrap:anywhere; word-break:break-all; font-family:ui-monospace,monospace; font-size:.82rem; }
  select { font:inherit; padding:.35rem; } [hidden] { display:none !important; }
  @media (max-width:560px) { body { margin:1.2rem auto; } .logo { margin-bottom:1rem; } th,td { padding:.4rem .3rem; font-size:.8rem; } [role=tab] { padding:.55rem; } }
  @media (prefers-color-scheme:dark) { :root { --fg:#eee; --muted:#aaa; --bg:#111; --line:#333; --card:#191919; } .logo-light { display:none; } .logo-dark { display:block; } .badge.operational { color:#9be2bc; background:#16472f; } .badge.degraded { color:#ffe08a; background:#51400e; } .badge.outage { color:#ffb3b3; background:#511c1c; } .badge.unknown { color:#ccc; background:#333; } }
</style>
</head>
<body>
${STATUS_LOGO_LIGHT_SVG}
${STATUS_LOGO_DARK_SVG}
<h1>Connectivity Cloud Status</h1>
<p class="subtitle">Live readiness and telemetry observability</p>
<div role="tablist" aria-label="Status dashboard">
  <button role="tab" id="overview-tab" aria-controls="overview-panel" aria-selected="true" data-tab="overview">Overview</button>
  <button role="tab" id="sensors-tab" aria-controls="sensors-panel" aria-selected="false" tabindex="-1" data-tab="sensors">Sensors</button>
  <button role="tab" id="network-tab" aria-controls="network-panel" aria-selected="false" tabindex="-1" data-tab="network">Network</button>
</div>
<section role="tabpanel" id="overview-panel" aria-labelledby="overview-tab" class="panel">
  <div class="summary"><div class="card"><strong id="overall-state">Loading…</strong><span>Overall system status</span></div>
  <div class="card"><strong id="overview-time">Unavailable</strong><span>Observation time</span></div></div>
  <h2>Service readiness</h2><div id="service-list" class="service-list"></div>
  <p id="overview-message" class="status-line" aria-live="polite">Loading…</p>
</section>
<section role="tabpanel" id="sensors-panel" aria-labelledby="sensors-tab" class="panel" hidden>
  <div class="summary">
    <div class="card"><strong id="sensor-sensorsOnline">Unavailable</strong><span>Online sensors</span></div>
    <div class="card"><strong id="sensor-sensorsTotalTracked">Unavailable</strong><span>Total tracked</span></div>
    <div class="card"><strong id="sensor-avgUptimeSeconds">Unavailable</strong><span>Average continuous uptime</span></div>
    <div class="card"><strong id="sensor-maxUptimeSeconds">Unavailable</strong><span>Maximum continuous uptime</span></div>
    <div class="card"><strong id="sensor-onlineWindowMs">Unavailable</strong><span>Online detection window</span></div>
  </div>
  <h2>Sensor activity</h2>
  <label for="sensor-filter">Filter sensors </label><select id="sensor-filter"><option value="all">All</option><option value="online">Online</option><option value="offline">Offline</option></select>
  <p id="sensor-note" class="muted"></p><div class="table-wrap"><table><thead><tr><th>Sensor</th><th>Status</th><th>Last seen</th><th>Uptime</th><th>First seen</th></tr></thead><tbody id="sensor-rows"></tbody></table></div>
  <button id="sensor-more" type="button" hidden>Load more sensors</button>
  <p id="sensor-time" class="muted"></p><p id="sensors-message" class="status-line" aria-live="polite">Open tab to load sensor data.</p>
</section>
<section role="tabpanel" id="network-panel" aria-labelledby="network-tab" class="panel" hidden>
  <p class="muted">Observed <span id="network-time">Unavailable</span></p>
  <h2>libp2p / GossipSub</h2><p id="network-scope" class="muted">Unavailable</p>
  <div class="grid">
    <div class="card"><strong id="network-connectedPeerCount">Unavailable</strong><span>Connected reserved peers</span></div>
    <div class="card"><strong id="network-minConnectedPeers">Unavailable</strong><span>Required minimum</span></div>
    <div class="card"><strong id="network-kafkaPaused">Unavailable</strong><span>Kafka consumption</span></div>
    <div class="card"><strong id="network-publishSuccess">Unavailable</strong><span>Successful publishes</span></div>
    <div class="card"><strong id="network-publishFailure">Unavailable</strong><span>Failed publishes</span></div>
    <div class="card"><strong id="network-pauseCount">Unavailable</strong><span>Pause count</span></div>
    <div class="card"><strong id="network-resumeCount">Unavailable</strong><span>Resume count</span></div>
  </div>
  <h2>Robonomics Blockchain</h2><div class="grid">
    <div class="card"><strong id="chain-queuedBatches">Unavailable</strong><span>Queued batches</span></div>
    <div class="card"><strong id="chain-lastFinalizedAt">Unavailable</strong><span>Last finalized batch</span></div>
    <div class="card"><strong id="chain-lastFinalizedBlock">Unavailable</strong><span>Last batch block</span></div>
    <div class="card"><strong id="chain-lastBatchId">Unavailable</strong><span>Last batch ID</span></div>
    <div class="card"><strong id="chain-freeBalance">Unavailable</strong><span>Anchor free balance <span id="chain-tokenSymbol"></span></span></div>
    <div class="card"><strong id="chain-tokenDecimals">Unavailable</strong><span>Token decimals</span></div>
    <div class="card"><strong id="chain-bestBlock">Unavailable</strong><span>Best block</span></div>
    <div class="card"><strong id="chain-finalizedBlock">Unavailable</strong><span>Finalized block</span></div>
    <div class="card"><strong id="chain-finalizationGap">Unavailable</strong><span>Finalization gap</span></div>
  </div>
  <p id="network-message" class="status-line" aria-live="polite">Open tab to load network data.</p>
</section>
<script>${DASHBOARD_SCRIPT}</script>
</body>
</html>`;
}
