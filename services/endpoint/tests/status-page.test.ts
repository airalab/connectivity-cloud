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
import { describe, expect, it } from 'vitest';
import { renderStatusPage } from '../src/status-page.js';
import {
  STATUS_LOGO_DARK_SVG,
  STATUS_LOGO_LIGHT_SVG,
} from '../src/status-logo.js';
import { InMemoryRegistryReader } from './in-memory-registry-reader.js';
import { createEndpointApp } from '../src/index.js';

describe('renderStatusPage', () => {
  it('renders fully static markup listing each configured service with its health port', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      targets: [
        { name: 'batcher', label: 'Telemetry Batcher', port: 3041 },
        { name: 'pubsub-broadcaster', port: 3020 },
      ],
    });

    expect(html).toContain('<!doctype html>');
    expect(html).toContain('data-service="endpoint"');
    expect(html).toContain('data-port="3000"');
    expect(html).toContain('data-service="batcher"');
    expect(html).toContain('data-port="3041"');
    expect(html).toContain('data-service="pubsub-broadcaster"');
    expect(html).toContain('data-port="3020"');
  });

  it('shows only Service and Status columns (no Port or Latency)', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      targets: [{ name: 'batcher', label: 'Telemetry Batcher', port: 3041 }],
    });

    expect(html).toContain('<tr><th>Service</th><th>Status</th></tr>');
    expect(html).not.toContain('<th>Port</th>');
    expect(html).not.toContain('<th>Latency</th>');
    expect(html).not.toContain('<td>3041</td>');
    expect(html).not.toMatch(/latency/i);
  });

  it('shows descriptive service labels with the technical name as a secondary hint', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      targets: [
        { name: 'batcher', label: 'Telemetry Batcher', port: 3041 },
        { name: 'pubsub-broadcaster', port: 3020 },
      ],
    });

    expect(html).toContain(
      'Telemetry Ingress API<span class="service-id">endpoint</span>'
    );
    expect(html).toContain(
      'Telemetry Batcher<span class="service-id">batcher</span>'
    );
    // Falls back to the technical name when no label is configured.
    expect(html).toContain(
      'pubsub-broadcaster<span class="service-id">pubsub-broadcaster</span>'
    );
  });

  it('inlines the Robonomics logo with light and dark mode variants', () => {
    const html = renderStatusPage({ selfPort: 3000, targets: [] });

    expect(html).toContain(STATUS_LOGO_LIGHT_SVG);
    expect(html).toContain(STATUS_LOGO_DARK_SVG);
    expect(STATUS_LOGO_LIGHT_SVG).toContain('class="logo logo-light"');
    expect(STATUS_LOGO_LIGHT_SVG).toContain('fill="black"');
    expect(STATUS_LOGO_DARK_SVG).toContain('class="logo logo-dark"');
    expect(STATUS_LOGO_DARK_SVG).toContain('fill="white"');
    expect(html).toMatch(
      /@media \(prefers-color-scheme: dark\) \{[^}]*\}[\s\S]*\.logo-light \{ display: none; \}\s*\.logo-dark \{ display: block; \}/
    );
    // Self-contained: no external image requests.
    expect(html).not.toMatch(/<img|https?:\/\/robonomics\.network/);
  });

  it('embeds a client-side script that polls /health using the browser hostname and refreshes on an interval', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      targets: [],
    });

    expect(html).toContain('<script>');
    expect(html).toContain('var HOST = window.location.hostname;');
    expect(html).toContain("fetch('http://' + HOST + ':' + port + '/health'");
    expect(html).toContain('setInterval(refresh, REFRESH_MS)');
  });

  it('renders a metrics section for configured metric targets', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      targets: [],
      metrics: [
        {
          label: 'Online sensors',
          service: 'heartbeat-tracker',
          port: 3030,
          field: 'sensors_online',
        },
        {
          label: 'libp2p peers',
          service: 'pubsub-broadcaster',
          port: 3020,
          field: 'connectedPeerCount',
        },
        {
          label: 'Anchored messages',
          service: 'blockchain-anchor',
          port: 3050,
          field: 'anchored',
        },
      ],
    });

    expect(html).toContain('<h2>Metrics</h2>');
    expect(html).toContain(
      'data-service="heartbeat-tracker" data-port="3030" data-field="sensors_online"'
    );
    expect(html).toContain('Online sensors');
    expect(html).toContain(
      'data-service="pubsub-broadcaster" data-port="3020" data-field="connectedPeerCount"'
    );
    expect(html).toContain('libp2p peers');
    expect(html).toContain(
      'data-service="blockchain-anchor" data-port="3050" data-field="anchored"'
    );
    expect(html).toContain('Anchored messages');
    expect(html).toContain("fetch('http://' + HOST + ':' + port + '/metrics'");
  });

  it('omits the metrics section entirely when no metrics are configured', () => {
    const html = renderStatusPage({ selfPort: 3000, targets: [] });
    expect(html).not.toContain('<h2>Metrics</h2>');
  });

  it('renders identical markup on every call given the same options (fully static)', () => {
    const options = {
      selfPort: 3000,
      targets: [{ name: 'batcher', port: 3041 }],
    };
    expect(renderStatusPage(options)).toBe(renderStatusPage(options));
  });
});

describe('GET / status page route', () => {
  it('returns the static HTML status page without probing any services', async () => {
    const app = createEndpointApp(
      {
        registryReader: new InMemoryRegistryReader([]),
        producer: {
          async publishAuthorized() {
            return 'event-1';
          },
          async publishRejected() {
            return 'event-2';
          },
        },
      },
      {
        statusPort: 3000,
        statusTargets: [{ name: 'batcher', port: 3041 }],
      }
    );

    const response = await app.inject({ method: 'GET', url: '/' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['content-type']).toContain('text/html');
    expect(response.body).toContain('Connectivity Status');
    expect(response.body).toContain('data-service="batcher"');
    await app.close();
  });

  it('exposes CORS on /health so the status page script can poll it cross-origin', async () => {
    const app = createEndpointApp({
      registryReader: new InMemoryRegistryReader([]),
      producer: {
        async publishAuthorized() {
          return 'event-1';
        },
        async publishRejected() {
          return 'event-2';
        },
      },
    });

    const response = await app.inject({ method: 'GET', url: '/health' });

    expect(response.statusCode).toBe(200);
    expect(response.headers['access-control-allow-origin']).toBe('*');
    await app.close();
  });
});
