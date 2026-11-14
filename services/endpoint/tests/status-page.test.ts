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
import { InMemoryNonceStore } from './in-memory-nonce-store.js';
import { InMemoryRegistryReader } from './in-memory-registry-reader.js';
import { createEndpointApp } from '../src/index.js';

describe('renderStatusPage', () => {
  it('renders one accessible static dashboard with three hash-linked tabs', () => {
    const html = renderStatusPage();

    expect(html).toContain('<!doctype html>');
    expect(html).toContain('role="tablist"');
    expect(html).toContain('data-tab="overview"');
    expect(html).toContain('data-tab="sensors"');
    expect(html).toContain('data-tab="network"');
    expect(html).toContain('id="overview-panel"');
    expect(html).toContain('id="sensors-panel"');
    expect(html).toContain('id="network-panel"');
    expect(html).toContain("history.pushState(null, '', '#' + name)");
    expect(html).toContain("event.key === 'ArrowRight'");
  });

  it('uses only same-origin status APIs and polls the active tab every 30 seconds', () => {
    const html = renderStatusPage();

    expect(html).toContain("'/api/status'");
    expect(html).toContain("'/api/status' + (activeTab");
    expect(html).toContain("'/' + activeTab");
    expect(html).toContain('}, 30000)');
    expect(html).not.toContain('window.location.hostname');
    expect(html).not.toMatch(/fetch\(['"]https?:\/\//);
    expect(html).not.toMatch(/:[0-9]{4}\/(health|metrics)/);
  });

  it('ships syntactically valid inline dashboard code', () => {
    const script = /<script>([\s\S]*)<\/script>/.exec(renderStatusPage())?.[1];
    expect(script).toBeTruthy();
    expect(() => new Function(script!)).not.toThrow();
  });

  it('distinguishes observation from fetch time and flags retained data as stale', () => {
    const html = renderStatusPage();
    for (const tab of ['overview', 'sensors', 'network']) {
      expect(html).toContain(`id="${tab}-stale"`);
    }
    expect(html).toContain("'Observed ' + clock(data.observedAt)");
    expect(html).toContain("'fetched ' + clock(data.fetchedAt)");
    expect(html).toContain(
      "setStale(tab, 'Stale data: the latest refresh failed"
    );
    expect(html).toContain('data-stale');
    expect(html).not.toContain("'Updated '");
  });

  it('inlines the Robonomics logo for light and dark themes', () => {
    const html = renderStatusPage();
    expect(html).toContain(STATUS_LOGO_LIGHT_SVG);
    expect(html).toContain(STATUS_LOGO_DARK_SVG);
    expect(html).toContain('@media (prefers-color-scheme:dark)');
    expect(html).not.toMatch(/<img|https?:\/\/robonomics\.network/);
  });
});

describe('status routes', () => {
  it('serves the static dashboard and same-origin status snapshots', async () => {
    const app = createEndpointApp(
      {
        registryReader: new InMemoryRegistryReader([]),
        nonceStore: new InMemoryNonceStore(),
        producer: {
          async publishAuthorized() {
            return 'event-1';
          },
          async publishRejected() {
            return 'event-2';
          },
        },
      },
      { statusUrls: [] }
    );

    const page = await app.inject({ method: 'GET', url: '/' });
    expect(page.statusCode).toBe(200);
    expect(page.headers['content-type']).toContain('text/html');
    expect(page.body).toContain('Connectivity Cloud Status');

    const overview = await app.inject({ method: 'GET', url: '/api/status' });
    expect(overview.statusCode).toBe(200);
    expect(overview.json().services).toEqual([
      expect.objectContaining({ name: 'endpoint', state: 'operational' }),
    ]);

    const sensors = await app.inject({
      method: 'GET',
      url: '/api/status/sensors',
    });
    expect(sensors.json()).toMatchObject({ available: false, sensors: [] });

    const network = await app.inject({
      method: 'GET',
      url: '/api/status/network',
    });
    expect(network.json().pubsub.available).toBe(false);
    expect(network.json().blockchain.available).toBe(false);
    await app.close();
  });
});
