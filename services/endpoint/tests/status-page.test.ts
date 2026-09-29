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
import { InMemoryRegistryReader } from '@scp/registry-sync';
import { createEndpointApp } from '../src/index.js';

describe('renderStatusPage', () => {
  it('renders fully static markup listing each configured service and its port', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      host: 'localhost',
      targets: [
        { name: 'batcher', port: 3041 },
        { name: 'registry-sync', port: 3011 },
      ],
    });

    expect(html).toContain('<!doctype html>');
    expect(html).toContain('data-service="endpoint"');
    expect(html).toContain('data-port="3000"');
    expect(html).toContain('data-service="batcher"');
    expect(html).toContain('data-port="3041"');
    expect(html).toContain('data-service="registry-sync"');
    expect(html).toContain('data-port="3011"');
  });

  it('embeds a client-side script that polls /health and refreshes on an interval', () => {
    const html = renderStatusPage({
      selfPort: 3000,
      host: 'localhost',
      targets: [],
    });

    expect(html).toContain('<script>');
    expect(html).toContain("fetch('http://' + HOST + ':' + port + '/health'");
    expect(html).toContain('setInterval(refresh, REFRESH_MS)');
    expect(html).toContain('"localhost"');
  });

  it('renders identical markup on every call given the same options (fully static)', () => {
    const options = {
      selfPort: 3000,
      host: 'localhost',
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
        statusHost: 'localhost',
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
