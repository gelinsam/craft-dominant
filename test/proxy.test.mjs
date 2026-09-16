/**
 * Behavioural tests for pages/api/proxy/[...path].js.
 *
 * Read this first, because it is the honest scope of the file under test:
 * the proxy does NOT authenticate the human caller. It authenticates itself to
 * Railway. The human boundary is Vercel Deployment Protection (SSO), which is
 * platform configuration and cannot be asserted from here -- it is verified
 * against the Vercel project settings and documented in the PR instead.
 *
 * What IS testable here, and is tested below, are the defences that live in the
 * file: the allowlist, per-prefix methods, cross-site rejection, fail-closed
 * behaviour when the key is missing, and the guarantee that the key is never
 * echoed to a caller.
 *
 * Run with: node --test test/
 */

import { test, describe } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

// The handler is an ES module in a CommonJS package, so load it as a module
// from a data URL. The real file runs -- nothing is reimplemented here.
const source = readFileSync(join(root, 'pages/api/proxy/[...path].js'), 'utf8');
const mod = await import(
  `data:text/javascript;base64,${Buffer.from(source).toString('base64')}`
);
const handler = mod.default;
const ALLOWED = mod.__ALLOWED_FOR_TESTS;

const KEY = 'test-command-key';

function mockRes() {
  const res = {
    statusCode: null, body: null, headers: {}, ended: false,
    setHeader(k, v) { this.headers[k.toLowerCase()] = v; },
    status(code) { this.statusCode = code; return this; },
    json(payload) { this.body = payload; this.ended = true; return this; },
    send(payload) { this.body = payload; this.ended = true; return this; },
  };
  return res;
}

function mockReq({ path, method = 'GET', headers = {}, body = null, query = '' }) {
  const segs = path.split('/');
  return {
    method,
    headers,
    body,
    query: { path: segs },
    url: `/api/proxy/${path}${query}`,
  };
}

async function call(opts, { key = KEY, fetchImpl } = {}) {
  const prevKey = process.env.COMMAND_API_KEY;
  const prevFetch = globalThis.fetch;
  if (key === null) delete process.env.COMMAND_API_KEY;
  else process.env.COMMAND_API_KEY = key;
  if (fetchImpl) globalThis.fetch = fetchImpl;
  const res = mockRes();
  try {
    await handler(mockReq(opts), res);
  } finally {
    globalThis.fetch = prevFetch;
    if (prevKey === undefined) delete process.env.COMMAND_API_KEY;
    else process.env.COMMAND_API_KEY = prevKey;
  }
  return res;
}

// A fetch that records what the proxy asked Railway for, and returns a body.
function spyFetch(payload = { ok: true }) {
  const calls = [];
  const impl = async (url, init) => {
    calls.push({ url, init });
    return {
      status: 200,
      headers: new Map([['content-type', 'application/json']]),
      arrayBuffer: async () => Buffer.from(JSON.stringify(payload)),
    };
  };
  impl.calls = calls;
  return impl;
}

describe('allowlist', () => {
  test('a prefix the dashboard does not call is not proxied', async () => {
    for (const path of ['api/meta-debug', 'api/auto-exports', 'api/curves',
                        'api/alerts', 'api/events', 'api/meta-status',
                        'api/v2/interventions', 'api/health']) {
      const res = await call({ path });
      assert.equal(res.statusCode, 404, `${path} should not be proxied`);
      assert.equal(res.body.error, 'not_proxied');
    }
  });

  test('a near-miss prefix is not treated as a match', async () => {
    const res = await call({ path: 'api/customersX/secret' });
    assert.equal(res.statusCode, 404);
  });

  test('an allowed prefix reaches the backend', async () => {
    const fetchImpl = spyFetch();
    const res = await call({ path: 'api/customers' }, { fetchImpl });
    assert.equal(res.statusCode, 200);
    assert.equal(fetchImpl.calls.length, 1);
  });
});

describe('methods', () => {
  test('read-only surfaces reject POST', async () => {
    for (const path of ['api/customers', 'api/overlap', 'api/export/csv',
                        'api/intelligence/E1', 'api/targeting/E1', 'api/dashboard']) {
      const res = await call({ path, method: 'POST' });
      assert.equal(res.statusCode, 405, `${path} must not accept POST`);
    }
  });

  test('campaigns accepts POST, because the UI approves and rejects', async () => {
    const fetchImpl = spyFetch();
    const res = await call({ path: 'api/campaigns/abc/approve', method: 'POST' }, { fetchImpl });
    assert.equal(res.statusCode, 200);
  });

  test('no method other than GET or POST is forwarded', async () => {
    for (const method of ['DELETE', 'PUT', 'PATCH']) {
      const res = await call({ path: 'api/campaigns/abc', method });
      assert.ok([404, 405].includes(res.statusCode));
    }
  });
});

describe('cross-site requests', () => {
  test('a cross-site browser request is blocked', async () => {
    const res = await call({ path: 'api/customers', headers: { 'sec-fetch-site': 'cross-site' } });
    assert.equal(res.statusCode, 403);
    assert.equal(res.body.error, 'cross_site_request_blocked');
  });

  test('same-origin is allowed', async () => {
    const fetchImpl = spyFetch();
    const res = await call(
      { path: 'api/customers', headers: { 'sec-fetch-site': 'same-origin' } },
      { fetchImpl },
    );
    assert.equal(res.statusCode, 200);
  });
});

describe('the backend key', () => {
  test('fails closed when the key is not configured', async () => {
    const res = await call({ path: 'api/customers' }, { key: null });
    assert.equal(res.statusCode, 503);
    assert.equal(res.body.error, 'command_api_not_configured');
  });

  test('is sent to Railway as a bearer, never returned to the caller', async () => {
    const fetchImpl = spyFetch({ customers: [] });
    const res = await call({ path: 'api/customers' }, { fetchImpl });
    assert.equal(fetchImpl.calls[0].init.headers.Authorization, `Bearer ${KEY}`);
    assert.ok(!JSON.stringify(res.body).includes(KEY), 'key leaked in response body');
    assert.ok(!JSON.stringify(res.headers).includes(KEY), 'key leaked in headers');
  });

  test('query parameters survive the hop but "path" does not', async () => {
    const fetchImpl = spyFetch();
    await call({ path: 'api/export/csv', query: '?event_id=E1&audience=all' }, { fetchImpl });
    const url = String(fetchImpl.calls[0].url);   // a URL object since the
    // destination is now resolved and re-checked before the fetch
    assert.ok(url.includes('event_id=E1'));
    assert.ok(url.includes('audience=all'));
    assert.ok(!url.includes('path='));
  });
});

describe('the allowlist matches what the dashboard actually calls', () => {
  const pageSources = readdirSync(join(root, 'pages'))
    .filter((f) => f.endsWith('.js'))
    .map((f) => readFileSync(join(root, 'pages', f), 'utf8'))
    .join('\n');

  test('every call site in the dashboard is covered by the allowlist', () => {
    const used = [...pageSources.matchAll(/\/api\/proxy\/(api\/[a-z0-9/-]+)/g)]
      .map((m) => m[1]);
    assert.ok(used.length > 0, 'expected the dashboard to call the proxy');
    for (const path of used) {
      const hit = ALLOWED.find((e) => path === e.prefix || path.startsWith(`${e.prefix}/`));
      assert.ok(hit, `dashboard calls ${path} but the allowlist does not cover it`);
    }
  });

  test('every allowlist entry is actually used — no speculative surface', () => {
    for (const entry of ALLOWED) {
      assert.ok(
        pageSources.includes(`/api/proxy/${entry.prefix}`),
        `${entry.prefix} is proxied but nothing calls it`,
      );
    }
  });
});

describe('path traversal — the allowlist must apply to the path actually requested', () => {
  // These four all reached the backend in the first version: match() ran on the
  // string as supplied, fetch() resolved it afterwards, and the two disagreed.
  const escapes = [
    ['api/customers/../meta-debug', 'GET'],
    ['api/campaigns/../v2/interventions/demo/execute', 'POST'],
    ['api/customers/%2e%2e/meta-debug', 'GET'],
    ['api/customers/..%2fmeta-debug', 'GET'],
    ['api/customers/..%5cmeta-debug', 'GET'],
    ['api/customers/./../meta-debug', 'GET'],
  ];

  for (const [path, method] of escapes) {
    test(`${method} ${path} is refused and never forwarded`, async () => {
      let reached = null;
      const fetchImpl = async (url) => {
        reached = url;
        return { status: 200, headers: new Map(), arrayBuffer: async () => Buffer.from('{}') };
      };
      const res = await call({ path, method }, { fetchImpl });
      assert.equal(res.statusCode, 400, `${path} should be rejected`);
      assert.equal(reached, null, `${path} must not reach the backend`);
    });
  }

  test('the V2 execution surface is unreachable through the proxy', async () => {
    let reached = null;
    const fetchImpl = async (url) => { reached = url; return { status: 200, headers: new Map(), arrayBuffer: async () => Buffer.from('{}') }; };
    for (const path of ['api/v2/interventions/x/execute',
                        'api/campaigns/../v2/interventions/x/execute']) {
      const res = await call({ path, method: 'POST' }, { fetchImpl });
      assert.ok([400, 404].includes(res.statusCode));
    }
    assert.equal(reached, null);
  });
});

describe('origin fallback when Sec-Fetch-Site is absent', () => {
  test('a foreign Origin is refused', async () => {
    const res = await call({
      path: 'api/customers',
      headers: { origin: 'https://evil.example', host: 'craft-dominant.vercel.app' },
    });
    assert.equal(res.statusCode, 403);
    assert.equal(res.body.error, 'cross_site_request_blocked');
  });

  test('a matching Origin is allowed', async () => {
    const fetchImpl = spyFetch();
    const res = await call({
      path: 'api/customers',
      headers: { origin: 'https://craft-dominant.vercel.app', host: 'craft-dominant.vercel.app' },
    }, { fetchImpl });
    assert.equal(res.statusCode, 200);
  });

  test('an unparseable Origin is refused', async () => {
    const res = await call({
      path: 'api/customers',
      headers: { origin: 'not-a-url', host: 'craft-dominant.vercel.app' },
    });
    assert.equal(res.statusCode, 403);
  });
});

describe('sync is a mutation', () => {
  test('GET /api/sync is not proxied', async () => {
    const res = await call({ path: 'api/sync', method: 'GET' });
    assert.equal(res.statusCode, 405);
  });

  test('POST /api/sync is', async () => {
    const fetchImpl = spyFetch();
    const res = await call({ path: 'api/sync', method: 'POST' }, { fetchImpl });
    assert.equal(res.statusCode, 200);
  });

  test('sync-status stays a read', async () => {
    const fetchImpl = spyFetch();
    const res = await call({ path: 'api/sync-status', method: 'GET' }, { fetchImpl });
    assert.equal(res.statusCode, 200);
  });
});

describe('encoded characters in the query string are data, not routing', () => {
  // Scanning the whole URL for %2e/%2f/%5c rejected legitimate traffic: a filter
  // value or audience name carrying those encodings is a query value and never
  // participates in routing. The check now covers the path only.
  const legit = [
    ['api/export/csv', '?event_id=E1&audience=a%2Fb'],
    ['api/customers', '?q=%2E%2E'],
    ['api/export/csv', '?event_id=E%5C1&audience=all'],
    ['api/customers', '?segment=champions%2Floyal&city=New%20York'],
  ];

  for (const [path, query] of legit) {
    test(`${path}${query} is forwarded intact`, async () => {
      const fetchImpl = spyFetch();
      const res = await call({ path, query }, { fetchImpl });
      assert.equal(res.statusCode, 200, `${query} should not be rejected`);
      assert.equal(fetchImpl.calls.length, 1, 'should have reached the backend');
      const sent = new URL(String(fetchImpl.calls[0].url));
      const expected = new URLSearchParams(query.slice(1));
      for (const [k, v] of expected) {
        assert.equal(sent.searchParams.get(k), v, `query value ${k} was altered`);
      }
    });
  }

  test('an encoded separator in the PATH is still rejected', async () => {
    let reached = null;
    const fetchImpl = async (url) => { reached = url; return { status: 200, headers: new Map(), arrayBuffer: async () => Buffer.from('{}') }; };
    const res = await call({ path: 'api/customers/%2e%2e/meta-debug', query: '?q=1' }, { fetchImpl });
    assert.equal(res.statusCode, 400);
    assert.equal(res.body.error, 'encoded_separator');
    assert.equal(reached, null);
  });

  test('a traversal path with an innocent query is still rejected', async () => {
    let reached = null;
    const fetchImpl = async (url) => { reached = url; return { status: 200, headers: new Map(), arrayBuffer: async () => Buffer.from('{}') }; };
    const res = await call({ path: 'api/customers/../meta-debug', query: '?audience=all' }, { fetchImpl });
    assert.equal(res.statusCode, 400);
    assert.equal(reached, null);
  });
});
