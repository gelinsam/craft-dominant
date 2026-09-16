/**
 * Server-side proxy to the Craft Dominant backend.
 *
 * WHY THIS EXISTS
 * The dashboard used to call Railway directly from the browser, which is why
 * every one of those endpoints had to be unauthenticated -- and why customer
 * emails, purchase history and segmentation were readable by anyone. Routing
 * them through here lets the backend require a key the browser never sees.
 *
 * WHERE THE HUMAN IS AUTHENTICATED  <-- read this before changing anything
 * This handler does NOT authenticate the caller. It authenticates *itself* to
 * Railway. The human boundary is Vercel Deployment Protection (SSO). Without
 * it, this file is an anonymous relay that hands out exactly the data the
 * bearer gate was added to protect -- the same exposure on a different
 * hostname.
 *
 * The project setting reads "All Deployments" (Vercel Authentication ->
 * Require Log In), which is intended to cover custom domains as well; it was
 * previously "Standard Protection", which exempts production custom domains.
 * Treat that as configuration, not as proof: neither this comment nor the
 * tests in test/proxy.test.mjs demonstrate that anonymous access is actually
 * refused. Before this relay is enabled in production, someone has to confirm
 * from a genuinely logged-out context that the production URL, every
 * deployment alias, any custom domain, this catch-all route and the existing
 * /api/command all challenge an anonymous visitor.
 *
 * If that protection is ever narrowed, or this app is served from anywhere
 * Vercel Authentication does not reach, an application session gate has to be
 * added here BEFORE that happens.
 *
 * The defences that do live in this file are deliberately narrow:
 *   - an allowlist of prefixes the dashboard actually calls, not everything
 *     the backend exposes;
 *   - per-prefix methods, so a read-only surface cannot be POSTed to;
 *   - rejection of cross-site browser requests, so another origin cannot use a
 *     logged-in viewer's session as a relay.
 * None of these is a substitute for the human boundary above.
 */

const DEFAULT_BACKEND = 'https://craft-dominant-production.up.railway.app';

// Only what the dashboard actually calls. Verified against pages/index.js and
// pages/campaigns.js -- api/events, api/curves, api/alerts, api/auto-exports
// and api/meta-status were reachable in the first draft and called by nothing.
// GET unless the UI genuinely needs to write.
const ALLOWED = [
  { prefix: 'api/dashboard', methods: ['GET'] },
  { prefix: 'api/customers', methods: ['GET'] },
  { prefix: 'api/targeting', methods: ['GET'] },
  { prefix: 'api/intelligence', methods: ['GET'] },
  { prefix: 'api/overlap', methods: ['GET'] },
  { prefix: 'api/export', methods: ['GET'] },
  { prefix: 'api/sync-status', methods: ['GET'] },
  { prefix: 'api/sync', methods: ['POST'] },         // triggers a local sync: never GET
  { prefix: 'api/engine/status', methods: ['GET'] },
  { prefix: 'api/campaigns', methods: ['GET', 'POST'] }, // approve/reject/generate/dry-run
];

// Carried back to the browser. Everything else is dropped: the backend should
// not be able to set cookies or CORS policy through this hop.
const PASSTHROUGH_HEADERS = ['content-type', 'content-disposition'];

function match(path, method) {
  const entry = ALLOWED.find(
    (e) => path === e.prefix || path.startsWith(`${e.prefix}/`),
  );
  if (!entry) return { ok: false, status: 404, error: 'not_proxied' };
  if (!entry.methods.includes(method)) {
    return { ok: false, status: 405, error: 'method_not_allowed', allow: entry.methods };
  }
  return { ok: true };
}

/**
 * Reject anything that could mean one path here and a different path after
 * URL normalisation.
 *
 * The allowlist used to be checked against the string as supplied, while
 * fetch() resolved "." and ".." before sending. So "api/customers/../meta-debug"
 * matched the api/customers prefix and then arrived at /api/meta-debug, and
 * "api/campaigns/../v2/interventions/<id>/execute" passed the campaigns entry's
 * POST permission and landed on the V2 execution surface. Percent-encoded
 * separators (%2e, %2f, %5c) did the same. The allowlist has to be applied to
 * the path that is actually requested, so suspicious input is refused outright
 * and the resolved destination is re-checked below.
 *
 * The encoded-separator scan covers the PATH only. Scanning the whole URL
 * rejected legitimate traffic: a filter value or event id carrying %2F, %2E or
 * %5C in the query string is data, not a path, and an export whose audience
 * name happens to contain an encoded slash is a valid request. Query values are
 * forwarded untouched and never participate in routing.
 */
function rejectTraversal(segments, rawUrl) {
  // Split rather than parse: the raw, still-encoded pathname is the evidence.
  // Decoding first would erase exactly what this check is looking for.
  const rawPath = String(rawUrl).split('?')[0].split('#')[0];
  if (/%2e|%2f|%5c/i.test(rawPath)) {
    return 'encoded_separator';
  }
  for (const seg of segments) {
    if (seg === '.' || seg === '..' || seg.includes('\\') || seg.includes('/')) {
      return 'path_traversal';
    }
  }
  return null;
}

export default async function handler(req, res) {
  // Same-site only. A browser sends Sec-Fetch-Site on every fetch; anything
  // cross-site is another origin trying to use a viewer's session as a relay.
  const site = req.headers['sec-fetch-site'];
  if (site && site !== 'same-origin' && site !== 'none') {
    return res.status(403).json({ error: 'cross_site_request_blocked' });
  }
  // Sec-Fetch-Site is absent on older clients and on non-browser callers, so an
  // explicitly foreign Origin is refused on its own. Checked for every method,
  // not just mutations: a cross-origin read of the customer list is the exposure
  // this whole change exists to close.
  const origin = req.headers.origin;
  if (!site && origin) {
    let originHost = null;
    try {
      originHost = new URL(origin).host;
    } catch {
      return res.status(403).json({ error: 'cross_site_request_blocked' });
    }
    if (originHost !== req.headers.host) {
      return res.status(403).json({ error: 'cross_site_request_blocked' });
    }
  }

  const segments = (Array.isArray(req.query.path) ? req.query.path : [req.query.path])
    .filter(Boolean);

  const traversal = rejectTraversal(segments, req.url || '');
  if (traversal) {
    return res.status(400).json({ error: traversal });
  }

  const path = segments.join('/');

  const verdict = match(path, req.method);
  if (!verdict.ok) {
    if (verdict.allow) res.setHeader('Allow', verdict.allow.join(', '));
    return res.status(verdict.status).json({ error: verdict.error, path });
  }

  const backend = process.env.CRAFT_BACKEND_URL || process.env.NEXT_PUBLIC_API_BASE || DEFAULT_BACKEND;
  const commandKey = process.env.COMMAND_API_KEY;

  if (!commandKey) {
    return res.status(503).json({
      error: 'command_api_not_configured',
      message: 'COMMAND_API_KEY is not configured on the frontend runtime.',
    });
  }

  // Rebuild the query string from the original URL, minus Next's own catch-all
  // parameter, so filters and event ids survive the hop.
  const incoming = new URL(req.url, 'http://localhost');
  incoming.searchParams.delete('path');
  const qs = incoming.searchParams.toString();

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    // Belt and braces: resolve the destination and confirm it is still the
    // path we authorised. rejectTraversal() should have made this unreachable;
    // it is here because the previous version of this file was bypassed by
    // exactly this gap between "what was checked" and "what was requested".
    const target = new URL(`${backend}/${path}${qs ? `?${qs}` : ''}`);
    if (target.pathname !== `/${path}`) {
      return res.status(400).json({ error: 'path_traversal' });
    }
    const resolved = match(target.pathname.replace(/^\//, ''), req.method);
    if (!resolved.ok) {
      return res.status(resolved.status).json({ error: resolved.error });
    }

    const response = await fetch(target, {
      method: req.method,
      headers: {
        Authorization: `Bearer ${commandKey}`,
        Accept: req.headers.accept || 'application/json',
        ...(req.method === 'POST' ? { 'Content-Type': 'application/json' } : {}),
      },
      ...(req.method === 'POST' && req.body
        ? { body: typeof req.body === 'string' ? req.body : JSON.stringify(req.body) }
        : {}),
      signal: controller.signal,
    });

    for (const name of PASSTHROUGH_HEADERS) {
      const value = response.headers.get(name);
      if (value) res.setHeader(name, value);
    }
    res.setHeader('Cache-Control', 'private, no-store, max-age=0');

    const buffer = Buffer.from(await response.arrayBuffer());
    return res.status(response.status).send(buffer);
  } catch (error) {
    const timedOut = error?.name === 'AbortError';
    return res.status(timedOut ? 504 : 502).json({
      error: timedOut ? 'backend_timeout' : 'backend_unavailable',
    });
  } finally {
    clearTimeout(timeout);
  }
}

// Exported for tests: the allowlist is a security boundary, so it is asserted
// against the dashboard's real call sites rather than trusted by inspection.
export const __ALLOWED_FOR_TESTS = ALLOWED;
