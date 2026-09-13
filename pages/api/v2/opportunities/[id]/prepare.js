const DEFAULT_BACKEND = 'https://craft-dominant-production.up.railway.app';

export default async function handler(req, res) {
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'method_not_allowed' });
  }

  const { id } = req.query;
  if (!id) {
    return res.status(400).json({ error: 'missing_opportunity_id' });
  }

  const backend = process.env.CRAFT_BACKEND_URL || process.env.NEXT_PUBLIC_API_BASE || DEFAULT_BACKEND;
  const commandKey = process.env.COMMAND_API_KEY;

  if (!commandKey) {
    return res.status(503).json({
      error: 'command_api_not_configured',
      message: 'COMMAND_API_KEY is not configured on the server.',
    });
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 30000);

  try {
    const response = await fetch(
      `${backend}/api/v2/opportunities/${encodeURIComponent(id)}/prepare`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${commandKey}`,
          'Content-Type': 'application/json',
          Accept: 'application/json',
        },
        signal: controller.signal,
      },
    );

    const body = await response.json().catch(() => ({ error: 'invalid_backend_response' }));
    res.setHeader('Cache-Control', 'private, no-store, max-age=0');
    return res.status(response.status).json(body);
  } catch (error) {
    const timedOut = error?.name === 'AbortError';
    return res.status(timedOut ? 504 : 502).json({
      error: timedOut ? 'backend_timeout' : 'backend_unavailable',
    });
  } finally {
    clearTimeout(timeout);
  }
}
