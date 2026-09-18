// Presentation only: opportunity ranking remains owned by Craft's backend.
export function summarizeCommand(data) {
  if (!data || !Array.isArray(data.opportunities) || !data.data_quality || !data.generated_at) {
    throw new Error('Intelligence response is incomplete');
  }
  return {
    opportunities: data.opportunities,
    actions: Array.isArray(data.actions) ? data.actions : [],
    checks: Array.isArray(data.operational_checks) ? data.operational_checks : [],
    failures: Array.isArray(data.action_failures) ? data.action_failures : [],
    warnings: Array.isArray(data.data_quality.warnings) ? data.data_quality.warnings : [],
    zeroTrusted: data.data_quality.zero_is_trustworthy === true,
    generatedAt: data.generated_at,
  };
}

export function draftPresentation(draft, now = Date.now()) {
  const verified = Date.parse(draft.verified_at);
  const stale = !Number.isFinite(verified) || verified > now || now - verified > 86400000;
  if (draft.state === 'blocked') return { label: 'Audience blocked', tone: 'amber', stale };
  if (draft.state === 'scheduled') return { label: 'Scheduled when last checked', tone: 'blue', stale };
  return { label: stale ? 'Draft • audience refresh needed' : 'Draft • review before scheduling', tone: 'blue', stale };
}

export function campaignUrl(id) {
  if (!/^\d+$/.test(String(id))) throw new Error('Invalid campaign identifier');
  return `https://www.eventbrite.com/organizations/campaigns/email/${id}`;
}

/** One bounded read at a time; dashboard timestamp changes do not restart it. */
export function startAttentionPolling({
  onChange, fetchImpl = fetch,
  isVisible = () => document.visibilityState === 'visible',
  schedule = setTimeout, cancel = clearTimeout,
}) {
  let stopped = false, inFlight = false, next, deadline, controller;
  const queue = () => {
    next = schedule(() => {
      if (isVisible()) refresh();
      else queue();
    }, 60000);
  };
  async function refresh() {
    if (stopped || inFlight) return;
    cancel(next);
    inFlight = true;
    controller = new AbortController();
    const signal = controller.signal;
    deadline = schedule(() => controller.abort(), 55000);
    onChange({ loading: true, error: false, summary: null });
    try {
      const response = await fetchImpl('/api/proxy/api/intelligence/action-plan', {
        cache: 'no-store', signal,
      });
      if (!response.ok) throw new Error('Intelligence unavailable');
      const summary = summarizeCommand(await response.json());
      if (!stopped && !signal.aborted) onChange({ summary });
    } catch {
      if (!stopped) onChange({ error: true });
    } finally {
      cancel(deadline);
      inFlight = false;
      if (!stopped) {
        onChange({ loading: false });
        queue();
      }
    }
  }
  refresh();
  return {
    refresh,
    stop() {
      stopped = true;
      cancel(next);
      cancel(deadline);
      controller?.abort();
    },
  };
}
