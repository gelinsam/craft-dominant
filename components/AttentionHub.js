import React, { useEffect, useState } from 'react';
import drafts from '../data/festival-drafts.json';
import { summarizeCommand, draftPresentation, campaignUrl } from '../lib/attention.mjs';

export default function AttentionHub({ expanded, onOpen, refreshKey }) {
  const [summary, setSummary] = useState(null);
  const [error, setError] = useState(false);
  const [loading, setLoading] = useState(true);
  const [retry, setRetry] = useState(0);
  const [filter, setFilter] = useState('');
  useEffect(() => {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 15000);
    let active = true;
    setLoading(true);
    setError(false);
    // A failed refresh must not leave old recommendations looking current.
    setSummary(null);
    fetch('/api/command', { cache: 'no-store', signal: controller.signal })
      .then(async res => {
        if (!res.ok) throw new Error('Intelligence unavailable');
        return summarizeCommand(await res.json());
      })
      .then(result => { if (active) setSummary(result); })
      .catch(() => { if (active) setError(true); })
      .finally(() => { clearTimeout(timeout); if (active) setLoading(false); });
    return () => { active = false; clearTimeout(timeout); controller.abort(); };
  }, [refreshKey, retry]);

  const opportunities = summary?.opportunities || [];
  const filtered = drafts.filter(d => d.event_name.toLowerCase().includes(filter.toLowerCase()));
  const message = loading ? 'Checking opportunities…' : error ? 'Recommendations unavailable — pacing remains below'
    : opportunities.length ? `${opportunities.length} pacing ${opportunities.length === 1 ? 'opportunity' : 'opportunities'}`
    : summary?.zeroTrusted ? 'No pacing opportunities detected' : 'Pacing assessment incomplete';

  return <section aria-label="Needs attention" className="mb-6 rounded-xl border border-slate-200 bg-white">
    <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
      <div><span className="font-semibold text-slate-900">Needs attention</span><span role="status" className="ml-3 text-sm text-slate-600">{message}</span></div>
      <div className="flex items-center gap-3 text-sm">
        <span className="text-slate-600">13 saved drafts · NYC audience blocked</span>
        {!expanded && <button onClick={onOpen} className="font-semibold text-blue-700">Review actions →</button>}
      </div>
    </div>
    {expanded && <div className="border-t p-4 sm:p-6 space-y-7">
      <div>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-lg font-semibold">What needs attention now</h2>
          <div className="flex gap-4 text-sm"><button disabled={loading} onClick={() => setRetry(n => n + 1)} className="text-blue-700 disabled:text-gray-400">Refresh recommendations</button><a href="/command" className="font-medium text-blue-700">Open Command Center →</a></div>
        </div>
        <p className="mt-1 text-sm text-slate-500">Ranked by Craft using the latest stored sales data. Refresh Data above to sync sales. Recovery estimates are modeled, not measured results.</p>
        {error && <p role="alert" className="mt-3 rounded-lg bg-amber-50 p-3 text-amber-900">Recommendations could not be loaded. Try refreshing; this does not mean every festival is on track.</p>}
        {summary && <>
          <p className="mt-2 text-xs text-slate-500">Analysis generated: {summary.generatedAt}</p>
          {summary.warnings.map((w, i) => <p key={i} className="mt-2 rounded-lg bg-amber-50 p-3 text-sm text-amber-900">{w}</p>)}
          {!opportunities.length && <p className="mt-3 text-sm text-slate-600">{summary.zeroTrusted ? 'No pacing opportunities detected in the current data.' : 'Missing or incomplete history prevents a reliable all-clear. Review data quality in Command Center.'}</p>}
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            {opportunities.slice(0, 6).map(o => <article key={o.opportunity_id} className="rounded-lg border p-4">
              <h3 className="font-semibold">{o.event_name}</h3>
              <p className="mt-1 text-sm text-slate-700">{o.title}</p>
              <p className="mt-2 text-sm text-slate-500">{o.rationale}</p>
              <p className="mt-2 text-sm">{o.recommended_action}</p>
              <a href="/command" className="mt-3 inline-block text-sm font-medium text-blue-700">Review evidence & next step →</a>
            </article>)}
          </div>
          {opportunities.length > 6 && <a href="/command" className="mt-3 inline-block text-sm text-blue-700">See all {opportunities.length} opportunities →</a>}
        </>}
      </div>
      <div>
        <h2 className="text-lg font-semibold">Festival email drafts</h2>
        <p className="mt-1 text-sm text-slate-600">Super-spreader drafts last checked September 16, 2026. These are saved audience snapshots; status and counts are not live Eventbrite data. Check each draft before scheduling in Eventbrite.</p>
        <p className="mt-2 text-sm text-slate-500">Campaign results are not yet connected. A saved draft is not evidence of a send or a sale.</p>
        <label className="mt-4 block text-sm font-medium">Find a festival<input type="search" value={filter} onChange={e => setFilter(e.target.value)} placeholder="City or festival name" className="mt-1 block w-full max-w-sm rounded-lg border px-3 py-2 font-normal" /></label>
        <div className="mt-4 grid gap-3 md:grid-cols-2 xl:grid-cols-3">
          {filtered.map(d => {
            const status = draftPresentation(d);
            return <article key={d.campaign_id} className="rounded-lg border p-4">
              <span className={`text-xs font-semibold ${status.tone === 'amber' ? 'text-amber-800' : 'text-blue-700'}`}>{status.label}</span>
              <h3 className="mt-2 font-semibold">{d.event_name}</h3>
              <p className="mt-1 text-sm text-slate-600">{d.active_recipients === null ? 'Recipient count not recorded' : `${d.active_recipients.toLocaleString()} active recipients when checked`}</p>
              <p className="mt-2 text-sm text-slate-500">{d.note}</p>
              <a href={campaignUrl(d.campaign_id)} target="_blank" rel="noopener noreferrer" className="mt-3 inline-block text-sm font-medium text-blue-700">Open Eventbrite {d.state === 'scheduled' ? 'campaign' : 'draft'} ↗</a>
            </article>;
          })}
        </div>
        {!filtered.length && <p className="mt-3 text-sm text-slate-600">No saved drafts match that festival.</p>}
      </div>
    </div>}
  </section>;
}
