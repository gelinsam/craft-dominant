import React, { useEffect, useState } from 'react';
import drafts from '../data/festival-drafts.json';
import CampaignIntelligence from './CampaignIntelligence';
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
    fetch('/api/proxy/api/intelligence/action-plan', { cache: 'no-store', signal: controller.signal })
      .then(async res => {
        if (!res.ok) throw new Error('Intelligence unavailable');
        return summarizeCommand(await res.json());
      })
      .then(result => { if (active) setSummary(result); })
      .catch(() => { if (active) setError(true); })
      .finally(() => { clearTimeout(timeout); if (active) setLoading(false); });
    return () => { active = false; clearTimeout(timeout); controller.abort(); };
  }, [refreshKey, retry]);

  useEffect(() => { const timer=setInterval(() => { if(document.visibilityState==='visible') setRetry(n=>n+1); },60000); return () => clearInterval(timer); }, []);
  const opportunities = summary?.opportunities || [];
  const filtered = drafts.filter(d => d.event_name.toLowerCase().includes(filter.toLowerCase()));
  const message = loading ? 'Checking opportunities…' : error ? 'Recommendations unavailable — pacing remains below'
    : opportunities.length ? `${opportunities.length} pacing ${opportunities.length === 1 ? 'opportunity' : 'opportunities'}`
    : summary?.zeroTrusted ? 'No pacing opportunities detected' : 'Pacing assessment incomplete';

  return <section aria-label="Needs attention" className="mb-6 rounded-xl border border-slate-200 bg-white">
    <div className="flex flex-wrap items-center justify-between gap-3 px-4 py-3">
      <div><span className="font-semibold text-slate-900">Needs attention</span><span role="status" className="ml-3 text-sm text-slate-600">{message}</span></div>
      <div className="flex items-center gap-3 text-sm">
        <span className="text-slate-600">Draft-only · sends remain your decision</span>
        {!expanded && <button onClick={onOpen} className="font-semibold text-blue-700">Review actions →</button>}
      </div>
    </div>
    {expanded && <div className="border-t p-4 sm:p-6 space-y-7">
      <div>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-lg font-semibold">What needs attention now</h2>
          <div className="flex gap-4 text-sm"><button disabled={loading} onClick={() => setRetry(n => n + 1)} className="text-blue-700 disabled:text-gray-400">Refresh recommendations</button><a href="/command" className="font-medium text-blue-700">Open Command Center →</a></div>
        </div>
        <p className="mt-1 text-sm text-slate-500">Ranked by Craft using its existing diagnosis and latest stored sales data, with one action per festival edition. Refreshes every minute while open. Use Refresh Data above to sync sales. Recovery estimates are modeled, not measured results.</p>
        {error && <p role="alert" className="mt-3 rounded-lg bg-amber-50 p-3 text-amber-900">Recommendations could not be loaded. Try refreshing; this does not mean every festival is on track.</p>}
        {summary && <>
          <p className="mt-2 text-xs text-slate-500">Analysis generated: {summary.generatedAt}</p>
          {summary.warnings.map((w, i) => <p key={i} className="mt-2 rounded-lg bg-amber-50 p-3 text-sm text-amber-900">{w}</p>)}
          {!opportunities.length && <p className="mt-3 text-sm text-slate-600">{summary.zeroTrusted ? 'No pacing opportunities detected in the current data.' : 'Missing or incomplete history prevents a reliable all-clear. Review data quality in Command Center.'}</p>}
          <div className="mt-4 grid gap-3 md:grid-cols-2">
            {summary.actions.map((a,index) => <article key={a.action_id} className="rounded-lg border p-4">
              <p className="text-xs font-semibold text-indigo-700">Priority {index+1} · {a.days_until} days out</p>
              <h3 className="mt-1 font-semibold">{a.event_name}</h3>
              <p className="mt-2 font-medium text-slate-800">{a.title}</p>
              <p className="mt-2 text-sm text-slate-600">{a.rationale}</p>
              {!a.data_current && <p className="mt-2 text-sm text-amber-800">Sales evidence needs a freshness or integrity check before acting.</p>}
              {!!a.related_pacing_views.length && <p className="mt-2 text-xs text-slate-500">Also covers {a.related_pacing_views.join(', ')}. Prepare one coordinated campaign for this edition.</p>}
              <p className="mt-2 text-xs text-slate-500">{a.priority_basis}</p>
              <details className="mt-3 text-sm"><summary className="cursor-pointer font-medium">Channels, alternatives and existing work</summary>
                <p className="mt-2">Mailchimp: {a.channel_readiness.mailchimp.replaceAll('_',' ')}</p>
                <p>Eventbrite: browser draft and recipient verification required</p>
                <p>Meta: {a.channel_readiness.meta.replaceAll('_',' ')}</p>
                {a.options.filter(o=>o.intervention_type!==a.recommended_intervention).map(o=><p key={o.intervention_type} className="mt-2"><strong>{o.label}:</strong> {o.rationale}</p>)}
                {!!a.existing_interventions.length && <p className="mt-2 text-amber-800">Existing work: {a.existing_interventions.map(i=>i.status.replaceAll('_',' ')).join(', ')}. Review it before preparing a duplicate.</p>}
                {a.missing_data.map((m,i)=><p key={i} className="mt-2 text-amber-800">{m}</p>)}
              </details>
              <a href="/command" className="mt-3 inline-block text-sm font-medium text-blue-700">Review and prepare in Command Center →</a>
            </article>)}
          </div>
          {!!summary.failures.length && <p className="mt-3 text-sm text-amber-800">{summary.failures.length} opportunities need a diagnosis or festival-scope check. They have not been marked resolved.</p>}
          <div className="mt-5 rounded-lg bg-slate-50 p-4"><h3 className="font-semibold">System checks</h3><p className="mt-1 text-xs text-slate-500">Rechecked every minute while this view is open. A current page does not imply current provider data.</p><div className="mt-3 grid gap-3 md:grid-cols-2">{summary.checks.map(c=><div key={c.id}><p className="text-sm font-medium">{c.label}: {c.status.replaceAll('_',' ')}</p><p className="text-xs text-slate-600">{c.action}</p>{c.observed_at && <p className="text-xs text-slate-500">Observed: {c.observed_at}</p>}</div>)}</div></div>
        </>}
      </div>
      <CampaignIntelligence />
      <div>
        <h2 className="text-lg font-semibold">Festival email drafts</h2>
        <p className="mt-1 text-sm text-slate-600">Super-spreader drafts last checked September 16, 2026. These are saved audience snapshots; status and counts are not live Eventbrite data. Check each draft before scheduling in Eventbrite.</p>
        <p className="mt-2 text-sm text-slate-500">Historical email evidence appears above. Results for these saved drafts are not yet connected; a draft is not evidence of a send or a sale.</p>
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
