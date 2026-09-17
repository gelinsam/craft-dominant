import React, {useEffect, useState} from 'react';
import {buildIntelligence} from '../lib/campaign-intelligence.mjs';

const percent = n => typeof n === 'number' ? `${n.toFixed(2)}%` : 'Unknown';
const label = s => s[0].toUpperCase()+s.slice(1);

function Evidence({row}) {
  return <div className="rounded-lg border border-slate-200 bg-white p-3 text-sm">
    <p className="font-medium text-slate-900">“{row.subject}”</p>
    <p className="mt-1 text-xs text-slate-500">{row.sent_at ? row.sent_at.slice(0,10) : `${row.send_label || 'Send date unknown'} · year unverified`} · {row.audience}</p>
    <p className="mt-2">{row.delivered?.toLocaleString('en-US') ?? 'Unknown'} delivered · {percent(row.click_rate)} clicks · {percent(row.unsubscribe_rate)} unsubscribed</p>
    {row.provider === 'eventbrite' && <p className="mt-1 text-slate-600">Provider reports {row.reported_tickets ?? 'unknown'} tickets / {row.reported_revenue == null ? 'unknown revenue' : `$${row.reported_revenue.toLocaleString('en-US')}`} · {percent(row.bounce_rate)} bounced</p>}
    <p className="mt-1 text-xs text-slate-500">{row.days_out === null ? 'Exact days before event unverified' : `${row.days_out} days before event`} · {row.content_verified ? 'Content reviewed' : 'Body not recovered'} · {row.category_basis === 'linked_event' || row.category_basis === 'preview_event' ? 'Promoted event verified' : 'Category inferred from campaign branding'}</p>
    {row.note && <p className="mt-2 text-xs text-amber-800">{row.note}</p>}
    {row.source_url && <a className="mt-2 inline-block text-blue-700" href={row.source_url} target="_blank" rel="noopener noreferrer">Open source campaign ↗</a>}
  </div>;
}

export default function CampaignIntelligence() {
  const [category,setCategory] = useState('coffee');
  const [report,setReport] = useState(null);
  const [failed,setFailed] = useState(false);
  const [retry,setRetry] = useState(0);
  useEffect(()=>{
    const controller = new AbortController();
    const timeout = setTimeout(()=>controller.abort(),15000);
    let active = true;
    setReport(null);setFailed(false);
    fetch('/api/proxy/api/intelligence/campaign-evidence',{cache:'no-store',signal:controller.signal})
      .then(async res=>{if(!res.ok) throw new Error('History unavailable');const data=await res.json();if(data.version!==1 || !Array.isArray(data.records)) throw new Error('Invalid history');return {...buildIntelligence(data.records,new Date().toISOString().slice(0,10)),feedback:data.feedback};})
      .then(data=>{if(active)setReport(data);})
      .catch(()=>{if(active)setFailed(true);})
      .finally(()=>clearTimeout(timeout));
    return ()=>{active=false;controller.abort();clearTimeout(timeout);};
  },[retry]);
  useEffect(()=>{const timer=setInterval(()=>{if(document.visibilityState==='visible')setRetry(n=>n+1);},60000);return ()=>clearInterval(timer);},[]);
  if (!report) return <section aria-label="Campaign intelligence" className="rounded-xl border p-4"><h2 className="text-lg font-semibold">Past email performance</h2><p role="status" className="mt-2 text-sm text-slate-600">{failed?'Campaign evidence is unavailable. This is not a finding that no patterns exist.':'Loading campaign evidence…'}</p>{failed && <button onClick={()=>setRetry(n=>n+1)} className="mt-2 text-sm text-blue-700">Retry campaign evidence</button>}</section>;
  const c = report.categories.find(r=>r.category===category);
  return <section aria-label="Campaign intelligence" className="rounded-xl border border-indigo-200 bg-indigo-50/40 p-4 sm:p-5">
    <p role="status" className="mb-3 text-xs text-slate-600">Mailchimp refresh: {report.feedback?.status?.replaceAll('_',' ') || 'unknown'} · Last successful check: {report.feedback?.last_success_at || 'not yet verified'} · Eventbrite history remains partial.</p>
    <div className="flex flex-wrap items-center justify-between gap-2"><h2 className="text-lg font-semibold text-slate-900">Past email performance</h2><span className="rounded-full bg-white px-3 py-1 text-xs font-medium text-indigo-800">Provisional intelligence</span></div>
    <p className="mt-2 text-sm text-slate-600">Built from {report.mailchimp} Mailchimp reports and {report.eventbrite} recovered Eventbrite campaigns. Comparison recalculated {report.as_of}; Eventbrite history remains partial. These observations support draft preparation; they do not prove sales lift.</p>
    <div className="mt-4 flex flex-wrap gap-2" aria-label="Festival category">{report.categories.map(r=><button key={r.category} aria-pressed={category===r.category} onClick={()=>setCategory(r.category)} className={`rounded-lg border px-4 py-2 text-sm font-medium ${category===r.category?'border-indigo-700 bg-indigo-700 text-white':'border-slate-200 bg-white text-slate-700'}`}>{label(r.category)}</button>)}</div>
    <p className="mt-3 text-sm text-slate-700">{c.eligible} comparable Mailchimp sends · typical click rate {percent(c.baseline_click_rate)} · {c.eventbrite} separate Eventbrite observations</p>
    <p className="mt-1 text-xs text-slate-500">Mailchimp comparison: last three years, at least 500 delivered, at least 72 hours old, known clicks and reviewed content. Rates use unique clickers ÷ estimated delivered. Audience, offer and timing differences can explain results.</p>
    <div className="mt-4 grid gap-4 lg:grid-cols-2">
      {c.groups.slice(0,2).map((g,i)=><article key={`${category}:${g.angle}`} className="rounded-xl border bg-white p-4">
        <p className="text-xs font-semibold text-indigo-700">{'Historical theme'} · {g.evidence}</p>
        <h3 className="mt-2 text-lg font-semibold">{g.label}</h3>
        <p className="mt-3 text-sm font-medium">{percent(g.median_click_rate)} median clicks · {percent(g.median_unsubscribe_rate)} median unsubscribes</p>
        <p className="mt-1 text-xs text-slate-500">{g.campaigns} sends · {g.creative_count} distinct subjects · {g.send_days} send dates. Repeated audiences may overlap.</p>
        <p className="mt-2 text-xs text-slate-600">{g.observed_timing ? `Known timing: ${g.observed_timing.min}–${g.observed_timing.max} days before the event (${g.observed_timing.count} sends). This is an observed range, not an optimal window.` : 'Timing remains a test decision; there is no verified send window for this group.'}</p>
        <details className="mt-3"><summary className="cursor-pointer text-sm font-medium text-blue-700">See stronger, typical and weaker examples</summary><div className="mt-3 space-y-2">{g.examples.map(r=><Evidence key={r.id} row={r}/>)}</div></details>
      </article>)}
      {!c.groups.length && <p className="text-sm">No qualifying Mailchimp comparison is available yet.</p>}
    </div>
    <p className="mt-4 text-sm text-slate-700">{c.audience_note}</p>
    {!!c.eventbrite_examples.length && <details className="mt-4"><summary className="cursor-pointer text-sm font-semibold text-blue-700">Eventbrite evidence: {c.eventbrite} recovered campaigns</summary><p className="mt-2 text-xs text-slate-600">Eventbrite rates are shown as reported and are not pooled with Mailchimp. Tickets/revenue are provider attribution, not incremental lift; zeros do not establish zero sales. Unknown send years prevent exact timing comparisons.</p><div className="mt-3 grid gap-3 md:grid-cols-2">{c.eventbrite_examples.map(r=><Evidence key={r.id} row={r}/>)}</div></details>}
    <details className="mt-4 text-sm text-slate-600"><summary className="cursor-pointer font-medium">How this changes with new evidence</summary><p className="mt-2">Mailchimp reports from the last 90 days refresh every six hours. This view recomputes category comparisons from the latest stored observations and refreshes every minute while open. Older Mailchimp and Eventbrite observations remain dated imports; new campaigns need content and category verification before creative ranking.</p><p className="mt-2">Ideas are grouped from subject and reviewed message wording. Ranking uses median click rate, pulled toward the category median for small groups. It does not optimize revenue, prove that copy caused clicks, or learn an audience’s consent. Unsubscribes are displayed as a tradeoff. Open rates are not used to rank.</p><p className="mt-2">{c.mailchimp-c.eligible} of {c.mailchimp} Mailchimp records in this category are outside the comparison: {Object.entries(c.exclusions).map(([k,v])=>`${v} ${k.replaceAll('_',' ')}`).join('; ') || 'none'}.</p></details>
  </section>;
}
