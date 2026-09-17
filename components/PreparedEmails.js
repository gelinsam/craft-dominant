import React, { useEffect, useState } from 'react';

export default function PreparedEmails() {
  const [data,setData]=useState(null);
  const [failed,setFailed]=useState(false);
  useEffect(()=>{
    let active=true;
    const controller=new AbortController();
    async function refresh(){
      try {
        const response=await fetch('/api/proxy/api/intelligence/provider-drafts',{cache:'no-store',signal:controller.signal});
        if(!response.ok) throw new Error('unavailable');
        const result=await response.json();
        if(active){setData(result);setFailed(false);}
      } catch(e){if(active && e.name!=='AbortError')setFailed(true);}
    }
    refresh();const timer=setInterval(refresh,60000);
    return()=>{active=false;controller.abort();clearInterval(timer);};
  },[]);
  const drafts=data?.drafts || [];
  const ready=drafts.filter(d=>d.state==='ready');
  const scheduled=drafts.filter(d=>['covered_by_scheduled','schedule','sending','sent'].includes(d.state));
  const others=drafts.filter(d=>d.state!=='ready' && !scheduled.includes(d));
  function card(d){return <article key={d.key} className="rounded-xl border border-slate-200 bg-white p-5">
    <div className="flex items-center justify-between gap-3"><h3 className="font-semibold text-slate-900">{d.event_name}</h3><span className="text-xs text-slate-500">Mailchimp</span></div>
    <p className="mt-3 text-lg font-semibold">{d.subject || d.purpose}</p>
    <p className="mt-2 text-sm text-slate-600">{d.purpose}</p>
    {d.state==='ready' && <p className="mt-2 text-sm font-medium text-emerald-800">{d.audience_count.toLocaleString()} recipients attached · ready to review</p>}
    {d.state!=='ready' && <p className="mt-2 text-sm text-slate-600">{['schedule','sending','sent'].includes(d.state)?'Already '+({schedule:'scheduled',sending:'sending',sent:'sent'}[d.state]):d.reason || 'Preparing the email and audience.'}</p>}
    {d.url && /^https:\/\/us\d+\.admin\.mailchimp\.com\/campaigns\//.test(d.url) && <a href={d.url} target="_blank" rel="noopener noreferrer" className="mt-4 inline-block rounded-lg bg-blue-700 px-4 py-2 text-sm font-semibold text-white">{d.state==='ready'?'Review email in Mailchimp':'Open saved campaign'} ↗</a>}
    {d.rationale && <details className="mt-3 text-xs text-slate-500"><summary className="cursor-pointer">Why this email</summary><p className="mt-2">{d.rationale}</p>{d.verified_at && <p className="mt-2">Audience checked {new Date(d.verified_at).toLocaleString()}</p>}</details>}
  </article>;}
  return <section aria-label="Prepared emails">
    <h2 className="text-xl font-semibold">Emails ready for your review</h2>
    <p className="mt-1 text-sm text-slate-600">Copy and recipients saved in Mailchimp. Open, review, and choose when to send.</p>
    <p className="mt-2 flex gap-4 text-sm"><a className="text-blue-700" href="https://us16.admin.mailchimp.com/campaigns" target="_blank" rel="noopener noreferrer">All Mailchimp campaigns ↗</a><a className="text-blue-700" href="https://www.eventbrite.com/organizations/campaigns/email" target="_blank" rel="noopener noreferrer">Eventbrite campaigns ↗</a></p>
    {failed && <p role="alert" className="mt-3 text-sm text-amber-800">Live draft status is temporarily unavailable.</p>}
    {!data && !failed && <p className="mt-4 text-sm text-slate-500">Checking saved campaigns…</p>}
    {!failed && <div className="mt-4 grid gap-4 md:grid-cols-2">{ready.map(card)}</div>}
    {data && !ready.length && !failed && <p className="mt-4 text-sm text-slate-600">No additional email is ready for review. Scheduled campaigns and any preparation issues are shown below.</p>}
    {!!scheduled.length && <div className="mt-6"><h3 className="font-semibold">Already scheduled</h3><div className="mt-3 grid gap-4 md:grid-cols-2">{scheduled.map(card)}</div></div>}
    {!!others.length && <details className="mt-5"><summary className="cursor-pointer text-sm font-medium">Preparing and existing campaigns ({others.length})</summary><div className="mt-3 grid gap-4 md:grid-cols-2">{others.map(card)}</div></details>}
  </section>;
}
