import Image from 'next/image';
import { useEffect, useState } from 'react';
const fmt=(x,money=false)=>typeof x==='number' ? (money?'$':'')+x.toLocaleString(undefined,{maximumFractionDigits:money?2:0}) : 'Unknown';
const recipes=[
 ['Announce','Event identity, date, venue and tickets. Establish the Facebook event engagement campaign.'],
 ['Show the experience','Alternate brewing footage, people enjoying coffee and the localized festival poster. Keep the paid presence running.'],
 ['Make it local','Feature confirmed roasters and the music lineup. Localize city, venue, date and destination without claiming borrowed footage is from this city.'],
 ['Close','Use countdowns, useful festival information and truthful inventory or pricing reminders. Adjust spend using ticket pacing; retain low-budget support where appropriate.']
];
function ReadyPosts({value}) {
 const [copied,setCopied]=useState('');
 if(!value)return null;
 const current=value.status==='current';
 return <section className="my-8" id="ready-posts">
  <h2 className="text-2xl font-semibold">Posts ready for your review</h2>
  <p className="text-slate-600 mt-2">Real festival photography and finished captions. Download a complete post, review it, and publish from the city’s account.</p>
  {!current&&<p role="status" className="text-amber-800 mt-2">{value.status==='stale'?'These posts need a source refresh before use.':'Preparing the first finished posts.'}</p>}
  <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-5 mt-5">{value.posts?.map(p=><article key={p.id} className="border rounded-xl overflow-hidden bg-white">
   <Image unoptimized src={'/api/proxy/api/intelligence/ready-posts/media/'+p.media_id} alt={p.alt} width={1043} height={857} className="w-full h-64 object-contain bg-slate-100"/>
   <div className="p-5"><h3 className="font-semibold text-lg">{p.festival}</h3><p className="text-sm text-slate-500">{p.event_date} · Awaiting your review</p>
   <p className="whitespace-pre-wrap mt-4">{p.caption}</p>
   <div className="flex flex-wrap gap-4 mt-4">{current&&<a className="bg-blue-700 text-white px-4 py-2 rounded-lg" href={'/api/proxy/api/intelligence/ready-posts/'+p.id+'/download'}>Download complete post</a>}<button className="text-blue-700 underline" onClick={async()=>{try{await navigator.clipboard.writeText(p.caption);setCopied(p.id);}catch{setCopied('failed');}}}>{copied===p.id?'Copied':'Copy caption'}</button></div>
   <details className="mt-4 text-sm"><summary className="cursor-pointer">Source and ticket links</summary><p className="mt-2">{p.selection_basis}</p><p className="mt-2">{p.performance_basis}</p><p className="mt-2">{p.review_note}</p><a className="block text-blue-700 underline mt-2" href={p.source_url} target="_blank" rel="noreferrer">Original photo · {p.source_city}</a>{p.ticket_links.map((u,i)=><a className="block text-blue-700 underline mt-2" key={u} href={u} target="_blank" rel="noreferrer">Ticket session {i+1}</a>)}</details>
   </div></article>)}</div>
  {value.held?.map((p,i)=><p className="text-sm text-amber-800 mt-3" key={i}>{p.festival}: {p.reason}</p>)}
  {copied==='failed'&&<p role="status">Select the caption to copy it manually.</p>}
 </section>;
}

function ActionPacks({value}){
 const [copied,setCopied]=useState('');
 if(!value)return null;
 return <section className="my-8"><h2 className="text-2xl font-semibold">Prepared action packs</h2><p className="text-sm text-slate-600 mt-2">{value.cadence} Last prepared: {value.generated_at||'Pending first maintenance run'} · {value.status}. Preparation only; no posts, emails or ad changes are executed.</p>
 {value.status==='stale'&&<p role="alert" className="text-amber-800">Preparation is stale. Refresh source evidence before using these packs.</p>}
 <div className="space-y-4 mt-4">{value.packs?.map(p=><details className="border rounded-xl p-5" key={p.id}><summary className="cursor-pointer font-semibold">{p.festival} · {p.event_date||'Date pending'} · {p.priority}</summary>
 <p className="mt-3">{p.phase} · {p.days_out==null?'Relative preparation':p.days_out+' days out'} · {p.status}</p>
 <p className="mt-2"><strong>Audience:</strong> {p.audience}</p><p className="text-sm mt-2">{p.crm_preparation}</p>
 {p.crm_candidates?.map(c=><p key={c.segment} className="text-sm mt-1">{c.label}: {fmt(c.candidates)} candidates · {c.status}. {c.excluded_current_buyers!=null?c.excluded_current_buyers+' current buyers excluded.':''}</p>)}
 <p className="text-sm mt-2">Candidate segments can overlap; these are not confirmed reachable recipients.</p>
 <p className="mt-2"><strong>Evidence:</strong> {fmt(p.tickets)} recorded tickets · {fmt(p.candidate_spend,true)} candidate spend · {p.pace_delta_pct==null?'No comparable pacing verdict':p.pace_delta_pct+'% vs '+p.reference_basis}.</p>
 {p.references?.map(r=><p key={r.city+r.date} className="text-sm">{r.city} {r.date}: {fmt(r.same_days_tickets)} tickets at the same days out; {fmt(r.final_tickets)} final.</p>)}
 <p className="text-sm text-slate-500 mt-2">Reference comparisons do not adjust for price, capacity or offer changes and do not establish causal lift.</p>
 {p.warnings?.map((w,i)=><p key={i} className="text-amber-800 text-sm mt-2">{w}</p>)}
 <div className="grid lg:grid-cols-3 gap-3 mt-4">{p.content.map(c=><article key={c.id} className="bg-slate-50 rounded-lg p-4"><h3 className="font-semibold">{c.format} · {c.suggested_date||'Relative sequence'}</h3><p className="whitespace-pre-wrap my-3">{c.caption}</p><button className="text-blue-700 underline" onClick={async()=>{try{await navigator.clipboard.writeText(c.caption);setCopied(c.id);}catch{setCopied('failed');}}}>{copied===c.id?'Copied':'Copy caption'}</button>
 {c.asset&&<p className="mt-3"><a className="text-blue-700 underline" href={c.asset.url} target="_blank" rel="noreferrer">Reuse source from {c.asset.source_city}</a></p>}<p className="text-sm mt-2">{c.asset_instructions}</p>{c.blockers.map((b,i)=><p key={i} className="text-amber-800 text-sm mt-2">{b}</p>)}</article>)}</div>
 {p.email_draft&&<div className="mt-4 p-4 border rounded-lg"><h3 className="font-semibold">Email copy prepared</h3><p className="mt-2">Subject: {p.email_draft.subject}</p><p className="mt-2">{p.email_draft.body}</p><p className="text-sm text-slate-500 mt-2">Copy only. Provider draft population and recipient checks are separate.</p></div>}
 <p className="mt-3 text-sm"><strong>Paid support:</strong> {p.boost_guidance}</p><div className="flex gap-3 mt-3">{p.ticket_links.map((u,i)=><a key={u} className="text-blue-700 underline" href={u} target="_blank" rel="noreferrer">Eventbrite session {i+1}</a>)}</div>
 </details>)}</div>{copied==='failed'&&<p role="status">Clipboard unavailable; select and copy the caption text.</p>}
 </section>;
}
export default function Launches(){
 const [data,setData]=useState(null),[error,setError]=useState(''),[selected,setSelected]=useState('');
 useEffect(()=>{const c=new AbortController();let alive=true;const load=async()=>{try{const r=await fetch('/api/proxy/api/intelligence/launches',{signal:c.signal,cache:'no-store'});if(!r.ok)throw Error('Launch intelligence is unavailable.');const d=await r.json();if(alive){setData(d);setError('');}}catch(e){if(alive&&e.name!=='AbortError')setError(e.message);}};load();const t=setInterval(load,60000);return()=>{alive=false;c.abort();clearInterval(t);};},[]);
 const editions=data?.editions||[];const key=e=>e.city+' '+e.date;const e=editions.find(x=>key(x)===selected)||editions.find(x=>x.city==='New York'&&!x.completed)||editions[0];
 return <main className="max-w-7xl mx-auto px-6 py-8 text-slate-800">
  <a href="/" className="text-blue-700">← Craft Dominant · Ticket pacing</a>
  <h1 className="text-3xl font-bold mt-5">Launch Intelligence</h1>
  <p className="mt-2 text-slate-600">Reconstruct the recipe, localize the content, and compare sales at the same days before the festival.</p>
  {error&&<p role="alert" className="p-4 bg-amber-50">{error}</p>}{!data&&!error&&<p role="status">Loading launch evidence…</p>}
  {data&&<>
   <div className="my-5 p-4 bg-amber-50 rounded-xl text-sm"><strong>Provisional evidence</strong> · {data.coverage}<br/>{data.creative_caution}<br/>Paid history collected: {data.meta_collected_at||'Not available'} · {data.request_errors} recorded request errors. No campaigns are executed from this view.</div>
   <div className="my-4 p-4 bg-slate-50 rounded-xl text-sm"><strong>Learning history</strong> · {data.evidence_history?.snapshots ?? 'Unknown'} saved observations. Latest: {data.evidence_history?.last_observed_at || 'Not recorded yet'}. Status: {data.evidence_history?.status || 'Unavailable'}. These preserve observed sales and creative versions; they do not establish causal lift.</div>
   <ReadyPosts value={data.ready_posts}/>
   <ActionPacks value={data.action_packs}/>
   <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr>{['City / edition','Lifecycle','Event date','Tickets','Gross revenue','Candidate ad spend'].map(x=><th className="p-3 border-b" key={x}>{x}</th>)}</tr></thead><tbody>{editions.map(x=><tr key={key(x)} className="border-b"><td className="p-3"><button className="text-blue-700 underline" onClick={()=>setSelected(key(x))}>{x.city}{x.edition?' · Year '+x.edition:''}</button></td><td className="p-3">{x.lifecycle} · {x.completed?'completed':x.days_out+' days out'}</td><td className="p-3">{x.date} · {x.event_days} day{x.event_days===1?'':'s'}</td><td className="p-3">{fmt(x.tickets)}</td><td className="p-3">{fmt(x.revenue,true)}</td><td className="p-3">{fmt(x.spend,true)}</td></tr>)}</tbody></table></div>
   {e&&<section className="mt-8"><h2 className="text-2xl font-semibold">{e.city} · {e.date}</h2><a className="text-blue-700 underline" href={e.profile} target="_blank" rel="noreferrer">Instagram content reference</a>
    {e.warnings.map((w,i)=><p key={i} className="text-amber-800 text-sm mt-2">{w}</p>)}
    <div className="grid md:grid-cols-3 gap-3 mt-4">{e.phases.map(p=><div key={p.label} className="border rounded-xl p-4"><h3 className="font-semibold">{p.label}</h3><p>{p.not_reached?'Phase not reached':`${fmt(p.tickets)} tickets · ${fmt(p.spend,true)} candidate spend`}</p>{(p.unknown_tickets>0||p.unknown_revenue>0)&&<p className="text-amber-800">Includes missing order values</p>}</div>)}</div>
    {e.launch_references?.length>0&&<div className="mt-5 p-4 bg-blue-50 rounded-xl"><h3 className="font-semibold">Recent first launches at {Math.max(0,e.days_out)} days out</h3><p className="text-sm">Reference cases, not a sales forecast. Compare capacity, event duration and pricing before judging performance. Returning festivals also have their own customer history.</p>{e.launch_references.map(p=><p key={p.city+p.date} className="mt-2">{p.city} {p.date.slice(0,4)}: {fmt(p.tickets_at_same_days_out)} tickets at this point; {fmt(p.final_tickets)} final recorded tickets ({(p.fraction_sold*100).toFixed(1)}% sold).</p>)}</div>}
    <details className="mt-5"><summary className="cursor-pointer font-semibold">Reviewed Instagram reference inventory ({e.instagram_references?.length||0} posts)</summary><p className="text-sm text-slate-500">Profile-wide references across editions. Dates shown are observed Instagram descriptions; they are not proof of when paid delivery started.</p>{e.instagram_references?.map(r=><p key={r.url} className="py-2 border-b text-sm"><a className="text-blue-700 underline" href={r.url} target="_blank" rel="noreferrer">{r.description||'Open referenced post'}</a></p>)}</details>
    <details className="mt-5"><summary className="cursor-pointer font-semibold">Paid sequence and current creative ({e.campaigns.length} campaigns)</summary>{e.campaigns.map((c,i)=><div key={i} className="border-b py-4"><h4 className="font-semibold">{c.name}</h4><p className="text-sm">{c.first_delivery} → {c.last_delivery} · {fmt(c.spend,true)} · {c.objective}</p>{c.creatives.map((a,j)=><div key={j} className="mt-2 text-sm"><p>{a.body}</p>{a.url&&<a className="text-blue-700 underline" target="_blank" rel="noreferrer" href={a.url}>View current creative · {a.video?'video':'post'}</a>}</div>)}</div>)}</details>
   </section>}
   <section className="mt-10"><h2 className="text-2xl font-semibold">Houston and NYC: reusable coffee launch playbook</h2><p className="mt-2">Houston’s exact date is not configured here. Use the relative sequence below; NYC is a relaunch after a long gap. Budget changes require an execution decision.</p><div className="grid md:grid-cols-2 gap-4 mt-4">{recipes.map(([title,body])=><article className="border rounded-xl p-5" key={title}><h3 className="font-bold">{title}</h3><p className="mt-2">{body}</p></article>)}</div><p className="text-sm text-slate-500 mt-4">{data.profile_review} Original post dates and creative changes need separate evidence. The sales comparison refreshes as stored orders change; paid history refreshes daily through the existing maintenance cycle.</p></section>
  </>}
 </main>;
}


