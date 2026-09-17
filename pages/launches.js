import { useEffect, useState } from 'react';
const fmt=(x,money=false)=>typeof x==='number' ? (money?'$':'')+x.toLocaleString(undefined,{maximumFractionDigits:money?2:0}) : 'Unknown';
const recipes=[
 ['Announce','Event identity, date, venue and tickets. Establish the Facebook event engagement campaign.'],
 ['Show the experience','Alternate brewing footage, people enjoying coffee and the localized festival poster. Keep the paid presence running.'],
 ['Make it local','Feature confirmed roasters and the music lineup. Localize city, venue, date and destination without claiming borrowed footage is from this city.'],
 ['Close','Use countdowns, useful festival information and truthful inventory or pricing reminders. Adjust spend using ticket pacing; retain low-budget support where appropriate.']
];
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
   <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr>{['City / edition','Lifecycle','Event date','Tickets','Gross revenue','Candidate ad spend'].map(x=><th className="p-3 border-b" key={x}>{x}</th>)}</tr></thead><tbody>{editions.map(x=><tr key={key(x)} className="border-b"><td className="p-3"><button className="text-blue-700 underline" onClick={()=>setSelected(key(x))}>{x.city}{x.edition?' · Year '+x.edition:''}</button></td><td className="p-3">{x.lifecycle} · {x.completed?'completed':x.days_out+' days out'}</td><td className="p-3">{x.date}</td><td className="p-3">{fmt(x.tickets)}</td><td className="p-3">{fmt(x.revenue,true)}</td><td className="p-3">{fmt(x.spend,true)}</td></tr>)}</tbody></table></div>
   {e&&<section className="mt-8"><h2 className="text-2xl font-semibold">{e.city} · {e.date}</h2><a className="text-blue-700 underline" href={e.profile} target="_blank" rel="noreferrer">Instagram content reference</a>
    {e.warnings.map((w,i)=><p key={i} className="text-amber-800 text-sm mt-2">{w}</p>)}
    <div className="grid md:grid-cols-3 gap-3 mt-4">{e.phases.map(p=><div key={p.label} className="border rounded-xl p-4"><h3 className="font-semibold">{p.label}</h3><p>{fmt(p.tickets)} tickets · {fmt(p.spend,true)} candidate spend</p>{(p.unknown_tickets>0||p.unknown_revenue>0)&&<p className="text-amber-800">Includes missing order values</p>}</div>)}</div>
    {e.launch_references?.length>0&&<div className="mt-5 p-4 bg-blue-50 rounded-xl"><h3 className="font-semibold">Recent first launches at {Math.max(0,e.days_out)} days out</h3><p className="text-sm">Reference cases, not a sales forecast. Returning festivals also have their own customer history.</p>{e.launch_references.map(p=><p key={p.city+p.date} className="mt-2">{p.city} {p.date.slice(0,4)}: {fmt(p.tickets_at_same_days_out)} tickets at this point; {fmt(p.final_tickets)} final recorded tickets ({(p.fraction_sold*100).toFixed(1)}% sold).</p>)}</div>}
    <details className="mt-5"><summary className="cursor-pointer font-semibold">Paid sequence and current creative ({e.campaigns.length} campaigns)</summary>{e.campaigns.map((c,i)=><div key={i} className="border-b py-4"><h4 className="font-semibold">{c.name}</h4><p className="text-sm">{c.first_delivery} → {c.last_delivery} · {fmt(c.spend,true)} · {c.objective}</p>{c.creatives.map((a,j)=><div key={j} className="mt-2 text-sm"><p>{a.body}</p>{a.url&&<a className="text-blue-700 underline" target="_blank" rel="noreferrer" href={a.url}>View current creative · {a.video?'video':'post'}</a>}</div>)}</div>)}</details>
   </section>}
   <section className="mt-10"><h2 className="text-2xl font-semibold">Houston and NYC: reusable coffee launch playbook</h2><p className="mt-2">Houston’s exact date is not configured here. Use the relative sequence below; NYC is a relaunch after a long gap. Budget changes require an execution decision.</p><div className="grid md:grid-cols-2 gap-4 mt-4">{recipes.map(([title,body])=><article className="border rounded-xl p-5" key={title}><h3 className="font-bold">{title}</h3><p className="mt-2">{body}</p></article>)}</div><p className="text-sm text-slate-500 mt-4">{data.profile_review} Original post dates and creative changes need separate evidence. The sales comparison refreshes as stored orders change; paid history refreshes daily through the existing maintenance cycle.</p></section>
  </>}
 </main>;
}
