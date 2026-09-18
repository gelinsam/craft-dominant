import {useEffect,useState} from 'react';

const labels={read_verified:'Meta read verified',read_unavailable:'Meta read unavailable',not_checked:'Not checked',not_verified:'Not verified',stale:'Check is stale'};
const shown=n=>typeof n==='number'&&Number.isFinite(n)?n.toLocaleString(undefined,{maximumFractionDigits:2}):'Not reported';
function PurchaseOutcomes({report}){
 const [festival,setFestival]=useState('');
 const rows=report?.campaigns||[];
 const choices=[...new Set(rows.filter(r=>r.mapping_status==='matched').map(r=>r.edition))].sort();
 const visible=rows.filter(r=>!festival||r.edition===festival).sort((a,b)=>(b.spend??-1)-(a.spend??-1));
 return <section className="my-6 rounded-xl border bg-white p-5">
  <div className="flex flex-wrap items-center justify-between gap-3"><h2 className="text-xl font-semibold">Which ads are associated with purchases?</h2><a className="text-blue-700" href="https://adsmanager.facebook.com/adsmanager/manage/campaigns?act=10102135202329320&business_id=1684700878260744&column_preset=10105411572432370" target="_blank" rel="noopener noreferrer">Open saved Meta report ↗</a></div>
  <p className="mt-2 text-slate-600">Meta’s reported website purchases alongside campaign spend and engagement. A purchase can contain several tickets; these figures do not prove that an ad caused the sale.</p>
  <p className="mt-3 text-sm">{report?.since?`${report.since} – ${report.until} · 7-day click / 1-day view · Purchases reported on conversion date`:'Waiting for the first purchase report.'}</p>
  <p className="mt-1 text-sm text-slate-500">Last successful read: {report?.observed_at||'Not yet available'}. Refreshes every six hours.</p>
  {report?.status!=='current'&&<p role="status" className="mt-3 rounded bg-amber-50 p-3">{rows.length?'The latest read is unavailable or stale. Showing the last successful report.':'Purchase reporting is not available yet. The saved Meta report is ready to open.'}</p>}
  {rows.length>0&&<><label className="block mt-5 font-medium" htmlFor="purchase-festival">Festival edition</label><select id="purchase-festival" value={festival} onChange={e=>setFestival(e.target.value)} className="border rounded p-2 mt-2 w-full sm:w-auto"><option value="">All campaigns, including unresolved matches</option>{choices.map(e=><option key={e} value={e}>{rows.find(r=>r.edition===e)?.festival} · {e}</option>)}</select>
   <div className="overflow-x-auto mt-4"><table className="w-full text-sm text-left"><thead><tr>{['Campaign','Spend','Website purchases','Purchase value','Cost / purchase','Website ROAS','Engagements'].map(h=><th key={h} className="p-3 border-b">{h}</th>)}</tr></thead><tbody>{visible.map(r=><tr key={`${r.account_id}:${r.campaign_id}`} className="border-b align-top"><td className="p-3 min-w-64"><a className="text-blue-700 font-medium" target="_blank" rel="noopener noreferrer" href={`https://adsmanager.facebook.com/adsmanager/manage/campaigns?act=${encodeURIComponent(r.account_id)}&selected_campaign_ids=${encodeURIComponent(r.campaign_id)}`}>{r.campaign_name} ↗</a><p className="text-slate-500 mt-1">{r.mapping_status==='matched'?r.festival:'Festival match needs review'}</p></td>{[r.spend,r.website_purchases,r.website_purchase_value,r.cost_per_reported_purchase,r.reported_website_roas,r.post_engagements].map((n,i)=><td key={i} className="p-3 whitespace-nowrap">{shown(n)}{typeof n==='number'&&[0,2,3].includes(i)?` ${r.account_currency||''}`:''}{typeof n==='number'&&i===4?'×':''}</td>)}</tr>)}</tbody></table></div></>}
  <p className="mt-4 text-sm text-slate-600">“Not reported” does not mean no sales. A working pixel can receive purchases while a boosted post still has no purchase attribution. Unresolved festival matches stay separate.</p>
 </section>;
}
export default function Tracking(){
 const [data,setData]=useState(null),[error,setError]=useState('');
 useEffect(()=>{const controller=new AbortController();let alive=true;const load=async()=>{try{const r=await fetch('/api/proxy/api/intelligence/tracking',{cache:'no-store',signal:controller.signal});if(!r.ok)throw Error('Tracking inventory is unavailable.');const d=await r.json();if(alive){setData(d);setError('');}}catch(e){if(alive&&e.name!=='AbortError')setError(e.message);}};load();const timer=setInterval(load,60000);return()=>{alive=false;controller.abort();clearInterval(timer);};},[]);
 return <main className="max-w-7xl mx-auto px-6 py-8 text-slate-800">
  <a href="/" className="text-blue-700">← Craft Dominant · Ticket pacing</a>
  <h1 className="text-3xl font-bold mt-5">Ads and ticket purchase tracking</h1>
  {error&&<p role="alert" className="bg-amber-50 p-4 mt-4">{error}</p>}
  {!data&&!error&&<p role="status" className="mt-4">Loading tracking inventory…</p>}
  {data&&<>
   <PurchaseOutcomes report={data.purchase_outcomes}/>
   <div className="bg-amber-50 rounded-xl p-4 my-5"><strong>Purchase tracking still needs verification.</strong><p className="mt-2">{data.caution}</p><p className="text-sm mt-2">Last check: {data.observed_at||'Not yet checked'}. Meta reads refresh through existing six-hour maintenance.</p></div>
   <div className="overflow-x-auto"><table className="w-full text-left text-sm"><thead><tr>{['Festival / reference','Pixel','Latest Meta evidence','Website setup','Eventbrite CAPI'].map(x=><th className="p-3 border-b" key={x}>{x}</th>)}</tr></thead><tbody>{data.registrations.map(r=><tr key={r.pixel_id} className="border-b align-top">
    <td className="p-3"><strong>{r.festival}</strong><p className="mt-2 text-slate-500">{r.referenced_event?`${r.referenced_event.name} · ${r.referenced_event.event_date?.slice(0,10)}`:'Original event reference unresolved'}</p></td>
    <td className="p-3 font-mono">{r.pixel_id}</td>
    <td className="p-3"><p>{labels[r.pixel_observation.status]||'Not verified'}</p>{r.pixel_observation.name&&<p className="mt-1">{r.pixel_observation.name}</p>}<p className="mt-2">Last event: {r.pixel_observation.last_fired_time||'Unknown'}</p>{r.pixel_observation.is_unavailable===true&&<p className="text-red-700 mt-2">Meta reports this pixel unavailable.</p>}</td>
    <td className="p-3"><p>{r.squarespace_site||'Not recorded'}</p><p className="text-amber-800 mt-2">Installation not independently verified</p></td>
    <td className="p-3"><p className="text-amber-800">Purchase delivery not verified</p><p className="text-slate-500 mt-2">Old board: {r.reported_capi_status||'Unknown'}</p></td>
   </tr>)}</tbody></table></div>
   <section className="mt-8"><h2 className="text-xl font-semibold">Upcoming editions without a matching setup reference</h2><p className="text-sm text-slate-600 mt-2">A previous edition’s configuration does not prove the new Eventbrite event is connected.</p>{data.upcoming_without_reference.length?data.upcoming_without_reference.map(e=><div className="border-b py-3" key={e.event_id}><p className="font-medium">{e.name} · {e.event_date?.slice(0,10)}</p><p className="text-sm mt-1">{e.reason}</p></div>):<p className="mt-3">All current editions have a registry reference. Delivery verification is still separate.</p>}</section>
  </>}
 </main>;
}
