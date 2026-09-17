import {useEffect,useState} from 'react';

const labels={read_verified:'Meta read verified',read_unavailable:'Meta read unavailable',not_checked:'Not checked',not_verified:'Not verified',stale:'Check is stale'};
export default function Tracking(){
 const [data,setData]=useState(null),[error,setError]=useState('');
 useEffect(()=>{const controller=new AbortController();let alive=true;const load=async()=>{try{const r=await fetch('/api/proxy/api/intelligence/tracking',{cache:'no-store',signal:controller.signal});if(!r.ok)throw Error('Tracking inventory is unavailable.');const d=await r.json();if(alive){setData(d);setError('');}}catch(e){if(alive&&e.name!=='AbortError')setError(e.message);}};load();const timer=setInterval(load,60000);return()=>{alive=false;controller.abort();clearInterval(timer);};},[]);
 return <main className="max-w-7xl mx-auto px-6 py-8 text-slate-800">
  <a href="/" className="text-blue-700">← Craft Dominant · Ticket pacing</a>
  <h1 className="text-3xl font-bold mt-5">Festival tracking</h1>
  <p className="mt-2 text-slate-600">One home for the pixel and website setup inventory, with fresh evidence kept separate from historical setup claims.</p>
  {error&&<p role="alert" className="bg-amber-50 p-4 mt-4">{error}</p>}
  {!data&&!error&&<p role="status" className="mt-4">Loading tracking inventory…</p>}
  {data&&<>
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
