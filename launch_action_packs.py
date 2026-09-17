"""Craft-specific automatic coffee action packs; preparation, never execution.

Reuses launch reconstruction, edition identity, atomic snapshots and maintenance.
"""
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import re
from statistics import median
from zoneinfo import ZoneInfo


def stable_id(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()[:20]


def complete_editions(report, db, today):
    """Include coffee markets beyond the six reconstructed reference cities."""
    editions=list(report['editions']); known={str(i) for e in editions for i in e['event_ids']}
    events={str(e['event_id']):e for e in db.get_events()}
    seen=set(known)
    for eid,row in events.items():
        if eid in seen or row.get('event_type')!='coffee' or row.get('status') in ('cancelled','canceled') or re.search(r'exhibitor|vendor|sponsor',row.get('name',''),re.I): continue
        ids=sorted(str(i) for i in db.edition_sibling_ids(eid) if str(i) in events)
        members=[events[i] for i in ids]
        if not members or any(m.get('event_type')!='coffee' or m.get('city')!=row.get('city') for m in members): continue
        seen.update(ids)
        dates=sorted(m['event_date'][:10] for m in members)
        marks=','.join('?' for _ in ids)
        sales=[dict(r) for r in db.conn.execute(f'SELECT substr(order_timestamp,1,10) date,SUM(ticket_count) tickets,SUM(gross_amount) revenue,SUM(ticket_count IS NULL) unknown_tickets,SUM(gross_amount IS NULL) unknown_revenue FROM orders WHERE event_id IN ({marks}) GROUP BY 1 ORDER BY 1',ids)]
        e={'city':row.get('city') or row['name'],'name':row['name'],'date':dates[0],'end_date':dates[-1], 'event_ids':ids,'edition':None,'lifecycle':'history unverified','completed':dates[-1]<today.isoformat(),'days_out':(date.fromisoformat(dates[0])-today).days}
        # summarize only needs a profile for the original reference cities.
        e.update(tickets=sum(s['tickets'] or 0 for s in sales) if any(s['tickets'] is not None for s in sales) else None,
                 revenue=sum(s['revenue'] or 0 for s in sales) if any(s['revenue'] is not None for s in sales) else None,
                 spend=None,unknown_tickets=sum(s['unknown_tickets'] for s in sales),
                 unknown_revenue=sum(s['unknown_revenue'] for s in sales),
                 daily=[{'date':s['date'],'tickets':s['tickets'],'revenue':s['revenue']} for s in sales],
                 event_days=(date.fromisoformat(dates[-1])-date.fromisoformat(dates[0])).days+1,
                 instagram_references=[],warnings=['Paid launch reconstruction unavailable for this market.'])
        editions.append(e)
    return editions


def references(edition, editions):
    past=[p for p in editions if p['completed'] and p['date']<(edition.get('date') or '9999-12-31') and p.get('tickets') and not p.get('unknown_tickets')]
    own=[p for p in past if p['city']==edition['city'] and p.get('event_days')==edition.get('event_days')]
    # NYC's old history does not make it an ordinary returning festival.
    peers=own if own and edition['lifecycle']!='relaunch' else [p for p in past if p.get('lifecycle')=='first launch' and p.get('event_days')==edition.get('event_days')]
    result=[]
    for p in peers:
        cutoff=(date.fromisoformat(p['date'])-timedelta(days=max(edition['days_out'],0))).isoformat()
        n=sum(d.get('tickets') or 0 for d in p['daily'] if d['date']<=cutoff)
        result.append({'city':p['city'],'date':p['date'],'same_days_tickets':n,'final_tickets':p['tickets'],'spend':p.get('spend'),'warnings':p.get('warnings',[])})
    return result, 'own comparable editions' if own and edition['lifecycle']!='relaunch' else 'same-duration first launches'


def asset_library(editions):
    found={}
    for e in editions:
        for r in e.get('instagram_references',[]):
            url=r.get('url','');desc=r.get('description','')
            if not re.match(r'^https://www\.instagram\.com/[\w.]+/(p|reel)/[\w-]+/$',url): continue
            text=desc.lower()
            kind='experience' if '/reel/' in url else 'community'
            if re.search(r'poster|save the date|tickets.*sale|coffee coffee coffee',text):kind='announcement'
            elif re.search(r'roaster|lineup|line.up',text):kind='local proof'
            elif re.search(r'countdown|days away|days to go|festival info',text):kind='countdown'
            found[url]={'url':url,'source_city':e['city'],'description':desc[:600], 'format':kind,'selection_basis':'Reviewed coffee account reference; performance not causally established'}
    return sorted(found.values(),key=lambda x:x['url'])


def phase(days):
    return 'announce' if days>90 else 'build' if days>30 else 'close' if days>7 else 'final'


def make_pack(e, editions, assets, today, sales_quality):
    week=(today-timedelta(days=today.weekday())).isoformat(); p=phase(e['days_out'])
    refs,basis=references(e,editions); baseline=median([r['same_days_tickets'] for r in refs]) if refs else None
    delta=round(100*(e['tickets']/baseline-1),1) if baseline and e.get('tickets') is not None and not e.get('unknown_tickets') else None
    decision='Review sales evidence' if sales_quality!='current' else 'Review conversion before increasing spend' if delta is not None and delta<-15 else 'Continue the launch sequence'
    warnings=list(e.get('warnings',[]))
    if sales_quality!='current': warnings.append('Sales source is '+sales_quality+'; pacing recommendations remain provisional.')
    if not refs:warnings.append('No same-duration reference; no performance verdict is inferred.')
    prior=sorted([x for x in editions if x['city']==e['city'] and x['completed'] and x['date']<(e.get('date') or '9999-12-31')],key=lambda x:x['date'])
    if len(prior)>1:
        a,b=prior[-2:]
        if a.get('spend') and b.get('spend') and a.get('tickets') and b.get('tickets') is not None and b['spend']>a['spend'] and b['tickets']<a['tickets'] and a.get('event_days')==b.get('event_days'):
            decision='Review conversion before increasing spend'
            warnings.append('The last completed edition spent more while selling fewer tickets than its predecessor. Diagnose offer, audience and conversion before recommending more budget.')
    launch=e['lifecycle'] in ('first launch','relaunch','planned launch')
    local_prior=bool(prior) and not launch
    audience=(f'{e["city"]} coffee fans and local discovery audiences; do not assume a prior customer list.' if not local_prior else f'{e["city"]} coffee audiences, plus exact-festival past buyers who have not bought this edition. Win-back: bought 1–3 years ago and never returned; keep separate from other not-yet-bought customers.')
    display={'Washington':'DC','Philadelphia':'Philly','New York':'NYC'}.get(e['city'],e['city'])
    title=display+' Coffee Festival'
    when=date.fromisoformat(e['date']).strftime('%B %-d') if e.get('date') else 'Date to be confirmed'
    if e.get('end_date') and e['end_date']!=e['date']:when+='–'+str(date.fromisoformat(e['end_date']).day)
    formats={'announce':['announcement','experience','community'],'build':['experience','community','local proof'],'close':['experience','local proof','countdown'],'final':['countdown','experience','community']}[p]
    content=[]; used=set()
    for i,fmt in enumerate(formats):
        pool=[a for a in assets if a['url'] not in used and a['format']==fmt]
        if not pool:pool=[a for a in assets if a['url'] not in used and a['format'] in ('community','experience')]
        # Cold starts deliberately reuse other coffee markets. Returners prefer local assets.
        pool.sort(key=lambda a:((a['source_city']==e['city']) if launch else (a['source_city']!=e['city']),stable_id([week,e['city'],fmt,a['url']])))
        asset=pool[0] if pool else None
        if asset:used.add(asset['url'])
        captions={
          'announcement':f'{display}, make room for a coffee day. {title} · {when}. Bring the friend who is always choosing the next coffee spot. Pick your session through the ticket link.',
          'experience':f'Your next coffee plan belongs in the group chat. Meet us at {title} on {when}. Bring your curiosity—and your favorite coffee person. Tickets through the link.',
          'community':f'Some plans are better with your coffee people. {title} · {when}. Send this to the friend you want beside you, then choose your session together.',
          'local proof':f'A coffee day deserves a little curiosity. Join us at {title} on {when}. Make a plan with your coffee crew and choose your session through the ticket link.',
          'countdown':f'{title} is coming up on {when}. Coffee people, this is your cue to make the plan. Choose your session and send the details to your crew.'}
        day=today+timedelta(days=i*2)
        if e.get('end_date') and day.isoformat()>e['end_date']:continue
        content.append({'id':stable_id([e['city'],e.get('date'),week,fmt]),'suggested_date':day.isoformat(),'format':fmt,'caption':captions[fmt], 'asset':asset,
          'asset_instructions':'Reuse the source photo or footage; replace city, date, venue and ticket overlays. Remove old roaster names and pricing. Use as a festival-experience illustration, not a claim of prior attendance in this city.',
          'blockers':(['Select an owned coffee asset'] if not asset else [])+(['Confirm this year’s local roaster details before adding any names; generic caption is ready.'] if fmt=='local proof' else [])})
    return {'id':stable_id([e['city'],e.get('date'),week]),'city':e['city'],'festival':title,'event_date':e.get('date'),'event_ids':e['event_ids'],'week':week,'phase':p,'days_out':e['days_out'],'priority':decision,
      'audience':audience,'has_prior_customers':local_prior,'crm_preparation': 'Build exact-festival candidates with existing CRM; verify destination consent and suppressions before provider draft population.' if local_prior else 'No historical win-back assumption; only independently verified local opted-in audiences.',
      'tickets':e.get('tickets'),'candidate_spend':e.get('spend'),'reference_basis':basis,'references':refs,'pace_delta_pct':delta,'warnings':warnings,
      'content':content,'boost_guidance':'Continue the engagement campaign and layered boosts. Review pacing and delivery before changing spend; keep the owner’s $3–$5/day support approach where appropriate. No automatic budget increase or shutdown.',
      'ticket_links':['https://www.eventbrite.com/e/'+str(x) for x in e['event_ids'] if str(x).isdigit()],
      'execution_allowed':False,'status':'prepared_for_review'}



def crm_counts(db, pack, now):
    """Reuse CRM eligibility logic; persist counts only, never customer rows."""
    from crm_audience import build_crm_audience
    if not pack['event_ids'] or not pack.get('has_prior_customers'):
        return []
    result=[]
    for segment,label in [('past_attendees','Past buyers, not yet bought'),('one_and_done','One-and-done win-backs'),('super_spreaders','Super spreader candidates')]:
        try:
            audience=build_crm_audience(db,pack['event_ids'][0],segment,'ticket_sales',now=now)
            result.append({'segment':segment,'label':label,'candidates':len(audience['records']),
                'excluded_current_buyers':audience['excluded_current_buyers'],
                'status':'candidates_only_consent_unverified'})
        except Exception:
            result.append({'segment':segment,'label':label,'candidates':None,'status':'unavailable'})
    return result

def refresh_packs(db):
    from launch_intelligence import build_report,root
    from campaign_feedback import atomic_json
    from action_plan import operational_checks
    now=datetime.now(timezone.utc);today=now.astimezone(ZoneInfo('America/New_York')).date()
    report=build_report(today=today);editions=complete_editions(report,db,today)
    checks=operational_checks(db,now);sales_quality=next((c['status'] for c in checks if c['id']=='sales_sync'),'unknown')
    assets=asset_library(editions)
    packs=[make_pack(e,editions,assets,today,sales_quality) for e in editions if not e['completed'] and e['days_out']>=0]
    houston=next((p for p in packs if p['city']=='Houston'),None)
    if not houston:
        seed={'city':'Houston','date':None,'end_date':None,'event_ids':[],'days_out':180,'lifecycle':'planned launch','event_days':1,'tickets':None,'spend':None,'warnings':['Exact date, venue and ticket destination are not configured. Relative launch preparation only.']}
        # No invented date is written into copy or the calendar.
        plan=make_pack(seed,[],assets,today,sales_quality)
        plan.update(status='awaiting_event_details',phase='announce',days_out=None)
        for c in plan['content']:c['suggested_date']=None;c['blockers'].append('Confirm date, venue and ticket destination before use.')
        packs.append(plan)
    for pack in packs:
        pack['crm_candidates']=crm_counts(db,pack,now)
        pack['email_draft']={'subject':'Your next coffee plan: '+pack['festival'], 'body':pack['content'][0]['caption'] if pack['content'] else '', 'status':'copy_prepared_not_a_provider_draft'}
    packs.sort(key=lambda p:(p['days_out'] is None,p['days_out'] or 0,p['city']))
    value={'generated_at':now.isoformat(),'source_meta_collected_at':report.get('meta_collected_at'),'sales_quality':sales_quality,'packs':packs,'execution_allowed':False,'cadence':'Prepared automatically by existing six-hour maintenance; weekly identity, updated from current evidence.'}
    revision=stable_id({k:v for k,v in value.items() if k!='generated_at'});value['revision']=revision
    history=root()/'launch-pack-history';history.mkdir(mode=0o700,exist_ok=True)
    target=history/(revision+'.json')
    if not target.exists():atomic_json(target,value)
    atomic_json(root()/'launch-action-packs.json',value)
    return {'status':'prepared','packs':len(packs),'revision':revision}


def read_packs():
    from launch_intelligence import read_snapshot
    value=read_snapshot('launch-action-packs.json')
    if not value:return {'status':'not_prepared','packs':[],'execution_allowed':False}
    try:age=(datetime.now(timezone.utc)-datetime.fromisoformat(value['generated_at'])).total_seconds()
    except (ValueError,KeyError):age=float('inf')
    value['status']='current' if 0<=age<86400 else 'stale'
    return value
