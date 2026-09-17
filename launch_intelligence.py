"""Descriptive launch reconstruction. Read-only sales; private Meta evidence.

Reuses the existing Meta transport, scheduler and atomic snapshot writer.
No ad/customer mutations, causal claims, forecasts or model calls.
"""
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import sqlite3

FIRST = {'Dallas': 2025, 'San Diego': 2024, 'Philadelphia': 2024,
         'Washington': 2023, 'Seattle': 2025}
PROFILES = {'Dallas':'dallascoffeefest','San Diego':'sandiegocoffeefest',
 'Philadelphia':'phillycoffeefest','Washington':'dccoffeefestival',
 'Seattle':'seattlecoffeefestival','New York':'nyccoffeefest'}
NAMES = {'Dallas':'Dallas Coffee Festival','San Diego':'San Diego Coffee Festival',
 'Philadelphia':'Philly Coffee Festival','Washington':'DC Coffee Festival',
 'Seattle':'Seattle Coffee Festival','New York':'NYC Coffee Festival'}
PATTERNS = {'Dallas':r'dallas|dalcf','San Diego':r'san diego|sdcf|\bsd\b',
 'Philadelphia':r'philly|philadelphia|\bpcf(?:\d{2})?\b','Washington':r'dccf|dc coffee',
 'Seattle':r'seattle|seacf','New York':r'nyc|new york'}
PHASES = [('Launch / 91+ days',91,10000),('Build / 61–90 days',61,90),
 ('Build / 31–60 days',31,60),('Close / 15–30 days',15,30),
 ('Close / 8–14 days',8,14),('Final / 0–7 days',0,7)]


def root():
    return Path(os.environ.get('DB_PATH','craft_unified.db')).resolve().parent


def read_snapshot(name):
    try:
        p=root()/name
        if p.stat().st_size > 40_000_000: return {}
        value=json.loads(p.read_text())
        return value if isinstance(value,dict) else {}
    except (OSError,ValueError): return {}


def number(value):
    try:
        n=float(value)
        return n if n==n and abs(n)<1e15 else None
    except (TypeError,ValueError): return None


def phase(days):
    return next((i for i,(_,lo,hi) in enumerate(PHASES) if lo<=days<=hi),None)


def summarize(edition, sales, campaigns, coverage_start):
    start=date.fromisoformat(edition['date']); end=edition['end_date']
    previous=edition.get('previous_end','1900-01-01')
    lower=max(previous,(start-timedelta(days=365)).isoformat())
    buckets=[{'label':label,'tickets':0,'revenue':0.0,'spend':0.0,'unknown_tickets':0,'unknown_revenue':0} for label,_,_ in PHASES]
    daily=defaultdict(lambda:{'tickets':0,'revenue':0.0,'spend':0.0})
    unknown_tickets=unknown_revenue=0; total_tickets=0; total_revenue=0.0
    for s in sales:
        d=s['date']; t=number(s['tickets']); r=number(s['revenue'])
        unknown_tickets+=s.get('unknown_tickets',0); unknown_revenue+=s.get('unknown_revenue',0)
        total_tickets+=t or 0; total_revenue+=r or 0
        daily[d]['tickets']+=t or 0; daily[d]['revenue']+=r or 0
        i=phase(max(0,(start-date.fromisoformat(d)).days))
        if i is not None:
            buckets[i]['tickets']+=t or 0; buckets[i]['revenue']+=r or 0
            buckets[i]['unknown_tickets']+=s.get('unknown_tickets',0); buckets[i]['unknown_revenue']+=s.get('unknown_revenue',0)
    matched=[]; seen=set(); warnings=[]
    for c in campaigns:
        key=(c.get('account_id'),c.get('campaign',{}).get('id'))
        if key in seen: continue
        seen.add(key)
        if c.get('candidate_cities') != [edition['city']]: continue
        name=c.get('campaign',{}).get('name','')
        # A city name alone is insufficient for coffee assignment.
        identity = name + ' ' + json.dumps(c.get('ads', []))
        if not re.search(r'coffee|dalcf|sdcf|dccf|seacf|\bpcf(?:\d{2})?\b|seattle engagement|san diego engagement',identity,re.I): continue
        years=set(re.findall(r'(?<!\d)20\d{2}(?!\d)',name))
        if years and str(start.year) not in years: continue
        days=[d for d in c.get('days',[]) if lower<d.get('date_start','')<=end and number(d.get('spend')) is not None]
        if not days: continue
        spend=sum(float(d['spend']) for d in days)
        creatives=[]
        for a in c.get('ads',[]):
            cr=a.get('creative',{}); url=cr.get('instagram_permalink_url','')
            if not re.match(r'^https://(www\.)?instagram\.com/(p|reel)/[\w-]+/?$',url): url=None
            creatives.append({'body':cr.get('body'),'url':url,'video':bool(cr.get('video_id')),'snapshot_only':True})
        matched.append({'name':name,'first_delivery':min(d['date_start'] for d in days),
          'last_delivery':max(d['date_start'] for d in days),'spend':round(spend,2),
          'objective':c['campaign'].get('objective'),'creatives':creatives})
        for d in days:
            when=d['date_start']; daily[when]['spend']+=float(d['spend'])
            i=phase(max(0,(start-date.fromisoformat(when)).days))
            if i is not None: buckets[i]['spend']+=float(d['spend'])
    total_spend=sum(c['spend'] for c in matched)
    if coverage_start and lower<coverage_start: warnings.append('Paid history begins after the comparison window; earlier spend is missing.')
    if unknown_tickets or unknown_revenue: warnings.append('Some stored order values are unknown; totals include observed values only.')
    if not matched: warnings.append('No qualifying paid campaign observations; spend is unknown, not zero.')
    for b in buckets:
        b['revenue']=round(b['revenue'],2); b['spend']=round(b['spend'],2) if matched else None
    return {**edition,'tickets':total_tickets if any(number(s['tickets']) is not None for s in sales) else None,'revenue':round(total_revenue,2) if any(number(s['revenue']) is not None for s in sales) else None,
      'spend':round(total_spend,2) if matched else None,'unknown_tickets':unknown_tickets,
      'unknown_revenue':unknown_revenue,'phases':buckets,'campaigns':sorted(matched,key=lambda c:c['first_delivery']),
      'daily':[{'date':d,**{k:round(v,2) for k,v in x.items()}} for d,x in sorted(daily.items())],
      'warnings':warnings,'profile':'https://www.instagram.com/'+PROFILES[edition['city']]}


def build_report(db_path=None, today=None):
    today=today or date.today(); db_path=db_path or os.environ.get('DB_PATH','craft_unified.db')
    history=read_snapshot('coffee-launch-history.json'); nyc=read_snapshot('nyc-launch-history.json')
    campaigns=history.get('campaigns',[])+nyc.get('campaigns',[])
    con=sqlite3.connect('file:'+str(Path(db_path).resolve())+'?mode=ro',uri=True); con.row_factory=sqlite3.Row
    editions=[]
    try:
        for city,name in NAMES.items():
            rows=con.execute('SELECT event_id,event_date FROM events WHERE name=? AND event_type=? ORDER BY event_date',(name,'coffee')).fetchall()
            groups=[]
            for row in rows:
                d=row['event_date'][:10]
                if city=='New York' and d<'2026-01-01': continue
                if not groups or (date.fromisoformat(d)-date.fromisoformat(groups[-1]['date'])).days>3:
                    groups.append({'city':city,'date':d,'end_date':d,'event_ids':[]})
                groups[-1]['event_ids'].append(row['event_id']); groups[-1]['end_date']=d
            previous='1900-01-01'
            for e in groups:
                year=int(e['date'][:4]); e['previous_end']=previous;previous=e['end_date']
                e['edition']=year-FIRST[city]+1 if city in FIRST else None
                e['lifecycle']='relaunch' if city=='New York' else 'first launch' if e['edition']==1 else 'returning'
                e['completed']=e['end_date']<today.isoformat();e['days_out']=(date.fromisoformat(e['date'])-today).days
                marks=','.join('?' for _ in e['event_ids'])
                sales=[dict(r) for r in con.execute(f'SELECT substr(order_timestamp,1,10) date,SUM(ticket_count) tickets,SUM(gross_amount) revenue,SUM(ticket_count IS NULL) unknown_tickets,SUM(gross_amount IS NULL) unknown_revenue FROM orders WHERE event_id IN ({marks}) GROUP BY 1 ORDER BY 1',e['event_ids'])]
                editions.append(summarize(e,sales,campaigns,history.get('since')))
    finally: con.close()
    refs=read_snapshot('coffee-instagram-references.json')
    ref_keys={'Dallas':'dallas','San Diego':'sandiego','Philadelphia':'philly','Washington':'dc','Seattle':'seattle','New York':'nyc'}
    for e in editions:
        candidates=refs.get(ref_keys[e['city']],[])
        if e['city']=='New York': candidates=candidates[:17]  # Reviewed current relaunch posts, excluding legacy 2017 rows.
        e['instagram_references']=[r for r in candidates if isinstance(r,dict) and re.match(r'^https://www\.instagram\.com/'+PROFILES[e['city']]+r'/(p|reel)/[\w-]+/$',r.get('url',''))]
        if e['completed']: continue
        peers=[p for p in editions if p['completed'] and p['lifecycle']=='first launch' and p['tickets'] and not p['unknown_tickets']]
        comparison=[]
        for p in peers:
            cutoff=(date.fromisoformat(p['date'])-timedelta(days=max(e['days_out'],0))).isoformat()
            observed=sum(d['tickets'] for d in p['daily'] if d['date']<=cutoff)
            comparison.append({'city':p['city'],'date':p['date'],'tickets_at_same_days_out':observed,'final_tickets':p['tickets'],'fraction_sold':round(observed/p['tickets'],4)})
        e['launch_references']=comparison
    return {'generated_at':datetime.now(timezone.utc).isoformat(),'meta_collected_at':history.get('collected_at'),
      'status':'provisional','editions':editions,'profile_review':'Six profiles reviewed September 16, 2026. Current grids and captions are references, not original creative version history.',
      'coverage':'Candidate campaign names plus delivery windows; not audited attribution. Live stored ticket data. Gross revenue is not incremental ROAS.',
      'creative_caution':'Current Meta creative may have replaced the original. Original post dates must be checked on Instagram.',
      'candidate_count':history.get('candidate_count',0)+nyc.get('candidate_count',0),
      'request_errors':sum(bool(c.get('errors')) for c in campaigns),
      'execution_allowed':False}


def refresh_history():
    """Daily incremental GET-only refresh; failed reads preserve prior evidence."""
    from campaign_feedback import atomic_json
    from craft_unified import MetaAdsSync
    now=datetime.now(timezone.utc); path=root()/'coffee-launch-history.json'
    old=read_snapshot(path.name)
    try:
        stamp=datetime.fromisoformat(old.get('collected_at',''))
        if stamp.tzinfo and (now-stamp).total_seconds()<86400: return {'status':'current'}
    except ValueError: pass
    # Missing historical evidence must be collected explicitly, not invented by a 30-day refresh.
    if not old.get('campaigns'): return {'status':'history_not_seeded'}
    prior=old.get('campaigns',[])+read_snapshot('nyc-launch-history.json').get('campaigns',[])
    keyed={(x['account_id'],x['campaign']['id']):x for x in prior}
    for acct in os.environ.get('META_AD_ACCOUNT_ID','').split(','):
        if not acct.strip(): continue
        acct=acct.strip(); m=MetaAdsSync(os.environ['META_ACCESS_TOKEN'],acct,None)
        for c in m._fetch_all_campaigns():
            cities=[city for city,pattern in PATTERNS.items() if re.search(pattern,c['name'],re.I)]
            if not cities or re.search(r'wine|cocktail|margarita|whisk|beer|taco',c['name'],re.I): continue
            key=(acct,c['id']); item=dict(keyed.get(key,{'account_id':acct,'days':[]}))
            r=m._api_get(m.BASE_URL+'/'+c['id']+'/insights',{'fields':'campaign_id,spend,impressions,clicks,actions',
              'time_range':json.dumps({'since':(now.date()-timedelta(days=30)).isoformat(),'until':now.date().isoformat()}),
              'time_increment':1,'limit':500})
            days=r.get('data',[]); nxt=r.get('paging',{}).get('next');seen=set()
            while nxt:
                if nxt in seen: raise ValueError('Repeated Meta page')
                seen.add(nxt);r=m._api_get(nxt);days+=r.get('data',[]);nxt=r.get('paging',{}).get('next')
            merged={d['date_start']:d for d in item.get('days',[])}
            for d in days:
                # Missing spend cannot erase an observed value.
                previous=merged.get(d['date_start'],{})
                merged[d['date_start']]={**previous,**{k:v for k,v in d.items() if v is not None}}
            item.update(campaign=c,candidate_cities=cities,days=list(merged.values()),errors=[])
            keyed[key]=item
    value={**old,'campaigns':list(keyed.values()),'candidate_count':len(keyed),'collected_at':now.isoformat(),'until':now.date().isoformat()}
    atomic_json(path,value)
    return {'status':'refreshed','campaigns':len(keyed)}
