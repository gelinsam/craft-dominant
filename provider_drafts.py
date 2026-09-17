"""Materialize reviewed festival opportunities as Mailchimp drafts, never sends.

Reuses Craft's strict provider client, CRM resolver and atomic private storage.
An intent is persisted before every create; retries reconcile the exact title.
"""
from datetime import datetime, timezone, timedelta
import fcntl
import hashlib
import html
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from campaign_feedback import atomic_json, read_json
from crm_audience import build_crm_audience, recipient_digest, email_key
from audience_routing import event_audience_id


def root():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent


def page_all(client, path, key):
    cache = getattr(client, '_draft_read_cache', None)
    if cache is not None and (path,key) in cache:
        return cache[(path,key)]
    fields = {'members':'members.id,members.email_address,members.status,total_items',
              'campaigns':'campaigns.id,campaigns.web_id,campaigns.status,campaigns.settings,campaigns.recipients,campaigns.send_time,total_items',
              'emails':'emails.email_id,emails.email_address,emails.activity,total_items',
              'automations':'automations.id,automations.status,automations.recipients,total_items'}
    rows, seen, total = [], set(), None
    while True:
        page = client._request_strict('GET', path + ('&' if '?' in path else '?') +
            urlencode({'count': 1000, 'offset': len(rows), 'fields':fields[key]}), timeout=60).body
        batch, expected = page.get(key), page.get('total_items')
        if not isinstance(batch, list) or not isinstance(expected, int) or expected < 0 or (total is not None and expected != total):
            raise ValueError('Provider pagination changed or is incomplete')
        total = expected
        for item in batch:
            identity = item.get('id') or item.get('email_id') or item.get('email_address')
            if not identity or identity in seen:
                raise ValueError('Provider pagination has duplicate or missing identities')
            seen.add(identity)
        rows.extend(batch)
        if len(rows) == total:
            if cache is not None: cache[(path,key)] = rows
            return rows
        if not batch or len(rows) > total:
            raise ValueError('Provider pagination truncated')


def fingerprint(campaign, content):
    return hashlib.sha256(json.dumps({'settings': campaign.get('settings'),
        'recipients': {k:v for k,v in campaign.get('recipients', {}).items()
                       if k in ('list_id', 'segment_opts')},
        'html': content.get('html')}, sort_keys=True).encode()).hexdigest()


def eligible_members(client, campaigns, now):
    members = page_all(client, f'/lists/{client.audience_id}/members?status=subscribed', 'members')
    eligible = {email_key(m.get('email_address')) for m in members if m.get('status') == 'subscribed'}
    eligible = {e for e in eligible if '@' in e and not any(c.isspace() for c in e)}
    reserved, recent = set(), set()
    for campaign in campaigns:
        cid, status = campaign['id'], campaign['status']
        if status in ('schedule', 'sending'):
            # Conservatively reserve all destination members when the provider's
            # scheduled segment cannot be reproduced, rather than invent coverage.
            recipients = campaign.get('recipients', {})
            list_id = recipients.get('list_id')
            if not list_id:
                raise ValueError('Scheduled campaign has unresolved audience')
            segment_id = recipients.get('segment_opts',{}).get('saved_segment_id')
            path = f'/lists/{list_id}/segments/{segment_id}/members' if segment_id else f'/lists/{list_id}/members?status=subscribed'
            selected = page_all(client, path, 'members')
            reserved.update(email_key(m['email_address']) for m in selected)
        elif status == 'sent' and campaign.get('send_time'):
            sent = datetime.fromisoformat(campaign['send_time'].replace('Z', '+00:00'))
            if now - sent < timedelta(days=7):
                activity = page_all(client, f'/reports/{cid}/email-activity', 'emails')
                for member in activity:
                    if any(a.get('action') in ('sent', 'open', 'click') for a in member.get('activity', [])):
                        recent.add(email_key(member['email_address']))
    return eligible - reserved - recent, {'provider_subscribed':len(eligible),
        'reserved_scheduled':len(eligible & reserved), 'recent_contacted':len(eligible & recent)}


def current_buyers(db, siblings):
    """Fresh read-only provider exclusions, independent of a long historical sync.

    Conservatively exclude every observed current order, including refunds.
    Unknown identity or truncated pagination never means 'not a buyer'.
    """
    from craft_unified import EventbriteSync
    if not os.environ.get('EVENTBRITE_API_KEY'):
        raise ValueError('Current buyer verification is unavailable')
    source=EventbriteSync(os.environ['EVENTBRITE_API_KEY'],db)
    emails=set()
    for eid in siblings:
        rows=source._paginate(f'/events/{eid}/orders/',{'expand':'attendees'},require_complete=True)
        ids=set()
        for row in rows:
            identity=row.get('id')
            if not identity or identity in ids:
                raise ValueError('Current purchase pagination has unresolved identities')
            ids.add(identity)
            email=email_key(row.get('email'))
            if not email:
                attendees=row.get('attendees') or []
                email=email_key((attendees[0].get('profile') or {}).get('email')) if attendees else ''
            if '@' not in email or any(c.isspace() for c in email):
                raise ValueError('Current purchase has an unresolved buyer')
            emails.add(email)
    return emails


def build_copy(event, brief):
    name = event['name']
    subject = brief['subject']
    text = brief['paragraphs']
    url = brief['ticket_url']
    if not url.startswith('https://www.eventbrite.com/e/') or any('[' in s or ']' in s for s in [subject, *text]):
        raise ValueError('Finished copy and verified ticket link required')
    e = html.escape
    body = '<!doctype html><html><body style="margin:0;background:#f4f2ed;color:#262626;font-family:Arial,sans-serif"><div style="max-width:600px;margin:24px auto;padding:36px;background:white">'
    if brief.get('image_url', '').startswith('https://img.evbuc.com/'):
        body += '<img src="'+e(brief['image_url'],quote=True)+'" alt="'+e(name,quote=True)+'" width="528" style="display:block;width:100%;height:auto;margin-bottom:28px">'
    body += '<p style="font-size:14px">'+e(name)+'</p><h1 style="font-size:30px;line-height:1.2">'+e(brief['heading'])+'</h1>'
    body += ''.join('<p style="font-size:18px;line-height:1.6">'+e(p)+'</p>' for p in text)
    body += '<p style="margin:32px 0"><a style="background:#183e32;color:white;padding:16px 24px;border-radius:6px;display:inline-block;text-decoration:none;font-weight:bold" href="'+e(url, quote=True)+'">'+e(brief.get('cta','Get tickets'))+'</a></p>'
    body += '<p style="font-size:12px;color:#666">*|LIST:DESCRIPTION|*</p><p style="font-size:12px"><a href="*|UNSUB|*">Unsubscribe</a> · <a href="*|UPDATE_PROFILE|*">Update preferences</a></p><p style="font-size:12px">*|LIST:ADDRESS|*</p></div></body></html>'
    return subject, body


def prepare_one(db, brief, client, state, save, now):
    eid = str(brief['event_id'])
    event = dict(db.get_event(eid) or {})
    if event_audience_id(eid) != client.audience_id or not event:
        raise ValueError('Reviewed audience route is missing')
    if (event.get('city'), event.get('event_type')) != (brief['city'], brief['event_type']):
        raise ValueError('Festival scope changed')
    days = (datetime.fromisoformat(event['event_date']).date() - now.date()).days
    if not 0 <= days <= 120:
        raise ValueError('Festival is outside the preparation window')
    # Identity spans all sibling sessions and remains stable across refreshes.
    siblings = sorted(db.edition_sibling_ids(eid))
    key = hashlib.sha256(json.dumps([siblings, brief['segment'], brief['idea_id']]).encode()).hexdigest()[:20]
    entry = state.setdefault('drafts', {}).setdefault(key, {'key': key,
        'event_id':eid, 'event_name':brief['festival'], 'provider':'mailchimp',
        'purpose':brief['purpose_label'], 'state':'preparing', 'audience_count':0})
    campaigns = page_all(client, '/campaigns', 'campaigns')
    title = brief['festival']+' | '+brief.get('draft_label','Discovery for past buyers')
    matches = [c for c in campaigns if c.get('settings',{}).get('title') == title]
    cid = entry.get('campaign_id')
    if len(matches) > 1:
        raise ValueError('Multiple matching provider drafts require reconciliation')
    if not cid and matches:
        cid = matches[0]['id']; entry['campaign_id'] = cid; save()
    current = content = None
    if cid:
        current = client._request_strict('GET', f'/campaigns/{cid}').body
        entry['url'] = f'https://{client.dc}.admin.mailchimp.com/campaigns/edit?id={current["web_id"]}'
        if current['status'] != 'save':
            entry.update(state=current['status'], verified_at=now.isoformat()); save(); return entry
        content = client._request_strict('GET', f'/campaigns/{cid}/content').body
        if entry.get('fingerprint') and entry['fingerprint'] != fingerprint(current, content):
            entry.update(state='user_edited', reason='Your changes are preserved.', verified_at=now.isoformat()); save(); return entry
        if not entry.get('fingerprint') and not entry.get('create_pending'):
            raise ValueError('Existing draft ownership cannot be established')
    print('Preparing current buyers:',brief['festival'],flush=True)
    buyers = current_buyers(db, siblings)
    candidates = build_crm_audience(db, eid, brief['segment'], 'ticket_sales', now)
    if candidates.get('history_coverage') == 'stored_records_only':
        raise ValueError('Win-back purchase history coverage needs verification')
    print('Preparing provider audience:',brief['festival'],flush=True)
    eligible, counts = eligible_members(client, campaigns, now)
    recipients = sorted({r['email'] for r in candidates['records']} & eligible - buyers)
    if not recipients:
        entry.update(state='no_audience', reason='No eligible recipients remain after current buyers and scheduled campaigns are excluded.', verified_at=now.isoformat(),counts=counts)
        # Link relevant owner-scheduled work without adopting it as an owned draft.
        for scheduled in campaigns:
            if scheduled['status'] not in ('schedule','sending') or scheduled.get('recipients',{}).get('list_id') != client.audience_id:
                continue
            saved = client._request_strict('GET',f'/campaigns/{scheduled["id"]}/content').body
            from html.parser import HTMLParser
            from urllib.parse import urlsplit
            links=[]
            class Links(HTMLParser):
                def handle_starttag(self,tag,attrs):
                    if tag=='a': links.extend(v for k,v in attrs if k=='href')
            Links().feed(saved.get('html',''))
            parent=brief['ticket_url'].rsplit('/',1)[-1]
            if any(urlsplit(u).hostname in ('www.eventbrite.com','eventbrite.com') and urlsplit(u).path.startswith('/e/') and urlsplit(u).path.rstrip('/').rsplit('/',1)[-1].rsplit('-',1)[-1] == parent for u in links):
                entry.update(state='covered_by_scheduled',reason='Your campaign is already scheduled. No overlapping draft was added.',
                    subject=scheduled.get('settings',{}).get('subject_line'),
                    url=f'https://{client.dc}.admin.mailchimp.com/campaigns/edit?id={scheduled["web_id"]}')
                break
        save(); return entry
    if (datetime.now(timezone.utc)-now).total_seconds()>3600:
        raise ValueError('Preparation evidence expired')
    subject, body = build_copy(event, brief)
    defaults = client._request_strict('GET', f'/lists/{client.audience_id}').body.get('campaign_defaults',{})
    client.from_name = defaults.get('from_name') or event['name']
    client.from_email = defaults.get('from_email') or client.from_email
    counts['current_buyers_excluded'] = len(buyers)
    digest = recipient_digest(recipients)
    automations = page_all(client, '/automations', 'automations')
    if any(a.get('status') not in ('paused','save','archived') and a.get('recipients',{}).get('list_id') == client.audience_id for a in automations):
        raise ValueError('Active audience automation requires trigger review')
    if digest != entry.get('recipient_digest') or not entry.get('segment_id'):
        # Unique new static segment: existing user tags are never altered.
        tag = 'Craft | '+brief['festival']+' | '+brief['segment'].replace('_',' ')+' | '+now.strftime('%b %d %H:%M UTC')
        client.ensure_members(recipients)
        client.tag_members(recipients, tag)
        entry['segment_id'] = client.get_tag_segment_id(tag)
        entry['recipient_digest'] = digest
        save()
    segment_id = entry['segment_id']
    if not set(recipients).issubset(set(client._get_members_by_status('subscribed') or [])):
        raise ValueError('Provider consent changed during preparation')
    if not cid:
        if entry.get('create_pending') and not matches:
            raise ValueError('Previous create response uncertain; reconcile before another create')
        entry['create_pending'] = True; save()
        result = client.create_campaign_strict(subject, brief['preview'], body,
            segment_id=segment_id, campaign_title=title)
        cid = result.provider_campaign_id
        if cid:
            entry['campaign_id'] = cid; save()
        if not cid or not result.content_set:
            raise ValueError('Provider draft creation incomplete; existing attempt preserved')
    else:
        # Recheck both status and owner edits after potentially slow audience reads.
        latest = client._request_strict('GET',f'/campaigns/{cid}').body
        latest_content = client._request_strict('GET',f'/campaigns/{cid}/content').body
        if latest['status'] != 'save':
            raise ValueError('Campaign was scheduled while preparation was running')
        if fingerprint(latest,latest_content) != fingerprint(current,content):
            entry.update(state='user_edited',reason='Your changes are preserved.',verified_at=now.isoformat()); save(); return entry
        if entry.get('create_pending') and (latest_content.get('html') not in (None,'',body) or latest.get('settings',{}).get('subject_line') != subject):
            raise ValueError('Existing draft changed; your edits are preserved')
        client._request_strict('PATCH', f'/campaigns/{cid}', {'recipients':{
            'list_id':client.audience_id,'segment_opts':{'saved_segment_id':segment_id}},
            'settings':{'subject_line':subject,'preview_text':brief['preview'],'title':title}})
        if entry.get('fingerprint') and content.get('html') != body:
            client._request_strict('PUT', f'/campaigns/{cid}/content', {'html':body})
    if entry.get('create_pending') and content is not None:
        if content.get('html') and content['html'] != body:
            raise ValueError('Existing draft content changed; your edits are preserved')
        if not content.get('html'):
            client._request_strict('PUT', f'/campaigns/{cid}/content', {'html':body})
    current = client._request_strict('GET', f'/campaigns/{cid}').body
    content = client._request_strict('GET', f'/campaigns/{cid}/content').body
    checklist = client._request_strict('GET', f'/campaigns/{cid}/send-checklist').body
    recipient_info = current.get('recipients',{})
    if current['status'] != 'save' or recipient_info.get('segment_opts',{}).get('saved_segment_id') != segment_id or recipient_info.get('recipient_count') != len(recipients):
        raise ValueError('Saved campaign audience does not match prepared recipients')
    if not content.get('html') or current['settings'].get('subject_line') != subject:
        raise ValueError('Saved campaign copy verification failed')
    entry.update(state='ready' if checklist.get('is_ready') is True else 'provider_check',
        reason=None if checklist.get('is_ready') is True else 'Mailchimp reports an incomplete campaign check.',
        campaign_id=cid, url=f'https://{client.dc}.admin.mailchimp.com/campaigns/edit?id={current["web_id"]}',
        subject=subject, audience_count=len(recipients), verified_at=now.isoformat(),
        fingerprint=fingerprint(current,content), create_pending=False, counts=counts,
        rationale=brief['rationale'])
    save(); return entry


def refresh_provider_drafts(db):
    from craft_engine import MailchimpClient
    config = read_json(root()/'provider-draft-briefs.json') or {}
    if config.get('enabled') is not True:
        return {'status':'not_configured'}
    with open(root()/'provider-drafts.lock','a') as lock:
        try: fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: return {'status':'already_running'}
        state = read_json(root()/'provider-drafts.json') or {'drafts':{}}
        def save(): atomic_json(root()/'provider-drafts.json',state)
        read_cache = {}
        for brief in config.get('briefs',[]):
            now=datetime.now(timezone.utc)
            try:
                client=MailchimpClient(os.environ['MAILCHIMP_API_KEY'],event_audience_id(brief['event_id']))
                client._draft_read_cache = read_cache
                prepare_one(db,brief,client,state,save,now)
            except Exception as exc:
                # No provider payloads/customer details in the dashboard or logs.
                event_entries=[e for e in state['drafts'].values() if e['event_id']==brief['event_id']]
                for entry in event_entries:
                    entry.update(state='preparing',reason=str(exc) if isinstance(exc,ValueError) else 'Provider verification unavailable.', checked_at=now.isoformat())
                save()
        state['checked_at']=datetime.now(timezone.utc).isoformat();save()
        return {'status':'checked','drafts':len(state['drafts'])}


def read_provider_drafts(now=None):
    now=now or datetime.now(timezone.utc)
    value=read_json(root()/'provider-drafts.json') or {}
    rows=[]
    public=('key','event_name','provider','purpose','state','audience_count','url','subject','reason','verified_at','rationale')
    for entry in value.get('drafts',{}).values():
        row={k:entry.get(k) for k in public}
        if row['state']=='ready':
            try: fresh=0 <= (now-datetime.fromisoformat(row['verified_at'])).total_seconds()<3600
            except (ValueError,TypeError): fresh=False
            if not fresh: row.update(state='refreshing',reason='Audience verification is being refreshed.')
        rows.append(row)
    return {'drafts':rows,'checked_at':value.get('checked_at')}


def periodic_provider_drafts():
    from campaign_worker import readonly_database
    db=readonly_database(os.environ.get('DB_PATH','craft_unified.db'))
    try: return refresh_provider_drafts(db)
    finally: db.close()
