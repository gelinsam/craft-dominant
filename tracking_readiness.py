"""Preserve the retired tracking board and refresh read-only Meta evidence.

Imported setup claims never become live Pixel/CAPI verification. Existing
transport, maintenance cadence and private snapshots supply the infrastructure.
"""
from datetime import datetime, timezone
import os
from pathlib import Path
import re
from campaign_feedback import atomic_json, read_json


def root():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent


def registry():
    value = read_json(root()/'tracking-registry.json') or {}
    rows = value.get('registrations', [])
    return [r for r in rows if isinstance(r, dict) and re.fullmatch(r'\d{5,30}', str(r.get('pixel_id', '')))]


def refresh_tracking(factory=None, now=None):
    now = now or datetime.now(timezone.utc)
    rows = registry()
    if not rows:
        return {'status': 'registry_unavailable'}
    previous = read_json(root()/'tracking-observations.json') or {}
    old = {p['pixel_id']: p for p in previous.get('pixels', [])}
    token = os.environ.get('META_ACCESS_TOKEN')
    account = os.environ.get('META_AD_ACCOUNT_ID', '').split(',')[0].strip()
    if not token or not account:
        return {'status': 'credentials_unavailable'}
    if factory is None:
        from craft_unified import MetaAdsSync
        factory = MetaAdsSync
    client = factory(token, account, None)
    result = []
    try:
        for row in rows:
            pixel = row['pixel_id']
            observation = dict(old.get(pixel, {}), pixel_id=pixel, attempted_at=now.isoformat())
            try:
                data = client._api_get(client.BASE_URL+'/'+pixel,
                                       {'fields': 'id,name,last_fired_time,is_unavailable'})
                if str(data.get('id')) != pixel:
                    raise ValueError('Pixel identity mismatch')
                observation.update(status='read_verified', observed_at=now.isoformat())
                # Unknown provider fields remain unknown; do not imply success.
                for key in ('name', 'last_fired_time', 'is_unavailable'):
                    observation[key] = data.get(key)
            except Exception:
                observation['status'] = 'read_unavailable'
            result.append(observation)
    finally:
        client.session.close()
    value = {'observed_at': now.isoformat(), 'pixels': result}
    atomic_json(root()/'tracking-observations.json', value)
    return {'status': 'current' if all(p['status']=='read_verified' for p in result) else 'partial_failure',
            'checked': len(result), 'read_verified': sum(p['status']=='read_verified' for p in result)}


def tracking_report(db, now=None):
    now = now or datetime.now(timezone.utc)
    imported = registry()
    observations = read_json(root()/'tracking-observations.json') or {}
    by_pixel = {p['pixel_id']:p for p in observations.get('pixels', [])}
    rows, mapped = [], set()
    for row in imported:
        item = dict(row)
        event = db.get_event(row.get('source_event_id')) if row.get('source_event_id') else None
        item['referenced_event'] = ({k:event.get(k) for k in ('event_id','name','event_date','city','event_type')} if event else None)
        if event:
            mapped.update(str(x) for x in db.edition_sibling_ids(event['event_id']))
        item['mapping_status'] = 'reference_found' if event else 'reference_unresolved'
        observation = dict(by_pixel.get(row['pixel_id'], {}))
        try:
            age = (now-datetime.fromisoformat(observation['observed_at'])).total_seconds()
            if not 0 <= age < 86400 and observation.get('status') == 'read_verified':
                observation['status'] = 'stale'
        except (KeyError, ValueError, TypeError):
            if observation.get('status') == 'read_verified':
                observation['status'] = 'not_verified'
        item['pixel_observation'] = observation or {'status': 'not_checked'}
        item['capi_verification'] = 'not_verified'
        item['site_installation_verification'] = 'not_verified'
        rows.append(item)
    pending, seen = [], set()
    for event in db.get_events(upcoming_only=True):
        eid = str(event['event_id'])
        if eid in seen or re.search(r'vendor|exhibitor|sponsor|table fee', event.get('name',''), re.I):
            continue
        ids = {str(x) for x in db.edition_sibling_ids(eid)} or {eid}
        seen.update(ids)
        if not ids.intersection(mapped):
            pending.append({'event_id': eid, 'name': event.get('name'), 'event_date': event.get('event_date'),
                            'reason': 'No imported tracking reference for this edition; confirm setup before assuming purchase tracking.'})
    return {'registrations': rows, 'upcoming_without_reference': pending,
            'source': 'Imported retired tracker setup claims, checked against existing event records and read-only Meta observations.',
            'observed_at': observations.get('observed_at'),
            'caution': 'Pixel visibility or a recent event does not verify browser installation, Eventbrite CAPI purchase delivery, deduplication or attribution.',
            'execution_allowed': False}
