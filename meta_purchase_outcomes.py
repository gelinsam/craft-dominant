"""Read Meta's reported website purchases using existing transport and scheduler.

No new SDK, queue, database migration, attribution model or advertising writes.
Meta owns attribution; Craft's existing edition matcher owns festival identity.
Missing purchase fields remain unknown. Overlapping purchase aliases are never
added together. All-account refreshes commit only after complete pagination.
"""
from datetime import datetime, timedelta, timezone
import json
import math
import os
from pathlib import Path
import re
from campaign_feedback import atomic_json, read_json

WINDOWS = ['7d_click', '1d_view']
PURCHASE = 'offsite_conversion.fb_pixel_purchase'


def path():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent / 'meta-purchase-outcomes.json'


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        n = float(value)
        return n if math.isfinite(n) and n >= 0 else None
    except (TypeError, ValueError):
        return None


def action(rows, kind):
    if not isinstance(rows, list):
        return None
    matches = [r for r in rows if isinstance(r, dict) and r.get('action_type') == kind]
    # Absence is not evidence of zero or a reason to substitute omni_purchase.
    if len(matches) != 1:
        return None
    return number(matches[0].get('value'))


def normalize(raw, account):
    if (not isinstance(raw, dict) or not re.fullmatch(r'\d+', str(raw.get('campaign_id', '')))
            or str(raw.get('account_id', '')) != account):
        raise ValueError('Unexpected campaign identity')
    result = {k: raw.get(k) for k in ('campaign_id', 'campaign_name', 'account_currency', 'date_start', 'date_stop')}
    result['account_id'] = account
    result['spend'] = number(raw.get('spend'))
    result['website_purchases'] = action(raw.get('actions'), PURCHASE)
    result['website_purchase_value'] = action(raw.get('action_values'), PURCHASE)
    result['post_engagements'] = action(raw.get('actions'), 'post_engagement')
    result['landing_page_views'] = action(raw.get('actions'), 'landing_page_view')
    result['link_clicks'] = action(raw.get('actions'), 'link_click')
    count, revenue, spend = (result[k] for k in ('website_purchases', 'website_purchase_value', 'spend'))
    result['cost_per_reported_purchase'] = spend / count if spend is not None and count is not None and count > 0 else None
    result['reported_website_roas'] = revenue / spend if revenue is not None and spend is not None and spend > 0 else None
    result['purchase_status'] = 'reported' if count is not None else 'not_reported'
    return result


def refresh_purchase_outcomes(db, factory=None, now=None):
    now = now or datetime.now(timezone.utc)
    attempt = {'attempted_at': now.isoformat(), 'status': 'refresh_failed'}
    token = os.environ.get('META_ACCESS_TOKEN')
    accounts = list(dict.fromkeys(x.strip().removeprefix('act_') for x in os.environ.get('META_AD_ACCOUNT_ID', '').split(',') if x.strip()))
    if not token or not accounts or not all(re.fullmatch(r'\d+', x) for x in accounts):
        attempt['status'] = 'credentials_unavailable'
        atomic_json(str(path())+'.status', attempt)
        return attempt
    if factory is None:
        from craft_unified import MetaAdsSync
        factory = MetaAdsSync
    until = now.date() - timedelta(days=1)
    since = until - timedelta(days=27)
    rows, seen = [], set()
    try:
        events = db.get_events(upcoming_only=False)
        for account in accounts:
            client = factory(token, account, db)
            try:
                url = client.BASE_URL+'/act_'+account+'/insights'
                params = {'fields': 'account_id,account_currency,campaign_id,campaign_name,spend,actions,action_values,date_start,date_stop',
                          'level': 'campaign', 'time_range': json.dumps({'since': since.isoformat(), 'until': until.isoformat()}),
                          'action_attribution_windows': json.dumps(WINDOWS), 'action_report_time': 'conversion', 'limit': 250}
                visited = set()
                while url:
                    if url in visited or len(visited) >= 20:
                        raise ValueError('Incomplete pagination')
                    visited.add(url)
                    data = client._api_get(url, params)
                    if not isinstance(data, dict) or not isinstance(data.get('data'), list):
                        raise ValueError('Incomplete response')
                    for raw in data['data']:
                        row = normalize(raw, account)
                        if not (since.isoformat() <= str(row['date_start']) <= str(row['date_stop']) <= until.isoformat()):
                            raise ValueError('Unexpected reporting dates')
                        key = (account, row['campaign_id'])
                        if key in seen:
                            raise ValueError('Duplicate aggregate campaign')
                        seen.add(key)
                        assigned = client._assign_campaign_to_edition(
                            {'id': row['campaign_id'], 'name': row['campaign_name'] or ''}, events, until)
                        if assigned and not assigned.get('ambiguous'):
                            event = assigned['canonical_event']
                            row.update(event_id=str(event['event_id']), festival=event['name'],
                                       edition=assigned['edition'], mapping_status='matched',
                                       mapping_reason=assigned.get('match_reason'))
                        else:
                            row['mapping_status'] = 'ambiguous' if assigned else 'unmatched'
                        rows.append(row)
                    paging = data.get('paging', {})
                    if not isinstance(paging, dict):
                        raise ValueError('Invalid pagination')
                    url, params = paging.get('next'), {}
            finally:
                client.session.close()
        previous = read_json(path()) or {}
        if previous.get('since') == since.isoformat() and previous.get('until') == until.isoformat():
            old = {(r['account_id'], r['campaign_id']): r for r in previous.get('campaigns', [])}
            fresh = {(r['account_id'], r['campaign_id']): r for r in rows}
            for key, prior in old.items():
                if key not in fresh or any(prior.get(metric) is not None and fresh[key].get(metric) is None
                                          for metric in ('spend', 'website_purchases', 'website_purchase_value')):
                    raise ValueError('Previously observed metrics missing from refresh')
        atomic_json(path(), {'observed_at': now.isoformat(), 'since': since.isoformat(), 'until': until.isoformat(),
                            'attribution_windows': WINDOWS, 'action_report_time': 'conversion', 'accounts': accounts, 'campaigns': rows})
        attempt.update(status='current', campaigns=len(rows))
    except Exception:
        # Keep prior good observations and never serialize provider exceptions.
        pass
    atomic_json(str(path())+'.status', attempt)
    return attempt


def purchase_report(now=None):
    now = now or datetime.now(timezone.utc)
    snapshot = read_json(path()) or {}
    attempt = read_json(str(path())+'.status') or {}
    state = 'not_checked'
    try:
        age = (now-datetime.fromisoformat(snapshot['observed_at'])).total_seconds()
        state = 'current' if 0 <= age <= 8*3600 else 'stale'
    except (KeyError, TypeError, ValueError):
        pass
    if attempt.get('status') in ('refresh_failed', 'credentials_unavailable'):
        state = attempt['status']
    return dict(snapshot, status=state, last_attempt_at=attempt.get('attempted_at'),
                campaigns=snapshot.get('campaigns', []),
                metric='Meta-reported website purchases, not tickets sold or proven incremental sales',
                missing_value='Not reported; does not mean no sales', execution_allowed=False)
