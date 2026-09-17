"""Small read-only health checks using Craft's existing Meta transport."""
from datetime import datetime, timezone
import hashlib
import json
import os
from campaign_feedback import atomic_json, feedback_path, read_json


def accounts():
    return sorted({x.strip().removeprefix('act_') for x in os.environ.get('META_AD_ACCOUNT_ID','').split(',') if x.strip()})


def fingerprint():
    # Detect changed account configuration without storing credentials or IDs.
    return hashlib.sha256((json.dumps(accounts())+os.environ.get('META_ACCESS_TOKEN','')).encode()).hexdigest()


def health_path():
    return str(feedback_path())+'.meta-health'


def meta_health(now=None):
    now=now or datetime.now(timezone.utc)
    if not accounts() or not os.environ.get('META_ACCESS_TOKEN'):
        return {'status':'not_configured','observed_at':None}
    value=read_json(health_path()) or {}
    if value.get('configuration')!=fingerprint():
        return {'status':'not_verified','observed_at':None}
    try:
        age=(now-datetime.fromisoformat(value['observed_at'])).total_seconds()
    except (ValueError,TypeError,KeyError):
        age=-1
    status=value.get('status','not_verified') if 0<=age<=8*3600 else 'stale'
    return {'status':status,'observed_at':value.get('observed_at'),'accounts_checked':value.get('accounts_checked',0),
            'detail':'Read access only; campaign attribution, tracking quality and ad changes require their own evidence.'}


def refresh_meta_health(factory=None, now=None):
    now=now or datetime.now(timezone.utc)
    if not accounts() or not os.environ.get('META_ACCESS_TOKEN'):
        return {'status':'not_configured'}
    checked=0
    try:
        if factory is None:
            from craft_unified import MetaAdsSync
            factory=MetaAdsSync
        for account in accounts():
            transport=factory(os.environ['META_ACCESS_TOKEN'], account, None)
            try:
                result=transport._api_get(transport.BASE_URL+'/act_'+account, {'fields':'account_status'})
                if not isinstance(result,dict) or 'account_status' not in result:
                    raise ValueError('Account response incomplete')
                checked+=1
            finally:
                transport.session.close()
        status='read_access_verified'
    except Exception:
        # No raw provider errors, tokens, account IDs or response bodies.
        status='read_access_failed'
    value={'status':status,'observed_at':now.isoformat(),'accounts_checked':checked,'configuration':fingerprint()}
    atomic_json(health_path(),value)
    return meta_health(now)
