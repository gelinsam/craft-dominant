"""Read-only Mailchimp observations, shared with the existing JS intelligence.

Official SDK supplies transport. Existing scheduler supplies cadence. Private
atomic snapshots survive deployments; failed refreshes never erase good data.
No member mutations, sends, scheduling, ad writes or LLM calls exist here.
"""
import base64
from datetime import datetime, timezone, timedelta
import json
import math
import os
from pathlib import Path
import re
import tempfile
import zlib

MAX_BYTES = 8_000_000


def utcnow():
    return datetime.now(timezone.utc)


def feedback_path():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent / 'campaign-feedback.json'


def atomic_json(path, value):
    path = Path(path)
    raw = json.dumps(value, allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError('Feedback snapshot exceeds size limit')
    fd, name = tempfile.mkstemp(prefix='.feedback-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as out:
            out.write(raw)
            out.flush()
            os.fsync(out.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def read_json(path):
    try:
        with open(path, 'rb') as source:
            raw = source.read(MAX_BYTES + 1)
        if len(raw) > MAX_BYTES:
            return None
        value = json.loads(raw)
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def seed_records():
    encoded = os.environ.get('CRAFT_CAMPAIGN_EVIDENCE_ZLIB_B64', '')
    if not encoded or len(encoded) > MAX_BYTES:
        return []
    try:
        decoder = zlib.decompressobj()
        raw = decoder.decompress(base64.b64decode(encoded, validate=True), MAX_BYTES + 1)
        if len(raw) > MAX_BYTES or not decoder.eof or decoder.unused_data:
            return []
        data = json.loads(raw)
        return data['records'] if isinstance(data, dict) and isinstance(data.get('records'), list) else []
    except (ValueError, TypeError, zlib.error):
        return []


def load_evidence():
    snapshot = read_json(feedback_path())
    if snapshot and isinstance(snapshot.get('records'), list):
        return snapshot
    records = seed_records()
    return {'version': 1, 'records': records, 'source': 'imported_seed',
            'last_success_at': None, 'coverage': 'Imported history; live refresh not yet verified'}


def feedback_status(now=None):
    now = now or utcnow()
    data = load_evidence()
    attempt = read_json(str(feedback_path()) + '.status') or {}
    stamp = data.get('last_success_at')
    age = None
    if stamp:
        try:
            age = (now - datetime.fromisoformat(stamp.replace('Z', '+00:00'))).total_seconds()
        except (ValueError, TypeError):
            pass
    status = 'current' if age is not None and 0 <= age <= 8 * 3600 else 'stale' if stamp else 'not_refreshed'
    if attempt.get('state') == 'failed':
        status = 'refresh_failed'
    return {'status': status, 'last_success_at': stamp,
            'last_attempt_at': attempt.get('attempted_at'),
            'records': len(data.get('records', [])),
            'mailchimp_reports_refreshed': data.get('mailchimp_reports_refreshed'),
            'eventbrite_status': 'manual_observations_only',
            'refresh_interval_hours': 6, 'coverage': data.get('coverage'),
            'error': attempt.get('error')}


def nonnegative(value):
    return value if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0 else None


def normalize_report(report, previous, stamp):
    cid = report.get('id')
    if not isinstance(cid, str) or not re.fullmatch(r'[a-zA-Z0-9]+', cid):
        raise ValueError('Report identity is invalid')
    if previous and previous.get('id') != cid:
        raise ValueError('Report identity changed')
    row = dict(previous or {}, provider='mailchimp', id=cid, observed_at=stamp)
    sent = nonnegative(report.get('emails_sent'))
    bounces = report.get('bounces') or {}
    hard, soft = nonnegative(bounces.get('hard_bounces')), nonnegative(bounces.get('soft_bounces'))
    delivered = sent-hard-soft if None not in (sent, hard, soft) and hard+soft <= sent else None
    observed = {'subject': report.get('subject_line'), 'sent_at': report.get('send_time'),
                'audience': report.get('list_name'), 'delivered': delivered,
                'clickers': nonnegative((report.get('clicks') or {}).get('unique_subscriber_clicks')),
                'unsubscribes': nonnegative(report.get('unsubscribed')),
                'bounce_rate': (100*(hard+soft)/sent) if delivered is not None and sent else None}
    # Never turn absent provider fields into fabricated zero corrections.
    row.update({k:v for k,v in observed.items() if v is not None})
    row.setdefault('category', 'needs_review')
    row.setdefault('category_basis', 'unresolved')
    row.setdefault('content_verified', False)
    row.setdefault('note', 'Provider-reported engagement; ticket-sales attribution is unverified.')
    return row


def collect_reports(client, since):
    rows, seen = [], set()
    total = None
    for page in range(100):
        payload = client.reports.get_all_campaign_reports(count=100, offset=page*100,
                    since_send_time=since, _request_timeout=20)
        batch = payload.get('reports') if isinstance(payload, dict) else None
        count = payload.get('total_items') if isinstance(payload, dict) else None
        if not isinstance(batch, list) or not isinstance(count, int) or isinstance(count, bool) or count < 0:
            raise ValueError('Report pagination is incomplete')
        if total is None:
            total = count
        if count != total:
            raise ValueError('Report set changed during pagination; retry next cycle')
        for row in batch:
            cid = row.get('id') if isinstance(row, dict) else None
            if not cid or cid in seen:
                raise ValueError('Report page repeats or omits an identity')
            seen.add(cid)
            rows.append(row)
        if len(rows) == total:
            return rows
        if not batch or len(rows) > total:
            raise ValueError('Report pagination is incomplete')
    raise ValueError('Report pagination exceeded supported size')


def sdk_client():
    from mailchimp_marketing import Client
    key = os.environ.get('MAILCHIMP_API_KEY', '')
    server = key.rsplit('-', 1)[-1]
    if not key or not re.fullmatch(r'us\d+', server):
        raise ValueError('Mailchimp connection is unavailable')
    client = Client()
    client.set_config({'api_key': key, 'server': server})
    return client


def refresh_feedback(client=None, now=None):
    now = now or utcnow()
    stamp = now.isoformat()
    path = feedback_path()
    try:
        previous = load_evidence()
        rows = {(r['provider'], r['id']): dict(r) for r in previous['records']}
        # Revisit recent sends as late opens, bounces and clicks arrive. The
        # imported older history is retained, not presented as refreshed.
        reports = collect_reports(client or sdk_client(), (now-timedelta(days=90)).isoformat())
        for report in reports:
            key = ('mailchimp', report.get('id'))
            rows[key] = normalize_report(report, rows.get(key), stamp)
        snapshot = {'version':1, 'records': list(rows.values()), 'source':'mailchimp_sdk_plus_imports',
                    'last_success_at':stamp, 'mailchimp_reports_refreshed':len(reports),
                    'coverage':'Mailchimp reports sent in the last 90 days refresh every 6 hours. Older Mailchimp and Eventbrite observations retain their original timestamps. New campaigns need category/content verification before creative ranking.'}
        atomic_json(path, snapshot)
        atomic_json(str(path)+'.status', {'state':'success','attempted_at':stamp})
        return {'status':'success','reports_refreshed':len(reports),'records':len(rows)}
    except Exception:
        # SDK exceptions can include URLs/body/credentials. Store no raw error.
        atomic_json(str(path)+'.status', {'state':'failed','attempted_at':stamp,
                                       'error':'mailchimp_feedback_refresh_failed'})
        return {'status':'failed','error':'mailchimp_feedback_refresh_failed'}
