"""Private, append-only observations using the existing SQLite storage stack.

Observation time is not publication time or proof a creative caused sales.
No contact records, credentials, provider paging URLs or remote mutations.
"""
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
from datetime import datetime, timezone

MAX_BYTES = 40_000_000


def archive_path():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent / 'launch_evidence.sqlite3'


def capture(report, campaigns, path=None):
    path = Path(path) if path else archive_path()
    # Deliberate allowlist: no full provider responses or customer rows.
    editions = [{k: e.get(k) for k in ('city', 'date', 'end_date', 'event_ids',
        'tickets', 'revenue', 'spend', 'unknown_tickets', 'unknown_revenue',
        'warnings', 'daily', 'lifecycle')} for e in report['editions']]
    ads = []
    for item in campaigns:
        c = item.get('campaign', {})
        ads.append({'account_id': item.get('account_id'),
            'campaign': {k: c.get(k) for k in ('id', 'name', 'status', 'objective')},
            'creative_observed_at': item.get('creative_observed_at'),
            'days': [{k: day.get(k) for k in ('date_start','date_stop','spend','impressions','clicks','actions')} for day in item.get('days',[])],
            'ads': [{**{k: a.get(k) for k in ('id', 'name', 'created_time')},
                     'creative': {k: (a.get('creative') or {}).get(k) for k in
                     ('id','body','title','instagram_permalink_url','video_id','effective_object_story_id')}}
                    for a in item.get('ads', [])]})
    value = {'editions': editions, 'campaigns': ads,
             'meta_collected_at': report.get('meta_collected_at')}
    raw = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
    if len(raw) > MAX_BYTES: raise ValueError('Evidence snapshot exceeds size limit')
    digest = hashlib.sha256(raw).hexdigest()
    observed = datetime.now(timezone.utc).isoformat()
    fd = os.open(path, os.O_CREAT | os.O_WRONLY, 0o600); os.close(fd)
    os.chmod(path, 0o600)
    with sqlite3.connect(path, timeout=30) as con:
        con.execute('CREATE TABLE IF NOT EXISTS observations (id INTEGER PRIMARY KEY, observed_at TEXT NOT NULL, digest TEXT NOT NULL, payload BLOB NOT NULL, UNIQUE(observed_at, digest))')
        con.execute('BEGIN IMMEDIATE')
        last = con.execute('SELECT digest, observed_at FROM observations ORDER BY id DESC LIMIT 1').fetchone()
        # Unchanged runs in the same UTC day need no duplicate payload.
        if last and last[0] == digest and last[1][:10] == observed[:10]:
            return {'status': 'unchanged'}
        con.execute('INSERT INTO observations(observed_at,digest,payload) VALUES(?,?,?)',
                    (observed, digest, gzip.compress(raw)))
    return {'status': 'recorded', 'observed_at': observed}


def status(path=None):
    path = Path(path) if path else archive_path()
    if not path.exists(): return {'status': 'not_started', 'snapshots': 0}
    try:
        with sqlite3.connect(path.as_uri()+'?mode=ro', uri=True, timeout=5) as con:
            row = con.execute('SELECT COUNT(*), MIN(observed_at), MAX(observed_at) FROM observations').fetchone()
        age = (datetime.now(timezone.utc) - datetime.fromisoformat(row[2])).total_seconds() if row[2] else float('inf')
        return {'status': 'recording' if age < 172800 else 'stale', 'snapshots': row[0], 'first_observed_at': row[1], 'last_observed_at': row[2]}
    except sqlite3.Error:
        return {'status': 'unavailable', 'snapshots': None}
