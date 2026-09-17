"""Edition-specific sales completeness from the existing full sync.

No provider writes, additional scheduler or relaxed global integrity status.
Receipts contain counts and event IDs only, never customers or order IDs.
"""
from datetime import datetime, timezone
import os
from pathlib import Path
from campaign_feedback import atomic_json, read_json


def evidence_path():
    return Path(os.environ.get('DB_PATH', 'craft_unified.db')).resolve().parent/'sales-sync-evidence.json'


def stamp(value):
    try:
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    except (AttributeError, ValueError, TypeError):
        return None


def event_receipt(db, event_id, source_count, parsed_ids, before_tickets, started_at, fields_observed=True):
    row = db.conn.execute('SELECT COUNT(*) n, SUM(ticket_count) tickets, '
        'SUM(CASE WHEN ticket_count IS NULL OR gross_amount IS NULL THEN 1 ELSE 0 END) unknown '
        'FROM orders WHERE event_id = ?', (event_id,)).fetchone()
    stored, tickets, unknown = row['n'], row['tickets'] or 0, row['unknown'] or 0
    complete = (fields_observed and source_count == len(parsed_ids) == len(set(parsed_ids)) == stored
                and not unknown and tickets >= (before_tickets or 0))
    return {'event_id': str(event_id), 'status': 'complete' if complete else 'incomplete',
            'read_started_at': started_at, 'source_orders': source_count,
            'parsed_orders': len(parsed_ids), 'stored_orders': stored,
            'unknown_orders': unknown, 'fields_observed': fields_observed,
            'ticket_decrease': tickets < (before_tickets or 0)}


def save_evidence(result, run):
    if run.get('status') not in ('completed', 'completed_with_integrity_warnings'):
        return
    atomic_json(evidence_path(), {'run_id': run['id'], 'finished_at': run['finished_at'],
                                 'events': result.get('event_evidence', [])})


def has_current_evidence(run):
    value = read_json(evidence_path()) or {}
    return run.get('id') is not None and value.get('run_id') == run['id'] and value.get('finished_at') == run.get('finished_at')


def verified_edition_at(db, event_id, run, now=None):
    """Unknown, partial, stale, mismatched and interrupted evidence stays held."""
    now = now or datetime.now(timezone.utc)
    if run.get('status') not in ('completed', 'completed_with_integrity_warnings'):
        return None
    finished = stamp(run.get('finished_at'))
    if finished is None or not 0 <= (now-finished).total_seconds() <= 86400:
        return None
    value = read_json(evidence_path()) or {}
    if run.get('id') is None or value.get('run_id') != run['id'] or value.get('finished_at') != run['finished_at']:
        return None
    if not db.get_event(event_id):
        return None
    ids = {str(i) for i in db.edition_sibling_ids(event_id)}
    if not ids or str(event_id) not in ids:
        return None
    rows = value.get('events', [])
    by_id = {str(r['event_id']): r for r in rows}
    if len(by_id) != len(rows):
        return None
    times = []
    for eid in ids:
        row = by_id.get(eid, {})
        read_at = stamp(row.get('read_started_at'))
        if row.get('status') != 'complete' or read_at is None or not 0 <= (now-read_at).total_seconds() <= 86400 or read_at > finished:
            return None
        times.append(read_at)
    return min(times)
