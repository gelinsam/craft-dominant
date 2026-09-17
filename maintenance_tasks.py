"""Isolate existing maintenance callbacks and record their last outcome.

Uses the existing six-hour scheduler and private atomic snapshot writer.
This is not an execution queue and never retries customer-facing mutations.
"""
from datetime import datetime, timezone
from campaign_feedback import atomic_json, feedback_path, read_json


def status_path():
    return str(feedback_path()) + '.maintenance'


def maintenance_status(now=None):
    now = now or datetime.now(timezone.utc)
    result = read_json(status_path()) or {'status': 'not_checked', 'tasks': []}
    try:
        observed = datetime.fromisoformat(result['observed_at'])
        if not 0 <= (now-observed).total_seconds() <= 8*3600:
            result = dict(result, status='stale')
    except (KeyError, TypeError, ValueError):
        result = dict(result, status='not_checked')
    return result


def run_maintenance(tasks, now=None):
    now = now or datetime.now(timezone.utc)
    result = {'status': 'current', 'observed_at': now.isoformat(), 'tasks': []}
    for task in tuple(tasks):
        name, callback = task if isinstance(task, tuple) else (getattr(task, '__name__', 'maintenance'), task)
        row = {'name': name, 'status': 'completed'}
        try:
            value = callback()
            if isinstance(value, dict):
                state = value.get('status') or value.get('state')
                if isinstance(state, str):
                    row['result'] = state
                    if any(word in state for word in ('failed', 'unavailable', 'error')):
                        row['status'] = 'failed'
        except Exception:
            # Provider exception text can contain credentials or customer data.
            row['status'] = 'failed'
        result['tasks'].append(row)
    if any(r['status'] == 'failed' for r in result['tasks']):
        result['status'] = 'partial_failure'
    atomic_json(status_path(), result)
    return result
