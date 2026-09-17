"""Schedule the existing attributed-results method; never execute a send.

Fresh, completed sales sync evidence gates measurement. Final windows need a
sync completed after their end. Per-intervention failures remain visible and
retry on the next existing maintenance cycle.
"""
from datetime import datetime, timezone
from campaign_feedback import atomic_json, feedback_path, read_json


def stamp(value):
    try:
        value=datetime.fromisoformat(value.replace('Z','+00:00'))
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    except (TypeError,ValueError,AttributeError):
        return None


def status_path():
    return str(feedback_path())+'.measurement'


def measurement_status(now=None):
    now=now or datetime.now(timezone.utc)
    result=read_json(status_path()) or {'status':'not_checked','observed_at':None}
    observed=stamp(result.get('observed_at'))
    if observed is not None and not 0<=(now-observed).total_seconds()<=8*3600:
        result=dict(result,status='stale')
    return result


def refresh_measurements(db, repo, adapter, now=None):
    now=now or datetime.now(timezone.utc)
    result={'status':'current','observed_at':now.isoformat(),'measured':0,'completed':0,'held':0,'failed':0}
    try:
        pending=[i for i in repo.list_interventions(include_terminal=False) if i.status.value=='measuring']
        result['pending']=len(pending)
        if not pending:
            result['status']='no_pending_measurements'
        else:
            run=db.last_sync_run() or {}
            synced=stamp(run.get('finished_at'))
            fresh=(run.get('status')=='completed' and synced is not None and 0<=(now-synced).total_seconds()<=24*3600)
            for item in pending:
                end=stamp(item.measurement_ends_at)
                event_synced = synced if fresh else None
                from sales_evidence import verified_edition_at, has_current_evidence
                if run.get('status') == 'completed_with_integrity_warnings' or has_current_evidence(run):
                    event_synced = verified_edition_at(db, getattr(item, 'event_id', None), run, now)
                if event_synced is None or end is None or (now>=end and event_synced<end):
                    result['held']+=1
                    continue
                try:
                    measured=adapter.measure(item.id,actor='scheduled_measurement')
                    if measured.get('error'):
                        result['failed']+=1
                    elif measured.get('status')=='learned' and repo.get_learning(item.id) is None:
                        # Never represent a missing learning record as complete.
                        result['failed']+=1
                    else:
                        result['measured']+=1
                        result['completed']+=int(measured.get('status')=='learned')
                except Exception:
                    result['failed']+=1
            if result['failed']:
                result['status']='measurement_failed'
            elif result['held']:
                result['status']='waiting_for_complete_sales_evidence'
    except Exception:
        result['status']='measurement_unavailable'
    atomic_json(status_path(),result)
    return result

