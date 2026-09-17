"""Live festival action briefs built from existing Craft diagnosis and evidence.

No new ranking model or execution path. Requests only read current stored data.
Edition identity is reused so different days do not create duplicate campaigns.
"""
from datetime import datetime, timezone
import hashlib
import os
from campaign_feedback import feedback_status
from channel_health import meta_health


def age_hours(value, now):
    if not isinstance(value, str):
        return None
    try:
        stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
        # Existing SQLite sync timestamps are naive UTC, not local event dates.
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        age = (now-stamp).total_seconds()/3600
        return age if age >= 0 else None
    except (ValueError, TypeError):
        return None


def operational_checks(db, now):
    checks = []
    try:
        run = db.last_sync_run() or {}
        age = age_hours(run.get('finished_at'), now)
        state = run.get('status')
        if state in ('running','interrupted','failed'):
            status = state
        elif age is None:
            status = 'unknown'
        elif age > 24:
            status = 'stale'
        elif state == 'completed_with_integrity_warnings':
            status = 'integrity_warnings'
        elif state == 'completed':
            status = 'current'
        else:
            status = 'unknown'
        checks.append({'id':'sales_sync','label':'Ticket and customer data','status':status,
                       'observed_at':run.get('finished_at'),'age_hours':round(age,1) if age is not None else None,
                       'action':'Review sync status and integrity warnings before acting on pacing.' if status!='current' else 'Latest completed sync is within 24 hours.'})
    except Exception:
        checks.append({'id':'sales_sync','label':'Ticket and customer data','status':'unavailable',
                       'action':'Sales sync status could not be verified.'})
    try:
        feedback = feedback_status(now)
        checks.append({'id':'campaign_feedback','label':'Campaign results','status':feedback['status'],
                       'observed_at':feedback['last_success_at'],'action':feedback.get('coverage') or 'Waiting for the first provider refresh.'})
    except Exception:
        checks.append({'id':'campaign_feedback','label':'Campaign results','status':'unavailable',
                       'action':'Stored feedback could not be read.'})
    from channel_health import meta_health
    from measurement_cycle import measurement_status
    meta = meta_health(now)
    checks.append({'id':'meta_connection','label':'Meta connection','status':meta['status'],
                   'observed_at':meta.get('observed_at'),'action':meta.get('detail','A current read-access check is not available.')})
    measurement = measurement_status(now)
    checks.append({'id':'measurement','label':'Attributed campaign results','status':measurement['status'],
                   'observed_at':measurement.get('observed_at'),
                   'action':'Existing executed interventions are measured every six hours when sales data is fresh and complete. Results are attributed, not incremental sales lift.'})
    from maintenance_tasks import maintenance_status
    maintenance = maintenance_status(now)
    failed = [r['name'] for r in maintenance.get('tasks', []) if r.get('status') == 'failed']
    checks.append({'id':'preparation_health','label':'Automatic preparation','status':maintenance['status'],
                   'observed_at':maintenance.get('observed_at'),
                   'action':('Needs retry: '+', '.join(failed)+'. Other tasks continue independently.') if failed else 'Existing background tasks run independently; last outcomes are recorded.'})
    checks.append({'id':'eventbrite_history','label':'Eventbrite delivery evidence','status':'partial',
                   'action':'Browser observations are partial. Do not infer failed delivery from a missing report.'})
    checks.append({'id':'external_execution','label':'Customer sends and ad changes','status':'approval_required',
                   'action':'This action queue prepares recommendations only; it cannot send, schedule or change budgets.'})
    return checks


def edition_ids(db, pacing):
    seeds = list(getattr(pacing, 'constituent_event_ids', None) or [])
    seeds.append(pacing.event_id)
    for seed in seeds:
        if db.get_event(seed):
            ids = db.edition_sibling_ids(seed)
            if ids and seed in ids:
                return tuple(sorted(set(ids)))
    raise ValueError('Edition identity unresolved')


def channel_readiness(event_ids):
    from audience_routing import event_audience_id
    mc = 'not_configured'
    if os.environ.get('MAILCHIMP_API_KEY'):
        try:
            mapped = {event_audience_id(eid) for eid in event_ids}
            mc = 'mapping_present_consent_check_required' if len(mapped)==1 else 'conflicting_audience_mapping'
        except RuntimeError:
            mc = 'audience_mapping_required'
    return {'mailchimp':mc, 'eventbrite':'browser_draft_and_recipient_verification_required',
            'meta':meta_health()['status']}


def build_action_plan(opportunity_engine, diagnosis_engine, db, repo, now=None):
    now = now or datetime.now(timezone.utc)
    portfolio = opportunity_engine.decision_engine.analyze_portfolio()
    summary = opportunity_engine.command_summary(portfolio=portfolio)
    checks = operational_checks(db, now)
    by_id = {p.event_id:p for p in portfolio}
    actions, editions, failures = [], {}, []
    try:
        interventions = repo.list_interventions(include_terminal=False)
        intervention_status = 'available'
    except Exception:
        interventions = []
        intervention_status = 'unavailable'
    scopes = {}
    for pacing in portfolio:
        try:
            scopes[pacing.event_id] = edition_ids(db, pacing)
        except ValueError:
            pass
    for opp in summary['opportunities']:
        pacing = by_id.get(opp['event_id'])
        if pacing is None:
            failures.append({'event_id':opp['event_id'],'reason':'pacing_unavailable'})
            continue
        try:
            ids = edition_ids(db, pacing)
            if ids in editions:
                editions[ids]['related_pacing_views'].append(opp['event_name'])
                continue
            context = opportunity_engine.get_event_context(pacing)
            if not context.get('event_type') or not context.get('city'):
                raise ValueError('Festival scope unresolved')
            if any(word in opp['event_name'].lower() for word in ('exhibitor','vendor','sponsor')):
                continue
            diagnosis = (diagnosis_engine.diagnose_grouped(pacing, opp, context)
                         if getattr(pacing, 'constituent_event_ids', None)
                         else diagnosis_engine.diagnose(pacing.event_id, opp)).to_dict()
            options = diagnosis.get('intervention_options', [])
            choice = diagnosis.get('recommended_intervention')
            selected = next((o for o in options if o['intervention_type']==choice), None)
            matching = [i for i in interventions if i.event_id==opp['event_id'] or i.event_id in ids or scopes.get(i.event_id)==ids]
            active = [{'id':i.id,'status':i.status.value,'type':i.intervention_type} for i in matching]
            card = {'action_id':hashlib.sha256(('|'.join(ids)).encode()).hexdigest()[:16],
                    'event_id':opp['event_id'],'event_ids':list(ids),'event_name':opp['event_name'],
                    'city':context['city'],'event_type':context['event_type'],
                    'opportunity_id':opp['opportunity_id'],'related_pacing_views':[],
                    'priority_basis':'Existing confidence-weighted opportunity estimate; not measured lift',
                    'title':('Prepare a festival recovery draft' if choice=='crm_campaign' else selected['label']) if selected else 'Review the evidence before choosing an action',
                    'rationale':(opp['rationale']+' Rebuild the exact festival audience, then verify provider consent, delivery history and current-edition purchases. CRM candidates are not confirmed reachable recipients.') if choice=='crm_campaign' else diagnosis.get('recommendation_rationale') or opp['rationale'],
                    'options':options,'recommended_intervention':choice,
                    'days_until':diagnosis['days_until'],'pace_delta_pct':diagnosis['pace_delta_pct'],
                    'recent_velocity':diagnosis['recent_velocity'],
                    'missing_data':diagnosis.get('missing_data',[]),
                    'channel_readiness':channel_readiness(ids),'existing_interventions':active,
                    'preparation_status':'existing_work_review_first' if active else 'review_evidence' if intervention_status!='available' else 'can_prepare',
                    'data_current':next((x['status']=='current' for x in checks if x['id']=='sales_sync'),False),
                    'execution_allowed':False}
            editions[ids] = card
            actions.append(card)
        except Exception:
            # One invalid festival must not take the whole operating home down.
            failures.append({'event_id':opp['event_id'],'reason':'diagnosis_or_scope_unavailable'})
    return dict(summary, actions=actions, operational_checks=checks,
                action_failures=failures, intervention_status=intervention_status,
                refresh_policy={'page_seconds':60,'mailchimp_hours':6,
                                'sales':'Existing sales sync; recommendations never pretend a page refresh syncs providers'},
                execution_allowed=False)

