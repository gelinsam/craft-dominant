"""Read-only work routing over Craft's existing edition-aware action plan.

Suggestions are not dispatched jobs. Durable execution remains in the existing
intervention repository and DBOS worker; this module has no provider tools.
"""
from datetime import datetime, timezone
from hashlib import sha256
import json


OWNERS = {'crm_campaign': 'crm_email', 'paid_media_review': 'meta_marketing',
          'ad_budget_shift': 'meta_marketing',
          'price_adjust': 'revenue_pacing'}


def build_brief(plan, now=None):
    now = now or datetime.now(timezone.utc)
    checks = plan.get('operational_checks', [])
    sales_current = any(c.get('id') == 'sales_sync' and c.get('status') == 'current'
                        for c in checks)
    repository_ready = plan.get('intervention_status') == 'available'
    work = []
    for action in plan.get('actions', []):
        blockers = []
        if not sales_current or action.get('data_current') is not True:
            blockers.append('sales_evidence_not_current')
        if not repository_ready:
            blockers.append('durable_intervention_state_unavailable')
        if action.get('missing_data'):
            blockers.append('diagnosis_evidence_incomplete')
        existing = action.get('existing_interventions', [])
        if existing:
            blockers.append('review_existing_work_before_preparing')
        choice = action.get('recommended_intervention')
        owner = OWNERS.get(choice, 'revenue_pacing')
        if choice not in OWNERS:
            blockers.append('intervention_requires_revenue_review')
        work.append({'action_id': action['action_id'], 'event_ids': action['event_ids'],
                     'event_name': action['event_name'], 'owner': owner,
                     'state': 'held' if blockers else 'ready_for_internal_preparation',
                     'blockers': blockers, 'existing_interventions': existing,
                     'next_step': 'Review evidence and existing interventions.' if blockers
                                  else 'Prepare evidence and a proposal through the existing workflow.',
                     'handoff': ['independent_qa', 'chief_of_staff', 'human_approval'],
                     'source': {'opportunity_id': action['opportunity_id'],
                                'pace_delta_pct': action.get('pace_delta_pct'),
                                'days_until': action.get('days_until'),
                                'channel_readiness': action.get('channel_readiness', {})},
                     'external_execution_allowed': False})
    exceptions = [dict(c, owner='revenue_pacing' if c.get('id') == 'sales_sync'
                       else 'chief_of_staff') for c in checks
                  if c.get('status') not in ('current', 'approval_required')]
    for failure in plan.get('action_failures', []):
        exceptions.append(dict(failure, owner='revenue_pacing'))
    if not repository_ready:
        exceptions.append({'id': 'intervention_state', 'status': 'unavailable',
                           'owner': 'chief_of_staff'})
    # Digest excludes render time, so unchanged evidence has stable provenance.
    digest = sha256(json.dumps(plan, sort_keys=True, allow_nan=False).encode()).hexdigest()
    return {'schema_version': 1, 'generated_at': now.isoformat(),
            'source_digest': digest, 'mode': 'read_only', 'dispatch_enabled': False,
            'objective': 'Incremental profitable events and contribution margin per founder hour',
            'work': work, 'exceptions': exceptions,
            'coverage': plan.get('data_quality', {}),
            'metrics': {'incremental_contribution_margin': None,
                        'founder_hours': None, 'contribution_per_founder_hour': None,
                        'additional_profitable_events': None,
                        'status': 'unmeasured',
                        'reason': 'No reconciled event contribution ledger or founder-time baseline.'},
            'approval_policy': {'outbound_email': 'explicit_human_approval',
                                'financial_commitment': 'explicit_human_approval',
                                'live_ad_budget_change': 'explicit_human_approval'},
            'external_execution_allowed': False}
