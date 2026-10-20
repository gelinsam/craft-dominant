from copy import deepcopy
from datetime import datetime, timezone
import os
import unittest
from unittest.mock import patch
from chief_of_staff import build_brief

NOW = datetime(2026, 10, 7, 20, tzinfo=timezone.utc)

def fixture():
    return {'intervention_status': 'available', 'operational_checks': [
        {'id': 'sales_sync', 'status': 'current'},
        {'id': 'external_execution', 'status': 'approval_required'}],
        'actions': [{'action_id': 'edition1', 'event_ids': ['1', '2'],
                     'event_name': 'Example Coffee', 'opportunity_id': 'opp1',
                     'data_current': True, 'recommended_intervention': 'crm_campaign',
                     'existing_interventions': [], 'missing_data': []}]}

class BriefTests(unittest.TestCase):
    def test_routes_without_dispatch_or_claimed_margin_and_preserves_input(self):
        plan = fixture(); before = deepcopy(plan)
        brief = build_brief(plan, NOW)
        self.assertEqual(plan, before)
        self.assertEqual(brief['work'][0]['owner'], 'crm_email')
        self.assertEqual(brief['work'][0]['event_ids'], ['1', '2'])
        self.assertEqual(brief['work'][0]['state'], 'ready_for_internal_preparation')
        self.assertFalse(brief['dispatch_enabled'])
        self.assertFalse(brief['external_execution_allowed'])
        self.assertIsNone(brief['metrics']['contribution_per_founder_hour'])

    def test_missing_stale_failed_sales_and_repository_failure_hold(self):
        for status in ('stale', 'failed', 'unknown', 'integrity_warnings', 'running'):
            plan = fixture(); plan['operational_checks'][0]['status'] = status
            self.assertIn('sales_evidence_not_current', build_brief(plan)['work'][0]['blockers'])
        plan = fixture(); plan['operational_checks'] = []; plan.pop('intervention_status')
        self.assertEqual(len(build_brief(plan)['work'][0]['blockers']), 2)

    def test_existing_work_unknown_type_and_missing_evidence_hold(self):
        plan = fixture(); action = plan['actions'][0]
        action.update(existing_interventions=[{'id': 'old', 'status': 'proposed'}],
                      missing_data=['consent'], recommended_intervention='unrecognized')
        brief = build_brief(plan)
        self.assertEqual(brief['work'][0]['state'], 'held')
        self.assertEqual(len(brief['work'][0]['blockers']), 3)
        self.assertEqual(brief['work'][0]['existing_interventions'][0]['id'], 'old')

    def test_existing_paid_media_diagnosis_routes_to_meta_without_budget_authority(self):
        plan = fixture()
        plan['actions'][0]['recommended_intervention'] = 'paid_media_review'
        work = build_brief(plan)['work'][0]
        self.assertEqual(work['owner'], 'meta_marketing')
        self.assertFalse(work['external_execution_allowed'])

    def test_digest_tracks_evidence_not_render_time_and_failures_visible(self):
        plan = fixture(); first = build_brief(plan, NOW)
        self.assertEqual(first['source_digest'], build_brief(plan)['source_digest'])
        plan['action_failures'] = [{'event_id': '3', 'reason': 'scope_unknown'}]
        second = build_brief(plan)
        self.assertNotEqual(first['source_digest'], second['source_digest'])
        self.assertEqual(second['exceptions'][0]['reason'], 'scope_unknown')

    def test_authenticated_get_only_and_no_store(self):
        with patch.dict(os.environ, {'TESTING': '1', 'CRAFT_AUTO_SYNC': '0',
                        'DB_PATH': ':memory:', 'COMMAND_API_KEY': 'brief-test',
                        'MAILCHIMP_API_KEY': '', 'MAILCHIMP_AUDIENCE_ID': ''}):
            from craft_v2 import _build_app
            client = _build_app().test_client(); path = '/api/intelligence/chief-of-staff'
            self.assertEqual(client.get(path).status_code, 401)
            headers = {'Authorization': 'Bearer brief-test'}
            with patch('action_plan.build_action_plan', return_value=fixture()):
                response = client.get(path, headers=headers)
            self.assertEqual(response.status_code, 200)
            self.assertFalse(response.json['dispatch_enabled'])
            self.assertIn('no-store', response.headers['Cache-Control'])
            self.assertEqual(client.post(path, headers=headers).status_code, 405)
