from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from action_plan import build_action_plan, operational_checks

NOW=datetime(2026,9,16,12,tzinfo=timezone.utc)

class ActionPlanTests(unittest.TestCase):
    def setup_engines(self):
        self.db=Mock()
        self.db.get_event.side_effect=lambda eid: {'event_id':eid} if eid in ['1','2'] else None
        self.db.edition_sibling_ids.return_value=['1','2']
        self.db.last_sync_run.return_value={'status':'completed','finished_at':NOW.isoformat()}
        portfolio=[SimpleNamespace(event_id='day1',constituent_event_ids=['1']),SimpleNamespace(event_id='day2',constituent_event_ids=['2'])]
        self.engine=Mock()
        self.engine.decision_engine.analyze_portfolio.return_value=portfolio
        self.engine.command_summary.return_value={'opportunities':[{'event_id':p.event_id,'event_name':p.event_id,'opportunity_id':p.event_id,'rationale':'Behind pace'} for p in portfolio]}
        self.engine.get_event_context.return_value={'event_type':'wine','city':'Example'}
        self.diagnosis=Mock()
        self.diagnosis.diagnose_grouped.return_value.to_dict.return_value={'intervention_options':[{'intervention_type':'crm_campaign','label':'Prepare audience'}], 'recommended_intervention':'crm_campaign','days_until':20,'pace_delta_pct':-15,'recent_velocity':12}
        self.repo=Mock()
        self.repo.list_interventions.return_value=[]

    @patch('action_plan.feedback_status',return_value={'status':'current','last_success_at':NOW.isoformat()})
    def test_one_action_per_edition_and_work_on_other_day_is_preserved(self,_):
        self.setup_engines()
        self.repo.list_interventions.return_value=[SimpleNamespace(id='i',event_id='day2',status=SimpleNamespace(value='draft'),intervention_type='crm_campaign')]
        result=build_action_plan(self.engine,self.diagnosis,self.db,self.repo,NOW)
        self.assertEqual(len(result['actions']),1)
        action=result['actions'][0]
        self.assertEqual(action['related_pacing_views'],['day2'])
        self.assertEqual(action['existing_interventions'][0]['id'],'i')
        self.assertEqual(action['preparation_status'],'existing_work_review_first')
        self.assertFalse(action['execution_allowed'])
        self.engine.decision_engine.analyze_portfolio.assert_called_once()
        self.assertEqual(self.diagnosis.diagnose_grouped.call_count,1)

    @patch('action_plan.feedback_status',return_value={'status':'stale','last_success_at':None})
    def test_missing_scope_never_fabricates_action(self,_):
        self.setup_engines(); self.db.get_event.return_value=None;self.db.get_event.side_effect=None
        result=build_action_plan(self.engine,self.diagnosis,self.db,self.repo,NOW)
        self.assertFalse(result['actions']);self.assertEqual(len(result['action_failures']),2)

    @patch('action_plan.feedback_status',return_value={'status':'stale','last_success_at':None})
    def test_old_failed_and_integrity_warning_syncs_are_not_current(self,_):
        self.setup_engines()
        for state,stamp,expected in [('completed','2026-09-10T12:00:00','stale'),('failed',NOW.isoformat(),'failed'),('completed_with_integrity_warnings',NOW.isoformat(),'integrity_warnings'),('completed','2026-09-17T12:00:00','unknown')]:
            self.db.last_sync_run.return_value={'status':state,'finished_at':stamp}
            self.assertEqual(operational_checks(self.db,NOW)[0]['status'],expected)

class ActionPlanBoundaryTests(unittest.TestCase):
    def test_authenticated_read_only_action_plan(self):
        import os
        with patch.dict(os.environ,{'TESTING':'1','CRAFT_AUTO_SYNC':'0','DB_PATH':':memory:','COMMAND_API_KEY':'action-test','MAILCHIMP_API_KEY':'','MAILCHIMP_AUDIENCE_ID':''}):
            from craft_v2 import _build_app
            client=_build_app().test_client()
            path='/api/intelligence/action-plan'
            self.assertEqual(client.get(path).status_code,401)
            response=client.get(path,headers={'Authorization':'Bearer action-test'})
            self.assertEqual(response.status_code,200)
            self.assertFalse(response.json['execution_allowed'])
            self.assertIn('no-store',response.headers['Cache-Control'])
            self.assertEqual(client.post(path,headers={'Authorization':'Bearer action-test'}).status_code,405)
