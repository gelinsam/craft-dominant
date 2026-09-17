import base64
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import zlib
import campaign_feedback as feedback

NOW = datetime(2026, 9, 16, 12, tzinfo=timezone.utc)

class CampaignFeedbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env = patch.dict(os.environ, {'DB_PATH': str(Path(self.temp.name)/'db.sqlite'),
                                           'CRAFT_CAMPAIGN_EVIDENCE_ZLIB_B64': ''})
        self.env.start(); self.addCleanup(self.env.stop)

    def client(self, pages):
        reports = Mock()
        reports.get_all_campaign_reports.side_effect = pages
        return SimpleNamespace(reports=reports)

    def test_partial_refresh_retains_last_good_snapshot_and_no_raw_error(self):
        original = {'version':1,'records':[{'provider':'eventbrite','id':'123'}],
                    'last_success_at':NOW.isoformat()}
        feedback.atomic_json(feedback.feedback_path(), original)
        client = self.client([{'reports':[{'id':'a'}], 'total_items':2},
                              RuntimeError('secret-credential-in-provider-url')])
        result = feedback.refresh_feedback(client, NOW)
        self.assertEqual(result['status'],'failed')
        self.assertEqual(feedback.load_evidence(),original)
        status=feedback.feedback_status(NOW)
        self.assertEqual(status['status'],'refresh_failed')
        self.assertNotIn('secret',json.dumps(status))

    def test_complete_pages_merge_without_erasing_imports(self):
        seed={'records':[{'provider':'eventbrite','id':'123'},
                         {'provider':'mailchimp','id':'old','subject':'Original'},
                         {'provider':'mailchimp','id':'a','clickers':12,'category':'coffee','content_verified':True}]}
        os.environ['CRAFT_CAMPAIGN_EVIDENCE_ZLIB_B64']=base64.b64encode(zlib.compress(json.dumps(seed).encode())).decode()
        c=self.client([{'reports':[{'id':'a','emails_sent':1000,'bounces':{'hard_bounces':2,'soft_bounces':3},'clicks':{'unique_subscriber_clicks':0}}], 'total_items':2},
                       {'reports':[{'id':'b'}], 'total_items':2}])
        self.assertEqual(feedback.refresh_feedback(c,NOW)['status'],'success')
        rows={r['id']:r for r in feedback.load_evidence()['records']}
        self.assertEqual(len(rows),4)
        self.assertEqual(rows['a']['clickers'],0)
        self.assertEqual(rows['a']['delivered'],995)
        self.assertEqual(rows['a']['category'],'coffee')
        self.assertEqual(rows['b']['category'],'needs_review')
        self.assertFalse(rows['b']['content_verified'])
        self.assertNotIn('delivered',rows['b'])
        self.assertEqual(feedback.feedback_status(NOW)['status'],'current')
        self.assertEqual(feedback.feedback_status(NOW+timedelta(hours=9))['status'],'stale')
        self.assertEqual(c.reports.get_all_campaign_reports.call_args.kwargs['offset'],100)

    def test_unobserved_fields_preserved_and_invalid_numbers_rejected(self):
        old={'id':'a','clickers':10,'delivered':1000}
        row=feedback.normalize_report({'id':'a','clicks':{'unique_subscriber_clicks':None}},old,NOW.isoformat())
        self.assertEqual(row['clickers'],10)
        self.assertEqual(row['delivered'],1000)
        for value in [float('inf'),float('nan'),-1,True,'0']:
            self.assertIsNone(feedback.nonnegative(value))

    def test_duplicate_changed_and_truncated_pagination_rejected(self):
        cases=[ [{'reports':[{'id':'a'}],'total_items':2},{'reports':[{'id':'a'}],'total_items':2}],
                [{'reports':[{'id':'a'}],'total_items':2},{'reports':[{'id':'b'}],'total_items':3}],
                [{'reports':[],'total_items':1}], [{'reports':[]}] ]
        for pages in cases:
            with self.subTest(pages=pages),self.assertRaises(ValueError):
                feedback.collect_reports(self.client(pages),'2026-01-01')

    def test_no_records_is_not_successful_refresh(self):
        self.assertEqual(feedback.feedback_status(NOW)['status'],'not_refreshed')

    def test_official_sdk_exposes_read_only_method(self):
        with patch.dict(os.environ,{'MAILCHIMP_API_KEY':'unit-test-us1'}):
            client=feedback.sdk_client()
        self.assertTrue(callable(client.reports.get_all_campaign_reports))

class CampaignFeedbackBoundaryTests(unittest.TestCase):
    def test_evidence_route_rejects_anonymous_and_cannot_write(self):
        from craft_unified import create_app, Database
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ,{'TESTING':'1','CRAFT_AUTO_SYNC':'0','DB_PATH':folder+'/db','COMMAND_API_KEY':'test-key','CRAFT_CAMPAIGN_EVIDENCE_ZLIB_B64':''}):
            client=create_app(Database(':memory:'),auto_sync=False).test_client()
            path='/api/intelligence/campaign-evidence'
            self.assertEqual(client.get(path).status_code,401)
            response=client.get(path,headers={'Authorization':'Bearer test-key'})
            self.assertEqual(response.status_code,503)
            self.assertIn('no-store',response.headers['Cache-Control'])
            self.assertEqual(client.post(path,headers={'Authorization':'Bearer test-key'}).status_code,405)
