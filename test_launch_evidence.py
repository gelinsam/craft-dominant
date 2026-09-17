import gzip
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
import launch_evidence as evidence

class TestLaunchEvidence(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'history.db'
        self.report={'editions':[{'city':'Test','date':'2026-10-03','tickets':10,'spend':None}], 'meta_collected_at':'2026-09-17T00:00:00+00:00'}
        self.campaigns=[{'account_id':'acct','campaign':{'id':'1','name':'Coffee','access_token':'SECRET'},'ads':[{'id':'a','creative':{'id':'c','body':'First','access_token':'SECRET'}}]}]
    def rows(self):
        with sqlite3.connect(self.path) as c:
            return [json.loads(gzip.decompress(r[0])) for r in c.execute('SELECT payload FROM observations ORDER BY id')]
    def test_versions_preserved_and_identical_retry_deduplicated(self):
        evidence.capture(self.report,self.campaigns,self.path)
        evidence.capture(self.report,self.campaigns,self.path)
        self.campaigns[0]['ads'][0]['creative']['body']='Second'
        evidence.capture(self.report,self.campaigns,self.path)
        rows=self.rows();self.assertEqual(len(rows),2)
        self.assertEqual(rows[0]['campaigns'][0]['ads'][0]['creative']['body'],'First')
        self.assertEqual(rows[1]['campaigns'][0]['ads'][0]['creative']['body'],'Second')
    def test_unknown_preserved_and_sensitive_fields_excluded(self):
        self.report['editions'][0]['email']='private@example.test'
        evidence.capture(self.report,self.campaigns,self.path)
        rows=self.rows();self.assertIsNone(rows[0]['editions'][0]['spend'])
        self.assertNotIn('SECRET',json.dumps(rows));self.assertNotIn('private@example',json.dumps(rows))
        self.assertEqual(self.path.stat().st_mode & 0o777,0o600)
    def test_oversize_rejected_without_altering_history(self):
        evidence.capture(self.report,self.campaigns,self.path)
        with patch.object(evidence,'MAX_BYTES',10),self.assertRaises(ValueError):
            evidence.capture(self.report,self.campaigns,self.path)
        self.assertEqual(len(self.rows()),1)
    def test_status_is_read_only_and_missing_is_explicit(self):
        self.assertEqual(evidence.status(self.path)['status'],'not_started')
        self.assertFalse(self.path.exists())
        evidence.capture(self.report,self.campaigns,self.path)
        self.assertEqual(evidence.status(self.path)['snapshots'],1)
        with sqlite3.connect(self.path) as c:c.execute("UPDATE observations SET observed_at='2020-01-01T00:00:00+00:00'")
        self.assertEqual(evidence.status(self.path)['status'],'stale')
    def test_corrupt_archive_not_reported_as_empty(self):
        self.path.write_text('broken')
        self.assertEqual(evidence.status(self.path)['status'],'unavailable')
        self.assertIsNone(evidence.status(self.path)['snapshots'])
    def test_refresh_preserves_snapshot_when_creative_read_fails(self):
        import launch_intelligence as launch
        import types
        import sys
        from unittest.mock import Mock
        old={'collected_at':'2020-01-01T00:00:00+00:00','campaigns':[{'account_id':'acct','campaign':{'id':'1','name':'Seattle coffee'},'days':[]}]}
        client=Mock();client._fetch_all_campaigns.return_value=[{'id':'1','name':'Seattle coffee'}]
        client.BASE_URL='https://example.test'
        client._api_get.side_effect=[{'data':[]},RuntimeError('failed read')]
        atomic=Mock()
        with patch.dict(sys.modules,{'craft_unified':types.SimpleNamespace(MetaAdsSync=Mock(return_value=client)), 'campaign_feedback':types.SimpleNamespace(atomic_json=atomic)}), patch.dict(os.environ,{'META_AD_ACCOUNT_ID':'acct','META_ACCESS_TOKEN':'test'}), patch.object(launch,'read_snapshot',side_effect=lambda name: old if name=='coffee-launch-history.json' else {}), patch.object(launch,'capture_evidence',return_value={'status':'recorded'}):
            with self.assertRaises(RuntimeError): launch.refresh_history()
        atomic.assert_not_called()
    def test_refresh_collects_all_creative_pages_and_keeps_observed_fields(self):
        import launch_intelligence as launch
        import types
        import sys
        from unittest.mock import Mock
        old={'collected_at':'2020-01-01T00:00:00+00:00','campaigns':[{'account_id':'acct','campaign':{'id':'1','name':'Seattle coffee'},'days':[], 'ads':[{'id':'a','creative':{'id':'c','body':'Known'}}]}]}
        client=Mock();client._fetch_all_campaigns.return_value=[{'id':'1','name':'Seattle coffee'}];client.BASE_URL='https://example.test'
        client._api_get.side_effect=[{'data':[]},{'data':[{'id':'a','creative':{'id':'c','body':None}}],'paging':{'next':'next'}},{'data':[{'id':'b','creative':{'id':'d','body':'New'}}]}]
        atomic=Mock()
        with patch.dict(sys.modules,{'craft_unified':types.SimpleNamespace(MetaAdsSync=Mock(return_value=client)), 'campaign_feedback':types.SimpleNamespace(atomic_json=atomic)}), patch.dict(os.environ,{'META_AD_ACCOUNT_ID':'acct','META_ACCESS_TOKEN':'test'}), patch.object(launch,'read_snapshot',side_effect=lambda name: old if name=='coffee-launch-history.json' else {}), patch.object(launch,'capture_evidence',return_value={'status':'recorded'}):
            launch.refresh_history()
        c=atomic.call_args.args[1]['campaigns'][0]
        self.assertEqual(len(c['ads']),2);self.assertEqual(c['ads'][0]['creative']['body'],'Known')
        self.assertTrue(c['creative_observed_at'])
