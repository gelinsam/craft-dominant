from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from campaign_feedback import atomic_json, read_json
from meta_purchase_outcomes import PURCHASE, normalize, path, purchase_report, refresh_purchase_outcomes


class PurchaseOutcomesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, {'DB_PATH':str(Path(self.temp.name)/'db'), 'META_ACCESS_TOKEN':'test', 'META_AD_ACCOUNT_ID':'123'})
        env.start(); self.addCleanup(env.stop)
        self.now = datetime(2026,9,18,tzinfo=timezone.utc)
        self.db = SimpleNamespace(get_events=lambda **kw:[])
        self.factory = Mock()
        self.client = self.factory.return_value
        self.client.BASE_URL = 'https://graph.facebook.com/v24.0'
        self.client._assign_campaign_to_edition.return_value = None
        self.raw = {'account_id':'123', 'campaign_id':'456', 'campaign_name':'Coffee', 'spend':'100', 'account_currency':'USD',
                    'date_start':'2026-08-21', 'date_stop':'2026-09-17',
                    'actions':[{'action_type':PURCHASE,'value':'5'}], 'action_values':[{'action_type':PURCHASE,'value':'250'}]}

    def test_purchase_aliases_are_not_added(self):
        self.raw['actions'] += [{'action_type':'purchase','value':'5'}, {'action_type':'omni_purchase','value':'5'}]
        row = normalize(self.raw, '123')
        self.assertEqual(row['website_purchases'],5)
        self.assertEqual(row['cost_per_reported_purchase'],20)
        self.assertEqual(row['reported_website_roas'],2.5)

    def test_omni_only_does_not_claim_website_purchase(self):
        self.raw['actions'] = [{'action_type':'omni_purchase','value':'5'}]
        self.assertIsNone(normalize(self.raw,'123')['website_purchases'])

    def test_missing_values_do_not_become_zero(self):
        self.raw.pop('actions'); self.raw.pop('action_values')
        row=normalize(self.raw,'123')
        self.assertEqual(row['purchase_status'],'not_reported')
        self.assertIsNone(row['website_purchases']); self.assertIsNone(row['reported_website_roas'])

    def test_explicit_zero_is_preserved(self):
        self.raw['actions'][0]['value']='0'
        row=normalize(self.raw,'123')
        self.assertEqual(row['website_purchases'],0)
        self.assertIsNone(row['cost_per_reported_purchase'])

    def test_nonfinite_and_duplicate_values_are_unknown(self):
        self.raw['actions'][0]['value']='NaN'
        self.assertIsNone(normalize(self.raw,'123')['website_purchases'])
        self.raw['actions']=[{'action_type':PURCHASE,'value':'5'}]*2
        self.assertIsNone(normalize(self.raw,'123')['website_purchases'])

    def test_wrong_account_is_rejected(self):
        with self.assertRaises(ValueError): normalize(self.raw,'999')

    def test_wrong_reporting_period_is_rejected(self):
        self.raw['date_start']='2025-01-01'
        self.client._api_get.return_value={'data':[self.raw]}
        self.assertEqual(refresh_purchase_outcomes(self.db,self.factory,self.now)['status'],'refresh_failed')
        self.assertFalse(path().exists())

    def test_missing_previously_observed_purchase_keeps_prior_snapshot(self):
        self.client._api_get.return_value={'data':[self.raw]}
        refresh_purchase_outcomes(self.db,self.factory,self.now)
        self.raw['actions']=[]
        refresh_purchase_outcomes(self.db,self.factory,self.now+timedelta(hours=1))
        report=purchase_report(self.now+timedelta(hours=1))
        self.assertEqual(report['status'],'refresh_failed')
        self.assertEqual(report['campaigns'][0]['website_purchases'],5)
        self.assertEqual(report['observed_at'],self.now.isoformat())

    def test_failed_page_preserves_last_success_without_leaking_error(self):
        self.client._api_get.return_value={'data':[self.raw]}
        self.assertEqual(refresh_purchase_outcomes(self.db,self.factory,self.now)['status'],'current')
        self.client._api_get.side_effect=[{'data':[], 'paging':{'next':'https://graph.facebook.com/next'}}, RuntimeError('secret')]
        self.assertEqual(refresh_purchase_outcomes(self.db,self.factory,self.now+timedelta(hours=1))['status'],'refresh_failed')
        report=purchase_report(self.now+timedelta(hours=1))
        self.assertEqual(len(report['campaigns']),1)
        self.assertEqual(report['observed_at'],self.now.isoformat())
        self.assertEqual(report['status'],'refresh_failed')
        self.assertNotIn('secret',str(report))

    def test_pagination_loop_rejected(self):
        self.client._api_get.return_value={'data':[], 'paging':{'next':'https://graph.facebook.com/next'}}
        self.assertEqual(refresh_purchase_outcomes(self.db,self.factory,self.now)['status'],'refresh_failed')
        self.assertFalse(path().exists())

    def test_ambiguous_festival_does_not_get_credit(self):
        self.client._api_get.return_value={'data':[self.raw]}
        self.client._assign_campaign_to_edition.return_value={'ambiguous':True}
        refresh_purchase_outcomes(self.db,self.factory,self.now)
        row=purchase_report(self.now)['campaigns'][0]
        self.assertEqual(row['mapping_status'],'ambiguous'); self.assertNotIn('event_id',row)

    def test_stale_and_credentials_failure_are_visible(self):
        self.client._api_get.return_value={'data':[self.raw]}
        refresh_purchase_outcomes(self.db,self.factory,self.now)
        self.assertEqual(purchase_report(self.now+timedelta(hours=9))['status'],'stale')
        with patch.dict(os.environ,{'META_ACCESS_TOKEN':''}): refresh_purchase_outcomes(self.db,self.factory,self.now)
        self.assertEqual(purchase_report(self.now)['status'],'credentials_unavailable')
        self.assertEqual(len(purchase_report(self.now)['campaigns']),1)

    def test_explicit_window_and_conversion_date_requested(self):
        self.client._api_get.return_value={'data':[]}
        refresh_purchase_outcomes(self.db,self.factory,self.now)
        args=self.client._api_get.call_args.args[1]
        self.assertEqual(args['action_report_time'],'conversion')
        self.assertEqual(args['action_attribution_windows'],'["7d_click", "1d_view"]')
        self.assertEqual(self.client.session.close.call_count,1)


if __name__=='__main__': unittest.main()
