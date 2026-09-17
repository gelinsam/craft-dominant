from datetime import datetime, timezone, timedelta
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from channel_health import refresh_meta_health, meta_health
from measurement_cycle import refresh_measurements, measurement_status

NOW=datetime(2026,9,17,1,tzinfo=timezone.utc)

class ScheduledMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        env=patch.dict(os.environ,{'DB_PATH':self.temp.name+'/db','META_ACCESS_TOKEN':'unit-token','META_AD_ACCOUNT_ID':'act_123,456'})
        env.start();self.addCleanup(env.stop)

    def test_meta_health_uses_all_accounts_and_expires(self):
        factory=Mock()
        factory.return_value.BASE_URL='https://graph.facebook.com/v24.0'
        factory.return_value._api_get.return_value={'account_status':1}
        result=refresh_meta_health(factory,NOW)
        self.assertEqual(result['status'],'read_access_verified')
        self.assertEqual(result['accounts_checked'],2)
        self.assertEqual(factory.return_value.session.close.call_count,2)
        self.assertEqual(meta_health(NOW+timedelta(hours=9))['status'],'stale')
        os.environ['META_ACCESS_TOKEN']='replacement'
        self.assertEqual(meta_health(NOW)['status'],'not_verified')

    def test_partial_meta_failure_does_not_claim_verified(self):
        factory=Mock();factory.return_value.BASE_URL='https://graph.facebook.com/v24.0'
        factory.return_value._api_get.side_effect=[{'account_status':1},RuntimeError('private-provider-error')]
        result=refresh_meta_health(factory,NOW)
        self.assertEqual(result['status'],'read_access_failed')
        self.assertEqual(result['accounts_checked'],1)
        self.assertNotIn('private-provider-error',str(result))

    def test_measurement_skips_drafts_and_requires_complete_recent_sales(self):
        db,repo,adapter=Mock(),Mock(),Mock()
        item=SimpleNamespace(id='a',status=SimpleNamespace(value='measuring'),measurement_ends_at=(NOW-timedelta(hours=2)).isoformat())
        draft=SimpleNamespace(id='b',status=SimpleNamespace(value='draft'))
        repo.list_interventions.return_value=[item,draft]
        for status,finished in [('failed',NOW),('completed',NOW-timedelta(days=2)),('completed_with_integrity_warnings',NOW),('completed',NOW-timedelta(hours=3))]:
            db.last_sync_run.return_value={'status':status,'finished_at':finished.isoformat()}
            result=refresh_measurements(db,repo,adapter,NOW)
            self.assertEqual(result['held'],1)
            adapter.measure.assert_not_called()
        db.last_sync_run.return_value={'status':'completed','finished_at':NOW.isoformat()}
        adapter.measure.return_value={'status':'learned'}
        repo.get_learning.return_value={'intervention_id':'a'}
        result=refresh_measurements(db,repo,adapter,NOW)
        adapter.measure.assert_called_once_with('a',actor='scheduled_measurement')
        self.assertEqual(result['completed'],1)
        self.assertEqual(measurement_status(NOW+timedelta(hours=9))['status'],'stale')

    def test_one_measurement_failure_does_not_skip_others_or_send(self):
        db,repo,adapter=Mock(),Mock(),Mock()
        db.last_sync_run.return_value={'status':'completed','finished_at':NOW.isoformat()}
        repo.list_interventions.return_value=[SimpleNamespace(id=x,status=SimpleNamespace(value='measuring'),measurement_ends_at=(NOW+timedelta(days=1)).isoformat()) for x in ['a','b']]
        adapter.measure.side_effect=[RuntimeError('failure'),{'status':'measuring'}]
        result=refresh_measurements(db,repo,adapter,NOW)
        self.assertEqual(result['failed'],1);self.assertEqual(result['measured'],1)
        adapter.execute.assert_not_called()

    def test_no_pending_is_distinct_from_zero_sales(self):
        repo=Mock();repo.list_interventions.return_value=[]
        result=refresh_measurements(Mock(),repo,Mock(),NOW)
        self.assertEqual(result['status'],'no_pending_measurements')
        self.assertNotIn('revenue',result)

    def test_failed_learning_write_raises_before_completion_can_be_saved(self):
        from execution_adapter import ExecutionAdapter
        adapter=ExecutionAdapter.__new__(ExecutionAdapter)
        adapter.db=Mock();adapter.db.get_event.return_value={'city':'Example','event_type':'wine'}
        adapter.v2_repo=Mock();adapter.v2_repo.save_learning.side_effect=RuntimeError('failed')
        item=SimpleNamespace(id='a',event_id='e',expected_revenue=10,attributed_revenue=0,evidence={},attributed_orders=0,attributed_tickets=0,sent_count=100,intervention_type='crm_campaign',confidence=.5,measurement_window=7,measurement_started_at=NOW.isoformat())
        with self.assertRaises(RuntimeError):
            adapter._persist_learning(item)
        adapter.v2_repo.save_intervention.assert_not_called()



import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from maintenance_tasks import run_maintenance, maintenance_status


class MaintenanceIsolationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, {'DB_PATH': self.temp.name+'/db'})
        env.start(); self.addCleanup(env.stop)
        self.now = datetime(2026, 9, 17, tzinfo=timezone.utc)

    def test_failure_does_not_skip_later_preparation(self):
        first = Mock(side_effect=RuntimeError('secret-provider-response'))
        second = Mock(return_value={'status': 'current'})
        result = run_maintenance([('Evidence', first), ('Drafts', second)], self.now)
        second.assert_called_once_with()
        self.assertEqual(result['status'], 'partial_failure')
        self.assertEqual(result['tasks'][1]['status'], 'completed')
        self.assertNotIn('secret-provider-response', str(maintenance_status(self.now)))

    def test_reported_failure_and_recovery_are_visible(self):
        run_maintenance([('Evidence', lambda: {'status': 'refresh_failed'})], self.now)
        self.assertEqual(maintenance_status(self.now)['status'], 'partial_failure')
        run_maintenance([('Evidence', lambda: {'status': 'current'})], self.now)
        self.assertEqual(maintenance_status(self.now)['status'], 'current')

    def test_old_or_future_outcomes_are_stale(self):
        run_maintenance([('Drafts', lambda: None)], self.now)
        self.assertEqual(maintenance_status(self.now+timedelta(hours=9))['status'], 'stale')
        self.assertEqual(maintenance_status(self.now-timedelta(hours=1))['status'], 'stale')

    def test_missing_status_is_not_success(self):
        self.assertEqual(maintenance_status(self.now)['status'], 'not_checked')




    def test_partial_failure_result_remains_visible(self):
        result = run_maintenance([('Tracking', lambda: {'status': 'partial_failure'})], self.now)
        self.assertEqual(result['status'], 'partial_failure')
        self.assertEqual(result['tasks'][0]['status'], 'failed')
