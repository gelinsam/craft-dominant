from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from sales_evidence import event_receipt, save_evidence, verified_edition_at, evidence_path
from campaign_feedback import atomic_json


class SalesEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        env = patch.dict(os.environ, {'DB_PATH': self.temp.name+'/db'})
        env.start(); self.addCleanup(env.stop)
        self.now = datetime(2026, 9, 17, 12, tzinfo=timezone.utc)
        self.run = {'id': 9, 'status': 'completed_with_integrity_warnings', 'finished_at': self.now.isoformat()}
        self.db = SimpleNamespace(get_event=lambda eid: {'event_id': eid} if eid else None,
                                 edition_sibling_ids=lambda eid: ['a', 'b'])

    def save(self, **override):
        rows = [{'event_id': x, 'status': 'complete', 'read_started_at': (self.now-timedelta(hours=1)).isoformat()} for x in ['a','b']]
        rows[1].update(override)
        save_evidence({'event_evidence': rows+[{'event_id': 'unrelated', 'status': 'incomplete'}]}, self.run)

    def test_unrelated_failure_does_not_block_verified_edition(self):
        self.save()
        self.assertEqual(verified_edition_at(self.db, 'a', self.run, self.now), self.now-timedelta(hours=1))

    def test_incomplete_sibling_and_unknown_scope_stay_held(self):
        self.save(status='incomplete')
        self.assertIsNone(verified_edition_at(self.db, 'a', self.run, self.now))
        self.save(); self.db.edition_sibling_ids=lambda eid:['a','missing']
        self.assertIsNone(verified_edition_at(self.db, 'a', self.run, self.now))

    def test_stale_future_and_missing_read_times_stay_held(self):
        for value in [None, (self.now+timedelta(hours=1)).isoformat(), (self.now-timedelta(days=2)).isoformat()]:
            self.save(read_started_at=value)
            self.assertIsNone(verified_edition_at(self.db, 'a', self.run, self.now))

    def test_previous_receipt_cannot_clear_new_or_interrupted_sync(self):
        self.save()
        for change in [{'id': 10}, {'status':'running'}, {'status':'interrupted'}, {'status':'failed'}]:
            self.assertIsNone(verified_edition_at(self.db,'a',dict(self.run,**change),self.now))

    def test_receipt_checks_skips_duplicates_missing_stored_unknown_and_drop(self):
        conn=sqlite3.connect(':memory:'); conn.row_factory=sqlite3.Row; self.addCleanup(conn.close)
        conn.execute('CREATE TABLE orders(event_id TEXT, ticket_count INTEGER, gross_amount REAL)')
        conn.execute("INSERT INTO orders VALUES ('a',2,50)")
        db=SimpleNamespace(conn=conn)
        def receipt(n,ids,before=2): return event_receipt(db,'a',n,ids,before,self.now.isoformat())
        self.assertEqual(receipt(1,['o'])['status'],'complete')
        self.assertEqual(event_receipt(db,'a',1,['o'],2,self.now.isoformat(),False)['status'],'incomplete')
        for n,ids,before in [(2,['o'],2),(2,['o','o'],2),(0,[],2),(1,['o'],3)]:
            self.assertEqual(receipt(n,ids,before)['status'],'incomplete')
        conn.execute('UPDATE orders SET gross_amount=NULL')
        self.assertEqual(receipt(1,['o'])['status'],'incomplete')

    def test_measurement_uses_scoped_evidence_and_waits_for_window(self):
        from measurement_cycle import refresh_measurements
        self.save(); self.db.last_sync_run=lambda:self.run
        repo,adapter=Mock(),Mock()
        item=SimpleNamespace(id='i',event_id='a',status=SimpleNamespace(value='measuring'),measurement_ends_at=(self.now-timedelta(hours=2)).isoformat())
        repo.list_interventions.return_value=[item];adapter.measure.return_value={'status':'learned'}
        result=refresh_measurements(self.db,repo,adapter,self.now)
        self.assertEqual(result['completed'],1)
        adapter.measure.reset_mock()
        item.measurement_ends_at=(self.now-timedelta(minutes=30)).isoformat()
        self.assertEqual(refresh_measurements(self.db,repo,adapter,self.now)['held'],1)
        adapter.measure.assert_not_called();adapter.execute.assert_not_called()

    def test_failed_sync_cannot_publish_receipt(self):
        save_evidence({'event_evidence':[]},dict(self.run,status='failed'))
        self.assertFalse(evidence_path().exists())


class SalesSyncReceiptIntegrationTests(unittest.TestCase):
    def test_existing_sync_records_each_event_and_does_not_invent_completeness(self):
        from craft_unified import Database, EventbriteSync
        db=Database(':memory:');self.addCleanup(db.conn.close)
        sync=EventbriteSync('test',db)
        sync.get_org_id=Mock(return_value='org')
        when=(datetime.now()+timedelta(days=30)).isoformat()
        events=[{'id':x,'name':{'text':'Example Coffee Festival'},'start':{'local':when}} for x in ['a','b']]
        sync._paginate=Mock(side_effect=[events,[],RuntimeError('provider read failed')])
        sync._build_all_customers=Mock(return_value=0);sync._build_curves=Mock(return_value=0)
        result=sync.sync_all()
        self.assertEqual([(r['event_id'],r['status']) for r in result['event_evidence']],[('a','complete'),('b','incomplete')])
        self.assertEqual(len(result['errors']),1)

    def test_order_pagination_missing_metadata_is_not_complete(self):
        from craft_unified import EventbriteSync
        sync=EventbriteSync('test',Mock())
        for response in [{}, {'orders':[]}, {'orders':[], 'pagination':{}}, {'orders':None,'pagination':{'has_more_items':False}}]:
            sync._get=Mock(return_value=response)
            with self.assertRaises(RuntimeError):
                sync._paginate('/events/a/orders/',require_complete=True)
        sync._get=Mock(return_value={'orders':[],'pagination':{'has_more_items':False}})
        self.assertEqual(sync._paginate('/events/a/orders/',require_complete=True),[])
