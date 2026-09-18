"""Complete event order reads commit atomically, using the existing DB guard."""
from datetime import date, timedelta
import unittest
from unittest.mock import Mock

from craft_unified import Database, EventbriteSync


class EventbriteOrderBatchTests(unittest.TestCase):
    def setUp(self):
        self.db = Database(':memory:')
        self.addCleanup(self.db.conn.close)
        self.sync = EventbriteSync('unused', self.db)
        self.event = {'event_id':'123', 'name':'Austin Coffee Festival',
                      'event_type':'coffee', 'city':'Austin', 'capacity':500,
                      'event_date':(date.today()+timedelta(days=30)).isoformat()}
        self.sync.get_org_id = Mock(return_value='org')
        self.sync._parse_event = Mock(side_effect=lambda raw:dict(self.event))
        self.sync._parse_order = Mock(side_effect=lambda raw, *args:raw)
        self.sync._build_all_customers = Mock(return_value=0)
        self.sync._build_curves = Mock(return_value=0)
        self.orders = []
        self.sync._paginate = Mock(side_effect=lambda path, *args, **kw:
                                   [{}] if path.endswith('/events/') else self.orders)

    def order(self, number, tickets=2, amount=50):
        return {'order_id':str(number), 'event_id':'123', 'email':'test@example.com',
                'order_timestamp':date.today().isoformat(), 'ticket_count':tickets,
                'gross_amount':amount, 'net_amount':amount}

    def test_complete_event_uses_one_order_commit(self):
        self.orders = [self.order(i) for i in range(100)]
        statements = []
        self.db.conn.set_trace_callback(statements.append)
        result = self.sync.sync_all()
        self.assertEqual(result['orders'], 100)
        self.assertEqual(self.db.conn.execute('SELECT COUNT(*) FROM orders').fetchone()[0], 100)
        # One event metadata commit and one order batch, not 101 commits.
        self.assertEqual(sum(s.strip().upper() == 'COMMIT' for s in statements), 2)
        self.assertEqual(result['event_evidence'][0]['status'], 'complete')

    def test_failed_order_batch_restores_prior_values_and_count(self):
        self.db.upsert_event(dict(self.event, status='upcoming'))
        self.db.insert_order(self.order(1, 4, 100))
        self.orders = [self.order(1, 2, 50), self.order(2), self.order(3)]
        def parse(raw, *args):
            if raw['order_id'] == '3':
                raise ValueError('failed parse fixture')
            return raw
        self.sync._parse_order.side_effect = parse
        result = self.sync.sync_all()
        stored = list(self.db.conn.execute('SELECT order_id,ticket_count,gross_amount FROM orders'))
        self.assertEqual([tuple(r) for r in stored], [('1', 4, 100)])
        self.assertEqual(result['orders'], 0)
        self.assertEqual(result['event_evidence'][0]['status'], 'incomplete')
        self.assertEqual(len(result['errors']), 1)
        self.assertEqual(result['integrity']['tickets_lost'], 0)

    def test_unknown_fields_still_preserve_existing_order(self):
        self.db.upsert_event(dict(self.event, status='upcoming'))
        self.db.insert_order(self.order(1, 4, 100))
        self.orders = [self.order(1, None, None)]
        result = self.sync.sync_all()
        stored = self.db.conn.execute('SELECT ticket_count,gross_amount FROM orders').fetchone()
        self.assertEqual(tuple(stored), (4, 100))
        self.assertEqual(result['orders'], 1)
        self.assertNotEqual(result['event_evidence'][0]['status'], 'complete')

    def test_incomplete_provider_page_never_enters_order_transaction(self):
        def pages(path, *args, **kwargs):
            if path.endswith('/events/'):
                return [{}]
            raise RuntimeError('incomplete page fixture')
        self.sync._paginate.side_effect = pages
        self.db.deferred_commit = Mock(wraps=self.db.deferred_commit)
        result = self.sync.sync_all()
        self.db.deferred_commit.assert_not_called()
        self.assertEqual(result['orders'], 0)
        self.assertEqual(len(result['errors']), 1)
        self.assertEqual(result['event_evidence'][0]['status'], 'incomplete')


if __name__ == '__main__':
    unittest.main()
