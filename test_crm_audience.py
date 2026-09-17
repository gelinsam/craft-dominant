import unittest
import sqlite3
from datetime import datetime, timezone
from unittest.mock import Mock
from crm_audience import build_crm_audience, eventbrite_eligible_emails, recipient_digest, _years_before


class CRMAudienceTests(unittest.TestCase):
    def setUp(self):
        self.db = Mock()
        self.db.get_event.return_value = {'event_id': 'sat', 'city': 'Washington', 'event_type': 'coffee'}
        self.db.edition_sibling_ids.return_value = ['sat', 'sun']
        self.db.get_event_buyers.side_effect = lambda eid: {'BUYER@example.com'} if eid == 'sun' else set()
        self.db.get_event_profiles.return_value = [
            {'email': 'buyer@example.com'}, {'email': ' Fan@example.com '}, {'email': 'fan@example.com'}]

    def test_rebuild_excludes_other_day_buyer_and_deduplicates(self):
        result = build_crm_audience(self.db, 'sat', 'dormant')
        self.assertEqual([r['email'] for r in result['records']], ['fan@example.com'])
        self.assertEqual(result['excluded_current_buyers'], 1)
        self.db.get_event_profiles.assert_called_once_with('coffee', 'Washington', limit=50001, momentum='dormant')
        self.db.get_event_buyers.side_effect = lambda eid: {'fan@example.com', 'buyer@example.com'}
        self.assertEqual(build_crm_audience(self.db, 'sat', 'dormant')['records'], [])

    def test_invalid_candidates_are_quarantined_without_mutating_source(self):
        rows = [{'email': 'valid@example.com'}, {'email': None},
                {'email': 'not-an-address'}, {'email': 'a@example.com\nb@example.com'},
                {'email': 'buyer@example.com'}]
        self.db.get_event_profiles.return_value = rows
        result = build_crm_audience(self.db, 'sat', 'dormant')
        self.assertEqual([r['email'] for r in result['records']], ['valid@example.com'])
        self.assertEqual(result['quarantined_invalid_email_records'], 3)
        self.assertEqual(result['candidate_source_records'], 5)
        self.assertEqual(result['excluded_current_buyers'], 1)
        self.assertIsNone(rows[1]['email'])
        self.assertEqual(rows[2]['email'], 'not-an-address')

    def test_all_invalid_is_explicitly_empty_not_a_source_failure(self):
        self.db.get_event_profiles.return_value = [{'email': None}]
        result = build_crm_audience(self.db, 'sat', 'dormant')
        self.assertEqual(result['records'], [])
        self.assertEqual(result['quarantined_invalid_email_records'], 1)

    def test_referrals_explicitly_allow_current_buyers(self):
        result = build_crm_audience(self.db, 'sat', 'super_spreaders', 'referral')
        self.assertEqual(len(result['records']), 2)
        with self.assertRaises(ValueError):
            build_crm_audience(self.db, 'sat', 'dormant', 'referral')

    def test_unknown_criteria_and_incomplete_scope_fail_closed(self):
        with self.assertRaises(ValueError):
            build_crm_audience(self.db, 'sat', 'all')
        self.db.get_event.return_value = {'event_type': 'coffee'}
        with self.assertRaises(ValueError):
            build_crm_audience(self.db, 'sat', 'dormant')

    def test_lookup_failure_is_not_partial_success(self):
        self.db.get_event_buyers.side_effect = RuntimeError('database unavailable')
        with self.assertRaises(RuntimeError):
            build_crm_audience(self.db, 'sat', 'dormant')

    def test_oversize_query_is_not_silently_truncated(self):
        self.db.get_event_profiles.return_value = [{'email': 'a@example.com'}] * 50001
        with self.assertRaises(ValueError):
            build_crm_audience(self.db, 'sat', 'dormant')

    def test_eventbrite_denies_unsubscribed_bounced_and_unknown(self):
        text = ('Email Address,Subscribed? Yes/No,Unsubscribed Date,Bounced\n'
                'a@example.com,Yes,,No\nb@example.com,Yes,,Yes\n'
                'c@example.com,No,2026-09-01,No\nd@example.com,Yes,,\n'
                'A@example.com,No,,No\ne@example.com,Yes,,No\n')
        self.assertEqual(eventbrite_eligible_emails(text), {'e@example.com'})
        with self.assertRaises(ValueError):
            eventbrite_eligible_emails('Email Address\na@example.com\n')

    def test_membership_hash_changes_with_recipients(self):
        self.assertEqual(recipient_digest(['A@example.com', 'a@example.com']), recipient_digest(['a@example.com']))
        self.assertNotEqual(recipient_digest(['a@example.com']), recipient_digest(['b@example.com']))

    def test_past_buyers_use_exact_scope_completed_dates_and_all_current_days(self):
        conn = sqlite3.connect(':memory:')
        self.addCleanup(conn.close)
        conn.row_factory = sqlite3.Row
        conn.executescript('''
          CREATE TABLE events(event_id TEXT, name TEXT, city TEXT, event_type TEXT, event_date TEXT);
          CREATE TABLE orders(event_id TEXT, email TEXT, ticket_count INTEGER);
        ''')
        events = [
            ('sat','Coffee','DC','coffee','2026-10-03'),
            ('sun','Coffee','DC','coffee','2026-10-04'),
            ('past','Unrelated display name','DC','coffee','2025-10-01'),
            ('wine','Coffee-ish name','DC','wine','2025-10-01'),
            ('philly','Coffee','Philly','coffee','2025-10-01'),
            ('future','Coffee','DC','coffee','2026-09-25'),
            ('unknown','Coffee','DC','coffee','not-a-date'),
            ('exhibitor','Coffee - Exhibitor Payment','DC','coffee','2025-10-01'),
        ]
        conn.executemany('INSERT INTO events VALUES (?,?,?,?,?)', events)
        conn.executemany('INSERT INTO orders VALUES (?,?,?)', [
            ('past',' Fan@example.com ',2), ('past','fan@example.com',1),
            ('past','buyer@example.com',1), ('sun','BUYER@example.com',1),
            ('wine','wine@example.com',2), ('philly','philly@example.com',2),
            ('future','future@example.com',2), ('unknown','unknown@example.com',2),
            ('past','refunded@example.com',0), ('past','unobserved@example.com',None),
            ('exhibitor','vendor@example.com',3),
        ])
        self.db.conn = conn
        self.db.get_event.side_effect = lambda eid: dict(conn.execute('SELECT * FROM events WHERE event_id=?',(eid,)).fetchone())
        result = build_crm_audience(self.db,'sat','past_attendees',now=datetime(2026,9,16,tzinfo=timezone.utc))
        self.assertEqual([r['email'] for r in result['records']], ['fan@example.com'])
        self.assertEqual(result['records'][0]['past_ticket_count'],3)
        self.assertEqual(result['excluded_current_buyers'],1)
        self.db.get_event_profiles.assert_not_called()

    def test_unresolved_edition_cannot_silently_skip_buyer_exclusion(self):
        self.db.edition_sibling_ids.return_value = []
        with self.assertRaises(ValueError):
            build_crm_audience(self.db,'sat','super_spreaders')

    def winback_db(self):
        conn=sqlite3.connect(':memory:'); conn.row_factory=sqlite3.Row
        self.addCleanup(conn.close)
        conn.executescript('CREATE TABLE events(event_id TEXT,name TEXT,city TEXT,event_type TEXT,event_date TEXT,status TEXT); CREATE TABLE orders(event_id TEXT,email TEXT,ticket_count INTEGER);')
        conn.executemany('INSERT INTO events(event_id,name,city,event_type,event_date) VALUES (?,?,?,?,?)',[
            ('old-sat','Wine 2024','DC','wine','2024-05-01'),
            ('old-sun','Wine 2024','DC','wine','2024-05-02'),
            ('later','Wine 2025','DC','wine','2025-05-01'),
            ('current','Wine 2026','DC','wine','2026-10-01'),
            ('future','Wine 2027','DC','wine','2027-05-01'),
            ('ancient','Wine 2022','DC','wine','2022-05-01'),
            ('coffee','Coffee','DC','coffee','2024-05-01'),
            ('philly','Wine','Philly','wine','2024-05-01'),
            ('vendor','Wine Vendor Payment','DC','wine','2024-05-01'),
        ])
        self.db.conn=conn
        self.db.get_event.side_effect=lambda eid: dict(conn.execute('SELECT * FROM events WHERE event_id=?',(eid,)).fetchone())
        self.db.edition_sibling_ids.side_effect=lambda eid: ['old-sat','old-sun'] if eid in ('old-sat','old-sun') else [eid]
        self.db.get_event_buyers.side_effect=lambda eid: {r[0] for r in conn.execute('SELECT email FROM orders WHERE event_id=?',(eid,))}
        return conn

    def test_one_and_done_counts_editions_not_orders_days_or_tickets(self):
        conn=self.winback_db()
        conn.executemany('INSERT INTO orders VALUES (?,?,?)',[
            ('old-sat',' Group@example.com ',4),('old-sun','group@example.com',2),
            ('old-sat','repeat@example.com',1),('later','repeat@example.com',1),
            ('old-sat','current@example.com',1),('current','current@example.com',1),
            ('old-sat','future@example.com',1),('future','future@example.com',1),
            ('later','notyet@example.com',1),('ancient','ancient@example.com',1),
            ('vendor','vendor@example.com',1),('old-sat','unknown@example.com',None),
            ('old-sat','refunded@example.com',0),
            ('coffee','group@example.com',2),('philly','group@example.com',2),
        ])
        r=build_crm_audience(self.db,'current','one_and_done',now=datetime(2026,9,16,tzinfo=timezone.utc))
        self.assertEqual([x['email'] for x in r['records']],['group@example.com'])
        self.assertEqual(r['records'][0]['past_ticket_count'],6)
        self.assertEqual(r['records'][0]['purchased_edition_count'],1)
        self.assertEqual(r['records'][0]['missed_completed_editions'],1)
        self.assertEqual(r['history_coverage'],'stored_records_only')

    def test_one_and_done_requires_a_later_completed_opportunity(self):
        conn=self.winback_db()
        conn.execute("DELETE FROM events WHERE event_id='later'")
        conn.execute("INSERT INTO orders VALUES ('old-sat','a@example.com',1)")
        self.assertEqual(build_crm_audience(self.db,'current','one_and_done',now=datetime(2026,9,16,tzinfo=timezone.utc))['records'],[])

    def test_uncertain_quantity_in_another_edition_prevents_one_and_done_claim(self):
        conn=self.winback_db()
        conn.executemany('INSERT INTO orders VALUES (?,?,?)',[('old-sat','a@example.com',1),('later','a@example.com',None)])
        self.assertEqual(build_crm_audience(self.db,'current','one_and_done',now=datetime(2026,9,16,tzinfo=timezone.utc))['records'],[])

    def test_cancelled_later_edition_is_not_a_missed_opportunity(self):
        conn=self.winback_db()
        conn.execute("UPDATE events SET status='cancelled' WHERE event_id='later'")
        conn.execute("INSERT INTO orders VALUES ('old-sat','a@example.com',1)")
        self.assertEqual(build_crm_audience(self.db,'current','one_and_done',now=datetime(2026,9,16,tzinfo=timezone.utc))['records'],[])

    def test_calendar_anniversary_handles_leap_day(self):
        self.assertEqual(_years_before(datetime(2024,2,29).date(),1).isoformat(),'2023-02-28')


if __name__ == '__main__':
    unittest.main()
