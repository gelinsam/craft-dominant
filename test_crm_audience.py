import unittest
import sqlite3
from datetime import datetime, timezone
from unittest.mock import Mock
from crm_audience import build_crm_audience, eventbrite_eligible_emails, recipient_digest


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
        ]
        conn.executemany('INSERT INTO events VALUES (?,?,?,?,?)', events)
        conn.executemany('INSERT INTO orders VALUES (?,?,?)', [
            ('past',' Fan@example.com ',2), ('past','fan@example.com',1),
            ('past','buyer@example.com',1), ('sun','BUYER@example.com',1),
            ('wine','wine@example.com',2), ('philly','philly@example.com',2),
            ('future','future@example.com',2), ('unknown','unknown@example.com',2),
            ('past','refunded@example.com',0), ('past','unobserved@example.com',None),
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


if __name__ == '__main__':
    unittest.main()
