import unittest
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


if __name__ == '__main__':
    unittest.main()
