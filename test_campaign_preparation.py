import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock
from campaign_preparation import prepare_draft, verify_import

HEADER = 'Email Address,Subscribed? Yes/No,Unsubscribed Date,Bounced\n'


class CampaignPreparationTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 16, 16, 0, tzinfo=timezone.utc)
        self.stamp = self.now.isoformat()
        self.db = Mock()
        self.db.get_event.return_value = {'event_type': 'coffee', 'city': 'DC'}
        self.db.edition_sibling_ids.return_value = ['sat', 'sun']
        self.db.get_event_buyers.return_value = set()
        self.db.get_event_profiles.return_value = [{'email':'a@example.com'}, {'email':'b@example.com'}]
        self.request = dict(run_id='test-1',event_id='sat',segment='super_spreaders',purpose='ticket_sales')
        self.sources = [dict(list_id='source',event_type='coffee',city='DC',observed_at=self.stamp,
                             csv=HEADER+'a@example.com,Yes,,No\nb@example.com,Yes,,No\n')]
        self.history = dict(complete=True,observed_at=self.stamp,scope='all_festivals_all_providers',
                            active_campaigns=[],contacts=[])

    def prepare(self):
        return prepare_draft(self.db,self.request,self.sources,self.history,self.now)

    def test_new_purchase_and_recent_contact_excluded(self):
        self.db.get_event_buyers.side_effect = lambda eid: {'b@example.com'} if eid=='sun' else set()
        package=self.prepare()
        self.assertEqual(package['upload_csv'],'a@example.com\n')
        self.history['contacts']=[dict(email='a@example.com',contacted_at=self.stamp)]
        with self.assertRaises(ValueError): self.prepare()

    def test_denial_across_lists_wins(self):
        self.sources.append(dict(self.sources[0],list_id='second',csv=HEADER+'a@example.com,No,,No\n'))
        self.assertEqual(self.prepare()['upload_csv'],'b@example.com\n')

    def test_unknown_history_and_active_campaign_stay_on_draft(self):
        self.history['complete']=False
        self.assertIn('contact_history_incomplete',self.prepare()['sending_blockers'])
        self.history['complete']=True
        self.history['active_campaigns']=['currently-sending']
        self.assertIn('active_campaigns_unresolved',self.prepare()['sending_blockers'])

    def test_wrong_city_and_stale_source_block(self):
        self.sources[0]['city']='NYC'
        with self.assertRaises(ValueError): self.prepare()
        self.sources[0]['city']='DC'
        self.sources[0]['observed_at']=(self.now-timedelta(hours=2)).isoformat()
        with self.assertRaises(ValueError): self.prepare()

    def test_export_exact_match_and_no_send_capability(self):
        p=self.prepare()
        result=verify_import(p,'destination',self.sources[0]['csv'],self.stamp,self.now)
        self.assertEqual(result['state'],'DRAFT_AUDIENCE_VERIFIED')
        self.assertFalse(result['external_send_enabled'])
        for invalid in (HEADER+'a@example.com,Yes,,No\nx@example.com,Yes,,No\n',
                        self.sources[0]['csv']+'a@example.com,Yes,,No\n',
                        HEADER+'a@example.com,Yes,,No\nb@example.com,No,,No\n'):
            with self.assertRaises(ValueError): verify_import(p,'destination',invalid,self.stamp,self.now)

    def test_stale_package_needs_new_preparation(self):
        p=self.prepare()
        with self.assertRaises(ValueError):
            verify_import(p,'destination',self.sources[0]['csv'],self.stamp,self.now+timedelta(hours=2))

    def test_source_list_cannot_be_destination(self):
        with self.assertRaises(ValueError):
            verify_import(self.prepare(),'source',self.sources[0]['csv'],self.stamp,self.now)

    def test_scheduled_membership_excluded_across_provider_and_city(self):
        self.history['active_campaigns'] = [dict(provider='mailchimp',campaign_id='other-city',
            observed_at=self.stamp,recipient_emails=[' A@example.com '],membership_complete=True)]
        result = self.prepare()
        self.assertEqual(result['upload_csv'],'b@example.com\n')
        self.assertEqual(result['excluded_active_campaign_recipients'],1)
        self.assertNotIn('active_campaigns_unresolved',result['sending_blockers'])
        self.assertIn('sending_disabled',result['sending_blockers'])

    def test_partial_active_membership_excludes_known_people_but_retains_hold(self):
        self.history['active_campaigns'] = [dict(provider='eventbrite',campaign_id='sending',
            observed_at=self.stamp,recipient_emails=['a@example.com'],membership_complete=False)]
        result = self.prepare()
        self.assertEqual(result['upload_csv'],'b@example.com\n')
        self.assertIn('active_campaigns_unresolved',result['sending_blockers'])

    def test_stale_or_malformed_membership_cannot_clear_hold(self):
        campaign = dict(provider='eventbrite',campaign_id='sending',observed_at=self.stamp,
                        recipient_emails=['a@example.com'],membership_complete=True)
        self.history['active_campaigns'] = [dict(campaign,observed_at=(self.now-timedelta(hours=2)).isoformat())]
        with self.assertRaises(ValueError): self.prepare()
        self.history['active_campaigns'] = [dict(campaign,recipient_emails='a@example.com')]
        self.assertIn('active_campaigns_unresolved',self.prepare()['sending_blockers'])
        self.history['active_campaigns'] = [dict(campaign,recipient_emails=['a@example.com\nBcc:x@example.com'])]
        with self.assertRaises(ValueError): self.prepare()

    def test_recent_and_scheduled_overlap_is_counted_once(self):
        self.history['contacts'] = [dict(email='a@example.com',contacted_at=self.stamp)]
        self.history['active_campaigns'] = [dict(provider='eventbrite',campaign_id='sending',
            observed_at=self.stamp,recipient_emails=['a@example.com'],membership_complete=True)]
        result = self.prepare()
        self.assertEqual(result['excluded_recent_contacts'],1)
        self.assertEqual(result['excluded_active_campaign_recipients'],0)

    def test_stored_purchase_history_never_becomes_verified_by_preparation(self):
        from unittest.mock import patch
        self.request['segment']='one_and_done'
        with patch('campaign_preparation.build_crm_audience',return_value={
            'event_type':'coffee','city':'DC','records':[{'email':'a@example.com'}],
            'history_coverage':'stored_records_only','purchase_window_start':'2023-09-16',
            'purchase_window_end':'2025-09-16','edition_count':4}):
            result=self.prepare()
        self.assertIn('purchase_history_coverage_unverified',result['sending_blockers'])
        self.assertEqual(result['audience_evidence']['edition_count'],4)

if __name__=='__main__': unittest.main()
