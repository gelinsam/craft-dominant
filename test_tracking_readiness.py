from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from campaign_feedback import atomic_json
from tracking_readiness import refresh_tracking, tracking_report


class TrackingReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        env=patch.dict(os.environ,{'DB_PATH':str(self.root/'db'),'META_ACCESS_TOKEN':'test','META_AD_ACCOUNT_ID':'123'})
        env.start();self.addCleanup(env.stop)
        self.now=datetime(2026,9,17,tzinfo=timezone.utc)
        atomic_json(self.root/'tracking-registry.json',{'registrations':[{'festival':'Example Coffee','pixel_id':'123456','source_event_id':'old','reported_capi_status':'active'}]})
        self.db=SimpleNamespace(get_event=lambda eid:{'event_id':eid,'event_date':'2025-01-01','name':'Old edition'},edition_sibling_ids=lambda eid:[eid],get_events=lambda **kw:[{'event_id':'new','name':'Example Coffee','event_date':'2026-10-01'}])

    def test_imported_active_does_not_mean_verified_capi_or_site(self):
        row=tracking_report(self.db,self.now)['registrations'][0]
        self.assertEqual(row['capi_verification'],'not_verified')
        self.assertEqual(row['site_installation_verification'],'not_verified')
        self.assertEqual(row['pixel_observation']['status'],'not_checked')

    def test_previous_edition_cannot_prove_new_event_setup(self):
        report=tracking_report(self.db,self.now)
        self.assertEqual(report['upcoming_without_reference'][0]['event_id'],'new')

    def test_live_read_and_failed_retry_keep_observation_without_false_success(self):
        factory=Mock();factory.return_value.BASE_URL='https://graph.facebook.com/v24.0'
        factory.return_value._api_get.return_value={'id':'123456','name':'Pixel','last_fired_time':'2026-09-16T12:00:00+0000'}
        self.assertEqual(refresh_tracking(factory,self.now)['status'],'current')
        factory.return_value._api_get.side_effect=RuntimeError('private-token')
        self.assertEqual(refresh_tracking(factory,self.now+timedelta(hours=6))['status'],'partial_failure')
        row=tracking_report(self.db,self.now+timedelta(hours=6))['registrations'][0]
        self.assertEqual(row['pixel_observation']['status'],'read_unavailable')
        self.assertEqual(row['pixel_observation']['last_fired_time'],'2026-09-16T12:00:00+0000')
        self.assertNotIn('private-token',str(row))
        self.assertEqual(factory.return_value.session.close.call_count,2)

    def test_identity_mismatch_cannot_be_verified(self):
        factory=Mock();factory.return_value.BASE_URL='https://graph.facebook.com/v24.0'
        factory.return_value._api_get.return_value={'id':'999999'}
        self.assertEqual(refresh_tracking(factory,self.now)['status'],'partial_failure')

    def test_old_observation_is_stale(self):
        atomic_json(self.root/'tracking-observations.json',{'pixels':[{'pixel_id':'123456','status':'read_verified','observed_at':(self.now-timedelta(days=2)).isoformat()}]})
        self.assertEqual(tracking_report(self.db,self.now)['registrations'][0]['pixel_observation']['status'],'stale')

    def test_tracking_route_requires_auth_and_is_read_only(self):
        with patch.dict(os.environ,{'TESTING':'1','CRAFT_AUTO_SYNC':'0','COMMAND_API_KEY':'tracking-test','MAILCHIMP_API_KEY':'','MAILCHIMP_AUDIENCE_ID':'','ANTHROPIC_API_KEY':''}):
            from craft_v2 import _build_app
            client=_build_app().test_client();path='/api/intelligence/tracking'
            self.assertEqual(client.get(path).status_code,401)
            response=client.get(path,headers={'Authorization':'Bearer tracking-test'})
            self.assertEqual(response.status_code,200)
            self.assertIn('no-store',response.headers['Cache-Control'])
            self.assertEqual(client.post(path,headers={'Authorization':'Bearer tracking-test'}).status_code,405)
