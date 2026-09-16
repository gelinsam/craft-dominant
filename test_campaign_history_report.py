import base64
import json
import os
import unittest
import zlib
from unittest.mock import patch
from campaign_history_report import load_report


def bundle(report):
    return base64.b64encode(zlib.compress(json.dumps(report).encode())).decode()


class CampaignHistoryReportTests(unittest.TestCase):
    def test_private_report_loads(self):
        report = {'version': 1, 'as_of': '2026-09-16', 'categories': [
            {'category': c} for c in ['coffee', 'wine', 'cocktail', 'whiskey', 'beer']]}
        with patch.dict(os.environ, {'CRAFT_CAMPAIGN_REPORT_ZLIB_B64': bundle(report)}):
            self.assertEqual(load_report(), report)

    def test_missing_invalid_and_oversized_reports_fail_closed(self):
        for value in ['', 'not-base64', bundle({}), bundle([]), bundle({'huge': 'x' * 2_000_001})]:
            with patch.dict(os.environ, {'CRAFT_CAMPAIGN_REPORT_ZLIB_B64': value}):
                self.assertIsNone(load_report())

    def test_route_requires_existing_auth_and_does_not_cache(self):
        os.environ.setdefault('TESTING', '1')
        os.environ.setdefault('CRAFT_AUTO_SYNC', '0')
        from craft_unified import create_app, Database
        with patch.dict(os.environ, {'COMMAND_API_KEY': 'test-campaign-key', 'CRAFT_CAMPAIGN_REPORT_ZLIB_B64': ''}):
            client = create_app(Database(':memory:'), auto_sync=False).test_client()
            path = '/api/intelligence/campaign-history'
            self.assertEqual(client.get(path).status_code, 401)
            response = client.get(path, headers={'Authorization': 'Bearer test-campaign-key'})
            self.assertEqual(response.status_code, 503)
            self.assertIn('no-store', response.headers['Cache-Control'])
            self.assertEqual(response.json, {'error': 'campaign_evidence_unavailable'})
            self.assertEqual(client.post(path, headers={'Authorization': 'Bearer test-campaign-key'}).status_code, 405)
