import hashlib
import os
from pathlib import Path
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import zipfile
from campaign_feedback import atomic_json
from ready_posts import build_posts, media_path, package, read_posts, ticket_url


class ReadyPostTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        env = patch.dict(os.environ, {'DB_PATH': str(self.root/'db')})
        env.start(); self.addCleanup(env.stop)
        self.now = datetime.now(timezone.utc)
        raw = b'\xff\xd8\xffsynthetic-test-image'
        self.key = hashlib.sha256(raw).hexdigest()
        (self.root/'ready-post-media').mkdir()
        (self.root/'ready-post-media'/(self.key+'.jpg')).write_bytes(raw)
        self.asset = {'id': self.key, 'reviewed': True, 'cross_city_safe': True, 'category': 'coffee', 'source_city': 'Source city', 'source_url': 'https://www.instagram.com/example/p/abc/'}
        self.pack = {'id': 'pack', 'festival': 'Example Coffee Festival', 'city': 'Destination city', 'event_date': (self.now+timedelta(days=20)).date().isoformat(), 'event_ids': ['123'], 'ticket_links': ['https://www.eventbrite.com/e/123'], 'week': '2026-09-14', 'content': []}

    def test_complete_package_contains_exact_photo_caption_and_ticket_links(self):
        value = build_posts([self.pack], [self.asset], self.now)
        self.assertEqual(len(value['posts']), 1)
        post = value['posts'][0]
        self.assertFalse(post['execution_allowed'])
        self.assertNotIn('Source city', post['caption'])
        atomic_json(self.root/'ready-posts.json', value)
        with zipfile.ZipFile(package(post['id'])) as bundle:
            self.assertEqual(bundle.namelist(), ['photo.jpg', 'caption.txt', 'review-and-links.txt'])
            self.assertEqual(bundle.read('photo.jpg'), media_path(self.key).read_bytes())
            self.assertEqual(bundle.read('caption.txt').decode(), post['caption'])
            self.assertIn('utm_content='+post['id'], bundle.read('review-and-links.txt').decode())

    def test_unreviewed_wrong_category_and_missing_media_are_held(self):
        for changes in [{'reviewed': False}, {'cross_city_safe': False}, {'category': 'wine'}, {'id': '0'*64}]:
            value = build_posts([self.pack], [dict(self.asset, **changes)], self.now)
            self.assertEqual(value['posts'], [])
            self.assertEqual(len(value['held']), 1)

    def test_missing_or_past_event_is_never_ready(self):
        for changes in [{'event_date': None}, {'event_date': '2000-01-01'}, {'ticket_links': []}, {'event_ids': []}]:
            self.assertEqual(build_posts([dict(self.pack, **changes)], [self.asset], self.now)['posts'], [])

    def test_media_path_and_hash_validation(self):
        for key in ['../secret', 'a'*63, '0'*64]:
            with self.assertRaises(ValueError): media_path(key)
        media_path(self.key).write_bytes(b'changed')
        with self.assertRaises(ValueError): media_path(self.key)

    def test_symlink_is_rejected(self):
        path = media_path(self.key); raw = path.read_bytes(); path.unlink()
        target = self.root/'outside'; target.write_bytes(raw); path.symlink_to(target)
        with self.assertRaises(ValueError): media_path(self.key)

    def test_old_package_requires_refresh_and_weekly_identity_is_stable(self):
        one = build_posts([self.pack], [self.asset], self.now)
        two = build_posts([self.pack], [self.asset], self.now+timedelta(hours=6))
        self.assertEqual(one['posts'][0]['id'], two['posts'][0]['id'])
        one['generated_at'] = (self.now-timedelta(days=2)).isoformat()
        atomic_json(self.root/'ready-posts.json', one)
        with self.assertRaises(ValueError): package(one['posts'][0]['id'])
        self.assertEqual(read_posts()['status'], 'stale')

    def test_ticket_destinations_cannot_redirect_to_arbitrary_hosts(self):
        for url in ['http://www.eventbrite.com/e/123', 'https://evil.test/e/123', 'https://www.eventbrite.com@evil.test/e/123', 'https://www.eventbrite.com.evil.test/e/123']:
            with self.assertRaises(ValueError): ticket_url(url, 'post')


if __name__ == '__main__': unittest.main()

class ReadyPostBoundaryTests(ReadyPostTests):
    def test_protected_media_and_zip_are_read_only(self):
        with patch.dict(os.environ, {'TESTING':'1','CRAFT_AUTO_SYNC':'0','COMMAND_API_KEY':'ready-test','MAILCHIMP_API_KEY':'','MAILCHIMP_AUDIENCE_ID':'','ANTHROPIC_API_KEY':''}):
            from craft_v2 import _build_app
            client = _build_app().test_client()
            value = build_posts([self.pack], [self.asset], self.now)
            atomic_json(self.root/'ready-posts.json', value)
            paths = ['/api/intelligence/ready-posts', '/api/intelligence/ready-posts/media/'+self.key,
                     '/api/intelligence/ready-posts/'+value['posts'][0]['id']+'/download']
            for path in paths:
                self.assertEqual(client.get(path).status_code, 401)
                response = client.get(path, headers={'Authorization':'Bearer ready-test'})
                self.assertEqual(response.status_code, 200)
                self.assertIn('no-store', response.headers['Cache-Control'])
                self.assertEqual(client.post(path, headers={'Authorization':'Bearer ready-test'}).status_code, 405)
