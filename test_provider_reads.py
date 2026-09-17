"""Exercise the real HTTP adapter against an isolated local server."""
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch
from provider_reads import ReadOnlySession


class TestProviderReads(unittest.TestCase):
    def setUp(self):
        self.codes = [200]
        self.headers = {}
        self.calls = 0
        test = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                test.calls += 1
                code = test.codes[min(test.calls - 1, len(test.codes) - 1)]
                self.send_response(code)
                for key, value in test.headers.items():
                    self.send_header(key, value)
                self.end_headers()
                self.wfile.write(b'{"ok":true}')
            def log_message(self, *args):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = 'http://127.0.0.1:%s/' % self.server.server_port
        self.session = ReadOnlySession()
        self.sleep = patch('urllib3.util.retry.time.sleep').start()

    def tearDown(self):
        patch.stopall()
        self.session.close()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()

    def test_transient_server_error_recovers(self):
        self.codes = [500, 503, 200]
        self.assertTrue(self.session.get(self.url, timeout=2).json()['ok'])
        self.assertEqual(self.calls, 3)

    def test_exhaustion_returns_failure_after_three_attempts(self):
        self.codes = [500]
        self.assertEqual(self.session.get(self.url, timeout=2).status_code, 500)
        self.assertEqual(self.calls, 3)

    def test_auth_failure_is_not_retried(self):
        self.codes = [401]
        self.assertEqual(self.session.get(self.url, timeout=2).status_code, 401)
        self.assertEqual(self.calls, 1)

    def test_rate_limit_honors_delay(self):
        self.codes = [429, 200]
        self.headers = {'Retry-After': '7'}
        self.assertEqual(self.session.get(self.url, timeout=2).status_code, 200)
        self.sleep.assert_called_once_with(7)

    def test_long_delay_fails_without_early_retry(self):
        self.codes = [429]
        self.headers = {'Retry-After': '3600'}
        with self.assertRaisesRegex(Exception, '60 second budget'):
            self.session.get(self.url, timeout=2)
        self.assertEqual(self.calls, 1)
        self.sleep.assert_not_called()

    def test_writes_never_reach_transport(self):
        with self.assertRaises(ValueError):
            self.session.post(self.url, json={'send': True})
        self.assertEqual(self.calls, 0)

    def test_redirects_are_not_followed(self):
        self.codes = [302]
        self.headers = {'Location': self.url + 'other'}
        self.assertEqual(self.session.get(self.url, timeout=2).status_code, 302)
        self.assertEqual(self.calls, 1)
