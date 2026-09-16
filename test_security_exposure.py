"""Security regression tests for the public-API exposure closure.

What these defend, in the order the exposure mattered:

1. Every /api/ route on this app served customer emails, purchase history and
   segmentation to anyone on the internet. Auth is now deny-by-default in
   create_app, so a newly added route is protected the moment it exists; the
   tests below assert both the default and the small public allowlist.

2. /api/targeting and /api/intelligence returned EXPORT_API_KEY -- the exact
   credential the export routes validated. A secret returned to unauthenticated
   clients is not authentication. No response may carry one now.

3. CORS was Access-Control-Allow-Origin: * on all of it.

4. POST /api/campaigns/<cid>/send reached real Mailchimp through a deprecated
   lossy send, bypassing suppression, buyer exclusion, durable claims, provider
   outcome classification and reconciliation -- and V2_ENABLE_EXTERNAL_SEND did
   not cover it. That route and its implementation are gone; V2 is the only
   path to customer email.
"""

import os
import re
import unittest

os.environ.setdefault("TESTING", "1")
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("CRAFT_AUTO_SYNC", "0")

import craft_unified as cu  # noqa: E402

KEY = "test-command-key-do-not-use-in-production"

# Routes that expose customer identity, purchase history or segmentation.
PII_ROUTES = [
    "/api/customers",
    "/api/customers/someone@example.com",
    "/api/customers/segments",
    "/api/customers/high-value",
    "/api/customers/at-risk",
    "/api/targeting/EVT1",
    "/api/intelligence/EVT1",
    "/api/overlap",
    "/api/export/csv?event_id=EVT1&audience=all",
    "/api/export/intelligence-csv?event_id=EVT1&audience=vips",
    "/api/export/overlap-csv?pair_id=p1&audience=overlap",
]

PUBLIC_ROUTES = ["/api/health"]


def _client():
    db = cu.Database(":memory:")
    app = cu.create_app(db, auto_sync=False)
    app.config["TESTING"] = True
    return app.test_client()


class _AuthEnv(unittest.TestCase):
    def setUp(self):
        self._prev = os.environ.get("COMMAND_API_KEY")
        os.environ["COMMAND_API_KEY"] = KEY
        self.client = _client()

    def tearDown(self):
        if self._prev is None:
            os.environ.pop("COMMAND_API_KEY", None)
        else:
            os.environ["COMMAND_API_KEY"] = self._prev


class TestCustomerDataRequiresAuth(_AuthEnv):
    def test_unauthenticated_pii_is_rejected(self):
        for route in PII_ROUTES:
            with self.subTest(route=route):
                self.assertEqual(self.client.get(route).status_code, 401)

    def test_invalid_bearer_is_rejected(self):
        for route in PII_ROUTES:
            with self.subTest(route=route):
                resp = self.client.get(route, headers={"Authorization": "Bearer wrong-key"})
                self.assertEqual(resp.status_code, 401)

    def test_malformed_authorization_header_is_rejected(self):
        for header in ("", "Basic abc", "Bearer", "bearer " + KEY, KEY):
            with self.subTest(header=header):
                resp = self.client.get("/api/customers", headers={"Authorization": header})
                self.assertEqual(resp.status_code, 401)

    def test_valid_server_auth_is_accepted(self):
        """Authorised callers get through the gate -- not 401."""
        for route in PII_ROUTES:
            with self.subTest(route=route):
                resp = self.client.get(route, headers={"Authorization": f"Bearer {KEY}"})
                self.assertNotEqual(resp.status_code, 401)

    def test_health_stays_public(self):
        """Railway's healthcheck cannot send a bearer token."""
        for route in PUBLIC_ROUTES:
            with self.subTest(route=route):
                self.assertNotEqual(self.client.get(route).status_code, 401)

    def test_unknown_api_route_is_denied_not_leaked(self):
        """Deny-by-default: a route nobody remembered to protect is still protected."""
        self.assertEqual(self.client.get("/api/some-future-endpoint").status_code, 401)

    def test_missing_command_key_fails_closed(self):
        """With no key configured the API refuses rather than serving openly."""
        os.environ.pop("COMMAND_API_KEY", None)
        client = _client()
        self.assertEqual(client.get("/api/customers").status_code, 503)


class TestNoSecretsInResponses(_AuthEnv):
    """A secret in a response body is a secret you have published."""

    SECRET_SHAPED = re.compile(
        r"EAA[A-Za-z0-9]{20,}"          # Meta access token
        r"|sk-ant-[A-Za-z0-9_-]{20,}"   # Anthropic key
        r"|[0-9a-f]{32}-us\d{1,2}"      # Mailchimp key
        r"|\"export_token\""            # the specific regression
    )

    def test_no_response_contains_a_secret_shaped_value(self):
        for route in PII_ROUTES + PUBLIC_ROUTES:
            with self.subTest(route=route):
                resp = self.client.get(route, headers={"Authorization": f"Bearer {KEY}"})
                body = resp.get_data(as_text=True)
                self.assertIsNone(self.SECRET_SHAPED.search(body),
                                  f"{route} returned a secret-shaped value")

    def test_export_token_is_gone_from_source(self):
        with open(_here("craft_unified.py")) as fh:
            source = fh.read()
        emitters = [ln for ln in source.splitlines()
                    if "'export_token'" in ln and not ln.strip().startswith("#")]
        self.assertEqual(emitters, [], f"export_token still emitted: {emitters}")

    def test_meta_debug_does_not_preview_the_token(self):
        with open(_here("craft_unified.py")) as fh:
            self.assertNotIn("token_preview", fh.read())


class TestCorsIsNotWildcard(_AuthEnv):
    def test_no_wildcard_origin_header_on_protected_routes(self):
        for route in PII_ROUTES:
            with self.subTest(route=route):
                resp = self.client.get(route, headers={"Authorization": f"Bearer {KEY}",
                                                       "Origin": "https://evil.example"})
                self.assertNotEqual(resp.headers.get("Access-Control-Allow-Origin"), "*")

    def test_source_has_no_hardcoded_wildcard(self):
        with open(_here("craft_unified.py")) as fh:
            source = fh.read()
        self.assertNotIn("'Access-Control-Allow-Origin': '*'", source)

    def test_cors_is_configured_with_explicit_origins(self):
        with open(_here("craft_unified.py")) as fh:
            source = fh.read()
        self.assertNotIn("\n    CORS(app)\n", source, "bare CORS(app) allows every origin")
        self.assertIn("allowed_origins", source)


class TestLegacySendPathIsGone(_AuthEnv):
    """V2 must be the only route to a customer's inbox."""

    def test_legacy_send_route_does_not_exist(self):
        resp = self.client.post("/api/campaigns/abc/send",
                                headers={"Authorization": f"Bearer {KEY}"})
        self.assertIn(resp.status_code, (404, 405))

    def test_no_url_rule_can_reach_a_legacy_send(self):
        db = cu.Database(":memory:")
        app = cu.create_app(db, auto_sync=False)
        rules = [str(r) for r in app.url_map.iter_rules()]
        offenders = [r for r in rules if r.endswith("/send")]
        self.assertEqual(offenders, [], f"a send route is still registered: {offenders}")

    def test_lossy_provider_send_is_deleted(self):
        with open(_here("craft_engine.py")) as fh:
            source = fh.read()
        self.assertNotIn("def send_campaign(self, mc_campaign_id: str) -> bool:", source)
        self.assertNotIn("def send_campaign(self, campaign_id: str", source)

    def test_strict_provider_send_survives_for_v2(self):
        with open(_here("craft_engine.py")) as fh:
            source = fh.read()
        self.assertIn("def send_campaign_strict(", source)
        self.assertIn("def create_campaign_strict(", source)

    def test_v2_execution_adapter_still_owns_the_send(self):
        with open(_here("execution_adapter.py")) as fh:
            self.assertIn("send_campaign_strict(", fh.read())

    def test_dry_run_survives_and_writes_nothing_external(self):
        with open(_here("craft_engine.py")) as fh:
            source = fh.read()
        self.assertIn("def validate_campaign(", source)
        start = source.index("def validate_campaign(")
        body = source[start:source.index("\n    # ", start)]
        for forbidden in ("ensure_members", "create_campaign", "send_campaign_strict"):
            with self.subTest(forbidden=forbidden):
                self.assertNotIn(forbidden, body)


class TestExternalSendStaysDisabled(unittest.TestCase):
    def setUp(self):
        self._prev = os.environ.get("V2_ENABLE_EXTERNAL_SEND")

    def tearDown(self):
        if self._prev is None:
            os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        else:
            os.environ["V2_ENABLE_EXTERNAL_SEND"] = self._prev

    def test_unset_means_disabled(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        from craft_v2 import external_send_enabled
        self.assertFalse(external_send_enabled())

    def test_zero_means_disabled(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "0"
        from craft_v2 import external_send_enabled
        self.assertFalse(external_send_enabled())

    def test_execution_adapter_still_checks_the_flag(self):
        with open(_here("execution_adapter.py")) as fh:
            self.assertIn("V2_ENABLE_EXTERNAL_SEND", fh.read())


class TestBrowserBundleCarriesNoServerKey(unittest.TestCase):
    """COMMAND_API_KEY must stay server-side; NEXT_PUBLIC_ would ship it."""

    def _client_sources(self):
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pages")
        for dirpath, _, names in os.walk(root):
            # pages/api/** runs server-side; everything else reaches the browser.
            if os.sep + "api" in dirpath.replace(root, ""):
                continue
            for name in names:
                if name.endswith(".js"):
                    yield os.path.join(dirpath, name)

    def test_no_client_file_references_a_server_secret(self):
        for path in self._client_sources():
            with open(path) as fh:
                source = fh.read()
            for secret in ("COMMAND_API_KEY", "EXPORT_API_KEY", "MAILCHIMP_API_KEY",
                           "META_ACCESS_TOKEN", "ANTHROPIC_API_KEY"):
                with self.subTest(path=os.path.basename(path), secret=secret):
                    self.assertNotIn(secret, source)

    def test_no_secret_is_exposed_via_next_public(self):
        root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pages")
        for dirpath, _, names in os.walk(root):
            for name in names:
                if not name.endswith(".js"):
                    continue
                with open(os.path.join(dirpath, name)) as fh:
                    for line in fh:
                        if "NEXT_PUBLIC_" in line:
                            with self.subTest(file=name, line=line.strip()[:60]):
                                self.assertNotRegex(line, r"NEXT_PUBLIC_\w*(KEY|TOKEN|SECRET)")

    def test_browser_does_not_call_the_backend_directly(self):
        """Direct calls are what forced every endpoint to stay unauthenticated."""
        for path in self._client_sources():
            with open(path) as fh:
                source = fh.read()
            with self.subTest(path=os.path.basename(path)):
                self.assertNotIn("${API_BASE}/api/", source)


def _here(name):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), name)


if __name__ == "__main__":
    unittest.main()
