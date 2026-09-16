"""Mailchimp webhook authenticity and payload handling.

Two defects converge on this endpoint.

AUTHENTICITY. The suppression table is safety-critical: ExecutionAdapter refuses
to send when it cannot trust that list, and every audience is filtered through
it. The webhook sits outside the bearer gate -- correctly, since Mailchimp
cannot send an Authorization header -- but it verified nothing, so any caller
could POST an unsubscribe for an arbitrary address and move the suppression
sentinel with it.

Mailchimp's Marketing API guide "Synchronize Audience Data with Webhooks"
documents HMAC signing for audience webhooks:

    X-Mailchimp-Signature: t=<unix_seconds>,v1=<hex>
    v1 = HMAC-SHA256(signing_secret, "{t}.{raw_body}")

verified against the raw body, compared in constant time, rejected beyond a
five-minute window.

PAYLOAD SHAPE. Mailchimp delivers application/x-www-form-urlencoded with
bracket notation -- data[email]=... -- not JSON. A flat form.to_dict() leaves
the key as the literal string "data[email]", so a consumer reading
payload['data']['email'] finds nothing and the handler returns success having
written no suppression. A genuine unsubscribe is then lost silently, which is
worse than rejecting it. The earlier tests here posted JSON only and missed it
entirely, so the form-encoded path is now the primary case.
"""

import hashlib
import hmac
import json
import os
import time
import unittest
from datetime import datetime, timezone
from unittest.mock import patch
from urllib.parse import quote_plus, urlencode

os.environ.setdefault("TESTING", "1")
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("CRAFT_AUTO_SYNC", "0")

import craft_unified as cu  # noqa: E402
from suppression_guard import SuppressionGuard, SuppressionStatus  # noqa: E402
from craft_engine import (  # noqa: E402
    MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS,
    normalize_mailchimp_form,
    verify_mailchimp_signature,
)

SIGNING_SECRET = "sig-secret-for-tests-only"
ROUTE = "/api/webhook/mailchimp"
VICTIM = "victim@example.com"

# Exactly the shape Mailchimp documents for an unsubscribe.
FORM_UNSUB = [
    ("type", "unsubscribe"),
    ("fired_at", "2026-03-26 21:40:57"),
    ("data[action]", "unsub"),
    ("data[reason]", "manual"),
    ("data[id]", "8a25ff1d98"),
    ("data[list_id]", "a6b5da1054"),
    ("data[email]", VICTIM),
    ("data[email_type]", "html"),
    ("data[merges][EMAIL]", VICTIM),
    ("data[merges][FNAME]", "Ada"),
]
FORM_CLEANED = [
    ("type", "cleaned"),
    ("data[list_id]", "a6b5da1054"),
    ("data[email]", "bounced@example.com"),
    ("data[reason]", "hard"),
]


def sign(raw: bytes, secret: str = SIGNING_SECRET, timestamp: int = None) -> str:
    ts = int(time.time()) if timestamp is None else timestamp
    mac = hmac.new(secret.encode(), f"{ts}.".encode() + raw, hashlib.sha256).hexdigest()
    return f"t={ts},v1={mac}"


class _Harness(unittest.TestCase):
    def setUp(self):
        self._env = {k: os.environ.get(k) for k in (
            "MAILCHIMP_WEBHOOK_SIGNING_SECRET", "MAILCHIMP_WEBHOOK_PATH_SECRET",
            "COMMAND_API_KEY")}
        os.environ["MAILCHIMP_WEBHOOK_SIGNING_SECRET"] = SIGNING_SECRET
        os.environ.pop("MAILCHIMP_WEBHOOK_PATH_SECRET", None)
        os.environ["COMMAND_API_KEY"] = "configured-but-irrelevant-here"
        self.db = cu.Database(":memory:")
        self.app = cu.create_app(self.db, auto_sync=False)
        self.app.config["TESTING"] = True
        self.client = self.app.test_client()
        # Production has this table; create it up front so the strict reads
        # above are asserting contents rather than tripping over absence.
        SuppressionGuard(self.db)

    def tearDown(self):
        for key, value in self._env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _count(self, table):
        """Deliberately no try/except.

        The previous version swallowed the exception and returned 0, so a
        missing table read as "empty" and an assertion comparing counts passed
        while proving nothing. A missing table must fail the test.
        """
        return self.db.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def sentinel(self):
        """Full sentinel contents, not a row count.

        Counting rows could not detect the failure that matters: the sentinel
        row exists but its row_count and last_mutation_* fields were never
        updated after a suppression was written.
        """
        rows = self.db.conn.execute(
            "SELECT row_count, last_mutation_at, last_mutation_source "
            "FROM v2_suppression_sync").fetchall()
        return [dict(r) for r in rows]

    def state(self):
        """Everything a forged delivery must not be able to move."""
        return {
            "suppressions": self._count("suppressions"),
            "email_events": self._count("email_events"),
            "sentinel": self.sentinel(),
        }

    def guard(self):
        return SuppressionGuard(self.db)

    def bootstrap_full_refresh(self):
        """Stand in for a completed POST /api/v2/suppressions/refresh.

        Only refresh_from_mailchimp sets last_full_refresh_at, and that needs a
        live Mailchimp client. Writing the sentinel directly gives the guard the
        authoritative baseline it requires without a network call. A seed row
        keeps the list non-empty, so EMPTY_UNVERIFIED is not what we measure.
        """
        self.db.conn.execute(
            "INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("seed@example.com",))
        self.db.conn.commit()
        now = datetime.now(timezone.utc).isoformat()
        self.guard()._write_sentinel({
            "last_synced_at": now,
            "last_full_refresh_at": now,
            "last_full_refresh_source": "test_bootstrap",
            "row_count": self._count("suppressions"),
            "source": "test_bootstrap",
        })

    def suppressed(self):
        return [r[0] for r in self.db.conn.execute("SELECT email FROM suppressions")]

    def post_form(self, fields, secret=SIGNING_SECRET, timestamp=None,
                  header=None, body_override=None, query=""):
        raw = urlencode(fields).encode()
        sig = header if header is not None else sign(raw, secret, timestamp)
        return self.client.post(
            ROUTE + query,
            data=body_override if body_override is not None else raw,
            content_type="application/x-www-form-urlencoded",
            headers={"X-Mailchimp-Signature": sig} if sig else {},
        )

    def post_json(self, payload, secret=SIGNING_SECRET, timestamp=None):
        raw = json.dumps(payload).encode()
        return self.client.post(
            ROUTE, data=raw, content_type="application/json",
            headers={"X-Mailchimp-Signature": sign(raw, secret, timestamp)},
        )


# ---------------------------------------------------------------- unit level

class TestSignatureHelper(unittest.TestCase):
    def test_valid_signature_verifies(self):
        raw = b"type=unsubscribe"
        ok, reason = verify_mailchimp_signature(SIGNING_SECRET, sign(raw), raw)
        self.assertTrue(ok, reason)

    def test_tolerance_matches_the_documented_five_minutes(self):
        self.assertEqual(MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS, 300)

    def test_body_is_bound_to_the_signature(self):
        raw = b"type=unsubscribe&data[email]=a@b.com"
        header = sign(raw)
        ok, reason = verify_mailchimp_signature(SIGNING_SECRET, header,
                                                b"type=unsubscribe&data[email]=attacker@evil.com")
        self.assertFalse(ok)
        self.assertEqual(reason, "signature_mismatch")

    def test_timestamp_is_bound_to_the_signature(self):
        """Re-dating a captured delivery must not make it fresh."""
        raw = b"x=1"
        old = int(time.time()) - 10_000
        header = sign(raw, timestamp=old)
        fresh = header.replace(f"t={old}", f"t={int(time.time())}")
        ok, reason = verify_mailchimp_signature(SIGNING_SECRET, fresh, raw)
        self.assertFalse(ok)
        self.assertEqual(reason, "signature_mismatch")

    def test_rejections(self):
        raw = b"x=1"
        now = time.time()
        cases = {
            "missing_signature_header": "",
            "malformed_signature_header": "garbage",
            "stale_timestamp": sign(raw, timestamp=int(now) - 301),
            "timestamp_in_future": sign(raw, timestamp=int(now) + 301),
            "signature_mismatch": f"t={int(now)},v1={'a' * 64}",
        }
        for expected, header in cases.items():
            with self.subTest(expected=expected):
                ok, reason = verify_mailchimp_signature(SIGNING_SECRET, header, raw, now=now)
                self.assertFalse(ok)
                self.assertEqual(reason, expected)

    def test_unsupported_version_only_is_rejected(self):
        raw = b"x=1"
        header = f"t={int(time.time())},v2=deadbeef"
        ok, reason = verify_mailchimp_signature(SIGNING_SECRET, header, raw)
        self.assertFalse(ok)
        self.assertEqual(reason, "malformed_signature_header")

    def test_boundary_is_accepted_at_exactly_the_tolerance(self):
        raw = b"x=1"
        now = float(int(time.time()))   # exact second; a fractional clock is +epsilon old
        header = sign(raw, timestamp=int(now) - MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS)
        ok, reason = verify_mailchimp_signature(SIGNING_SECRET, header, raw, now=now)
        self.assertTrue(ok, reason)

    def test_one_second_past_the_tolerance_is_rejected(self):
        raw = b"x=1"
        now = float(int(time.time()))
        header = sign(raw, timestamp=int(now) - MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS - 1)
        ok, reason = verify_mailchimp_signature(SIGNING_SECRET, header, raw, now=now)
        self.assertFalse(ok)
        self.assertEqual(reason, "stale_timestamp")


class TestFormNormalization(unittest.TestCase):
    """The defect that made a signed, genuine unsubscribe a no-op."""

    def test_bracket_notation_becomes_nested(self):
        out = normalize_mailchimp_form(dict(FORM_UNSUB))
        self.assertEqual(out["type"], "unsubscribe")
        self.assertEqual(out["data"]["email"], VICTIM)
        self.assertEqual(out["data"]["merges"]["FNAME"], "Ada")

    def test_flat_keys_survive(self):
        self.assertEqual(normalize_mailchimp_form({"type": "cleaned"}), {"type": "cleaned"})

    def test_empty_form_is_empty_dict(self):
        self.assertEqual(normalize_mailchimp_form({}), {})


# ---------------------------------------------------- through the real app

class TestAuthenticFormDeliveries(_Harness):
    """Mailchimp posts form-encoded. These are the deliveries that matter."""

    def test_signed_form_unsubscribe_actually_suppresses(self):
        resp = self.post_form(FORM_UNSUB)
        self.assertEqual(resp.status_code, 200)
        self.assertIn(VICTIM, self.suppressed(),
                      "a signed, genuine unsubscribe must record a suppression")

    def test_signed_form_unsubscribe_updates_the_sentinel_contents(self):
        """The sentinel must reflect the suppression, not merely exist."""
        self.post_form(FORM_UNSUB)
        self.assertIn(VICTIM, self.suppressed())
        rows = self.sentinel()
        self.assertEqual(len(rows), 1, "expected exactly one sentinel row")
        row = rows[0]
        self.assertEqual(row["row_count"], self._count("suppressions"),
                         "sentinel row_count must match the suppression table")
        self.assertEqual(row["last_mutation_source"], "webhook_unsubscribe")
        self.assertIsNotNone(row["last_mutation_at"])

    def test_webhook_alone_does_not_claim_completeness(self):
        """A webhook mutation is evidence of one event, not of a synced list.

        record_mutation deliberately leaves last_full_refresh_at alone, so the
        guard still refuses until a real refresh has run. Pinned here because
        it would be easy to "fix" by having the webhook claim completeness.
        """
        self.post_form(FORM_UNSUB)
        status, details = self.guard().validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED, details)

    def test_guard_is_healthy_after_a_refresh_plus_a_signed_webhook(self):
        """With a completed refresh as the baseline, the webhook keeps it valid."""
        self.bootstrap_full_refresh()
        self.post_form(FORM_UNSUB)
        status, details = self.guard().validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY, details)
        self.assertEqual(self.sentinel()[0]["row_count"], self._count("suppressions"))
        self.assertIn(VICTIM, self.suppressed())

    def test_signed_cleaned_records_a_bounce_and_updates_the_sentinel(self):
        self.post_form(FORM_CLEANED)
        rows = self.db.conn.execute(
            "SELECT email, reason FROM suppressions").fetchall()
        by_email = {r[0]: r[1] for r in rows}
        self.assertIn("bounced@example.com", by_email)
        self.assertEqual(by_email["bounced@example.com"], "bounce")
        self.assertEqual(self.sentinel()[0]["last_mutation_source"], "webhook_cleaned")

    def test_the_sentinel_assertion_has_teeth(self):
        """Prove the regression test fails if the sentinel update is disabled.

        Isolated to this test: record_mutation is stubbed out, the same signed
        delivery is replayed, and the assertions above are shown to fail. Without
        this, a silently removed record_mutation() call would leave the suite
        green.
        """
        import craft_engine

        # Baseline: a completed refresh, so the guard would otherwise be HEALTHY
        # and any failure below is attributable to the missing sentinel update.
        self.bootstrap_full_refresh()
        status, _ = self.guard().validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY,
                         "baseline should be healthy before the stub is applied")

        with patch.object(craft_engine.SuppressionGuard, "record_mutation",
                          lambda self, source="webhook": {"stubbed": True}):
            self.post_form(FORM_UNSUB)

        self.assertIn(VICTIM, self.suppressed(),
                      "the suppression itself should still have been written")

        self.assertNotEqual(
            self.sentinel()[0]["row_count"], self._count("suppressions"),
            "sentinel still matched the table with record_mutation disabled — "
            "the assertion would not catch a regression")

        status, details = self.guard().validate()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH, details)

    def test_json_delivery_also_works(self):
        resp = self.post_json({"type": "unsubscribe", "data": {"email": "json@example.com"}})
        self.assertEqual(resp.status_code, 200)
        self.assertIn("json@example.com", self.suppressed())


class TestForgedDeliveriesChangeNothing(_Harness):
    def _assert_inert(self, resp, expected_status, before):
        self.assertEqual(resp.status_code, expected_status)
        self.assertEqual(self.state(), before)
        self.assertNotIn(VICTIM, self.suppressed())

    def test_missing_signature(self):
        before = self.state()
        self._assert_inert(self.post_form(FORM_UNSUB, header=""), 401, before)

    def test_malformed_signature(self):
        before = self.state()
        self._assert_inert(self.post_form(FORM_UNSUB, header="not-a-signature"), 401, before)

    def test_wrong_signing_secret(self):
        before = self.state()
        self._assert_inert(self.post_form(FORM_UNSUB, secret="attacker-secret"), 401, before)

    def test_tampered_body_after_signing(self):
        """Signed for one address, delivered for another."""
        before = self.state()
        raw = urlencode(FORM_UNSUB).encode()
        # urlencode percent-escapes the @, so tamper with the encoded form --
        # replacing the plain address would leave the bytes identical and the
        # test would pass while proving nothing.
        tampered = raw.replace(quote_plus(VICTIM).encode(), quote_plus("attacker@evil.com").encode())
        assert tampered != raw, "tamper did not change the body"
        resp = self.client.post(
            ROUTE, data=tampered, content_type="application/x-www-form-urlencoded",
            headers={"X-Mailchimp-Signature": sign(raw)})
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(self.state(), before)
        self.assertNotIn("attacker@evil.com", self.suppressed())

    def test_stale_replay(self):
        before = self.state()
        self._assert_inert(
            self.post_form(FORM_UNSUB, timestamp=int(time.time()) - 3600), 401, before)

    def test_future_timestamp(self):
        before = self.state()
        self._assert_inert(
            self.post_form(FORM_UNSUB, timestamp=int(time.time()) + 3600), 401, before)

    def test_unsigned_plain_post(self):
        before = self.state()
        resp = self.client.post(ROUTE, json={"type": "unsubscribe", "data": {"email": VICTIM}})
        self._assert_inert(resp, 401, before)

    def test_unconfigured_signing_secret_fails_closed(self):
        os.environ.pop("MAILCHIMP_WEBHOOK_SIGNING_SECRET", None)
        before = self.state()
        self._assert_inert(self.post_form(FORM_UNSUB), 503, before)

    def test_sustained_forgery_changes_nothing(self):
        before = self.state()
        for i in range(25):
            self.post_form(FORM_UNSUB, secret=f"guess-{i}")
        self.assertEqual(self.state(), before)


class TestPathSecretIsAnAdditionalLayer(_Harness):
    """Retained as defence in depth, under its own name -- never a substitute."""

    def test_valid_signature_still_needs_the_path_secret_when_configured(self):
        os.environ["MAILCHIMP_WEBHOOK_PATH_SECRET"] = "path-secret"
        before = self.state()
        resp = self.post_form(FORM_UNSUB)          # correctly signed, no ?secret=
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(self.state(), before)

    def test_both_factors_together_are_accepted(self):
        os.environ["MAILCHIMP_WEBHOOK_PATH_SECRET"] = "path-secret"
        resp = self.post_form(FORM_UNSUB, query="?secret=path-secret")
        self.assertEqual(resp.status_code, 200)
        self.assertIn(VICTIM, self.suppressed())

    def test_path_secret_alone_cannot_authenticate(self):
        os.environ["MAILCHIMP_WEBHOOK_PATH_SECRET"] = "path-secret"
        before = self.state()
        resp = self.post_form(FORM_UNSUB, secret="wrong", query="?secret=path-secret")
        self.assertEqual(resp.status_code, 401)
        self.assertEqual(self.state(), before)

    def test_secrets_are_distinct_env_vars(self):
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "craft_engine.py")
        with open(path) as fh:
            source = fh.read()
        self.assertIn("MAILCHIMP_WEBHOOK_SIGNING_SECRET", source)
        self.assertIn("MAILCHIMP_WEBHOOK_PATH_SECRET", source)


class TestGetHandshake(_Harness):
    def test_get_returns_200(self):
        self.assertEqual(self.client.get(ROUTE).status_code, 200)

    def test_get_works_without_any_secret_configured(self):
        os.environ.pop("MAILCHIMP_WEBHOOK_SIGNING_SECRET", None)
        self.assertEqual(self.client.get(ROUTE).status_code, 200)

    def test_get_mutates_nothing(self):
        before = self.state()
        self.client.get(ROUTE)
        self.assertEqual(self.state(), before)

    def test_get_is_not_behind_the_bearer_gate(self):
        self.assertNotEqual(self.client.get(ROUTE).status_code, 401)


if __name__ == "__main__":
    unittest.main()


class TestMailchimpInventoryIntegrity(unittest.TestCase):
    def setUp(self):
        from craft_engine import MailchimpClient
        self.client = MailchimpClient.__new__(MailchimpClient)
        self.client.audience_id = 'philly'

    def test_distinct_audiences_remain_distinct(self):
        from unittest.mock import Mock
        self.client._request = Mock(return_value={'total_items': 2, 'lists': [
            {'id':'philly', 'name':'Philly Whiskey', 'stats': {'member_count':100}},
            {'id':'austin', 'name':'Austin Coffee', 'stats': {'member_count':200}}]})
        result = self.client.audience_inventory()
        self.assertEqual(len(result), 2)
        self.assertTrue(result[0]['legacy_configured_audience'])
        self.assertFalse(result[1]['legacy_configured_audience'])
        self.assertNotIn('email', result[0])
        self.assertEqual(self.client._request.call_args.args[0], 'GET')

    def test_truncated_inventory_is_not_reported_complete(self):
        from unittest.mock import Mock
        self.client._request = Mock(return_value={'total_items':30, 'lists':[]})
        with self.assertRaises(RuntimeError):
            self.client.audience_inventory()

    def test_truncated_suppression_page_preserves_unknown(self):
        from unittest.mock import Mock
        self.client._request = Mock(return_value={'total_items':30, 'members':[]})
        self.assertIsNone(self.client.get_suppressed_members())

    def test_missing_suppression_total_is_not_empty_success(self):
        from unittest.mock import Mock
        self.client._request = Mock(return_value={'members':[]})
        self.assertIsNone(self.client.get_suppressed_members())


class TestMailchimpConsentBoundary(unittest.TestCase):
    def setUp(self):
        from craft_engine import MailchimpClient
        from unittest.mock import Mock
        self.client = MailchimpClient.__new__(MailchimpClient)
        self.client.audience_id = 'austin'
        self.client._request_strict = Mock()
        self.client._get_members_by_status = Mock(return_value=['yes@example.com'])

    def test_buyer_without_audience_consent_is_never_subscribed(self):
        with self.assertRaises(RuntimeError):
            self.client.ensure_members(['buyer@example.com'], tag='test')
        self.client._request_strict.assert_not_called()

    def test_unknown_consent_blocks_before_provider_mutation(self):
        self.client._get_members_by_status.return_value = None
        with self.assertRaises(RuntimeError):
            self.client.ensure_members(['yes@example.com'], tag='test')
        self.client._request_strict.assert_not_called()

    def test_verified_segment_contains_exact_existing_subscribers(self):
        from unittest.mock import Mock
        from provider_outcome import ProviderResponse
        self.client._request_strict.return_value = ProviderResponse(200, {'id':123})
        self.client._request = Mock(return_value={'total_items':1, 'members':[
            {'email_address':'yes@example.com', 'status':'subscribed'}]})
        result = self.client.ensure_members(['yes@example.com'], tag='attempt-one')
        self.assertEqual(result['added'], 0)
        self.assertEqual(self.client.get_tag_segment_id('attempt-one'), 123)
        self.assertIsNone(self.client.get_tag_segment_id('old-attempt'))
        args = self.client._request_strict.call_args.args
        self.assertEqual(args[1], '/lists/austin/segments')
        self.assertNotIn('status_if_new', str(args))

    def test_equal_size_wrong_membership_still_blocks(self):
        from unittest.mock import Mock
        from provider_outcome import ProviderResponse
        self.client._request_strict.return_value = ProviderResponse(200, {'id':123})
        self.client._request = Mock(return_value={'total_items':1, 'members':[
            {'email_address':'different@example.com', 'status':'subscribed'}]})
        with self.assertRaises(RuntimeError):
            self.client.ensure_members(['yes@example.com'], tag='attempt-one')
        self.assertIsNone(self.client.get_tag_segment_id('attempt-one'))


class TestFestivalAudienceRouting(unittest.TestCase):
    def setUp(self):
        from craft_engine import CraftCampaignEngine
        from unittest.mock import Mock
        self.engine = CraftCampaignEngine.__new__(CraftCampaignEngine)
        self.engine.db = Mock()
        self.engine.db.get_event.return_value = {'event_id':'E'}

    def test_single_legacy_audience_is_never_a_festival_fallback(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'MAILCHIMP_AUDIENCE_ID':'299007183e', 'MAILCHIMP_EVENT_AUDIENCES':'{}'}):
            with self.assertRaisesRegex(RuntimeError, 'no verified'):
                self.engine.mailchimp_for_event('Austin')

    def test_explicit_routes_keep_city_audiences_separate(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'MAILCHIMP_API_KEY':'test-us16', 'MAILCHIMP_EVENT_AUDIENCES':'{"Austin":"aaaaaaaaaa","Philly":"bbbbbbbbbb"}'}):
            self.assertEqual(self.engine.mailchimp_for_event('Austin').audience_id, 'aaaaaaaaaa')
            self.assertEqual(self.engine.mailchimp_for_event('Philly').audience_id, 'bbbbbbbbbb')

    def test_invalid_mapping_fails_closed(self):
        from unittest.mock import patch
        for raw in ('[]', '{', '{"E":"../other"}'):
            with self.subTest(raw=raw), patch.dict(os.environ, {'MAILCHIMP_EVENT_AUDIENCES':raw}):
                with self.assertRaises(RuntimeError):
                    self.engine.mailchimp_for_event('E')


class TestAudienceBoundWebhook(_Harness):
    def test_signed_unsubscribe_only_mutates_its_own_audience(self):
        from unittest.mock import patch
        from audience_suppression import AudienceSuppressionGuard
        with patch.dict(os.environ, {'MAILCHIMP_WEBHOOK_SIGNING_SECRETS':json.dumps({'aaaaaaaaaa':SIGNING_SECRET, 'bbbbbbbbbb':'different-key'})}):
            response = self.post_form({'type':'unsubscribe', 'data[list_id]':'aaaaaaaaaa', 'data[email]':'local@example.com'})
        self.assertEqual(response.status_code, 200)
        a = AudienceSuppressionGuard(self.db, 'aaaaaaaaaa')
        b = AudienceSuppressionGuard(self.db, 'bbbbbbbbbb')
        self.assertEqual(a._actual_suppression_count(), 1)
        self.assertEqual(b._actual_suppression_count(), 0)
        self.assertNotIn('local@example.com', self.suppressed())

    def test_another_audiences_key_cannot_authenticate_delivery(self):
        from unittest.mock import patch
        with patch.dict(os.environ, {'MAILCHIMP_WEBHOOK_SIGNING_SECRETS':json.dumps({'aaaaaaaaaa':SIGNING_SECRET, 'bbbbbbbbbb':'different-key'})}):
            response = self.post_form({'type':'unsubscribe', 'data[list_id]':'bbbbbbbbbb', 'data[email]':'local@example.com'})
        self.assertEqual(response.status_code, 401)
        self.assertNotIn('local@example.com', self.suppressed())
