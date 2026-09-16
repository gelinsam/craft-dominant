"""Phase 2 idempotent execution tests.

Verifies the claim-checkpoint-send pattern, crash recovery,
reconciliation, duplicate prevention, and dry-run semantics for
the V2 CRM execution pipeline.
"""

import os
import json
import sqlite3
import unittest
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from send_attempt_model import (
    SendAttempt,
    SendAttemptStatus,
    IllegalAttemptTransition,
    DuplicateClaimError,
    compute_audience_hash,
    compute_idempotency_key,
    TERMINAL_ATTEMPT_STATES,
    ACTIVE_ATTEMPT_STATES,
    SUCCESSFUL_SEND_STATES,
)
from execution_adapter import (
    ExecutionAdapter,
    EXECUTION_GENERATION,
    ATTRIBUTION_WINDOW_DAYS,
    AmbiguousSendOutcome,
)
from v2_state_repository import SQLiteV2StateRepository
from intervention_model import Intervention, InterventionStatus
from suppression_guard import SuppressionGuard, SENTINEL_TABLE_SCHEMA
from craft_engine import MailchimpClient
from provider_outcome import (
    ProviderResponse,
    ProviderTransportError,
    ProviderHTTPError,
    ProviderMalformedResponseError,
    ProviderSendStatus,
    ProviderCreateStatus,
    ReconciliationVerdict,
    classify_reconciliation_status,
    classify_send_http_error,
    DEFINITE_SEND_REJECTION_CODES,
    RECONCILE_NOT_SENT_STATUSES,
    RECONCILE_KNOWN_UNSAFE_STATUSES,
)


# ─────────────────────────────────────────────────────────────────────
# Fake provider infrastructure
# ─────────────────────────────────────────────────────────────────────

class FakeDeterministicMailchimp(MailchimpClient):
    """Mailchimp double that stubs the HTTP TRANSPORT, not the API methods.

    This subclasses the real ``MailchimpClient`` and overrides only
    ``_request_strict`` — the single place where bytes would go on the
    wire.  Everything above it (``send_campaign_strict``,
    ``create_campaign_strict``, ``get_campaign_status``, and all the
    outcome classification) is the real production code.

    That matters: an earlier version of this fake reimplemented the
    high-level methods, so the tests validated the test double's model
    of the provider rather than the shipped contract.  A lost HTTP
    response was never exercised against the real classifier at all.
    By faking only the transport we can inject genuine timeouts,
    connection resets, 5xx responses and 204 No Content bodies and
    watch the real code classify them.
    """

    def __init__(self):
        super().__init__(api_key="test-key-us16", audience_id="test-audience")
        self.calls = []
        self._campaign_counter = 0
        self._campaign_status = "sent"
        self._campaign_emails_sent = 50
        self._campaign_send_time = "2026-01-15T10:00:00+00:00"
        # transport injection: path-substring -> behaviour
        self._transport_overrides = []
        self._fail_at = None
        self._raise_at = None
        self._raise_exception = None

    # ── transport injection ──────────────────────────────────────────

    def inject(self, path_contains, behaviour, method=None):
        """Make matching requests behave a particular way.

        behaviour is one of:
          ('timeout',)                     -> requests.Timeout
          ('conn_reset',)                  -> requests.ConnectionError
          ('exception', exc)               -> raise exc verbatim
          ('http', status_code)            -> non-2xx HTTP response
          ('ok', status_code, body)        -> 2xx response
          ('malformed', status_code)       -> 2xx with unparseable body
        """
        self._transport_overrides.append((method, path_contains, behaviour))

    def clear_injections(self):
        self._transport_overrides = []

    def set_campaign_status(self, status, emails_sent=50,
                            send_time="2026-01-15T10:00:00+00:00"):
        """Status returned by GET /campaigns/{id} during reconciliation."""
        self._campaign_status = status
        self._campaign_emails_sent = emails_sent
        self._campaign_send_time = send_time

    def set_fail_at(self, step, exception=None):
        """Legacy helper: fail a named pipeline step.

        step: 'members' | 'segment' | 'campaign' | 'send'
        """
        self._fail_at = step
        if exception:
            self._raise_at = step
            self._raise_exception = exception
        else:
            self._raise_at = None
            self._raise_exception = None

    def reset(self):
        self.calls = []
        self._campaign_counter = 0
        self._campaign_status = "sent"
        self._campaign_emails_sent = 50
        self._campaign_send_time = "2026-01-15T10:00:00+00:00"
        self._transport_overrides = []
        self._fail_at = None
        self._raise_at = None
        self._raise_exception = None

    # ── the ONLY stubbed layer: HTTP transport ───────────────────────

    def _request_strict(self, method, path, data=None, timeout=30):
        self.calls.append(('_request_strict', {'method': method, 'path': path}))

        for m, needle, behaviour in self._transport_overrides:
            if m is not None and m != method:
                continue
            if needle not in path:
                continue
            return self._apply_behaviour(behaviour, path)

        return self._default_response(method, path)

    def _apply_behaviour(self, behaviour, path):
        kind = behaviour[0]
        if kind == 'timeout':
            raise ProviderTransportError(
                f"timeout on {path}", error_type="Timeout")
        if kind == 'conn_reset':
            raise ProviderTransportError(
                f"connection reset on {path}", error_type="ConnectionError")
        if kind == 'exception':
            raise behaviour[1]
        if kind == 'http':
            raise ProviderHTTPError(
                f"HTTP {behaviour[1]} on {path}",
                http_status=behaviour[1], detail="injected")
        if kind == 'malformed':
            raise ProviderMalformedResponseError(
                f"unparseable body on {path}", http_status=behaviour[1])
        if kind == 'ok':
            return ProviderResponse(http_status=behaviour[1],
                                    body=behaviour[2] or {})
        raise AssertionError(f"unknown injected behaviour {kind!r}")

    def _default_response(self, method, path):
        # Legacy step-based failure injection, mapped onto transport.
        if self._raise_at and self._step_matches(self._raise_at, method, path):
            raise self._raise_exception
        if self._fail_at and self._step_matches(self._fail_at, method, path):
            # A plain "failure" with no specific exception means the
            # provider answered and rejected it.
            raise ProviderHTTPError(
                f"simulated {self._fail_at} failure",
                http_status=422, detail="simulated")

        if method == 'POST' and path == '/campaigns':
            self._campaign_counter += 1
            return ProviderResponse(
                http_status=200,
                body={'id': f'mc-campaign-{self._campaign_counter}'})

        if method == 'PUT' and '/content' in path:
            return ProviderResponse(http_status=200, body={'html': 'ok'})

        if '/actions/send' in path:
            # Mailchimp's documented success shape: 204 No Content.
            return ProviderResponse(http_status=204, body={})

        if method == 'GET' and path.startswith('/campaigns/'):
            return ProviderResponse(http_status=200, body={
                'status': self._campaign_status,
                'emails_sent': self._campaign_emails_sent,
                'send_time': self._campaign_send_time,
            })

        if method == 'GET' and '/segments' in path:
            return ProviderResponse(http_status=200, body={
                'segments': [{'id': 12345, 'name': 'seg'}]})

        if method == 'POST' and path.startswith('/lists/'):
            return ProviderResponse(http_status=200, body={
                'total_created': 1, 'total_updated': 0, 'error_count': 0})

        return ProviderResponse(http_status=200, body={})

    @staticmethod
    def _step_matches(step, method, path):
        if step == 'members':
            return path.startswith('/lists/') and method == 'POST' \
                and '/segments' not in path
        if step == 'segment':
            return '/segments' in path
        if step == 'campaign':
            return path == '/campaigns' and method == 'POST'
        if step == 'send':
            return '/actions/send' in path
        return False

    # ── convenience used by existing tests ───────────────────────────

    def ensure_members(self, emails, tag=None):
        self.calls.append(('ensure_members', {'emails': emails, 'tag': tag}))
        if self._raise_at == 'members':
            raise self._raise_exception
        if self._fail_at == 'members':
            raise RuntimeError("ensure_members simulated failure")
        return {'added': len(emails), 'updated': 0, 'errors': 0}

    def get_tag_segment_id(self, tag):
        self.calls.append(('get_tag_segment_id', {'tag': tag}))
        if self._raise_at == 'segment':
            raise self._raise_exception
        if self._fail_at == 'segment':
            raise RuntimeError("get_tag_segment_id simulated failure")
        return 12345

    def sent_campaign_ids(self):
        """Campaign IDs for which a send was actually dispatched."""
        return [
            c[1]['path'].split('/')[2]
            for c in self.calls
            if c[0] == '_request_strict' and '/actions/send' in c[1]['path']
        ]

    def send_call_count(self):
        return len(self.sent_campaign_ids())


class FakeCampaignEngine:
    """Wraps FakeDeterministicMailchimp as .mailchimp attribute."""

    def __init__(self, mailchimp=None):
        self.mailchimp = mailchimp or FakeDeterministicMailchimp()


# ─────────────────────────────────────────────────────────────────────
# Fake execution DB (replicating pattern from test_intervention.py)
# ─────────────────────────────────────────────────────────────────────

class FakeExecutionDB:
    """In-memory SQLite with events, orders, customers, suppressions, campaigns."""

    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY, name TEXT, event_type TEXT,
                city TEXT, event_date TEXT, capacity INTEGER
            );
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY, event_id TEXT, email TEXT,
                order_timestamp TEXT NOT NULL DEFAULT '2026-01-01T00:00:00+00:00',
                ticket_count INTEGER, gross_amount REAL
            );
            CREATE TABLE customers (
                email TEXT PRIMARY KEY, favorite_city TEXT,
                event_types TEXT, rfm_segment TEXT
            );
            CREATE TABLE suppressions (email TEXT PRIMARY KEY, reason TEXT);
            CREATE TABLE campaigns (
                id TEXT PRIMARY KEY, intervention_id TEXT, event_id TEXT,
                subject_line TEXT, preview_text TEXT, body_html TEXT,
                audience_json TEXT, audience_count INTEGER,
                segment_description TEXT, status TEXT DEFAULT 'draft',
                sent_at TEXT, created_at TEXT
            );
        """)

    def get_event(self, event_id):
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_event_buyers(self, event_id):
        rows = self.conn.execute(
            "SELECT DISTINCT email FROM orders WHERE event_id = ?",
            (event_id,),
        ).fetchall()
        return [r["email"] for r in rows]

    def get_past_attendees_not_purchased(self, event_id, event_name,
                                         limit=50000, current_buyer_emails=None):
        buyer_set = set(current_buyer_emails or [])
        rows = self.conn.execute("SELECT * FROM customers").fetchall()
        return [dict(r) for r in rows if r["email"] not in buyer_set]

    def get_city_prospects(self, city, exclude_emails=None, limit=50000):
        exclude = set(exclude_emails or [])
        rows = self.conn.execute(
            "SELECT * FROM customers WHERE favorite_city = ?", (city,)
        ).fetchall()
        return [dict(r) for r in rows if r["email"] not in exclude]


# ─────────────────────────────────────────────────────────────────────
# Test helpers
# ─────────────────────────────────────────────────────────────────────

def _seed_valid_suppression_sentinel(db, row_count=1, source="test"):
    """Create a valid, fresh suppression sentinel."""
    db.conn.executescript(SENTINEL_TABLE_SCHEMA)
    now = datetime.now(timezone.utc).isoformat()
    db.conn.execute(
        """INSERT OR REPLACE INTO v2_suppression_sync
           (id, last_synced_at, row_count, source,
            last_full_refresh_at, last_full_refresh_source)
           VALUES (1, ?, ?, ?, ?, ?)""",
        (now, row_count, source, now, source),
    )
    db.conn.commit()


def _make_test_environment(fail_at=None, exception=None):
    """Build a fully wired test environment.

    Returns (db, v2_repo, adapter, mc, intervention_id).

    The environment has:
    - 1 event (evt1, Philadelphia)
    - 2 customer audience members
    - 1 suppressed email
    - 1 campaign draft (draft-001)
    - 1 approved intervention
    - A valid suppression sentinel
    """
    db = FakeExecutionDB()
    v2_repo = SQLiteV2StateRepository(db)

    # Seed event
    db.conn.execute(
        "INSERT INTO events VALUES (?,?,?,?,?,?)",
        ("evt1", "Test Fest", "coffee", "Philadelphia", "2026-06-15", 5000),
    )

    # Seed 2 audience members
    db.conn.execute(
        "INSERT INTO customers VALUES (?,?,?,?)",
        ("alice@example.com", "Philadelphia", "coffee", "regular"),
    )
    db.conn.execute(
        "INSERT INTO customers VALUES (?,?,?,?)",
        ("bob@example.com", "Philadelphia", "coffee", "regular"),
    )

    # Seed suppressed email
    db.conn.execute(
        "INSERT INTO suppressions (email, reason) VALUES (?, ?)",
        ("suppressed@example.com", "unsubscribed"),
    )

    # Seed campaign draft
    db.conn.execute(
        """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
           preview_text, body_html, audience_count, status, created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        ("draft-001", "intv-001", "evt1", "Come to Test Fest!",
         "Preview text here", "<h1>Hello</h1>", 2, "draft", "2026-01-01"),
    )
    db.conn.commit()

    # Suppression sentinel (1 row = healthy)
    _seed_valid_suppression_sentinel(db, row_count=1)

    # Create and save intervention in APPROVED state
    intervention = Intervention(
        id="intv-001",
        opportunity_id="opp-001",
        event_id="evt1",
        intervention_type="crm_campaign",
        status=InterventionStatus.NEW,
        expected_revenue=5000.0,
        confidence=0.6,
        campaign_draft_id="draft-001",
        measurement_window=7,
    )
    intervention.transition_to(InterventionStatus.INVESTIGATED)
    intervention.transition_to(InterventionStatus.PROPOSED)
    intervention.transition_to(InterventionStatus.APPROVED)
    v2_repo.save_intervention(intervention)

    # Build fake Mailchimp and campaign engine
    mc = FakeDeterministicMailchimp()
    if fail_at:
        mc.set_fail_at(fail_at, exception=exception)
    engine = FakeCampaignEngine(mc)
    adapter = ExecutionAdapter(db, v2_repo, campaign_engine=engine)

    return db, v2_repo, adapter, mc, "intv-001"


# ─────────────────────────────────────────────────────────────────────
# Test: SendAttempt model unit tests
# ─────────────────────────────────────────────────────────────────────

class TestSendAttemptModel(unittest.TestCase):
    """Unit tests for the SendAttempt model and state machine."""

    def test_legal_transitions(self):
        """All valid state machine transitions succeed."""
        valid_paths = [
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED),
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.FAILED_PRE_SEND),
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.CANCELLED),
            (SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED, SendAttemptStatus.AUDIENCE_CONFIGURED),
            (SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED, SendAttemptStatus.FAILED_PRE_SEND),
            (SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED, SendAttemptStatus.CANCELLED),
            (SendAttemptStatus.AUDIENCE_CONFIGURED, SendAttemptStatus.SEND_REQUESTED),
            (SendAttemptStatus.AUDIENCE_CONFIGURED, SendAttemptStatus.FAILED_PRE_SEND),
            (SendAttemptStatus.AUDIENCE_CONFIGURED, SendAttemptStatus.CANCELLED),
            (SendAttemptStatus.SEND_REQUESTED, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.SEND_REQUESTED, SendAttemptStatus.AMBIGUOUS),
            (SendAttemptStatus.SEND_REQUESTED, SendAttemptStatus.CANCELLED),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.RECONCILED_SENT),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.RECONCILED_NOT_SENT),
        ]
        for from_status, to_status in valid_paths:
            attempt = SendAttempt(
                id=1, intervention_id="test", execution_generation=1,
                attempt_status=from_status, idempotency_key="key",
                audience_hash="hash",
            )
            attempt.transition_to(to_status)
            self.assertEqual(attempt.attempt_status, to_status,
                             f"Transition {from_status.value} -> {to_status.value} failed")

    def test_illegal_transitions(self):
        """Invalid state machine transitions raise IllegalAttemptTransition."""
        # Note: AMBIGUOUS is now reachable from every pre-send state,
        # because any provider call can lose its response. And
        # SEND_REQUESTED -> FAILED_PRE_SEND is permitted, reachable only
        # via a documented rejection code. Both are covered by their own
        # tests below; neither can be entered from uncertainty.
        illegal_paths = [
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.SEND_REQUESTED),
            (SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.CONFIRMED_SENT, SendAttemptStatus.AMBIGUOUS),
            # The critical one: ambiguity may never become "sent" or
            # retryable without going through reconciliation.
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.FAILED_PRE_SEND),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.CANCELLED),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.SEND_REQUESTED),
        ]
        for from_status, to_status in illegal_paths:
            attempt = SendAttempt(
                id=1, intervention_id="test", execution_generation=1,
                attempt_status=from_status, idempotency_key="key",
                audience_hash="hash",
            )
            with self.assertRaises(IllegalAttemptTransition,
                                   msg=f"Expected {from_status.value} -> {to_status.value} to fail"):
                attempt.transition_to(to_status)

    def test_terminal_states(self):
        """Terminal states have no outgoing transitions."""
        for status in TERMINAL_ATTEMPT_STATES:
            attempt = SendAttempt(
                id=1, intervention_id="test", execution_generation=1,
                attempt_status=status, idempotency_key="key",
                audience_hash="hash",
            )
            self.assertTrue(attempt.is_terminal)
            self.assertFalse(attempt.is_active)
            # Every other status should be an illegal transition
            for target in SendAttemptStatus:
                if target == status:
                    continue
                with self.assertRaises(IllegalAttemptTransition):
                    attempt_copy = SendAttempt(
                        id=1, intervention_id="test", execution_generation=1,
                        attempt_status=status, idempotency_key="key",
                        audience_hash="hash",
                    )
                    attempt_copy.transition_to(target)

    def test_is_terminal_active_successful(self):
        """Property checks for is_terminal, is_active, is_successful."""
        cases = {
            SendAttemptStatus.CLAIMED: (False, True, False),
            SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED: (False, True, False),
            SendAttemptStatus.AUDIENCE_CONFIGURED: (False, True, False),
            SendAttemptStatus.SEND_REQUESTED: (False, True, False),
            SendAttemptStatus.AMBIGUOUS: (False, True, False),
            SendAttemptStatus.CONFIRMED_SENT: (True, False, True),
            SendAttemptStatus.FAILED_PRE_SEND: (True, False, False),
            SendAttemptStatus.RECONCILED_SENT: (True, False, True),
            SendAttemptStatus.RECONCILED_NOT_SENT: (True, False, False),
            SendAttemptStatus.CANCELLED: (True, False, False),
        }
        for status, (is_terminal, is_active, is_successful) in cases.items():
            attempt = SendAttempt(
                id=1, intervention_id="test", execution_generation=1,
                attempt_status=status, idempotency_key="key",
                audience_hash="hash",
            )
            self.assertEqual(attempt.is_terminal, is_terminal,
                             f"{status.value}: is_terminal should be {is_terminal}")
            self.assertEqual(attempt.is_active, is_active,
                             f"{status.value}: is_active should be {is_active}")
            self.assertEqual(attempt.is_successful, is_successful,
                             f"{status.value}: is_successful should be {is_successful}")

    def test_audience_hash_deterministic(self):
        """Same emails in different order produce the same hash."""
        emails_a = ["bob@example.com", "alice@example.com"]
        emails_b = ["alice@example.com", "bob@example.com"]
        self.assertEqual(
            compute_audience_hash(emails_a),
            compute_audience_hash(emails_b),
        )

    def test_audience_hash_normalization(self):
        """Whitespace and case are normalized before hashing."""
        emails_a = ["  Alice@Example.COM  ", "BOB@example.com"]
        emails_b = ["alice@example.com", "bob@example.com"]
        self.assertEqual(
            compute_audience_hash(emails_a),
            compute_audience_hash(emails_b),
        )

    def test_idempotency_key_deterministic(self):
        """Same inputs produce the same idempotency key."""
        key1 = compute_idempotency_key("intv-1", 1, "draft-1", "hash-abc")
        key2 = compute_idempotency_key("intv-1", 1, "draft-1", "hash-abc")
        self.assertEqual(key1, key2)

    def test_idempotency_key_changes_with_inputs(self):
        """Different inputs produce different keys."""
        base = compute_idempotency_key("intv-1", 1, "draft-1", "hash-abc")
        different_intv = compute_idempotency_key("intv-2", 1, "draft-1", "hash-abc")
        different_gen = compute_idempotency_key("intv-1", 2, "draft-1", "hash-abc")
        different_draft = compute_idempotency_key("intv-1", 1, "draft-2", "hash-abc")
        different_hash = compute_idempotency_key("intv-1", 1, "draft-1", "hash-xyz")
        keys = {base, different_intv, different_gen, different_draft, different_hash}
        self.assertEqual(len(keys), 5, "All keys should be unique")

    def test_to_dict_no_secrets(self):
        """to_dict never contains provider secrets (tag, segment)."""
        attempt = SendAttempt(
            id=1, intervention_id="test", execution_generation=1,
            attempt_status=SendAttemptStatus.CONFIRMED_SENT,
            idempotency_key="key", audience_hash="hash",
            provider_tag="v2-intv-001",
            provider_segment_id=99999,
        )
        d = attempt.to_dict()
        self.assertNotIn('provider_tag', d)
        self.assertNotIn('provider_segment_id', d)
        # Should still have the non-secret fields
        self.assertIn('intervention_id', d)
        self.assertIn('audience_hash', d)
        self.assertIn('provider_campaign_id', d)


# ─────────────────────────────────────────────────────────────────────
# Test: Idempotent execution integration tests
# ─────────────────────────────────────────────────────────────────────

class TestIdempotentExecution(unittest.TestCase):
    """Integration tests: full execution path with SQLite in-memory DB."""

    def setUp(self):
        self._env_patch = patch.dict(os.environ, {}, clear=False)
        self._env_patch.start()
        # Ensure external send is disabled by default
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def tearDown(self):
        self._env_patch.stop()

    def test_dry_run_does_not_create_claim(self):
        """Dry run (V2_ENABLE_EXTERNAL_SEND=0) does NOT create a v2_send_attempts row."""
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "0"
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        result = adapter.execute(intv_id)
        self.assertEqual(result["status"], "external_send_disabled")

        # No send attempts should exist
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts), 0, "Dry run must not create send attempt rows")

        # Mailchimp should not have been called
        self.assertEqual(len(mc.calls), 0, "Dry run must not call Mailchimp")

    def test_dry_run_does_not_block_real_execution(self):
        """After a dry run, real execution succeeds."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        # Step 1: dry run
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "0"
        dry_result = adapter.execute(intv_id)
        self.assertEqual(dry_result["status"], "external_send_disabled")

        # Step 2: real execution
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        real_result = adapter.execute(intv_id)
        self.assertEqual(real_result["status"], "executed")

    def test_successful_execution_creates_claim_and_checkpoints(self):
        """Full success path creates a claim that reaches confirmed_sent."""
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        result = adapter.execute(intv_id)
        self.assertEqual(result["status"], "executed")
        self.assertIn("send_attempt", result)

        # Verify the send attempt reached confirmed_sent
        attempt_dict = result["send_attempt"]
        self.assertEqual(attempt_dict["attempt_status"], "confirmed_sent")
        self.assertTrue(attempt_dict["is_terminal"])
        self.assertTrue(attempt_dict["is_successful"])
        self.assertFalse(attempt_dict["requires_reconciliation"])

        # Verify claim persisted in DB
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts), 1)
        db_attempt = attempts[0]
        self.assertEqual(db_attempt.attempt_status, SendAttemptStatus.CONFIRMED_SENT)
        self.assertIsNotNone(db_attempt.provider_campaign_id)

        # Verify Mailchimp was called in order
        call_names = [c[0] for c in mc.calls]
        self.assertEqual(call_names[:2], ['ensure_members', 'get_tag_segment_id'])

        # Provider operations are asserted at the transport layer, which
        # is where the real client actually issues them.
        paths = [c[1]['path'] for c in mc.calls if c[0] == '_request_strict']
        self.assertIn('/campaigns', paths)                 # create shell
        self.assertTrue(any('/content' in p for p in paths))   # set content
        self.assertEqual(mc.send_call_count(), 1)              # exactly one send

    def test_successful_execution_promotes_recipients(self):
        """After success, v2_campaign_sends is populated via promote."""
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        result = adapter.execute(intv_id)
        self.assertEqual(result["status"], "executed")

        # Check v2_campaign_sends has the recipients
        sends = v2_repo.get_sends(intv_id)
        sent_emails = {s["email"] for s in sends}
        self.assertIn("alice@example.com", sent_emails)
        self.assertIn("bob@example.com", sent_emails)
        self.assertNotIn("suppressed@example.com", sent_emails)

    def test_duplicate_execute_returns_already_sent(self):
        """Second execute call returns 'already_sent' error."""
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        # First execution succeeds
        result1 = adapter.execute(intv_id)
        self.assertEqual(result1["status"], "executed")

        # Second execution must report the real reason — already sent —
        # rather than the incidental 'illegal_status' that Gate 1 used
        # to produce once the intervention had moved to 'measuring'.
        # The proven-send check now runs before every other gate.
        result2 = adapter.execute(intv_id)
        self.assertIn("error", result2)
        self.assertEqual(result2["error"], "already_sent")
        # Fully finalized, so no repair was needed.
        self.assertNotIn("recovered", result2)
        # And no second send was dispatched.
        self.assertEqual(mc.send_call_count(), 1)

    def test_audience_hash_in_response(self):
        """Dry-run and execute responses include audience_hash."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        # Dry-run response
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "0"
        dry_result = adapter.execute(intv_id)
        self.assertIn("audience_hash", dry_result["dry_run"])
        self.assertTrue(len(dry_result["dry_run"]["audience_hash"]) > 0)

        # Real execution response
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        real_result = adapter.execute(intv_id)
        self.assertEqual(real_result["status"], "executed")
        self.assertIn("audience_hash", real_result["send_attempt"])


# ─────────────────────────────────────────────────────────────────────
# Test: Crash simulation
# ─────────────────────────────────────────────────────────────────────

class TestCrashSimulation(unittest.TestCase):
    """Crash/failure at each provider step produces correct state."""

    def setUp(self):
        self._env_patch = patch.dict(os.environ, {"V2_ENABLE_EXTERNAL_SEND": "1"})
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()

    def test_crash_after_campaign_creation(self):
        """Exception after create_campaign: attempt in failed_pre_send, NOT ambiguous.

        When ensure_members fails, the campaign was never created,
        so the attempt should be failed_pre_send (safe to retry).
        """
        db, v2_repo, adapter, mc, intv_id = _make_test_environment(
            fail_at='members',
            exception=RuntimeError("Mailchimp connection lost"),
        )

        result = adapter.execute(intv_id)
        self.assertIn("error", result)

        # The attempt should be in failed_pre_send (not ambiguous)
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts), 1)
        attempt = attempts[0]
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.FAILED_PRE_SEND)
        self.assertNotEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)

    def test_crash_during_send(self):
        """send_campaign raises ConnectionError: attempt is AMBIGUOUS."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment(
            fail_at='send',
            exception=ConnectionError("Connection reset by peer"),
        )

        result = adapter.execute(intv_id)
        self.assertIn("error", result)

        # The attempt should be ambiguous because we were in the danger zone
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts), 1)
        attempt = attempts[0]
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)
        self.assertTrue(attempt.requires_reconciliation)
        # Campaign should have been created before send failed
        self.assertIsNotNone(attempt.provider_campaign_id)

    def test_ambiguous_blocks_retry(self):
        """After ambiguous, another execute returns 'reconciliation_required'."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment(
            fail_at='send',
            exception=ConnectionError("timeout"),
        )

        # First: create ambiguous state
        result1 = adapter.execute(intv_id)
        self.assertIn("error", result1)
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status, SendAttemptStatus.AMBIGUOUS)

        # Second: try again -> must be blocked
        mc.reset()  # Reset Mailchimp so it would succeed, but should never be called
        result2 = adapter.execute(intv_id)
        self.assertEqual(result2["error"], "reconciliation_required")
        # Mailchimp should not have been called for the retry
        self.assertEqual(len(mc.calls), 0)

    def test_failed_pre_send_allows_retry(self):
        """After failed_pre_send, a new execute can create a new claim."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment(
            fail_at='members',
            exception=RuntimeError("temporary failure"),
        )

        # First: fail at members -> failed_pre_send
        result1 = adapter.execute(intv_id)
        self.assertIn("error", result1)
        attempts_before = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts_before), 1)
        self.assertEqual(
            attempts_before[0].attempt_status,
            SendAttemptStatus.FAILED_PRE_SEND,
        )

        # Now fix the Mailchimp and retry
        mc.reset()
        result2 = adapter.execute(intv_id)
        self.assertEqual(result2["status"], "executed")

        # Should now have 2 attempts: one failed, one successful
        attempts_after = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts_after), 2)
        statuses = {a.attempt_status for a in attempts_after}
        self.assertIn(SendAttemptStatus.FAILED_PRE_SEND, statuses)
        self.assertIn(SendAttemptStatus.CONFIRMED_SENT, statuses)


# ─────────────────────────────────────────────────────────────────────
# Test: Reconciliation
# ─────────────────────────────────────────────────────────────────────

class TestReconciliation(unittest.TestCase):
    """Reconciliation resolves ambiguous send attempts."""

    def setUp(self):
        self._env_patch = patch.dict(os.environ, {"V2_ENABLE_EXTERNAL_SEND": "1"})
        self._env_patch.start()

    def tearDown(self):
        self._env_patch.stop()

    def _make_ambiguous(self):
        """Create an environment with an ambiguous send attempt."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment(
            fail_at='send',
            exception=ConnectionError("timeout during send"),
        )
        result = adapter.execute(intv_id)
        self.assertIn("error", result)
        # Verify ambiguous
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status, SendAttemptStatus.AMBIGUOUS)
        # Reset Mailchimp for reconciliation queries
        mc.reset()
        return db, v2_repo, adapter, mc, intv_id

    def test_reconcile_sent(self):
        """Ambiguous + Mailchimp says 'sent' -> reconciled_sent, intervention measuring."""
        db, v2_repo, adapter, mc, intv_id = self._make_ambiguous()
        mc.set_campaign_status("sent")

        result = adapter.reconcile_send_attempt(intv_id)
        self.assertEqual(result["status"], "reconciled_sent")
        self.assertIn("send_attempt", result)
        self.assertEqual(result["send_attempt"]["attempt_status"], "reconciled_sent")

        # Intervention should have transitioned to measuring
        intervention = v2_repo.get_intervention(intv_id)
        self.assertEqual(intervention.status, InterventionStatus.MEASURING)

    def test_reconcile_not_sent(self):
        """Ambiguous + Mailchimp proves draft -> reconciled_not_sent, safe to retry.

        A genuine draft reports status 'save' AND zero delivered mail AND
        no send_time. All three must line up; 'save' alone alongside
        evidence of delivery is contradictory and stays ambiguous
        (see test_reconcile_save_with_delivery_evidence_stays_ambiguous).
        """
        db, v2_repo, adapter, mc, intv_id = self._make_ambiguous()
        mc.set_campaign_status("save", emails_sent=0, send_time=None)

        result = adapter.reconcile_send_attempt(intv_id)
        self.assertEqual(result["status"], "reconciled_not_sent")
        self.assertIn("Safe to retry", result["message"])

        # Intervention should still be in approved state
        intervention = v2_repo.get_intervention(intv_id)
        self.assertEqual(intervention.status, InterventionStatus.APPROVED)

    def test_reconcile_no_ambiguous(self):
        """No ambiguous attempt -> error."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        result = adapter.reconcile_send_attempt(intv_id)
        self.assertEqual(result["error"], "no_ambiguous_attempt")

    def test_reconcile_promotes_recipients_on_sent(self):
        """reconciled_sent promotes recipients to v2_campaign_sends."""
        db, v2_repo, adapter, mc, intv_id = self._make_ambiguous()
        mc.set_campaign_status("sent")

        # Before reconciliation: no sends
        sends_before = v2_repo.get_sends(intv_id)
        self.assertEqual(len(sends_before), 0)

        result = adapter.reconcile_send_attempt(intv_id)
        self.assertEqual(result["status"], "reconciled_sent")

        # After reconciliation: recipients promoted
        sends_after = v2_repo.get_sends(intv_id)
        sent_emails = {s["email"] for s in sends_after}
        self.assertIn("alice@example.com", sent_emails)
        self.assertIn("bob@example.com", sent_emails)


# ─────────────────────────────────────────────────────────────────────
# Test: Duplicate claim prevention
# ─────────────────────────────────────────────────────────────────────

class TestDuplicateClaimPrevention(unittest.TestCase):
    """Concurrent claim prevention at the repository level."""

    def test_two_concurrent_claims_one_wins(self):
        """Simulate two claims for same intervention+generation; second must fail."""
        db = FakeExecutionDB()
        v2_repo = SQLiteV2StateRepository(db)

        audience_hash = compute_audience_hash(["a@test.com", "b@test.com"])
        idem_key = compute_idempotency_key("intv-001", 1, "draft-001", audience_hash)

        # First claim succeeds
        attempt1 = SendAttempt(
            id=None,
            intervention_id="intv-001",
            execution_generation=1,
            attempt_status=SendAttemptStatus.CLAIMED,
            idempotency_key=idem_key,
            audience_hash=audience_hash,
            audience_count=2,
        )
        created1 = v2_repo.create_send_attempt(attempt1)
        self.assertIsNotNone(created1.id)

        # Second claim for same intervention+generation must fail
        attempt2 = SendAttempt(
            id=None,
            intervention_id="intv-001",
            execution_generation=1,
            attempt_status=SendAttemptStatus.CLAIMED,
            idempotency_key=idem_key,
            audience_hash=audience_hash,
            audience_count=2,
        )
        with self.assertRaises(DuplicateClaimError):
            v2_repo.create_send_attempt(attempt2)


# ─────────────────────────────────────────────────────────────────────
# Provider uncertainty — the blocker this module exists to prevent
# ─────────────────────────────────────────────────────────────────────

class TestLostSendResponse(unittest.TestCase):
    """MANDATORY regression: a lost send response must never unlock retry.

    The failure this guards against: Mailchimp accepts and sends the
    campaign, the HTTP response is lost, the client reports failure, the
    adapter releases the claim, a later execute retries, and every
    recipient gets a second email.
    """

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def test_provider_accepted_but_response_lost_blocks_retry(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()

        # Provider ACCEPTS and sends; we simply never hear back.
        mc.inject('/actions/send', ('timeout',))

        result = adapter.execute(intv_id)

        # 1. Reported as ambiguous, not as a retryable failure.
        self.assertEqual(result["error"], "execution_outcome_ambiguous")
        self.assertTrue(result["reconciliation_required"])

        # 2. The send WAS dispatched exactly once.
        self.assertEqual(mc.send_call_count(), 1)

        # 3. Attempt is ambiguous and the campaign ID was retained, so
        #    the send can be reconciled later.
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].attempt_status, SendAttemptStatus.AMBIGUOUS)
        self.assertIsNotNone(attempts[0].provider_campaign_id)
        self.assertTrue(attempts[0].requires_reconciliation)

        # 4. The claim is NOT released — it is still active.
        self.assertTrue(attempts[0].is_active)
        self.assertFalse(attempts[0].is_terminal)

        # ── Second execute attempt ────────────────────────────────────
        sends_before = mc.send_call_count()
        second = adapter.execute(intv_id)

        # 5. The second execute must NOT contact the provider to send.
        self.assertEqual(mc.send_call_count(), sends_before,
                         "second execute dispatched another send")

        # 6. It must report that reconciliation is required.
        self.assertIn("error", second)
        self.assertIn(second["error"],
                      ("reconciliation_required", "attempt_in_progress",
                       "execution_outcome_ambiguous"))

        # 7. Still exactly one attempt — no new claim was created.
        self.assertEqual(len(v2_repo.get_send_attempts(intv_id)), 1)

    def test_request_returning_none_equivalent_is_ambiguous(self):
        """The historical None path must classify as AMBIGUOUS.

        The legacy client collapsed a 5xx into None, send_campaign
        turned None into False, and the adapter read False as
        "definitely not sent". A 500 proves nothing.
        """
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('http', 500))

        result = adapter.execute(intv_id)

        self.assertEqual(result["error"], "execution_outcome_ambiguous")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status, SendAttemptStatus.AMBIGUOUS)
        self.assertNotEqual(attempts[0].attempt_status,
                            SendAttemptStatus.FAILED_PRE_SEND)


class TestSendHttpFailureMatrix(unittest.TestCase):
    """Per-status classification of the send action."""

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def _run_with(self, behaviour):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', behaviour)
        result = adapter.execute(intv_id)
        attempts = v2_repo.get_send_attempts(intv_id)
        return result, attempts[0], mc

    def test_timeout_is_ambiguous(self):
        _, attempt, _ = self._run_with(('timeout',))
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)

    def test_connection_reset_is_ambiguous(self):
        _, attempt, _ = self._run_with(('conn_reset',))
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)

    def test_http_500_is_ambiguous(self):
        _, attempt, _ = self._run_with(('http', 500))
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)

    def test_http_502_503_504_are_ambiguous(self):
        for code in (502, 503, 504):
            with self.subTest(code=code):
                _, attempt, _ = self._run_with(('http', code))
                self.assertEqual(attempt.attempt_status,
                                 SendAttemptStatus.AMBIGUOUS)

    def test_http_429_is_ambiguous(self):
        """Rate limiting is not proof of rejection for our purposes."""
        _, attempt, _ = self._run_with(('http', 429))
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)

    def test_malformed_success_body_is_ambiguous(self):
        """2xx we could not parse leans accepted, so it is never a failure."""
        _, attempt, _ = self._run_with(('malformed', 200))
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)

    def test_documented_rejection_codes_are_definite_failure(self):
        for code in sorted(DEFINITE_SEND_REJECTION_CODES):
            with self.subTest(code=code):
                _, attempt, _ = self._run_with(('http', code))
                self.assertEqual(
                    attempt.attempt_status, SendAttemptStatus.FAILED_PRE_SEND,
                    f"HTTP {code} should prove rejection")

    def test_204_no_content_is_confirmed_sent(self):
        """Mailchimp's documented success shape for the send action."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('ok', 204, {}))

        result = adapter.execute(intv_id)

        self.assertEqual(result["status"], "executed")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.CONFIRMED_SENT)

    def test_definite_failure_releases_claim_and_allows_retry(self):
        """Only a proven rejection may unlock a retry."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('http', 422))

        first = adapter.execute(intv_id)
        self.assertEqual(first["error"], "execution_failed")

        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.FAILED_PRE_SEND)
        self.assertTrue(attempts[0].is_terminal)

        # Claim released: a retry may now proceed and create a new claim.
        mc.clear_injections()
        second = adapter.execute(intv_id)
        self.assertEqual(second["status"], "executed")
        self.assertEqual(len(v2_repo.get_send_attempts(intv_id)), 2)


class TestSendOutcomeClassifier(unittest.TestCase):
    """Unit-level coverage of the send HTTP classifier."""

    def test_rejection_codes_classify_as_definite_failure(self):
        for code in sorted(DEFINITE_SEND_REJECTION_CODES):
            with self.subTest(code=code):
                outcome = classify_send_http_error(code)
                self.assertEqual(outcome.status,
                                 ProviderSendStatus.DEFINITE_FAILURE)

    def test_server_errors_classify_as_ambiguous(self):
        for code in (500, 502, 503, 504, 408, 409, 429, 418, 599):
            with self.subTest(code=code):
                outcome = classify_send_http_error(code)
                self.assertEqual(outcome.status,
                                 ProviderSendStatus.AMBIGUOUS)

    def test_outcome_dict_carries_no_secrets(self):
        outcome = classify_send_http_error(500, provider_campaign_id="mc-1")
        d = outcome.to_dict()
        blob = json.dumps(d).lower()
        for forbidden in ("api_key", "apikey", "authorization", "bearer",
                          "password", "@"):
            self.assertNotIn(forbidden, blob)


class TestReconciliationAllowlist(unittest.TestCase):
    """Status classification must be allowlist-based, never `!= sent`."""

    def test_sent_is_sent(self):
        self.assertEqual(
            classify_reconciliation_status("sent"),
            ReconciliationVerdict.SENT)

    def test_save_with_no_delivery_is_definitely_not_sent(self):
        self.assertEqual(
            classify_reconciliation_status("save", emails_sent=0,
                                           send_time=None),
            ReconciliationVerdict.DEFINITELY_NOT_SENT)

    def test_known_unsafe_statuses_are_unknown(self):
        """sending / canceling / canceled / paused / schedule / archived.

        'canceled' and 'canceling' matter most: Mailchimp's cancel
        endpoint is documented as cancelling a campaign AFTER you send,
        before all recipients receive it. Reaching those states means
        delivery already began.
        """
        for status in sorted(RECONCILE_KNOWN_UNSAFE_STATUSES):
            with self.subTest(status=status):
                self.assertEqual(
                    classify_reconciliation_status(status),
                    ReconciliationVerdict.UNKNOWN,
                    f"{status!r} must not unlock a retry")

    def test_unrecognised_and_empty_statuses_are_unknown(self):
        for status in (None, "", "   ", "unknown", "wat", "SENT_MAYBE",
                       "delivered", "queued"):
            with self.subTest(status=status):
                self.assertEqual(
                    classify_reconciliation_status(status),
                    ReconciliationVerdict.UNKNOWN)

    def test_status_matching_is_case_and_whitespace_insensitive(self):
        self.assertEqual(classify_reconciliation_status("  SENT "),
                         ReconciliationVerdict.SENT)

    def test_save_contradicted_by_delivery_is_unknown(self):
        """Draft status plus evidence of delivery is contradictory."""
        self.assertEqual(
            classify_reconciliation_status("save", emails_sent=50),
            ReconciliationVerdict.UNKNOWN)
        self.assertEqual(
            classify_reconciliation_status(
                "save", emails_sent=0, send_time="2026-01-15T10:00:00Z"),
            ReconciliationVerdict.UNKNOWN)

    def test_only_save_is_in_the_not_sent_allowlist(self):
        self.assertEqual(RECONCILE_NOT_SENT_STATUSES, frozenset({"save"}))


class TestReconciliationTransitionalStatuses(unittest.TestCase):
    """End-to-end: transitional provider states keep the attempt blocked."""

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def _ambiguous_env(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('timeout',))
        adapter.execute(intv_id)
        mc.clear_injections()
        return db, v2_repo, adapter, mc, intv_id

    def test_sending_status_stays_ambiguous(self):
        """Delivery is in flight right now — retry would duplicate."""
        db, v2_repo, adapter, mc, intv_id = self._ambiguous_env()
        mc.set_campaign_status("sending", emails_sent=12, send_time=None)

        result = adapter.reconcile_send_attempt(intv_id)

        self.assertEqual(result["status"], "provider_state_still_ambiguous")
        self.assertTrue(result["reconciliation_required"])
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.AMBIGUOUS)

    def test_canceled_status_stays_ambiguous(self):
        """Cancel happens AFTER send begins — partial delivery occurred."""
        db, v2_repo, adapter, mc, intv_id = self._ambiguous_env()
        mc.set_campaign_status("canceled", emails_sent=30)

        result = adapter.reconcile_send_attempt(intv_id)

        self.assertEqual(result["status"], "provider_state_still_ambiguous")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.AMBIGUOUS)

    def test_all_transitional_statuses_stay_ambiguous(self):
        for status in sorted(RECONCILE_KNOWN_UNSAFE_STATUSES):
            with self.subTest(status=status):
                db, v2_repo, adapter, mc, intv_id = self._ambiguous_env()
                mc.set_campaign_status(status, emails_sent=0, send_time=None)

                result = adapter.reconcile_send_attempt(intv_id)

                self.assertEqual(
                    result["status"], "provider_state_still_ambiguous",
                    f"{status!r} must not resolve the attempt")
                attempts = v2_repo.get_send_attempts(intv_id)
                self.assertEqual(attempts[0].attempt_status,
                                 SendAttemptStatus.AMBIGUOUS)

    def test_missing_status_field_stays_ambiguous(self):
        """A campaign object with no status must not read as not-sent."""
        db, v2_repo, adapter, mc, intv_id = self._ambiguous_env()
        mc.inject('/campaigns/', ('ok', 200, {'emails_sent': 0}), method='GET')

        result = adapter.reconcile_send_attempt(intv_id)

        self.assertEqual(result["status"], "provider_state_still_ambiguous")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.AMBIGUOUS)

    def test_unreachable_provider_stays_ambiguous(self):
        """Failing to ask is not an answer."""
        db, v2_repo, adapter, mc, intv_id = self._ambiguous_env()
        mc.inject('/campaigns/', ('timeout',), method='GET')

        result = adapter.reconcile_send_attempt(intv_id)

        self.assertEqual(result["error"], "provider_query_failed")
        self.assertTrue(result["reconciliation_required"])
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.AMBIGUOUS)

    def test_inconclusive_reconciliation_does_not_permit_resend(self):
        """After an inconclusive reconcile, execute still must not send."""
        db, v2_repo, adapter, mc, intv_id = self._ambiguous_env()
        mc.set_campaign_status("sending")
        adapter.reconcile_send_attempt(intv_id)

        sends_before = mc.send_call_count()
        result = adapter.execute(intv_id)

        self.assertEqual(mc.send_call_count(), sends_before)
        self.assertIn("error", result)


class TestCampaignCreationUncertainty(unittest.TestCase):
    """Creation whose outcome is unknown must not silently retry."""

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def test_lost_creation_response_is_ambiguous_not_retryable(self):
        """A campaign may exist whose ID we never learned."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/campaigns', ('timeout',), method='POST')

        result = adapter.execute(intv_id)

        self.assertEqual(result["error"], "execution_outcome_ambiguous")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.AMBIGUOUS)
        # No send was ever dispatched.
        self.assertEqual(mc.send_call_count(), 0)

    def test_creation_rejected_is_failed_pre_send(self):
        """A rejected creation proves nothing was sent."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/campaigns', ('http', 400), method='POST')

        result = adapter.execute(intv_id)

        self.assertEqual(result["error"], "execution_failed")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.FAILED_PRE_SEND)
        self.assertEqual(mc.send_call_count(), 0)

    def test_content_failure_retains_campaign_id_for_cleanup(self):
        """Orphan shell: created but contentless. Never sent, ID kept."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/content', ('http', 500), method='PUT')

        result = adapter.execute(intv_id)

        self.assertEqual(result["error"], "execution_failed")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.FAILED_PRE_SEND)
        self.assertIsNotNone(attempts[0].provider_campaign_id)
        self.assertEqual(mc.send_call_count(), 0)

    def test_success_without_campaign_id_is_ambiguous(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/campaigns', ('ok', 200, {}), method='POST')

        result = adapter.execute(intv_id)

        self.assertEqual(result["error"], "execution_outcome_ambiguous")
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.AMBIGUOUS)


class TestRetryInvariant(unittest.TestCase):
    """Structural proof that uncertainty cannot reach a retryable state."""

    def test_no_bare_except_converts_uncertainty_to_retryable(self):
        """After send dispatch, no handler may produce failed_pre_send."""
        import inspect
        import execution_adapter as ea

        src = inspect.getsource(ea.ExecutionAdapter._send_via_mailchimp_checkpointed)
        marker = "DANGER ZONE"
        self.assertIn(marker, src)
        danger = src[src.index(marker):]

        # Catching broadly past the send boundary is fine — what matters
        # is where it lands. Between the send call and the proven
        # definite-failure branch, nothing may transition the attempt to
        # a retryable state.
        head = danger.split("is_definite_failure")[0]
        self.assertNotIn("FAILED_PRE_SEND", head,
                         "a pre-send (retryable) transition appears after "
                         "the send was dispatched")

        # And any broad handler there must resolve to AMBIGUOUS.
        if "except Exception" in head:
            tail = head[head.index("except Exception"):]
            self.assertIn("AMBIGUOUS", tail)

    def test_ambiguous_cannot_transition_to_retryable_states(self):
        """The state machine forbids ambiguous -> retryable directly."""
        for target in (SendAttemptStatus.FAILED_PRE_SEND,
                       SendAttemptStatus.CANCELLED):
            with self.subTest(target=target):
                attempt = SendAttempt(
                    id=1, intervention_id="i", execution_generation=1,
                    attempt_status=SendAttemptStatus.AMBIGUOUS,
                    idempotency_key="k", audience_hash="h",
                )
                with self.assertRaises(IllegalAttemptTransition):
                    attempt.transition_to(target)

    def test_ambiguous_only_resolves_via_reconciliation(self):
        attempt = SendAttempt(
            id=1, intervention_id="i", execution_generation=1,
            attempt_status=SendAttemptStatus.AMBIGUOUS,
            idempotency_key="k", audience_hash="h",
        )
        attempt.transition_to(SendAttemptStatus.RECONCILED_NOT_SENT)
        self.assertEqual(attempt.attempt_status,
                         SendAttemptStatus.RECONCILED_NOT_SENT)

    def test_legacy_bool_send_is_not_used_in_execution_path(self):
        """The execution path must use the strict, three-valued send."""
        import inspect
        import execution_adapter as ea

        src = inspect.getsource(ea.ExecutionAdapter)
        self.assertIn("send_campaign_strict", src)
        # The lossy boolean form must not appear as a call.
        self.assertNotIn("mc.send_campaign(", src)
        self.assertNotIn("mc.create_campaign(", src)


# ─────────────────────────────────────────────────────────────────────
# Crash-after-confirmed-send recovery
# ─────────────────────────────────────────────────────────────────────

def _crash_after_confirmed_send(db, v2_repo, adapter, mc, intv_id):
    """Drive a real send, then rewind local state to the crash shape.

    Simulates dying between "attempt persisted confirmed_sent" and the
    local bookkeeping: intervention still approved, recipients still
    only staged, no attribution rows, no finalization audit.

    The send attempt row is left exactly as the provider path wrote it,
    because that is what really survives a crash.
    """
    result = adapter.execute(intv_id)
    assert result["status"] == "executed", result

    attempt = v2_repo.get_send_attempts(intv_id)[0]
    assert attempt.attempt_status == SendAttemptStatus.CONFIRMED_SENT

    # Rewind the LOCAL side only.
    db.conn.execute("DELETE FROM v2_campaign_sends WHERE intervention_id = ?",
                    (intv_id,))
    db.conn.execute(
        "DELETE FROM intervention_audit_log WHERE intervention_id = ? "
        "AND action = ?", (intv_id, ExecutionAdapter.FINALIZATION_AUDIT_ACTION))
    db.conn.commit()

    iv = v2_repo.get_intervention(intv_id)
    iv.status = InterventionStatus.APPROVED
    iv.sent_count = None
    iv.measurement_started_at = None
    iv.measurement_ends_at = None
    iv.executed_at = None
    v2_repo.save_intervention(iv)

    return attempt


def _fresh_adapter(db, v2_repo, mc):
    """A brand-new adapter, as after a process restart."""
    return ExecutionAdapter(
        db=db, v2_repo=v2_repo, campaign_engine=FakeCampaignEngine(mc),
    )


class TestCrashAfterConfirmedSend(unittest.TestCase):
    """MANDATORY: a proven send must converge locally after a restart.

    Before this work the system was duplicate-safe but not recoverable:
    the successful attempt blocked any resend (good), but the
    intervention stayed 'approved', recipients stayed in staging, and no
    attribution rows existed — so measurement could never run and the
    state was unrepairable without manual SQL.
    """

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def test_crash_after_confirmed_send_recovers_without_resending(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        attempt = _crash_after_confirmed_send(db, v2_repo, adapter, mc, intv_id)
        sends_before = mc.send_call_count()
        self.assertEqual(sends_before, 1)

        # Restart: brand-new adapter, no in-memory state carried over.
        recovered_adapter = _fresh_adapter(db, v2_repo, mc)
        result = recovered_adapter.execute(intv_id)

        # 1. NO provider call of any kind.
        self.assertEqual(mc.send_call_count(), sends_before,
                         "recovery dispatched another send")

        # 2. Result signals a recovered already-sent state.
        self.assertEqual(result["error"], "already_sent_recovered")
        self.assertTrue(result["recovered"])
        self.assertTrue(result["finalization"]["repaired"])

        # 3. Same successful attempt — no new claim.
        attempts = v2_repo.get_send_attempts(intv_id)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0].id, attempt.id)
        self.assertEqual(attempts[0].attempt_status,
                         SendAttemptStatus.CONFIRMED_SENT)

        # 4. Intervention advanced to measuring.
        iv = v2_repo.get_intervention(intv_id)
        self.assertEqual(iv.status, InterventionStatus.MEASURING)

        # 5. Recipients promoted to attribution.
        staged = v2_repo.get_attempt_recipients(attempt.id)
        promoted = {s["email"] for s in v2_repo.get_sends(intv_id)}
        self.assertEqual(promoted, set(staged))
        self.assertTrue(promoted)

        # 6. sent_count correct.
        self.assertEqual(iv.sent_count, len(staged))

        # 7. Measurement window set from the send time.
        self.assertIsNotNone(iv.measurement_started_at)
        self.assertIsNotNone(iv.measurement_ends_at)

        # 8. Finalization audited exactly once.
        finals = [e for e in v2_repo.get_audit_log(intv_id)
                  if e["action"] == ExecutionAdapter.FINALIZATION_AUDIT_ACTION]
        self.assertEqual(len(finals), 1)

    def test_repeated_recovery_is_idempotent(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        _crash_after_confirmed_send(db, v2_repo, adapter, mc, intv_id)

        a1 = _fresh_adapter(db, v2_repo, mc)
        first = a1.execute(intv_id)
        self.assertEqual(first["error"], "already_sent_recovered")

        sends_after_first = mc.send_call_count()
        sends_count = len(v2_repo.get_sends(intv_id))

        # Run recovery repeatedly from fresh adapters.
        for _ in range(3):
            r = _fresh_adapter(db, v2_repo, mc).execute(intv_id)
            self.assertEqual(r["error"], "already_sent")

        self.assertEqual(mc.send_call_count(), sends_after_first)
        self.assertEqual(len(v2_repo.get_sends(intv_id)), sends_count)
        finals = [e for e in v2_repo.get_audit_log(intv_id)
                  if e["action"] == ExecutionAdapter.FINALIZATION_AUDIT_ACTION]
        self.assertEqual(len(finals), 1, "duplicate finalization audit")

    def test_recovery_works_even_when_external_send_disabled(self):
        """Local repair must not be gated behind the send flag.

        Recovery contacts nobody, so a proven send left unfinalized must
        still be repairable after the flag is turned off.
        """
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        _crash_after_confirmed_send(db, v2_repo, adapter, mc, intv_id)
        sends_before = mc.send_call_count()

        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "0"
        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        self.assertEqual(result["error"], "already_sent_recovered")
        self.assertEqual(mc.send_call_count(), sends_before)
        self.assertEqual(v2_repo.get_intervention(intv_id).status,
                         InterventionStatus.MEASURING)


class TestPartialFinalizationRecovery(unittest.TestCase):
    """Every partial-crash boundary must converge safely."""

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def _sent_env(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        result = adapter.execute(intv_id)
        self.assertEqual(result["status"], "executed")
        attempt = v2_repo.get_send_attempts(intv_id)[0]
        return db, v2_repo, mc, intv_id, attempt

    def _assert_converged(self, v2_repo, intv_id, attempt, mc, sends_before):
        self.assertEqual(mc.send_call_count(), sends_before)
        iv = v2_repo.get_intervention(intv_id)
        self.assertEqual(iv.status, InterventionStatus.MEASURING)
        staged = v2_repo.get_attempt_recipients(attempt.id)
        promoted = [s["email"] for s in v2_repo.get_sends(intv_id)]
        self.assertEqual(sorted(promoted), sorted(staged))
        self.assertEqual(len(promoted), len(set(promoted)),
                         "duplicate recipient rows")
        self.assertEqual(iv.sent_count, len(staged))
        finals = [e for e in v2_repo.get_audit_log(intv_id)
                  if e["action"] == ExecutionAdapter.FINALIZATION_AUDIT_ACTION]
        self.assertEqual(len(finals), 1)

    def test_a_transitioned_but_recipients_not_promoted(self):
        db, v2_repo, mc, intv_id, attempt = self._sent_env()
        db.conn.execute("DELETE FROM v2_campaign_sends WHERE intervention_id = ?",
                        (intv_id,))
        db.conn.commit()
        sends_before = mc.send_call_count()

        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        self.assertEqual(result["error"], "already_sent_recovered")
        self._assert_converged(v2_repo, intv_id, attempt, mc, sends_before)

    def test_b_recipients_promoted_but_not_transitioned(self):
        db, v2_repo, mc, intv_id, attempt = self._sent_env()
        iv = v2_repo.get_intervention(intv_id)
        iv.status = InterventionStatus.APPROVED
        iv.sent_count = None
        iv.measurement_started_at = None
        iv.measurement_ends_at = None
        v2_repo.save_intervention(iv)
        db.conn.execute(
            "DELETE FROM intervention_audit_log WHERE intervention_id = ? "
            "AND action = ?",
            (intv_id, ExecutionAdapter.FINALIZATION_AUDIT_ACTION))
        db.conn.commit()
        sends_before = mc.send_call_count()

        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        self.assertEqual(result["error"], "already_sent_recovered")
        self._assert_converged(v2_repo, intv_id, attempt, mc, sends_before)

    def test_c_both_complete_but_audit_missing(self):
        db, v2_repo, mc, intv_id, attempt = self._sent_env()
        db.conn.execute(
            "DELETE FROM intervention_audit_log WHERE intervention_id = ? "
            "AND action = ?",
            (intv_id, ExecutionAdapter.FINALIZATION_AUDIT_ACTION))
        db.conn.commit()
        sends_before = mc.send_call_count()

        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        self.assertEqual(result["error"], "already_sent_recovered")
        self._assert_converged(v2_repo, intv_id, attempt, mc, sends_before)

    def test_d_audit_present_duplicate_recovery_invoked(self):
        """Audit already recorded — repair the rest without re-auditing."""
        db, v2_repo, mc, intv_id, attempt = self._sent_env()
        db.conn.execute("DELETE FROM v2_campaign_sends WHERE intervention_id = ?",
                        (intv_id,))
        iv = v2_repo.get_intervention(intv_id)
        iv.sent_count = None
        v2_repo.save_intervention(iv)
        db.conn.commit()
        sends_before = mc.send_call_count()

        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        self.assertEqual(result["error"], "already_sent_recovered")
        self._assert_converged(v2_repo, intv_id, attempt, mc, sends_before)

    def test_promotion_preserves_original_sent_at(self):
        """Replaying promotion must not rewrite attribution timestamps."""
        db, v2_repo, mc, intv_id, attempt = self._sent_env()
        original = {s["email"]: s["sent_at"] for s in v2_repo.get_sends(intv_id)}
        self.assertTrue(original)

        for _ in range(3):
            v2_repo.promote_attempt_recipients(
                attempt.id, intv_id, "draft-001",
                sent_at=attempt.provider_sent_at)

        after = {s["email"]: s["sent_at"] for s in v2_repo.get_sends(intv_id)}
        self.assertEqual(after, original)
        self.assertEqual(len(after), len(original))

    def test_no_illegal_transition_from_measuring(self):
        """Recovery on an already-measuring intervention must not blow up."""
        db, v2_repo, mc, intv_id, attempt = self._sent_env()
        iv = v2_repo.get_intervention(intv_id)
        self.assertEqual(iv.status, InterventionStatus.MEASURING)

        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)
        self.assertEqual(result["error"], "already_sent")
        self.assertEqual(v2_repo.get_intervention(intv_id).status,
                         InterventionStatus.MEASURING)


class TestAuthoritativeSendTimestamp(unittest.TestCase):
    """Attribution must measure from when the provider actually sent."""

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def test_direct_send_records_confirmation_time(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        before = datetime.now(timezone.utc)
        adapter.execute(intv_id)
        after = datetime.now(timezone.utc)

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        sent_at = ExecutionAdapter._parse_provider_time(attempt.provider_sent_at)
        self.assertIsNotNone(sent_at)
        self.assertIsNotNone(sent_at.tzinfo, "must be timezone-aware UTC")
        self.assertGreaterEqual(sent_at, before.replace(microsecond=0))
        self.assertLessEqual(sent_at, after)

    def test_measurement_window_starts_at_send_time(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        iv = v2_repo.get_intervention(intv_id)
        sent_at = ExecutionAdapter._parse_provider_time(attempt.provider_sent_at)
        started = ExecutionAdapter._parse_provider_time(iv.measurement_started_at)
        ends = ExecutionAdapter._parse_provider_time(iv.measurement_ends_at)

        self.assertEqual(started, sent_at)
        self.assertEqual(ends - started,
                         timedelta(days=ATTRIBUTION_WINDOW_DAYS))

    def test_reconciliation_uses_provider_send_time_not_reconcile_time(self):
        """A send reconciled hours later still measures from the send.

        This is the case that matters: if the window started at
        reconciliation time, every order placed between the real send
        and the reconciliation would fall outside attribution.
        """
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('timeout',))
        adapter.execute(intv_id)
        mc.clear_injections()

        provider_send_time = "2026-01-15T10:00:00+00:00"
        mc.set_campaign_status("sent", emails_sent=2,
                               send_time=provider_send_time)

        result = _fresh_adapter(db, v2_repo, mc).reconcile_send_attempt(intv_id)
        self.assertEqual(result["status"], "reconciled_sent")

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        self.assertEqual(attempt.attempt_status,
                         SendAttemptStatus.RECONCILED_SENT)
        stored = ExecutionAdapter._parse_provider_time(attempt.provider_sent_at)
        self.assertEqual(stored,
                         datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc))

        iv = v2_repo.get_intervention(intv_id)
        started = ExecutionAdapter._parse_provider_time(iv.measurement_started_at)
        ends = ExecutionAdapter._parse_provider_time(iv.measurement_ends_at)
        self.assertEqual(started, stored)
        self.assertEqual(ends - started,
                         timedelta(days=ATTRIBUTION_WINDOW_DAYS))

    def test_reconciled_sent_promotes_and_finalizes_via_shared_path(self):
        """Reconciliation must reuse the finalizer, not a second copy."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('timeout',))
        adapter.execute(intv_id)
        mc.clear_injections()
        mc.set_campaign_status("sent", emails_sent=2)

        result = _fresh_adapter(db, v2_repo, mc).reconcile_send_attempt(intv_id)
        self.assertEqual(result["status"], "reconciled_sent")

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        iv = v2_repo.get_intervention(intv_id)
        staged = v2_repo.get_attempt_recipients(attempt.id)
        promoted = {s["email"] for s in v2_repo.get_sends(intv_id)}

        self.assertEqual(iv.status, InterventionStatus.MEASURING)
        self.assertEqual(promoted, set(staged))
        self.assertEqual(iv.sent_count, len(staged))
        finals = [e for e in v2_repo.get_audit_log(intv_id)
                  if e["action"] == ExecutionAdapter.FINALIZATION_AUDIT_ACTION]
        self.assertEqual(len(finals), 1)

    def test_finalizer_refuses_unproven_attempts(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('timeout',))
        adapter.execute(intv_id)

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        self.assertEqual(attempt.attempt_status, SendAttemptStatus.AMBIGUOUS)
        iv = v2_repo.get_intervention(intv_id)

        out = adapter.finalize_confirmed_send(attempt, iv)
        self.assertEqual(out["error"], "attempt_not_successful")
        self.assertEqual(iv.status, InterventionStatus.APPROVED)


# ─────────────────────────────────────────────────────────────────────
# Attribution clock — provider_sent_at must be the ONE authority
# ─────────────────────────────────────────────────────────────────────

REAL_SEND = datetime(2026, 9, 14, 10, 0, tzinfo=timezone.utc)


def _force_provider_sent_at(db, v2_repo, intv_id, when):
    """Rewrite the attempt's provider send time, then rewind local state.

    Produces the crash shape: provider proven to have sent at `when`,
    nothing finalized locally yet.
    """
    attempt = v2_repo.get_send_attempts(intv_id)[0]
    attempt.provider_sent_at = when
    v2_repo.update_send_attempt(attempt)

    db.conn.execute("DELETE FROM v2_campaign_sends WHERE intervention_id = ?",
                    (intv_id,))
    db.conn.execute(
        "DELETE FROM intervention_audit_log WHERE intervention_id = ? "
        "AND action = ?", (intv_id, ExecutionAdapter.FINALIZATION_AUDIT_ACTION))
    db.conn.commit()

    iv = v2_repo.get_intervention(intv_id)
    iv.status = InterventionStatus.APPROVED
    iv.sent_count = None
    iv.measurement_started_at = None
    iv.measurement_ends_at = None
    iv.executed_at = None
    v2_repo.save_intervention(iv)
    return attempt


class TestAttributionClockConsistency(unittest.TestCase):
    """Recipient send rows must carry the PROVIDER send time.

    The defect: promotion stamped rows with datetime.now() at
    finalization. measure() then read those rows as the window start, so
    a send recovered six hours after a crash started its attribution
    window six hours late and silently dropped every order in between.
    """

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def _sent_at_values(self, v2_repo, intv_id):
        return {
            ExecutionAdapter._parse_provider_time(r["sent_at"])
            for r in v2_repo.get_sends(intv_id)
        }

    def test_crash_recovery_stamps_rows_with_provider_send_time(self):
        """MANDATORY: send 10:00, recover later, rows must say 10:00."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        _force_provider_sent_at(db, v2_repo, intv_id, REAL_SEND)

        # Recovery happens "now", hours after the real send.
        result = _fresh_adapter(db, v2_repo, mc).execute(intv_id)
        self.assertEqual(result["error"], "already_sent_recovered")

        stamped = self._sent_at_values(v2_repo, intv_id)
        self.assertEqual(stamped, {REAL_SEND},
                         "recipient rows must carry the provider send time, "
                         "not the recovery time")

        # And the row time is nowhere near the recovery clock.
        self.assertGreater(datetime.now(timezone.utc) - REAL_SEND,
                           timedelta(hours=0))

    def test_all_three_clocks_agree_after_recovery(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        _force_provider_sent_at(db, v2_repo, intv_id, REAL_SEND)
        _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        iv = v2_repo.get_intervention(intv_id)

        provider = ExecutionAdapter._parse_provider_time(attempt.provider_sent_at)
        started = ExecutionAdapter._parse_provider_time(iv.measurement_started_at)
        rows = self._sent_at_values(v2_repo, intv_id)

        self.assertEqual(provider, REAL_SEND)
        self.assertEqual(started, REAL_SEND)
        self.assertEqual(rows, {REAL_SEND})

    def test_clock_report_flags_consistency(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        _force_provider_sent_at(db, v2_repo, intv_id, REAL_SEND)
        a = _fresh_adapter(db, v2_repo, mc)
        a.execute(intv_id)

        report = a.attribution_clock_report(intv_id)
        self.assertTrue(report["consistent"])
        self.assertEqual(report["provider_sent_at"], REAL_SEND.isoformat())
        self.assertEqual(report["recipient_sent_at_min"], REAL_SEND.isoformat())
        self.assertEqual(report["recipient_sent_at_max"], REAL_SEND.isoformat())

    def test_direct_send_rows_match_confirmation_time(self):
        """Normal path: recipient rows equal attempt.provider_sent_at."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        provider = ExecutionAdapter._parse_provider_time(attempt.provider_sent_at)
        rows = self._sent_at_values(v2_repo, intv_id)

        self.assertEqual(rows, {provider})

    def test_late_reconciliation_rows_use_provider_send_time(self):
        """Ambiguous send reconciled hours later still stamps T, not T+N."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        mc.inject('/actions/send', ('timeout',))
        adapter.execute(intv_id)
        mc.clear_injections()

        mc.set_campaign_status("sent", emails_sent=2,
                               send_time=REAL_SEND.isoformat())
        result = _fresh_adapter(db, v2_repo, mc).reconcile_send_attempt(intv_id)
        self.assertEqual(result["status"], "reconciled_sent")

        rows = self._sent_at_values(v2_repo, intv_id)
        self.assertEqual(rows, {REAL_SEND},
                         "reconciled rows must use the provider send time, "
                         "not the reconciliation time")

    def test_replay_preserves_provider_timestamp(self):
        """Repeated promotion must not rewrite an existing row."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        _force_provider_sent_at(db, v2_repo, intv_id, REAL_SEND)
        _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        before = {s["email"]: s["sent_at"] for s in v2_repo.get_sends(intv_id)}
        attempt = v2_repo.get_send_attempts(intv_id)[0]
        for _ in range(3):
            v2_repo.promote_attempt_recipients(
                attempt.id, intv_id, "draft-001",
                sent_at=datetime.now(timezone.utc),  # hostile: wrong clock
            )
        after = {s["email"]: s["sent_at"] for s in v2_repo.get_sends(intv_id)}
        self.assertEqual(after, before,
                         "existing rows must keep their original timestamp")

    def test_partial_promotion_fills_gaps_with_provider_time(self):
        """Missing rows get provider time; existing rows are untouched."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        _force_provider_sent_at(db, v2_repo, intv_id, REAL_SEND)
        _fresh_adapter(db, v2_repo, mc).execute(intv_id)

        rows = v2_repo.get_sends(intv_id)
        self.assertGreaterEqual(len(rows), 2)
        victim = rows[0]["email"]
        db.conn.execute(
            "DELETE FROM v2_campaign_sends WHERE intervention_id = ? AND email = ?",
            (intv_id, victim))
        db.conn.commit()

        attempt = v2_repo.get_send_attempts(intv_id)[0]
        v2_repo.promote_attempt_recipients(
            attempt.id, intv_id, "draft-001",
            sent_at=attempt.provider_sent_at)

        self.assertEqual(self._sent_at_values(v2_repo, intv_id), {REAL_SEND})
        self.assertEqual(len(v2_repo.get_sends(intv_id)), len(rows))

    def test_repository_refuses_to_invent_a_send_time(self):
        """A None send time must raise, never silently become now()."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        attempt = v2_repo.get_send_attempts(intv_id)[0]
        with self.assertRaises(ValueError):
            v2_repo.promote_attempt_recipients(
                attempt.id, intv_id, "draft-001", sent_at=None)

    def test_execute_response_reflects_persisted_state(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        result = adapter.execute(intv_id)

        iv = v2_repo.get_intervention(intv_id)
        self.assertEqual(result["measurement_started_at"],
                         iv.measurement_started_at)
        self.assertEqual(result["measurement_ends_at"], iv.measurement_ends_at)
        self.assertEqual(result["sent_count"], iv.sent_count)
        self.assertIsNotNone(result["provider_sent_at"])
        self.assertEqual(result["intervention"]["status"], "measuring")


class TestAttributionWindowAgainstProviderTime(unittest.TestCase):
    """Orders must be attributed against the send, not the finalization."""

    def setUp(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"

    def tearDown(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def _order(self, db, order_id, email, when, tickets=1, amount=100.0):
        db.conn.execute(
            """INSERT INTO orders (order_id, event_id, email,
               order_timestamp, ticket_count, gross_amount)
               VALUES (?,?,?,?,?,?)""",
            (order_id, "evt1", email, when.isoformat(), tickets, amount),
        )
        db.conn.commit()

    def _recovered_env(self, send_time):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        adapter.execute(intv_id)
        _force_provider_sent_at(db, v2_repo, intv_id, send_time)
        a = _fresh_adapter(db, v2_repo, mc)
        a.execute(intv_id)
        return db, v2_repo, a, intv_id

    def test_orders_measured_against_send_not_finalization(self):
        """Send 10:00, finalize later. 09:59 out, 12:00 in, 17:00 in."""
        db, v2_repo, adapter, intv_id = self._recovered_env(REAL_SEND)

        self._order(db, "before", "alice@example.com",
                    REAL_SEND - timedelta(minutes=1))
        self._order(db, "during", "alice@example.com",
                    REAL_SEND + timedelta(hours=2))
        self._order(db, "after_recovery", "bob@example.com",
                    REAL_SEND + timedelta(hours=7))

        result = adapter.measure(intv_id)
        self.assertNotIn("error", result)

        # Two included (12:00 and 17:00), the 09:59 one excluded.
        self.assertEqual(result["actual"]["attributed_orders"], 2)

    def test_pre_send_order_excluded(self):
        db, v2_repo, adapter, intv_id = self._recovered_env(REAL_SEND)
        self._order(db, "before", "alice@example.com",
                    REAL_SEND - timedelta(minutes=1))
        result = adapter.measure(intv_id)
        self.assertEqual(result["actual"]["attributed_orders"], 0)

    def test_seven_day_boundary_uses_provider_send_time(self):
        """Sept 1 10:00 send: Sept 8 09:59 in, Sept 8 10:01 out.

        Both orders are seeded before a single measure() call — the
        first measurement completes the window and moves the
        intervention to 'learned', so it can only be measured once.
        """
        send = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
        db, v2_repo, adapter, intv_id = self._recovered_env(send)

        self._order(db, "inside", "alice@example.com",
                    datetime(2026, 9, 8, 9, 59, tzinfo=timezone.utc))
        self._order(db, "outside", "bob@example.com",
                    datetime(2026, 9, 8, 10, 1, tzinfo=timezone.utc))

        result = adapter.measure(intv_id)
        self.assertNotIn("error", result)
        self.assertEqual(
            result["actual"]["attributed_orders"], 1,
            "exactly the order inside the 7-day window from the PROVIDER "
            "send time should count")

    def test_boundary_is_measured_from_send_not_finalization(self):
        """The same order flips in/out purely on the send timestamp.

        Identical order, two environments differing only in
        provider_sent_at. Proves the boundary tracks the send.
        """
        order_at = datetime(2026, 9, 8, 9, 59, tzinfo=timezone.utc)

        # Sent Sept 1 10:00 -> order is 6d23h59m later: inside.
        db_a, repo_a, adapter_a, id_a = self._recovered_env(
            datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc))
        self._order(db_a, "o", "alice@example.com", order_at)
        self.assertEqual(
            adapter_a.measure(id_a)["actual"]["attributed_orders"], 1)

        # Sent Sept 1 09:00 -> the same order is 7d0h59m later: outside.
        db_b, repo_b, adapter_b, id_b = self._recovered_env(
            datetime(2026, 9, 1, 9, 0, tzinfo=timezone.utc))
        self._order(db_b, "o", "alice@example.com", order_at)
        self.assertEqual(
            adapter_b.measure(id_b)["actual"]["attributed_orders"], 0)

    def test_window_does_not_shift_when_finalized_days_later(self):
        send = datetime(2026, 9, 1, 10, 0, tzinfo=timezone.utc)
        db, v2_repo, adapter, intv_id = self._recovered_env(send)

        iv = v2_repo.get_intervention(intv_id)
        started = ExecutionAdapter._parse_provider_time(iv.measurement_started_at)
        ends = ExecutionAdapter._parse_provider_time(iv.measurement_ends_at)
        self.assertEqual(started, send)
        self.assertEqual(ends - started,
                         timedelta(days=ATTRIBUTION_WINDOW_DAYS))


class TestHealthReadiness(unittest.TestCase):
    """Health must not report ready without the Phase 2 schema."""

    def test_sqlite_healthy_with_all_tables(self):
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        h = v2_repo.health_check()
        self.assertTrue(h["ready"])
        self.assertTrue(h["tables_ok"])
        self.assertEqual(h["status"], "ok")
        self.assertEqual(h["tables_expected"], 7)

    def test_sqlite_degraded_when_send_attempts_missing(self):
        """Dropping the idempotency table must break readiness."""
        db, v2_repo, adapter, mc, intv_id = _make_test_environment()
        db.conn.execute("DROP TABLE v2_send_attempt_recipients")
        db.conn.execute("DROP TABLE v2_send_attempts")
        db.conn.commit()

        h = v2_repo.health_check()
        self.assertFalse(h["ready"])
        self.assertFalse(h["tables_ok"])
        self.assertEqual(h["status"], "degraded")
        self.assertIn("v2_send_attempts", h["missing_tables"])
        self.assertIn("v2_send_attempts", h["error"])

    def test_expected_tables_include_phase_2(self):
        from v2_state_repository import (
            _V2_EXPECTED_TABLES, REQUIRED_SCHEMA_VERSION,
        )
        self.assertIn("v2_send_attempts", _V2_EXPECTED_TABLES)
        self.assertIn("v2_send_attempt_recipients", _V2_EXPECTED_TABLES)
        self.assertEqual(len(_V2_EXPECTED_TABLES), 7)
        self.assertEqual(REQUIRED_SCHEMA_VERSION, 3)


if __name__ == '__main__':
    unittest.main()


class TestIncompleteAudienceCannotSend(unittest.TestCase):
    def test_missing_segment_never_creates_whole_audience_campaign(self):
        with patch.dict(os.environ, {'V2_ENABLE_EXTERNAL_SEND':'1'}):
            db, repo, adapter, mc, intervention_id = _make_test_environment()
            with patch.object(mc, 'get_tag_segment_id', return_value=None):
                result = adapter.execute(intervention_id)
            self.assertNotEqual(result.get('status'), 'executed')
            self.assertEqual(mc.send_call_count(), 0)
            self.assertFalse(any(c[0] == '_request_strict' and c[1]['path'] == '/campaigns' for c in mc.calls))

    def test_partial_audience_error_never_creates_campaign(self):
        with patch.dict(os.environ, {'V2_ENABLE_EXTERNAL_SEND':'1'}):
            db, repo, adapter, mc, intervention_id = _make_test_environment()
            with patch.object(mc, 'ensure_members', return_value={'added':1, 'errors':1}):
                result = adapter.execute(intervention_id)
            self.assertNotEqual(result.get('status'), 'executed')
            self.assertEqual(mc.send_call_count(), 0)
            self.assertFalse(any(c[0] == '_request_strict' and c[1]['path'] == '/campaigns' for c in mc.calls))
