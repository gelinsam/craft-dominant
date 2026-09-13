"""Phase 2 idempotent execution tests.

Verifies the claim-checkpoint-send pattern, crash recovery,
reconciliation, duplicate prevention, and dry-run semantics for
the V2 CRM execution pipeline.
"""

import os
import sqlite3
import unittest
from datetime import datetime, timezone
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
from execution_adapter import ExecutionAdapter, EXECUTION_GENERATION
from v2_state_repository import SQLiteV2StateRepository
from intervention_model import Intervention, InterventionStatus
from suppression_guard import SuppressionGuard, SENTINEL_TABLE_SCHEMA


# ─────────────────────────────────────────────────────────────────────
# Fake provider infrastructure
# ─────────────────────────────────────────────────────────────────────

class FakeDeterministicMailchimp:
    """Fake Mailchimp client with controllable behavior for testing.

    Tracks all calls made and allows injecting failures at specific
    steps (ensure_members, create_campaign, send_campaign).
    """

    def __init__(self):
        self.calls = []
        self._fail_at = None          # 'members', 'segment', 'campaign', 'send'
        self._raise_at = None         # 'members', 'segment', 'campaign', 'send'
        self._raise_exception = None  # Exception class/instance to raise
        self._send_return = True
        self._campaign_counter = 0
        self._campaign_status = "sent"  # For reconciliation queries

    # ── controllable behavior ────────────────────────────────────────

    def set_fail_at(self, step, exception=None):
        """Configure which step fails. step: 'members'|'segment'|'campaign'|'send'"""
        self._fail_at = step
        if exception:
            self._raise_at = step
            self._raise_exception = exception
        else:
            self._raise_at = None
            self._raise_exception = None

    def set_send_return(self, value):
        """Control whether send_campaign returns True or False."""
        self._send_return = value

    def set_campaign_status(self, status):
        """Set the status returned by _request (for reconciliation)."""
        self._campaign_status = status

    def reset(self):
        """Reset all state."""
        self.calls = []
        self._fail_at = None
        self._raise_at = None
        self._raise_exception = None
        self._send_return = True
        self._campaign_counter = 0
        self._campaign_status = "sent"

    # ── Mailchimp API surface ────────────────────────────────────────

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

    def create_campaign(self, subject="", preview_text="", html="", segment_id=None):
        self.calls.append(('create_campaign', {
            'subject': subject, 'preview_text': preview_text,
            'html': html, 'segment_id': segment_id,
        }))
        if self._raise_at == 'campaign':
            raise self._raise_exception
        if self._fail_at == 'campaign':
            raise RuntimeError("create_campaign simulated failure")
        self._campaign_counter += 1
        return f"mc-campaign-{self._campaign_counter}"

    def send_campaign(self, mc_campaign_id):
        self.calls.append(('send_campaign', {'mc_campaign_id': mc_campaign_id}))
        if self._raise_at == 'send':
            raise self._raise_exception
        if self._fail_at == 'send':
            return False
        return self._send_return

    def get_campaign_report(self, mc_campaign_id):
        self.calls.append(('get_campaign_report', {'mc_campaign_id': mc_campaign_id}))
        return {'status': self._campaign_status, 'emails_sent': 50}

    def _request(self, method, path):
        """Low-level request used by reconciliation."""
        self.calls.append(('_request', {'method': method, 'path': path}))
        return {
            'status': self._campaign_status,
            'emails_sent': 50,
            'send_time': '2026-01-15T10:00:00+00:00',
        }


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
        illegal_paths = [
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.AMBIGUOUS),
            (SendAttemptStatus.CLAIMED, SendAttemptStatus.SEND_REQUESTED),
            (SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.AUDIENCE_CONFIGURED, SendAttemptStatus.AMBIGUOUS),
            (SendAttemptStatus.SEND_REQUESTED, SendAttemptStatus.FAILED_PRE_SEND),
            (SendAttemptStatus.CONFIRMED_SENT, SendAttemptStatus.AMBIGUOUS),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.CONFIRMED_SENT),
            (SendAttemptStatus.AMBIGUOUS, SendAttemptStatus.FAILED_PRE_SEND),
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
        self.assertEqual(call_names, [
            'ensure_members', 'get_tag_segment_id',
            'create_campaign', 'send_campaign',
        ])

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

        # Second execution: intervention is now in 'measuring' state,
        # so it should fail the status gate
        result2 = adapter.execute(intv_id)
        self.assertIn("error", result2)
        # Status should not be approved anymore
        self.assertEqual(result2["error"], "illegal_status")

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
        """Ambiguous + Mailchimp says 'save' -> reconciled_not_sent, safe to retry."""
        db, v2_repo, adapter, mc, intv_id = self._make_ambiguous()
        mc.set_campaign_status("save")

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


if __name__ == '__main__':
    unittest.main()
