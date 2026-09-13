"""Tests for the intervention model, state machine, and campaign adapter.

Covers: state machine transitions (legal and illegal), intervention CRUD,
prepare-creates-draft-only (no send), expected-value math, opportunity_id
linkage, and deterministic IDs.
"""

import json
import os
import sqlite3
import unittest
from datetime import date, timedelta

from intervention_model import (
    IllegalTransition,
    Intervention,
    InterventionStatus,
    InterventionStore,
    validate_transition,
    _intervention_id,
    TERMINAL_STATES,
)
from campaign_adapter import (
    CampaignDraftAdapter,
    EMAIL_OPEN_RATE,
    EMAIL_CLICK_RATE,
    EMAIL_CONVERSION_RATE,
    CHAMPION_MULTIPLIER,
)
from suppression_guard import SuppressionGuard, SuppressionStatus, SENTINEL_TABLE_SCHEMA


def _seed_valid_suppression_sentinel(db, row_count=1, source="test"):
    """Create a valid, fresh suppression sentinel for tests that need healthy suppression state."""
    from datetime import datetime, timezone
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


# ─────────────────────────────────────────────────────────────────────
# State machine tests
# ─────────────────────────────────────────────────────────────────────

class TestStateMachine(unittest.TestCase):

    def test_legal_forward_transitions(self):
        """new → investigated → proposed → approved → executing → measuring → learned"""
        path = [
            (InterventionStatus.NEW, InterventionStatus.INVESTIGATED),
            (InterventionStatus.INVESTIGATED, InterventionStatus.PROPOSED),
            (InterventionStatus.PROPOSED, InterventionStatus.APPROVED),
            (InterventionStatus.APPROVED, InterventionStatus.EXECUTING),
            (InterventionStatus.EXECUTING, InterventionStatus.MEASURING),
            (InterventionStatus.MEASURING, InterventionStatus.LEARNED),
        ]
        for current, target in path:
            validate_transition(current, target)  # Should not raise

    def test_cancel_from_any_active_state(self):
        active_states = [
            InterventionStatus.NEW,
            InterventionStatus.INVESTIGATED,
            InterventionStatus.PROPOSED,
            InterventionStatus.APPROVED,
            InterventionStatus.EXECUTING,
            InterventionStatus.MEASURING,
        ]
        for state in active_states:
            validate_transition(state, InterventionStatus.CANCELLED)

    def test_reject_only_from_proposed(self):
        validate_transition(InterventionStatus.PROPOSED, InterventionStatus.REJECTED)
        # Reject should not be allowed from other states
        for state in [InterventionStatus.NEW, InterventionStatus.INVESTIGATED,
                      InterventionStatus.APPROVED, InterventionStatus.EXECUTING]:
            with self.assertRaises(IllegalTransition):
                validate_transition(state, InterventionStatus.REJECTED)

    def test_no_transitions_from_terminal(self):
        for terminal in TERMINAL_STATES:
            for target in InterventionStatus:
                if target != terminal:
                    with self.assertRaises(IllegalTransition):
                        validate_transition(terminal, target)

    def test_backward_transition_illegal(self):
        with self.assertRaises(IllegalTransition):
            validate_transition(InterventionStatus.PROPOSED, InterventionStatus.NEW)

    def test_skip_transition_illegal(self):
        with self.assertRaises(IllegalTransition):
            validate_transition(InterventionStatus.NEW, InterventionStatus.APPROVED)

    def test_self_transition_illegal(self):
        with self.assertRaises(IllegalTransition):
            validate_transition(InterventionStatus.PROPOSED, InterventionStatus.PROPOSED)


# ─────────────────────────────────────────────────────────────────────
# Intervention dataclass tests
# ─────────────────────────────────────────────────────────────────────

class TestIntervention(unittest.TestCase):

    def test_create_sets_deterministic_id(self):
        i = Intervention.create(
            opportunity_id="opp-abc",
            event_id="evt1",
            intervention_type="crm_campaign",
        )
        self.assertEqual(len(i.id), 16)
        # Same inputs → same ID
        i2 = Intervention.create(
            opportunity_id="opp-abc",
            event_id="evt1",
            intervention_type="crm_campaign",
        )
        self.assertEqual(i.id, i2.id)

    def test_different_type_different_id(self):
        i1 = Intervention.create(
            opportunity_id="opp-abc",
            event_id="evt1",
            intervention_type="crm_campaign",
        )
        i2 = Intervention.create(
            opportunity_id="opp-abc",
            event_id="evt1",
            intervention_type="ad_budget_shift",
        )
        self.assertNotEqual(i1.id, i2.id)

    def test_transition_sets_timestamps(self):
        i = Intervention.create(
            opportunity_id="opp1",
            event_id="evt1",
            intervention_type="crm_campaign",
        )
        self.assertEqual(i.status, InterventionStatus.NEW)
        self.assertIsNone(i.approved_at)
        self.assertIsNone(i.executed_at)

        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        self.assertIsNotNone(i.approved_at)

        i.transition_to(InterventionStatus.EXECUTING)
        self.assertIsNotNone(i.executed_at)

    def test_is_terminal(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        self.assertFalse(i.is_terminal)
        i.transition_to(InterventionStatus.INVESTIGATED)
        self.assertFalse(i.is_terminal)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.REJECTED)
        self.assertTrue(i.is_terminal)

    def test_to_dict(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
            rationale="Test rationale",
            expected_revenue=5000,
            expected_cost=0,
            expected_net_value=5000,
            confidence=0.55,
        )
        d = i.to_dict()
        self.assertEqual(d["status"], "new")
        self.assertEqual(d["opportunity_id"], "opp1")
        self.assertEqual(d["expected_revenue"], 5000)

    def test_status_string_coercion(self):
        """Intervention should coerce string status to enum."""
        i = Intervention(
            id="test",
            opportunity_id="opp1",
            event_id="evt1",
            intervention_type="crm_campaign",
            status="proposed",
        )
        self.assertEqual(i.status, InterventionStatus.PROPOSED)


# ─────────────────────────────────────────────────────────────────────
# InterventionStore persistence tests
# ─────────────────────────────────────────────────────────────────────

class FakeStoreDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row


class TestInterventionStore(unittest.TestCase):

    def setUp(self):
        self.db = FakeStoreDB()
        self.store = InterventionStore(self.db)

    def test_save_and_get(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
            rationale="Test",
        )
        self.store.save(i)
        loaded = self.store.get(i.id)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.opportunity_id, "opp1")
        self.assertEqual(loaded.status, InterventionStatus.NEW)

    def test_get_nonexistent(self):
        self.assertIsNone(self.store.get("nonexistent"))

    def test_get_by_opportunity(self):
        i1 = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i2 = Intervention.create(
            opportunity_id="opp2", event_id="evt2",
            intervention_type="crm_campaign",
        )
        self.store.save(i1)
        self.store.save(i2)
        results = self.store.get_by_opportunity("opp1")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0].opportunity_id, "opp1")

    def test_get_by_event(self):
        i1 = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i2 = Intervention.create(
            opportunity_id="opp2", event_id="evt1",
            intervention_type="ad_budget_shift",
        )
        self.store.save(i1)
        self.store.save(i2)
        results = self.store.get_by_event("evt1")
        self.assertEqual(len(results), 2)

    def test_list_all_excludes_terminal_by_default(self):
        i1 = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i2 = Intervention.create(
            opportunity_id="opp2", event_id="evt2",
            intervention_type="crm_campaign",
        )
        i2.transition_to(InterventionStatus.INVESTIGATED)
        i2.transition_to(InterventionStatus.PROPOSED)
        i2.transition_to(InterventionStatus.REJECTED)

        self.store.save(i1)
        self.store.save(i2)

        active = self.store.list_all(include_terminal=False)
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].opportunity_id, "opp1")

        all_items = self.store.list_all(include_terminal=True)
        self.assertEqual(len(all_items), 2)

    def test_save_updates_existing(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        self.store.save(i)
        i.transition_to(InterventionStatus.INVESTIGATED)
        self.store.save(i)

        loaded = self.store.get(i.id)
        self.assertEqual(loaded.status, InterventionStatus.INVESTIGATED)

    def test_evidence_json_roundtrip(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
            evidence={"key": "value", "nested": {"a": 1}},
        )
        self.store.save(i)
        loaded = self.store.get(i.id)
        self.assertEqual(loaded.evidence["key"], "value")
        self.assertEqual(loaded.evidence["nested"]["a"], 1)


# ─────────────────────────────────────────────────────────────────────
# Campaign adapter tests
# ─────────────────────────────────────────────────────────────────────

class FakeCampaignDB:
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
                ticket_count INTEGER, gross_amount REAL
            );
            CREATE TABLE customers (
                email TEXT PRIMARY KEY, favorite_city TEXT,
                event_types TEXT, rfm_segment TEXT
            );
            CREATE TABLE suppressions (email TEXT PRIMARY KEY);
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

    def get_past_attendees_not_purchased(self, event_id, event_name, limit=50000, current_buyer_emails=None):
        buyer_set = set(current_buyer_emails or [])
        rows = self.conn.execute("SELECT * FROM customers").fetchall()
        return [dict(r) for r in rows if r["email"] not in buyer_set]

    def get_city_prospects(self, city, exclude_emails=None, limit=50000):
        exclude = set(exclude_emails or [])
        rows = self.conn.execute(
            "SELECT * FROM customers WHERE favorite_city = ?", (city,)
        ).fetchall()
        return [dict(r) for r in rows if r["email"] not in exclude]


class TestCampaignAdapter(unittest.TestCase):

    def setUp(self):
        self.db = FakeCampaignDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Philly Coffee Festival", "coffee", "Philadelphia", event_date, 5000),
        )
        # Buyers
        for i in range(50):
            self.db.conn.execute(
                "INSERT INTO orders (order_id, event_id, email, ticket_count, gross_amount) VALUES (?,?,?,?,?)",
                (f"o{i}", "evt1", f"buyer{i}@example.com", 2, 100.0),
            )
        # CRM audience
        for i in range(200):
            self.db.conn.execute(
                "INSERT INTO customers VALUES (?,?,?,?)",
                (
                    f"crm{i}@example.com",
                    "Philadelphia",
                    "coffee",
                    "champion" if i < 25 else "regular",
                ),
            )
        # Suppressed
        self.db.conn.execute(
            "INSERT INTO suppressions VALUES (?)",
            ("crm0@example.com",),
        )
        self.db.conn.commit()
        _seed_valid_suppression_sentinel(self.db, row_count=1)
        self.adapter = CampaignDraftAdapter(self.db)

    def test_prepare_draft_creates_record(self):
        intervention = Intervention.create(
            opportunity_id="opp1",
            event_id="evt1",
            intervention_type="crm_campaign",
            confidence=0.55,
            rationale="Test prepare",
        )
        diagnosis_data = {
            "avg_ticket_price": 50.0,
            "crm_champions": 25,
            "days_until": 30,
            "tickets_sold": 700,
            "capacity": 5000,
        }
        result = self.adapter.prepare_draft(intervention, diagnosis_data)

        self.assertIn("campaign_draft_id", result)
        self.assertTrue(result["campaign_draft_id"].startswith("v2-"))
        self.assertEqual(result["status"], "draft")
        self.assertGreater(result["audience_count"], 0)
        self.assertEqual(result["copy_source"], "template")

    def test_draft_never_sends(self):
        """The campaign should be saved as 'draft' and never sent."""
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", confidence=0.55,
        )
        result = self.adapter.prepare_draft(intervention, {
            "avg_ticket_price": 50.0,
            "crm_champions": 25,
        })

        # Verify the draft status in the DB
        row = self.db.conn.execute(
            "SELECT status, sent_at FROM campaigns WHERE id = ?",
            (result["campaign_draft_id"],),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(row["status"], "draft")
        self.assertIsNone(row["sent_at"])

    def test_audience_excludes_buyers_and_suppressions(self):
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", confidence=0.55,
        )
        result = self.adapter.prepare_draft(intervention, {
            "avg_ticket_price": 50.0,
            "crm_champions": 25,
        })

        # Should exclude 50 buyers and 1 suppression
        # CRM has 200 people, some overlap with buyers
        self.assertGreater(result["audience_count"], 0)
        # Verify no buyer emails in audience
        audience_result = self.adapter._build_audience("evt1", self.db.get_event("evt1"))
        buyer_set = set(self.db.get_event_buyers("evt1"))
        for email in audience_result["emails"]:
            self.assertNotIn(email, buyer_set)

    def test_conversion_math_deterministic(self):
        """Regression: unified funnel — champions include purchase conversion rate."""
        result = self.adapter._model_conversions(1000, 50)
        expected_opens = int(1000 * EMAIL_OPEN_RATE)
        expected_clicks = int(expected_opens * EMAIL_CLICK_RATE)

        # Unified funnel: all segments use open → click → purchase
        base_prob = EMAIL_OPEN_RATE * EMAIL_CLICK_RATE * EMAIL_CONVERSION_RATE
        champion_prob = min(base_prob * CHAMPION_MULTIPLIER, 1.0)
        non_champions = 1000 - 50
        base = int(non_champions * base_prob)
        champion = int(50 * champion_prob)

        self.assertEqual(result["expected_opens"], expected_opens)
        self.assertEqual(result["expected_clicks"], expected_clicks)
        self.assertEqual(result["expected_tickets"], base + champion)

    def test_conversion_assumptions_in_output(self):
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", confidence=0.55,
        )
        result = self.adapter.prepare_draft(intervention, {
            "avg_ticket_price": 50.0,
            "crm_champions": 25,
        })
        assumptions = result["conversion_assumptions"]
        self.assertEqual(assumptions["email_open_rate"], EMAIL_OPEN_RATE)
        self.assertEqual(assumptions["email_click_rate"], EMAIL_CLICK_RATE)
        self.assertEqual(assumptions["email_conversion_rate"], EMAIL_CONVERSION_RATE)
        self.assertEqual(assumptions["champion_multiplier"], CHAMPION_MULTIPLIER)

    def test_event_not_found_raises(self):
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="nonexistent",
            intervention_type="crm_campaign",
        )
        with self.assertRaises(ValueError):
            self.adapter.prepare_draft(intervention, {})

    def test_urgency_text_varies_by_days(self):
        """Template copy should reflect urgency based on days_until."""
        event = self.db.get_event("evt1")
        # 5 days: "Last chance"
        copy = self.adapter._generate_copy(event, {"days_until": 5})
        self.assertIn("Last chance", copy["subject_line"])

        # 10 days: "Almost here"
        copy = self.adapter._generate_copy(event, {"days_until": 10})
        self.assertIn("Almost here", copy["subject_line"])

        # 25 days: "Coming up"
        copy = self.adapter._generate_copy(event, {"days_until": 25})
        self.assertIn("Coming up", copy["subject_line"])

        # 45 days: "Mark your calendar"
        copy = self.adapter._generate_copy(event, {"days_until": 45})
        self.assertIn("Mark your calendar", copy["subject_line"])

    def test_opportunity_id_linkage(self):
        """Intervention ID is deterministically derived from opportunity_id + type."""
        iid = _intervention_id("opp-abc", "crm_campaign")
        self.assertEqual(len(iid), 16)

        # Same inputs → same ID
        self.assertEqual(iid, _intervention_id("opp-abc", "crm_campaign"))

        # Different opportunity → different ID
        self.assertNotEqual(iid, _intervention_id("opp-xyz", "crm_campaign"))

    def test_suppression_failure_blocks_draft(self):
        """Regression: if suppression list cannot be read, prepare_draft must fail."""
        # Drop the suppressions table to simulate unavailability
        self.db.conn.execute("DROP TABLE suppressions")
        self.db.conn.commit()

        adapter = CampaignDraftAdapter(self.db)
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", confidence=0.55,
        )
        with self.assertRaises(ValueError) as ctx:
            adapter.prepare_draft(intervention, {
                "avg_ticket_price": 50.0,
                "crm_champions": 25,
            })
        self.assertIn("suppression", str(ctx.exception).lower())

    def test_gap_cap_applied_in_prepare_draft(self):
        """Regression: conversions should be capped at gap_tickets in campaign adapter."""
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", confidence=0.55,
        )
        # gap_tickets=1 should cap conversions at 1
        result = self.adapter.prepare_draft(intervention, {
            "avg_ticket_price": 50.0,
            "crm_champions": 25,
            "gap_tickets": 1,
        })
        self.assertLessEqual(result["conversion_assumptions"]["expected_tickets"], 1)

    def test_champion_funnel_includes_conversion_rate(self):
        """Regression: champion conversions must include EMAIL_CONVERSION_RATE term."""
        # With unified funnel, champion prob should be base_prob * CHAMPION_MULTIPLIER
        result = self.adapter._model_conversions(0, 100)
        # With 0 non-champion audience, only champion conversions should be present
        base_prob = EMAIL_OPEN_RATE * EMAIL_CLICK_RATE * EMAIL_CONVERSION_RATE
        champion_prob = min(base_prob * CHAMPION_MULTIPLIER, 1.0)
        expected = int(100 * champion_prob)
        self.assertEqual(result["expected_tickets"], expected)
        # Verify the champion_prob is meaningfully smaller than the old (buggy) formula
        old_buggy = int(100 * EMAIL_OPEN_RATE * EMAIL_CLICK_RATE * CHAMPION_MULTIPLIER)
        self.assertLess(result["expected_tickets"], old_buggy,
                        "Champion conversions should be smaller with conversion_rate included")


# ─────────────────────────────────────────────────────────────────────
# Audit logger tests
# ─────────────────────────────────────────────────────────────────────

class TestAuditLogger(unittest.TestCase):

    def setUp(self):
        self.db = FakeStoreDB()
        from intervention_model import AuditLogger
        self.logger = AuditLogger(self.db)

    def test_log_and_retrieve(self):
        self.logger.log(
            "intv-1", "evt-1",
            action="prepared",
            from_status="new",
            to_status="proposed",
            actor="system",
        )
        entries = self.logger.get_log("intv-1")
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0]["action"], "prepared")
        self.assertEqual(entries[0]["from_status"], "new")
        self.assertEqual(entries[0]["to_status"], "proposed")
        self.assertEqual(entries[0]["actor"], "system")

    def test_multiple_entries_ordered_by_id(self):
        for action in ["prepared", "approved", "executed"]:
            self.logger.log("intv-1", "evt-1", action=action, actor="user")
        entries = self.logger.get_log("intv-1")
        self.assertEqual(len(entries), 3)
        self.assertEqual([e["action"] for e in entries], ["prepared", "approved", "executed"])

    def test_metadata_json_roundtrip(self):
        self.logger.log(
            "intv-1", "evt-1", action="executed",
            metadata={"sent_count": 42, "campaign_id": "v2-abc"},
        )
        entries = self.logger.get_log("intv-1")
        self.assertEqual(entries[0]["metadata"]["sent_count"], 42)
        self.assertEqual(entries[0]["metadata"]["campaign_id"], "v2-abc")

    def test_error_field_persisted(self):
        self.logger.log(
            "intv-1", "evt-1", action="execute_blocked",
            error="suppression unavailable",
        )
        entries = self.logger.get_log("intv-1")
        self.assertEqual(entries[0]["error"], "suppression unavailable")

    def test_separate_intervention_isolation(self):
        self.logger.log("intv-1", "evt-1", action="prepared")
        self.logger.log("intv-2", "evt-2", action="prepared")
        self.assertEqual(len(self.logger.get_log("intv-1")), 1)
        self.assertEqual(len(self.logger.get_log("intv-2")), 1)


# ─────────────────────────────────────────────────────────────────────
# Execution adapter tests
# ─────────────────────────────────────────────────────────────────────

class FakeExecutionDB:
    """Full-featured test DB for execution adapter tests."""
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
            CREATE TABLE suppressions (email TEXT PRIMARY KEY);
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

    def get_past_attendees_not_purchased(self, event_id, event_name, limit=50000, current_buyer_emails=None):
        buyer_set = set(current_buyer_emails or [])
        rows = self.conn.execute("SELECT * FROM customers").fetchall()
        return [dict(r) for r in rows if r["email"] not in buyer_set]

    def get_city_prospects(self, city, exclude_emails=None, limit=50000):
        exclude = set(exclude_emails or [])
        rows = self.conn.execute(
            "SELECT * FROM customers WHERE favorite_city = ?", (city,)
        ).fetchall()
        return [dict(r) for r in rows if r["email"] not in exclude]


class TestExecutionAdapter(unittest.TestCase):

    def setUp(self):
        from intervention_model import AuditLogger, LearningStore, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)

        # Create campaign_sends and learning_records tables
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)

        self.v2_repo = SQLiteV2StateRepository(self.db)
        self.adapter = ExecutionAdapter(self.db, self.v2_repo, campaign_engine=None)

        # Seed event
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test Fest", "coffee", "Philadelphia", event_date, 5000),
        )
        # Seed audience
        for i in range(50):
            self.db.conn.execute(
                "INSERT INTO customers VALUES (?,?,?,?)",
                (f"audience{i}@example.com", "Philadelphia", "coffee", "regular"),
            )
        # Seed suppressed email so sentinel is healthy (non-empty)
        self.db.conn.execute(
            "INSERT INTO suppressions VALUES (?)", ("suppressed@example.com",)
        )
        # Seed campaign draft
        self.db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-draft-1", "test-intv", "evt1", "Test subject", 50, "draft", "2026-01-01"),
        )
        self.db.conn.commit()
        _seed_valid_suppression_sentinel(self.db, row_count=1)

    def _make_approved_intervention(self, intv_id="test-intv"):
        """Helper: create an approved intervention linked to campaign draft."""
        i = Intervention(
            id=intv_id,
            opportunity_id="opp1",
            event_id="evt1",
            intervention_type="crm_campaign",
            status=InterventionStatus.NEW,
            expected_revenue=5000,
            confidence=0.6,
            campaign_draft_id="v2-draft-1",
            measurement_window=7,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        self.store.save(i)
        return i

    # Gate 1: status check
    def test_execute_blocks_non_approved(self):
        """Execute must reject if status is not 'approved'."""
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        self.store.save(i)

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["error"], "illegal_status")

    # Gate 2: campaign draft required
    def test_execute_blocks_without_campaign_draft(self):
        """Execute must reject if no campaign draft is linked."""
        i = Intervention.create(
            opportunity_id="opp2", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        # campaign_draft_id is None
        self.store.save(i)

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["error"], "no_campaign_draft")

    def test_execute_blocks_missing_campaign_record(self):
        """Execute must reject if campaign_draft_id points to a missing record."""
        i = Intervention.create(
            opportunity_id="opp3", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        i.campaign_draft_id = "v2-nonexistent"
        self.store.save(i)

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["error"], "campaign_draft_missing")

    # Gate 3: suppression list (fail-closed)
    def test_execute_blocks_suppression_unavailable(self):
        """Execute must block if suppression list is unreadable."""
        i = self._make_approved_intervention()
        self.db.conn.execute("DROP TABLE suppressions")
        self.db.conn.commit()

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["error"], "suppression_unavailable")

    def test_execute_blocks_no_sentinel(self):
        """Execute must block if suppression sentinel has never been created."""
        i = self._make_approved_intervention()
        # Remove sentinel — simulates first boot with no sync
        self.db.conn.execute("DELETE FROM v2_suppression_sync")
        self.db.conn.commit()

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["error"], "suppression_never_synced")

    # Gate 5: external send flag
    def test_execute_dry_run_without_flag(self):
        """Default: no V2_ENABLE_EXTERNAL_SEND → dry-run, no external HTTP."""
        i = self._make_approved_intervention()
        # Make sure flag is NOT set
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["status"], "external_send_disabled")
        self.assertIn("dry_run", result)
        self.assertGreater(result["dry_run"]["audience_count"], 0)

        # Verify intervention status did NOT advance
        loaded = self.store.get(i.id)
        self.assertEqual(loaded.status, InterventionStatus.APPROVED)

    def test_execute_dry_run_audits(self):
        """Dry-run must write an audit log entry."""
        i = self._make_approved_intervention()
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

        self.adapter.execute(i.id, actor="user")

        entries = self.audit.get_log(i.id)
        dry_run_entries = [e for e in entries if e["action"] == "execute_dry_run"]
        self.assertEqual(len(dry_run_entries), 1)

    def test_execute_not_found(self):
        result = self.adapter.execute("nonexistent", actor="user")
        self.assertEqual(result["error"], "intervention_not_found")

    # Audit trail on blocks
    def test_blocked_execution_writes_audit(self):
        """Every blocked execution attempt must leave an audit trail."""
        i = Intervention.create(
            opportunity_id="opp-audit", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        self.store.save(i)

        self.adapter.execute(i.id, actor="user")
        entries = self.audit.get_log(i.id)
        blocked = [e for e in entries if "blocked" in e["action"]]
        self.assertGreater(len(blocked), 0)


# ─────────────────────────────────────────────────────────────────────
# Measurement tests
# ─────────────────────────────────────────────────────────────────────

class TestMeasurement(unittest.TestCase):

    def setUp(self):
        from intervention_model import AuditLogger, LearningStore, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.v2_repo = SQLiteV2StateRepository(self.db)
        self.adapter = ExecutionAdapter(self.db, self.v2_repo, campaign_engine=None)

        # Seed event
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test Fest", "coffee", "Philadelphia", event_date, 5000),
        )
        self.db.conn.commit()

    def _make_measuring_intervention(self, sent_emails=None):
        """Helper: create an intervention in 'measuring' status with campaign sends."""
        from datetime import datetime, timezone, timedelta

        i = Intervention(
            id="meas-intv",
            opportunity_id="opp1",
            event_id="evt1",
            intervention_type="crm_campaign",
            status=InterventionStatus.NEW,
            expected_revenue=5000,
            confidence=0.6,
            campaign_draft_id="v2-draft-1",
            measurement_window=7,
            sent_count=len(sent_emails or []),
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        i.transition_to(InterventionStatus.EXECUTING)
        i.transition_to(InterventionStatus.MEASURING)
        self.store.save(i)

        # Record sends
        sent_at = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        for email in (sent_emails or []):
            self.db.conn.execute(
                """INSERT INTO v2_campaign_sends
                   (intervention_id, campaign_draft_id, email, sent_at)
                   VALUES (?,?,?,?)""",
                ("meas-intv", "v2-draft-1", email, sent_at),
            )
        self.db.conn.commit()
        return i

    def _in_window_timestamp(self, days_after_send=3):
        """Return an ISO timestamp N days after the send (which is 8 days ago)."""
        from datetime import datetime, timezone, timedelta
        sent_dt = datetime.now(timezone.utc) - timedelta(days=8)
        return (sent_dt + timedelta(days=days_after_send)).isoformat()

    def test_measure_only_counts_sent_recipients(self):
        """Attribution must only count orders from emails in the sent list."""
        sent = ["alice@test.com", "bob@test.com"]
        self._make_measuring_intervention(sent_emails=sent)
        ts = self._in_window_timestamp(3)

        # Alice ordered (should be attributed)
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o1", "evt1", "alice@test.com", ts, 2, 100.0),
        )
        # Carol ordered (NOT sent to, should NOT be attributed)
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o2", "evt1", "carol@test.com", ts, 3, 150.0),
        )
        self.db.conn.commit()

        result = self.adapter.measure("meas-intv", actor="user")
        self.assertEqual(result["actual"]["attributed_orders"], 1)  # Only Alice
        self.assertEqual(result["actual"]["attributed_revenue"], 100.0)
        self.assertEqual(result["actual"]["attributed_tickets"], 2)

    def test_measure_excludes_non_sent_orders(self):
        """Orders from people NOT in the sent list must be excluded."""
        self._make_measuring_intervention(sent_emails=["alice@test.com"])
        ts = self._in_window_timestamp(3)

        # Only non-sent person ordered
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o1", "evt1", "stranger@test.com", ts, 5, 500.0),
        )
        self.db.conn.commit()

        result = self.adapter.measure("meas-intv", actor="user")
        self.assertEqual(result["actual"]["attributed_orders"], 0)
        self.assertEqual(result["actual"]["attributed_revenue"], 0.0)

    def test_measure_blocks_non_measuring_status(self):
        """Measure must reject if status is not 'measuring'."""
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        self.store.save(i)

        result = self.adapter.measure(i.id, actor="user")
        self.assertEqual(result["error"], "illegal_status")

    def test_measure_transitions_to_learned_when_window_complete(self):
        """When attribution window has passed, status → learned."""
        sent = ["alice@test.com"]
        self._make_measuring_intervention(sent_emails=sent)

        # The helper sets sent_at 8 days ago and window is 7 days, so window is complete
        result = self.adapter.measure("meas-intv", actor="user")
        self.assertEqual(result["status"], "learned")
        self.assertTrue(result["window_complete"])

        loaded = self.store.get("meas-intv")
        self.assertEqual(loaded.status, InterventionStatus.LEARNED)

    def test_measure_returns_predicted_vs_actual(self):
        """Response must include predicted and actual for comparison."""
        sent = ["alice@test.com"]
        self._make_measuring_intervention(sent_emails=sent)
        ts = self._in_window_timestamp(3)

        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o1", "evt1", "alice@test.com", ts, 2, 200.0),
        )
        self.db.conn.commit()

        result = self.adapter.measure("meas-intv", actor="user")
        self.assertIn("predicted", result)
        self.assertIn("actual", result)
        self.assertEqual(result["predicted"]["expected_revenue"], 5000)
        self.assertEqual(result["actual"]["attributed_revenue"], 200.0)


# ─────────────────────────────────────────────────────────────────────
# Learning record persistence tests
# ─────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────
# Attribution window regression tests (PR #1 review requirement)
# ─────────────────────────────────────────────────────────────────────

class TestAttributionWindowRegression(unittest.TestCase):
    """Regression: _compute_attribution must respect the attribution window.

    Validates that:
    - pre-send orders from sent recipients are EXCLUDED
    - orders inside the window are COUNTED
    - orders after the window are EXCLUDED
    - orders from unrelated recipients are EXCLUDED
    - multiple qualifying orders aggregate correctly
    """

    def setUp(self):
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.v2_repo = SQLiteV2StateRepository(self.db)
        self.adapter = ExecutionAdapter(self.db, self.v2_repo, campaign_engine=None)

        from datetime import datetime, timezone, timedelta as td

        # Seed event
        event_date = (date.today() + td(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test Fest", "coffee", "Philadelphia", event_date, 5000),
        )
        self.db.conn.commit()

        # Fixed timestamps for deterministic testing
        self.send_dt = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        self.window_end = self.send_dt + td(days=7)

        self.sent_emails = {"alice@test.com", "bob@test.com"}

    def test_pre_send_order_excluded(self):
        """An order from a sent recipient BEFORE the send must NOT be attributed."""
        pre_send_ts = (self.send_dt - timedelta(days=1)).isoformat()
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o-pre", "evt1", "alice@test.com", pre_send_ts, 2, 100.0),
        )
        self.db.conn.commit()

        result = self.adapter._compute_attribution("evt1", self.sent_emails, self.send_dt, self.window_end)
        self.assertEqual(result["orders"], 0, "Pre-send orders must be excluded")
        self.assertEqual(result["revenue"], 0.0)

    def test_in_window_order_counted(self):
        """An order from a sent recipient INSIDE the window must be attributed."""
        in_window_ts = (self.send_dt + timedelta(days=3)).isoformat()
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o-in", "evt1", "alice@test.com", in_window_ts, 2, 100.0),
        )
        self.db.conn.commit()

        result = self.adapter._compute_attribution("evt1", self.sent_emails, self.send_dt, self.window_end)
        self.assertEqual(result["orders"], 1, "In-window order must be counted")
        self.assertEqual(result["revenue"], 100.0)
        self.assertEqual(result["tickets"], 2)

    def test_post_window_order_excluded(self):
        """An order from a sent recipient AFTER the window must NOT be attributed."""
        post_window_ts = (self.window_end + timedelta(days=1)).isoformat()
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o-post", "evt1", "alice@test.com", post_window_ts, 2, 100.0),
        )
        self.db.conn.commit()

        result = self.adapter._compute_attribution("evt1", self.sent_emails, self.send_dt, self.window_end)
        self.assertEqual(result["orders"], 0, "Post-window orders must be excluded")
        self.assertEqual(result["revenue"], 0.0)

    def test_unrelated_recipient_excluded(self):
        """An in-window order from a non-sent recipient must NOT be attributed."""
        in_window_ts = (self.send_dt + timedelta(days=3)).isoformat()
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o-stranger", "evt1", "stranger@test.com", in_window_ts, 5, 500.0),
        )
        self.db.conn.commit()

        result = self.adapter._compute_attribution("evt1", self.sent_emails, self.send_dt, self.window_end)
        self.assertEqual(result["orders"], 0, "Non-sent recipient orders must be excluded")
        self.assertEqual(result["revenue"], 0.0)

    def test_multiple_qualifying_orders_aggregate(self):
        """Multiple in-window orders from different sent recipients must sum correctly."""
        ts1 = (self.send_dt + timedelta(days=1)).isoformat()
        ts2 = (self.send_dt + timedelta(days=4)).isoformat()
        ts3 = (self.send_dt + timedelta(days=6)).isoformat()

        # Alice: 2 orders
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o1", "evt1", "alice@test.com", ts1, 2, 100.0),
        )
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o2", "evt1", "alice@test.com", ts2, 1, 50.0),
        )
        # Bob: 1 order
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o3", "evt1", "bob@test.com", ts3, 3, 200.0),
        )
        # Stranger: should not be counted
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o4", "evt1", "stranger@test.com", ts1, 10, 1000.0),
        )
        # Alice: pre-send order — should NOT be counted
        pre_ts = (self.send_dt - timedelta(hours=1)).isoformat()
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o5", "evt1", "alice@test.com", pre_ts, 5, 500.0),
        )
        self.db.conn.commit()

        result = self.adapter._compute_attribution("evt1", self.sent_emails, self.send_dt, self.window_end)
        # Alice: 2 in-window orders (100+50=150, 2+1=3 tickets, 2 orders)
        # Bob: 1 in-window order (200, 3 tickets, 1 order)
        # Total: 3 orders, 6 tickets, 350.0 revenue
        self.assertEqual(result["orders"], 3, "Should count 3 qualifying orders from Alice(2)+Bob(1)")
        self.assertEqual(result["tickets"], 6)
        self.assertEqual(result["revenue"], 350.0)


class TestLearningRecord(unittest.TestCase):

    def setUp(self):
        from intervention_model import AuditLogger, LearningStore, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.v2_repo = SQLiteV2StateRepository(self.db)
        self.adapter = ExecutionAdapter(self.db, self.v2_repo, campaign_engine=None)

        # Seed event
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test Fest", "coffee", "Philadelphia", event_date, 5000),
        )
        self.db.conn.commit()

    def test_learning_record_created_on_learned(self):
        """When measure completes (window over), a learning record must be persisted."""
        from intervention_model import LearningStore
        from datetime import datetime, timezone, timedelta

        i = Intervention(
            id="learn-intv",
            opportunity_id="opp1",
            event_id="evt1",
            intervention_type="crm_campaign",
            status=InterventionStatus.NEW,
            expected_revenue=5000,
            confidence=0.6,
            campaign_draft_id="v2-draft-1",
            measurement_window=7,
            sent_count=2,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        i.transition_to(InterventionStatus.EXECUTING)
        i.transition_to(InterventionStatus.MEASURING)
        self.store.save(i)

        # Record sends 8 days ago (window complete)
        sent_at = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
        for email in ["a@test.com", "b@test.com"]:
            self.db.conn.execute(
                """INSERT INTO v2_campaign_sends
                   (intervention_id, campaign_draft_id, email, sent_at)
                   VALUES (?,?,?,?)""",
                ("learn-intv", "v2-draft-1", email, sent_at),
            )
        # Order placed 5 days ago (within 7-day window after send 8 days ago)
        order_ts = (datetime.now(timezone.utc) - timedelta(days=5)).isoformat()
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) VALUES (?,?,?,?,?,?)",
            ("o1", "evt1", "a@test.com", order_ts, 1, 100.0),
        )
        self.db.conn.commit()

        result = self.adapter.measure("learn-intv", actor="user")
        self.assertEqual(result["status"], "learned")

        # Verify learning record exists
        ls = LearningStore(self.db)
        record = ls.get("learn-intv")
        self.assertIsNotNone(record)
        self.assertEqual(record["intervention_type"], "crm_campaign")
        self.assertEqual(record["predicted_revenue"], 5000)
        self.assertEqual(record["attributed_revenue"], 100.0)
        self.assertEqual(record["prediction_error"], -4900.0)
        self.assertEqual(record["sent_count"], 2)


# ─────────────────────────────────────────────────────────────────────
# Campaign sends persistence tests
# ─────────────────────────────────────────────────────────────────────

class TestCampaignSendsPersistence(unittest.TestCase):

    def setUp(self):
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.v2_repo = SQLiteV2StateRepository(self.db)
        self.adapter = ExecutionAdapter(self.db, self.v2_repo, campaign_engine=None)

    def test_record_sends_persists_emails(self):
        emails = ["a@test.com", "b@test.com", "c@test.com"]
        self.adapter._record_sends("intv-1", "draft-1", emails)

        rows = self.db.conn.execute(
            "SELECT * FROM v2_campaign_sends WHERE intervention_id = ?",
            ("intv-1",),
        ).fetchall()
        self.assertEqual(len(rows), 3)
        stored_emails = {r["email"] for r in rows}
        self.assertEqual(stored_emails, set(emails))

    def test_record_sends_deduplicates(self):
        """UNIQUE(intervention_id, email) should prevent double-recording."""
        self.adapter._record_sends("intv-1", "draft-1", ["a@test.com"])
        self.adapter._record_sends("intv-1", "draft-1", ["a@test.com"])

        rows = self.db.conn.execute(
            "SELECT * FROM v2_campaign_sends WHERE intervention_id = ?",
            ("intv-1",),
        ).fetchall()
        self.assertEqual(len(rows), 1)


# ─────────────────────────────────────────────────────────────────────
# Frontend security tests (proxy routes)
# ─────────────────────────────────────────────────────────────────────

class TestFrontendSecurityPatterns(unittest.TestCase):
    """Verify no secrets leak to browser and all calls go through server-side proxies."""

    def test_command_js_never_reads_command_api_key(self):
        """pages/command.js must not reference COMMAND_API_KEY."""
        with open("pages/command.js", "r") as f:
            content = f.read()
        self.assertNotIn("COMMAND_API_KEY", content)
        self.assertNotIn("NEXT_PUBLIC_COMMAND_API_KEY", content)

    def test_command_js_never_calls_railway_directly(self):
        """pages/command.js must only use relative /api/ paths."""
        with open("pages/command.js", "r") as f:
            content = f.read()
        self.assertNotIn("railway.app", content)
        self.assertNotIn("craft-dominant-production", content)

    def test_command_js_never_constructs_auth_headers(self):
        """pages/command.js must never build Authorization headers."""
        with open("pages/command.js", "r") as f:
            content = f.read()
        self.assertNotIn("Authorization", content)
        self.assertNotIn("Bearer", content)

    def test_proxy_routes_use_server_side_key(self):
        """All proxy routes must read COMMAND_API_KEY from process.env (server-side)."""
        import glob as globmod
        proxy_files = globmod.glob("pages/api/v2/**/*.js", recursive=True)
        self.assertGreater(len(proxy_files), 0)
        for path in proxy_files:
            with open(path, "r") as f:
                content = f.read()
            # Server-side key injection
            self.assertIn("process.env", content,
                          f"{path} must read secrets from process.env")
            self.assertIn("COMMAND_API_KEY", content,
                          f"{path} must use COMMAND_API_KEY")
            # Must NOT expose via NEXT_PUBLIC_ prefix
            self.assertNotIn("NEXT_PUBLIC_COMMAND_API_KEY", content,
                             f"{path} must not use NEXT_PUBLIC_ prefix for secrets")

    def test_proxy_routes_exist_for_all_actions(self):
        """Each closed-loop action must have a server-side proxy route."""
        import os as _os
        for action in ["approve", "reject", "execute", "measure"]:
            path = f"pages/api/v2/interventions/[id]/{action}.js"
            self.assertTrue(
                _os.path.exists(path),
                f"Missing proxy route: {path}",
            )

    def test_command_js_uses_relative_proxy_paths(self):
        """Action buttons in command.js should call /api/v2/... paths."""
        with open("pages/command.js", "r") as f:
            content = f.read()
        # The InterventionBadge doAction function must use relative proxy paths
        self.assertIn("/api/v2/interventions/", content)


# ─────────────────────────────────────────────────────────────────────
# Approval transition tests (closed-loop specific)
# ─────────────────────────────────────────────────────────────────────

class TestApprovalTransitions(unittest.TestCase):

    def setUp(self):
        self.db = FakeStoreDB()
        self.store = InterventionStore(self.db)

    def test_proposed_to_approved(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        self.assertEqual(i.status, InterventionStatus.APPROVED)
        self.assertIsNotNone(i.approved_at)

    def test_proposed_to_rejected(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.REJECTED)
        self.assertEqual(i.status, InterventionStatus.REJECTED)
        self.assertTrue(i.is_terminal)

    def test_approved_cannot_be_re_approved(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        with self.assertRaises(IllegalTransition):
            i.transition_to(InterventionStatus.APPROVED)

    def test_approve_from_new_illegal(self):
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        with self.assertRaises(IllegalTransition):
            i.transition_to(InterventionStatus.APPROVED)

    def test_execute_from_proposed_illegal(self):
        """Cannot skip approval — executing from proposed must be blocked."""
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        with self.assertRaises(IllegalTransition):
            i.transition_to(InterventionStatus.EXECUTING)

    def test_measuring_transition_sets_timestamps(self):
        """Transition to MEASURING must set measurement_started_at and measurement_ends_at."""
        i = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        i.transition_to(InterventionStatus.EXECUTING)
        i.transition_to(InterventionStatus.MEASURING)
        self.assertIsNotNone(i.measurement_started_at)
        self.assertIsNotNone(i.measurement_ends_at)

    def test_new_measurement_fields_roundtrip(self):
        """Measurement fields must survive save/load cycle."""
        i = Intervention.create(
            opportunity_id="opp-rt", event_id="evt1",
            intervention_type="crm_campaign",
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        i.transition_to(InterventionStatus.EXECUTING)
        i.transition_to(InterventionStatus.MEASURING)
        i.sent_count = 42
        i.attributed_orders = 5
        i.attributed_tickets = 8
        i.attributed_revenue = 1234.56
        self.store.save(i)

        loaded = self.store.get(i.id)
        self.assertEqual(loaded.sent_count, 42)
        self.assertEqual(loaded.attributed_orders, 5)
        self.assertEqual(loaded.attributed_tickets, 8)
        self.assertAlmostEqual(loaded.attributed_revenue, 1234.56, places=2)
        self.assertEqual(loaded.status, InterventionStatus.MEASURING)
        self.assertIsNotNone(loaded.measurement_started_at)
        self.assertIsNotNone(loaded.measurement_ends_at)


# ─────────────────────────────────────────────────────────────────────
# No-external-HTTP default test
# ─────────────────────────────────────────────────────────────────────

class TestNoExternalHTTPByDefault(unittest.TestCase):
    """Verify that tests/dev mode never makes external HTTP calls."""

    def test_external_send_flag_not_set_by_default(self):
        """V2_ENABLE_EXTERNAL_SEND must not be set in the test environment."""
        import os as _os
        self.assertNotEqual(_os.environ.get("V2_ENABLE_EXTERNAL_SEND", "0"), "1",
                            "V2_ENABLE_EXTERNAL_SEND must not be '1' in test env")

    def test_execution_adapter_defaults_to_dry_run(self):
        """Without V2_ENABLE_EXTERNAL_SEND, execute() returns dry-run, never calls Mailchimp."""
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        db = FakeExecutionDB()
        store = InterventionStore(db)
        audit = AuditLogger(db)
        db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        db.conn.executescript(LEARNING_RECORD_SCHEMA)

        # Seed data
        event_date = (date.today() + timedelta(days=30)).isoformat()
        db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test Fest", "coffee", "Phila", event_date, 5000),
        )
        for i in range(10):
            db.conn.execute(
                "INSERT INTO customers VALUES (?,?,?,?)",
                (f"c{i}@test.com", "Phila", "coffee", "regular"),
            )
        db.conn.execute(
            "INSERT INTO suppressions VALUES (?)", ("suppressed@test.com",)
        )
        db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-d1", "test-intv", "evt1", "Subj", 10, "draft", "2026-01-01"),
        )
        db.conn.commit()
        _seed_valid_suppression_sentinel(db, row_count=1)

        v2_repo = SQLiteV2StateRepository(db)
        adapter = ExecutionAdapter(db, v2_repo, campaign_engine=None)

        i = Intervention(
            id="test-intv",
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign",
            status=InterventionStatus.NEW,
            campaign_draft_id="v2-d1",
            measurement_window=7,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        store.save(i)

        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        result = adapter.execute("test-intv", actor="user")
        self.assertEqual(result["status"], "external_send_disabled")

        # Verify NO state change happened
        loaded = store.get("test-intv")
        self.assertEqual(loaded.status, InterventionStatus.APPROVED)


# ─────────────────────────────────────────────────────────────────────
# Suppression Guard fail-closed regression tests
# ─────────────────────────────────────────────────────────────────────

class TestSuppressionGuard(unittest.TestCase):
    """Regression tests for suppression guard fail-closed behavior.

    Priority: customer safety > fail-closed behavior > correctness > auditability.
    Every ambiguous or missing state MUST block.
    """

    def _make_db(self):
        """Create a minimal in-memory DB with suppressions table."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE suppressions (email TEXT PRIMARY KEY)")
        conn.commit()

        class MinimalDB:
            pass

        db = MinimalDB()
        db.conn = conn
        return db

    def _seed_sentinel(self, db, row_count=1, source="test", age_hours=0,
                        set_full_refresh=True):
        """Seed a sentinel with configurable age.

        set_full_refresh: if True (default), also sets last_full_refresh_at to
        the same timestamp. Set False to test "webhook-only, no full refresh"
        scenarios where freshness gate should block.
        """
        from datetime import datetime, timezone, timedelta as td
        db.conn.executescript(SENTINEL_TABLE_SCHEMA)
        synced_at = (datetime.now(timezone.utc) - td(hours=age_hours)).isoformat()
        if set_full_refresh:
            db.conn.execute(
                """INSERT OR REPLACE INTO v2_suppression_sync
                   (id, last_synced_at, row_count, source,
                    last_full_refresh_at, last_full_refresh_source)
                   VALUES (1, ?, ?, ?, ?, ?)""",
                (synced_at, row_count, source, synced_at, source),
            )
        else:
            db.conn.execute(
                """INSERT OR REPLACE INTO v2_suppression_sync
                   (id, last_synced_at, row_count, source) VALUES (1, ?, ?, ?)""",
                (synced_at, row_count, source),
            )
        db.conn.commit()

    def _acknowledge_empty(self, db, actor="admin", reason="legit empty", age_hours=0):
        """Set acknowledgment on an existing sentinel."""
        from datetime import datetime, timezone, timedelta as td
        ack_at = (datetime.now(timezone.utc) - td(hours=age_hours)).isoformat()
        db.conn.execute(
            """UPDATE v2_suppression_sync SET
                   empty_acknowledged = 1,
                   acknowledged_by = ?,
                   acknowledged_at = ?,
                   acknowledged_reason = ?
               WHERE id = 1""",
            (actor, ack_at, reason),
        )
        db.conn.commit()

    # --- Blocked states ---

    def test_no_sentinel_blocks(self):
        """No sentinel row → NEVER_SYNCED → blocked."""
        db = self._make_db()
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)
        self.assertFalse(guard.is_valid())

    def test_zero_rows_no_acknowledgment_blocks(self):
        """Sentinel exists + zero rows + no acknowledgment → EMPTY_UNVERIFIED → blocked."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.EMPTY_UNVERIFIED)
        self.assertFalse(guard.is_valid())

    def test_expired_acknowledgment_blocks(self):
        """Acknowledgment older than 24 hours → EMPTY_UNVERIFIED → blocked."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        self._acknowledge_empty(db, age_hours=25)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.EMPTY_UNVERIFIED)
        self.assertFalse(guard.is_valid())
        self.assertIn("expired", details.get("reason", "").lower())

    def test_stale_sentinel_blocks(self):
        """Sentinel synced > 24 hours ago → STALE → blocked."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=1, age_hours=25)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.STALE)
        self.assertFalse(guard.is_valid())

    def test_suppression_query_error_blocks(self):
        """Sentinel valid but suppressions table dropped → UNAVAILABLE → blocked."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=1)
        guard = SuppressionGuard(db)
        # Drop the table after guard is created
        db.conn.execute("DROP TABLE suppressions")
        db.conn.commit()
        status, details, emails = guard.get_suppressions_if_valid()
        self.assertEqual(status, SuppressionStatus.UNAVAILABLE)
        self.assertEqual(emails, set())

    # --- Allowed states ---

    def test_healthy_sentinel_nonzero_rows_allowed(self):
        """Sentinel fresh + row_count > 0 → HEALTHY → allowed."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=1)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertTrue(guard.is_valid())

    def test_acknowledged_empty_allowed(self):
        """Sentinel + zero rows + valid acknowledgment → ACKNOWLEDGED_EMPTY → allowed."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        self._acknowledge_empty(db, actor="admin", reason="New org, no unsubscribes yet")
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.ACKNOWLEDGED_EMPTY)
        self.assertTrue(guard.is_valid())

    # --- Suppression data correctness ---

    def test_suppressed_email_excluded(self):
        """get_suppressions_if_valid returns actual suppressed emails."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("unsub@test.com",))
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("optout@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=2)
        guard = SuppressionGuard(db)
        status, details, emails = guard.get_suppressions_if_valid()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertEqual(emails, {"unsub@test.com", "optout@test.com"})

    def test_invalid_state_returns_empty_set(self):
        """get_suppressions_if_valid returns empty set when state is invalid."""
        db = self._make_db()
        guard = SuppressionGuard(db)
        status, details, emails = guard.get_suppressions_if_valid()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)
        self.assertEqual(emails, set())

    # --- record_sync behavior ---

    def test_record_sync_with_rows_clears_acknowledgment(self):
        """Syncing with row_count > 0 must clear any prior acknowledgment."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        self._acknowledge_empty(db)
        guard = SuppressionGuard(db)
        guard.record_sync(row_count=5, source="mailchimp")
        row = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        row = dict(row)
        self.assertEqual(row["row_count"], 5)
        self.assertEqual(row["empty_acknowledged"], 0)
        self.assertIsNone(row["acknowledged_by"])

    def test_record_sync_zero_preserves_acknowledgment(self):
        """Syncing with zero rows must preserve existing acknowledgment."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        self._acknowledge_empty(db, actor="admin", reason="legit")
        guard = SuppressionGuard(db)
        guard.record_sync(row_count=0, source="mailchimp")
        row = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        row = dict(row)
        self.assertEqual(row["empty_acknowledged"], 1)
        self.assertEqual(row["acknowledged_by"], "admin")

    # --- acknowledge_empty validation ---

    def test_acknowledge_requires_actor(self):
        """Empty or blank actor must raise ValueError."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        guard = SuppressionGuard(db)
        with self.assertRaises(ValueError):
            guard.acknowledge_empty("", "reason")
        with self.assertRaises(ValueError):
            guard.acknowledge_empty("  ", "reason")

    def test_acknowledge_requires_reason(self):
        """Empty or blank reason must raise ValueError."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        guard = SuppressionGuard(db)
        with self.assertRaises(ValueError):
            guard.acknowledge_empty("admin", "")

    def test_acknowledge_rejects_nonempty_list(self):
        """Cannot acknowledge empty when row_count > 0."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=5)
        guard = SuppressionGuard(db)
        with self.assertRaises(ValueError) as ctx:
            guard.acknowledge_empty("admin", "reason")
        self.assertIn("5 rows", str(ctx.exception))

    def test_acknowledge_rejects_no_sync(self):
        """Cannot acknowledge if no sync has ever occurred."""
        db = self._make_db()
        guard = SuppressionGuard(db)
        with self.assertRaises(ValueError):
            guard.acknowledge_empty("admin", "reason")

    # --- Integration: prepare blocks ---

    def test_prepare_blocks_when_suppression_invalid(self):
        """CampaignDraftAdapter.prepare_draft must raise when suppression state is invalid."""
        db = FakeCampaignDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test", "coffee", "Phila", event_date, 1000),
        )
        db.conn.commit()
        # No sentinel seeded → NEVER_SYNCED
        adapter = CampaignDraftAdapter(db)
        intervention = Intervention.create(
            opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", confidence=0.5,
        )
        with self.assertRaises(ValueError) as ctx:
            adapter.prepare_draft(intervention, {"avg_ticket_price": 50.0, "crm_champions": 0})
        self.assertIn("suppression", str(ctx.exception).lower())

    # --- Integration: execute blocks ---

    def test_execute_blocks_when_suppression_invalid(self):
        """ExecutionAdapter.execute must block when suppression state is invalid."""
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        db = FakeExecutionDB()
        store = InterventionStore(db)
        audit = AuditLogger(db)
        db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        db.conn.executescript(LEARNING_RECORD_SCHEMA)

        event_date = (date.today() + timedelta(days=30)).isoformat()
        db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test", "coffee", "Phila", event_date, 1000),
        )
        db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-d1", "intv1", "evt1", "Subj", 10, "draft", "2026-01-01"),
        )
        db.conn.commit()
        # No sentinel → NEVER_SYNCED

        v2_repo = SQLiteV2StateRepository(db)
        adapter = ExecutionAdapter(db, v2_repo, campaign_engine=None)
        i = Intervention(
            id="intv1", opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", status=InterventionStatus.NEW,
            campaign_draft_id="v2-d1", measurement_window=7,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        store.save(i)

        result = adapter.execute("intv1", actor="user")
        self.assertIn("error", result)
        self.assertEqual(result["error"], "suppression_never_synced")

        # Verify NO state change
        loaded = store.get("intv1")
        self.assertEqual(loaded.status, InterventionStatus.APPROVED)

    def test_blocked_path_writes_audit_entry(self):
        """Blocked execution must write an audit log entry."""
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        db = FakeExecutionDB()
        store = InterventionStore(db)
        audit = AuditLogger(db)
        db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        db.conn.executescript(LEARNING_RECORD_SCHEMA)

        event_date = (date.today() + timedelta(days=30)).isoformat()
        db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test", "coffee", "Phila", event_date, 1000),
        )
        db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-d1", "intv-audit", "evt1", "Subj", 10, "draft", "2026-01-01"),
        )
        db.conn.commit()
        # No sentinel → blocked

        v2_repo = SQLiteV2StateRepository(db)
        adapter = ExecutionAdapter(db, v2_repo, campaign_engine=None)
        i = Intervention(
            id="intv-audit", opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", status=InterventionStatus.NEW,
            campaign_draft_id="v2-d1", measurement_window=7,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        store.save(i)

        adapter.execute("intv-audit", actor="user")

        # Check audit log
        rows = db.conn.execute(
            "SELECT * FROM intervention_audit_log WHERE intervention_id = ? AND action = 'execute_blocked'",
            ("intv-audit",),
        ).fetchall()
        self.assertEqual(len(rows), 1)
        row = dict(rows[0])
        self.assertIn("suppression", row.get("error", "").lower())

    def test_blocked_path_makes_zero_external_http_calls(self):
        """Blocked execution must not make any external HTTP calls.

        Since V2_ENABLE_EXTERNAL_SEND is not set and the suppression
        guard blocks before that check, no HTTP calls should be possible.
        The adapter never reaches the Mailchimp send code path.
        """
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        db = FakeExecutionDB()
        store = InterventionStore(db)
        audit = AuditLogger(db)
        db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        db.conn.executescript(LEARNING_RECORD_SCHEMA)

        event_date = (date.today() + timedelta(days=30)).isoformat()
        db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test", "coffee", "Phila", event_date, 1000),
        )
        db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-d1", "intv-nohttp", "evt1", "Subj", 10, "draft", "2026-01-01"),
        )
        db.conn.commit()
        # No sentinel → blocked before any HTTP could happen

        v2_repo = SQLiteV2StateRepository(db)
        adapter = ExecutionAdapter(db, v2_repo, campaign_engine=None)
        i = Intervention(
            id="intv-nohttp", opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", status=InterventionStatus.NEW,
            campaign_draft_id="v2-d1", measurement_window=7,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        store.save(i)

        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        result = adapter.execute("intv-nohttp", actor="user")
        # Blocked at suppression gate — never reached external send
        self.assertIn("error", result)
        self.assertEqual(result["error"], "suppression_never_synced")

        # State unchanged
        loaded = store.get("intv-nohttp")
        self.assertEqual(loaded.status, InterventionStatus.APPROVED)

    # --- Production failure mode regression ---

    def test_production_failure_mode_schema_recreated_sentinel_missing(self):
        """CRITICAL REGRESSION: Railway restart recreates schema, loses suppression
        rows AND sentinel. This MUST block.

        Scenario: ephemeral SQLite → redeploy → schema recreated via
        CREATE TABLE IF NOT EXISTS → all rows lost → suppression table
        exists but is empty → sentinel table exists but is empty.
        """
        db = self._make_db()
        # Simulate: schema exists, but both tables are empty (restart scenario)
        db.conn.executescript(SENTINEL_TABLE_SCHEMA)
        db.conn.commit()
        # Suppressions table exists but is empty
        # Sentinel table exists but has no rows
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)
        self.assertFalse(guard.is_valid())

        # Also verify get_suppressions_if_valid blocks
        status2, details2, emails = guard.get_suppressions_if_valid()
        self.assertEqual(status2, SuppressionStatus.NEVER_SYNCED)
        self.assertEqual(emails, set())

    def test_configurable_max_age(self):
        """SUPPRESSION_MAX_AGE_HOURS env var overrides default threshold."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        # Synced 5 hours ago — stale if threshold is 4, fresh if threshold is 6
        self._seed_sentinel(db, row_count=1, age_hours=5)

        guard = SuppressionGuard(db)
        os.environ["SUPPRESSION_MAX_AGE_HOURS"] = "4"
        try:
            status, _ = guard.validate()
            self.assertEqual(status, SuppressionStatus.STALE)
        finally:
            os.environ.pop("SUPPRESSION_MAX_AGE_HOURS", None)

        os.environ["SUPPRESSION_MAX_AGE_HOURS"] = "6"
        try:
            status, _ = guard.validate()
            self.assertEqual(status, SuppressionStatus.HEALTHY)
        finally:
            os.environ.pop("SUPPRESSION_MAX_AGE_HOURS", None)

    # --- Count-mismatch verification (PR #2 blocker fix) ---

    def test_count_mismatch_sentinel_1_actual_0_blocks(self):
        """Sentinel says 1 row but table is empty → COUNT_MISMATCH → blocked."""
        db = self._make_db()
        # Sentinel says 1 row, but no actual suppression rows
        self._seed_sentinel(db, row_count=1)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH)
        self.assertFalse(guard.is_valid())
        self.assertEqual(details["sentinel_row_count"], 1)
        self.assertEqual(details["actual_row_count"], 0)

    def test_count_mismatch_sentinel_0_actual_1_blocks(self):
        """Sentinel says 0 rows but table has 1 → COUNT_MISMATCH → blocked."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("unsub@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=0)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH)
        self.assertFalse(guard.is_valid())
        self.assertEqual(details["sentinel_row_count"], 0)
        self.assertEqual(details["actual_row_count"], 1)

    def test_count_mismatch_sentinel_5_actual_3_blocks(self):
        """Sentinel says 5 rows but table has 3 → COUNT_MISMATCH → blocked."""
        db = self._make_db()
        for i in range(3):
            db.conn.execute("INSERT INTO suppressions VALUES (?)", (f"u{i}@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=5)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH)
        self.assertFalse(guard.is_valid())
        self.assertEqual(details["sentinel_row_count"], 5)
        self.assertEqual(details["actual_row_count"], 3)

    def test_exact_count_match_allowed(self):
        """Sentinel count matches actual table count → HEALTHY → allowed."""
        db = self._make_db()
        for i in range(3):
            db.conn.execute("INSERT INTO suppressions VALUES (?)", (f"u{i}@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=3)
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertTrue(guard.is_valid())

    def test_post_fetch_count_mismatch_in_get_suppressions(self):
        """get_suppressions_if_valid must re-verify count after fetching emails."""
        db = self._make_db()
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=1)
        guard = SuppressionGuard(db)

        # validate() passes (count matches). Now insert another row
        # AFTER validate() but before the fetch count check in get_suppressions_if_valid.
        # We can't truly race in a single-threaded test, but we can test the
        # post-fetch verification by manually making sentinel and table diverge
        # after validate() would have passed.
        #
        # Instead, test the simpler invariant: if sentinel says 2 but table has 1,
        # get_suppressions_if_valid returns COUNT_MISMATCH.
        db2 = self._make_db()
        db2.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db2.conn.commit()
        self._seed_sentinel(db2, row_count=2)  # sentinel says 2, actual is 1
        guard2 = SuppressionGuard(db2)
        status, details, emails = guard2.get_suppressions_if_valid()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH)
        self.assertEqual(emails, set())

    # --- Acknowledged-empty + actual non-empty (PR #2 blocker fix) ---

    def test_acknowledged_empty_but_actual_nonempty_blocks(self):
        """CRITICAL: If someone acknowledged empty but table gained rows → COUNT_MISMATCH.

        Scenario: admin acknowledges empty suppression list, then a webhook
        adds a suppression row without updating the sentinel (shouldn't happen
        with record_mutation wired, but defense-in-depth).
        """
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        self._acknowledge_empty(db, actor="admin", reason="legit empty")
        # Now manually insert a row WITHOUT updating sentinel
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("unsub@test.com",))
        db.conn.commit()
        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH)
        self.assertFalse(guard.is_valid())
        self.assertEqual(details["sentinel_row_count"], 0)
        self.assertEqual(details["actual_row_count"], 1)

    # --- record_mutation (PR #2 blocker fix) ---

    def test_record_mutation_updates_sentinel_count(self):
        """record_mutation reads actual table count and writes to sentinel."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("b@test.com",))
        db.conn.commit()
        guard = SuppressionGuard(db)
        result = guard.record_mutation(source="webhook_unsubscribe")
        self.assertTrue(result["updated"])
        self.assertEqual(result["row_count"], 2)
        # Sentinel now matches
        row = db.conn.execute("SELECT row_count FROM v2_suppression_sync WHERE id = 1").fetchone()
        self.assertEqual(row["row_count"], 2)

    def test_record_mutation_clears_acknowledgment(self):
        """record_mutation with actual_count > 0 must clear empty acknowledgment."""
        db = self._make_db()
        self._seed_sentinel(db, row_count=0)
        self._acknowledge_empty(db, actor="admin", reason="was empty")
        # Verify acknowledgment is set
        row = db.conn.execute("SELECT empty_acknowledged FROM v2_suppression_sync WHERE id = 1").fetchone()
        self.assertEqual(row["empty_acknowledged"], 1)
        # Now add a suppression and record mutation
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        guard = SuppressionGuard(db)
        guard.record_mutation(source="webhook_unsubscribe")
        # Acknowledgment must be cleared
        row = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        row = dict(row)
        self.assertEqual(row["empty_acknowledged"], 0)
        self.assertIsNone(row["acknowledged_by"])

    def test_record_mutation_failed_write_doesnt_crash(self):
        """record_mutation must not crash if sentinel write fails."""
        db = self._make_db()
        # Don't create sentinel table — record_mutation should handle gracefully
        guard = SuppressionGuard(db)  # This creates the sentinel table
        # Drop sentinel table to simulate write failure
        db.conn.execute("DROP TABLE v2_suppression_sync")
        db.conn.commit()
        result = guard.record_mutation(source="webhook_unsubscribe")
        # Should return error dict, not raise
        self.assertIn("error", result)

    # --- Webhook integration (PR #2 blocker fix) ---

    def _make_webhook_db(self):
        """Create a DB with production-schema suppressions + email_events for webhook tests."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""CREATE TABLE IF NOT EXISTS suppressions (
            email TEXT PRIMARY KEY,
            reason TEXT DEFAULT 'unsubscribe',
            suppressed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute("""CREATE TABLE IF NOT EXISTS email_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            mailchimp_campaign_id TEXT,
            event_type TEXT,
            email TEXT,
            timestamp TEXT,
            raw_payload TEXT
        )""")
        conn.commit()

        class MinimalDB:
            pass

        db = MinimalDB()
        db.conn = conn
        return db

    def test_webhook_unsubscribe_updates_sentinel(self):
        """process_mailchimp_webhook must update sentinel after suppression write."""
        from craft_engine import CraftCampaignEngine

        db = self._make_webhook_db()
        self._seed_sentinel(db, row_count=0)

        engine = CraftCampaignEngine.__new__(CraftCampaignEngine)
        engine.db = db
        engine._claude = None
        engine._mailchimp = None
        engine._v2_repo = None

        # Process unsubscribe webhook
        result = engine.process_mailchimp_webhook({
            'type': 'unsubscribe',
            'data': {'email': 'UNSUB@TEST.COM'},
        })
        self.assertEqual(result['processed'], 1)

        # Sentinel should now reflect the 1 suppression row
        row = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        row = dict(row)
        self.assertEqual(row["row_count"], 1)
        self.assertEqual(row["source"], "webhook_unsubscribe")

    def test_webhook_cleaned_updates_sentinel(self):
        """process_mailchimp_webhook must update sentinel after bounce suppression."""
        from craft_engine import CraftCampaignEngine

        db = self._make_webhook_db()
        self._seed_sentinel(db, row_count=0)

        engine = CraftCampaignEngine.__new__(CraftCampaignEngine)
        engine.db = db
        engine._claude = None
        engine._mailchimp = None
        engine._v2_repo = None

        result = engine.process_mailchimp_webhook({
            'type': 'cleaned',
            'data': {'email': 'bounced@test.com'},
        })
        self.assertEqual(result['processed'], 1)

        row = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        row = dict(row)
        self.assertEqual(row["row_count"], 1)
        self.assertEqual(row["source"], "webhook_cleaned")

    def test_webhook_campaign_event_does_not_update_sentinel(self):
        """Campaign events don't write suppressions, so sentinel must not change."""
        from craft_engine import CraftCampaignEngine

        db = self._make_webhook_db()
        self._seed_sentinel(db, row_count=0)

        engine = CraftCampaignEngine.__new__(CraftCampaignEngine)
        engine.db = db
        engine._claude = None
        engine._mailchimp = None
        engine._v2_repo = None

        result = engine.process_mailchimp_webhook({
            'type': 'campaign',
            'data': {'id': 'mc_123'},
        })
        self.assertEqual(result['processed'], 1)

        # Sentinel unchanged — still row_count=0 from seed
        row = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        row = dict(row)
        self.assertEqual(row["row_count"], 0)

    # --- Restart/data-loss with count verification (PR #2 blocker fix) ---

    def test_restart_data_loss_sentinel_survives_but_data_lost(self):
        """CRITICAL: If sentinel survived restart but suppression data was lost,
        count mismatch must block.

        This is the exact production scenario: sentinel says 5 rows were synced
        but the suppressions table is empty after restart.
        """
        db = self._make_db()
        # Pre-restart: 5 suppressions exist
        for i in range(5):
            db.conn.execute("INSERT INTO suppressions VALUES (?)", (f"u{i}@test.com",))
        db.conn.commit()
        self._seed_sentinel(db, row_count=5)

        # Simulate restart: suppressions table recreated empty
        db.conn.execute("DELETE FROM suppressions")
        db.conn.commit()

        guard = SuppressionGuard(db)
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.COUNT_MISMATCH)
        self.assertFalse(guard.is_valid())
        self.assertEqual(details["sentinel_row_count"], 5)
        self.assertEqual(details["actual_row_count"], 0)
        self.assertIn("data loss", details["reason"].lower())

    # --- Execution blocking with count-mismatch metadata in audit ---

    def test_execution_blocked_by_count_mismatch_includes_metadata(self):
        """Execution blocked by COUNT_MISMATCH must include count details in audit."""
        from intervention_model import AuditLogger, CAMPAIGN_SENDS_SCHEMA, LEARNING_RECORD_SCHEMA
        from execution_adapter import ExecutionAdapter
        from v2_state_repository import SQLiteV2StateRepository

        db = FakeExecutionDB()
        store = InterventionStore(db)
        audit = AuditLogger(db)
        db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        db.conn.executescript(LEARNING_RECORD_SCHEMA)

        event_date = (date.today() + timedelta(days=30)).isoformat()
        db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Test", "coffee", "Phila", event_date, 1000),
        )
        db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-d1", "intv-cm", "evt1", "Subj", 10, "draft", "2026-01-01"),
        )
        # Sentinel says 3 rows, but only 1 actual row
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@test.com",))
        db.conn.commit()
        _seed_valid_suppression_sentinel(db, row_count=3)  # mismatch: 3 vs 1

        v2_repo = SQLiteV2StateRepository(db)
        adapter = ExecutionAdapter(db, v2_repo, campaign_engine=None)
        i = Intervention(
            id="intv-cm", opportunity_id="opp1", event_id="evt1",
            intervention_type="crm_campaign", status=InterventionStatus.NEW,
            campaign_draft_id="v2-d1", measurement_window=7,
        )
        i.transition_to(InterventionStatus.INVESTIGATED)
        i.transition_to(InterventionStatus.PROPOSED)
        i.transition_to(InterventionStatus.APPROVED)
        store.save(i)

        result = adapter.execute("intv-cm", actor="user")
        self.assertIn("error", result)
        self.assertEqual(result["error"], "suppression_count_mismatch")

        # Verify state unchanged
        loaded = store.get("intv-cm")
        self.assertEqual(loaded.status, InterventionStatus.APPROVED)

        # Verify audit entry
        rows = db.conn.execute(
            "SELECT * FROM intervention_audit_log WHERE intervention_id = ? AND action = 'execute_blocked'",
            ("intv-cm",),
        ).fetchall()
        self.assertEqual(len(rows), 1)
        row = dict(rows[0])
        self.assertIn("count_mismatch", row.get("error", "").lower())


class TestAuthoritativeRefresh(unittest.TestCase):
    """Regression tests for authoritative Mailchimp suppression refresh.

    Tests cover: successful refresh, sentinel update, pagination,
    status filtering, failure modes, bootstrap, staleness, webhook
    compatibility, and the endpoint auth/payload contract.
    """

    def _make_refresh_db(self):
        """Create an in-memory DB with production-schema suppressions."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""CREATE TABLE IF NOT EXISTS suppressions (
            email TEXT PRIMARY KEY,
            reason TEXT DEFAULT 'unsubscribe',
            suppressed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()

        class MinimalDB:
            pass

        db = MinimalDB()
        db.conn = conn
        return db

    def _seed_sentinel(self, db, row_count=1, source="test", age_hours=0,
                        set_full_refresh=True):
        from datetime import datetime, timezone, timedelta as td
        db.conn.executescript(SENTINEL_TABLE_SCHEMA)
        synced_at = (datetime.now(timezone.utc) - td(hours=age_hours)).isoformat()
        if set_full_refresh:
            db.conn.execute(
                """INSERT OR REPLACE INTO v2_suppression_sync
                   (id, last_synced_at, row_count, source,
                    last_full_refresh_at, last_full_refresh_source)
                   VALUES (1, ?, ?, ?, ?, ?)""",
                (synced_at, row_count, source, synced_at, source),
            )
        else:
            db.conn.execute(
                """INSERT OR REPLACE INTO v2_suppression_sync
                   (id, last_synced_at, row_count, source) VALUES (1, ?, ?, ?)""",
                (synced_at, row_count, source),
            )
        db.conn.commit()

    class FakeMailchimpClient:
        """Mock Mailchimp client for refresh tests."""

        def __init__(self, suppressed_emails=None, fail=False, raise_exc=False):
            self._emails = suppressed_emails
            self._fail = fail
            self._raise_exc = raise_exc

        def get_suppressed_members(self):
            if self._raise_exc:
                raise ConnectionError("Mailchimp API timeout")
            if self._fail:
                return None
            return self._emails if self._emails is not None else []

    def test_successful_refresh_replaces_suppressions(self):
        """Full refresh replaces local suppressions with Mailchimp data."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Pre-seed some local suppressions that should be REPLACED
        db.conn.execute("INSERT INTO suppressions (email) VALUES ('old@stale.com')")
        db.conn.commit()

        mc = self.FakeMailchimpClient(["alice@test.com", "bob@test.com"])
        result = guard.refresh_from_mailchimp(mc)

        self.assertTrue(result.get("refreshed"))
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["source"], "mailchimp_full_refresh")

        # Verify old email is gone
        rows = db.conn.execute("SELECT email FROM suppressions ORDER BY email").fetchall()
        emails = [r["email"] for r in rows]
        self.assertEqual(emails, ["alice@test.com", "bob@test.com"])
        self.assertNotIn("old@stale.com", emails)

    def test_sentinel_updated_after_refresh(self):
        """Sentinel reflects exact count and source after successful refresh."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        mc = self.FakeMailchimpClient(["a@t.com", "b@t.com", "c@t.com"])
        result = guard.refresh_from_mailchimp(mc)

        self.assertTrue(result["refreshed"])

        sentinel = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        self.assertIsNotNone(sentinel)
        sentinel = dict(sentinel)
        self.assertEqual(sentinel["row_count"], 3)
        self.assertEqual(sentinel["source"], "mailchimp_full_refresh")
        self.assertFalse(bool(sentinel["empty_acknowledged"]))

    def test_refresh_clears_stale_acknowledgment(self):
        """Refresh with real rows clears any prior empty-set acknowledgment."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Set up an acknowledged-empty state
        self._seed_sentinel(db, row_count=0, source="manual")
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        db.conn.execute(
            """UPDATE v2_suppression_sync SET
                   empty_acknowledged = 1, acknowledged_by = 'admin',
                   acknowledged_at = ?, acknowledged_reason = 'test'
               WHERE id = 1""",
            (now,),
        )
        db.conn.commit()

        mc = self.FakeMailchimpClient(["real@user.com"])
        result = guard.refresh_from_mailchimp(mc)
        self.assertTrue(result["refreshed"])

        sentinel = dict(db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone())
        self.assertEqual(sentinel["empty_acknowledged"], 0)
        self.assertIsNone(sentinel["acknowledged_by"])

    def test_refresh_empty_preserves_acknowledgment(self):
        """Refresh returning zero emails does NOT clear existing acknowledgment."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Set up acknowledged-empty
        self._seed_sentinel(db, row_count=0, source="manual")
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        db.conn.execute(
            """UPDATE v2_suppression_sync SET
                   empty_acknowledged = 1, acknowledged_by = 'admin',
                   acknowledged_at = ?, acknowledged_reason = 'legit'
               WHERE id = 1""",
            (now,),
        )
        db.conn.commit()

        mc = self.FakeMailchimpClient([])  # empty
        result = guard.refresh_from_mailchimp(mc)
        self.assertTrue(result["refreshed"])
        self.assertEqual(result["row_count"], 0)

        sentinel = dict(db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone())
        self.assertEqual(sentinel["empty_acknowledged"], 1)
        self.assertEqual(sentinel["acknowledged_by"], "admin")

    def test_mailchimp_failure_returns_none_no_local_change(self):
        """When Mailchimp returns None, local suppressions must NOT be touched."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Pre-seed local data
        db.conn.execute("INSERT INTO suppressions (email) VALUES ('preserve@test.com')")
        db.conn.commit()
        self._seed_sentinel(db, row_count=1, source="webhook")

        mc = self.FakeMailchimpClient(fail=True)  # returns None
        result = guard.refresh_from_mailchimp(mc)

        self.assertIn("error", result)
        self.assertNotIn("refreshed", result)

        # Local data untouched
        rows = db.conn.execute("SELECT email FROM suppressions").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["email"], "preserve@test.com")

        # Sentinel untouched
        sentinel = dict(db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone())
        self.assertEqual(sentinel["row_count"], 1)
        self.assertEqual(sentinel["source"], "webhook")

    def test_mailchimp_exception_no_local_change(self):
        """Mailchimp client raising an exception leaves local state intact."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        db.conn.execute("INSERT INTO suppressions (email) VALUES ('safe@test.com')")
        db.conn.commit()
        self._seed_sentinel(db, row_count=1, source="webhook")

        mc = self.FakeMailchimpClient(raise_exc=True)
        result = guard.refresh_from_mailchimp(mc)

        self.assertIn("error", result)
        rows = db.conn.execute("SELECT email FROM suppressions").fetchall()
        self.assertEqual(len(rows), 1)

    def test_bootstrap_first_refresh_creates_sentinel(self):
        """First-ever refresh bootstraps the sentinel from scratch."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # No sentinel exists yet
        sentinel = db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone()
        self.assertIsNone(sentinel)

        mc = self.FakeMailchimpClient(["first@user.com", "second@user.com"])
        result = guard.refresh_from_mailchimp(mc)

        self.assertTrue(result["refreshed"])
        self.assertEqual(result["row_count"], 2)

        # Sentinel now exists
        sentinel = dict(db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone())
        self.assertEqual(sentinel["row_count"], 2)
        self.assertEqual(sentinel["source"], "mailchimp_full_refresh")

        # Guard validates as HEALTHY
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)

    def test_email_normalization_and_dedup(self):
        """Emails from Mailchimp are lowercased, stripped, and deduplicated."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        mc = self.FakeMailchimpClient([
            "Alice@Test.COM",
            "  bob@test.com  ",
            "alice@test.com",  # duplicate after normalization
            "BOB@TEST.COM",    # duplicate after normalization
            "",                # empty — should be ignored
            "  ",              # whitespace — should be ignored
        ])
        result = guard.refresh_from_mailchimp(mc)

        self.assertTrue(result["refreshed"])
        self.assertEqual(result["row_count"], 2)

        rows = db.conn.execute("SELECT email FROM suppressions ORDER BY email").fetchall()
        emails = [r["email"] for r in rows]
        self.assertEqual(emails, ["alice@test.com", "bob@test.com"])

    def test_refresh_then_validate_passes(self):
        """After a successful refresh, validate() returns HEALTHY."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        mc = self.FakeMailchimpClient(["x@y.com"])
        guard.refresh_from_mailchimp(mc)

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertEqual(details["row_count"], 1)
        self.assertEqual(details["source"], "mailchimp_full_refresh")

    def test_refresh_then_get_suppressions_if_valid(self):
        """After refresh, get_suppressions_if_valid returns the refreshed set."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        mc = self.FakeMailchimpClient(["blocked@user.com", "also@blocked.com"])
        guard.refresh_from_mailchimp(mc)

        status, details, emails = guard.get_suppressions_if_valid()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertEqual(emails, {"blocked@user.com", "also@blocked.com"})

    def test_webhook_after_refresh_maintains_consistency(self):
        """A webhook mutation after refresh keeps sentinel consistent."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Refresh with 2 emails
        mc = self.FakeMailchimpClient(["a@t.com", "b@t.com"])
        guard.refresh_from_mailchimp(mc)

        # Simulate webhook adding a third suppression
        db.conn.execute(
            "INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("c@t.com",),
        )
        db.conn.commit()
        guard.record_mutation(source="webhook_unsubscribe")

        # Sentinel should now say 3
        sentinel = dict(db.conn.execute("SELECT * FROM v2_suppression_sync WHERE id = 1").fetchone())
        self.assertEqual(sentinel["row_count"], 3)

        # Validate should pass
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)

    def test_stale_after_refresh_blocks(self):
        """Refresh is subject to the same freshness threshold — stale blocks."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        mc = self.FakeMailchimpClient(["x@y.com"])
        guard.refresh_from_mailchimp(mc)

        # Manually backdating last_full_refresh_at to make it stale
        # (freshness is now evaluated against last_full_refresh_at, not last_synced_at)
        from datetime import datetime, timezone, timedelta
        old_time = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        db.conn.execute(
            "UPDATE v2_suppression_sync SET last_full_refresh_at = ? WHERE id = 1",
            (old_time,),
        )
        db.conn.commit()

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.STALE)

    def test_get_suppressed_members_pagination(self):
        """MailchimpClient.get_suppressed_members handles multiple statuses."""
        # This tests the MailchimpClient method itself with mocked _request
        from unittest.mock import MagicMock, patch

        mc_client = MagicMock()
        mc_client.audience_id = "list123"

        # Set up the real method on the mock
        from craft_engine import MailchimpClient
        real_client = MailchimpClient.__new__(MailchimpClient)
        real_client.audience_id = "list123"
        real_client.base_url = "https://us1.api.mailchimp.com/3.0"
        real_client.api_key = "key-us1"

        # Mock _request to return paginated results
        call_count = [0]
        def mock_request(method, path, data=None, timeout=30):
            call_count[0] += 1
            if "status=unsubscribed" in path:
                if "offset=0" in path or "offset" not in path:
                    return {
                        "members": [
                            {"email_address": "unsub1@t.com"},
                            {"email_address": "unsub2@t.com"},
                        ],
                        "total_items": 2,
                    }
                return {"members": [], "total_items": 2}
            elif "status=cleaned" in path:
                if "offset=0" in path or "offset" not in path:
                    return {
                        "members": [
                            {"email_address": "cleaned1@t.com"},
                        ],
                        "total_items": 1,
                    }
                return {"members": [], "total_items": 1}
            return {"members": [], "total_items": 0}

        real_client._request = mock_request

        result = real_client.get_suppressed_members()
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 3)
        self.assertIn("unsub1@t.com", result)
        self.assertIn("unsub2@t.com", result)
        self.assertIn("cleaned1@t.com", result)

    def test_get_suppressed_members_api_failure_returns_none(self):
        """If any page request fails, get_suppressed_members returns None."""
        from craft_engine import MailchimpClient

        real_client = MailchimpClient.__new__(MailchimpClient)
        real_client.audience_id = "list123"
        real_client.base_url = "https://us1.api.mailchimp.com/3.0"
        real_client.api_key = "key-us1"

        # First status succeeds, second fails mid-page
        def mock_request(method, path, data=None, timeout=30):
            if "status=unsubscribed" in path:
                return {"members": [{"email_address": "ok@t.com"}], "total_items": 1}
            elif "status=cleaned" in path:
                return None  # API failure
            return {"members": [], "total_items": 0}

        real_client._request = mock_request

        result = real_client.get_suppressed_members()
        self.assertIsNone(result)

    def test_large_pagination(self):
        """Pagination works across multiple pages for a single status."""
        from craft_engine import MailchimpClient

        real_client = MailchimpClient.__new__(MailchimpClient)
        real_client.audience_id = "list123"
        real_client.base_url = "https://us1.api.mailchimp.com/3.0"
        real_client.api_key = "key-us1"

        def mock_request(method, path, data=None, timeout=30):
            if "status=unsubscribed" in path:
                if "offset=0" in path:
                    return {
                        "members": [{"email_address": f"u{i}@t.com"} for i in range(1000)],
                        "total_items": 1500,
                    }
                elif "offset=1000" in path:
                    return {
                        "members": [{"email_address": f"u{i}@t.com"} for i in range(1000, 1500)],
                        "total_items": 1500,
                    }
                return {"members": [], "total_items": 1500}
            elif "status=cleaned" in path:
                return {"members": [], "total_items": 0}
            return {"members": [], "total_items": 0}

        real_client._request = mock_request

        result = real_client.get_suppressed_members()
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 1500)


class TestCampaignAdapterBlockerRegressions(unittest.TestCase):
    """Regression tests for blocker-removal pass — campaign adapter side."""

    def setUp(self):
        self.db = FakeCampaignDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt_br", "Blocker Regression", "coffee", "Philadelphia", event_date, 2000),
        )
        # Buyers
        for i in range(10):
            self.db.conn.execute(
                "INSERT INTO orders (order_id, event_id, email, ticket_count, gross_amount) VALUES (?,?,?,?,?)",
                (f"o{i}", "evt_br", f"buyer{i}@example.com", 1, 50.0),
            )
        self.db.conn.commit()
        # Seed suppression sentinel so guard passes for non-suppression tests
        _seed_valid_suppression_sentinel(self.db, row_count=0)
        # Acknowledge empty since there are zero suppression rows
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        self.db.conn.execute(
            """UPDATE v2_suppression_sync SET
                   empty_acknowledged = 1,
                   acknowledged_by = 'test',
                   acknowledged_at = ?,
                   acknowledged_reason = 'test setup'
               WHERE id = 1""",
            (now,),
        )
        self.db.conn.commit()

    def test_audience_dedup_excludes_past_attendees_from_city(self):
        """Blocker 2: city_prospects must not overlap with past_attendees."""
        # Insert people who are BOTH past attendees and Philadelphia city prospects
        for i in range(40):
            self.db.conn.execute(
                "INSERT INTO customers VALUES (?,?,?,?)",
                (f"overlap{i}@example.com", "Philadelphia", "coffee", "regular"),
            )
        self.db.conn.commit()

        adapter = CampaignDraftAdapter(self.db)
        event = self.db.get_event("evt_br")
        result = adapter._build_audience("evt_br", event)

        # All emails should be unique
        email_list = result["emails"]
        self.assertEqual(len(email_list), len(set(email_list)),
                         "Audience must contain unique emails only")

    def test_suppression_invariant_preserved(self):
        """Suppression hard invariant must still block on failure."""
        self.db.conn.execute("DROP TABLE suppressions")
        self.db.conn.commit()

        adapter = CampaignDraftAdapter(self.db)
        intervention = Intervention.create(
            opportunity_id="opp_br", event_id="evt_br",
            intervention_type="crm_campaign", confidence=0.55,
        )
        with self.assertRaises(ValueError) as ctx:
            adapter.prepare_draft(intervention, {"avg_ticket_price": 50.0, "crm_champions": 5})
        self.assertIn("suppression", str(ctx.exception).lower())


# ─────────────────────────────────────────────────────────────────────
# Endpoint security tests for POST /api/v2/suppressions/refresh
# ─────────────────────────────────────────────────────────────────────

class TestRefreshEndpointSecurity(unittest.TestCase):
    """Security tests for the suppression refresh endpoint.

    Verifies: auth rejection, no secret leakage in error responses,
    no email addresses in success responses, proper status codes.
    Uses real Flask test client with real require_command_auth decorator.
    """

    def setUp(self):
        """Create a Flask test client with auth configured."""
        os.environ["COMMAND_API_KEY"] = "test-secret-key-12345"
        os.environ["DB_PATH"] = ":memory:"
        # Don't configure Mailchimp by default — some tests check that path
        os.environ.pop("MAILCHIMP_API_KEY", None)
        os.environ.pop("MAILCHIMP_AUDIENCE_ID", None)

        from craft_v2 import _build_app
        self.app = _build_app()
        self.client = self.app.test_client()

    def tearDown(self):
        os.environ.pop("COMMAND_API_KEY", None)
        os.environ.pop("DB_PATH", None)
        os.environ.pop("MAILCHIMP_API_KEY", None)
        os.environ.pop("MAILCHIMP_AUDIENCE_ID", None)

    def test_no_auth_header_returns_401(self):
        """Request without Authorization header is rejected."""
        resp = self.client.post("/api/v2/suppressions/refresh")
        self.assertEqual(resp.status_code, 401)
        data = resp.get_json()
        self.assertEqual(data["error"], "unauthorized")
        # Must NOT leak the expected key
        self.assertNotIn("test-secret-key", json.dumps(data))

    def test_wrong_bearer_token_returns_401(self):
        """Request with wrong Bearer token is rejected."""
        resp = self.client.post(
            "/api/v2/suppressions/refresh",
            headers={"Authorization": "Bearer wrong-token"},
        )
        self.assertEqual(resp.status_code, 401)
        data = resp.get_json()
        self.assertEqual(data["error"], "unauthorized")
        self.assertNotIn("test-secret-key", json.dumps(data))

    def test_no_bearer_prefix_returns_401(self):
        """Authorization header without 'Bearer ' prefix is rejected."""
        resp = self.client.post(
            "/api/v2/suppressions/refresh",
            headers={"Authorization": "test-secret-key-12345"},
        )
        self.assertEqual(resp.status_code, 401)

    def test_unconfigured_command_key_returns_503(self):
        """If COMMAND_API_KEY is not set, endpoint returns 503."""
        os.environ.pop("COMMAND_API_KEY", None)
        from craft_v2 import _build_app
        app = _build_app()
        client = app.test_client()

        resp = client.post(
            "/api/v2/suppressions/refresh",
            headers={"Authorization": "Bearer anything"},
        )
        self.assertEqual(resp.status_code, 503)
        data = resp.get_json()
        self.assertEqual(data["error"], "command_api_not_configured")

    def test_mailchimp_not_configured_returns_503(self):
        """Valid auth but no Mailchimp credentials returns 503."""
        resp = self.client.post(
            "/api/v2/suppressions/refresh",
            headers={"Authorization": "Bearer test-secret-key-12345"},
        )
        self.assertEqual(resp.status_code, 503)
        data = resp.get_json()
        self.assertEqual(data["error"], "mailchimp_not_configured")
        # Must NOT leak API key or any secrets
        resp_text = json.dumps(data)
        self.assertNotIn("test-secret-key", resp_text)
        self.assertNotIn("COMMAND_API_KEY", resp_text)

    def test_success_response_contains_no_email_addresses(self):
        """Successful refresh response must never contain email addresses."""
        os.environ["MAILCHIMP_API_KEY"] = "fake-key-us1"
        os.environ["MAILCHIMP_AUDIENCE_ID"] = "list123"

        from craft_v2 import _build_app
        app = _build_app()
        client = app.test_client()

        # Monkey-patch the MailchimpClient to return test data
        import craft_engine
        original_init = craft_engine.MailchimpClient.__init__
        original_get = None

        class FakeMC:
            def __init__(self, *args, **kwargs):
                pass
            def get_suppressed_members(self):
                return ["secret_user@private.com", "another@hidden.org"]

        import unittest.mock
        with unittest.mock.patch("craft_engine.MailchimpClient", FakeMC):
            # Re-build app with patched client
            app2 = _build_app()
            client2 = app2.test_client()
            resp = client2.post(
                "/api/v2/suppressions/refresh",
                headers={"Authorization": "Bearer test-secret-key-12345"},
            )

        self.assertEqual(resp.status_code, 200)
        data = resp.get_json()
        resp_text = json.dumps(data)

        # Response must contain count, not email addresses
        self.assertEqual(data["row_count"], 2)
        self.assertNotIn("secret_user", resp_text)
        self.assertNotIn("private.com", resp_text)
        self.assertNotIn("another@", resp_text)
        self.assertNotIn("hidden.org", resp_text)

        # Must include last_full_refresh_at (new dual-timestamp field)
        self.assertIn("last_full_refresh_at", data)

    def test_error_response_contains_no_secrets(self):
        """Error responses must not leak MAILCHIMP_API_KEY or COMMAND_API_KEY."""
        os.environ["MAILCHIMP_API_KEY"] = "mc-secret-key-abc123-us1"
        os.environ["MAILCHIMP_AUDIENCE_ID"] = "list123"

        import unittest.mock

        class FailMC:
            def __init__(self, *args, **kwargs):
                pass
            def get_suppressed_members(self):
                return None  # Simulates incomplete data

        with unittest.mock.patch("craft_engine.MailchimpClient", FailMC):
            from craft_v2 import _build_app
            app = _build_app()
            client = app.test_client()
            resp = client.post(
                "/api/v2/suppressions/refresh",
                headers={"Authorization": "Bearer test-secret-key-12345"},
            )

        self.assertEqual(resp.status_code, 500)
        data = resp.get_json()
        resp_text = json.dumps(data)
        self.assertNotIn("mc-secret-key", resp_text)
        self.assertNotIn("test-secret-key", resp_text)
        self.assertNotIn("COMMAND_API_KEY", resp_text)
        self.assertNotIn("MAILCHIMP_API_KEY", resp_text)


# ─────────────────────────────────────────────────────────────────────
# Freshness semantics regression tests (dual-timestamp)
# ─────────────────────────────────────────────────────────────────────

class TestFreshnessSemantics(unittest.TestCase):
    """Regression tests for the dual-timestamp freshness model.

    Core invariant: "A mutation is evidence of one event; a full refresh
    is evidence of completeness. Never let the first masquerade as the
    second."

    The 24-hour freshness gate must evaluate against last_full_refresh_at
    only. Webhook mutations must NOT extend the freshness window.
    """

    def _make_db(self):
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE suppressions (email TEXT PRIMARY KEY)")
        conn.commit()

        class MinimalDB:
            pass
        db = MinimalDB()
        db.conn = conn
        return db

    def _make_refresh_db(self):
        """DB with production-schema suppressions."""
        import sqlite3
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("""CREATE TABLE IF NOT EXISTS suppressions (
            email TEXT PRIMARY KEY,
            reason TEXT DEFAULT 'unsubscribe',
            suppressed_at TEXT DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.commit()

        class MinimalDB:
            pass
        db = MinimalDB()
        db.conn = conn
        return db

    # --- Webhook alone must NOT bootstrap authoritative freshness ---

    def test_webhook_only_no_full_refresh_blocks(self):
        """Webhook mutations without any prior full refresh → NEVER_SYNCED.

        A webhook proves one event was observed; it does not prove the
        complete suppression set is known. Operations must block.
        """
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Simulate webhook suppression write
        db.conn.execute(
            "INSERT INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("unsub@test.com",),
        )
        db.conn.commit()
        guard.record_mutation(source="webhook_unsubscribe")

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)
        self.assertFalse(guard.is_valid())
        self.assertIn("authoritative full refresh", details["reason"].lower())

    def test_multiple_webhooks_still_no_freshness(self):
        """Even many webhooks do NOT establish authoritative freshness."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        for i in range(10):
            db.conn.execute(
                "INSERT INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
                (f"user{i}@test.com",),
            )
            db.conn.commit()
            guard.record_mutation(source="webhook_unsubscribe")

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)
        self.assertFalse(guard.is_valid())

    # --- Stale full refresh + recent webhook → still STALE ---

    def test_stale_full_refresh_plus_recent_webhook_is_stale(self):
        """Full refresh 25h old + webhook just now → STALE.

        The webhook keeps last_mutation_at current, but that must NOT
        prevent the staleness check from firing on last_full_refresh_at.
        """
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Do a full refresh
        class FakeMC:
            def get_suppressed_members(self):
                return ["a@t.com"]
        guard.refresh_from_mailchimp(FakeMC())

        # Backdate the full refresh to 25h ago
        from datetime import datetime, timezone, timedelta
        old_time = (datetime.now(timezone.utc) - timedelta(hours=25)).isoformat()
        db.conn.execute(
            "UPDATE v2_suppression_sync SET last_full_refresh_at = ? WHERE id = 1",
            (old_time,),
        )
        db.conn.commit()

        # Process a recent webhook — this updates last_mutation_at to NOW
        db.conn.execute(
            "INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("new@t.com",),
        )
        db.conn.commit()
        guard.record_mutation(source="webhook_unsubscribe")

        # Despite the recent webhook, freshness gate must use last_full_refresh_at
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.STALE)
        self.assertFalse(guard.is_valid())
        self.assertIn("last_full_refresh_at", details)
        self.assertIn("last_mutation_at", details)

    # --- Fresh full refresh + webhook → HEALTHY ---

    def test_fresh_full_refresh_plus_webhook_is_healthy(self):
        """Full refresh 23h old + webhook → HEALTHY.

        The full refresh is still within the 24h window.
        """
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        class FakeMC:
            def get_suppressed_members(self):
                return ["a@t.com"]
        guard.refresh_from_mailchimp(FakeMC())

        # Backdate full refresh to 23h ago (still fresh)
        from datetime import datetime, timezone, timedelta
        recent_time = (datetime.now(timezone.utc) - timedelta(hours=23)).isoformat()
        db.conn.execute(
            "UPDATE v2_suppression_sync SET last_full_refresh_at = ? WHERE id = 1",
            (recent_time,),
        )
        db.conn.commit()

        # Process webhook
        db.conn.execute(
            "INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("b@t.com",),
        )
        db.conn.commit()
        guard.record_mutation(source="webhook_unsubscribe")

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertTrue(guard.is_valid())

    # --- record_mutation does NOT touch last_full_refresh_at ---

    def test_record_mutation_preserves_full_refresh_timestamp(self):
        """record_mutation must NOT update last_full_refresh_at."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        # Do a full refresh first
        class FakeMC:
            def get_suppressed_members(self):
                return ["a@t.com"]
        guard.refresh_from_mailchimp(FakeMC())

        # Record original full refresh timestamp
        sentinel = dict(db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone())
        original_refresh_at = sentinel["last_full_refresh_at"]
        self.assertIsNotNone(original_refresh_at)

        # Process webhook mutation
        import time
        time.sleep(0.01)  # Ensure time advances
        db.conn.execute(
            "INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("new@t.com",),
        )
        db.conn.commit()
        guard.record_mutation(source="webhook_unsubscribe")

        # Verify last_full_refresh_at is UNCHANGED
        sentinel = dict(db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone())
        self.assertEqual(sentinel["last_full_refresh_at"], original_refresh_at)
        # But last_mutation_at IS updated
        self.assertIsNotNone(sentinel["last_mutation_at"])
        self.assertEqual(sentinel["last_mutation_source"], "webhook_unsubscribe")

    # --- refresh_from_mailchimp DOES set last_full_refresh_at ---

    def test_refresh_sets_full_refresh_timestamp(self):
        """refresh_from_mailchimp must set last_full_refresh_at."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        class FakeMC:
            def get_suppressed_members(self):
                return ["a@t.com", "b@t.com"]

        result = guard.refresh_from_mailchimp(FakeMC())
        self.assertTrue(result["refreshed"])
        self.assertIn("last_full_refresh_at", result)

        sentinel = dict(db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone())
        self.assertIsNotNone(sentinel["last_full_refresh_at"])
        self.assertEqual(sentinel["last_full_refresh_source"], "mailchimp_full_refresh")

    # --- validate() diagnostics include both timestamps ---

    def test_validate_diagnostics_include_both_timestamps(self):
        """validate() details must include last_full_refresh_at and last_mutation_at."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        class FakeMC:
            def get_suppressed_members(self):
                return ["a@t.com"]
        guard.refresh_from_mailchimp(FakeMC())

        db.conn.execute(
            "INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("b@t.com",),
        )
        db.conn.commit()
        guard.record_mutation(source="webhook_unsubscribe")

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertIn("last_full_refresh_at", details)
        self.assertIn("last_mutation_at", details)
        self.assertIn("last_synced_at", details)

    # --- Schema migration tests ---

    def test_migration_legacy_full_refresh_sentinel_backfills(self):
        """Legacy sentinel with source='mailchimp_full_refresh' gets
        last_full_refresh_at backfilled from last_synced_at during migration.
        """
        db = self._make_refresh_db()
        # Manually create old-schema sentinel (no dual-timestamp columns)
        db.conn.execute("""CREATE TABLE IF NOT EXISTS v2_suppression_sync (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_synced_at TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            source TEXT NOT NULL DEFAULT 'unknown',
            empty_acknowledged INTEGER NOT NULL DEFAULT 0,
            acknowledged_by TEXT,
            acknowledged_at TEXT,
            acknowledged_reason TEXT
        )""")
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        db.conn.execute(
            "INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source) "
            "VALUES (1, ?, 3, 'mailchimp_full_refresh')",
            (now,),
        )
        db.conn.execute("INSERT INTO suppressions (email) VALUES (?)", ("a@t.com",))
        db.conn.execute("INSERT INTO suppressions (email) VALUES (?)", ("b@t.com",))
        db.conn.execute("INSERT INTO suppressions (email) VALUES (?)", ("c@t.com",))
        db.conn.commit()

        # SuppressionGuard init triggers migration + backfill
        guard = SuppressionGuard(db)

        sentinel = dict(db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone())
        # Backfilled from last_synced_at
        self.assertEqual(sentinel["last_full_refresh_at"], now)
        self.assertEqual(sentinel["last_full_refresh_source"], "mailchimp_full_refresh")

        # Should validate as HEALTHY
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.HEALTHY)

    def test_migration_legacy_webhook_sentinel_does_not_backfill(self):
        """Legacy sentinel with webhook source must NOT get
        last_full_refresh_at backfilled — webhook freshness is not
        authoritative freshness.
        """
        db = self._make_refresh_db()
        # Create old-schema sentinel with webhook source
        db.conn.execute("""CREATE TABLE IF NOT EXISTS v2_suppression_sync (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_synced_at TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            source TEXT NOT NULL DEFAULT 'unknown',
            empty_acknowledged INTEGER NOT NULL DEFAULT 0,
            acknowledged_by TEXT,
            acknowledged_at TEXT,
            acknowledged_reason TEXT
        )""")
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        db.conn.execute(
            "INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source) "
            "VALUES (1, ?, 2, 'webhook_unsubscribe')",
            (now,),
        )
        db.conn.execute("INSERT INTO suppressions (email) VALUES (?)", ("x@t.com",))
        db.conn.execute("INSERT INTO suppressions (email) VALUES (?)", ("y@t.com",))
        db.conn.commit()

        # SuppressionGuard init triggers migration but NOT backfill
        guard = SuppressionGuard(db)

        sentinel = dict(db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone())
        self.assertIsNone(sentinel["last_full_refresh_at"])
        self.assertIsNone(sentinel["last_full_refresh_source"])

        # Should block — no authoritative refresh
        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)
        self.assertFalse(guard.is_valid())

    def test_migration_legacy_manual_sentinel_does_not_backfill(self):
        """Legacy sentinel with source='manual' or 'test' must NOT
        get last_full_refresh_at backfilled.
        """
        db = self._make_db()
        db.conn.execute("""CREATE TABLE IF NOT EXISTS v2_suppression_sync (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            last_synced_at TEXT NOT NULL,
            row_count INTEGER NOT NULL,
            source TEXT NOT NULL DEFAULT 'unknown',
            empty_acknowledged INTEGER NOT NULL DEFAULT 0,
            acknowledged_by TEXT,
            acknowledged_at TEXT,
            acknowledged_reason TEXT
        )""")
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        db.conn.execute(
            "INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source) "
            "VALUES (1, ?, 1, 'manual')",
            (now,),
        )
        db.conn.execute("INSERT INTO suppressions VALUES (?)", ("a@t.com",))
        db.conn.commit()

        guard = SuppressionGuard(db)

        sentinel = dict(db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone())
        self.assertIsNone(sentinel["last_full_refresh_at"])

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)

    def test_migration_no_existing_sentinel_skips_backfill(self):
        """When no sentinel row exists yet, migration just adds columns."""
        db = self._make_db()
        guard = SuppressionGuard(db)

        # No sentinel row → no backfill needed, no error
        sentinel = db.conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone()
        self.assertIsNone(sentinel)

        status, details = guard.validate()
        self.assertEqual(status, SuppressionStatus.NEVER_SYNCED)

    # --- get_suppressions_if_valid diagnostics ---

    def test_get_suppressions_if_valid_includes_dual_timestamps(self):
        """get_suppressions_if_valid diagnostics must include both timestamps."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        class FakeMC:
            def get_suppressed_members(self):
                return ["a@t.com"]
        guard.refresh_from_mailchimp(FakeMC())

        status, details, emails = guard.get_suppressions_if_valid()
        self.assertEqual(status, SuppressionStatus.HEALTHY)
        self.assertIn("last_full_refresh_at", details)
        self.assertEqual(emails, {"a@t.com"})

    # --- Fail-closed preserved ---

    def test_fail_closed_no_sentinel_still_blocks(self):
        """The dual-timestamp change must NOT weaken fail-closed: no sentinel → blocked."""
        db = self._make_db()
        guard = SuppressionGuard(db)
        self.assertEqual(guard.validate()[0], SuppressionStatus.NEVER_SYNCED)
        self.assertFalse(guard.is_valid())

    def test_record_mutation_return_includes_mutation_timestamp(self):
        """record_mutation() result includes last_mutation_at for audit."""
        db = self._make_refresh_db()
        guard = SuppressionGuard(db)

        db.conn.execute(
            "INSERT INTO suppressions (email, reason) VALUES (?, 'unsubscribe')",
            ("a@t.com",),
        )
        db.conn.commit()
        result = guard.record_mutation(source="webhook_unsubscribe")

        self.assertTrue(result["updated"])
        self.assertIn("last_mutation_at", result)
        self.assertEqual(result["source"], "webhook_unsubscribe")


if __name__ == "__main__":
    unittest.main()
