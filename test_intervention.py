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

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)

        # Create campaign_sends and learning_records tables
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)

        self.adapter = ExecutionAdapter(self.db, self.store, self.audit, campaign_engine=None)

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
        # Seed campaign draft
        self.db.conn.execute(
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-draft-1", "test-intv", "evt1", "Test subject", 50, "draft", "2026-01-01"),
        )
        self.db.conn.commit()

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

    # Gate 3: suppression list
    def test_execute_blocks_suppression_unavailable(self):
        """Execute must block if suppression list is unreadable."""
        i = self._make_approved_intervention()
        self.db.conn.execute("DROP TABLE suppressions")
        self.db.conn.commit()

        result = self.adapter.execute(i.id, actor="user")
        self.assertEqual(result["error"], "suppression_unavailable")

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

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.adapter = ExecutionAdapter(self.db, self.store, self.audit, campaign_engine=None)

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

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.adapter = ExecutionAdapter(self.db, self.store, self.audit, campaign_engine=None)

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

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.adapter = ExecutionAdapter(self.db, self.store, self.audit, campaign_engine=None)

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

        self.db = FakeExecutionDB()
        self.store = InterventionStore(self.db)
        self.audit = AuditLogger(self.db)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.adapter = ExecutionAdapter(self.db, self.store, self.audit, campaign_engine=None)

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
            """INSERT INTO campaigns (id, intervention_id, event_id, subject_line,
               audience_count, status, created_at) VALUES (?,?,?,?,?,?,?)""",
            ("v2-d1", "test-intv", "evt1", "Subj", 10, "draft", "2026-01-01"),
        )
        db.conn.commit()

        adapter = ExecutionAdapter(db, store, audit, campaign_engine=None)

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


if __name__ == "__main__":
    unittest.main()
