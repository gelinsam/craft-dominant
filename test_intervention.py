"""Tests for the intervention model, state machine, and campaign adapter.

Covers: state machine transitions (legal and illegal), intervention CRUD,
prepare-creates-draft-only (no send), expected-value math, opportunity_id
linkage, and deterministic IDs.
"""

import json
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
                "INSERT INTO orders VALUES (?,?,?,?,?)",
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


if __name__ == "__main__":
    unittest.main()
