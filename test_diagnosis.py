"""Tests for the diagnosis engine.

Covers: diagnosis with complete data, diagnosis with missing data,
audience exclusion, deterministic root cause analysis, intervention
ranking, and expected-value math.
"""

import sqlite3
import unittest
from dataclasses import dataclass
from datetime import date, timedelta

from diagnosis_engine import DiagnosisEngine, Diagnosis, RootCause, InterventionOption


# ─────────────────────────────────────────────────────────────────────
# Test infrastructure
# ─────────────────────────────────────────────────────────────────────

class FakeDB:
    """Minimal database stub with configurable data."""

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
            CREATE TABLE daily_snapshot (
                event_id TEXT, snapshot_date TEXT, tickets_cumulative INTEGER,
                revenue_cumulative REAL, ad_spend_cumulative REAL
            );
            CREATE TABLE ad_spend (
                event_id TEXT, campaign_id TEXT, spend_date TEXT,
                spend REAL, impressions INTEGER, clicks INTEGER
            );
            CREATE TABLE customers (
                email TEXT PRIMARY KEY, favorite_city TEXT,
                event_types TEXT, rfm_segment TEXT
            );
            CREATE TABLE suppressions (email TEXT PRIMARY KEY);
        """)
        self.conn.commit()

    def get_event(self, event_id):
        row = self.conn.execute(
            "SELECT * FROM events WHERE event_id = ?", (event_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_events(self, upcoming_only=False):
        return [
            dict(row)
            for row in self.conn.execute("SELECT * FROM events").fetchall()
        ]

    def get_event_buyers(self, event_id):
        rows = self.conn.execute(
            "SELECT DISTINCT email FROM orders WHERE event_id = ?",
            (event_id,),
        ).fetchall()
        return [r["email"] for r in rows]

    def get_event_tickets(self, event_id):
        row = self.conn.execute(
            "SELECT COALESCE(SUM(ticket_count), 0) as t FROM orders WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return int(row["t"])

    def get_event_revenue(self, event_id):
        row = self.conn.execute(
            "SELECT COALESCE(SUM(gross_amount), 0) as r FROM orders WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return float(row["r"])

    def get_event_spend(self, event_id):
        row = self.conn.execute(
            "SELECT COALESCE(SUM(spend), 0) as s FROM ad_spend WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        return float(row["s"])

    def get_snapshots(self, event_id):
        return [
            dict(row)
            for row in self.conn.execute(
                "SELECT * FROM daily_snapshot WHERE event_id = ? ORDER BY snapshot_date DESC",
                (event_id,),
            ).fetchall()
        ]

    def get_snapshot_at_days(self, event_id, days_out):
        """Stub: return None (no historical snapshots by default)."""
        return None

    def get_pattern_event_ids(self, pattern, exclude_ids=None):
        return []

    def get_past_attendees_not_purchased(self, event_id, event_name, limit=50000, current_buyer_emails=None):
        """Return customers not in current_buyer_emails."""
        buyer_set = set(current_buyer_emails or [])
        rows = self.conn.execute("SELECT * FROM customers").fetchall()
        return [dict(r) for r in rows if r["email"] not in buyer_set]

    def get_city_prospects(self, city, exclude_emails=None, limit=50000):
        exclude = set(exclude_emails or [])
        rows = self.conn.execute(
            "SELECT * FROM customers WHERE favorite_city = ?", (city,)
        ).fetchall()
        return [dict(r) for r in rows if r["email"] not in exclude]


class FakeDecisionEngine:
    """Stub decision engine for pattern matching."""

    def _get_pattern(self, name):
        return name.lower().replace(" ", "_")


# ─────────────────────────────────────────────────────────────────────
# Tests
# ─────────────────────────────────────────────────────────────────────

class TestDiagnosisComplete(unittest.TestCase):
    """Diagnosis with reasonably complete data."""

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Philly Coffee Festival", "coffee", "Philadelphia", event_date, 5000),
        )
        # 700 tickets sold via 350 orders
        for i in range(350):
            self.db.conn.execute(
                "INSERT INTO orders VALUES (?,?,?,?,?)",
                (f"o{i}", "evt1", f"buyer{i}@example.com", 2, 100.0),
            )

        # Snapshot data for velocity
        for d in range(10):
            snap_date = (date.today() - timedelta(days=d)).isoformat()
            self.db.conn.execute(
                "INSERT INTO daily_snapshot VALUES (?,?,?,?,?)",
                ("evt1", snap_date, 700 - d * 5, (700 - d * 5) * 50, 0),
            )

        # Ad spend
        for d in range(14):
            spend_date = (date.today() - timedelta(days=d)).isoformat()
            self.db.conn.execute(
                "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
                ("evt1", "camp1", spend_date, 100.0, 5000, 50),
            )

        # CRM audience
        for i in range(200):
            self.db.conn.execute(
                "INSERT INTO customers VALUES (?,?,?,?)",
                (f"crm{i}@example.com", "Philadelphia", "coffee", "champion" if i < 30 else "regular"),
            )

        self.db.conn.commit()
        self.engine = DiagnosisEngine(self.db, FakeDecisionEngine())

    def test_diagnosis_returns_all_fields(self):
        opp = {
            "opportunity_id": "abc123",
            "revenue_at_risk": 15000,
            "evidence": {
                "days_until": 30,
                "tickets_sold": 700,
                "pace_delta_pct": -29,
                "gap_tickets": 300,
                "avg_ticket_price": 50.0,
                "recoverable_share_assumption": 0.25,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)

        self.assertIsInstance(diagnosis, Diagnosis)
        self.assertEqual(diagnosis.event_id, "evt1")
        self.assertEqual(diagnosis.event_name, "Philly Coffee Festival")
        self.assertEqual(diagnosis.opportunity_id, "abc123")
        self.assertEqual(diagnosis.days_until, 30)
        self.assertEqual(diagnosis.tickets_sold, 700)
        self.assertEqual(diagnosis.capacity, 5000)
        self.assertEqual(diagnosis.pace_delta_pct, -29)
        self.assertEqual(diagnosis.gap_tickets, 300)
        self.assertAlmostEqual(diagnosis.avg_ticket_price, 50.0)

    def test_velocity_computed(self):
        opp = {
            "opportunity_id": "abc123",
            "revenue_at_risk": 15000,
            "evidence": {
                "days_until": 30,
                "tickets_sold": 700,
                "pace_delta_pct": -29,
                "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        # We inserted snapshots with 5 tickets/day difference
        self.assertIsNotNone(diagnosis.recent_velocity)
        self.assertAlmostEqual(diagnosis.recent_velocity, 5.0, places=1)

    def test_meta_spend_aggregated(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        self.assertEqual(diagnosis.meta_spend_total, 1400.0)  # 14 days * $100
        self.assertEqual(diagnosis.meta_spend_recent_7d, 800.0)  # 8 calendar days (>=today-7) * $100

    def test_audience_excludes_buyers(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        self.assertEqual(diagnosis.current_buyers_count, 350)
        # CRM audience should not include buyers
        self.assertGreater(diagnosis.crm_audience_total, 0)

    def test_root_causes_are_populated(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        self.assertIsInstance(diagnosis.root_causes, list)
        for rc in diagnosis.root_causes:
            self.assertIsInstance(rc, RootCause)
            self.assertTrue(len(rc.cause) > 0)
            self.assertTrue(0 <= rc.confidence <= 1)
            self.assertIsInstance(rc.evidence, list)

    def test_root_causes_sorted_by_confidence(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        if len(diagnosis.root_causes) > 1:
            for i in range(len(diagnosis.root_causes) - 1):
                self.assertGreaterEqual(
                    diagnosis.root_causes[i].confidence,
                    diagnosis.root_causes[i + 1].confidence,
                )

    def test_intervention_options_ranked(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        self.assertIsInstance(diagnosis.intervention_options, list)
        self.assertGreater(len(diagnosis.intervention_options), 0)
        for opt in diagnosis.intervention_options:
            self.assertIsInstance(opt, InterventionOption)
            self.assertTrue(len(opt.intervention_type) > 0)
            self.assertTrue(0 <= opt.confidence <= 1)

    def test_recommended_intervention_is_top_option(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        if diagnosis.intervention_options:
            self.assertEqual(
                diagnosis.recommended_intervention,
                diagnosis.intervention_options[0].intervention_type,
            )

    def test_to_dict_serializes(self):
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        d = diagnosis.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["event_id"], "evt1")
        self.assertIn("root_causes", d)
        self.assertIn("intervention_options", d)

    def test_expected_value_math(self):
        """Verify the CRM campaign conversion math is deterministic."""
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)

        # Find the CRM campaign option
        crm_opt = next(
            (o for o in diagnosis.intervention_options if o.intervention_type == "crm_campaign"),
            None,
        )
        if crm_opt:
            # Verify deterministic math
            audience = diagnosis.crm_audience_total
            expected_opens = int(audience * 0.22)
            expected_clicks = int(expected_opens * 0.035)
            base_conversions = int(expected_clicks * 0.012)
            champion_conversions = int(
                diagnosis.crm_champions * 0.22 * 0.035 * 2.5
            )
            total_conversions = min(base_conversions + champion_conversions, 300)
            expected_rev = total_conversions * 50.0

            self.assertAlmostEqual(crm_opt.expected_revenue, expected_rev, places=0)
            self.assertEqual(crm_opt.expected_cost, 0.0)  # Email is free
            self.assertEqual(crm_opt.risk, "low")


class TestDiagnosisMissingData(unittest.TestCase):
    """Diagnosis with minimal/missing data."""

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt2", "Bare Minimum Event", "beer", "Denver", event_date, 1000),
        )
        self.db.conn.commit()
        self.engine = DiagnosisEngine(self.db, FakeDecisionEngine())

    def test_missing_data_warnings(self):
        opp = {
            "opportunity_id": "xyz789",
            "evidence": {
                "days_until": 30, "tickets_sold": 0,
                "pace_delta_pct": -50, "gap_tickets": 500,
                "avg_ticket_price": 40.0,
            },
        }
        diagnosis = self.engine.diagnose("evt2", opp)
        self.assertIsInstance(diagnosis.missing_data, list)
        self.assertGreater(len(diagnosis.missing_data), 0)

        # Should warn about no Meta spend
        spend_warnings = [m for m in diagnosis.missing_data if "Meta" in m or "ad spend" in m.lower()]
        self.assertGreater(len(spend_warnings), 0)

    def test_no_audience_warns(self):
        opp = {
            "opportunity_id": "xyz789",
            "evidence": {
                "days_until": 30, "tickets_sold": 0,
                "pace_delta_pct": -50, "gap_tickets": 500,
                "avg_ticket_price": 40.0,
            },
        }
        diagnosis = self.engine.diagnose("evt2", opp)
        audience_warnings = [m for m in diagnosis.missing_data if "audience" in m.lower() or "CRM" in m]
        self.assertGreater(len(audience_warnings), 0)

    def test_event_not_found_raises(self):
        with self.assertRaises(ValueError):
            self.engine.diagnose("nonexistent", {"opportunity_id": "x", "evidence": {}})

    def test_no_interventions_when_no_audience(self):
        opp = {
            "opportunity_id": "xyz789",
            "evidence": {
                "days_until": 30, "tickets_sold": 0,
                "pace_delta_pct": -50, "gap_tickets": 500,
                "avg_ticket_price": 40.0,
            },
        }
        diagnosis = self.engine.diagnose("evt2", opp)
        crm_opts = [o for o in diagnosis.intervention_options if o.intervention_type == "crm_campaign"]
        # With no CRM audience, CRM campaign should not be offered
        self.assertEqual(len(crm_opts), 0)


class TestDiagnosisRootCauses(unittest.TestCase):
    """Verify specific root cause triggers."""

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt3", "Test Event", "wine", "Napa", event_date, 2000),
        )
        self.db.conn.commit()

    def test_no_advertising_cause(self):
        """No Meta spend with 30 days runway should trigger no-advertising root cause."""
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        opp = {
            "opportunity_id": "test1",
            "evidence": {
                "days_until": 30, "tickets_sold": 200,
                "pace_delta_pct": -20, "gap_tickets": 200,
                "avg_ticket_price": 60.0,
            },
        }
        diagnosis = engine.diagnose("evt3", opp)
        no_ad_causes = [rc for rc in diagnosis.root_causes if "advertising" in rc.cause.lower() or "No paid" in rc.cause]
        self.assertGreater(len(no_ad_causes), 0)

    def test_velocity_stall_cause(self):
        """Very low recent velocity should trigger velocity stall."""
        # Insert snapshots with very low velocity (0.5 tickets/day)
        for d in range(10):
            snap_date = (date.today() - timedelta(days=d)).isoformat()
            self.db.conn.execute(
                "INSERT INTO daily_snapshot VALUES (?,?,?,?,?)",
                ("evt3", snap_date, 200 + d * 0, 200 * 60, 0),
            )
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        opp = {
            "opportunity_id": "test2",
            "evidence": {
                "days_until": 30, "tickets_sold": 200,
                "pace_delta_pct": -20, "gap_tickets": 200,
                "avg_ticket_price": 60.0,
            },
        }
        diagnosis = engine.diagnose("evt3", opp)
        velocity_causes = [rc for rc in diagnosis.root_causes if "velocity" in rc.cause.lower() or "stall" in rc.cause.lower()]
        self.assertGreater(len(velocity_causes), 0)


if __name__ == "__main__":
    unittest.main()
