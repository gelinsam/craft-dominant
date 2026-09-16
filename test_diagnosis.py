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

    def edition_sibling_ids(self, event_id):
        """Single-row editions: this double predates timed-entry grouping."""
        return [event_id]

    def get_edition_spend(self, event_id):
        return self.get_event_spend(event_id)

    def get_snapshots(self, event_id):
        return [
            dict(row)
            for row in self.conn.execute(
                "SELECT * FROM daily_snapshot WHERE event_id = ? ORDER BY snapshot_date ASC",
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
        """Verify the CRM campaign conversion math uses unified funnel model."""
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
            # Verify unified funnel: all segments use open → click → purchase
            audience = diagnosis.crm_audience_total
            base_prob = 0.22 * 0.035 * 0.012
            champion_prob = min(base_prob * 2.5, 1.0)
            non_champions = max(0, audience - diagnosis.crm_champions)
            base_conversions = int(non_champions * base_prob)
            champion_conversions = int(diagnosis.crm_champions * champion_prob)
            total_conversions = min(base_conversions + champion_conversions, 300)
            expected_rev = total_conversions * 50.0

            self.assertAlmostEqual(crm_opt.expected_revenue, expected_rev, places=0)
            self.assertEqual(crm_opt.expected_cost, 0.0)  # Email is free
            self.assertEqual(crm_opt.risk, "low")

    def test_blended_ad_spend_per_ticket_field(self):
        """Regression: field is blended_ad_spend_per_ticket, not cac."""
        opp = {
            "opportunity_id": "abc123",
            "evidence": {
                "days_until": 30, "tickets_sold": 700,
                "pace_delta_pct": -29, "gap_tickets": 300,
                "avg_ticket_price": 50.0,
            },
        }
        diagnosis = self.engine.diagnose("evt1", opp)
        self.assertTrue(hasattr(diagnosis, "blended_ad_spend_per_ticket"))
        self.assertFalse(hasattr(diagnosis, "cac"))
        # 1400 total spend / 700 tickets = 2.0
        self.assertAlmostEqual(diagnosis.blended_ad_spend_per_ticket, 2.0, places=1)
        # Verify serialized dict also uses correct name
        d = diagnosis.to_dict()
        self.assertIn("blended_ad_spend_per_ticket", d)
        self.assertNotIn("cac", d)


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

    def test_meta_unavailable_vs_zero_spend(self):
        """Regression: distinguish meta data unavailable from no spend."""
        # evt3 has an ad_spend table (created by FakeDB) but no rows → "no_records"
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        opp = {
            "opportunity_id": "test_meta",
            "evidence": {
                "days_until": 30, "tickets_sold": 200,
                "pace_delta_pct": -20, "gap_tickets": 200,
                "avg_ticket_price": 60.0,
            },
        }
        diagnosis = engine.diagnose("evt3", opp)
        # With ad_spend table existing but no rows: should say "No paid advertising detected"
        no_ad = [rc for rc in diagnosis.root_causes if "No paid advertising" in rc.cause]
        self.assertGreater(len(no_ad), 0, "Should detect no paid advertising when table exists but has no rows")

    def test_meta_unavailable_warns_differently(self):
        """Regression: when ad_spend table is missing, root cause should say 'unavailable'."""
        # Drop the ad_spend table to simulate unavailable Meta data
        self.db.conn.execute("DROP TABLE ad_spend")
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        opp = {
            "opportunity_id": "test_meta_unavail",
            "evidence": {
                "days_until": 30, "tickets_sold": 200,
                "pace_delta_pct": -20, "gap_tickets": 200,
                "avg_ticket_price": 60.0,
            },
        }
        diagnosis = engine.diagnose("evt3", opp)
        # Should say "unavailable" not "No paid advertising detected"
        unavailable = [rc for rc in diagnosis.root_causes if "unavailable" in rc.cause.lower()]
        self.assertGreater(len(unavailable), 0, "Should detect Meta data unavailability")
        no_ad = [rc for rc in diagnosis.root_causes if "No paid advertising detected" in rc.cause]
        self.assertEqual(len(no_ad), 0, "Should NOT say 'no paid advertising' when data is unavailable")

    def test_campaign_history_counts_only_sent(self):
        """Regression: historical_campaigns_sent counts only sent campaigns, not drafts."""
        # Create campaigns table with both draft and sent campaigns
        self.db.conn.executescript("""
            CREATE TABLE campaigns (
                id TEXT PRIMARY KEY, event_id TEXT, campaign_type TEXT,
                channel TEXT, phase TEXT, subject_line TEXT, preview_text TEXT,
                body_html TEXT, cta_text TEXT, cta_url TEXT,
                segment_name TEXT, segment_sql TEXT, audience_count INTEGER,
                status TEXT, sent_at TEXT, barrier_addressed TEXT,
                confidence_score REAL, strategic_reasoning TEXT,
                predicted_open_rate REAL, predicted_click_rate REAL,
                predicted_revenue REAL, created_at TEXT, updated_at TEXT
            );
            INSERT INTO campaigns (id, event_id, campaign_type, channel, subject_line,
                body_html, status, sent_at)
            VALUES ('c1', 'evt3', 'recovery', 'email', 'Test1', '<p>test</p>', 'sent', '2024-01-01');
            INSERT INTO campaigns (id, event_id, campaign_type, channel, subject_line,
                body_html, status, sent_at)
            VALUES ('c2', 'evt3', 'recovery', 'email', 'Test2', '<p>test</p>', 'draft', NULL);
            INSERT INTO campaigns (id, event_id, campaign_type, channel, subject_line,
                body_html, status, sent_at)
            VALUES ('c3', 'evt3', 'recovery', 'email', 'Test3', '<p>test</p>', 'draft', NULL);
        """)
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        opp = {
            "opportunity_id": "test_campaigns",
            "evidence": {
                "days_until": 30, "tickets_sold": 200,
                "pace_delta_pct": -20, "gap_tickets": 200,
                "avg_ticket_price": 60.0,
            },
        }
        diagnosis = engine.diagnose("evt3", opp)
        # Should count only the 1 sent campaign, not all 3
        self.assertEqual(diagnosis.current_event_campaigns_sent, 1)

    def test_root_cause_uses_blended_spend_label(self):
        """Regression: root cause text should say 'Blended ad spend' not 'CAC'."""
        # Add ad spend and enough tickets to make blended_ad_spend_per_ticket > 50
        for d in range(14):
            spend_date = (date.today() - timedelta(days=d)).isoformat()
            self.db.conn.execute(
                "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
                ("evt3", "camp1", spend_date, 500.0, 5000, 50),
            )
        # Just 1 ticket sold at $60 → blended = $7000/1 = $7000
        self.db.conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            ("o1", "evt3", "buyer1@example.com", 1, 60.0),
        )
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        opp = {
            "opportunity_id": "test_label",
            "evidence": {
                "days_until": 30, "tickets_sold": 1,
                "pace_delta_pct": -90, "gap_tickets": 1999,
                "avg_ticket_price": 60.0,
            },
        }
        diagnosis = engine.diagnose("evt3", opp)
        # Find the high-cost root cause
        cost_causes = [rc for rc in diagnosis.root_causes if "spend" in rc.cause.lower() or "cost" in rc.cause.lower()]
        for rc in cost_causes:
            self.assertNotIn("CAC", rc.cause)
            for e in rc.evidence:
                self.assertNotIn("CAC is", e)


class TestBlockerRegressions(unittest.TestCase):
    """Regression tests for blocker-removal pass."""

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt_br", "Blocker Regression Event", "coffee", "Philadelphia", event_date, 2000),
        )
        self.db.conn.commit()

    def _make_opp(self, **overrides):
        base = {
            "opportunity_id": "blocker_test",
            "evidence": {
                "days_until": 30, "tickets_sold": 200,
                "pace_delta_pct": -20, "gap_tickets": 200,
                "avg_ticket_price": 60.0,
            },
        }
        base.update(overrides)
        return base

    # ── Blocker 2: Audience double-counting ─────────────────────────
    def test_audience_dedup_past_attendees_and_city_prospects(self):
        """city_prospects must exclude past_attendee emails — no double counting."""
        # Insert customers who are BOTH past attendees and Philly city prospects
        for i in range(50):
            self.db.conn.execute(
                "INSERT INTO customers VALUES (?,?,?,?)",
                (f"overlap{i}@example.com", "Philadelphia", "coffee", "champion" if i < 10 else "regular"),
            )
        # Insert 30 Philly-only prospects (not past attendees in this test's stub,
        # but since FakeDB.get_past_attendees returns ALL customers not in buyers,
        # these overlap too — the dedup must handle it)
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())

        # Total must equal past_attendees + city_prospects (disjoint sets)
        self.assertEqual(
            diagnosis.crm_audience_total,
            diagnosis.crm_past_attendees + diagnosis.crm_city_prospects,
        )
        # city_prospects should be 0 because all Philly customers are already past_attendees
        self.assertEqual(diagnosis.crm_city_prospects, 0)

    # ── Blocker 3: Meta data freshness ──────────────────────────────
    def test_meta_freshness_current_has_spend(self):
        """Recent spend data should return current_has_spend."""
        today = date.today().isoformat()
        self.db.conn.execute(
            "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
            ("evt_br", "c1", today, 100.0, 5000, 50),
        )
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        status = engine._check_meta_data_status("evt_br")
        self.assertEqual(status, "current_has_spend")

    def test_meta_freshness_current_zero_spend(self):
        """Recent data with $0 spend should return current_zero_spend."""
        today = date.today().isoformat()
        self.db.conn.execute(
            "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
            ("evt_br", "c1", today, 0.0, 0, 0),
        )
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        status = engine._check_meta_data_status("evt_br")
        self.assertEqual(status, "current_zero_spend")

    def test_meta_freshness_stale(self):
        """Spend data older than threshold should return stale."""
        old_date = (date.today() - timedelta(days=10)).isoformat()
        self.db.conn.execute(
            "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
            ("evt_br", "c1", old_date, 100.0, 5000, 50),
        )
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        status = engine._check_meta_data_status("evt_br")
        self.assertEqual(status, "stale")

    def test_meta_freshness_no_records(self):
        """No ad_spend rows should return no_records."""
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        status = engine._check_meta_data_status("evt_br")
        self.assertEqual(status, "no_records")

    def test_meta_freshness_unavailable(self):
        """Missing ad_spend table should return unavailable."""
        self.db.conn.execute("DROP TABLE ad_spend")
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        status = engine._check_meta_data_status("evt_br")
        self.assertEqual(status, "unavailable")

    def test_stale_data_does_not_claim_no_advertising(self):
        """Stale Meta data must NOT produce 'No paid advertising detected' root cause."""
        old_date = (date.today() - timedelta(days=10)).isoformat()
        self.db.conn.execute(
            "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
            ("evt_br", "c1", old_date, 100.0, 5000, 50),
        )
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())

        no_ad_detected = [rc for rc in diagnosis.root_causes if "No paid advertising detected" in rc.cause]
        self.assertEqual(len(no_ad_detected), 0, "Stale data must not claim 'no paid advertising detected'")

        stale = [rc for rc in diagnosis.root_causes if "stale" in rc.cause.lower()]
        self.assertGreater(len(stale), 0, "Should flag stale Meta data")

    def test_current_zero_spend_claims_no_advertising(self):
        """Current data with $0 spend SHOULD produce 'No paid advertising detected'."""
        today = date.today().isoformat()
        self.db.conn.execute(
            "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
            ("evt_br", "c1", today, 0.0, 0, 0),
        )
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())
        no_ad = [rc for rc in diagnosis.root_causes if "No paid advertising detected" in rc.cause]
        self.assertGreater(len(no_ad), 0, "Current zero spend should confirm no advertising")

    def test_missing_data_warns_on_stale(self):
        """Missing data warnings should include staleness."""
        old_date = (date.today() - timedelta(days=10)).isoformat()
        self.db.conn.execute(
            "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
            ("evt_br", "c1", old_date, 100.0, 5000, 50),
        )
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())
        stale_warnings = [m for m in diagnosis.missing_data if "stale" in m.lower()]
        self.assertGreater(len(stale_warnings), 0, "Should warn about stale Meta data")

    # ── Blocker 4: Campaign-history naming ──────────────────────────
    def test_field_is_current_event_campaigns_sent(self):
        """Regression: field is current_event_campaigns_sent, not historical_campaigns_sent."""
        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())
        self.assertTrue(hasattr(diagnosis, "current_event_campaigns_sent"))
        self.assertFalse(hasattr(diagnosis, "historical_campaigns_sent"))
        d = diagnosis.to_dict()
        self.assertIn("current_event_campaigns_sent", d)
        self.assertNotIn("historical_campaigns_sent", d)

    # ── Blocker 5: Paid-media revenue model disabled ────────────────
    def test_no_ad_budget_shift_intervention(self):
        """ad_budget_shift intervention type must not exist anywhere."""
        # Add spend data so paid media review is triggered
        for d in range(7):
            spend_date = (date.today() - timedelta(days=d)).isoformat()
            self.db.conn.execute(
                "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
                ("evt_br", "camp1", spend_date, 500.0, 5000, 50),
            )
        self.db.conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            ("o1", "evt_br", "buyer1@example.com", 1, 60.0),
        )
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())
        ad_shift = [o for o in diagnosis.intervention_options if o.intervention_type == "ad_budget_shift"]
        self.assertEqual(len(ad_shift), 0, "ad_budget_shift intervention must not exist")

    def test_paid_media_review_is_non_financial(self):
        """paid_media_review must have zero expected_revenue and zero expected_cost."""
        for d in range(7):
            spend_date = (date.today() - timedelta(days=d)).isoformat()
            self.db.conn.execute(
                "INSERT INTO ad_spend VALUES (?,?,?,?,?,?)",
                ("evt_br", "camp1", spend_date, 500.0, 5000, 50),
            )
        self.db.conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            ("o1", "evt_br", "buyer1@example.com", 1, 60.0),
        )
        self.db.conn.commit()

        engine = DiagnosisEngine(self.db, FakeDecisionEngine())
        diagnosis = engine.diagnose("evt_br", self._make_opp())
        pmr = [o for o in diagnosis.intervention_options if o.intervention_type == "paid_media_review"]
        self.assertGreater(len(pmr), 0, "paid_media_review should appear when spend data exists")
        for opt in pmr:
            self.assertEqual(opt.expected_revenue, 0.0)
            self.assertEqual(opt.expected_cost, 0.0)
            self.assertEqual(opt.expected_net_value, 0.0)
            self.assertEqual(opt.confidence, 0.0)


if __name__ == "__main__":
    unittest.main()
