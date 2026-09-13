import sqlite3
import unittest
from dataclasses import dataclass
from datetime import date, timedelta

from dominant_agent import OpportunityEngine


@dataclass
class FakePacing:
    pace_vs_historical: float
    tickets_sold: int
    historical_median_at_point: float
    days_until: int = 30
    urgency: int = 5


class FakeDecisionEngine:
    def __init__(self, values):
        self.values = values

    def analyze_event(self, event_id):
        return self.values.get(event_id)


class FakeDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript("""
            CREATE TABLE events (event_id TEXT PRIMARY KEY, name TEXT, event_type TEXT, city TEXT, event_date TEXT, capacity INTEGER);
            CREATE TABLE orders (order_id TEXT PRIMARY KEY, event_id TEXT, email TEXT, ticket_count INTEGER, gross_amount REAL);
        """)

    def get_event(self, event_id):
        row = self.conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return dict(row) if row else None

    def get_events(self, upcoming_only=False):
        return [dict(row) for row in self.conn.execute("SELECT * FROM events").fetchall()]


class FakePacingFull:
    """Extended pacing stub that includes comparison_events for history checks."""
    def __init__(self, pace_vs_historical, tickets_sold, historical_median_at_point,
                 days_until=30, urgency=5, comparison_events=None):
        self.pace_vs_historical = pace_vs_historical
        self.tickets_sold = tickets_sold
        self.historical_median_at_point = historical_median_at_point
        self.days_until = days_until
        self.urgency = urgency
        self.comparison_events = comparison_events or []


class Tests(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "Philly Coffee Festival", "coffee", "Philadelphia", event_date, 5000),
        )
        self.db.conn.executemany(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            [
                ("o1", "evt1", "a@example.com", 2, 100.0),
                ("o2", "evt1", "b@example.com", 1, 50.0),
            ],
        )
        self.db.conn.commit()

    def test_behind_pace_surfaces_calibrated_opportunity(self):
        engine = OpportunityEngine(
            self.db,
            FakeDecisionEngine({
                "evt1": FakePacingFull(
                    pace_vs_historical=-29,
                    tickets_sold=700,
                    historical_median_at_point=1000,
                    days_until=30,
                    urgency=8,
                    comparison_events=["Past Edition 2024"],
                )
            }),
        )
        items = engine.evaluate_event("evt1")
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item.opportunity_type, "pace_recovery")
        self.assertIn("29% behind", item.rationale)
        self.assertTrue(item.requires_approval)
        self.assertGreater(item.revenue_at_risk, item.expected_revenue)
        self.assertEqual(item.evidence["gap_tickets"], 300)
        self.assertEqual(item.evidence["recoverable_share_assumption"], 0.25)
        self.assertEqual(item.evidence["days_until"], 30)
        self.assertEqual(len(item.opportunity_id), 16)

    def test_ahead_pace(self):
        engine = OpportunityEngine(
            self.db,
            FakeDecisionEngine({"evt1": FakePacing(12, 1100, 1000)}),
        )
        self.assertEqual(engine.evaluate_event("evt1"), [])

    def test_small_variance(self):
        engine = OpportunityEngine(
            self.db,
            FakeDecisionEngine({"evt1": FakePacing(-2, 980, 1000)}),
        )
        self.assertEqual(engine.evaluate_event("evt1"), [])

    def test_missing_event(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        self.assertEqual(engine.evaluate_event("missing"), [])

    def test_more_runway_allows_larger_recoverable_share(self):
        self.assertGreater(
            OpportunityEngine._recoverable_share(50),
            OpportunityEngine._recoverable_share(5),
        )

    def test_opportunity_id_is_stable_for_same_signal(self):
        pacing = FakePacingFull(-25, 750, 1000, days_until=30, urgency=9,
                                comparison_events=["Past Edition"])
        engine = OpportunityEngine(self.db, FakeDecisionEngine({"evt1": pacing}))
        first = engine.evaluate_event("evt1")[0].opportunity_id
        second = engine.evaluate_event("evt1")[0].opportunity_id
        self.assertEqual(first, second)

    def test_summary_separates_revenue_at_risk_from_modeled_recovery(self):
        engine = OpportunityEngine(
            self.db,
            FakeDecisionEngine({
                "evt1": FakePacingFull(-25, 750, 1000, days_until=30, urgency=9,
                                       comparison_events=["Past Edition"])
            }),
        )
        summary = engine.command_summary()
        self.assertEqual(summary["opportunity_count"], 1)
        self.assertGreater(summary["revenue_at_risk"], summary["gross_opportunity"])
        self.assertGreater(summary["net_opportunity"], 0)
        self.assertGreater(summary["confidence_weighted_net"], 0)


# ─────────────────────────────────────────────────────────────────────
# Zero-opportunity diagnosis regression tests
# ─────────────────────────────────────────────────────────────────────

class TestZeroOpportunityDiagnosis(unittest.TestCase):
    """Regression: a first-year event with zero historical comparisons must
    not silently pass as 'healthy'.  The system must distinguish:
      - portfolio genuinely healthy (has history, all events on pace)
      - insufficient evidence (no historical comparisons)
      - data unavailable/broken (analysis errors)
    """

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt1", "NYC Coffee Festival", "coffee", "New York", event_date, 3000),
        )
        self.db.conn.executemany(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            [("o1", "evt1", "a@example.com", 2, 100.0),
             ("o2", "evt1", "b@example.com", 1, 50.0)],
        )
        self.db.conn.commit()

    # --- Classification tests ---

    def test_no_history_classified_correctly(self):
        """An event with zero historical comparisons must be classified
        as 'no_history', not 'on_pace'."""
        # pace_vs_historical=0 AND historical_median=0 AND comparison_events=[]
        pacing = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0, comparison_events=[],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "no_history")
        self.assertIn("0 past edition", cls["detail"])

    def test_no_history_produces_no_opportunity(self):
        """An event without history must not produce a fabricated opportunity."""
        pacing = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0, comparison_events=[],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        items = engine.evaluate_event("evt1")
        self.assertEqual(len(items), 0)

    def test_on_pace_classified_correctly(self):
        """An event with valid history and pace >= -3% must be 'on_pace'."""
        pacing = FakePacingFull(
            pace_vs_historical=-1.5, tickets_sold=985,
            historical_median_at_point=1000, comparison_events=["Past Edition"],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "on_pace")

    def test_ahead_of_pace_classified_as_on_pace(self):
        """An event ahead of pace should also be 'on_pace'."""
        pacing = FakePacingFull(
            pace_vs_historical=15.0, tickets_sold=1150,
            historical_median_at_point=1000, comparison_events=["Past Edition"],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "on_pace")

    def test_behind_pace_classified_as_opportunity(self):
        """An event behind pace with material recovery → 'opportunity'."""
        pacing = FakePacingFull(
            pace_vs_historical=-29, tickets_sold=700,
            historical_median_at_point=1000,
            comparison_events=["Past Edition 2024", "Past Edition 2023"],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "opportunity")

    def test_below_materiality_classified_correctly(self):
        """Behind pace but tiny gap → 'below_materiality'."""
        pacing = FakePacingFull(
            pace_vs_historical=-5.0, tickets_sold=998,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "below_materiality")

    def test_no_ticket_price_classified_correctly(self):
        """Event with no orders → 'no_ticket_price'."""
        self.db.conn.execute("DELETE FROM orders WHERE event_id = 'evt1'")
        self.db.conn.commit()
        pacing = FakePacingFull(
            pace_vs_historical=-20, tickets_sold=0,
            historical_median_at_point=500, comparison_events=["Past"],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "no_ticket_price")

    def test_analysis_error_classified_correctly(self):
        """DecisionEngine returning None → 'analysis_error'."""
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": None}),
        )
        cls = engine._classify_event("evt1")
        self.assertEqual(cls["classification"], "analysis_error")

    def test_event_not_found_classified(self):
        """Non-existent event → 'event_not_found'."""
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        cls = engine._classify_event("missing")
        self.assertEqual(cls["classification"], "event_not_found")

    # --- Data quality in command_summary ---

    def test_summary_includes_data_quality(self):
        """command_summary() must include a data_quality field."""
        pacing = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0, comparison_events=[],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        summary = engine.command_summary()
        self.assertIn("data_quality", summary)
        dq = summary["data_quality"]
        self.assertIn("events_evaluated", dq)
        self.assertIn("events_with_history", dq)
        self.assertIn("events_without_history", dq)
        self.assertIn("coverage_pct", dq)
        self.assertIn("zero_is_trustworthy", dq)
        self.assertIn("warnings", dq)
        self.assertIn("event_details", dq)

    def test_zero_not_trustworthy_without_history(self):
        """Zero opportunities from no-history events → zero_is_trustworthy=False."""
        pacing = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0, comparison_events=[],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        summary = engine.command_summary()
        self.assertEqual(summary["opportunity_count"], 0)
        self.assertFalse(summary["data_quality"]["zero_is_trustworthy"])
        self.assertEqual(summary["data_quality"]["events_without_history"], 1)
        self.assertGreater(len(summary["data_quality"]["warnings"]), 0)

    def test_zero_trustworthy_when_all_on_pace(self):
        """Zero opportunities when all events are on pace → zero_is_trustworthy=True."""
        pacing = FakePacingFull(
            pace_vs_historical=5.0, tickets_sold=1050,
            historical_median_at_point=1000,
            comparison_events=["Past Edition 2024"],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        summary = engine.command_summary()
        self.assertEqual(summary["opportunity_count"], 0)
        self.assertTrue(summary["data_quality"]["zero_is_trustworthy"])
        self.assertEqual(summary["data_quality"]["events_without_history"], 0)
        self.assertEqual(len(summary["data_quality"]["warnings"]), 0)

    def test_coverage_pct_reflects_history(self):
        """coverage_pct must reflect how many events have historical data."""
        pacing = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0, comparison_events=[],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        summary = engine.command_summary()
        self.assertEqual(summary["data_quality"]["coverage_pct"], 0.0)

    def test_event_details_in_summary(self):
        """Each event should appear in event_details with its classification."""
        pacing = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0, comparison_events=[],
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        summary = engine.command_summary()
        details = summary["data_quality"]["event_details"]
        self.assertEqual(len(details), 1)
        self.assertEqual(details[0]["event_id"], "evt1")
        self.assertEqual(details[0]["classification"], "no_history")

    # --- Production-like portfolio fixture ---

    def test_production_like_portfolio_all_no_history(self):
        """Simulate production: 5 events, all with no historical comparisons.
        Summary must report zero opportunities with clear data quality warnings."""
        event_date = (date.today() + timedelta(days=30)).isoformat()
        for i in range(2, 6):
            self.db.conn.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?)",
                (f"evt{i}", f"Festival {i}", "wine", "Philadelphia", event_date, 2000),
            )
            self.db.conn.execute(
                "INSERT INTO orders VALUES (?,?,?,?,?)",
                (f"o_extra_{i}", f"evt{i}", f"buyer{i}@example.com", 3, 150.0),
            )
        self.db.conn.commit()

        pacing_map = {}
        for i in range(1, 6):
            pacing_map[f"evt{i}"] = FakePacingFull(
                pace_vs_historical=0, tickets_sold=300,
                historical_median_at_point=0, comparison_events=[],
            )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine(pacing_map),
        )
        summary = engine.command_summary()
        self.assertEqual(summary["opportunity_count"], 0)
        self.assertEqual(summary["revenue_at_risk"], 0)
        dq = summary["data_quality"]
        self.assertEqual(dq["events_evaluated"], 5)
        self.assertEqual(dq["events_without_history"], 5)
        self.assertEqual(dq["events_with_history"], 0)
        self.assertFalse(dq["zero_is_trustworthy"])
        self.assertEqual(len(dq["event_details"]), 5)
        # All should be classified as no_history
        for detail in dq["event_details"]:
            self.assertEqual(detail["classification"], "no_history")

    def test_mixed_portfolio(self):
        """Portfolio with one event having history (on pace) and one without.
        Summary must reflect mixed coverage."""
        event_date = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("evt2", "DC Wine Fest", "wine", "DC", event_date, 2000),
        )
        self.db.conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            ("o3", "evt2", "c@example.com", 2, 80.0),
        )
        self.db.conn.commit()

        pacing_map = {
            # evt1: has history, on pace
            "evt1": FakePacingFull(
                pace_vs_historical=5.0, tickets_sold=1050,
                historical_median_at_point=1000,
                comparison_events=["Past Coffee Fest 2024"],
            ),
            # evt2: no history
            "evt2": FakePacingFull(
                pace_vs_historical=0, tickets_sold=200,
                historical_median_at_point=0, comparison_events=[],
            ),
        }
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine(pacing_map),
        )
        summary = engine.command_summary()
        dq = summary["data_quality"]
        self.assertEqual(dq["events_evaluated"], 2)
        self.assertEqual(dq["events_with_history"], 1)
        self.assertEqual(dq["events_without_history"], 1)
        self.assertFalse(dq["zero_is_trustworthy"])
        self.assertEqual(dq["coverage_pct"], 50.0)

    # --- Backward compatibility ---

    def test_summary_preserves_existing_fields(self):
        """Existing fields in command_summary() must not change."""
        pacing = FakePacingFull(
            pace_vs_historical=-25, tickets_sold=750,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            days_until=30, urgency=9,
        )
        engine = OpportunityEngine(
            self.db, FakeDecisionEngine({"evt1": pacing}),
        )
        summary = engine.command_summary()
        # All original fields still present
        for field in ["generated_at", "opportunity_count", "revenue_at_risk",
                      "gross_opportunity", "net_opportunity",
                      "confidence_weighted_net", "opportunities"]:
            self.assertIn(field, summary)
        self.assertEqual(summary["opportunity_count"], 1)

    # --- _has_history edge cases ---

    def test_has_history_requires_both_median_and_comparisons(self):
        """_has_history must require BOTH median > 0 AND comparison_events non-empty."""
        # median > 0 but no comparisons
        pacing1 = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=100, comparison_events=[],
        )
        self.assertFalse(OpportunityEngine._has_history(pacing1))

        # comparisons exist but median = 0 (no snapshot at current days-out)
        pacing2 = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0,
            comparison_events=["Past Edition"],
        )
        self.assertFalse(OpportunityEngine._has_history(pacing2))

        # Both present → True
        pacing3 = FakePacingFull(
            pace_vs_historical=-10, tickets_sold=900,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
        )
        self.assertTrue(OpportunityEngine._has_history(pacing3))

    def test_comparison_events_missing_attribute(self):
        """If pacing object lacks comparison_events, _has_history returns False."""
        # FakePacing (minimal) doesn't have comparison_events
        pacing = FakePacing(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=1000,
        )
        self.assertFalse(OpportunityEngine._has_history(pacing))


if __name__ == "__main__":
    unittest.main()
