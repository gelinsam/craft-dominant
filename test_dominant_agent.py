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

    def analyze_portfolio(self):
        """Simulate grouped portfolio: return all non-None pacing objects.

        Sets event_id and constituent_event_ids on each pacing object
        when not already present, mirroring what the real
        analyze_portfolio() returns (EventPacing objects with those
        fields populated).
        """
        result = []
        for event_id, pacing in self.values.items():
            if pacing is not None:
                if not getattr(pacing, "event_id", ""):
                    pacing.event_id = event_id
                if not getattr(pacing, "constituent_event_ids", None):
                    pacing.constituent_event_ids = [event_id]
                result.append(pacing)
        return result


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
    """Extended pacing stub that includes comparison_events for history checks.

    Also carries event_id, event_name, revenue, and constituent_event_ids
    so that analyze_portfolio() (which returns grouped EventPacing objects)
    can be simulated in tests.
    """
    def __init__(self, pace_vs_historical, tickets_sold, historical_median_at_point,
                 days_until=30, urgency=5, comparison_events=None,
                 event_id="", event_name="", constituent_event_ids=None,
                 revenue=0.0):
        self.pace_vs_historical = pace_vs_historical
        self.tickets_sold = tickets_sold
        self.historical_median_at_point = historical_median_at_point
        self.days_until = days_until
        self.urgency = urgency
        self.comparison_events = comparison_events or []
        self.event_id = event_id
        self.event_name = event_name
        self.constituent_event_ids = constituent_event_ids
        self.revenue = revenue


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


# ─────────────────────────────────────────────────────────────────────
# Timed-entry grouping regression tests
# ─────────────────────────────────────────────────────────────────────

class TestTimedEntryGrouping(unittest.TestCase):
    """Regression: V2 must consume the grouped portfolio from
    analyze_portfolio() — the same path the existing dashboard uses —
    rather than evaluating individual timed-entry slot IDs.

    The dashboard groups multiple time-slot event IDs (e.g. six "DC
    Coffee Festival" slots: 3 times × 2 days) into logical day-events
    with aggregated tickets, capacity, and properly matched historical
    comparisons.  Without grouping, individual slot IDs have no
    cross-year match and report no history.
    """

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        # Simulate three timed-entry slot IDs for one festival
        for i, time in enumerate(["09:00", "10:00", "13:30"]):
            eid = f"slot_{i}"
            self.db.conn.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?)",
                (eid, "DC Coffee Festival", "coffee", "Washington",
                 f"{event_date}T{time}:00", 2000),
            )
            self.db.conn.executemany(
                "INSERT INTO orders VALUES (?,?,?,?,?)",
                [(f"o_{i}_a", eid, f"a{i}@example.com", 5, 250.0),
                 (f"o_{i}_b", eid, f"b{i}@example.com", 3, 150.0)],
            )
        self.db.conn.commit()

    def test_evaluate_all_uses_grouped_portfolio(self):
        """evaluate_all() must use analyze_portfolio(), not raw slot IDs."""
        # The grouped pacing (as dashboard would produce) has real history.
        grouped_pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=750,
            historical_median_at_point=1000,
            comparison_events=["DC Coffee Festival 2025", "DC Coffee Festival 2024"],
            event_id="grouped_dc_coffee_sat",
            event_name="DC Coffee Festival - Saturday",
            constituent_event_ids=["slot_0", "slot_1", "slot_2"],
        )
        # The decision engine's analyze_portfolio returns the grouped result.
        de = FakeDecisionEngine({})
        de.analyze_portfolio = lambda: [grouped_pacing]

        engine = OpportunityEngine(self.db, de)
        items = engine.evaluate_all()
        self.assertEqual(len(items), 1)
        self.assertIn("25% behind", items[0]["rationale"])

    def test_grouped_ticket_price_sums_across_constituents(self):
        """_avg_ticket_price_for_ids must aggregate across all slot IDs."""
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        # Total orders: slot_0(8 tickets, $400) + slot_1(8, $400) + slot_2(8, $400)
        # = 24 tickets, $1200  →  avg = $50
        price = engine._avg_ticket_price_for_ids(["slot_0", "slot_1", "slot_2"])
        self.assertEqual(price, 50.0)

    def test_grouped_opportunity_uses_correct_event_name(self):
        """Opportunity from grouped pacing must carry the grouped name."""
        grouped_pacing = FakePacingFull(
            pace_vs_historical=-30.0, tickets_sold=700,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="grouped_id",
            event_name="DC Coffee Festival - Saturday",
            constituent_event_ids=["slot_0", "slot_1", "slot_2"],
        )
        de = FakeDecisionEngine({})
        de.analyze_portfolio = lambda: [grouped_pacing]

        engine = OpportunityEngine(self.db, de)
        items = engine.evaluate_all()
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["event_name"], "DC Coffee Festival - Saturday")

    def test_command_summary_reports_grouped_events(self):
        """command_summary() must report grouped events, not raw slot count."""
        grouped = FakePacingFull(
            pace_vs_historical=-20.0, tickets_sold=800,
            historical_median_at_point=1000,
            comparison_events=["Past Edition 2025"],
            event_id="grouped_id",
            event_name="DC Coffee Festival - Saturday",
            constituent_event_ids=["slot_0", "slot_1", "slot_2"],
        )
        de = FakeDecisionEngine({})
        de.analyze_portfolio = lambda: [grouped]

        engine = OpportunityEngine(self.db, de)
        summary = engine.command_summary()
        dq = summary["data_quality"]
        # Should report 1 grouped event, not 3 raw slots
        self.assertEqual(dq["events_evaluated"], 1)
        self.assertEqual(dq["events_with_history"], 1)
        self.assertEqual(dq["events_without_history"], 0)
        self.assertEqual(summary["opportunity_count"], 1)

    def test_ungrouped_event_still_works(self):
        """Non-timed-entry events (single slot) must still evaluate correctly."""
        single_pacing = FakePacingFull(
            pace_vs_historical=-15.0, tickets_sold=850,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="slot_0",
            event_name="DC Coffee Festival",
            constituent_event_ids=None,  # no constituent IDs → use event_id
        )
        de = FakeDecisionEngine({})
        de.analyze_portfolio = lambda: [single_pacing]

        engine = OpportunityEngine(self.db, de)
        items = engine.evaluate_all()
        self.assertEqual(len(items), 1)

    def test_classify_pacing_with_grouped_event(self):
        """_classify_pacing must work with grouped EventPacing objects."""
        grouped = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=750,
            historical_median_at_point=1000,
            comparison_events=["Past 2025", "Past 2024"],
            event_id="grouped_id",
            event_name="DC Coffee Festival - Saturday",
            constituent_event_ids=["slot_0", "slot_1", "slot_2"],
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        cls = engine._classify_pacing(grouped)
        self.assertEqual(cls["classification"], "opportunity")

    def test_classify_pacing_no_history(self):
        """Grouped event with no history must be classified correctly."""
        grouped = FakePacingFull(
            pace_vs_historical=0, tickets_sold=500,
            historical_median_at_point=0,
            comparison_events=[],
            event_id="grouped_id",
            event_name="New Festival",
            constituent_event_ids=["slot_0", "slot_1"],
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        cls = engine._classify_pacing(grouped)
        self.assertEqual(cls["classification"], "no_history")

    def test_evaluate_pacing_directly(self):
        """evaluate_pacing must produce opportunities from pre-computed pacing."""
        pacing = FakePacingFull(
            pace_vs_historical=-29, tickets_sold=700,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="grouped_id",
            event_name="Test Festival",
            constituent_event_ids=["slot_0", "slot_1", "slot_2"],
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        items = engine.evaluate_pacing(pacing)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].event_name, "Test Festival")
        self.assertEqual(items[0].event_id, "grouped_id")


# ─────────────────────────────────────────────────────────────────────
# Pre-merge audit regression tests (PR #3)
# ─────────────────────────────────────────────────────────────────────

class TestGroupedPriceScopeFix(unittest.TestCase):
    """Audit §1: Verify avg ticket price uses day-scoped revenue/tickets
    from the pacing object, not cross-day constituent IDs.

    Background: constituent_event_ids in _create_day_event() contains ALL
    event IDs across ALL days of a multi-day festival, while tickets_sold
    and revenue on the grouped EventPacing are scoped to ONE logical day.
    The price computation must match the gap scope (day-scoped).
    """

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        # Saturday slots: expensive VIP-heavy
        for i in range(3):
            eid = f"sat_slot_{i}"
            self.db.conn.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?)",
                (eid, "Wine Fest", "wine", "DC", event_date, 500),
            )
            self.db.conn.execute(
                "INSERT INTO orders VALUES (?,?,?,?,?)",
                (f"o_sat_{i}", eid, f"sat{i}@example.com", 2, 200.0),
            )
        # Sunday slots: cheap GA-heavy
        sun_date = (date.today() + timedelta(days=31)).isoformat()
        for i in range(3):
            eid = f"sun_slot_{i}"
            self.db.conn.execute(
                "INSERT INTO events VALUES (?,?,?,?,?,?)",
                (eid, "Wine Fest", "wine", "DC", sun_date, 500),
            )
            self.db.conn.execute(
                "INSERT INTO orders VALUES (?,?,?,?,?)",
                (f"o_sun_{i}", eid, f"sun{i}@example.com", 10, 100.0),
            )
        self.db.conn.commit()

    def test_price_uses_day_scoped_revenue_not_cross_day_ids(self):
        """Price must come from pacing.revenue/tickets_sold (day-scoped),
        not from querying orders across all-day constituent IDs.

        Saturday: 6 tickets, $600 → $100/ticket (day-scoped)
        Sunday: 30 tickets, $300 → $10/ticket (day-scoped)
        All-days DB query: 36 tickets, $900 → $25/ticket (WRONG for either day)
        """
        saturday_pacing = FakePacingFull(
            pace_vs_historical=-30.0, tickets_sold=6,
            historical_median_at_point=20,
            comparison_events=["Past Edition"],
            event_id="grouped_sat",
            event_name="Wine Fest - Saturday",
            # constituent_event_ids spans BOTH days (as _create_day_event does)
            constituent_event_ids=["sat_slot_0", "sat_slot_1", "sat_slot_2",
                                   "sun_slot_0", "sun_slot_1", "sun_slot_2"],
            revenue=600.0,  # day-scoped: Saturday only
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        items = engine.evaluate_pacing(saturday_pacing)
        self.assertEqual(len(items), 1)
        # Price should be $100 (Saturday day-scoped), NOT $25 (cross-day)
        self.assertEqual(items[0].evidence["avg_ticket_price"], 100.0)

    def test_weighted_avg_not_unweighted(self):
        """Regression: unweighted average of per-slot averages would be wrong.

        Imagine two slots with very different economics:
        Slot A: 20 tickets at $200ea → $100/ticket avg
        Slot B: 100 tickets at $10ea → $10/ticket avg
        Unweighted average of ($100, $10) = $55 — WRONG
        Weighted (correct): $3000/120 = $25
        Day-scoped pacing revenue/tickets gives the right answer.
        """
        pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=120,
            historical_median_at_point=200,
            comparison_events=["Past Edition"],
            event_id="grouped_mixed",
            event_name="Mixed Pricing Fest",
            constituent_event_ids=["sat_slot_0"],  # doesn't matter, pacing.revenue used
            revenue=3000.0,  # 120 tickets, $3000 total → $25/ticket
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        items = engine.evaluate_pacing(pacing)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].evidence["avg_ticket_price"], 25.0)

    def test_fallback_to_db_when_no_revenue(self):
        """When pacing has no revenue field, falls back to DB query."""
        pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=6,
            historical_median_at_point=20,
            comparison_events=["Past Edition"],
            event_id="sat_slot_0",
            event_name="Wine Fest Slot",
            constituent_event_ids=None,
            revenue=0.0,  # no revenue on pacing → fallback
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        items = engine.evaluate_pacing(pacing)
        self.assertEqual(len(items), 1)
        # Should use DB query for sat_slot_0: 2 tickets, $200 → $100
        self.assertEqual(items[0].evidence["avg_ticket_price"], 100.0)


class TestCrossDayIsolation(unittest.TestCase):
    """Audit §4/§5: Saturday and Sunday logical events must have
    independent economics, classifications, and opportunity IDs.
    """

    def setUp(self):
        self.db = FakeDB()
        event_date = (date.today() + timedelta(days=30)).isoformat()
        # Need at least one DB event for FakeDB
        self.db.conn.execute(
            "INSERT INTO events VALUES (?,?,?,?,?,?)",
            ("placeholder", "Placeholder", "wine", "DC", event_date, 100),
        )
        self.db.conn.execute(
            "INSERT INTO orders VALUES (?,?,?,?,?)",
            ("o_p", "placeholder", "p@example.com", 1, 50.0),
        )
        self.db.conn.commit()

    def test_saturday_sunday_different_opportunity_ids(self):
        """Saturday and Sunday must produce different opportunity IDs."""
        sat_pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="grouped_sat_id",
            event_name="DC Wine Fest - Saturday",
            revenue=25000.0,
        )
        sun_pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="grouped_sun_id",
            event_name="DC Wine Fest - Sunday",
            revenue=25000.0,
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        sat_items = engine.evaluate_pacing(sat_pacing)
        sun_items = engine.evaluate_pacing(sun_pacing)
        self.assertEqual(len(sat_items), 1)
        self.assertEqual(len(sun_items), 1)
        self.assertNotEqual(sat_items[0].opportunity_id, sun_items[0].opportunity_id)
        self.assertNotEqual(sat_items[0].event_id, sun_items[0].event_id)

    def test_same_event_different_city_different_ids(self):
        """Same event name in different cities → different opportunity IDs."""
        dc_pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="grouped_dc_id",
            event_name="Coffee Festival - Saturday",
            revenue=25000.0,
        )
        philly_pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="grouped_philly_id",
            event_name="Coffee Festival - Saturday",
            revenue=25000.0,
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        dc_items = engine.evaluate_pacing(dc_pacing)
        philly_items = engine.evaluate_pacing(philly_pacing)
        # event_ids are different (synthetic MD5 includes city in name normally)
        # but in this test the event_names are the same — only event_ids differ
        self.assertNotEqual(dc_items[0].opportunity_id, philly_items[0].opportunity_id)

    def test_opportunity_id_independent_of_constituent_order(self):
        """Opportunity ID must be the same regardless of constituent ID order,
        because it depends on the synthetic event_id (name+date), not on
        the constituent list."""
        pacing_a = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="same_grouped_id",
            event_name="Wine Fest - Saturday",
            constituent_event_ids=["slot_1", "slot_2", "slot_3"],
            revenue=25000.0,
        )
        pacing_b = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="same_grouped_id",
            event_name="Wine Fest - Saturday",
            constituent_event_ids=["slot_3", "slot_1", "slot_2"],  # different order
            revenue=25000.0,
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        items_a = engine.evaluate_pacing(pacing_a)
        items_b = engine.evaluate_pacing(pacing_b)
        self.assertEqual(items_a[0].opportunity_id, items_b[0].opportunity_id)

    def test_opportunity_id_stable_across_runs(self):
        """Same inputs → same opportunity ID across repeated evaluations."""
        pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=500,
            historical_median_at_point=1000,
            comparison_events=["Past Edition"],
            event_id="stable_test_id",
            event_name="Stability Test",
            revenue=25000.0,
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        first = engine.evaluate_pacing(pacing)[0].opportunity_id
        second = engine.evaluate_pacing(pacing)[0].opportunity_id
        third = engine.evaluate_pacing(pacing)[0].opportunity_id
        self.assertEqual(first, second)
        self.assertEqual(second, third)

    def test_classify_pacing_uses_day_scoped_price(self):
        """_classify_pacing must use pacing.revenue/tickets for price."""
        pacing = FakePacingFull(
            pace_vs_historical=-25.0, tickets_sold=10,
            historical_median_at_point=20,
            comparison_events=["Past Edition"],
            event_id="classify_test",
            event_name="Price Scope Test",
            revenue=1000.0,  # $100/ticket
        )
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        cls = engine._classify_pacing(pacing)
        self.assertEqual(cls["classification"], "opportunity")
        # gap=10, price=$100, risk=$1000
        self.assertIn("$1,000", cls["detail"])


if __name__ == "__main__":
    unittest.main()
