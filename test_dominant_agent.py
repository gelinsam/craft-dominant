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
                "evt1": FakePacing(
                    pace_vs_historical=-29,
                    tickets_sold=700,
                    historical_median_at_point=1000,
                    days_until=30,
                    urgency=8,
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
        pacing = FakePacing(-25, 750, 1000, days_until=30, urgency=9)
        engine = OpportunityEngine(self.db, FakeDecisionEngine({"evt1": pacing}))
        first = engine.evaluate_event("evt1")[0].opportunity_id
        second = engine.evaluate_event("evt1")[0].opportunity_id
        self.assertEqual(first, second)

    def test_summary_separates_revenue_at_risk_from_modeled_recovery(self):
        engine = OpportunityEngine(
            self.db,
            FakeDecisionEngine({
                "evt1": FakePacing(-25, 750, 1000, days_until=30, urgency=9)
            }),
        )
        summary = engine.command_summary()
        self.assertEqual(summary["opportunity_count"], 1)
        self.assertGreater(summary["revenue_at_risk"], summary["gross_opportunity"])
        self.assertGreater(summary["net_opportunity"], 0)
        self.assertGreater(summary["confidence_weighted_net"], 0)


if __name__ == "__main__":
    unittest.main()
