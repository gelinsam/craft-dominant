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
        self.db.conn.execute("INSERT INTO events VALUES (?,?,?,?,?,?)", ("evt1", "Philly Coffee Festival", "coffee", "Philadelphia", event_date, 5000))
        self.db.conn.executemany("INSERT INTO orders VALUES (?,?,?,?,?)", [
            ("o1", "evt1", "a@example.com", 2, 100.0),
            ("o2", "evt1", "b@example.com", 1, 50.0),
        ])
        self.db.conn.commit()

    def test_behind_pace(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({"evt1": FakePacing(-29, 700, 1000, 8)}))
        items = engine.evaluate_event("evt1")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0].opportunity_type, "pace_recovery")
        self.assertIn("29% behind", items[0].rationale)
        self.assertTrue(items[0].requires_approval)

    def test_ahead_pace(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({"evt1": FakePacing(12, 1100, 1000)}))
        self.assertEqual(engine.evaluate_event("evt1"), [])

    def test_small_variance(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({"evt1": FakePacing(-2, 980, 1000)}))
        self.assertEqual(engine.evaluate_event("evt1"), [])

    def test_missing_event(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        self.assertEqual(engine.evaluate_event("missing"), [])

    def test_summary(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({"evt1": FakePacing(-25, 750, 1000, 9)}))
        summary = engine.command_summary()
        self.assertEqual(summary["opportunity_count"], 1)
        self.assertGreater(summary["net_opportunity"], 0)
        self.assertGreater(summary["confidence_weighted_net"], 0)


if __name__ == "__main__":
    unittest.main()
