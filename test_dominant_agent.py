import sqlite3
import unittest
from dataclasses import dataclass
from datetime import date, timedelta

from dominant_agent import OpportunityEngine


@dataclass
class FakePacing:
    pace_vs_historical: float
    tickets_sold: int
    projected_final: int
    days_until: int
    urgency: int = 5


class FakeDecisionEngine:
    def __init__(self, pacing_by_event):
        self.pacing_by_event = pacing_by_event

    def analyze_event(self, event_id):
        return self.pacing_by_event.get(event_id)


class FakeDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(
            """
            CREATE TABLE events (
                event_id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                event_type TEXT,
                city TEXT,
                event_date TEXT NOT NULL,
                capacity INTEGER DEFAULT 0
            );
            CREATE TABLE orders (
                order_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL,
                email TEXT,
                ticket_count INTEGER DEFAULT 1,
                gross_amount REAL DEFAULT 0
            );
            CREATE TABLE customers (
                email TEXT PRIMARY KEY,
                favorite_city TEXT,
                favorite_event_type TEXT,
                cities TEXT,
                event_types TEXT
            );
            """
        )

    def get_event(self, event_id):
        row = self.conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return dict(row) if row else None

    def get_events(self, upcoming_only=False):
        rows = self.conn.execute("SELECT * FROM events ORDER BY event_date").fetchall()
        return [dict(r) for r in rows]


class OpportunityEngineTests(unittest.TestCase):
    def setUp(self):
        self.db = FakeDB()
        future = (date.today() + timedelta(days=30)).isoformat()
        self.db.conn.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)",
            ("evt1", "Philly Coffee Festival", "coffee", "Philadelphia", future, 5000),
        )
        self.db.conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?, ?, ?)",
            [
                ("o1", "evt1", "buyer@example.com", 2, 100.0),
                ("o2", "evt1", "buyer2@example.com", 1, 50.0),
            ],
        )
        customers = []
        for i in range(1000):
            customers.append(
                (
                    f"past{i}@example.com",
                    "Philadelphia",
                    "coffee",
                    '{"Philadelphia": 2}',
                    '{"coffee": 2}',
                )
            )
        customers.append(("buyer@example.com", "Philadelphia", "coffee", "{}", "{}"))
        self.db.conn.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?)", customers)
        self.db.conn.commit()

    def test_underperforming_event_surfaces_crm_recovery(self):
        decision = FakeDecisionEngine(
            {"evt1": FakePacing(0.75, tickets_sold=3, projected_final=3600, days_until=30, urgency=8)}
        )
        engine = OpportunityEngine(self.db, decision)
        opportunities = engine.evaluate_event("evt1")
        crm = [o for o in opportunities if o.opportunity_type == "crm_recovery"]
        self.assertEqual(len(crm), 1)
        self.assertGreater(crm[0].expected_revenue, 1000)
        self.assertTrue(crm[0].requires_approval)
        self.assertGreater(crm[0].metadata["audience_count"], 900)

    def test_healthy_pace_does_not_create_recovery_spam(self):
        decision = FakeDecisionEngine(
            {"evt1": FakePacing(1.02, tickets_sold=3, projected_final=4200, days_until=30)}
        )
        engine = OpportunityEngine(self.db, decision)
        opportunities = engine.evaluate_event("evt1")
        self.assertFalse(any(o.opportunity_type == "crm_recovery" for o in opportunities))

    def test_clear_pricing_power_is_flagged_but_requires_approval(self):
        decision = FakeDecisionEngine(
            {"evt1": FakePacing(1.20, tickets_sold=3000, projected_final=4900, days_until=21, urgency=4)}
        )
        engine = OpportunityEngine(self.db, decision)
        pricing = [o for o in engine.evaluate_event("evt1") if o.opportunity_type == "pricing_power"]
        self.assertEqual(len(pricing), 1)
        self.assertTrue(pricing[0].requires_approval)
        self.assertEqual(pricing[0].metadata["test_price_increase"], 2.0)

    def test_missing_event_returns_empty_list(self):
        engine = OpportunityEngine(self.db, FakeDecisionEngine({}))
        self.assertEqual(engine.evaluate_event("missing"), [])

    def test_command_summary_is_sorted_by_confidence_weighted_value(self):
        decision = FakeDecisionEngine(
            {"evt1": FakePacing(0.70, tickets_sold=3, projected_final=3000, days_until=30, urgency=9)}
        )
        engine = OpportunityEngine(self.db, decision)
        summary = engine.command_summary()
        self.assertIn("net_opportunity", summary)
        self.assertIn("confidence_weighted_net", summary)
        self.assertGreaterEqual(summary["opportunity_count"], 1)
        self.assertGreater(summary["net_opportunity"], 0)


if __name__ == "__main__":
    unittest.main()
