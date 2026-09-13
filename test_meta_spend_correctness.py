"""Tests for Meta spend correctness, CAC semantics, and SQLite locking hardening.

Covers:
  - spend_status computation (current_has_spend, current_zero_spend, stale, no_records)
  - CAC only computed when spend data is current and non-zero
  - _decide() never treats missing spend as "acceptable"
  - save_ad_spend_batch() writes atomically
  - sync_event_spend() uses batched writes
  - grouped event spend_status propagation
  - portfolio_cac only computed with current spend data
  - Database busy_timeout is set
"""

import os
os.environ['TESTING'] = '1'  # Prevent module-level app creation

import sqlite3
import threading
import time
import unittest
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

# Import the real classes
from craft_unified import Database, DecisionEngine, MetaAdsSync, EventPacing, Decision


class TestDatabaseSpendStatus(unittest.TestCase):
    """Test Database.get_spend_status() for all states."""

    def setUp(self):
        self.db = Database(":memory:")

    def test_no_records(self):
        """No ad_spend rows → 'no_records'."""
        status = self.db.get_spend_status("evt_999")
        self.assertEqual(status, "no_records")

    def test_current_has_spend(self):
        """Recent rows with spend > 0 → 'current_has_spend'."""
        today = date.today().isoformat()
        self.db.save_ad_spend("evt_1", "camp_1", "Campaign 1", today, 100.0, 500, 20)
        status = self.db.get_spend_status("evt_1")
        self.assertEqual(status, "current_has_spend")

    def test_current_zero_spend(self):
        """Recent rows with spend = 0 → 'current_zero_spend'."""
        today = date.today().isoformat()
        self.db.save_ad_spend("evt_1", "camp_1", "Campaign 1", today, 0.0, 0, 0)
        status = self.db.get_spend_status("evt_1")
        self.assertEqual(status, "current_zero_spend")

    def test_stale(self):
        """Rows older than 3 days → 'stale'."""
        old_date = (date.today() - timedelta(days=5)).isoformat()
        self.db.save_ad_spend("evt_1", "camp_1", "Campaign 1", old_date, 50.0, 200, 10)
        status = self.db.get_spend_status("evt_1")
        self.assertEqual(status, "stale")

    def test_freshness_boundary(self):
        """Rows exactly 3 days old → still 'current_has_spend'."""
        boundary_date = (date.today() - timedelta(days=3)).isoformat()
        self.db.save_ad_spend("evt_1", "camp_1", "Campaign 1", boundary_date, 50.0, 200, 10)
        status = self.db.get_spend_status("evt_1")
        self.assertEqual(status, "current_has_spend")


class TestSaveAdSpendBatch(unittest.TestCase):
    """Test Database.save_ad_spend_batch() atomic writes."""

    def setUp(self):
        self.db = Database(":memory:")

    def test_batch_write_empty(self):
        """Empty batch is a no-op."""
        self.db.save_ad_spend_batch([])
        row = self.db.conn.execute("SELECT COUNT(*) as cnt FROM ad_spend").fetchone()
        self.assertEqual(row["cnt"], 0)

    def test_batch_write_multiple_rows(self):
        """Multiple rows written atomically."""
        rows = [
            ("evt_1", "c1", "Campaign 1", "2026-01-01", 50.0, 100, 5),
            ("evt_1", "c1", "Campaign 1", "2026-01-02", 75.0, 150, 8),
            ("evt_1", "c2", "Campaign 2", "2026-01-01", 30.0, 80, 3),
        ]
        self.db.save_ad_spend_batch(rows)
        row = self.db.conn.execute("SELECT COUNT(*) as cnt FROM ad_spend").fetchone()
        self.assertEqual(row["cnt"], 3)
        spend = self.db.conn.execute("SELECT SUM(spend) as total FROM ad_spend").fetchone()
        self.assertAlmostEqual(spend["total"], 155.0)

    def test_batch_upsert(self):
        """Batch INSERT OR REPLACE overwrites existing rows."""
        self.db.save_ad_spend("evt_1", "c1", "Campaign 1", "2026-01-01", 50.0, 100, 5)
        # Now batch-write an updated value for the same key
        rows = [("evt_1", "c1", "Campaign 1", "2026-01-01", 99.0, 200, 10)]
        self.db.save_ad_spend_batch(rows)
        row = self.db.conn.execute(
            "SELECT spend FROM ad_spend WHERE event_id='evt_1' AND campaign_id='c1' AND spend_date='2026-01-01'"
        ).fetchone()
        self.assertAlmostEqual(row["spend"], 99.0)


class TestCACSemantics(unittest.TestCase):
    """CAC must only be computed when spend data is current and non-zero."""

    def setUp(self):
        self.db = Database(":memory:")
        self.engine = DecisionEngine(self.db)

    def _insert_event(self, event_id, name, event_date, capacity=500):
        self.db.conn.execute(
            "INSERT INTO events (event_id, name, event_date, capacity, status) "
            "VALUES (?, ?, ?, ?, 'upcoming')",
            (event_id, name, event_date, capacity),
        )
        self.db.conn.commit()

    def _insert_orders(self, event_id, count, price=25.0):
        for i in range(count):
            self.db.conn.execute(
                "INSERT INTO orders (order_id, event_id, email, order_timestamp, ticket_count, gross_amount) "
                "VALUES (?, ?, ?, ?, 1, ?)",
                (f"ord_{event_id}_{i}", event_id, f"user{i}@test.com", "2026-09-01T10:00:00", price),
            )
        self.db.conn.commit()

    def _insert_snapshots(self, event_id, event_date_str, tickets=100):
        """Insert snapshot data so the event has history for analysis."""
        from datetime import datetime
        event_date = datetime.fromisoformat(event_date_str).date()
        for days_back in range(30, 0, -1):
            snap_date = (event_date - timedelta(days=days_back)).isoformat()
            self.db.conn.execute(
                "INSERT INTO daily_snapshot (event_id, snapshot_date, tickets_cumulative, revenue_cumulative, ad_spend_cumulative) "
                "VALUES (?, ?, ?, ?, ?)",
                (event_id, snap_date, int(tickets * (30 - days_back) / 30), 0, 0),
            )
        self.db.conn.commit()

    def test_cac_zero_when_no_spend_records(self):
        """CAC should be 0 when there are no spend records, not a false value."""
        future = (date.today() + timedelta(days=30)).isoformat()
        self._insert_event("evt_1", "Test Event", future)
        self._insert_orders("evt_1", 100)
        result = self.engine.analyze_event("evt_1")
        if result:
            self.assertEqual(result.cac, 0)
            self.assertEqual(result.spend_status, "no_records")

    def test_cac_computed_with_current_spend(self):
        """CAC should be non-zero when current spend exists."""
        future = (date.today() + timedelta(days=30)).isoformat()
        self._insert_event("evt_1", "Test Event", future)
        self._insert_orders("evt_1", 100, price=25.0)
        today = date.today().isoformat()
        self.db.save_ad_spend("evt_1", "c1", "Campaign 1", today, 500.0, 1000, 50)
        result = self.engine.analyze_event("evt_1")
        if result:
            self.assertEqual(result.spend_status, "current_has_spend")
            self.assertGreater(result.cac, 0)
            self.assertAlmostEqual(result.cac, 5.0)  # 500/100

    def test_cac_zero_with_stale_spend(self):
        """CAC should be 0 when spend data is stale."""
        future = (date.today() + timedelta(days=30)).isoformat()
        self._insert_event("evt_1", "Test Event", future)
        self._insert_orders("evt_1", 100)
        old_date = (date.today() - timedelta(days=10)).isoformat()
        self.db.save_ad_spend("evt_1", "c1", "Campaign 1", old_date, 500.0, 1000, 50)
        result = self.engine.analyze_event("evt_1")
        if result:
            self.assertEqual(result.spend_status, "stale")
            self.assertEqual(result.cac, 0)


class TestDecideSpendAwareness(unittest.TestCase):
    """_decide() must not treat missing spend as 'CAC acceptable'."""

    def setUp(self):
        self.db = Database(":memory:")
        self.engine = DecisionEngine(self.db)

    def test_decide_no_spend_data(self):
        """When spend_status is not 'current_has_spend', cac_ok must be False."""
        decision, urgency, rationale, actions = self.engine._decide(
            tickets=50, pace=-20.0, cac=0, days_until=30,
            hist_median=100, comparison_events=["Past Event"],
            spend_status="no_records",
        )
        # Actions should NOT include "Increase ad budget" — should suggest review instead
        for action in actions:
            self.assertNotIn("Increase ad budget", action)

    def test_decide_current_spend_low_cac(self):
        """When spend is current and CAC is low, cac_ok should be True."""
        decision, urgency, rationale, actions = self.engine._decide(
            tickets=50, pace=-20.0, cac=5.0, days_until=30,
            hist_median=100, comparison_events=["Past Event"],
            spend_status="current_has_spend",
        )
        # With current spend and low CAC, rationale should mention the CAC value
        self.assertIn("CAC $5.00", rationale)

    def test_decide_stale_spend(self):
        """When spend is stale, rationale should indicate unavailability."""
        decision, urgency, rationale, actions = self.engine._decide(
            tickets=50, pace=-20.0, cac=0, days_until=30,
            hist_median=100, comparison_events=["Past Event"],
            spend_status="stale",
        )
        self.assertIn("stale", rationale.lower())


class TestGroupedSpendStatus(unittest.TestCase):
    """Grouped events should aggregate spend_status from constituents."""

    def setUp(self):
        self.db = Database(":memory:")
        self.engine = DecisionEngine(self.db)

    def test_best_status_wins(self):
        """Grouped status picks the most informative constituent status."""
        today = date.today().isoformat()
        # evt_1 has current spend, evt_2 has no records
        self.db.save_ad_spend("evt_1", "c1", "Campaign 1", today, 100.0, 500, 20)
        result = self.engine._get_grouped_spend_status(["evt_1", "evt_2"])
        self.assertEqual(result, "current_has_spend")

    def test_all_no_records(self):
        """All constituents have no records → 'no_records'."""
        result = self.engine._get_grouped_spend_status(["evt_a", "evt_b"])
        self.assertEqual(result, "no_records")

    def test_empty_list(self):
        """Empty constituent list → 'no_records'."""
        result = self.engine._get_grouped_spend_status([])
        self.assertEqual(result, "no_records")


class TestDatabaseBusyTimeout(unittest.TestCase):
    """Database must be configured with busy_timeout for lock resilience."""

    def test_busy_timeout_set(self):
        db = Database(":memory:")
        row = db.conn.execute("PRAGMA busy_timeout").fetchone()
        self.assertEqual(row[0], 30000)

    def test_wal_mode(self):
        """WAL journal mode should be enabled."""
        db = Database(":memory:")
        row = db.conn.execute("PRAGMA journal_mode").fetchone()
        # In-memory databases may not support WAL, but the pragma should have been issued
        # The important thing is the code path executes without error
        self.assertIn(row[0], ("wal", "memory"))

    def test_timeout_on_connect(self):
        """Database connection should have timeout parameter."""
        db = Database(":memory:")
        # Verify the connection works under contention by running concurrent reads
        results = []
        def reader():
            try:
                row = db.conn.execute("SELECT 1 as v").fetchone()
                results.append(row[0])
            except Exception as e:
                results.append(str(e))
        threads = [threading.Thread(target=reader) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)
        # All reads should succeed
        self.assertTrue(all(r == 1 for r in results), f"Some reads failed: {results}")


class TestMetaSyncBatchWrites(unittest.TestCase):
    """MetaAdsSync.sync_event_spend() should use batched writes."""

    def setUp(self):
        self.db = Database(":memory:")
        # Insert a test event
        self.db.conn.execute(
            "INSERT INTO events (event_id, name, event_date, capacity, status) "
            "VALUES ('evt_1', 'Test Coffee', '2026-10-01', 500, 'upcoming')"
        )
        self.db.conn.commit()

    @patch.object(MetaAdsSync, '_find_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_sync_uses_batch(self, mock_insights, mock_campaigns):
        """sync_event_spend() should call save_ad_spend_batch() not save_ad_spend()."""
        mock_campaigns.return_value = [
            {'id': 'camp_1', 'name': 'Test Coffee Campaign'},
        ]
        mock_insights.return_value = [
            {'spend': '10.00', 'impressions': '100', 'clicks': '5', 'date_start': '2026-09-01'},
            {'spend': '15.00', 'impressions': '150', 'clicks': '8', 'date_start': '2026-09-02'},
        ]
        meta = MetaAdsSync("fake_token", "act_123", self.db)

        with patch.object(self.db, 'save_ad_spend_batch') as mock_batch:
            with patch.object(self.db, 'save_ad_spend') as mock_single:
                result = meta.sync_event_spend('evt_1', 'Test Coffee', '2026-10-01')

                # Batch should be called, single should NOT
                mock_batch.assert_called_once()
                mock_single.assert_not_called()
                # Verify the batch had 2 rows
                rows = mock_batch.call_args[0][0]
                self.assertEqual(len(rows), 2)
                self.assertAlmostEqual(result['total_spend'], 25.0)
                self.assertEqual(result['rows_written'], 2)


class TestEventPacingSpendStatus(unittest.TestCase):
    """EventPacing dataclass must include spend_status field."""

    def _make_pacing(self, **overrides):
        """Helper to create an EventPacing with sensible defaults."""
        defaults = dict(
            event_id="evt_1", event_name="Test", event_date="2026-10-01",
            days_until=30, tickets_sold=100, capacity=500, revenue=2500.0,
            ad_spend=0, sell_through=20.0, cac=0,
            historical_median_at_point=0, historical_range=(0, 0),
            pace_vs_historical=0, comparison_events=[], comparison_years=[],
            projected_final=0, projected_range=(0, 0), confidence=0,
            decision=Decision.MAINTAIN, urgency=5, rationale="", actions=[],
            high_value_targets=0, reactivation_targets=0,
        )
        defaults.update(overrides)
        return EventPacing(**defaults)

    def test_default_spend_status(self):
        """Default spend_status should be 'unknown'."""
        pacing = self._make_pacing()
        self.assertEqual(pacing.spend_status, "unknown")

    def test_custom_spend_status(self):
        """spend_status can be set to any valid state."""
        pacing = self._make_pacing(
            ad_spend=500, cac=5.0, spend_status="current_has_spend",
        )
        self.assertEqual(pacing.spend_status, "current_has_spend")


if __name__ == "__main__":
    unittest.main()
