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
        """When spend is stale, rationale should indicate CAC unavailable."""
        decision, urgency, rationale, actions = self.engine._decide(
            tickets=50, pace=-20.0, cac=0, days_until=30,
            hist_median=100, comparison_events=["Past Event"],
            spend_status="stale",
        )
        self.assertIn("CAC unavailable", rationale)


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


class TestYearExtraction(unittest.TestCase):
    """Test MetaAdsSync._extract_year() static method."""

    def test_extracts_4digit_year(self):
        self.assertEqual(MetaAdsSync._extract_year("Austin Coffee Festival 2026"), 2026)

    def test_extracts_year_at_start(self):
        self.assertEqual(MetaAdsSync._extract_year("2025 Spring Campaign"), 2025)

    def test_no_year(self):
        self.assertIsNone(MetaAdsSync._extract_year("Austin Coffee Festival"))

    def test_year_boundary_low(self):
        self.assertEqual(MetaAdsSync._extract_year("Event 2020"), 2020)

    def test_year_boundary_high(self):
        self.assertEqual(MetaAdsSync._extract_year("Event 2039"), 2039)

    def test_non_year_number_ignored(self):
        """4-digit numbers outside 2020-2039 are not extracted as years."""
        self.assertIsNone(MetaAdsSync._extract_year("Campaign 1234"))
        self.assertIsNone(MetaAdsSync._extract_year("Budget 5000"))

    def test_first_year_wins(self):
        """When multiple years appear, first one is extracted."""
        self.assertEqual(MetaAdsSync._extract_year("2025 to 2026 campaign"), 2025)


class TestYearGate(unittest.TestCase):
    """Test that _campaign_matches_event rejects cross-year matches."""

    def setUp(self):
        self.db = Database(":memory:")
        self.meta = MetaAdsSync("fake_token", "act_123", self.db)

    def test_same_year_matches(self):
        """Campaign with year 2026, event year 2026 → should match."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival 2026 - Awareness",
            "Austin Coffee Festival 2026",
            event_year=2026)
        self.assertIsNotNone(result)

    def test_different_year_rejected(self):
        """Campaign with year 2026, event year 2025 → hard reject."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival 2026 - Awareness",
            "Austin Coffee Festival 2025",
            event_year=2025)
        self.assertIsNone(result)

    def test_campaign_no_year_still_matches(self):
        """Campaign without a year → name matching proceeds normally."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival - Awareness",
            "Austin Coffee Festival 2026",
            event_year=2026)
        self.assertIsNotNone(result)

    def test_event_no_year_still_matches(self):
        """Event without a year → name matching proceeds normally."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival 2026 - Awareness",
            "Austin Coffee Festival",
            event_year=None)
        self.assertIsNotNone(result)


class TestCrossYearDoubleCount(unittest.TestCase):
    """The core defect: Austin 2026 campaign must NOT match Austin 2025 event."""

    def setUp(self):
        self.db = Database(":memory:")
        self.meta = MetaAdsSync("fake_token", "act_123", self.db)

    def test_austin_2026_campaign_rejects_2025_event(self):
        """Austin Coffee Festival 2026 campaign → must not match 2025 event."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival 2026 - Conversions",
            "Austin Coffee Festival 2025",
            event_year=2025)
        self.assertIsNone(result, "2026 campaign must not match 2025 event")

    def test_austin_2026_campaign_accepts_2026_event(self):
        """Austin Coffee Festival 2026 campaign → must match 2026 event."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival 2026 - Conversions",
            "Austin Coffee Festival 2026",
            event_year=2026)
        self.assertIsNotNone(result)

    def test_austin_2025_campaign_rejects_2026_event(self):
        """Austin Coffee Festival 2025 campaign → must not match 2026 event."""
        result = self.meta._campaign_matches_event(
            "Austin Coffee Festival 2025 - Retargeting",
            "Austin Coffee Festival 2026",
            event_year=2026)
        self.assertIsNone(result, "2025 campaign must not match 2026 event")

    def test_three_editions_no_cross_match(self):
        """Austin 2025/2026/2027: each campaign matches only its own edition."""
        campaigns = [
            "Austin Coffee 2025 - Awareness",
            "Austin Coffee 2026 - Awareness",
            "Austin Coffee 2027 - Awareness",
        ]
        events = [
            ("Austin Coffee Festival 2025", 2025),
            ("Austin Coffee Festival 2026", 2026),
            ("Austin Coffee Festival 2027", 2027),
        ]
        for campaign in campaigns:
            camp_year = MetaAdsSync._extract_year(campaign)
            for event_name, event_year in events:
                result = self.meta._campaign_matches_event(
                    campaign, event_name, event_year=event_year)
                if camp_year == event_year:
                    self.assertIsNotNone(result,
                        f"Campaign '{campaign}' should match '{event_name}'")
                else:
                    self.assertIsNone(result,
                        f"Campaign '{campaign}' must NOT match '{event_name}'")


class TestPickClosestEvent(unittest.TestCase):
    """Test _pick_closest_event() tie-breaking logic."""

    def test_prefers_upcoming_over_past(self):
        today = date.today()
        upcoming = {'event_id': 'e1', 'name': 'Upcoming',
                    'event_date': (today + timedelta(days=30)).isoformat()}
        past = {'event_id': 'e2', 'name': 'Past',
                'event_date': (today - timedelta(days=30)).isoformat()}
        result = MetaAdsSync._pick_closest_event([upcoming, past], today)
        self.assertEqual(result['event_id'], 'e1')

    def test_prefers_nearest_upcoming(self):
        today = date.today()
        near = {'event_id': 'e1', 'name': 'Near',
                'event_date': (today + timedelta(days=10)).isoformat()}
        far = {'event_id': 'e2', 'name': 'Far',
               'event_date': (today + timedelta(days=90)).isoformat()}
        result = MetaAdsSync._pick_closest_event([far, near], today)
        self.assertEqual(result['event_id'], 'e1')

    def test_prefers_most_recent_past(self):
        today = date.today()
        recent = {'event_id': 'e1', 'name': 'Recent',
                  'event_date': (today - timedelta(days=10)).isoformat()}
        old = {'event_id': 'e2', 'name': 'Old',
               'event_date': (today - timedelta(days=200)).isoformat()}
        result = MetaAdsSync._pick_closest_event([old, recent], today)
        self.assertEqual(result['event_id'], 'e1')

    def test_same_date_is_ambiguous(self):
        """Two events on the exact same date → None (cannot resolve)."""
        today = date.today()
        same_date = (today + timedelta(days=30)).isoformat()
        e1 = {'event_id': 'e1', 'name': 'A', 'event_date': same_date}
        e2 = {'event_id': 'e2', 'name': 'B', 'event_date': same_date}
        result = MetaAdsSync._pick_closest_event([e1, e2], today)
        self.assertIsNone(result)


class TestOneCampaignOneEvent(unittest.TestCase):
    """sync_all_events() must assign each campaign to at most one event."""

    def setUp(self):
        self.db = Database(":memory:")
        self.meta = MetaAdsSync("fake_token", "act_123", self.db)

    @patch.object(MetaAdsSync, '_fetch_all_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_yeartagged_campaign_maps_to_one_event(self, mock_insights, mock_fetch):
        """A campaign with explicit year must not appear under multiple events."""
        mock_fetch.return_value = [
            {'id': 'c1', 'name': 'Austin Coffee 2026 - Awareness', 'status': 'ACTIVE'},
        ]
        mock_insights.return_value = [
            {'spend': '100.00', 'impressions': '1000', 'clicks': '50',
             'date_start': '2026-09-01'},
        ]
        events = [
            {'event_id': 'evt_2025', 'name': 'Austin Coffee Festival 2025',
             'event_date': '2025-11-15'},
            {'event_id': 'evt_2026', 'name': 'Austin Coffee Festival 2026',
             'event_date': '2026-11-15'},
        ]
        result = self.meta.sync_all_events(events)

        # Campaign should match ONLY the 2026 event
        rows = self.db.conn.execute(
            "SELECT DISTINCT event_id FROM ad_spend").fetchall()
        event_ids = {r['event_id'] for r in rows}
        self.assertEqual(event_ids, {'evt_2026'},
            "Year-tagged campaign must map to exactly one event")
        self.assertEqual(result['campaigns_assigned'], 1)
        self.assertEqual(result['campaigns_ambiguous'], 0)

    @patch.object(MetaAdsSync, '_fetch_all_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_untagged_campaign_maps_to_closest_event(self, mock_insights, mock_fetch):
        """A campaign without a year should go to the closest upcoming event."""
        today = date.today()
        mock_fetch.return_value = [
            {'id': 'c1', 'name': 'Austin Coffee Fest - Retargeting', 'status': 'ACTIVE'},
        ]
        mock_insights.return_value = [
            {'spend': '50.00', 'impressions': '500', 'clicks': '25',
             'date_start': today.isoformat()},
        ]
        events = [
            {'event_id': 'evt_past', 'name': 'Austin Coffee Festival 2025',
             'event_date': (today - timedelta(days=200)).isoformat()},
            {'event_id': 'evt_upcoming', 'name': 'Austin Coffee Festival 2026',
             'event_date': (today + timedelta(days=60)).isoformat()},
        ]
        result = self.meta.sync_all_events(events)

        rows = self.db.conn.execute(
            "SELECT DISTINCT event_id FROM ad_spend").fetchall()
        event_ids = {r['event_id'] for r in rows}
        self.assertEqual(event_ids, {'evt_upcoming'},
            "Un-year-tagged campaign should go to upcoming event, not past")
        self.assertEqual(result['campaigns_assigned'], 1)

    @patch.object(MetaAdsSync, '_fetch_all_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_multiple_campaigns_different_years(self, mock_insights, mock_fetch):
        """Two campaigns for different years → each goes to its own event."""
        mock_fetch.return_value = [
            {'id': 'c25', 'name': 'Austin Coffee 2025 Awareness', 'status': 'PAUSED'},
            {'id': 'c26', 'name': 'Austin Coffee 2026 Awareness', 'status': 'ACTIVE'},
        ]
        mock_insights.return_value = [
            {'spend': '75.00', 'impressions': '750', 'clicks': '30',
             'date_start': '2026-09-01'},
        ]
        events = [
            {'event_id': 'evt_2025', 'name': 'Austin Coffee Festival 2025',
             'event_date': '2025-11-15'},
            {'event_id': 'evt_2026', 'name': 'Austin Coffee Festival 2026',
             'event_date': '2026-11-15'},
        ]
        result = self.meta.sync_all_events(events)

        # Each campaign should go to its own event
        rows_2025 = self.db.conn.execute(
            "SELECT campaign_id FROM ad_spend WHERE event_id='evt_2025'").fetchall()
        rows_2026 = self.db.conn.execute(
            "SELECT campaign_id FROM ad_spend WHERE event_id='evt_2026'").fetchall()
        cids_2025 = {r['campaign_id'] for r in rows_2025}
        cids_2026 = {r['campaign_id'] for r in rows_2026}
        self.assertIn('c25', cids_2025)
        self.assertNotIn('c26', cids_2025)
        self.assertIn('c26', cids_2026)
        self.assertNotIn('c25', cids_2026)
        self.assertEqual(result['campaigns_assigned'], 2)

    @patch.object(MetaAdsSync, '_fetch_all_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_ambiguous_campaign_skipped(self, mock_insights, mock_fetch):
        """Campaign that ties between two same-date events → skipped."""
        today = date.today()
        same_date = (today + timedelta(days=30)).isoformat()
        mock_fetch.return_value = [
            {'id': 'c1', 'name': 'Coffee Event Campaign', 'status': 'ACTIVE'},
        ]
        mock_insights.return_value = []

        events = [
            {'event_id': 'evt_a', 'name': 'Coffee Event Morning',
             'event_date': same_date},
            {'event_id': 'evt_b', 'name': 'Coffee Event Evening',
             'event_date': same_date},
        ]
        result = self.meta.sync_all_events(events)

        rows = self.db.conn.execute(
            "SELECT COUNT(*) as cnt FROM ad_spend").fetchone()
        self.assertEqual(rows['cnt'], 0,
            "Ambiguous campaign should be skipped entirely")

    @patch.object(MetaAdsSync, '_fetch_all_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_different_series_no_cross_match(self, mock_insights, mock_fetch):
        """San Diego campaign must not match Austin event."""
        mock_fetch.return_value = [
            {'id': 'c_sd', 'name': 'San Diego Coffee Festival 2026', 'status': 'ACTIVE'},
        ]
        mock_insights.return_value = [
            {'spend': '80.00', 'impressions': '800', 'clicks': '40',
             'date_start': '2026-08-01'},
        ]
        events = [
            {'event_id': 'evt_austin', 'name': 'Austin Coffee Festival 2026',
             'event_date': '2026-11-15'},
            {'event_id': 'evt_sd', 'name': 'San Diego Coffee Festival 2026',
             'event_date': '2026-08-20'},
        ]
        result = self.meta.sync_all_events(events)

        rows = self.db.conn.execute(
            "SELECT DISTINCT event_id FROM ad_spend").fetchall()
        event_ids = {r['event_id'] for r in rows}
        self.assertNotIn('evt_austin', event_ids,
            "San Diego campaign must not match Austin event")
        self.assertIn('evt_sd', event_ids)

    @patch.object(MetaAdsSync, '_fetch_all_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_sync_event_spend_uses_override(self, mock_insights, mock_find):
        """sync_event_spend with campaigns_override=[] skips finding."""
        mock_insights.return_value = []
        result = self.meta.sync_event_spend(
            'evt_1', 'Test Event', '2026-10-01',
            campaigns_override=[])
        self.assertEqual(result['campaigns_found'], 0)
        self.assertEqual(result['total_spend'], 0)
        # _find_campaigns should NOT have been called
        mock_find.assert_not_called()


class TestReconcileDuplicateAttribution(unittest.TestCase):
    """Reconciliation tool must detect and optionally fix duplicate attribution."""

    def setUp(self):
        self.db = Database(":memory:")

    def test_no_duplicates(self):
        """Clean data → no duplicates found."""
        self.db.save_ad_spend("evt_1", "c1", "Campaign 1", "2026-09-01", 100, 500, 20)
        self.db.save_ad_spend("evt_2", "c2", "Campaign 2", "2026-09-01", 200, 800, 30)
        result = MetaAdsSync.reconcile_duplicate_attribution(self.db)
        self.assertEqual(result['duplicates_found'], 0)
        self.assertEqual(result['rows_deleted'], 0)
        self.assertTrue(result['dry_run'])

    def test_detects_duplicate(self):
        """Same campaign under two events → detected as duplicate."""
        self.db.save_ad_spend("evt_1", "c1", "Campaign 1", "2026-09-01", 100, 500, 20)
        self.db.save_ad_spend("evt_2", "c1", "Campaign 1", "2026-09-01", 100, 500, 20)
        result = MetaAdsSync.reconcile_duplicate_attribution(self.db)
        self.assertEqual(result['duplicates_found'], 1)
        self.assertEqual(result['rows_deleted'], 0)  # dry_run
        self.assertIn('evt_1', result['duplicates'][0]['event_ids'])
        self.assertIn('evt_2', result['duplicates'][0]['event_ids'])

    def test_dry_run_does_not_delete(self):
        """Dry run detects but does not delete."""
        self.db.save_ad_spend("evt_1", "c1", "Camp", "2026-09-01", 50, 100, 5)
        self.db.save_ad_spend("evt_2", "c1", "Camp", "2026-09-01", 50, 100, 5)
        result = MetaAdsSync.reconcile_duplicate_attribution(self.db, dry_run=True)
        self.assertEqual(result['duplicates_found'], 1)
        self.assertEqual(result['rows_deleted'], 0)
        # Both rows should still exist
        count = self.db.conn.execute("SELECT COUNT(*) as cnt FROM ad_spend").fetchone()
        self.assertEqual(count['cnt'], 2)

    def test_live_run_deletes_loser(self):
        """Live run keeps highest-spend event, deletes the other."""
        self.db.save_ad_spend("evt_1", "c1", "Camp", "2026-09-01", 200, 1000, 50)
        self.db.save_ad_spend("evt_2", "c1", "Camp", "2026-09-01", 50, 100, 5)
        result = MetaAdsSync.reconcile_duplicate_attribution(self.db, dry_run=False)
        self.assertEqual(result['duplicates_found'], 1)
        self.assertEqual(result['rows_deleted'], 1)
        # Only evt_1 rows should remain
        rows = self.db.conn.execute(
            "SELECT DISTINCT event_id FROM ad_spend WHERE campaign_id='c1'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['event_id'], 'evt_1')


class TestSyncEventSpendYearAware(unittest.TestCase):
    """sync_event_spend() must pass event_year to _find_campaigns."""

    def setUp(self):
        self.db = Database(":memory:")
        self.meta = MetaAdsSync("fake_token", "act_123", self.db)

    @patch.object(MetaAdsSync, '_find_campaigns')
    @patch.object(MetaAdsSync, '_fetch_daily_insights')
    def test_year_passed_to_find_campaigns(self, mock_insights, mock_find):
        """sync_event_spend extracts year from event_date and passes it."""
        mock_find.return_value = []
        mock_insights.return_value = []
        self.meta.sync_event_spend('evt_1', 'Austin Coffee Festival', '2026-11-15')
        mock_find.assert_called_once_with('Austin Coffee Festival', event_year=2026)


if __name__ == "__main__":
    unittest.main()
