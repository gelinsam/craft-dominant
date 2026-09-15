"""End-to-end proof that the EXISTING spend pipeline produces Spend @ Nd.

Nothing here is new behaviour. The chain already exists:

    daily Meta insights
      -> ad_spend
      -> backfill_ad_spend_into_snapshots()
      -> daily_snapshots.ad_spend_cumulative
      -> historical comparison at T-N
      -> "Spend @ Nd" in the Events dashboard

It has never produced a number in production only because `ad_spend` is empty.
These tests drive the chain with spend rows written the way the corrected
attribution will write them — one canonical row per festival edition — and
assert the dashboard-facing values appear.

They also pin the invariant that makes canonical-row storage safe: the
dashboard SUMS spend across a day-group's constituent rows, so a Meta dollar
must be stored exactly once.
"""

import datetime
import os
import sys
import tempfile
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from craft_unified import Database, canonical_edition_event  # noqa: E402


class _Pipeline(unittest.TestCase):
    EVENT_DATE = datetime.date(2025, 10, 25)

    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "spend.db"))
        self.sessions = []
        for i in range(3):
            eid = f"S{i}"
            self.db.upsert_event({
                "event_id": eid, "name": "Austin Coffee Festival",
                "event_type": "coffee", "city": "Austin",
                "event_date": self.EVENT_DATE.isoformat(),
                "capacity": 2000, "status": "completed",
            })
            self.sessions.append({"event_id": eid, "name": "Austin Coffee Festival",
                                  "event_date": self.EVENT_DATE.isoformat()})
        self.canonical = canonical_edition_event(self.sessions)["event_id"]

    def _snapshots(self, event_id, per_day_tickets=10):
        """Daily snapshots from T-60 to the event, as the sync builds them."""
        cum = 0
        for back in range(60, -1, -1):
            day = self.EVENT_DATE - datetime.timedelta(days=back)
            cum += per_day_tickets
            self.db.save_snapshot(event_id, day.isoformat(), back, cum, cum * 40.0,
                                  per_day_tickets, per_day_tickets * 40.0,
                                  per_day_tickets, cum / 2000 * 100, spend=0.0)

    def _spend(self, event_id, per_day, days=60, campaign_id="camp1"):
        rows = []
        for back in range(days, -1, -1):
            day = self.EVENT_DATE - datetime.timedelta(days=back)
            rows.append((event_id, campaign_id, "Instagram post: ACF ...",
                         day.isoformat(), per_day, 100, 5))
        self.db.save_ad_spend_batch(rows)
        return rows


class TestSpendAtNd(_Pipeline):
    def test_backfill_populates_cumulative_spend_at_t_minus_32(self):
        self._snapshots(self.canonical)
        self._spend(self.canonical, per_day=10.0)          # $10/day for 61 days
        self.db.backfill_ad_spend_into_snapshots()

        snap = self.db.get_snapshot_at_days(self.canonical, 32)
        self.assertIsNotNone(snap, "no snapshot at T-32")
        self.assertEqual(snap["days_before_event"], 32)
        # Spend ran T-60..T-32 inclusive = 29 days * $10
        self.assertAlmostEqual(float(snap["ad_spend_cumulative"]), 290.0, places=2)

    def test_spend_at_days_out_accessor_agrees(self):
        self._snapshots(self.canonical)
        self._spend(self.canonical, per_day=10.0)
        self.db.backfill_ad_spend_into_snapshots()
        self.assertAlmostEqual(
            self.db.get_event_spend_at_days_out(self.canonical, 32), 290.0, places=2)

    def test_cumulative_is_monotonic_across_the_run_up(self):
        self._snapshots(self.canonical)
        self._spend(self.canonical, per_day=10.0)
        self.db.backfill_ad_spend_into_snapshots()
        prev = -1.0
        for back in (60, 50, 40, 32, 20, 10, 0):
            snap = self.db.get_snapshot_at_days(self.canonical, back)
            cur = float(snap["ad_spend_cumulative"])
            self.assertGreaterEqual(cur, prev)
            prev = cur

    def test_zero_spend_stays_zero_rather_than_null(self):
        self._snapshots(self.canonical)
        self.db.backfill_ad_spend_into_snapshots()
        snap = self.db.get_snapshot_at_days(self.canonical, 32)
        self.assertEqual(float(snap["ad_spend_cumulative"] or 0), 0.0)

    def test_backfill_is_idempotent(self):
        self._snapshots(self.canonical)
        self._spend(self.canonical, per_day=10.0)
        self.db.backfill_ad_spend_into_snapshots()
        first = self.db.get_event_spend_at_days_out(self.canonical, 32)
        self.db.backfill_ad_spend_into_snapshots()
        self.assertEqual(self.db.get_event_spend_at_days_out(self.canonical, 32), first)


class TestCurrentSpendAndCac(_Pipeline):
    def test_event_spend_aggregates_the_canonical_row(self):
        self._spend(self.canonical, per_day=10.0)
        self.assertAlmostEqual(self.db.get_event_spend(self.canonical), 610.0, places=2)

    def test_cac_is_spend_over_tickets(self):
        self._spend(self.canonical, per_day=10.0)
        spend = self.db.get_event_spend(self.canonical)
        tickets = 610
        self.assertAlmostEqual(spend / tickets, 1.0, places=6)

    def test_spend_status_reports_records_exist(self):
        self._spend(self.canonical, per_day=10.0)
        self.assertNotEqual(self.db.get_spend_status(self.canonical), "no_records")

    def test_missing_spend_is_no_records_not_zero(self):
        """A false $0 must stay distinguishable from a real $0."""
        self.assertEqual(self.db.get_spend_status(self.canonical), "no_records")


class TestNoSpendDuplication(_Pipeline):
    def test_edition_total_equals_single_campaign_spend(self):
        """Summing across sibling rows must not multiply the campaign's spend."""
        self._spend(self.canonical, per_day=10.0)
        total = sum(self.db.get_event_spend(s["event_id"]) for s in self.sessions)
        self.assertAlmostEqual(total, 610.0, places=2)

    def test_non_canonical_sessions_hold_no_spend(self):
        self._spend(self.canonical, per_day=10.0)
        for s in self.sessions:
            if s["event_id"] == self.canonical:
                continue
            self.assertEqual(self.db.get_event_spend(s["event_id"]), 0)

    def test_writing_to_every_session_would_multiply_it(self):
        """Documents exactly why canonical-row storage is required."""
        for s in self.sessions:
            self._spend(s["event_id"], per_day=10.0)
        total = sum(self.db.get_event_spend(s["event_id"]) for s in self.sessions)
        self.assertAlmostEqual(total, 1830.0, places=2)
        self.assertNotAlmostEqual(total, 610.0, places=2)

    def test_resync_is_idempotent_on_the_unique_key(self):
        self._spend(self.canonical, per_day=10.0)
        before = self.db.get_event_spend(self.canonical)
        self._spend(self.canonical, per_day=10.0)
        self.assertAlmostEqual(self.db.get_event_spend(self.canonical), before, places=2)

    def test_two_campaigns_on_one_edition_both_count(self):
        self._spend(self.canonical, per_day=10.0, campaign_id="camp1")
        self._spend(self.canonical, per_day=5.0, campaign_id="camp2")
        self.assertAlmostEqual(self.db.get_event_spend(self.canonical), 915.0, places=2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
