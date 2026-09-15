"""Every constituent day of a festival resolves the same paid-media context.

Spend is STORED once, on one canonical raw row per edition. That is a storage
detail. Meta advertises the festival, so Saturday and Sunday of one edition
share a single paid context — and the portfolio must still count that spend
once, not once per view of it.

  festival stored spend = $1,000
  Saturday contextual view = $1,000
  Sunday contextual view   = $1,000
  portfolio total          = $1,000     <- not $2,000

CAC follows the same scope: festival spend over festival tickets, never
festival spend over one day's tickets.
"""

import datetime
import os
import sys
import tempfile
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from craft_unified import (  # noqa: E402
    Database,
    canonical_edition_event,
    festival_edition_key,
    portfolio_spend_total,
)


class _Fixture(unittest.TestCase):
    """Production-shaped edition: N sessions per day across two days."""

    NAME = "Austin Coffee Festival"
    DAYS = [("2025-10-25", 3), ("2025-10-26", 3)]
    SPEND_PER_DAY = 10.0
    RUN_UP_DAYS = 60

    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "shared.db"))
        self.rows = []
        n = 0
        for d, count in self.DAYS:
            for _ in range(count):
                eid = f"S{n}"
                self.db.upsert_event({
                    "event_id": eid, "name": self.NAME, "event_type": "coffee",
                    "city": "Austin", "event_date": d, "capacity": 2000,
                    "status": "completed",
                })
                self.rows.append({"event_id": eid, "name": self.NAME, "event_date": d})
                n += 1
        self.canonical = canonical_edition_event(self.rows)["event_id"]
        self.last_date = datetime.date.fromisoformat(self.DAYS[-1][0])

        # 10 tickets/day per session, so the edition denominator is real.
        for row in self.rows:
            for back in range(self.RUN_UP_DAYS, -1, -1):
                day = datetime.date.fromisoformat(row["event_date"]) - datetime.timedelta(days=back)
                self.db.insert_order({
                    "order_id": f"{row['event_id']}-{back}", "event_id": row["event_id"],
                    "email": f"c{back}@example.com",
                    "order_timestamp": day.isoformat() + "T10:00:00",
                    "ticket_count": 1, "gross_amount": 40.0, "net_amount": 36.0,
                    "days_before_event": back,
                })

    def _write_edition_spend(self, campaign_id="camp1"):
        """One stored stream on the canonical row, as the sync now writes it."""
        rows = []
        for back in range(self.RUN_UP_DAYS, -1, -1):
            day = self.last_date - datetime.timedelta(days=back)
            rows.append((self.canonical, campaign_id, "Instagram post: ACF ...",
                         day.isoformat(), self.SPEND_PER_DAY, 100, 5))
        self.db.save_ad_spend_batch(rows)
        return round(self.SPEND_PER_DAY * (self.RUN_UP_DAYS + 1), 2)

    def _snapshots(self, event_id):
        cum = 0
        ev_date = datetime.date.fromisoformat(
            next(r["event_date"] for r in self.rows if r["event_id"] == event_id))
        for back in range(self.RUN_UP_DAYS, -1, -1):
            day = ev_date - datetime.timedelta(days=back)
            cum += 10
            self.db.save_snapshot(event_id, day.isoformat(), back, cum, cum * 40.0,
                                  10, 400.0, 10, cum / 2000 * 100, spend=0.0)

    def saturday_rows(self):
        return [r for r in self.rows if r["event_date"] == self.DAYS[0][0]]

    def sunday_rows(self):
        return [r for r in self.rows if r["event_date"] == self.DAYS[1][0]]


# ---------------------------------------------------------------------------
# Austin: Saturday and Sunday share one festival paid context
# ---------------------------------------------------------------------------
class TestAustinSharedReads(_Fixture):
    def test_canonical_row_is_on_saturday(self):
        """Makes the asymmetry the old behaviour produced explicit."""
        self.assertIn(self.canonical, [r["event_id"] for r in self.saturday_rows()])

    def test_saturday_resolves_festival_spend(self):
        total = self._write_edition_spend()
        for row in self.saturday_rows():
            self.assertAlmostEqual(
                self.db.get_edition_spend(row["event_id"]), total, places=2)

    def test_sunday_resolves_the_same_festival_spend(self):
        """The regression: Sunday holds no stored row, and must not read $0."""
        total = self._write_edition_spend()
        for row in self.sunday_rows():
            self.assertAlmostEqual(
                self.db.get_edition_spend(row["event_id"]), total, places=2)
            self.assertGreater(self.db.get_edition_spend(row["event_id"]), 0)

    def test_both_days_agree_exactly(self):
        self._write_edition_spend()
        sat = self.db.get_edition_spend(self.saturday_rows()[0]["event_id"])
        sun = self.db.get_edition_spend(self.sunday_rows()[0]["event_id"])
        self.assertEqual(sat, sun)

    def test_raw_per_row_read_still_shows_storage_truth(self):
        """The raw accessor is unchanged; only the edition reader is new."""
        self._write_edition_spend()
        self.assertGreater(self.db.get_event_spend(self.canonical), 0)
        for row in self.sunday_rows():
            self.assertEqual(self.db.get_event_spend(row["event_id"]), 0)

    def test_spend_status_is_shared_across_days(self):
        self._write_edition_spend()
        statuses = {self.db.get_edition_spend_status(r["event_id"]) for r in self.rows}
        self.assertEqual(len(statuses), 1)
        self.assertNotIn("no_records", statuses)

    def test_missing_spend_still_reads_no_records_everywhere(self):
        """False zero must stay distinguishable from a real zero."""
        for row in self.rows:
            self.assertEqual(self.db.get_edition_spend_status(row["event_id"]),
                             "no_records")


# ---------------------------------------------------------------------------
# Festival-level CAC
# ---------------------------------------------------------------------------
class TestFestivalCac(_Fixture):
    def test_denominator_is_all_six_sessions(self):
        self._write_edition_spend()
        edition_tickets = self.db.get_edition_tickets(self.rows[0]["event_id"])
        per_session = sum(o["ticket_count"] for o in
                          self.db.get_orders_for_event(self.rows[0]["event_id"]))
        self.assertEqual(edition_tickets, per_session * 6)

    def test_cac_identical_from_any_constituent_row(self):
        total = self._write_edition_spend()
        cacs = set()
        for row in self.rows:
            tickets = self.db.get_edition_tickets(row["event_id"])
            cacs.add(round(total / tickets, 6))
        self.assertEqual(len(cacs), 1)

    def test_festival_cac_is_lower_than_single_day_cac(self):
        """Festival spend over one day's tickets would overstate CAC."""
        total = self._write_edition_spend()
        edition_tickets = self.db.get_edition_tickets(self.rows[0]["event_id"])
        one_day_tickets = sum(
            sum(o["ticket_count"] for o in self.db.get_orders_for_event(r["event_id"]))
            for r in self.saturday_rows())
        self.assertLess(total / edition_tickets, total / one_day_tickets)


# ---------------------------------------------------------------------------
# Historical Spend @ Nd
# ---------------------------------------------------------------------------
class TestHistoricalSpendAtNd(_Fixture):
    def test_spend_at_t32_resolves_from_the_canonical_day(self):
        self._snapshots(self.canonical)
        self._write_edition_spend()
        self.db.backfill_ad_spend_into_snapshots()
        self.assertGreater(
            self.db.get_edition_spend_at_days_out(self.canonical, 32), 0)

    def test_spend_at_t32_resolves_from_a_non_canonical_day(self):
        """Sunday's Spend @ 32d must not be — just because storage sits on Sat."""
        self._snapshots(self.canonical)
        for row in self.sunday_rows():
            self._snapshots(row["event_id"])
        self._write_edition_spend()
        self.db.backfill_ad_spend_into_snapshots()

        canonical_value = self.db.get_edition_spend_at_days_out(self.canonical, 32)
        self.assertGreater(canonical_value, 0)
        for row in self.sunday_rows():
            self.assertAlmostEqual(
                self.db.get_edition_spend_at_days_out(row["event_id"], 32),
                canonical_value, places=2)

    def test_does_not_depend_on_which_day_holds_storage(self):
        self._snapshots(self.canonical)
        self._write_edition_spend()
        self.db.backfill_ad_spend_into_snapshots()
        values = {round(self.db.get_edition_spend_at_days_out(r["event_id"], 32), 2)
                  for r in self.rows}
        self.assertEqual(len(values), 1)

    def test_zero_without_spend_rather_than_error(self):
        self._snapshots(self.canonical)
        self.db.backfill_ad_spend_into_snapshots()
        self.assertEqual(
            self.db.get_edition_spend_at_days_out(self.canonical, 32), 0.0)


# ---------------------------------------------------------------------------
# Portfolio deduplication — the mandatory one
# ---------------------------------------------------------------------------
class _Analysis:
    def __init__(self, event_id, name, event_date, ad_spend):
        self.event_id, self.event_name = event_id, name
        self.event_date, self.ad_spend = event_date, ad_spend


class TestPortfolioDeduplication(_Fixture):
    def test_one_thousand_stays_one_thousand(self):
        analyses = [_Analysis(r["event_id"], self.NAME, r["event_date"], 1000.0)
                    for r in self.rows]
        self.assertAlmostEqual(portfolio_spend_total(self.db, analyses), 1000.0, places=2)

    def test_saturday_and_sunday_views_do_not_double(self):
        sat = _Analysis("S0", self.NAME, self.DAYS[0][0], 1000.0)
        sun = _Analysis("S3", self.NAME, self.DAYS[1][0], 1000.0)
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, [sat, sun]), 1000.0, places=2)

    def test_distinct_editions_still_add_up(self):
        a = _Analysis("A", "Austin Coffee Festival", "2026-10-17", 1000.0)
        b = _Analysis("B", "Philly Coffee Festival", "2026-10-17", 250.0)
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, [a, b]), 1250.0, places=2)

    def test_distinct_years_still_add_up(self):
        a = _Analysis("A", "Austin Coffee Festival", "2025-10-25", 400.0)
        b = _Analysis("B", "Austin Coffee Festival", "2026-10-17", 600.0)
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, [a, b]), 1000.0, places=2)

    def test_grouped_day_view_alongside_raw_sessions_counts_once(self):
        """A day-group and its constituents are several views of one edition."""
        views = [_Analysis("grouped", self.NAME, self.DAYS[0][0], 1000.0)]
        views += [_Analysis(r["event_id"], self.NAME, r["event_date"], 1000.0)
                  for r in self.rows]
        self.assertAlmostEqual(portfolio_spend_total(self.db, views), 1000.0, places=2)

    def test_unkeyable_analyses_are_not_silently_merged(self):
        a = _Analysis("A", "", "", 100.0)
        b = _Analysis("B", "", "", 250.0)
        self.assertAlmostEqual(portfolio_spend_total(self.db, [a, b]), 350.0, places=2)


# ---------------------------------------------------------------------------
# DC Wine: 1 row on 10/16, 4 timed rows on 10/17
# ---------------------------------------------------------------------------
class TestDCWineSharedReads(_Fixture):
    NAME = "DC Wine Fest"
    DAYS = [("2026-10-16", 1), ("2026-10-17", 4)]

    def test_both_days_see_the_festival_spend(self):
        total = self._write_edition_spend()
        for row in self.rows:
            self.assertAlmostEqual(
                self.db.get_edition_spend(row["event_id"]), total, places=2)

    def test_the_four_timed_rows_are_not_zero(self):
        """The silent-partial symptom, now at read time."""
        self._write_edition_spend()
        for row in self.sunday_rows():
            self.assertGreater(self.db.get_edition_spend(row["event_id"]), 0)

    def test_five_rows_belong_to_one_edition(self):
        ids = self.db.edition_sibling_ids(self.rows[0]["event_id"])
        self.assertEqual(len(ids), 5)

    def test_portfolio_counts_the_edition_once(self):
        analyses = [_Analysis(r["event_id"], self.NAME, r["event_date"], 800.0)
                    for r in self.rows]
        self.assertAlmostEqual(portfolio_spend_total(self.db, analyses), 800.0, places=2)

    def test_cac_context_shared_across_both_days(self):
        total = self._write_edition_spend()
        tickets = {self.db.get_edition_tickets(r["event_id"]) for r in self.rows}
        self.assertEqual(len(tickets), 1)
        self.assertGreater(total / tickets.pop(), 0)


# ---------------------------------------------------------------------------
# Sibling resolution must not leak across editions
# ---------------------------------------------------------------------------
class TestSiblingScoping(_Fixture):
    def test_siblings_stay_within_the_edition(self):
        self.db.upsert_event({
            "event_id": "OTHER", "name": "Philly Coffee Festival",
            "event_type": "coffee", "city": "Philly",
            "event_date": self.DAYS[0][0], "capacity": 2000, "status": "completed"})
        ids = set(self.db.edition_sibling_ids(self.rows[0]["event_id"]))
        self.assertNotIn("OTHER", ids)
        self.assertEqual(len(ids), 6)

    def test_other_year_is_a_separate_edition(self):
        self.db.upsert_event({
            "event_id": "PRIOR", "name": self.NAME, "event_type": "coffee",
            "city": "Austin", "event_date": "2024-10-25", "capacity": 2000,
            "status": "completed"})
        ids = set(self.db.edition_sibling_ids(self.rows[0]["event_id"]))
        self.assertNotIn("PRIOR", ids)

    def test_unknown_event_id_degrades_to_itself(self):
        self.assertEqual(self.db.edition_sibling_ids("does-not-exist"),
                         ["does-not-exist"])

    def test_edition_key_matches_the_attribution_key(self):
        key = festival_edition_key(self.NAME, self.DAYS[0][0])
        for row in self.rows:
            self.assertEqual(
                festival_edition_key(row["name"], row["event_date"]), key)


if __name__ == "__main__":
    unittest.main(verbosity=2)
