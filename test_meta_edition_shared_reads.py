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
        """A day-group and its constituents are several views of one edition.

        The grouped view carries constituent_event_ids, as the dashboard
        actually builds it. It previously did not, and a synthesized id with no
        constituents is not a shape production ever produces — that gap is part
        of why this class passed while production double-counted.
        """
        sat_ids = [r["event_id"] for r in self.rows
                   if r["event_date"] == self.DAYS[0][0]]
        grouped = _Analysis("grouped", self.NAME, self.DAYS[0][0], 1000.0)
        grouped.constituent_event_ids = sat_ids
        views = [grouped]
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


# ---------------------------------------------------------------------------
# Production-shaped portfolio deduplication
#
# The class above passed while production double-counted, because every
# synthetic analysis was given the bare festival name. The real dashboard
# builds a GROUPED DAY analysis whose display name carries a weekday suffix:
#
#   "Austin Coffee Festival - Saturday"  ->  austin_coffee_fest_saturday
#   "Austin Coffee Festival - Sunday"    ->  austin_coffee_fest_sunday
#
# Two different normalised keys, so name-derived identity deduped nothing and
# one festival's spend was added once per day view. Production showed
# $88,403.98 against $49,580.99 of stored spend.
#
# Those same grouped analyses already carry the REAL constituent event ids, so
# identity is resolved from the database instead of from a display string.
# ---------------------------------------------------------------------------
class _GroupedAnalysis:
    """Mirrors the EventPacing shape the dashboard builds for a day group."""

    def __init__(self, event_id, name, event_date, ad_spend, constituent_event_ids):
        self.event_id, self.event_name = event_id, name
        self.event_date, self.ad_spend = event_date, ad_spend
        self.constituent_event_ids = list(constituent_event_ids)


class TestPortfolioDeduplicationProductionShape(_Fixture):
    """The exact production defect, reproduced."""

    def _day_groups(self, spend):
        sat_ids = [r["event_id"] for r in self.rows if r["event_date"] == self.DAYS[0][0]]
        sun_ids = [r["event_id"] for r in self.rows if r["event_date"] == self.DAYS[1][0]]
        return (
            _GroupedAnalysis("grp-sat", f"{self.NAME} - Saturday",
                             self.DAYS[0][0], spend, sat_ids),
            _GroupedAnalysis("grp-sun", f"{self.NAME} - Sunday",
                             self.DAYS[1][0], spend, sun_ids),
        )

    def test_weekday_suffixed_day_groups_count_once(self):
        """THE regression. Old code returned 2000.0 here."""
        sat, sun = self._day_groups(1000.0)
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, [sat, sun]), 1000.0, places=2)

    def test_the_old_name_derived_identity_really_did_split(self):
        """Proves the test has teeth: the display names DO normalise apart."""
        sat, sun = self._day_groups(1000.0)
        self.assertNotEqual(
            festival_edition_key(sat.event_name, sat.event_date),
            festival_edition_key(sun.event_name, sun.event_date),
        )

    def test_dc_coffee_production_values(self):
        """$9,596.49 on either day view contributes $9,596.49, not $19,192.98."""
        sat, sun = self._day_groups(9596.49)
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, [sat, sun]), 9596.49, places=2)

    def test_grouped_views_plus_raw_sessions_count_once(self):
        """Day groups and their own constituents are all views of one edition."""
        sat, sun = self._day_groups(1000.0)
        raw = [_GroupedAnalysis(r["event_id"], self.NAME, r["event_date"], 1000.0, [])
               for r in self.rows]
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, [sat, sun] + raw), 1000.0, places=2)

    def test_constituent_ids_win_over_a_misleading_display_name(self):
        """Identity comes from the database, not from whatever the label says."""
        ids = [r["event_id"] for r in self.rows]
        a = _GroupedAnalysis("g1", "Totally Different Label", "2026-01-01", 500.0, ids[:3])
        b = _GroupedAnalysis("g2", "Another Unrelated Label", "2030-12-31", 500.0, ids[3:])
        self.assertAlmostEqual(portfolio_spend_total(self.db, [a, b]), 500.0, places=2)

    def test_unresolvable_analyses_are_not_merged(self):
        """Fail safe to a unique identity rather than collapsing by fuzzy name."""
        a = _GroupedAnalysis("ghost-1", "", "", 100.0, ["nonexistent-a"])
        b = _GroupedAnalysis("ghost-2", "", "", 250.0, ["nonexistent-b"])
        self.assertAlmostEqual(portfolio_spend_total(self.db, [a, b]), 350.0, places=2)


class TestPortfolioMultiEditionProductionShape(unittest.TestCase):
    """Distinct festivals and distinct years must still add normally."""

    FESTIVALS = [
        ("Austin Coffee Festival", "Austin", [("2026-10-17", 3), ("2026-10-18", 3)], 8732.18),
        ("DC Coffee Festival", "DC", [("2026-10-03", 3), ("2026-10-04", 3)], 9596.49),
        ("San Diego Coffee Festival", "San Diego", [("2026-10-10", 3), ("2026-10-11", 3)], 7146.49),
        ("Austin Coffee Festival", "Austin", [("2025-10-25", 3), ("2025-10-26", 3)], 400.0),
    ]

    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "multi.db"))
        self.groups, n = [], 0
        for name, city, days, spend in self.FESTIVALS:
            for d, count in days:
                ids = []
                for _ in range(count):
                    eid = f"E{n}"
                    self.db.upsert_event({
                        "event_id": eid, "name": name, "event_type": "coffee",
                        "city": city, "event_date": d, "capacity": 2000,
                        "status": "upcoming",
                    })
                    ids.append(eid)
                    n += 1
                weekday = datetime.date.fromisoformat(d).strftime("%A")
                self.groups.append(
                    _GroupedAnalysis(f"grp-{n}", f"{name} - {weekday}", d, spend, ids))

    def test_each_edition_counted_exactly_once(self):
        expected = round(8732.18 + 9596.49 + 7146.49 + 400.0, 2)
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, self.groups), expected, places=2)

    def test_eight_day_groups_collapse_to_four_editions(self):
        """Eight day views, four editions — the naive sum counts each twice."""
        self.assertEqual(len(self.groups), 8)
        naive = round(sum(g.ad_spend for g in self.groups), 2)
        deduped = portfolio_spend_total(self.db, self.groups)
        self.assertAlmostEqual(naive, round(deduped * 2, 2), places=2)
        self.assertLess(deduped, naive - 1.0)

    def test_austin_2025_and_2026_stay_distinct(self):
        austin = [g for g in self.groups if "Austin" in g.event_name]
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, austin), round(8732.18 + 400.0, 2), places=2)

    def test_distinct_festivals_do_not_collapse(self):
        y2026 = [g for g in self.groups if g.event_date.startswith("2026")]
        self.assertAlmostEqual(
            portfolio_spend_total(self.db, y2026),
            round(8732.18 + 9596.49 + 7146.49, 2), places=2)


class TestGroupedDashboardTruth(_Fixture):
    def _groups(self):
        from craft_unified import DecisionEngine
        from unittest.mock import patch
        engine = DecisionEngine(self.db)
        with patch.object(self.db, 'get_edition_spend_status', return_value='current_has_spend'):
            raw = [engine.analyze_event(r['event_id']) for r in self.rows]
            return [engine._create_day_event(engine._get_pattern(self.NAME),
                    [a for a in raw if a.event_date == day], raw)
                    for day, _ in self.DAYS]

    def test_both_day_cards_use_edition_ticket_denominator(self):
        spend = self._write_edition_spend()
        tickets = self.db.get_edition_tickets(self.canonical)
        groups = self._groups()
        self.assertEqual(len(groups), 2)
        self.assertEqual(sum(g.tickets_sold for g in groups), tickets)
        for group in groups:
            self.assertEqual(group.cac, round(spend / tickets, 2))
            self.assertEqual(group.ad_spend, spend)
            self.assertLess(group.cac, spend / group.tickets_sold)

    def test_empty_day_still_shares_real_festival_acquisition_cost(self):
        spend = self._write_edition_spend()
        for row in self.sunday_rows():
            self.db.conn.execute('DELETE FROM orders WHERE event_id = ?', (row['event_id'],))
        self.db.conn.commit()
        groups = self._groups()
        self.assertEqual(groups[1].tickets_sold, 0)
        self.assertEqual(groups[0].cac, groups[1].cac)
        self.assertEqual(groups[1].cac, round(spend / groups[0].tickets_sold, 2))

    def test_administrative_event_is_preserved_but_not_analyzed(self):
        from craft_unified import DecisionEngine, EventbriteSync
        event = {'event_id': 'admin-payment', 'name': 'Dallas Coffee Festival 2027 — Exhibitor Payment',
                 'event_type': 'coffee', 'city': 'Dallas', 'event_date': '2035-03-20',
                 'status': 'upcoming', 'capacity': 1300}
        self.db.upsert_event(event)
        self.assertTrue(EventbriteSync._is_junk_event(event['name']))
        engine = DecisionEngine(self.db)
        self.assertIsNone(engine.analyze_event(event['event_id']))
        self.assertNotIn(event['event_id'], [a.event_id for a in engine.analyze_portfolio()])
        self.assertNotIn(event['event_id'], [e['event_id'] for e in engine._get_all_events()])
        self.assertIsNotNone(self.db.get_event(event['event_id']))

    def test_consumer_festival_is_not_filtered(self):
        from craft_unified import DecisionEngine, EventbriteSync
        event = {'event_id': 'consumer-festival', 'name': 'Dallas Coffee Festival',
                 'event_type': 'coffee', 'city': 'Dallas', 'event_date': '2035-03-20',
                 'status': 'upcoming', 'capacity': 1300}
        self.db.upsert_event(event)
        self.assertFalse(EventbriteSync._is_junk_event(event['name']))
        self.assertIsNotNone(DecisionEngine(self.db).analyze_event(event['event_id']))


class TestPortfolioTargetCountReuse(unittest.TestCase):
    def test_counts_are_equivalent_scoped_and_discarded_after_each_read(self):
        from unittest.mock import patch
        from dataclasses import asdict
        from craft_unified import DecisionEngine
        with tempfile.TemporaryDirectory() as folder:
            db = Database(os.path.join(folder, "counts.db"))
            try:
                day = (datetime.date.today() + datetime.timedelta(days=90)).isoformat()
                for eid, city, kind, name in [
                    ("a1", "Austin", "coffee", "Austin Coffee Festival"),
                    ("a2", "Austin", "coffee", "Austin Coffee Festival"),
                    ("s1", "Seattle", "coffee", "Seattle Coffee Festival"),
                    ("s2", "Seattle", "coffee", "Seattle Coffee Festival"),
                    ("c1", "Austin", "cocktail", "Austin Cocktail Festival"),
                ]:
                    db.upsert_event(dict(event_id=eid, name=name, city=city,
                                         event_type=kind, event_date=day,
                                         capacity=100, status="live"))
                counts = {("coffee", "Austin"): 3, ("coffee", "Seattle"): 7,
                          ("cocktail", "Austin"): 11}
                def high(**kwargs):
                    self.assertEqual(kwargs["min_ltv"], 50)
                    self.assertEqual(kwargs["limit"], 1000)
                    return [None] * counts[(kwargs["event_type"], kwargs["city"])]
                engine = DecisionEngine(db)
                with patch.object(db, "get_high_value_customers", side_effect=high) as hv, \
                     patch.object(db, "get_at_risk_customers", return_value=[None]*4) as risk:
                    original = engine.analyze_event
                    # Reference behavior: every session fetches its own counts.
                    with patch.object(engine, "analyze_event",
                                      side_effect=lambda eid, **kw: original(eid)):
                        reference = [asdict(x) for x in engine.analyze_portfolio()]
                    self.assertEqual(risk.call_count, 5)
                    hv.reset_mock(); risk.reset_mock()
                    actual = [asdict(x) for x in engine.analyze_portfolio()]
                    self.assertEqual(actual, reference)
                    self.assertEqual(hv.call_count, 3)
                    self.assertEqual(risk.call_count, 1)
                    risk.assert_called_once_with(min_orders=2, min_days_inactive=180)
                    # A later read must observe changed eligibility/counts.
                    counts[("coffee", "Austin")] = 9
                    risk.return_value = [None]*2
                    again = engine.analyze_portfolio()
                    self.assertEqual(risk.call_count, 2)
                    self.assertEqual(hv.call_count, 6)
                    self.assertTrue(any(x.high_value_targets == 9 for x in again))
                    # Standalone reads never reuse a prior request's count.
                    counts[("coffee", "Austin")] = 12
                    self.assertEqual(engine.analyze_event("a1").high_value_targets, 12)
                    counts[("coffee", "Austin")] = 13
                    self.assertEqual(engine.analyze_event("a1").high_value_targets, 13)
            finally:
                db.close()
