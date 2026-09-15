"""V2 diagnosis must read one festival edition ONCE, at one consistent scope.

PR #13 moved DiagnosisEngine._get_meta_spend() from raw-event spend to
FESTIVAL-EDITION spend, but three neighbours were left at raw-event scope.
All three are the same mismatch, and all three drive recommendations:

1. MULTIPLICATION. _get_meta_spend_for_ids() loops the constituent ids of a
   grouped day and ADDS _get_meta_spend(eid) for each. Every constituent now
   returns the whole edition's spend, so a 3-session Saturday reports 3x.
   Production: Austin $26,196.54 against $8,732.18 actually stored.

2. STATUS. _check_meta_data_status() queries `ad_spend WHERE event_id = ?`.
   Spend is stored once, on one canonical raw row, so a day group that does
   not contain that row can report `no_records` while _get_meta_spend()
   resolves real festival spend — an internally contradictory diagnosis.

3. DENOMINATOR. Festival-level spend is divided by day-level tickets. Meta
   advertises the edition; buyers then pick a day or session. Numerator and
   denominator must share one scope.

These use the real Database rather than a stub: the existing FakeDB models
edition_sibling_ids() as a single-row edition, which is exactly why none of
this was caught.
"""

import datetime
import os
import sys
import tempfile
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from craft_unified import Database, canonical_edition_event  # noqa: E402
from diagnosis_engine import DiagnosisEngine  # noqa: E402


class _Pacing:
    def __init__(self, constituent_event_ids):
        self.constituent_event_ids = list(constituent_event_ids)
        self.historical_comparisons = []


class _EditionFixture(unittest.TestCase):
    """A production-shaped festival: N sessions per day across two days."""

    NAME = "Austin Coffee Festival"
    CITY = "Austin"
    DAYS = [("2026-10-17", 3), ("2026-10-18", 3)]
    EDITION_SPEND = 8732.18
    SAT_TICKETS = 483
    SUN_TICKETS = 232
    RUN_UP_DAYS = 30

    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "diag.db"))
        self.engine = DiagnosisEngine(self.db, decision_engine=None)
        self.rows, self.by_day = [], {}
        n = 0
        for d, count in self.DAYS:
            ids = []
            for _ in range(count):
                eid = f"S{n}"
                self.db.upsert_event({
                    "event_id": eid, "name": self.NAME, "event_type": "coffee",
                    "city": self.CITY, "event_date": d, "capacity": 2000,
                    "status": "upcoming",
                })
                self.rows.append({"event_id": eid, "name": self.NAME, "event_date": d})
                ids.append(eid)
                n += 1
            self.by_day[d] = ids

        self.sat_ids = self.by_day[self.DAYS[0][0]]
        self.sun_ids = self.by_day[self.DAYS[-1][0]]
        self.canonical = canonical_edition_event(self.rows)["event_id"]
        # The canonical row is a Saturday row, so Sunday holds no ad_spend of
        # its own -- the exact production arrangement.
        self.assertIn(self.canonical, self.sat_ids)

        self._orders(self.sat_ids, self.SAT_TICKETS)
        self._orders(self.sun_ids, self.SUN_TICKETS)
        self._write_edition_spend()

    def _orders(self, ids, total_tickets):
        per = total_tickets // len(ids)
        extra = total_tickets - per * len(ids)
        for i, eid in enumerate(ids):
            count = per + (extra if i == 0 else 0)
            for k in range(count):
                self.db.insert_order({
                    "order_id": f"{eid}-{k}", "event_id": eid,
                    "email": f"{eid}-{k}@example.com",
                    "order_timestamp": "2026-09-01T10:00:00",
                    "ticket_count": 1, "gross_amount": 40.0, "net_amount": 36.0,
                    "days_before_event": 30,
                })

    def _write_edition_spend(self):
        """One stored stream on the canonical row, as the sync writes it."""
        today = datetime.date.today()
        per_day = round(self.EDITION_SPEND / (self.RUN_UP_DAYS + 1), 6)
        rows = []
        for back in range(self.RUN_UP_DAYS, -1, -1):
            day = today - datetime.timedelta(days=back)
            rows.append((self.canonical, "camp1", "Instagram post: ACF ...",
                         day.isoformat(), per_day, 100, 5))
        self.db.save_ad_spend_batch(rows)

    def _edition_spend(self):
        return round(self.db.get_edition_spend(self.canonical), 2)


# ---------------------------------------------------------------------------
# 1. Multiplication
# ---------------------------------------------------------------------------
class TestSpendCountedOncePerEdition(_EditionFixture):

    def test_saturday_constituents_do_not_triple_spend(self):
        """THE regression. Old code returned 3x this."""
        total, _, _, _ = self.engine._get_meta_spend_for_ids(self.sat_ids)
        self.assertAlmostEqual(total, self._edition_spend(), places=2)

    def test_multiplier_is_not_the_constituent_count(self):
        one = self.engine._get_meta_spend(self.canonical)[0]
        many = self.engine._get_meta_spend_for_ids(self.sat_ids)[0]
        self.assertAlmostEqual(many, one, places=2)
        self.assertNotAlmostEqual(many, one * len(self.sat_ids), places=2)

    def test_sunday_sees_the_same_edition_spend(self):
        sat = self.engine._get_meta_spend_for_ids(self.sat_ids)[0]
        sun = self.engine._get_meta_spend_for_ids(self.sun_ids)[0]
        self.assertAlmostEqual(sat, sun, places=2)

    def test_recent_seven_day_spend_counted_once(self):
        _, one7, _, _ = self.engine._get_meta_spend(self.canonical)
        _, many7, _, _ = self.engine._get_meta_spend_for_ids(self.sat_ids)
        self.assertGreater(one7, 0)
        self.assertAlmostEqual(many7, one7, places=2)

    def test_impressions_counted_once(self):
        _, _, one_i, _ = self.engine._get_meta_spend(self.canonical)
        _, _, many_i, _ = self.engine._get_meta_spend_for_ids(self.sat_ids)
        self.assertGreater(one_i, 0)
        self.assertEqual(many_i, one_i)

    def test_clicks_counted_once(self):
        _, _, _, one_c = self.engine._get_meta_spend(self.canonical)
        _, _, _, many_c = self.engine._get_meta_spend_for_ids(self.sat_ids)
        self.assertGreater(one_c, 0)
        self.assertEqual(many_c, one_c)

    def test_whole_edition_passed_in_still_counts_once(self):
        both = self.engine._get_meta_spend_for_ids(self.sat_ids + self.sun_ids)[0]
        self.assertAlmostEqual(both, self._edition_spend(), places=2)

    def test_empty_list_is_zero_not_an_error(self):
        self.assertEqual(self.engine._get_meta_spend_for_ids([]), (0.0, 0.0, 0, 0))


# ---------------------------------------------------------------------------
# 2. Status on a non-canonical day
# ---------------------------------------------------------------------------
class TestStatusIsEditionAware(_EditionFixture):

    def test_sunday_does_not_claim_no_records(self):
        """Sunday holds no ad_spend rows, but its edition is advertised."""
        self.assertEqual(
            self.engine._check_meta_data_status_for_ids(self.sun_ids),
            "current_has_spend")

    def test_saturday_and_sunday_report_the_same_status(self):
        self.assertEqual(
            self.engine._check_meta_data_status_for_ids(self.sat_ids),
            self.engine._check_meta_data_status_for_ids(self.sun_ids))

    def test_status_never_contradicts_resolved_spend(self):
        """A day reporting spend must not also report no_records."""
        for ids in (self.sat_ids, self.sun_ids):
            spend = self.engine._get_meta_spend_for_ids(ids)[0]
            status = self.engine._check_meta_data_status_for_ids(ids)
            if spend > 0:
                self.assertNotEqual(status, "no_records")

    def test_empty_list_is_unavailable(self):
        self.assertEqual(self.engine._check_meta_data_status_for_ids([]), "unavailable")


class TestStatusWithNoSpendAtAll(_EditionFixture):
    """A genuinely unadvertised edition must still report no_records."""

    def _write_edition_spend(self):
        return

    def test_no_records_when_the_edition_has_none(self):
        self.assertEqual(
            self.engine._check_meta_data_status_for_ids(self.sun_ids), "no_records")

    def test_spend_is_zero_not_invented(self):
        self.assertEqual(self.engine._get_meta_spend_for_ids(self.sat_ids)[0], 0.0)


# ---------------------------------------------------------------------------
# 3. Denominator scope
# ---------------------------------------------------------------------------
class TestBlendedSpendPerTicketScope(_EditionFixture):

    def test_denominator_is_edition_wide(self):
        """Festival spend over festival tickets, not over one day's tickets."""
        spend = self._edition_spend()
        blended = self.engine._compute_blended_ad_spend_per_ticket(
            self.sat_ids[0], spend)
        edition_tickets = self.db.get_edition_tickets(self.sat_ids[0])
        self.assertEqual(edition_tickets, self.SAT_TICKETS + self.SUN_TICKETS)
        self.assertAlmostEqual(blended, spend / edition_tickets, places=4)

    def test_saturday_and_sunday_agree(self):
        spend = self._edition_spend()
        self.assertAlmostEqual(
            self.engine._compute_blended_ad_spend_per_ticket(self.sat_ids[0], spend),
            self.engine._compute_blended_ad_spend_per_ticket(self.sun_ids[0], spend),
            places=4)

    def test_not_the_day_only_denominator(self):
        spend = self._edition_spend()
        blended = self.engine._compute_blended_ad_spend_per_ticket(
            self.sat_ids[0], spend)
        sat_only = self.db.get_event_tickets(self.sat_ids[0])
        self.assertNotAlmostEqual(blended, spend / sat_only, places=2)

    def test_zero_spend_stays_zero(self):
        self.assertEqual(
            self.engine._compute_blended_ad_spend_per_ticket(self.sat_ids[0], 0.0), 0.0)

    def test_a_db_without_edition_helpers_does_not_return_false_zero(self):
        """The FakeDB trap: a missing method must not silently become $0."""
        class Legacy:
            def __init__(self, inner):
                self._inner = inner

            def get_event_tickets(self, event_id):
                return self._inner.get_event_tickets(event_id)

        engine = DiagnosisEngine(Legacy(self.db), decision_engine=None)
        value = engine._compute_blended_ad_spend_per_ticket(self.sat_ids[0], 1000.0)
        self.assertGreater(value, 0.0)


# ---------------------------------------------------------------------------
# 4. Production values
# ---------------------------------------------------------------------------
class TestPhillyProductionValues(_EditionFixture):
    NAME = "Philly Coffee Festival"
    CITY = "Philly"
    DAYS = [("2026-10-24", 3), ("2026-10-25", 3)]
    EDITION_SPEND = 6414.96
    SAT_TICKETS = 381
    SUN_TICKETS = 186

    def test_not_tripled(self):
        total = self.engine._get_meta_spend_for_ids(self.sat_ids)[0]
        self.assertAlmostEqual(total, self._edition_spend(), places=2)
        self.assertLess(total, self._edition_spend() * 2)


class TestSanDiegoProductionValues(_EditionFixture):
    NAME = "San Diego Coffee Festival"
    CITY = "San Diego"
    DAYS = [("2026-10-10", 3), ("2026-10-11", 3)]
    EDITION_SPEND = 7146.49
    SAT_TICKETS = 657
    SUN_TICKETS = 249

    def test_not_tripled(self):
        total = self.engine._get_meta_spend_for_ids(self.sat_ids)[0]
        self.assertAlmostEqual(total, self._edition_spend(), places=2)
        self.assertLess(total, self._edition_spend() * 2)


class TestSingleRowEditionUnchanged(_EditionFixture):
    """Philly Wine shape: one row, no grouping. Must behave identically."""
    NAME = "Philly Wine Fest! Fall Edition"
    CITY = "Philly"
    DAYS = [("2026-11-07", 1)]
    EDITION_SPEND = 1241.90
    SAT_TICKETS = 300
    SUN_TICKETS = 0

    def setUp(self):
        super().setUp()
        self.sun_ids = self.sat_ids

    def _orders(self, ids, total_tickets):
        if total_tickets:
            super()._orders(ids, total_tickets)

    def test_spend_unchanged(self):
        self.assertAlmostEqual(
            self.engine._get_meta_spend_for_ids(self.sat_ids)[0],
            self._edition_spend(), places=2)

    def test_denominator_equals_the_single_event(self):
        spend = self._edition_spend()
        self.assertAlmostEqual(
            self.engine._compute_blended_ad_spend_per_ticket(self.sat_ids[0], spend),
            spend / self.db.get_event_tickets(self.sat_ids[0]), places=4)


# ---------------------------------------------------------------------------
# 5. Two distinct editions in one list must each count once
# ---------------------------------------------------------------------------
class TestTwoEditionsEachCountedOnce(unittest.TestCase):

    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "two.db"))
        self.engine = DiagnosisEngine(self.db, decision_engine=None)
        self.ids, self.spend = {}, {"Austin": 8732.18, "DC Coffee": 9596.49}
        n = 0
        today = datetime.date.today()
        for name, city, d in [("Austin Coffee Festival", "Austin", "2026-10-17"),
                              ("DC Coffee Festival", "DC", "2026-10-03")]:
            group = []
            for _ in range(3):
                eid = f"T{n}"
                self.db.upsert_event({
                    "event_id": eid, "name": name, "event_type": "coffee",
                    "city": city, "event_date": d, "capacity": 2000,
                    "status": "upcoming",
                })
                group.append(eid)
                n += 1
            key = "Austin" if "Austin" in name else "DC Coffee"
            self.ids[key] = group
            self.db.save_ad_spend_batch(
                [(group[0], f"c-{key}", "x", today.isoformat(), self.spend[key], 10, 1)])

    def test_each_edition_counted_once(self):
        mixed = self.ids["Austin"] + self.ids["DC Coffee"]
        total = self.engine._get_meta_spend_for_ids(mixed)[0]
        self.assertAlmostEqual(
            total, round(self.spend["Austin"] + self.spend["DC Coffee"], 2), places=2)

    def test_distinct_editions_do_not_collapse(self):
        a = self.engine._get_meta_spend_for_ids(self.ids["Austin"])[0]
        d = self.engine._get_meta_spend_for_ids(self.ids["DC Coffee"])[0]
        self.assertNotAlmostEqual(a, d, places=2)


# ---------------------------------------------------------------------------
# 6. Root cause rests on correct math, not on a multiplier
# ---------------------------------------------------------------------------
class TestRootCauseUsesTruthfulInput(_EditionFixture):

    def _causes(self, blended):
        return self.engine._analyze_root_causes(
            pace_delta=-30.0, recent_velocity=1.0, historical_velocity=5.0,
            meta_total=self._edition_spend(), meta_7d_spend=100.0, meta_7d_clicks=50,
            blended_ad_spend_per_ticket=blended, days_until=32, sell_through=20.0,
            audience={"total": 100, "past_attendees": 100, "champions": 10,
                      "city_prospects": 0, "at_risk": 0},
            historical_campaigns=0, meta_data_status="current_has_spend",
        )

    def _high_spend_cause(self, causes):
        return [c for c in causes
                if "spend per ticket" in str(getattr(c, "cause", "")).lower()]

    def test_threshold_still_fires_when_genuinely_high(self):
        """The fix must not merely suppress the root cause."""
        self.assertTrue(self._high_spend_cause(self._causes(75.0)))

    def test_does_not_fire_on_the_correct_edition_metric(self):
        spend = self._edition_spend()
        correct = spend / self.db.get_edition_tickets(self.sat_ids[0])
        self.assertLess(correct, 50.0)
        self.assertFalse(self._high_spend_cause(self._causes(correct)))

    def test_the_tripled_metric_would_have_fired_falsely(self):
        """Documents the production symptom: 3x spend over day-only tickets."""
        tripled = self._edition_spend() * 3
        false_metric = tripled / self.db.get_event_tickets(self.sat_ids[0])
        self.assertGreater(false_metric, 50.0)
        self.assertTrue(self._high_spend_cause(self._causes(false_metric)))


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# 7. The single-event diagnose() path carries the same scope
# ---------------------------------------------------------------------------
class TestSingleEventPathScope(_EditionFixture):
    """diagnose() reads one raw event id, which may still belong to an edition."""

    def _opportunity(self):
        return {"evidence": {"days_until": 32, "tickets_sold": self.SAT_TICKETS,
                             "avg_ticket_price": 40.0, "pace_delta_pct": -20.0,
                             "gap_tickets": 100},
                "revenue_at_risk": 4000.0}

    def test_non_canonical_raw_session_does_not_claim_no_records(self):
        """A Sunday session holds no ad_spend rows of its own."""
        self.assertEqual(
            self.engine._check_meta_data_status_for_ids([self.sun_ids[0]]),
            "current_has_spend")

    def test_raw_session_spend_and_status_agree(self):
        for eid in self.sat_ids + self.sun_ids:
            spend = self.engine._get_meta_spend(eid)[0]
            status = self.engine._check_meta_data_status_for_ids([eid])
            self.assertGreater(spend, 0)
            self.assertNotEqual(status, "no_records")

    def test_diagnose_reports_edition_scoped_values(self):
        d = self.engine.diagnose(self.sun_ids[0], self._opportunity())
        self.assertAlmostEqual(d.meta_spend_total, self._edition_spend(), places=2)
        self.assertFalse([w for w in (d.missing_data or [])
                          if "no paid advertising" in str(w).lower()])

    def test_diagnose_denominator_is_edition_wide(self):
        d = self.engine.diagnose(self.sat_ids[0], self._opportunity())
        expected = self._edition_spend() / self.db.get_edition_tickets(self.sat_ids[0])
        # The dataclass rounds to cents; the scope is what is under test.
        self.assertAlmostEqual(d.blended_ad_spend_per_ticket, expected, places=2)
        sat_only = self._edition_spend() / self.db.get_event_tickets(self.sat_ids[0])
        self.assertNotAlmostEqual(d.blended_ad_spend_per_ticket, sat_only, places=2)

    def test_every_raw_session_agrees_on_the_metric(self):
        values = {round(self.engine.diagnose(eid, self._opportunity())
                        .blended_ad_spend_per_ticket, 6)
                  for eid in self.sat_ids + self.sun_ids}
        self.assertEqual(len(values), 1)
