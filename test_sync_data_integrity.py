"""A sync must never overwrite a known value with one it did not observe.

Production incident: the dashboard showed Philly Coffee Saturday at 251 tickets
and Austin Coffee Saturday at 316, against Eventbrite's 379 and 462. Revenue was
untouched, which is what made it invisible -- $16,390 over 316 tickets implies
$51.87 a ticket for a festival whose own history runs $30.71-$40.29.

The cause was one line:

    ticket_count = len(attendees) if attendees else 1

`attendees` only exists because of `expand=attendees`. When that expansion came
back empty, the count silently became 1, and `INSERT OR REPLACE` wrote it over a
correct multi-ticket order. The real value was gone, replaced by a plausible one.

That is a CLASS of defect, not one line. Every field these parsers derive from an
optional part of the payload can degrade the same way, and every one of them is
written through a blind REPLACE:

  orders  ticket_count, gross_amount, net_amount, ticket_type, promo_code,
          order_timestamp/days_before_event (fabricated from now() on a parse error)
  events  capacity, city, meta_campaign_id

net_amount and meta_campaign_id are worse: the parsers never return them at all,
so every sync wrote 0 and NULL over whatever was stored.

The invariant these tests hold to: an unobserved field is UNKNOWN, never a
default, and a write merges rather than replaces. A genuinely observed change
(a refund taking 4 tickets to 2) must still be written.
"""

import datetime
import os
import sys
import tempfile
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from craft_unified import Database, EventbriteSync  # noqa: E402


EVENT_DATE = datetime.datetime(2026, 10, 17, 9, 0, 0)


def _order_payload(order_id="ORD1", n_attendees=4, with_costs=True,
                   with_attendees=True, promo="SAVE10"):
    data = {
        "id": order_id,
        "created": "2026-09-01T10:00:00Z",
        "email": "Buyer@Example.com",
        "promo_code": promo,
    }
    if with_costs:
        data["costs"] = {"gross": {"major_value": "140.00"},
                         "net": {"major_value": "126.00"}}
    if with_attendees:
        data["attendees"] = [
            {"profile": {"email": "buyer@example.com"},
             "ticket_class_name": "General Admission"}
            for _ in range(n_attendees)
        ]
    return data


def _event_payload(event_id="E1", capacity=2000, with_venue=True):
    data = {
        "id": event_id,
        "name": {"text": "Austin Coffee Festival"},
        "start": {"local": "2026-10-17T09:00:00"},
    }
    if capacity is not None:
        data["capacity"] = capacity
    if with_venue:
        data["venue"] = {"address": {"city": "Austin"}}
    return data


class _Fixture(unittest.TestCase):
    def setUp(self):
        self.db = Database(os.path.join(tempfile.mkdtemp(), "integrity.db"))
        self.sync = EventbriteSync("", self.db)
        self.db.upsert_event({
            "event_id": "E1", "name": "Austin Coffee Festival",
            "event_type": "coffee", "city": "Austin",
            "event_date": EVENT_DATE.isoformat(), "capacity": 2000,
            "status": "upcoming",
        })

    def _store(self, payload):
        order = self.sync._parse_order(payload, "E1", EVENT_DATE)
        if order:
            self.db.insert_order(order)
        return order

    def _row(self, order_id="ORD1"):
        r = self.db.conn.execute(
            "SELECT * FROM orders WHERE order_id = ?", (order_id,)).fetchone()
        return dict(r) if r else None

    def _event(self, event_id="E1"):
        return self.db.get_event(event_id)


# ---------------------------------------------------------------------------
# The proven production corruption
# ---------------------------------------------------------------------------
class TestTicketCountSurvivesMissingExpansion(_Fixture):

    def test_a_good_order_stores_its_real_count(self):
        self._store(_order_payload(n_attendees=4))
        self.assertEqual(self._row()["ticket_count"], 4)

    def test_missing_expansion_does_not_overwrite_with_one(self):
        """THE incident. Old code wrote 1 over 4."""
        self._store(_order_payload(n_attendees=4))
        self._store(_order_payload(with_attendees=False))
        self.assertEqual(self._row()["ticket_count"], 4)

    def test_a_real_refund_is_still_written(self):
        """Observed decreases must apply -- this is not a ratchet."""
        self._store(_order_payload(n_attendees=4))
        self._store(_order_payload(n_attendees=2))
        self.assertEqual(self._row()["ticket_count"], 2)

    def test_unknown_count_is_not_invented_on_first_sight(self):
        """A brand-new order with no expansion must not claim to know 1."""
        order = self.sync._parse_order(
            _order_payload(order_id="NEW", with_attendees=False), "E1", EVENT_DATE)
        self.assertIsNone(order["ticket_count"])

    def test_repeated_bad_syncs_cannot_erode_the_value(self):
        self._store(_order_payload(n_attendees=6))
        for _ in range(5):
            self._store(_order_payload(with_attendees=False))
        self.assertEqual(self._row()["ticket_count"], 6)


# ---------------------------------------------------------------------------
# Same class: money
# ---------------------------------------------------------------------------
class TestAmountsSurviveMissingCosts(_Fixture):

    def test_gross_is_stored(self):
        self._store(_order_payload())
        self.assertAlmostEqual(self._row()["gross_amount"], 140.0, places=2)

    def test_missing_costs_does_not_zero_revenue(self):
        """Revenue survived the real incident by luck, not by design."""
        self._store(_order_payload())
        self._store(_order_payload(with_costs=False))
        self.assertAlmostEqual(self._row()["gross_amount"], 140.0, places=2)

    def test_net_amount_is_actually_parsed(self):
        """It was never returned, so every sync wrote 0 over it."""
        self._store(_order_payload())
        self.assertAlmostEqual(self._row()["net_amount"], 126.0, places=2)

    def test_net_amount_is_not_zeroed_by_a_later_partial_sync(self):
        self._store(_order_payload())
        self._store(_order_payload(with_costs=False))
        self.assertAlmostEqual(self._row()["net_amount"], 126.0, places=2)

    def test_a_real_price_change_is_written(self):
        self._store(_order_payload())
        p = _order_payload()
        p["costs"]["gross"]["major_value"] = "70.00"
        self._store(p)
        self.assertAlmostEqual(self._row()["gross_amount"], 70.0, places=2)


# ---------------------------------------------------------------------------
# Same class: descriptive fields
# ---------------------------------------------------------------------------
class TestDescriptiveFieldsSurvive(_Fixture):

    def test_ticket_type_not_erased(self):
        self._store(_order_payload())
        self.assertEqual(self._row()["ticket_type"], "General Admission")
        self._store(_order_payload(with_attendees=False))
        self.assertEqual(self._row()["ticket_type"], "General Admission")

    def test_promo_code_not_erased(self):
        self._store(_order_payload())
        self._store(_order_payload(promo=None))
        self.assertEqual(self._row()["promo_code"], "SAVE10")

    def test_order_timestamp_is_never_fabricated_from_now(self):
        """An unparseable date used to store datetime.now(), moving the sale."""
        bad = _order_payload()
        bad["created"] = "not-a-date"
        order = self.sync._parse_order(bad, "E1", EVENT_DATE)
        if order is not None:
            today = datetime.date.today().isoformat()
            self.assertFalse(str(order.get("order_timestamp", "")).startswith(today))

    def test_a_bad_date_does_not_move_an_existing_sale(self):
        self._store(_order_payload())
        before = self._row()["order_timestamp"]
        bad = _order_payload()
        bad["created"] = "not-a-date"
        self._store(bad)
        self.assertEqual(self._row()["order_timestamp"], before)


# ---------------------------------------------------------------------------
# Same class: events
# ---------------------------------------------------------------------------
class TestEventFieldsSurvive(_Fixture):

    def _store_event(self, payload):
        ev = self.sync._parse_event(payload)
        if ev:
            ev["status"] = "upcoming"
            self.db.upsert_event(ev)
        return ev

    def test_capacity_not_zeroed_by_a_partial_payload(self):
        self._store_event(_event_payload(capacity=2000))
        self.assertEqual(self._event()["capacity"], 2000)
        self._store_event(_event_payload(capacity=None))
        self.assertEqual(self._event()["capacity"], 2000)

    def test_city_not_erased_when_venue_missing(self):
        self._store_event(_event_payload())
        self.assertEqual(self._event()["city"], "Austin")
        self._store_event(_event_payload(with_venue=False))
        self.assertEqual(self._event()["city"], "Austin")

    def test_meta_campaign_id_not_nulled_every_sync(self):
        """_parse_event never returned it, so upsert wrote NULL each time."""
        self.db.conn.execute(
            "UPDATE events SET meta_campaign_id = ? WHERE event_id = ?",
            ("camp-123", "E1"))
        self.db.conn.commit()
        self._store_event(_event_payload())
        self.assertEqual(self._event()["meta_campaign_id"], "camp-123")

    def test_a_real_capacity_change_is_written(self):
        self._store_event(_event_payload(capacity=2000))
        self._store_event(_event_payload(capacity=2500))
        self.assertEqual(self._event()["capacity"], 2500)


# ---------------------------------------------------------------------------
# Production shape: the exact Austin/Philly numbers
# ---------------------------------------------------------------------------
class TestProductionShapeReconstruction(_Fixture):
    """462 Saturday tickets must not become 316 because an expansion blinked."""

    ORDERS = [(f"A{i}", 3) for i in range(100)] + [(f"B{i}", 2) for i in range(81)]

    def _seed(self):
        for oid, n in self.ORDERS:
            self._store(_order_payload(order_id=oid, n_attendees=n))

    def _total(self):
        return self.db.get_event_tickets("E1")

    def test_baseline(self):
        self._seed()
        self.assertEqual(self._total(), 100 * 3 + 81 * 2)  # 462

    def test_a_flaky_resync_does_not_collapse_the_total(self):
        self._seed()
        before = self._total()
        # Two thirds of the orders come back without their expansion.
        for oid, _ in self.ORDERS[: int(len(self.ORDERS) * 0.66)]:
            self._store(_order_payload(order_id=oid, with_attendees=False))
        self.assertEqual(self._total(), before)

    def test_implied_price_per_ticket_stays_sane(self):
        """The detector that caught this: revenue/tickets leaving its band."""
        self._seed()
        for oid, _ in self.ORDERS:
            self._store(_order_payload(order_id=oid, with_attendees=False))
        revenue = self.db.get_event_revenue("E1")
        ppt = revenue / self._total()
        self.assertLess(ppt, 60.0)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# The detector: a sync that loses tickets must say so
# ---------------------------------------------------------------------------
class TestTicketLossDetector(_Fixture):
    """Defence in depth. The merge above prevents this class of loss; this
    catches any future write path that finds a new way to do it."""

    def test_the_real_incident_would_have_been_caught(self):
        """Austin 483 -> 316 is a 34.6% drop on 483 stored tickets."""
        found = Database.ticket_total_regressions({"E1": 483}, {"E1": 316})
        self.assertEqual(len(found), 1)
        self.assertEqual(found[0]["lost"], 167)
        self.assertAlmostEqual(found[0]["pct"], 34.6, places=1)

    def test_philly_too(self):
        found = Database.ticket_total_regressions({"E1": 381}, {"E1": 251})
        self.assertEqual(found[0]["lost"], 130)

    def test_growth_is_not_flagged(self):
        self.assertEqual(Database.ticket_total_regressions({"E1": 300}, {"E1": 340}), [])

    def test_unchanged_is_not_flagged(self):
        self.assertEqual(Database.ticket_total_regressions({"E1": 300}, {"E1": 300}), [])

    def test_a_small_refund_is_not_flagged(self):
        """Two tickets off 500 is business, not corruption."""
        self.assertEqual(Database.ticket_total_regressions({"E1": 500}, {"E1": 498}), [])

    def test_an_event_vanishing_entirely_is_flagged(self):
        found = Database.ticket_total_regressions({"E1": 400}, {})
        self.assertEqual(found[0]["after"], 0)
        self.assertEqual(found[0]["pct"], 100.0)

    def test_worst_offender_is_reported_first(self):
        found = Database.ticket_total_regressions(
            {"A": 100, "B": 900, "C": 200}, {"A": 50, "B": 300, "C": 100})
        # B lost 600, C lost 100, A lost 50.
        self.assertEqual([r["event_id"] for r in found], ["B", "C", "A"])

    def test_probe_reads_live_totals(self):
        self._store(_order_payload(order_id="X1", n_attendees=5))
        self.assertEqual(self.db.event_ticket_totals().get("E1"), 5)

    def test_unknown_counts_are_countable(self):
        self.assertEqual(self.db.unknown_ticket_count_orders(), 0)
        self._store(_order_payload(order_id="X2", with_attendees=False))
        self.assertEqual(self.db.unknown_ticket_count_orders(), 1)

    def test_an_unknown_count_is_not_silently_a_one(self):
        """The whole point: absence is recorded as absence."""
        self._store(_order_payload(order_id="X3", with_attendees=False))
        row = self._row("X3")
        self.assertIsNone(row["ticket_count"])


# ---------------------------------------------------------------------------
# Durable sync state: an interrupted sync must remain visible after restart
# ---------------------------------------------------------------------------
class TestSyncRunVisibility(_Fixture):
    """The in-memory flag died with the container, so after the deploy that
    killed a 74-minute traversal, /api/sync-status reported nothing had run."""

    def test_a_running_sync_is_recorded(self):
        run_id = self.db.start_sync_run("eventbrite")
        self.assertEqual(len(self.db.interrupted_sync_runs()), 1)
        self.db.finish_sync_run(run_id, "completed")
        self.assertEqual(self.db.interrupted_sync_runs(), [])

    def test_a_killed_sync_survives_the_restart(self):
        self.db.start_sync_run("eventbrite")
        reopened = Database(self.db.path)  # a new process, same volume
        self.assertEqual(len(reopened.interrupted_sync_runs()), 1)

    def test_startup_marks_orphans_interrupted(self):
        self.db.start_sync_run("eventbrite")
        self.assertEqual(self.db.mark_orphaned_sync_runs(), 1)
        self.assertEqual(self.db.interrupted_sync_runs(), [])
        self.assertEqual(self.db.last_sync_run("eventbrite")["status"], "interrupted")

    def test_a_clean_run_is_not_marked_interrupted(self):
        run_id = self.db.start_sync_run("eventbrite")
        self.db.finish_sync_run(run_id, "completed")
        self.assertEqual(self.db.mark_orphaned_sync_runs(), 0)
        self.assertEqual(self.db.last_sync_run("eventbrite")["status"], "completed")

    def test_failures_are_recorded_with_detail(self):
        run_id = self.db.start_sync_run("eventbrite")
        self.db.finish_sync_run(run_id, "failed", "API error 500")
        last = self.db.last_sync_run("eventbrite")
        self.assertEqual(last["status"], "failed")
        self.assertIn("500", last["detail"])

    def test_last_run_is_the_most_recent(self):
        first = self.db.start_sync_run("eventbrite")
        self.db.finish_sync_run(first, "completed")
        second = self.db.start_sync_run("eventbrite")
        self.db.finish_sync_run(second, "failed", "boom")
        self.assertEqual(self.db.last_sync_run("eventbrite")["status"], "failed")


# ---------------------------------------------------------------------------
# Refunds and cancellations
# ---------------------------------------------------------------------------
def _attendee(cancelled=False, refunded=False, status="Attending"):
    a = {"profile": {"email": "buyer@example.com"},
         "ticket_class_name": "General Admission", "status": status}
    if cancelled:
        a["cancelled"] = True
    if refunded:
        a["refunded"] = True
    return a


class TestRefundedTicketsAreNotCounted(_Fixture):
    """Eventbrite marks a withdrawn attendee rather than removing it, so
    len(attendees) counted every refund as a ticket still sold. Nothing in the
    codebase looked at these flags."""

    def _order(self, attendees, order_id="R1"):
        d = _order_payload(order_id=order_id)
        d["attendees"] = attendees
        return d

    def test_refunded_attendee_is_excluded(self):
        self._store(self._order([_attendee(), _attendee(), _attendee(refunded=True)]))
        self.assertEqual(self._row("R1")["ticket_count"], 2)

    def test_cancelled_attendee_is_excluded(self):
        self._store(self._order([_attendee(), _attendee(cancelled=True)]))
        self.assertEqual(self._row("R1")["ticket_count"], 1)

    def test_deleted_status_is_excluded(self):
        self._store(self._order([_attendee(), _attendee(status="Deleted")]))
        self.assertEqual(self._row("R1")["ticket_count"], 1)

    def test_not_attending_is_excluded(self):
        self._store(self._order([_attendee(), _attendee(status="Not Attending")]))
        self.assertEqual(self._row("R1")["ticket_count"], 1)

    def test_a_fully_refunded_order_counts_zero(self):
        self._store(self._order([_attendee(refunded=True), _attendee(refunded=True)]))
        self.assertEqual(self._row("R1")["ticket_count"], 0)

    def test_a_later_refund_reduces_the_stored_count(self):
        """The observed-change path: 3 tickets, then one refunded."""
        self._store(self._order([_attendee(), _attendee(), _attendee()]))
        self.assertEqual(self._row("R1")["ticket_count"], 3)
        self._store(self._order([_attendee(), _attendee(), _attendee(refunded=True)]))
        self.assertEqual(self._row("R1")["ticket_count"], 2)

    def test_missing_flags_keep_every_attendee(self):
        """Conservative: a thin payload must not silently delete tickets."""
        bare = [{"profile": {"email": "b@example.com"}} for _ in range(4)]
        self._store(self._order(bare))
        self.assertEqual(self._row("R1")["ticket_count"], 4)

    def test_ticket_type_comes_from_a_live_attendee(self):
        a = _attendee(refunded=True)
        a["ticket_class_name"] = "Refunded VIP"
        self._store(self._order([a, _attendee()]))
        self.assertEqual(self._row("R1")["ticket_type"], "General Admission")

    def test_event_totals_exclude_refunds(self):
        self._store(self._order([_attendee(), _attendee(refunded=True)], order_id="R1"))
        self._store(self._order([_attendee(), _attendee()], order_id="R2"))
        self.assertEqual(self.db.get_event_tickets("E1"), 3)


class TestParsedUnknownsDoNotReachNonNullConsumers(_Fixture):
    """A regression from the merge work: parse output carries None for
    unobserved fields, and it was handed straight to code expecting numbers."""

    def test_unobserved_capacity_does_not_break_snapshot_building(self):
        ev = self.sync._parse_event(_event_payload(event_id="E2", capacity=None))
        self.assertIsNone(ev["capacity"])
        ev["status"] = "completed"
        self.db.upsert_event(ev)
        self._store(_order_payload(order_id="S1"))
        stored = self.db.get_event("E2") or ev
        # Must not raise: this is exactly what failed in production.
        self.sync._build_snapshots("E2", EVENT_DATE.date(), stored.get("capacity") or 0)

    def test_stored_capacity_is_preserved_for_downstream_use(self):
        ev = self.sync._parse_event(_event_payload(capacity=None))
        ev["status"] = "upcoming"
        self.db.upsert_event(ev)
        self.assertEqual(self.db.get_event("E1")["capacity"], 2000)


class TestAuditTruthRegressions(unittest.TestCase):
    """Production ordering, sparse payloads, and partial fetches from the audit."""

    def setUp(self):
        from craft_unified import MetaAdsSync
        self.db = Database(':memory:')
        self.meta = MetaAdsSync.__new__(MetaAdsSync)
        self.meta.db = self.db
        self.meta.ad_account_id = '123'

    def tearDown(self):
        self.db.conn.close()

    def test_missing_meta_metrics_preserve_observed_values(self):
        today = datetime.date.today().isoformat()
        self.db.save_ad_spend('E', 'C', 'Campaign', today, 250, 1000, 20)
        self.db.save_ad_spend('E', 'C', None, today, None, None, 0)
        row = dict(self.db.conn.execute('SELECT * FROM ad_spend').fetchone())
        self.assertEqual((row['spend'], row['impressions'], row['clicks']), (250, 1000, 0))
        self.db.save_ad_spend('E', 'C', None, today, 200)
        self.assertEqual(self.db.get_event_spend('E'), 200)

    def test_unobserved_spend_is_not_trustworthy_zero(self):
        today = datetime.date.today().isoformat()
        self.db.save_ad_spend('E', 'C', 'Campaign', today, None)
        self.assertEqual(self.db.get_spend_status('E'), 'unavailable')
        self.db.save_ad_spend('E', 'C', 'Campaign', today, 0)
        self.assertEqual(self.db.get_spend_status('E'), 'current_zero_spend')

    def test_partial_meta_fetch_does_not_commit_any_rows(self):
        from unittest.mock import Mock
        self.meta._fetch_daily_insights = Mock(side_effect=[
            [{'date_start': '2026-09-01', 'spend': '250'}],
            RuntimeError('Meta API retry budget exhausted')])
        result = self.meta.sync_event_spend('E', 'Austin Coffee', '2026-10-17',
            campaigns_override=[{'id':'C1','name':'One'}, {'id':'C2','name':'Two'}])
        self.assertIn('error', result)
        self.assertEqual(self.db.conn.execute('SELECT COUNT(*) FROM ad_spend').fetchone()[0], 0)

    def test_meta_pagination_failure_is_not_empty_success(self):
        from unittest.mock import Mock
        self.meta._api_get = Mock(return_value=None)
        with self.assertRaises(RuntimeError):
            self.meta._fetch_all_campaigns()

    def test_meta_repeated_page_fails(self):
        from unittest.mock import Mock
        self.meta._api_get = Mock(return_value={'data': [], 'paging': {'next':'https://graph.facebook.com/next'}})
        with self.assertRaises(RuntimeError):
            self.meta._fetch_daily_insights('C', '2026-09-01', '2026-09-15')
        self.assertEqual(self.meta._api_get.call_count, 2)

    def test_meta_token_removed_and_failure_redacted(self):
        from unittest.mock import Mock, patch
        self.meta.session = Mock()
        self.meta.session.get.side_effect = RuntimeError('secret-value-in-URL')
        with patch('time.sleep'), self.assertLogs('craft', level='ERROR') as logs:
            with self.assertRaisesRegex(RuntimeError, 'retry budget exhausted'):
                self.meta._api_get('https://graph.facebook.com/next?access_token=secret-value-in-URL&after=abc')
        self.assertNotIn('secret-value', str(logs.output))
        args, kwargs = self.meta.session.get.call_args
        self.assertNotIn('access_token', args[0])
        self.assertFalse(kwargs['allow_redirects'])
        self.assertEqual(kwargs['params'], {})

    def test_meta_foreign_paging_origin_rejected_before_request(self):
        from unittest.mock import Mock
        self.meta.session = Mock()
        with self.assertRaises(ValueError):
            self.meta._api_get('https://example.com/collect')
        self.meta.session.get.assert_not_called()

    def test_eventbrite_incomplete_pagination_fails(self):
        from unittest.mock import Mock
        sync = EventbriteSync.__new__(EventbriteSync)
        sync._get = Mock(return_value={'orders': [], 'pagination': {'has_more_items': True}})
        with self.assertRaises(RuntimeError):
            sync._paginate('/orders')

    def test_null_amount_does_not_abort_profile_and_unknown_ticket_not_invented(self):
        sync = EventbriteSync.__new__(EventbriteSync)
        profile = sync._build_customer_profile('buyer@example.com', [
            {'order_timestamp': '2026-09-01T10:00:00', 'gross_amount': None, 'ticket_count': None},
            {'order_timestamp': '2026-09-02T10:00:00', 'gross_amount': 100, 'ticket_count': 2}], 3, 3, 3)
        self.assertEqual(profile.total_spent, 100)
        self.assertEqual(profile.total_tickets, 2)

    def test_velocity_uses_latest_calendar_window_with_production_order(self):
        from diagnosis_engine import DiagnosisEngine
        self.db.conn.executemany('INSERT INTO daily_snapshots (event_id,snapshot_date,days_before_event,tickets_cumulative) VALUES (?,?,?,?)', [
            ('E', '2026-09-01', 30, 10), ('E', '2026-09-08', 23, 80),
            ('E', '2026-09-13', 18, 105), ('E', '2026-09-15', 16, 115)])
        self.db.conn.commit()
        engine = DiagnosisEngine(self.db, None)
        self.assertEqual(engine._compute_velocity('E'), 5)
        self.assertEqual(engine._compute_velocity('E', days=2), 5)
        self.assertIsNone(engine._compute_velocity('E', days=1))
