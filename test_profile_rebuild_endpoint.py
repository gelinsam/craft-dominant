"""Contract for POST /api/v2/maintenance/rebuild-profiles.

This endpoint is a TEMPORARY validation surface: it runs the two profile-build
phases over already-persisted SQLite rows so PR #11's write batching can be
measured against the real Railway volume without a 70-minute Eventbrite
traversal and without the post-sync Meta behaviour PR #10 restored.

Because it is a production route that rewrites two real tables, the things
that must stay true are pinned here rather than documented:

  * it cannot reach the Eventbrite or Meta APIs, proved both structurally
    (call-graph closure) and behaviourally (all outbound HTTP made to explode);
  * it requires the existing COMMAND_API_KEY and fails closed without it;
  * it and the Eventbrite full sync refuse each other, in BOTH directions,
    because sync_all() runs the same two phases over an orders table it is
    still filling in;
  * its output is idempotent, and a failure rolls the affected phase back.
"""

import ast
import json
import os
import sys
import threading
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import craft_unified  # noqa: E402
from craft_unified import Database, EventbriteSync, ProfileRebuildBusy  # noqa: E402


ROUTE = "/api/v2/maintenance/rebuild-profiles"
KEY = "test-secret-key-12345"
AUTH = {"Authorization": f"Bearer {KEY}"}


# ---------------------------------------------------------------------------
# Structural proof: nothing network-capable is reachable from the phases
# ---------------------------------------------------------------------------
class TestNoExternalApiIsReachable(unittest.TestCase):
    """Walk the call graph rather than trusting the code to stay honest."""

    #: EventbriteSync members that can issue an Eventbrite request.
    NETWORK_MEMBERS = {"_get", "_paginate", "get_org_id", "session", "api_key"}

    @staticmethod
    def _class_methods(tree, name):
        cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name)
        return {m.name: m for m in cls.body if isinstance(m, ast.FunctionDef)}

    @classmethod
    def _closure(cls):
        """Transitive (owner, method) set reachable from _build_all_customers."""
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_unified.py")).read()
        tree = ast.parse(src)
        tables = {
            "EventbriteSync": cls._class_methods(tree, "EventbriteSync"),
            "Database": cls._class_methods(tree, "Database"),
        }

        def edges(owner, fn):
            """Resolve self.X() against the OWNING class, and self.db.X() to Database."""
            found = set()
            for node in ast.walk(fn):
                if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)):
                    continue
                base = node.func.value
                if isinstance(base, ast.Name) and base.id == "self":
                    found.add((owner, node.func.attr))
                elif (isinstance(base, ast.Attribute) and base.attr == "db"
                      and isinstance(base.value, ast.Name) and base.value.id == "self"):
                    found.add(("Database", node.func.attr))
                elif isinstance(base, ast.Attribute) and base.attr == "conn":
                    found.add(("sqlite3.Connection", node.func.attr))
            return found

        seen, stack = set(), [("EventbriteSync", "_build_all_customers")]
        while stack:
            key = stack.pop()
            if key in seen:
                continue
            seen.add(key)
            owner, name = key
            fn = tables.get(owner, {}).get(name)
            if fn is not None:
                stack.extend(edges(owner, fn))
        return seen

    def test_no_network_capable_method_is_reachable(self):
        reached = self._closure()
        offenders = sorted(
            n for owner, n in reached
            if owner == "EventbriteSync" and n in self.NETWORK_MEMBERS
        )
        self.assertEqual(
            offenders, [],
            f"profile rebuild can reach Eventbrite network method(s): {offenders}",
        )

    def test_closure_touches_no_requests_or_session_attribute(self):
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_unified.py")).read()
        tree = ast.parse(src)
        tables = {
            "EventbriteSync": self._class_methods(tree, "EventbriteSync"),
            "Database": self._class_methods(tree, "Database"),
        }
        offenders = []
        for owner, name in self._closure():
            fn = tables.get(owner, {}).get(name)
            if fn is None:
                continue
            for node in ast.walk(fn):
                if isinstance(node, ast.Attribute) and node.attr == "session":
                    offenders.append(f"{owner}.{name} -> self.session")
                if isinstance(node, ast.Name) and node.id == "requests":
                    offenders.append(f"{owner}.{name} -> requests")
        self.assertEqual(offenders, [], f"HTTP client referenced in closure: {offenders}")

    def test_closure_is_small_and_is_pinned(self):
        """A large jump here means the phases grew new dependencies — re-audit."""
        self.assertLessEqual(len(self._closure()), 25)

    def test_eventbrite_sync_constructor_makes_no_request(self):
        """The control constructs EventbriteSync; that must not call out."""
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_unified.py")).read()
        init = self._class_methods(ast.parse(src), "EventbriteSync")["__init__"]
        for node in ast.walk(init):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                self.assertNotIn(
                    node.func.attr, {"get", "post", "request", "_get", "_paginate"},
                    "EventbriteSync.__init__ issues a request",
                )


# ---------------------------------------------------------------------------
# Route-level behaviour
# ---------------------------------------------------------------------------
class _RouteBase(unittest.TestCase):
    ORDERS = 40
    CUSTOMERS = 8

    def setUp(self):
        self._saved = {k: os.environ.get(k) for k in (
            "COMMAND_API_KEY", "DB_PATH", "V2_ENABLE_EXTERNAL_SEND",
            "MAILCHIMP_API_KEY", "MAILCHIMP_AUDIENCE_ID",
            "META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID",
            "EVENTBRITE_API_KEY", "CRAFT_AUTO_SYNC",
        )}
        os.environ["COMMAND_API_KEY"] = KEY
        os.environ["DB_PATH"] = ":memory:"
        os.environ["CRAFT_AUTO_SYNC"] = "0"
        for k in ("V2_ENABLE_EXTERNAL_SEND", "MAILCHIMP_API_KEY", "MAILCHIMP_AUDIENCE_ID",
                  "META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID"):
            os.environ.pop(k, None)
        os.environ["EVENTBRITE_API_KEY"] = "eb-test-key"

        from craft_v2 import _build_app
        self.app = _build_app()
        self.client = self.app.test_client()
        self.db = self.app.profile_rebuild_control._db
        self._seed()

    def tearDown(self):
        for k, v in self._saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    def _seed(self):
        for i in range(4):
            self.db.upsert_event({
                'event_id': f'ev{i}', 'name': f'Event {i}', 'event_type': 'workshop',
                'city': 'Austin' if i % 2 else 'Denver', 'event_date': f'2026-0{i+1}-10',
                'capacity': 100, 'status': 'completed',
            })
        for n in range(self.ORDERS):
            self.db.insert_order({
                'order_id': f'o{n}', 'event_id': f'ev{n % 4}',
                'email': f'c{n % self.CUSTOMERS}@example.com',
                'order_timestamp': f'2026-01-{(n % 27) + 1:02d}T10:00:00',
                'ticket_count': (n % 3) + 1, 'gross_amount': 50 + n,
                'net_amount': 45 + n, 'days_before_event': (n % 30) + 1,
            })

    def _counts(self):
        c = self.db.conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        p = self.db.conn.execute(
            "SELECT COUNT(*) FROM customer_event_profiles").fetchone()[0]
        return c, p

    #: Rewritten on every run by design; excluded from content comparisons.
    STAMP = "updated_at"

    def _cols(self, table):
        return [r[1] for r in self.db.conn.execute(f"PRAGMA table_info({table})")]

    def _rows(self, include_stamp=False):
        """Content of both profile tables, stable-ordered.

        `updated_at` is excluded unless asked for: both phases stamp it on
        every write, so raw-row equality would be false even when the derived
        content is identical. test_only_updated_at_changes_between_runs pins
        that it really is the only moving column.
        """
        out = []
        for table, order in (("customers", "email"),
                             ("customer_event_profiles", "email, event_type, city")):
            cols = [c for c in self._cols(table)
                    if include_stamp or c != self.STAMP]
            sel = ", ".join(f'"{c}"' for c in cols)
            out.append([tuple(r) for r in self.db.conn.execute(
                f"SELECT {sel} FROM {table} ORDER BY {order}")])
        return tuple(out)


class TestAuth(_RouteBase):
    def test_missing_authorization_is_rejected(self):
        self.assertEqual(self.client.post(ROUTE).status_code, 401)

    def test_wrong_token_is_rejected(self):
        r = self.client.post(ROUTE, headers={"Authorization": "Bearer nope"})
        self.assertEqual(r.status_code, 401)

    def test_unconfigured_command_key_fails_closed(self):
        os.environ.pop("COMMAND_API_KEY", None)
        r = self.client.post(ROUTE, headers=AUTH)
        self.assertEqual(r.status_code, 503)

    def test_rejected_request_does_not_rebuild_anything(self):
        before = self._counts()
        self.client.post(ROUTE)
        self.assertEqual(self._counts(), before)

    def test_get_is_not_allowed(self):
        self.assertEqual(self.client.get(ROUTE, headers=AUTH).status_code, 405)

    def test_no_second_credential_was_introduced(self):
        """The route itself must read no environment credential at all.

        It rides on the existing require_command_auth decorator; if it ever
        starts reaching into os.environ for a token of its own, that is a new
        secret to manage and this fails.
        """
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_v2.py")).read()
        route_fn = next(
            n for n in ast.walk(ast.parse(src))
            if isinstance(n, ast.FunctionDef) and n.name == "v2_rebuild_profiles"
        )
        reads = [
            node.args[0].value
            for node in ast.walk(route_fn)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in ("get", "getenv")
            and node.args and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
            and node.args[0].value.isupper()
        ]
        self.assertEqual(reads, [], f"route reads environment credential(s): {reads}")

    def test_route_is_wrapped_by_the_existing_command_auth(self):
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_v2.py")).read()
        route_fn = next(
            n for n in ast.walk(ast.parse(src))
            if isinstance(n, ast.FunctionDef) and n.name == "v2_rebuild_profiles"
        )
        names = [d.id for d in route_fn.decorator_list if isinstance(d, ast.Name)]
        self.assertIn("require_command_auth", names)


class TestEvidenceResponse(_RouteBase):
    def test_returns_timings_and_counts(self):
        r = self.client.post(ROUTE, headers=AUTH)
        self.assertEqual(r.status_code, 200)
        d = json.loads(r.data)
        for field in ("status", "elapsed_seconds_total",
                      "elapsed_seconds_global_profiles", "elapsed_seconds_event_profiles",
                      "customers_built", "customer_event_profiles_built",
                      "orders_source_count", "events_source_count"):
            self.assertIn(field, d, f"missing evidence field {field}")
        self.assertEqual(d["status"], "ok")
        self.assertEqual(d["orders_source_count"], self.ORDERS)
        self.assertEqual(d["events_source_count"], 4)
        self.assertEqual(d["customers_built"], self.CUSTOMERS)
        self.assertGreater(d["customer_event_profiles_built"], 0)

    def test_phase_timings_are_positive_and_sum_to_total(self):
        d = json.loads(self.client.post(ROUTE, headers=AUTH).data)
        g, e, t = (d["elapsed_seconds_global_profiles"],
                   d["elapsed_seconds_event_profiles"], d["elapsed_seconds_total"])
        self.assertIsNotNone(g)
        self.assertIsNotNone(e)
        self.assertGreaterEqual(g, 0)
        self.assertGreaterEqual(e, 0)
        self.assertAlmostEqual(g + e, t, places=2)

    def test_response_leaks_no_secret_or_customer_data(self):
        body = self.client.post(ROUTE, headers=AUTH).data.decode()
        self.assertNotIn(KEY, body)
        self.assertNotIn("eb-test-key", body)
        self.assertNotIn("@example.com", body)


class TestNoOutboundCallsAtRuntime(_RouteBase):
    def test_rebuild_succeeds_with_all_http_disabled(self):
        """Behavioural counterpart to the call-graph proof."""
        import requests

        calls = []

        class _Exploding:
            """A Session whose headers can be set but which cannot transmit.

            EventbriteSync.__init__ assigns into session.headers, so that has
            to keep working; every method that could put bytes on the wire
            must not.
            """

            def __init__(self):
                self.headers = {}

            def __getattr__(self, name):
                def boom(*a, **k):
                    calls.append(name)
                    raise AssertionError(f"outbound HTTP attempted: {name}")
                return boom

        original_session = requests.Session
        original_get, original_post = requests.get, requests.post

        def _session():
            return _Exploding()

        requests.Session = _session
        requests.get = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("requests.get attempted"))
        requests.post = lambda *a, **k: (_ for _ in ()).throw(
            AssertionError("requests.post attempted"))
        try:
            r = self.client.post(ROUTE, headers=AUTH)
        finally:
            requests.Session = original_session
            requests.get, requests.post = original_get, original_post

        self.assertEqual(r.status_code, 200, r.data)
        self.assertEqual(calls, [], f"HTTP was attempted: {calls}")

    def test_meta_sync_is_not_triggered(self):
        """A rebuild must not start Meta the way a full sync does."""
        started = []
        original = craft_unified.MetaAdsSync
        craft_unified.MetaAdsSync = lambda *a, **k: started.append(1)
        try:
            self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        finally:
            craft_unified.MetaAdsSync = original
        self.assertEqual(started, [], "rebuild constructed a MetaAdsSync")

    def test_no_external_send_occurs(self):
        r = self.client.post(ROUTE, headers=AUTH)
        self.assertEqual(r.status_code, 200)
        health = json.loads(self.client.get("/api/v2/health").data)
        self.assertFalse(health["external_send_enabled"])
        self.assertEqual(health["execution_mode"], "dry_run")
        sends = self.db.conn.execute(
            "SELECT COUNT(*) FROM suppressions").fetchone()[0]
        self.assertEqual(sends, 0, "rebuild touched suppression state")

    def test_pacing_curves_and_snapshots_are_untouched(self):
        self.db.conn.execute(
            "INSERT OR REPLACE INTO pacing_curves (pattern, event_type, source_events,"
            " curve_data, avg_final_sell_through, sample_count)"
            " VALUES ('p','workshop','[]','{}',0.5,1)")
        self.db.conn.commit()
        before_curves = self.db.conn.execute(
            "SELECT COUNT(*) FROM pacing_curves").fetchone()[0]
        before_snaps = self.db.conn.execute(
            "SELECT COUNT(*) FROM daily_snapshots").fetchone()[0]
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        self.assertEqual(self.db.conn.execute(
            "SELECT COUNT(*) FROM pacing_curves").fetchone()[0], before_curves)
        self.assertEqual(self.db.conn.execute(
            "SELECT COUNT(*) FROM daily_snapshots").fetchone()[0], before_snaps)


class TestIdempotence(_RouteBase):
    def test_second_run_produces_identical_rows(self):
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        first = self._rows()
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        self.assertEqual(self._rows(), first, "rebuild is not idempotent")

    def test_counts_are_stable_across_runs(self):
        a = json.loads(self.client.post(ROUTE, headers=AUTH).data)
        b = json.loads(self.client.post(ROUTE, headers=AUTH).data)
        self.assertEqual(a["customers_built"], b["customers_built"])
        self.assertEqual(a["customer_event_profiles_built"],
                         b["customer_event_profiles_built"])

    def test_rebuild_matches_a_direct_phase_invocation(self):
        """Endpoint output must equal calling the phases directly."""
        EventbriteSync('', self.db)._build_all_customers()
        direct = self._rows()
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        self.assertEqual(self._rows(), direct)

    def test_only_updated_at_changes_between_runs(self):
        """Name precisely what a re-run rewrites: the stamp, nothing else."""
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        before = self._rows(include_stamp=True)
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        after = self._rows(include_stamp=True)

        moved = set()
        for table, (b_rows, a_rows) in zip(
                ("customers", "customer_event_profiles"), zip(before, after)):
            cols = self._cols(table)
            self.assertEqual(len(b_rows), len(a_rows), f"{table} row count changed")
            for b, a in zip(b_rows, a_rows):
                for name, bv, av in zip(cols, b, a):
                    if bv != av:
                        moved.add(f"{table}.{name}")
        self.assertEqual(
            moved - {f"customers.{self.STAMP}",
                     f"customer_event_profiles.{self.STAMP}"},
            set(),
            f"a re-run changed more than the write stamp: {sorted(moved)}",
        )


class TestMutualExclusionWithFullSync(_RouteBase):
    def test_rebuild_is_rejected_while_an_eventbrite_sync_runs(self):
        """Drives the REAL gate: /api/sync admits a sync, rebuild must refuse.

        EventbriteSync is stubbed to block, so the sync thread holds the gate
        without touching the network.
        """
        in_sync = threading.Event()
        release = threading.Event()

        class _BlockingSync:
            def __init__(self, api_key, db):
                pass

            def sync_all(self, years_back=2):
                in_sync.set()
                release.wait(10)
                return {'events': 0, 'orders': 0, 'customers': 0, 'curves': 0, 'errors': []}

        original = craft_unified.EventbriteSync
        craft_unified.EventbriteSync = _BlockingSync
        try:
            started = self.client.get("/api/sync")
            self.assertEqual(json.loads(started.data)["status"], "started")
            self.assertTrue(in_sync.wait(10), "stub sync never started")
            r = self.client.post(ROUTE, headers=AUTH)
            self.assertEqual(r.status_code, 409)
            self.assertEqual(json.loads(r.data)["reason"], "eventbrite_sync_running")
        finally:
            release.set()
            craft_unified.EventbriteSync = original

    def test_rebuild_is_admitted_once_the_sync_finishes(self):
        """The refusal must be temporary, not a latch."""
        done = threading.Event()

        class _QuickSync:
            def __init__(self, api_key, db):
                pass

            def sync_all(self, years_back=2):
                done.set()
                return {'events': 0, 'orders': 0, 'customers': 0, 'curves': 0, 'errors': []}

        original = craft_unified.EventbriteSync
        craft_unified.EventbriteSync = _QuickSync
        try:
            self.client.get("/api/sync")
            self.assertTrue(done.wait(10))
            for _ in range(100):
                if not json.loads(self.client.get("/api/sync-status").data)["running"]:
                    break
                threading.Event().wait(0.05)
        finally:
            craft_unified.EventbriteSync = original
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)

    def test_full_sync_is_rejected_while_a_rebuild_runs(self):
        """Exclusion must be mutual; a one-way check races."""
        in_rebuild = threading.Event()
        release = threading.Event()
        sync_status = {}

        control = self.app.profile_rebuild_control
        original = control._db.get_all_emails

        def slow_emails():
            in_rebuild.set()
            release.wait(10)
            return original()

        control._db.get_all_emails = slow_emails
        try:
            t = threading.Thread(
                target=lambda: self.client.post(ROUTE, headers=AUTH), daemon=True)
            t.start()
            self.assertTrue(in_rebuild.wait(10), "rebuild never started")
            resp = self.client.get("/api/sync")
            sync_status['code'] = resp.status_code
            sync_status['body'] = json.loads(resp.data)
            release.set()
            t.join(15)
        finally:
            control._db.get_all_emails = original
            release.set()

        self.assertEqual(sync_status['code'], 409,
                         "/api/sync started on top of a running profile rebuild")
        self.assertEqual(sync_status['body']["status"], "profile_rebuild_running")

    def test_concurrent_rebuilds_are_rejected(self):
        in_rebuild = threading.Event()
        release = threading.Event()
        second = {}

        control = self.app.profile_rebuild_control
        original = control._db.get_all_emails

        def slow_emails():
            in_rebuild.set()
            release.wait(10)
            return original()

        control._db.get_all_emails = slow_emails
        try:
            t = threading.Thread(
                target=lambda: self.client.post(ROUTE, headers=AUTH), daemon=True)
            t.start()
            self.assertTrue(in_rebuild.wait(10), "first rebuild never started")
            r = self.client.post(ROUTE, headers=AUTH)
            second['code'] = r.status_code
            second['reason'] = json.loads(r.data).get("reason")
            release.set()
            t.join(15)
        finally:
            control._db.get_all_emails = original
            release.set()

        self.assertEqual(second['code'], 409)
        self.assertEqual(second['reason'], "profile_rebuild_running")

    def test_gate_is_released_after_a_successful_run(self):
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)

    def test_gate_is_released_after_a_failed_run(self):
        control = self.app.profile_rebuild_control
        original = control._db.get_all_emails
        control._db.get_all_emails = lambda: (_ for _ in ()).throw(RuntimeError("boom"))
        try:
            self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 500)
        finally:
            control._db.get_all_emails = original
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200,
                         "gate stayed claimed after a failure")

    def test_meta_sync_is_not_excluded(self):
        """Meta writes ad_spend/daily_snapshots — disjoint from the profile tables."""
        control = self.app.profile_rebuild_control
        self.assertIsNone(control._claim())
        control._release()
        # No Meta guard is consulted by the rebuild gate.
        src = open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "craft_unified.py")).read()
        self.assertIn("_claim_profile_rebuild", src)


class TestFailureRollsBackThePhase(_RouteBase):
    def test_failure_mid_phase_leaves_previous_contents(self):
        """PR #11's batching must still own the rollback for this caller."""
        self.assertEqual(self.client.post(ROUTE, headers=AUTH).status_code, 200)
        good = self._rows()

        real = Database.upsert_event_profile
        state = {"n": 0}

        def flaky(self_db, *a, **k):
            state["n"] += 1
            if state["n"] > 2:
                raise RuntimeError("phase fails partway")
            return real(self_db, *a, **k)

        Database.upsert_event_profile = flaky
        try:
            r = self.client.post(ROUTE, headers=AUTH)
            self.assertEqual(r.status_code, 500)
        finally:
            Database.upsert_event_profile = real

        self.assertEqual(
            self._rows()[1], good[1],
            "event-profile phase did not roll back; table holds a partial rebuild",
        )

    def test_busy_exception_type_is_exported(self):
        self.assertTrue(issubclass(ProfileRebuildBusy, RuntimeError))
        self.assertEqual(ProfileRebuildBusy("x").reason, "x")


class TestControlIsNotBroadened(_RouteBase):
    def test_only_one_maintenance_route_exists(self):
        routes = [r.rule for r in self.app.url_map.iter_rules()
                  if "/maintenance/" in r.rule]
        self.assertEqual(routes, [ROUTE],
                         "maintenance surface grew beyond the single validation route")

    def test_route_accepts_no_parameters(self):
        rule = next(r for r in self.app.url_map.iter_rules() if r.rule == ROUTE)
        self.assertEqual(rule.arguments, set())


if __name__ == "__main__":
    unittest.main(verbosity=2)
