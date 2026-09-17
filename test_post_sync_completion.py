"""A completed Eventbrite sync must run its post-sync block to the end.

`_do_background_sync()` assigns `_meta_sync_running` in its post-sync block.
In Python that makes the name local to the *entire* function unless declared
nonlocal, so the read in `if _meta_sync_running:` raised

    UnboundLocalError: cannot access local variable '_meta_sync_running'
                       where it is not associated with a value

The Eventbrite phase had already finished and its rows were committed, so the
data was fine — but everything after it was skipped: the Meta block, the
milestone export check, and the alert check. The run then recorded that
exception in sync_state, so a successful rebuild reported itself as failed.

Production only ever showed this once the rebuild could reach the end. Before
the customer-profile N+1 was fixed, gunicorn killed the worker during profile
building and execution never got this far.

These tests pin both the binding itself and the behaviour that depends on it.
"""

import json
import os
import threading
import time
import unittest
from unittest.mock import patch

os.environ.setdefault("TESTING", "1")
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("CRAFT_AUTO_SYNC", "0")

import craft_unified  # noqa: E402
from craft_unified import Database, create_app  # noqa: E402


SYNC_RESULT = {"events": 3, "orders": 11, "customers": 7, "curves": 2, "errors": []}

_META_ENV = ("META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID")


def _nested_code(func, name):
    """The code object of a function defined inside `func`."""
    for const in func.__code__.co_consts:
        if getattr(const, "co_name", None) == name:
            return const
    raise AssertionError(f"no nested function named {name!r}")



#: /api/meta-sync is authenticated; these tests drive it as an operator would.
_AUTH = {"Authorization": "Bearer test-secret-key-12345"}

class _FakeEventbriteSync:
    """Stands in for the real traversal — returns immediately."""

    instances = 0

    def __init__(self, api_key, db):
        type(self).instances += 1
        self.db = db

    def sync_all(self, years_back=4):
        return dict(SYNC_RESULT)


class _FakeMetaAdsSync:
    """Records that Meta was constructed, and can be made to block."""

    instances = 0
    entered = None   # threading.Event — set once sync_all_events starts
    release = None   # threading.Event — sync_all_events waits on this

    def __init__(self, token, account_id, db):
        type(self).instances += 1

    def sync_all_events(self, events):
        cls = type(self)
        if cls.entered is not None:
            cls.entered.set()
        if cls.release is not None:
            cls.release.wait(timeout=10)
        return {"total_spend": 0.0, "successful": 0}

    @classmethod
    def reset(cls):
        cls.instances = 0
        cls.entered = None
        cls.release = None


class _RecordingConnection:
    """Delegating wrapper that records the SQL it is asked to run.

    sqlite3.Connection.execute is read-only, so the connection has to be
    wrapped rather than monkeypatched.
    """

    def __init__(self, conn, sink):
        self._conn = conn
        self._sink = sink

    def execute(self, sql, *a, **kw):
        self._sink.append(" ".join(str(sql).split()))
        return self._conn.execute(sql, *a, **kw)

    def executescript(self, sql, *a, **kw):
        self._sink.append(" ".join(str(sql).split()))
        return self._conn.executescript(sql, *a, **kw)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class _PostSyncHarness(unittest.TestCase):
    """Builds an app whose sync finishes instantly, and watches what follows."""

    def setUp(self):
        _FakeEventbriteSync.instances = 0
        _FakeMetaAdsSync.reset()

        self._env_before = {
            k: os.environ.get(k)
            for k in _META_ENV + ("EVENTBRITE_API_KEY", "COMMAND_API_KEY")
        }
        os.environ["EVENTBRITE_API_KEY"] = "fake-eventbrite-key"
        # /api/sync and /api/sync-status now sit behind the app-wide bearer gate,
        # so the harness must authenticate the way the dashboard's proxy does.
        os.environ["COMMAND_API_KEY"] = _AUTH["Authorization"].split()[-1]
        for key in _META_ENV:
            os.environ.pop(key, None)

        self.db = Database(":memory:")
        # Both post-sync checks begin by listing upcoming events. Returning an
        # empty list keeps them side-effect free while still proving they ran.
        self.get_events_calls = []
        real_get_events = self.db.get_events

        def spy_get_events(*a, **kw):
            self.get_events_calls.append(kw.get("upcoming_only", a[0] if a else None))
            return []

        self.db.get_events = spy_get_events
        self._real_get_events = real_get_events

        self.executed_sql = []
        self.db.conn = _RecordingConnection(self.db.conn, self.executed_sql)

        self._patchers = [
            patch.object(craft_unified, "EventbriteSync", _FakeEventbriteSync),
            patch.object(craft_unified, "MetaAdsSync", _FakeMetaAdsSync),
        ]
        for p in self._patchers:
            p.start()

        self.app = create_app(self.db, auto_sync=False)
        self.client = self.app.test_client()

    def tearDown(self):
        if _FakeMetaAdsSync.release is not None:
            _FakeMetaAdsSync.release.set()
        for p in self._patchers:
            p.stop()
        for key, value in self._env_before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    def _run_sync_to_completion(self, timeout=10):
        resp = self.client.get("/api/sync", headers=_AUTH)
        self.assertEqual(json.loads(resp.data)["status"], "started")
        deadline = time.time() + timeout
        while time.time() < deadline:
            state = json.loads(self.client.get("/api/sync-status", headers=_AUTH).data)
            if state["done"]:
                return state
            time.sleep(0.02)
        self.fail("background sync did not finish within timeout")

    def _ran_sql_containing(self, needle):
        return any(needle in sql for sql in self.executed_sql)


class TestBindingIsShared(unittest.TestCase):
    """The precise defect: is _meta_sync_running local or captured?

    This is a direct assertion about the compiled binding rather than about
    behaviour, so it fails for exactly one reason and cannot pass by accident.
    """

    def test_background_sync_captures_the_shared_guard(self):
        code = _nested_code(create_app, "_do_background_sync")
        self.assertIn(
            "_meta_sync_running", code.co_freevars,
            "_do_background_sync must capture the shared guard (nonlocal), "
            "otherwise reading it raises UnboundLocalError",
        )
        self.assertNotIn(
            "_meta_sync_running", code.co_varnames,
            "_meta_sync_running must not be a local of _do_background_sync",
        )

    def test_meta_sync_endpoint_captures_the_same_guard(self):
        """Both writers must share one variable or single-flight is fiction."""
        code = _nested_code(create_app, "meta_sync_endpoint")
        self.assertIn("_meta_sync_running", code.co_freevars)

    def test_every_writer_of_the_guard_declares_nonlocal(self):
        """Guards against a third writer being added without the declaration."""
        offenders = []
        for const in create_app.__code__.co_consts:
            name = getattr(const, "co_name", None)
            if name is None:
                continue
            writes = "_meta_sync_running" in const.co_names or \
                     "_meta_sync_running" in const.co_varnames
            if "_meta_sync_running" in const.co_varnames:
                offenders.append(name)
            del writes
        self.assertEqual(
            offenders, [],
            f"these nested functions treat _meta_sync_running as a local: {offenders}",
        )


class TestPostSyncBlockCompletes(_PostSyncHarness):
    """With no Meta credentials — the configuration production runs today."""

    def test_sync_finishes_without_recording_an_error(self):
        state = self._run_sync_to_completion()
        self.assertIsNone(
            state["error"],
            f"sync recorded an error after completing: {state['error']!r}",
        )
        self.assertFalse(state["running"])
        self.assertEqual(state["result"], SYNC_RESULT)

    def test_error_is_not_the_unbound_local(self):
        """Names the specific regression so a failure is self-explaining."""
        state = self._run_sync_to_completion()
        self.assertNotIn("_meta_sync_running", str(state["error"]))
        self.assertNotIn("UnboundLocalError", str(state["error"]))
        self.assertNotIn("not associated with a value", str(state["error"]))

    def test_eventbrite_phase_still_ran_exactly_once(self):
        self._run_sync_to_completion()
        self.assertEqual(_FakeEventbriteSync.instances, 1)

    def test_milestone_export_check_is_reached(self):
        """_check_milestones_and_export() lists upcoming events first."""
        self._run_sync_to_completion()
        self.assertIn(
            True, self.get_events_calls,
            "no upcoming-events listing after the sync — the post-sync block "
            "was skipped",
        )

    def test_alert_check_is_reached(self):
        """_check_and_send_alerts() queries auto_exports for unnotified rows.

        That query belongs to no other code path, so seeing it proves the
        second post-sync function ran — and it runs *after* the milestone
        check, so reaching it proves both.
        """
        self._run_sync_to_completion()
        self.assertTrue(
            self._ran_sql_containing("FROM auto_exports WHERE sent_notified = 0"),
            "alert check never ran",
        )

    def test_both_post_sync_checks_ran_in_order(self):
        self._run_sync_to_completion()
        self.assertGreaterEqual(
            len(self.get_events_calls), 2,
            "expected upcoming events to be listed by both post-sync checks",
        )

    def test_meta_not_invoked_without_credentials(self):
        """Completing a rebuild must not by itself cause a Meta sync."""
        self._run_sync_to_completion()
        self.assertEqual(
            _FakeMetaAdsSync.instances, 0,
            "Meta was invoked even though no credentials are configured",
        )

    def test_meta_status_untouched(self):
        self._run_sync_to_completion()
        data = json.loads(self.client.get("/api/meta-status", headers=_AUTH).data)
        self.assertEqual(data["data"]["total_spend"], 0)
        self.assertEqual(data["data"]["campaigns"], 0)


class TestPartialSyncEvidence(_PostSyncHarness):
    def test_partial_event_failure_is_not_complete_sales_evidence(self):
        result = dict(SYNC_RESULT, errors=['one event page failed'])
        with patch.object(_FakeEventbriteSync, 'sync_all', return_value=result):
            state = self._run_sync_to_completion()
        self.assertFalse(state['running'])
        self.assertEqual(state['last_run']['status'], 'completed_with_integrity_warnings')
        self.assertEqual(json.loads(state['last_run']['detail'])['event_errors'], 1)
        self.assertEqual(state['interrupted_runs'], [])

    def test_existing_unknown_ticket_counts_also_hold_measurement(self):
        result = dict(SYNC_RESULT, integrity={
            'events_with_ticket_loss': 0,
            'orders_with_unknown_ticket_count': 2,
            'unknown_ticket_count_delta': 0,
        })
        with patch.object(_FakeEventbriteSync, 'sync_all', return_value=result):
            state = self._run_sync_to_completion()
        self.assertEqual(state['last_run']['status'], 'completed_with_integrity_warnings')

    def test_clean_traversal_remains_complete(self):
        state = self._run_sync_to_completion()
        self.assertEqual(state['last_run']['status'], 'completed')
        self.assertEqual(json.loads(state['last_run']['detail'])['event_errors'], 0)


class TestExistingMetaSemanticsPreserved(_PostSyncHarness):
    """With credentials present the prior behaviour must be unchanged."""

    def setUp(self):
        super().setUp()
        os.environ["META_ACCESS_TOKEN"] = "fake-meta-token"
        os.environ["META_AD_ACCOUNT_ID"] = "act_fake_1"

    def test_meta_runs_when_configured(self):
        """The fix must not turn the existing Meta trigger off."""
        state = self._run_sync_to_completion()
        self.assertIsNone(state["error"])
        self.assertEqual(_FakeMetaAdsSync.instances, 1)

    def test_multiple_accounts_each_sync(self):
        os.environ["META_AD_ACCOUNT_ID"] = "act_fake_1, act_fake_2"
        self._run_sync_to_completion()
        self.assertEqual(_FakeMetaAdsSync.instances, 2)

    def test_guard_is_released_after_the_sync(self):
        """A later manual Meta sync must not be blocked by a stale flag."""
        self._run_sync_to_completion()
        resp = self.client.get("/api/meta-sync", headers=_AUTH)
        self.assertNotEqual(
            resp.status_code, 409,
            "guard left set — /api/meta-sync wrongly reports already_running",
        )

    def test_milestone_export_check_is_reached_with_meta_configured(self):
        """The production configuration — this is where the bug actually bit.

        Without Meta credentials the guard was never read, so the exception
        never fired. Production has META_ACCESS_TOKEN and META_AD_ACCOUNT_ID
        set, which is why the milestone check silently stopped running there.
        """
        state = self._run_sync_to_completion()
        self.assertIsNone(state["error"])
        self.assertIn(
            True, self.get_events_calls,
            "milestone export check was skipped",
        )

    def test_alert_check_is_reached_with_meta_configured(self):
        state = self._run_sync_to_completion()
        self.assertIsNone(state["error"])
        self.assertTrue(
            self._ran_sql_containing("FROM auto_exports WHERE sent_notified = 0"),
            "alert check was skipped",
        )

    def test_sync_state_records_success_not_the_unbound_local(self):
        state = self._run_sync_to_completion()
        self.assertIsNone(state["error"])
        self.assertEqual(state["result"], SYNC_RESULT)
        self.assertTrue(state["done"])
        self.assertFalse(state["running"])


class TestSingleFlightIsGenuinelyShared(_PostSyncHarness):
    """The guard must be one variable, observable from both writers.

    If the background sync held its own local copy, the endpoint would happily
    start a second concurrent Meta sync against the same SQLite file — which is
    the contention the single-flight guard exists to prevent.
    """

    def setUp(self):
        super().setUp()
        os.environ["META_ACCESS_TOKEN"] = "fake-meta-token"
        os.environ["META_AD_ACCOUNT_ID"] = "act_fake_1"
        # /api/meta-sync is authenticated now, so the gate must be configured
        # for this test to reach the single-flight guard behind it.
        os.environ["COMMAND_API_KEY"] = _AUTH["Authorization"].split()[-1]

    def test_endpoint_sees_the_flag_set_by_the_background_sync(self):
        _FakeMetaAdsSync.entered = threading.Event()
        _FakeMetaAdsSync.release = threading.Event()

        self.assertEqual(json.loads(self.client.get("/api/sync", headers=_AUTH).data)["status"], "started")
        self.assertTrue(
            _FakeMetaAdsSync.entered.wait(timeout=10),
            "background sync never reached the Meta phase",
        )

        resp = self.client.get("/api/meta-sync", headers=_AUTH)
        self.assertEqual(
            resp.status_code, 409,
            "endpoint did not see the guard set by the background sync — "
            "the two are not sharing one variable",
        )
        self.assertEqual(json.loads(resp.data)["status"], "already_running")

        _FakeMetaAdsSync.release.set()
        deadline = time.time() + 10
        while time.time() < deadline:
            if json.loads(self.client.get("/api/sync-status", headers=_AUTH).data)["done"]:
                break
            time.sleep(0.02)
        else:
            self.fail("sync did not finish after release")

        self.assertIsNone(json.loads(self.client.get("/api/sync-status", headers=_AUTH).data)["error"])


if __name__ == "__main__":
    unittest.main()


class TestCoherentSyncStatus(_PostSyncHarness):
    def test_completion_during_durable_read_does_not_publish_mixed_snapshot(self):
        import inspect
        state = inspect.getclosurevars(self.app.view_functions['sync_status']).nonlocals['_sync_state']
        state.update(done=False, running=True)
        def finishes_during_read(*args):
            state.update(done=True, running=False)
            return {'status': 'running'}
        with patch.object(self.db, 'last_sync_run', side_effect=finishes_during_read):
            response = self.client.get('/api/sync-status', headers=_AUTH).get_json()
        self.assertFalse(response['done'])
        self.assertTrue(response['running'])


class TestMissingOrderEmail(unittest.TestCase):
    def test_null_email_uses_observed_attendee_identity(self):
        from datetime import datetime
        sync = craft_unified.EventbriteSync('unused', None)
        result = sync._parse_order({'id':'test','created':'2025-01-01T00:00:00Z',
            'email':None,'attendees':[{'profile':{'email':'BUYER@example.com'}}]},
            'event',datetime(2025,2,1))
        self.assertEqual(result['email'],'buyer@example.com')
    def test_unknown_identity_does_not_crash_or_invent_customer(self):
        from datetime import datetime
        sync = craft_unified.EventbriteSync('unused', None)
        for attendees in [None,[],[{'profile':None}],[{'profile':{'email':None}}]]:
            result = sync._parse_order({'id':'test','created':'2025-01-01T00:00:00Z',
                'email':None,'attendees':attendees},'event',datetime(2025,2,1))
            self.assertIsNone(result)

from test_launch_intelligence import LaunchEvidenceTests, LaunchCoverageTests
