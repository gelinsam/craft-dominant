"""Analytics durability + external-send observability (PR #7).

Two concerns, both about making production state legible:

1. `external_send_enabled` — whether real sending is possible must be a fact you
   can read, not something inferred from the absence of evidence. The truth
   table is exhaustive because the safe state has to be the default: anything
   that is not exactly "1" means disabled.

2. Analytics storage — the analytical SQLite database lived on the container's
   ephemeral filesystem, so every Railway redeploy destroyed it. These tests
   cover the diagnostics used to prove the database now lives on a mounted
   volume and that its contents survive a restart.
"""

import json
import os
import sqlite3
import tempfile
import unittest

os.environ.setdefault("TESTING", "1")
# craft_v2 builds its Flask app at import time, which opens the analytics
# database. Point that at an in-memory DB before the first import so importing
# this module can never touch (or be poisoned by) a real SQLite file on disk.
os.environ.setdefault("DB_PATH", ":memory:")

from craft_v2 import (  # noqa: E402
    ANALYTICS_TABLES,
    analytics_row_counts,
    analytics_storage_info,
    external_send_enabled,
)


class TestExternalSendFlagSemantics(unittest.TestCase):
    """Only the literal "1" may enable sending; everything else fails safe."""

    def setUp(self):
        self._original = os.environ.get("V2_ENABLE_EXTERNAL_SEND")

    def tearDown(self):
        if self._original is None:
            os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        else:
            os.environ["V2_ENABLE_EXTERNAL_SEND"] = self._original

    def test_unset_is_false(self):
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        self.assertFalse(external_send_enabled())

    def test_zero_is_false(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "0"
        self.assertFalse(external_send_enabled())

    def test_one_is_true(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        self.assertTrue(external_send_enabled())

    def test_truthy_looking_strings_are_still_false(self):
        """"true"/"yes"/"on" must NOT enable sending.

        A permissive parser here would be the single most dangerous bug in the
        system: a typo in a Railway variable would start sending real email.
        """
        for value in ("true", "TRUE", "True", "yes", "on", "enabled", "2", "-1", "1 ", " 1", "", "01"):
            with self.subTest(value=value):
                os.environ["V2_ENABLE_EXTERNAL_SEND"] = value
                self.assertFalse(
                    external_send_enabled(),
                    f"{value!r} must not enable external sending",
                )

    def test_matches_execution_adapter_gate_exactly(self):
        """Health must not be able to disagree with the adapter that sends.

        Read the adapter's own expression so the two cannot drift apart
        silently; a comforting-but-wrong health field is worse than none.
        """
        import inspect

        import execution_adapter

        source = inspect.getsource(execution_adapter)
        self.assertIn(
            'os.environ.get("V2_ENABLE_EXTERNAL_SEND", "0") == "1"',
            source,
            "ExecutionAdapter gate changed — external_send_enabled() must be updated to match",
        )


class TestAnalyticsStorageInfo(unittest.TestCase):
    """Storage diagnostics must describe reality, and never raise."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, "craft_unified.db")
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("CREATE TABLE events (event_id TEXT PRIMARY KEY)")
        conn.execute("INSERT INTO events VALUES ('e1')")
        conn.commit()

        class _FakeDB:
            pass

        self.db = _FakeDB()
        self.db.path = self.db_path
        self.db.conn = conn

    def tearDown(self):
        self.db.conn.close()
        self._tmp.cleanup()

    def test_reports_absolute_path_and_existence(self):
        info = analytics_storage_info(self.db)
        self.assertTrue(os.path.isabs(info["db_path"]))
        self.assertEqual(info["db_path"], os.path.abspath(self.db_path))
        self.assertTrue(info["exists"])
        self.assertGreater(info["size_bytes"], 0)

    def test_reports_wal_journal_mode(self):
        """WAL must be preserved — it is part of the locking hardening."""
        info = analytics_storage_info(self.db)
        self.assertEqual((info["journal_mode"] or "").lower(), "wal")

    def test_directory_is_mount_false_for_ordinary_directory(self):
        """A temp dir is not a mount point, which is the ephemeral case."""
        info = analytics_storage_info(self.db)
        self.assertFalse(info["directory_is_mount"])

    def test_db_path_configured_reflects_env(self):
        original = os.environ.get("DB_PATH")
        try:
            os.environ.pop("DB_PATH", None)
            self.assertFalse(analytics_storage_info(self.db)["db_path_configured"])
            os.environ["DB_PATH"] = self.db_path
            self.assertTrue(analytics_storage_info(self.db)["db_path_configured"])
        finally:
            if original is None:
                os.environ.pop("DB_PATH", None)
            else:
                os.environ["DB_PATH"] = original

    def test_never_raises_on_broken_database(self):
        """Diagnostics must never be the reason health fails."""

        class _Broken:
            path = "/nonexistent/dir/craft_unified.db"

            class conn:  # noqa: N801
                @staticmethod
                def execute(*_a, **_k):
                    raise sqlite3.OperationalError("boom")

        info = analytics_storage_info(_Broken())
        self.assertFalse(info["exists"])
        self.assertIsNone(info["journal_mode"])


class TestAnalyticsRowCounts(unittest.TestCase):
    """Missing tables must read as None, never as a confident zero."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        path = os.path.join(self._tmp.name, "a.db")
        conn = sqlite3.connect(path)
        conn.execute("CREATE TABLE events (event_id TEXT)")
        conn.executemany("INSERT INTO events VALUES (?)", [("a",), ("b",), ("c",)])
        conn.execute("CREATE TABLE orders (id TEXT)")
        conn.commit()

        class _FakeDB:
            pass

        self.db = _FakeDB()
        self.db.path = path
        self.db.conn = conn

    def tearDown(self):
        self.db.conn.close()
        self._tmp.cleanup()

    def test_counts_present_tables(self):
        counts = analytics_row_counts(self.db)
        self.assertEqual(counts["events"], 3)
        self.assertEqual(counts["orders"], 0)

    def test_missing_table_is_none_not_zero(self):
        """None means "unknown"; 0 would be a false negative.

        This is the same false-zero distinction the paid-media work depends on:
        a failed lookup is not evidence of absence.
        """
        counts = analytics_row_counts(self.db)
        self.assertIsNone(counts["ad_spend"])
        self.assertIsNone(counts["suppressions"])

    def test_covers_every_declared_analytics_table(self):
        counts = analytics_row_counts(self.db)
        self.assertEqual(set(counts), set(ANALYTICS_TABLES))
        for table in (
            "events",
            "orders",
            "customers",
            "customer_event_profiles",
            "daily_snapshots",
            "pacing_curves",
            "ad_spend",
            "suppressions",
        ):
            self.assertIn(table, counts)


class TestHealthAndDiagnosticsRoutes(unittest.TestCase):
    """Route-level contract for the new observability."""

    def setUp(self):
        os.environ["COMMAND_API_KEY"] = "test-secret-key-12345"
        os.environ["DB_PATH"] = ":memory:"
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)
        os.environ.pop("MAILCHIMP_API_KEY", None)
        os.environ.pop("MAILCHIMP_AUDIENCE_ID", None)

        from craft_v2 import _build_app

        self.app = _build_app()
        self.client = self.app.test_client()

    def tearDown(self):
        os.environ.pop("COMMAND_API_KEY", None)
        os.environ.pop("DB_PATH", None)
        os.environ.pop("V2_ENABLE_EXTERNAL_SEND", None)

    def test_health_exposes_external_send_disabled_by_default(self):
        data = json.loads(self.client.get("/api/v2/health").data)
        self.assertIn("external_send_enabled", data)
        self.assertIs(data["external_send_enabled"], False)
        self.assertEqual(data["execution_mode"], "dry_run")

    def test_health_reflects_enabled_flag(self):
        os.environ["V2_ENABLE_EXTERNAL_SEND"] = "1"
        from craft_v2 import _build_app

        client = _build_app().test_client()
        data = json.loads(client.get("/api/v2/health").data)
        self.assertIs(data["external_send_enabled"], True)
        self.assertEqual(data["execution_mode"], "external")

    def test_health_is_boolean_not_string(self):
        """Consumers gate on this; a truthy string like "false" would be a trap."""
        data = json.loads(self.client.get("/api/v2/health").data)
        self.assertIsInstance(data["external_send_enabled"], bool)

    def test_health_never_leaks_env_or_secrets(self):
        body = self.client.get("/api/v2/health").data.decode()
        self.assertNotIn("test-secret-key-12345", body)
        self.assertNotIn("V2_ENABLE_EXTERNAL_SEND", body)
        self.assertNotIn("COMMAND_API_KEY", body)

    def test_health_does_not_expose_row_counts_publicly(self):
        """Health is unauthenticated, so business volumes stay out of it."""
        data = json.loads(self.client.get("/api/v2/health").data)
        self.assertNotIn("row_counts", data)
        self.assertIn("analytics_persistent", data)

    def test_diagnostics_requires_auth(self):
        self.assertEqual(self.client.get("/api/v2/diagnostics/analytics").status_code, 401)

    def test_diagnostics_returns_storage_and_counts(self):
        resp = self.client.get(
            "/api/v2/diagnostics/analytics",
            headers={"Authorization": "Bearer test-secret-key-12345"},
        )
        self.assertEqual(resp.status_code, 200)
        data = json.loads(resp.data)
        self.assertIn("storage", data)
        self.assertIn("row_counts", data)
        self.assertIn("db_path", data["storage"])
        self.assertEqual(set(data["row_counts"]), set(ANALYTICS_TABLES))

    def test_diagnostics_does_not_leak_secret(self):
        resp = self.client.get(
            "/api/v2/diagnostics/analytics",
            headers={"Authorization": "Bearer test-secret-key-12345"},
        )
        self.assertNotIn("test-secret-key-12345", resp.data.decode())


if __name__ == "__main__":
    unittest.main()
