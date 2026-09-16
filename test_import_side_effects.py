"""Importing a module must never start a network sync.

craft_unified used to end with a module-level `app = create_app_with_db(
auto_sync=True)`. Because craft_v2 imports craft_unified for Database /
DecisionEngine / create_app, production booted two Flask apps, opened two
SQLite connections to the same file, and ran two concurrent Eventbrite syncs —
every event fetched twice.

That is a correctness problem (two writers on one SQLite file, double API
traffic), and it also made a clean "restart without syncing" durability proof
impossible, because a sync started on its own within seconds of every boot.

These tests pin the invariant: import is inert, construction decides.
"""

import importlib
import os
import sys
import unittest
from unittest.mock import patch


class TestImportIsSideEffectFree(unittest.TestCase):
    """Importing must not construct apps, open databases, or hit the network."""

    def test_craft_unified_has_no_module_level_app(self):
        """The module-level `app` is gone entirely.

        Nothing imports craft_unified.app, and Railway overrides the Procfile
        with `gunicorn craft_v2:app`, so it was never served — only a side
        effect. Deploying craft_unified directly now uses gunicorn's factory
        syntax instead.
        """
        import craft_unified

        self.assertFalse(
            hasattr(craft_unified, "app"),
            "craft_unified must not expose a module-level app; construction "
            "must go through create_app_with_db()",
        )

    def test_importing_craft_unified_starts_no_sync(self):
        """Re-importing craft_unified must not call the Eventbrite sync."""
        sys.modules.pop("craft_unified", None)
        with patch("threading.Thread") as thread:
            importlib.import_module("craft_unified")
        self.assertEqual(
            thread.call_count,
            0,
            "importing craft_unified must not start any background thread",
        )

    def test_source_has_no_import_time_construction(self):
        """Guard against the pattern being reintroduced.

        A future edit could re-add `app = create_app_with_db(...)` at module
        scope; this fails loudly if so.
        """
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "craft_unified.py")) as fh:
            lines = fh.read().splitlines()

        offenders = [
            line
            for line in lines
            if line.startswith("app = ") or line.startswith("app=")
        ]
        self.assertEqual(
            offenders,
            [],
            f"module-level app construction reintroduced: {offenders}",
        )


class TestAutoSyncPolicy(unittest.TestCase):
    """CRAFT_AUTO_SYNC is the single, shared switch."""

    def setUp(self):
        self._original = os.environ.get("CRAFT_AUTO_SYNC")

    def tearDown(self):
        if self._original is None:
            os.environ.pop("CRAFT_AUTO_SYNC", None)
        else:
            os.environ["CRAFT_AUTO_SYNC"] = self._original

    def test_unset_defaults_to_enabled(self):
        """Production must keep syncing by default after this change.

        Disabling ingestion silently would be its own outage, so unset means
        enabled here — the opposite default to V2_ENABLE_EXTERNAL_SEND, which
        must fail closed.
        """
        from craft_unified import auto_sync_enabled

        os.environ.pop("CRAFT_AUTO_SYNC", None)
        self.assertTrue(auto_sync_enabled())

    def test_one_enables(self):
        from craft_unified import auto_sync_enabled

        os.environ["CRAFT_AUTO_SYNC"] = "1"
        self.assertTrue(auto_sync_enabled())

    def test_zero_disables(self):
        from craft_unified import auto_sync_enabled

        os.environ["CRAFT_AUTO_SYNC"] = "0"
        self.assertFalse(auto_sync_enabled())

    def test_unrecognised_values_disable(self):
        from craft_unified import auto_sync_enabled

        for value in ("true", "TRUE", "yes", "on", "", "2", " 1"):
            with self.subTest(value=value):
                os.environ["CRAFT_AUTO_SYNC"] = value
                self.assertFalse(auto_sync_enabled())

    def test_both_entry_points_share_one_policy(self):
        """craft_v2 must not re-implement the env check independently."""
        here = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(here, "craft_v2.py")) as fh:
            source = fh.read()

        self.assertIn("auto_sync_enabled()", source)
        self.assertNotIn(
            'os.environ.get("CRAFT_AUTO_SYNC"',
            source,
            "craft_v2 must defer to the shared auto_sync_enabled() policy",
        )


class TestExactlyOneStartupSync(unittest.TestCase):
    """One production app construction starts at most one sync."""

    def setUp(self):
        self._original = os.environ.get("CRAFT_AUTO_SYNC")
        os.environ.setdefault("DB_PATH", ":memory:")

    def tearDown(self):
        if self._original is None:
            os.environ.pop("CRAFT_AUTO_SYNC", None)
        else:
            os.environ["CRAFT_AUTO_SYNC"] = self._original

    def test_disabled_starts_no_sync(self):
        """CRAFT_AUTO_SYNC=0 must start no background sync at all.

        This is what makes a clean restart-durability proof possible: the row
        counts after a restart cannot be muddied by a sync writing rows.
        """
        import craft_unified

        os.environ["CRAFT_AUTO_SYNC"] = "0"
        with patch("threading.Thread") as thread:
            craft_unified.create_app_with_db()
        self.assertEqual(
            thread.call_count,
            0,
            "no background threads may start when auto-sync is disabled",
        )

    def test_enabled_starts_sync_threads(self):
        """With auto-sync on, construction starts the sync machinery once."""
        import craft_unified

        os.environ["CRAFT_AUTO_SYNC"] = "1"
        with patch("threading.Thread") as thread:
            craft_unified.create_app_with_db()
        self.assertGreater(
            thread.call_count,
            0,
            "auto-sync enabled must start the background sync",
        )

    def test_explicit_argument_overrides_env(self):
        import craft_unified

        os.environ["CRAFT_AUTO_SYNC"] = "1"
        with patch("threading.Thread") as thread:
            craft_unified.create_app_with_db(auto_sync=False)
        self.assertEqual(thread.call_count, 0)


if __name__ == "__main__":
    unittest.main()


class TestProductionEntryImport(unittest.TestCase):
    def test_v2_import_does_not_construct_database_or_start_threads(self):
        import subprocess
        result = subprocess.run([sys.executable, '-c', '''
from unittest.mock import patch
import craft_unified
with patch('sqlite3.connect') as database, patch('threading.Thread') as thread:
    import craft_v2
    assert callable(craft_v2.create_app_v2)
    database.assert_not_called()
    thread.assert_not_called()
'''], capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
