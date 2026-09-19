"""Exercise the nested milestone job without starting the app or its workers."""
import ast
import json
import logging
import sqlite3
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock


def load_job(db):
    source = ast.parse(Path(__file__).with_name("craft_unified.py").read_text())
    app = next(n for n in source.body if isinstance(n, ast.FunctionDef) and n.name == "create_app")
    job = next(n for n in app.body if isinstance(n, ast.FunctionDef) and n.name == "_check_milestones_and_export")
    namespace = dict(db=db, date=date, datetime=datetime, json=json, log=logging.getLogger(__name__))
    exec(compile(ast.Module(body=[job], type_ignores=[]), "craft_unified.py", "exec"), namespace)
    return namespace[job.name]


class MilestoneExportTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute("""CREATE TABLE auto_exports (
            id INTEGER PRIMARY KEY, event_id TEXT, milestone TEXT, export_type TEXT,
            audience_count INTEGER, audience_emails TEXT, created_at TEXT)""")

    def tearDown(self):
        self.conn.close()

    def database(self, days):
        event = dict(event_id="city-coffee", name="City Coffee", city="City", event_type="coffee",
                     event_date=(date.today() + timedelta(days=days)).isoformat())
        return SimpleNamespace(conn=self.conn, get_events=Mock(return_value=[event]),
            get_event_profiles=Mock(), get_event_purchasers=Mock(return_value=["buyer@example.com"]),
            get_past_attendees_not_purchased=Mock(return_value=[{"email": "past@example.com"}]))

    def test_overlapping_dictionary_profiles_export_once_and_retry_is_idempotent(self):
        for days in (45, 14):
            with self.subTest(days=days):
                self.conn.execute("DELETE FROM auto_exports")
                db = self.database(days)
                db.get_event_profiles.side_effect = [
                    [{"email": " First@example.com ", "score": 1}, {"email": "repeat@example.com"}],
                    [{"email": "REPEAT@example.com", "score": 8}, {"email": "last@example.com"},
                     {"email": None}, {}, {"email": " "}, {"email": 42}]]
                run = load_job(db)
                run()
                row = self.conn.execute("SELECT audience_count, audience_emails FROM auto_exports").fetchone()
                self.assertIsNotNone(row, "The job must persist an export, not swallow a dict hashing error")
                self.assertEqual(row[0], 3)
                self.assertEqual(json.loads(row[1]), ["first@example.com", "repeat@example.com", "last@example.com"])
                run()
                self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM auto_exports").fetchone()[0], 1)
                self.assertEqual(db.get_event_profiles.call_count, 2)
                for call in db.get_event_profiles.call_args_list:
                    self.assertEqual(call.args, ("coffee", "City"))

    def test_thirty_day_export_preserves_current_buyer_exclusions(self):
        db = self.database(30)
        load_job(db)()
        self.assertEqual(db.get_past_attendees_not_purchased.call_args.kwargs["current_buyer_emails"],
                         {"buyer@example.com"})
        self.assertEqual(self.conn.execute("SELECT audience_count FROM auto_exports").fetchone()[0], 1)
        db.get_event_profiles.assert_not_called()

    def test_empty_and_single_segment_audiences(self):
        for days, profiles, expected in [(60, [], []), (7, [{"email": "a@example.com"}, {"email": "a@example.com"}], ["a@example.com"])]:
            with self.subTest(days=days):
                self.conn.execute("DELETE FROM auto_exports")
                db = self.database(days)
                db.get_event_profiles.return_value = profiles
                load_job(db)()
                row = self.conn.execute("SELECT audience_count, audience_emails FROM auto_exports").fetchone()
                self.assertEqual(row[0], len(expected))
                self.assertEqual(json.loads(row[1]), expected)


if __name__ == "__main__":
    unittest.main()
