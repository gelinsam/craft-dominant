import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from campaign_worker import readonly_database,write_private

class WorkerTests(unittest.TestCase):
    def test_real_database_reader_cannot_write_or_create_database(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'analytics.sqlite'
            conn=sqlite3.connect(path)
            conn.execute('CREATE TABLE events (event_id TEXT PRIMARY KEY, name TEXT)')
            conn.execute("INSERT INTO events VALUES ('dc','DC Coffee Festival')")
            conn.commit(); conn.close()
            reader=readonly_database(path)
            try:
                self.assertEqual(reader.get_event('dc')['name'],'DC Coffee Festival')
                with self.assertRaises(sqlite3.OperationalError):
                    reader.conn.execute("DELETE FROM events")
            finally: reader.close()
            with self.assertRaises(sqlite3.OperationalError): readonly_database(Path(root)/'missing.sqlite')
            self.assertFalse((Path(root)/'missing.sqlite').exists())

    def test_private_package_never_overwrites_previous_result(self):
        with tempfile.TemporaryDirectory() as root:
            path=Path(root)/'package.json'
            write_private(path,{'state':'draft'})
            self.assertEqual(path.stat().st_mode & 0o777,0o600)
            with self.assertRaises(FileExistsError): write_private(path,{'state':'changed'})
            self.assertEqual(json.loads(path.read_text())['state'],'draft')

if __name__=='__main__': unittest.main()
