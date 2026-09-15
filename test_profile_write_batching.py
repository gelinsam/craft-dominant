"""Profile phases must commit once, not once per row — and roll back whole.

Building profiles used to issue one commit per row: 63,675 for the global
phase and 64,997 for the event-scoped phase, ~128,672 for a rebuild. Each
commit fsyncs the WAL, and on production's network-backed volume that was
about 90% of the phase's runtime.

Two things are asserted here, and they are different claims:

  * output equivalence — batching must not change a single persisted value
  * commit shape — O(1) commits per phase instead of O(rows)

and a third that is a deliberate *change* rather than a preservation: a
failure partway through a phase now discards that phase entirely. The old
behaviour left the table holding a mixture of freshly written rows and stale
ones, with nothing recording which was which.
"""

import contextlib
import os
import random
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta

os.environ.setdefault("TESTING", "1")
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("CRAFT_AUTO_SYNC", "0")

from craft_unified import Database, EventbriteSync  # noqa: E402

_VOLATILE = {"updated_at"}


class _CommitCountingConnection:
    """Delegating wrapper that counts commits and rollbacks."""

    def __init__(self, conn):
        self._conn = conn
        self.commits = 0
        self.rollbacks = 0

    def commit(self):
        self.commits += 1
        return self._conn.commit()

    def rollback(self):
        self.rollbacks += 1
        return self._conn.rollback()

    def __getattr__(self, name):
        return getattr(self._conn, name)


@contextlib.contextmanager
def _per_row_commits(db):
    """Restore the pre-batching write pattern, for equivalence comparison.

    deferred_commit() becomes a no-op that never sets the flag, so every
    writer commits its own row exactly as before. The computation code is
    untouched, so any difference in output is attributable to batching alone.
    """
    original = type(db).deferred_commit

    @contextlib.contextmanager
    def noop(self):
        yield self

    type(db).deferred_commit = noop
    try:
        yield
    finally:
        type(db).deferred_commit = original


def _rows(db, table):
    cols = [r[1] for r in db.conn.execute(f"PRAGMA table_info({table})").fetchall()]
    keep = [c for c in cols if c not in _VOLATILE]
    order = "email" if "email" in cols else cols[0]
    return [tuple(r) for r in db.conn.execute(
        f"SELECT {', '.join(keep)} FROM {table} ORDER BY {order}"
    ).fetchall()]


def _seed(db, customers=400, seed=5):
    """Deterministic fixture with varied order counts and several scopes."""
    rng = random.Random(seed)
    base = datetime(2026, 1, 1)
    types = ["coffee", "beer", "wine"]
    cities = ["Philadelphia", "Austin", "Seattle"]
    events = []
    for i in range(12):
        eid = f"evt_{i}"
        events.append(eid)
        db.conn.execute(
            "INSERT OR REPLACE INTO events (event_id,name,event_type,city,event_date,capacity,status)"
            " VALUES (?,?,?,?,?,?, 'upcoming')",
            (eid, f"Festival {i}", types[i % 3], cities[i % 3],
             (base + timedelta(days=30 * i)).date().isoformat(), 5000),
        )
    for c in range(customers):
        for o in range((c % 4) + 1):
            db.conn.execute(
                "INSERT OR REPLACE INTO orders (order_id,event_id,email,order_timestamp,"
                "ticket_count,gross_amount,net_amount,days_before_event) VALUES (?,?,?,?,?,?,?,?)",
                (f"ord_{c}_{o}", events[(c + o) % len(events)], f"customer{c}@example.com",
                 (base - timedelta(days=rng.randint(1, 900))).isoformat(),
                 rng.randint(1, 4), round(rng.uniform(15, 250), 2),
                 round(rng.uniform(10, 200), 2), rng.randint(0, 90)),
            )
    db.conn.commit()


def _fresh(customers=400):
    tmp = tempfile.TemporaryDirectory()
    db = Database(os.path.join(tmp.name, "t.db"))
    _seed(db, customers)
    return db, tmp


class TestOutputIsUnchanged(unittest.TestCase):
    """Batching is a write-pattern change, not a data change."""

    def _build(self, batched):
        db, tmp = _fresh()
        sync = EventbriteSync("k", db)
        if batched:
            sync._build_all_customers()
        else:
            with _per_row_commits(db):
                sync._build_all_customers()
        out = (_rows(db, "customers"), _rows(db, "customer_event_profiles"))
        db.conn.close()
        tmp.cleanup()
        return out

    def test_customer_rows_identical(self):
        old_c, _ = self._build(batched=False)
        new_c, _ = self._build(batched=True)
        self.assertGreater(len(new_c), 0, "fixture produced no customers")
        self.assertEqual(new_c, old_c)

    def test_event_profile_rows_identical(self):
        _, old_p = self._build(batched=False)
        _, new_p = self._build(batched=True)
        self.assertGreater(len(new_p), 0, "fixture produced no event profiles")
        self.assertEqual(new_p, old_p)

    def test_counts_identical(self):
        old_c, old_p = self._build(batched=False)
        new_c, new_p = self._build(batched=True)
        self.assertEqual((len(new_c), len(new_p)), (len(old_c), len(old_p)))

    def test_quintiles_identical_and_varied(self):
        """Equal-but-degenerate would prove nothing, so check both."""
        db, tmp = _fresh()
        EventbriteSync("k", db)._build_all_customers()
        for column in ("rfm_r", "rfm_f", "rfm_m"):
            values = {r[0] for r in db.conn.execute(
                f"SELECT DISTINCT {column} FROM customers").fetchall()}
            self.assertGreater(len(values), 1, f"{column} is constant")
        db.conn.close()
        tmp.cleanup()

    def test_cross_event_affinity_identical_and_nonzero(self):
        def affinities(batched):
            db, tmp = _fresh()
            sync = EventbriteSync("k", db)
            if batched:
                sync._build_all_customers()
            else:
                with _per_row_commits(db):
                    sync._build_all_customers()
            vals = [r[0] for r in db.conn.execute(
                "SELECT cross_event_affinity FROM customer_event_profiles ORDER BY email"
            ).fetchall()]
            db.conn.close()
            tmp.cleanup()
            return vals

        old, new = affinities(False), affinities(True)
        self.assertEqual(new, old)
        self.assertTrue(any(v for v in new), "all affinities zero — test is vacuous")


class TestCommitShape(unittest.TestCase):
    """O(1) commits per phase, not O(rows)."""

    def _commits(self, customers):
        db, tmp = _fresh(customers)
        db.conn = _CommitCountingConnection(db.conn)
        db.conn.commits = 0
        EventbriteSync("k", db)._build_all_customers()
        n_cust = db.conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        n_prof = db.conn.execute("SELECT COUNT(*) FROM customer_event_profiles").fetchone()[0]
        commits = db.conn.commits
        db.conn.close()
        tmp.cleanup()
        return commits, n_cust + n_prof

    def test_commit_count_does_not_grow_with_rows(self):
        small_commits, small_rows = self._commits(200)
        large_commits, large_rows = self._commits(1000)
        self.assertGreater(large_rows, small_rows * 3, "fixture did not actually scale")
        self.assertEqual(
            small_commits, large_commits,
            f"commits moved with row count: {small_commits} -> {large_commits} "
            f"while rows went {small_rows} -> {large_rows}",
        )

    def test_commit_count_is_a_small_constant(self):
        commits, rows = self._commits(1000)
        self.assertLess(
            commits, 10,
            f"{commits} commits for {rows} rows — expected one per phase",
        )

    def test_pre_batching_pattern_did_scale(self):
        """Proves the counter measures something real."""
        counts = []
        for customers in (200, 1000):
            db, tmp = _fresh(customers)
            db.conn = _CommitCountingConnection(db.conn)
            db.conn.commits = 0
            with _per_row_commits(db):
                EventbriteSync("k", db)._build_all_customers()
            counts.append(db.conn.commits)
            db.conn.close()
            tmp.cleanup()
        self.assertGreater(counts[1], counts[0] * 3,
                           "per-row pattern should commit roughly once per row")


class TestPhaseRollback(unittest.TestCase):
    """A failure partway through a phase discards that phase entirely.

    This is the deliberate behaviour change. Previously the table was left
    holding some new rows and some stale ones with no record of the split.
    """

    def _prime(self, db):
        """Build once so there is a complete prior state to roll back to."""
        EventbriteSync("k", db)._build_all_customers()
        return _rows(db, "customers"), _rows(db, "customer_event_profiles")

    def test_failure_mid_customer_phase_rolls_back_the_phase(self):
        db, tmp = _fresh()
        before_c, before_p = self._prime(db)
        self.assertGreater(len(before_c), 50)

        sync = EventbriteSync("k", db)
        real = sync._build_customer_profile
        calls = {"n": 0}

        def explode(*a, **kw):
            calls["n"] += 1
            if calls["n"] == 25:
                raise RuntimeError("injected failure mid customer phase")
            return real(*a, **kw)

        sync._build_customer_profile = explode
        with self.assertRaises(RuntimeError):
            sync._build_all_customers()

        self.assertEqual(_rows(db, "customers"), before_c,
                         "customer phase was not rolled back")
        db.conn.close()
        tmp.cleanup()

    def test_failure_mid_event_profile_phase_rolls_back_the_phase(self):
        db, tmp = _fresh()
        before_c, before_p = self._prime(db)
        self.assertGreater(len(before_p), 50)

        sync = EventbriteSync("k", db)
        real = db.upsert_event_profile
        calls = {"n": 0}

        def explode(profile):
            calls["n"] += 1
            if calls["n"] == 25:
                raise RuntimeError("injected failure mid event-profile phase")
            return real(profile)

        db.upsert_event_profile = explode
        with self.assertRaises(RuntimeError):
            sync._build_event_profiles()

        self.assertEqual(_rows(db, "customer_event_profiles"), before_p,
                         "event-profile phase was not rolled back")
        db.conn.close()
        tmp.cleanup()

    def test_database_is_usable_after_rollback(self):
        db, tmp = _fresh()
        self._prime(db)
        sync = EventbriteSync("k", db)
        real = db.upsert_event_profile
        calls = {"n": 0}

        def explode(profile):
            calls["n"] += 1
            if calls["n"] == 10:
                raise RuntimeError("boom")
            return real(profile)

        db.upsert_event_profile = explode
        with self.assertRaises(RuntimeError):
            sync._build_event_profiles()

        db.upsert_event_profile = real
        # reads still work
        self.assertGreater(db.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)
        # writes still work, and commit normally
        db.conn.execute(
            "INSERT OR REPLACE INTO events (event_id,name,event_type,city,event_date,capacity,status)"
            " VALUES ('after_rollback','After','coffee','Austin','2026-12-01',10,'upcoming')")
        db.conn.commit()
        self.assertEqual(db.conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_id='after_rollback'").fetchone()[0], 1)
        # and a clean rebuild afterwards still succeeds
        EventbriteSync("k", db)._build_all_customers()
        self.assertGreater(
            db.conn.execute("SELECT COUNT(*) FROM customer_event_profiles").fetchone()[0], 0)
        db.conn.close()
        tmp.cleanup()

    def test_rollback_was_actually_issued(self):
        db, tmp = _fresh()
        self._prime(db)
        db.conn = _CommitCountingConnection(db.conn)
        db.conn.rollbacks = 0
        sync = EventbriteSync("k", db)
        real = db.upsert_event_profile
        calls = {"n": 0}

        def explode(profile):
            calls["n"] += 1
            if calls["n"] == 5:
                raise RuntimeError("boom")
            return real(profile)

        db.upsert_event_profile = explode
        with self.assertRaises(RuntimeError):
            sync._build_event_profiles()
        self.assertGreaterEqual(db.conn.rollbacks, 1)
        db.conn.close()
        tmp.cleanup()


class TestDeferredCommitContract(unittest.TestCase):
    """Behaviour of the primitive itself."""

    def test_flag_is_cleared_after_success(self):
        db, tmp = _fresh(20)
        with db.deferred_commit():
            pass
        self.assertFalse(db._defer_commits)
        db.conn.close()
        tmp.cleanup()

    def test_flag_is_cleared_after_failure(self):
        db, tmp = _fresh(20)
        with self.assertRaises(ValueError):
            with db.deferred_commit():
                raise ValueError("x")
        self.assertFalse(db._defer_commits,
                         "a failed phase must not leave writes deferred forever")
        db.conn.close()
        tmp.cleanup()

    def test_nesting_does_not_commit_early(self):
        """The outermost block owns the transaction."""
        db, tmp = _fresh(20)
        db.conn = _CommitCountingConnection(db.conn)
        db.conn.commits = 0
        with db.deferred_commit():
            with db.deferred_commit():
                db.conn.execute(
                    "INSERT OR REPLACE INTO events (event_id,name,event_type,city,"
                    "event_date,capacity,status) VALUES ('n1','N','coffee','Austin',"
                    "'2026-12-01',10,'upcoming')")
            self.assertEqual(db.conn.commits, 0, "inner block committed early")
        self.assertEqual(db.conn.commits, 1, "outer block should commit exactly once")
        db.conn.close()
        tmp.cleanup()

    def test_writers_outside_the_block_still_commit_themselves(self):
        """Normal single-row writes elsewhere are unaffected."""
        db, tmp = _fresh(20)
        db.conn = _CommitCountingConnection(db.conn)
        db.conn.commits = 0
        db.upsert_event({"event_id": "solo", "name": "Solo", "event_date": "2026-12-01",
                         "capacity": 10, "status": "upcoming"})
        self.assertEqual(db.conn.commits, 1)
        db.conn.close()
        tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
