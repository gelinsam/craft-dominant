"""Concurrency regressions for batched profile writes.

The profile phases commit once instead of once per row. A SQLite connection has
exactly one transaction, and this app deliberately shares one connection across
threads (``check_same_thread=False``) while running the Eventbrite sync in a
background thread. So "one transaction per phase" is only true if no other
thread can write through that connection while the phase is open.

Two concrete failures were reproduced against the unguarded implementation and
are pinned here:

  A. a request thread's unrelated write joined the phase transaction through
     ``_maybe_commit()`` and was destroyed when the phase rolled back;
  B. a request thread's plain ``conn.commit()`` committed the half-built phase,
     defeating the atomic rollback the batching exists to provide.

Failure mode B is not hypothetical: ``craft_engine``, ``campaign_adapter``,
``intervention_model``, ``suppression_guard`` and the SQLite branch of
``v2_state_repository`` all write with a bare ``self.db.conn.execute(...)``
followed by ``self.db.conn.commit()`` on the *same* Database instance that
``_build_app()`` hands to the sync.

Every test here that asserts safety has a paired "teeth" test proving it fails
when the guard is removed.
"""

import os
import sqlite3
import sys
import tempfile
import threading
import time
import types
import unittest

os.environ.setdefault("CRAFT_AUTO_SYNC", "0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from craft_unified import Database, WriteLockTimeout  # noqa: E402


TIMEOUT = 10  # generous; a correct run never approaches it


class _Base(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        self.db = Database(os.path.join(self.dir, "concurrency.db"))
        self.db.conn.execute("CREATE TABLE IF NOT EXISTS unrelated (k TEXT PRIMARY KEY)")
        self.db.conn.commit()
        self.errors = []

    def tearDown(self):
        try:
            self.db.conn.close()
        except Exception:
            pass

    # -- helpers -----------------------------------------------------------
    def unguard(self):
        """Point Database.conn back at the bare sqlite3 connection.

        Reproduces the pre-fix behaviour so a test can prove it has teeth.
        begin_write/end_write become no-ops, which is exactly what the
        unguarded implementation did: nothing. ``_defer_state`` is also
        downgraded from thread-local to a plain shared object, restoring the
        shared ``_defer_commits`` boolean the fix replaced. Both halves have to
        go for the original races to reproduce.
        """
        self.db._defer_state = types.SimpleNamespace()
        raw = self.db.conn._conn

        class _Unguarded:
            def __init__(self, c):
                object.__setattr__(self, "_c", c)

            def begin_write(self):
                pass

            def end_write(self):
                pass

            def __getattr__(self, n):
                return getattr(self._c, n)

        self.db.conn = _Unguarded(raw)

    def spawn(self, fn):
        def wrapper():
            try:
                fn()
            except BaseException as exc:  # noqa: BLE001 - reported, not swallowed
                self.errors.append(exc)

        t = threading.Thread(target=wrapper, daemon=True)
        t.start()
        return t

    def join(self, *threads):
        for t in threads:
            t.join(TIMEOUT)
            self.assertFalse(t.is_alive(), "thread did not finish — probable deadlock")

    def count(self, table, where="1=1"):
        return self.db.conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}").fetchone()[0]

    def phase_row(self, email):
        self.db.conn.execute(
            "INSERT OR REPLACE INTO customers (email, total_orders) VALUES (?, 1)", (email,)
        )


# ---------------------------------------------------------------------------
# Failure mode A — an unrelated writer must not join the phase transaction
# ---------------------------------------------------------------------------
class TestUnrelatedWriterCannotJoinPhase(_Base):
    def _run(self):
        """Other thread writes via transaction()/_maybe_commit(); phase then fails."""
        in_phase = threading.Event()
        other_done = threading.Event()

        def other():
            in_phase.wait(TIMEOUT)
            with self.db.transaction() as conn:
                conn.execute("INSERT INTO unrelated (k) VALUES ('joined')")
            other_done.set()

        t = self.spawn(other)
        with self.assertRaises(RuntimeError):
            with self.db.deferred_commit():
                self.phase_row("phase@x")
                in_phase.set()
                other_done.wait(1.0)  # guarded: stays blocked; unguarded: proceeds
                raise RuntimeError("phase fails partway")
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")
        return self.count("unrelated", "k='joined'")

    def test_unrelated_write_survives_phase_rollback(self):
        self.assertEqual(
            self._run(), 1,
            "an unrelated write was swallowed by the profile phase rollback",
        )

    def test_teeth_unguarded_connection_loses_the_unrelated_write(self):
        self.unguard()
        self.assertEqual(
            self._run(), 0,
            "expected the unguarded connection to reproduce failure mode A",
        )

    def test_unrelated_writer_is_not_committed_by_a_successful_phase(self):
        """A phase that succeeds must commit its own rows and nothing else."""
        in_phase = threading.Event()
        released = threading.Event()
        wrote = threading.Event()

        def other():
            in_phase.wait(TIMEOUT)
            # Blocks here until the phase commits and releases the write lock.
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('later')")
            wrote.set()
            released.wait(TIMEOUT)
            self.db.conn.commit()

        t = self.spawn(other)
        with self.db.deferred_commit():
            self.phase_row("ok@x")
            in_phase.set()
            # The other thread must NOT have written by the time we commit.
            self.assertFalse(wrote.wait(0.5), "writer entered the phase transaction")
        # Phase committed. Its row is durable; the other thread's is not yet.
        self.assertEqual(self.count("customers", "email='ok@x'"), 1)
        released.set()
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")


# ---------------------------------------------------------------------------
# Failure mode B — an unrelated commit must not commit the phase
# ---------------------------------------------------------------------------
class TestUnrelatedWriterCannotCommitPhase(_Base):
    def _run(self):
        """Other thread does a bare execute+commit while the phase is open."""
        in_phase = threading.Event()
        other_done = threading.Event()

        def other():
            in_phase.wait(TIMEOUT)
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('committer')")
            self.db.conn.commit()
            other_done.set()

        t = self.spawn(other)
        with self.assertRaises(RuntimeError):
            with self.db.deferred_commit():
                self.phase_row("leak@x")
                in_phase.set()
                other_done.wait(1.0)
                raise RuntimeError("phase fails partway")
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")
        return self.count("customers", "email='leak@x'")

    def test_phase_rollback_is_atomic_despite_foreign_commit(self):
        self.assertEqual(
            self._run(), 0,
            "a foreign conn.commit() prematurely committed the profile phase",
        )

    def test_teeth_unguarded_connection_leaks_the_partial_phase(self):
        self.unguard()
        self.assertEqual(
            self._run(), 1,
            "expected the unguarded connection to reproduce failure mode B",
        )

    def test_foreign_writer_completes_after_the_phase_rolls_back(self):
        """Serialization must not lose the other thread's work — only delay it."""
        in_phase = threading.Event()

        def other():
            in_phase.wait(TIMEOUT)
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('deferred-but-kept')")
            self.db.conn.commit()

        t = self.spawn(other)
        with self.assertRaises(RuntimeError):
            with self.db.deferred_commit():
                self.phase_row("gone@x")
                in_phase.set()
                time.sleep(0.2)
                raise RuntimeError("phase fails partway")
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")
        self.assertEqual(self.count("customers", "email='gone@x'"), 0, "phase should be gone")
        self.assertEqual(
            self.count("unrelated", "k='deferred-but-kept'"), 1,
            "the blocked writer's work must still land once the phase ends",
        )


# ---------------------------------------------------------------------------
# Readers must not be blocked
# ---------------------------------------------------------------------------
class TestReadsProceedDuringPhase(_Base):
    def test_select_from_another_thread_returns_while_phase_is_open(self):
        self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('preexisting')")
        self.db.conn.commit()

        in_phase = threading.Event()
        read_done = threading.Event()
        result = {}

        def reader():
            in_phase.wait(TIMEOUT)
            started = time.perf_counter()
            row = self.db.conn.execute("SELECT COUNT(*) FROM unrelated").fetchone()
            result["count"] = row[0]
            result["elapsed"] = time.perf_counter() - started
            read_done.set()

        t = self.spawn(reader)
        with self.db.deferred_commit():
            for i in range(200):
                self.phase_row(f"r{i}@x")
            in_phase.set()
            self.assertTrue(
                read_done.wait(3.0),
                "a reader blocked on the phase's write lock — WAL readers must proceed",
            )
            # Sees the pre-phase snapshot, not the phase's uncommitted rows.
            self.assertEqual(result["count"], 1)
        self.join(t)
        self.assertFalse(self.errors, f"reader thread raised: {self.errors}")
        self.assertLess(result["elapsed"], 1.0)

    def test_separate_connection_does_not_see_the_uncommitted_phase(self):
        """WAL isolation: another *connection* reads the pre-phase snapshot."""
        in_phase = threading.Event()
        seen = {}

        def reader():
            in_phase.wait(TIMEOUT)
            other = sqlite3.connect(self.db.path, timeout=5)
            try:
                seen["during"] = other.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
            finally:
                other.close()

        t = self.spawn(reader)
        with self.db.deferred_commit():
            self.phase_row("invisible@x")
            in_phase.set()
            self.join(t)
        self.assertFalse(self.errors, f"reader thread raised: {self.errors}")
        self.assertEqual(seen["during"], 0, "uncommitted phase rows leaked to another connection")
        self.assertEqual(self.count("customers"), 1, "phase rows missing after commit")

    def test_shared_connection_readers_do_see_the_in_flight_phase(self):
        """Documents a real consequence of batching, not a guarantee of it.

        Flask request handlers read through the *same* connection the sync
        writes on, and SQLite shows a connection its own uncommitted work. So a
        dashboard read taken mid-phase sees rows that a phase rollback would
        then remove. Per-row commits had the same visibility but never took
        rows away again.

        This is a read-consistency property, not a corruption risk: the
        analytical tables are fully re-derivable and a failed phase is followed
        by a re-run. Pinned here so the behaviour is a decision rather than a
        surprise.
        """
        in_phase = threading.Event()
        seen = {}

        def reader():
            in_phase.wait(TIMEOUT)
            seen["during"] = self.db.conn.execute(
                "SELECT COUNT(*) FROM customers"
            ).fetchone()[0]

        t = self.spawn(reader)
        with self.assertRaises(RuntimeError):
            with self.db.deferred_commit():
                self.phase_row("transient@x")
                in_phase.set()
                self.join(t)
                raise RuntimeError("phase fails partway")
        self.assertFalse(self.errors, f"reader thread raised: {self.errors}")
        self.assertEqual(seen["during"], 1, "expected same-connection read-your-writes")
        self.assertEqual(self.count("customers"), 0, "phase should have rolled back")


# ---------------------------------------------------------------------------
# Nesting and re-entrancy must not deadlock
# ---------------------------------------------------------------------------
class TestNoDeadlock(_Base):
    def test_transaction_nested_inside_deferred_commit(self):
        with self.db.deferred_commit():
            with self.db.transaction() as conn:
                conn.execute("INSERT INTO unrelated (k) VALUES ('nested-txn')")
            self.phase_row("nested@x")
        self.assertEqual(self.count("unrelated", "k='nested-txn'"), 1)
        self.assertEqual(self.count("customers", "email='nested@x'"), 1)

    def test_deferred_commit_nested_inside_deferred_commit(self):
        with self.db.deferred_commit():
            self.phase_row("outer@x")
            with self.db.deferred_commit():
                self.phase_row("inner@x")
            # Inner block must not have committed or released ownership.
            self.assertEqual(
                self.db.conn.execute(
                    "SELECT COUNT(*) FROM customers"
                ).fetchone()[0], 2,
            )
        self.assertEqual(self.count("customers"), 2)

    def test_sequential_phases_do_not_strand_the_lock(self):
        for n in range(3):
            with self.db.deferred_commit():
                self.phase_row(f"seq{n}@x")
        self.assertEqual(self.count("customers"), 3)
        # A fresh thread must still be able to write.
        t = self.spawn(lambda: (
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('after')"),
            self.db.conn.commit(),
        ))
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")
        self.assertEqual(self.count("unrelated", "k='after'"), 1)

    def test_failed_phase_releases_the_lock_for_the_next_writer(self):
        with self.assertRaises(ValueError):
            with self.db.deferred_commit():
                self.phase_row("doomed@x")
                raise ValueError("boom")
        t = self.spawn(lambda: (
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('post-failure')"),
            self.db.conn.commit(),
        ))
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")
        self.assertEqual(self.count("unrelated", "k='post-failure'"), 1)

    def test_two_threads_running_phases_serialize_without_deadlock(self):
        barrier = threading.Barrier(2, timeout=TIMEOUT)

        def phase(tag):
            barrier.wait()
            with self.db.deferred_commit():
                for i in range(50):
                    self.phase_row(f"{tag}-{i}@x")

        a = self.spawn(lambda: phase("a"))
        b = self.spawn(lambda: phase("b"))
        self.join(a, b)
        self.assertFalse(self.errors, f"phase thread raised: {self.errors}")
        self.assertEqual(self.count("customers"), 100)

    def test_a_statement_error_does_not_strand_the_lock(self):
        with self.assertRaises(sqlite3.Error):
            self.db.conn.execute("INSERT INTO table_that_does_not_exist VALUES (1)")
        t = self.spawn(lambda: (
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('after-sql-error')"),
            self.db.conn.commit(),
        ))
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")


# ---------------------------------------------------------------------------
# The guard's own contract
# ---------------------------------------------------------------------------
class TestWriteGuardContract(_Base):
    def test_reads_do_not_take_the_write_lock(self):
        held = []
        self.db.conn.begin_write()
        try:
            def reader():
                self.db.conn.execute("SELECT 1").fetchone()
                held.append("read-ok")

            t = self.spawn(reader)
            self.join(t)
        finally:
            self.db.conn.rollback()
        self.assertEqual(held, ["read-ok"])

    def test_write_waiting_past_the_timeout_raises_rather_than_hanging(self):
        """A leaked uncommitted write must surface loudly, not wedge the worker."""
        self.db.conn._timeout = 0.2
        started = threading.Event()
        done = threading.Event()
        outcome = {}

        def holder():
            self.db.conn.begin_write()
            started.set()
            done.wait(TIMEOUT)
            self.db.conn.rollback()

        t = self.spawn(holder)
        started.wait(TIMEOUT)
        try:
            with self.assertRaises(WriteLockTimeout):
                self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('never')")
            outcome["raised"] = True
        finally:
            done.set()
            self.join(t)
        self.assertTrue(outcome.get("raised"))
        self.assertFalse(self.errors, f"holder thread raised: {self.errors}")

    def test_defer_flag_is_thread_local(self):
        """One thread's phase must not suppress another thread's commit logic."""
        in_phase = threading.Event()
        observed = {}

        def observer():
            in_phase.wait(TIMEOUT)
            observed["other_thread_sees"] = self.db._defer_commits

        t = self.spawn(observer)
        with self.db.deferred_commit():
            self.assertTrue(self.db._defer_commits, "owning thread should see the flag")
            in_phase.set()
            self.join(t)
        self.assertFalse(self.errors, f"observer thread raised: {self.errors}")
        self.assertFalse(
            observed["other_thread_sees"],
            "_defer_commits leaked across threads — a foreign commit would be suppressed",
        )

    def test_row_factory_and_attributes_pass_through(self):
        self.assertIs(self.db.conn.row_factory, sqlite3.Row)
        row = self.db.conn.execute("SELECT 1 AS n").fetchone()
        self.assertEqual(row["n"], 1)

    def test_executemany_and_executescript_release_ownership(self):
        self.db.conn.executemany(
            "INSERT INTO unrelated (k) VALUES (?)", [("m1",), ("m2",)]
        )
        self.db.conn.commit()
        self.db.conn.executescript(
            "CREATE TABLE IF NOT EXISTS scripted (x INTEGER); INSERT INTO scripted VALUES (1);"
        )
        t = self.spawn(lambda: (
            self.db.conn.execute("INSERT INTO unrelated (k) VALUES ('after-bulk')"),
            self.db.conn.commit(),
        ))
        self.join(t)
        self.assertFalse(self.errors, f"writer thread raised: {self.errors}")
        self.assertEqual(self.count("unrelated"), 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
