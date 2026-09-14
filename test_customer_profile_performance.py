"""Customer profile building must not scale with customer count.

Production symptom this covers: `_build_all_customers()` issued one query per
distinct email and did three linear scans per customer over lists as long as
the whole customer base. On the real dataset that phase ran past gunicorn's
120s worker timeout, so the worker was killed partway through, gunicorn booted
a replacement, and the replacement started the entire Eventbrite sync again.
The rebuild looped forever and `customers` stayed empty.

The fix is a performance change only, so the first and most important test here
is equivalence: the pre-fix algorithm is reimplemented as an oracle and every
persisted column is compared against the current implementation.
"""

import os
import random
import sqlite3
import time
import unittest
from datetime import datetime, timedelta

os.environ.setdefault("TESTING", "1")
os.environ.setdefault("DB_PATH", ":memory:")
os.environ.setdefault("CRAFT_AUTO_SYNC", "0")

from craft_unified import Database, EventbriteSync  # noqa: E402


# Columns whose value is a wall-clock stamp rather than derived data.
_VOLATILE_COLUMNS = {"updated_at"}


def _persisted(db, table):
    """Every row of a table, volatile columns dropped, in a stable order."""
    cols = [r[1] for r in db.conn.execute(f"PRAGMA table_info({table})").fetchall()]
    keep = [c for c in cols if c not in _VOLATILE_COLUMNS]
    order = "email" if "email" in cols else cols[0]
    rows = db.conn.execute(
        f"SELECT {', '.join(keep)} FROM {table} ORDER BY {order}"
    ).fetchall()
    return [tuple(r) for r in rows]


class _CountingConnection:
    """Wraps a sqlite3 connection and counts reads separately from writes.

    Writes necessarily scale with customer count — you cannot persist N
    profiles in fewer than N statements. Reads are the thing under test: the
    number of SELECTs must not grow with the customer base.
    """

    def __init__(self, conn):
        self._conn = conn
        self.read_count = 0
        self.write_count = 0

    def execute(self, sql, *args, **kwargs):
        if sql.lstrip().upper().startswith("SELECT"):
            self.read_count += 1
        else:
            self.write_count += 1
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _seed(db, customers, orders_per_customer=None, seed=7):
    """Build a deterministic dataset of events, orders and customers.

    orders_per_customer=None varies the count per customer (1..4) so the
    frequency quintile is not constant — a fixture where every customer looks
    identical would make the equivalence comparison prove nothing.
    """
    rng = random.Random(seed)
    base = datetime(2026, 1, 1)
    event_types = ["coffee", "beer", "wine"]
    cities = ["Philadelphia", "Austin", "Seattle"]

    events = []
    for i in range(9):
        event_id = f"evt_{i}"
        events.append(event_id)
        db.conn.execute(
            "INSERT OR REPLACE INTO events "
            "(event_id, name, event_type, city, event_date, capacity, status) "
            "VALUES (?, ?, ?, ?, ?, ?, 'upcoming')",
            (
                event_id,
                f"Festival {i}",
                event_types[i % len(event_types)],
                cities[i % len(cities)],
                (base + timedelta(days=30 * i)).date().isoformat(),
                5000,
            ),
        )

    for c in range(customers):
        email = f"customer{c}@example.com"
        n_orders = orders_per_customer if orders_per_customer else (c % 4) + 1
        for o in range(n_orders):
            db.conn.execute(
                "INSERT OR REPLACE INTO orders "
                "(order_id, event_id, email, order_timestamp, ticket_count, "
                " gross_amount, net_amount, days_before_event) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"ord_{c}_{o}",
                    events[(c + o) % len(events)],
                    email,
                    (base - timedelta(days=rng.randint(1, 900))).isoformat(),
                    rng.randint(1, 4),
                    round(rng.uniform(15, 250), 2),
                    round(rng.uniform(10, 200), 2),
                    rng.randint(0, 90),
                ),
            )
    db.conn.commit()


def _build_all_customers_pre_fix(sync) -> int:
    """The implementation as it stood before this change, used as an oracle.

    One query per email, and `list.index()` for the quintile position. Kept
    verbatim in behaviour so any divergence in the new code shows up as a
    failing comparison rather than a plausible-looking number.
    """
    emails = sync.db.get_all_emails()
    count = 0
    all_customers_data = []
    for email in emails:
        orders = sync.db.get_orders_for_customer(email)
        if orders:
            total_spent = sum(o.get("gross_amount", 0) for o in orders)
            last_date = max(o["order_timestamp"] for o in orders)
            try:
                days_since = (datetime.now() - datetime.fromisoformat(last_date)).days
            except Exception:
                days_since = 999
            all_customers_data.append({
                "email": email,
                "orders": orders,
                "total_spent": total_spent,
                "days_since": days_since,
                "order_count": len(orders),
            })

    if all_customers_data:
        recency_values = sorted([c["days_since"] for c in all_customers_data])
        frequency_values = sorted([c["order_count"] for c in all_customers_data])
        monetary_values = sorted([c["total_spent"] for c in all_customers_data])

        def get_quintile(value, sorted_list, reverse=False):
            n = len(sorted_list)
            if n == 0:
                return 3
            idx = sorted_list.index(value) if value in sorted_list else 0
            pct = idx / n
            if reverse:
                pct = 1 - pct
            if pct >= 0.8:
                return 5
            elif pct >= 0.6:
                return 4
            elif pct >= 0.4:
                return 3
            elif pct >= 0.2:
                return 2
            return 1

    for c_data in all_customers_data:
        customer = sync._build_customer_profile(
            c_data["email"], c_data["orders"],
            get_quintile(c_data["days_since"], recency_values, reverse=True),
            get_quintile(c_data["order_count"], frequency_values),
            get_quintile(c_data["total_spent"], monetary_values),
        )
        if customer:
            sync.db.upsert_customer(customer)
            count += 1

    sync._build_event_profiles()
    return count


class TestOutputEquivalence(unittest.TestCase):
    """The fast path must produce byte-for-byte the same profiles."""

    def _run_both(self, customers, orders_per_customer=None):
        old_db = Database(":memory:")
        _seed(old_db, customers, orders_per_customer)
        old_count = _build_all_customers_pre_fix(EventbriteSync("k", old_db))

        new_db = Database(":memory:")
        _seed(new_db, customers, orders_per_customer)
        new_count = EventbriteSync("k", new_db)._build_all_customers()

        return (old_db, old_count), (new_db, new_count)

    def test_customer_rows_identical(self):
        (old_db, old_count), (new_db, new_count) = self._run_both(120)
        self.assertEqual(new_count, old_count)
        self.assertGreater(new_count, 0, "fixture produced no customers")
        self.assertEqual(_persisted(new_db, "customers"), _persisted(old_db, "customers"))

    def test_event_scoped_profiles_identical(self):
        old, new = self._run_both(120)
        self.assertEqual(
            _persisted(new[0], "customer_event_profiles"),
            _persisted(old[0], "customer_event_profiles"),
        )

    def test_identical_with_many_tied_values(self):
        """Ties are where a quintile rewrite is most likely to drift.

        One order each, so recency/frequency/monetary values collide heavily
        and the "first occurrence" semantics of list.index() actually matter.
        """
        (old_db, _), (new_db, _) = self._run_both(200, orders_per_customer=1)
        self.assertEqual(_persisted(new_db, "customers"), _persisted(old_db, "customers"))

    def test_rfm_quintiles_are_not_all_the_same(self):
        """Guards the comparison itself — equal-but-degenerate proves nothing."""
        _, (new_db, _) = self._run_both(120)
        for column in ("rfm_r", "rfm_f", "rfm_m"):
            values = {
                r[0] for r in new_db.conn.execute(
                    f"SELECT DISTINCT {column} FROM customers"
                ).fetchall()
            }
            self.assertGreater(
                len(values), 1, f"{column} is constant — equivalence test is vacuous"
            )

    def test_empty_database_builds_nothing(self):
        db = Database(":memory:")
        self.assertEqual(EventbriteSync("k", db)._build_all_customers(), 0)
        self.assertEqual(_persisted(db, "customers"), [])

    def test_identical_with_a_single_customer(self):
        """Degenerate quintile input: every sorted list has one element."""
        (old_db, _), (new_db, _) = self._run_both(1)
        self.assertEqual(_persisted(new_db, "customers"), _persisted(old_db, "customers"))

    def test_cross_event_affinity_is_populated_and_identical(self):
        """This field was computed by two queries per profile; now precomputed."""
        (old_db, _), (new_db, _) = self._run_both(120)
        new_values = [
            r[0] for r in new_db.conn.execute(
                "SELECT cross_event_affinity FROM customer_event_profiles ORDER BY email"
            ).fetchall()
        ]
        old_values = [
            r[0] for r in old_db.conn.execute(
                "SELECT cross_event_affinity FROM customer_event_profiles ORDER BY email"
            ).fetchall()
        ]
        self.assertEqual(new_values, old_values)
        self.assertTrue(
            any(v for v in new_values),
            "every affinity is zero — the comparison is vacuous",
        )


class TestBulkQueryContract(unittest.TestCase):
    """get_orders_grouped_by_customer must match the per-customer query."""

    def setUp(self):
        self.db = Database(":memory:")
        _seed(self.db, 60)

    def test_matches_get_orders_for_customer_for_every_email(self):
        grouped = self.db.get_orders_grouped_by_customer()
        for email in self.db.get_all_emails():
            with self.subTest(email=email):
                self.assertEqual(
                    grouped.get(email.lower().strip(), []),
                    self.db.get_orders_for_customer(email),
                )

    def test_orders_are_newest_first(self):
        for orders in self.db.get_orders_grouped_by_customer().values():
            stamps = [o["order_timestamp"] for o in orders]
            self.assertEqual(stamps, sorted(stamps, reverse=True))

    def test_key_is_normalized_like_the_lookup(self):
        """A legacy row with a non-normalized address resolves the same way.

        insert_order() lowercases and strips, but rows written before that did
        not, and the per-customer lookup normalizes its argument. The bulk key
        has to normalize identically or those customers would silently change.
        """
        self.db.conn.execute(
            "INSERT INTO orders (order_id, event_id, email, order_timestamp, "
            "ticket_count, gross_amount, net_amount, days_before_event) "
            "VALUES ('legacy', 'evt_0', '  MiXeD@Example.COM ', "
            "'2026-01-02T00:00:00', 2, 99.0, 80.0, 5)"
        )
        self.db.conn.commit()
        grouped = self.db.get_orders_grouped_by_customer()
        self.assertIn("mixed@example.com", grouped)
        self.assertEqual(len(grouped["mixed@example.com"]), 1)


class TestQueryCountDoesNotScale(unittest.TestCase):
    """The whole point: work per customer must not mean a query per customer."""

    def _count(self, customers, builder=None):
        db = Database(":memory:")
        _seed(db, customers)
        db.conn = _CountingConnection(db.conn)
        sync = EventbriteSync("k", db)
        (builder or (lambda s: s._build_all_customers()))(sync)
        return db.conn.read_count, db.conn.write_count

    def test_read_count_is_flat_as_customers_grow(self):
        small, _ = self._count(50)
        large, _ = self._count(500)
        self.assertEqual(
            small, large,
            f"SELECT count moved with customer count: {small} -> {large}",
        )

    def test_read_count_is_small_in_absolute_terms(self):
        """A handful of statements, not one or three per customer."""
        reads, _ = self._count(500)
        self.assertLess(reads, 20, f"{reads} SELECTs for 500 customers")

    def test_writes_still_scale_because_they_must(self):
        """Guards against a false pass from counting nothing at all."""
        _, small = self._count(50)
        _, large = self._count(500)
        self.assertGreater(large, small)

    def test_pre_fix_implementation_did_scale(self):
        """Confirms the counter is measuring the right thing.

        If the pre-fix oracle ever stops showing per-customer growth, the
        instrumentation is broken and the assertions above prove nothing.
        """
        small, _ = self._count(50, _build_all_customers_pre_fix)
        large, _ = self._count(500, _build_all_customers_pre_fix)
        self.assertGreater(
            large, small * 5,
            "the pre-fix oracle should issue roughly one query per customer",
        )


class TestCompletesWellInsideWorkerTimeout(unittest.TestCase):
    """Gunicorn kills the worker at 120s; this phase must finish long before."""

    GUNICORN_TIMEOUT_SECONDS = 120
    CUSTOMERS = 4000

    def test_build_finishes_with_large_headroom(self):
        db = Database(":memory:")
        _seed(db, self.CUSTOMERS)
        sync = EventbriteSync("k", db)

        started = time.monotonic()
        count = sync._build_all_customers()
        elapsed = time.monotonic() - started

        self.assertEqual(count, self.CUSTOMERS)
        # A tenth of the budget on a CI runner, for a customer base far larger
        # than production's. Loose enough not to flake, tight enough that a
        # return to per-customer work would blow straight through it.
        self.assertLess(
            elapsed, self.GUNICORN_TIMEOUT_SECONDS / 10,
            f"took {elapsed:.1f}s for {self.CUSTOMERS} customers; the worker "
            f"timeout is {self.GUNICORN_TIMEOUT_SECONDS}s",
        )

    def test_scales_close_to_linearly(self):
        """Quadratic growth is what actually broke production, so measure it."""

        def timed(customers):
            db = Database(":memory:")
            _seed(db, customers)
            sync = EventbriteSync("k", db)
            started = time.monotonic()
            sync._build_all_customers()
            return time.monotonic() - started

        small = timed(1000)
        large = timed(4000)
        # 4x the customers. Linear would be ~4x; the old quadratic form was
        # ~16x. 10x leaves room for fixture and constant-factor noise while
        # still failing loudly on a return to quadratic behaviour.
        self.assertLess(
            large, max(small, 0.05) * 10,
            f"4x the customers took {large / max(small, 1e-6):.1f}x the time",
        )


if __name__ == "__main__":
    unittest.main()
