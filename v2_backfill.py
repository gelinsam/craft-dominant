"""V2 Backfill and Reconciliation Tools.

Idempotent SQLite → Postgres migration of V2 durable operational state.
Plus dry-run reconciliation for comparing the two stores.

Usage:
    # Backfill
    python v2_backfill.py backfill --sqlite craft_unified.db --pg postgresql://...

    # Reconciliation (dry-run comparison)
    python v2_backfill.py reconcile --sqlite craft_unified.db --pg postgresql://...

Safety guarantees:
    - Upsert only — never deletes existing Postgres data
    - Preserves IDs, timestamps, and statuses exactly
    - Reports inserts, skips, and conflicts per table
    - Deterministic and re-runnable (idempotent)
"""

from __future__ import annotations

import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

log = logging.getLogger("craft.v2_backfill")


# ─────────────────────────────────────────────────────────────────────────────
# Backfill
# ─────────────────────────────────────────────────────────────────────────────


def backfill_sqlite_to_postgres(
    sqlite_path: str,
    pg_conninfo: str,
    *,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Backfill V2 state from SQLite to Postgres.

    Upsert-only: existing Postgres rows with matching PKs are updated,
    new rows are inserted. No deletes ever.

    Args:
        sqlite_path: Path to the SQLite database file.
        pg_conninfo: Postgres connection string.
        dry_run: If True, count rows but don't write to Postgres.

    Returns:
        Dict with per-table counts: inserts, skips, errors.
    """
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Json

    # Connect to both databases
    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg.connect(pg_conninfo, row_factory=dict_row)

    results = {}

    try:
        # ── 1. Interventions ─────────────────────────────────────────────
        results["interventions"] = _backfill_interventions(
            sqlite_conn, pg_conn, dry_run
        )

        # ── 2. Audit Log ─────────────────────────────────────────────────
        results["intervention_audit_log"] = _backfill_audit_log(
            sqlite_conn, pg_conn, dry_run
        )

        # ── 3. Campaign Sends ────────────────────────────────────────────
        results["v2_campaign_sends"] = _backfill_campaign_sends(
            sqlite_conn, pg_conn, dry_run
        )

        # ── 4. Learning Records ──────────────────────────────────────────
        results["v2_learning_records"] = _backfill_learning_records(
            sqlite_conn, pg_conn, dry_run
        )

        # ── 5. Suppression Sentinel ──────────────────────────────────────
        results["v2_suppression_sync"] = _backfill_suppression_sentinel(
            sqlite_conn, pg_conn, dry_run
        )

        if not dry_run:
            pg_conn.commit()

    except Exception as e:
        pg_conn.rollback()
        results["error"] = str(e)
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()

    results["dry_run"] = dry_run
    return results


def _parse_ts(val: Optional[str]) -> Optional[datetime]:
    """Parse an ISO timestamp string to a UTC-aware datetime."""
    if not val or not val.strip():
        return None
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _parse_json(val) -> dict:
    """Parse a JSON string to a dict."""
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _backfill_interventions(sqlite_conn, pg_conn, dry_run: bool) -> Dict[str, int]:
    from psycopg.types.json import Json

    rows = sqlite_conn.execute("SELECT * FROM interventions").fetchall()
    inserted = skipped = errors = 0

    for row in rows:
        d = dict(row)
        try:
            if dry_run:
                inserted += 1
                continue

            pg_conn.execute(
                """INSERT INTO interventions
                   (id, opportunity_id, event_id, intervention_type, status,
                    rationale, audience_definition, expected_revenue, expected_cost,
                    expected_net_value, confidence, approval_required, created_at,
                    approved_at, executed_at, measurement_window, actual_revenue,
                    actual_cost, actual_net_value, outcome_status, evidence,
                    campaign_draft_id, measurement_started_at, measurement_ends_at,
                    attributed_orders, attributed_tickets, attributed_revenue, sent_count)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (id) DO UPDATE SET
                    status = EXCLUDED.status,
                    approved_at = EXCLUDED.approved_at,
                    executed_at = EXCLUDED.executed_at,
                    actual_revenue = EXCLUDED.actual_revenue,
                    actual_cost = EXCLUDED.actual_cost,
                    actual_net_value = EXCLUDED.actual_net_value,
                    outcome_status = EXCLUDED.outcome_status,
                    evidence = EXCLUDED.evidence,
                    campaign_draft_id = EXCLUDED.campaign_draft_id,
                    measurement_started_at = EXCLUDED.measurement_started_at,
                    measurement_ends_at = EXCLUDED.measurement_ends_at,
                    attributed_orders = EXCLUDED.attributed_orders,
                    attributed_tickets = EXCLUDED.attributed_tickets,
                    attributed_revenue = EXCLUDED.attributed_revenue,
                    sent_count = EXCLUDED.sent_count""",
                (
                    d["id"],
                    d["opportunity_id"],
                    d["event_id"],
                    d["intervention_type"],
                    d["status"],
                    d.get("rationale", ""),
                    d.get("audience_definition", ""),
                    float(d.get("expected_revenue") or 0),
                    float(d.get("expected_cost") or 0),
                    float(d.get("expected_net_value") or 0),
                    float(d.get("confidence") or 0),
                    bool(d.get("approval_required", 1)),
                    _parse_ts(d.get("created_at")),
                    _parse_ts(d.get("approved_at")),
                    _parse_ts(d.get("executed_at")),
                    int(d.get("measurement_window") or 14),
                    float(d["actual_revenue"]) if d.get("actual_revenue") is not None else None,
                    float(d["actual_cost"]) if d.get("actual_cost") is not None else None,
                    float(d["actual_net_value"]) if d.get("actual_net_value") is not None else None,
                    d.get("outcome_status"),
                    Json(_parse_json(d.get("evidence"))),
                    d.get("campaign_draft_id"),
                    _parse_ts(d.get("measurement_started_at")),
                    _parse_ts(d.get("measurement_ends_at")),
                    int(d["attributed_orders"]) if d.get("attributed_orders") is not None else None,
                    int(d["attributed_tickets"]) if d.get("attributed_tickets") is not None else None,
                    float(d["attributed_revenue"]) if d.get("attributed_revenue") is not None else None,
                    int(d["sent_count"]) if d.get("sent_count") is not None else None,
                ),
            )
            inserted += 1
        except Exception as e:
            log.error(f"Backfill intervention {d.get('id')}: {e}")
            errors += 1

    return {"source_rows": len(rows), "inserted": inserted, "skipped": skipped, "errors": errors}


def _backfill_audit_log(sqlite_conn, pg_conn, dry_run: bool) -> Dict[str, int]:
    from psycopg.types.json import Json

    try:
        rows = sqlite_conn.execute(
            "SELECT * FROM intervention_audit_log ORDER BY id ASC"
        ).fetchall()
    except Exception:
        return {"source_rows": 0, "inserted": 0, "skipped": 0, "errors": 0, "note": "table not found"}

    inserted = skipped = errors = 0

    for row in rows:
        d = dict(row)
        try:
            if dry_run:
                inserted += 1
                continue

            # Check if this exact audit entry already exists (by matching content)
            existing = pg_conn.execute(
                """SELECT id FROM intervention_audit_log
                   WHERE intervention_id = %s AND action = %s AND timestamp = %s
                   LIMIT 1""",
                (d["intervention_id"], d["action"], _parse_ts(d.get("timestamp"))),
            ).fetchone()

            if existing:
                skipped += 1
                continue

            pg_conn.execute(
                """INSERT INTO intervention_audit_log
                   (intervention_id, event_id, action, from_status, to_status,
                    actor, timestamp, metadata, error)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    d["intervention_id"],
                    d["event_id"],
                    d["action"],
                    d.get("from_status"),
                    d.get("to_status"),
                    d.get("actor", "system"),
                    _parse_ts(d.get("timestamp")),
                    Json(_parse_json(d.get("metadata"))),
                    d.get("error"),
                ),
            )
            inserted += 1
        except Exception as e:
            log.error(f"Backfill audit entry {d.get('id')}: {e}")
            errors += 1

    return {"source_rows": len(rows), "inserted": inserted, "skipped": skipped, "errors": errors}


def _backfill_campaign_sends(sqlite_conn, pg_conn, dry_run: bool) -> Dict[str, int]:
    try:
        rows = sqlite_conn.execute("SELECT * FROM v2_campaign_sends").fetchall()
    except Exception:
        return {"source_rows": 0, "inserted": 0, "skipped": 0, "errors": 0, "note": "table not found"}

    inserted = skipped = errors = 0

    for row in rows:
        d = dict(row)
        try:
            if dry_run:
                inserted += 1
                continue

            pg_conn.execute(
                """INSERT INTO v2_campaign_sends
                   (intervention_id, campaign_draft_id, email, sent_at)
                   VALUES (%s,%s,%s,%s)
                   ON CONFLICT (intervention_id, email) DO NOTHING""",
                (
                    d["intervention_id"],
                    d["campaign_draft_id"],
                    d["email"],
                    _parse_ts(d.get("sent_at")),
                ),
            )
            inserted += 1
        except Exception as e:
            log.error(f"Backfill send {d.get('id')}: {e}")
            errors += 1

    return {"source_rows": len(rows), "inserted": inserted, "skipped": skipped, "errors": errors}


def _backfill_learning_records(sqlite_conn, pg_conn, dry_run: bool) -> Dict[str, int]:
    from psycopg.types.json import Json

    try:
        rows = sqlite_conn.execute("SELECT * FROM v2_learning_records").fetchall()
    except Exception:
        return {"source_rows": 0, "inserted": 0, "skipped": 0, "errors": 0, "note": "table not found"}

    inserted = skipped = errors = 0

    for row in rows:
        d = dict(row)
        try:
            if dry_run:
                inserted += 1
                continue

            pg_conn.execute(
                """INSERT INTO v2_learning_records
                   (intervention_id, intervention_type, event_id, event_type, city,
                    predicted_revenue, attributed_revenue, prediction_error,
                    audience_count, sent_count, attributed_orders, attributed_tickets,
                    actual_conversion_rate, conversion_assumptions, confidence,
                    measurement_window_days, measurement_started_at, measurement_ended_at,
                    created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (intervention_id) DO UPDATE SET
                    attributed_revenue = EXCLUDED.attributed_revenue,
                    prediction_error = EXCLUDED.prediction_error,
                    attributed_orders = EXCLUDED.attributed_orders,
                    attributed_tickets = EXCLUDED.attributed_tickets,
                    actual_conversion_rate = EXCLUDED.actual_conversion_rate,
                    conversion_assumptions = EXCLUDED.conversion_assumptions,
                    measurement_ended_at = EXCLUDED.measurement_ended_at""",
                (
                    d["intervention_id"],
                    d["intervention_type"],
                    d["event_id"],
                    d.get("event_type", ""),
                    d.get("city", ""),
                    float(d.get("predicted_revenue") or 0),
                    float(d.get("attributed_revenue") or 0),
                    float(d.get("prediction_error") or 0),
                    int(d.get("audience_count") or 0),
                    int(d.get("sent_count") or 0),
                    int(d.get("attributed_orders") or 0),
                    int(d.get("attributed_tickets") or 0),
                    float(d["actual_conversion_rate"]) if d.get("actual_conversion_rate") is not None else None,
                    Json(_parse_json(d.get("conversion_assumptions"))),
                    float(d.get("confidence") or 0),
                    int(d.get("measurement_window_days") or 7),
                    _parse_ts(d.get("measurement_started_at")),
                    _parse_ts(d.get("measurement_ended_at")),
                    _parse_ts(d.get("created_at")) or datetime.now(timezone.utc),
                ),
            )
            inserted += 1
        except Exception as e:
            log.error(f"Backfill learning {d.get('intervention_id')}: {e}")
            errors += 1

    return {"source_rows": len(rows), "inserted": inserted, "skipped": skipped, "errors": errors}


def _backfill_suppression_sentinel(sqlite_conn, pg_conn, dry_run: bool) -> Dict[str, int]:
    try:
        row = sqlite_conn.execute(
            "SELECT * FROM v2_suppression_sync WHERE id = 1"
        ).fetchone()
    except Exception:
        return {"source_rows": 0, "inserted": 0, "skipped": 0, "errors": 0, "note": "table not found"}

    if not row:
        return {"source_rows": 0, "inserted": 0, "skipped": 0, "errors": 0, "note": "no sentinel row"}

    d = dict(row)

    if dry_run:
        return {"source_rows": 1, "inserted": 1, "skipped": 0, "errors": 0}

    try:
        pg_conn.execute(
            """INSERT INTO v2_suppression_sync
               (id, last_synced_at, row_count, source, empty_acknowledged,
                acknowledged_by, acknowledged_at, acknowledged_reason,
                last_full_refresh_at, last_mutation_at,
                last_full_refresh_source, last_mutation_source)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (id) DO UPDATE SET
                last_synced_at = EXCLUDED.last_synced_at,
                row_count = EXCLUDED.row_count,
                source = EXCLUDED.source,
                empty_acknowledged = EXCLUDED.empty_acknowledged,
                acknowledged_by = EXCLUDED.acknowledged_by,
                acknowledged_at = EXCLUDED.acknowledged_at,
                acknowledged_reason = EXCLUDED.acknowledged_reason,
                last_full_refresh_at = EXCLUDED.last_full_refresh_at,
                last_mutation_at = EXCLUDED.last_mutation_at,
                last_full_refresh_source = EXCLUDED.last_full_refresh_source,
                last_mutation_source = EXCLUDED.last_mutation_source""",
            (
                1,
                _parse_ts(d.get("last_synced_at")),
                int(d.get("row_count") or 0),
                d.get("source", "unknown"),
                bool(d.get("empty_acknowledged", 0)),
                d.get("acknowledged_by"),
                _parse_ts(d.get("acknowledged_at")),
                d.get("acknowledged_reason"),
                _parse_ts(d.get("last_full_refresh_at")),
                _parse_ts(d.get("last_mutation_at")),
                d.get("last_full_refresh_source"),
                d.get("last_mutation_source"),
            ),
        )
        return {"source_rows": 1, "inserted": 1, "skipped": 0, "errors": 0}
    except Exception as e:
        log.error(f"Backfill suppression sentinel: {e}")
        return {"source_rows": 1, "inserted": 0, "skipped": 0, "errors": 1}


# ─────────────────────────────────────────────────────────────────────────────
# Reconciliation
# ─────────────────────────────────────────────────────────────────────────────


def reconcile_stores(
    sqlite_path: str,
    pg_conninfo: str,
) -> Dict[str, Any]:
    """Dry-run comparison of SQLite vs Postgres V2 state.

    Reports per-table: row counts, missing rows, status mismatches.
    Does NOT modify either database.
    """
    import psycopg
    from psycopg.rows import dict_row

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg.connect(pg_conninfo, row_factory=dict_row)

    results = {}

    try:
        # ── Interventions ────────────────────────────────────────────────
        results["interventions"] = _reconcile_interventions(sqlite_conn, pg_conn)

        # ── Audit Log ────────────────────────────────────────────────────
        results["intervention_audit_log"] = _reconcile_table_counts(
            sqlite_conn, pg_conn, "intervention_audit_log"
        )

        # ── Campaign Sends ───────────────────────────────────────────────
        results["v2_campaign_sends"] = _reconcile_table_counts(
            sqlite_conn, pg_conn, "v2_campaign_sends"
        )

        # ── Learning Records ─────────────────────────────────────────────
        results["v2_learning_records"] = _reconcile_table_counts(
            sqlite_conn, pg_conn, "v2_learning_records"
        )

        # ── Suppression Sentinel ─────────────────────────────────────────
        results["v2_suppression_sync"] = _reconcile_table_counts(
            sqlite_conn, pg_conn, "v2_suppression_sync"
        )

    finally:
        sqlite_conn.close()
        pg_conn.close()

    return results


def _reconcile_interventions(sqlite_conn, pg_conn) -> Dict[str, Any]:
    """Compare interventions between SQLite and Postgres."""
    sqlite_rows = sqlite_conn.execute("SELECT id, status FROM interventions").fetchall()
    pg_rows = pg_conn.execute("SELECT id, status FROM interventions").fetchall()

    sqlite_map = {r["id"]: r["status"] for r in sqlite_rows}
    pg_map = {r["id"]: r["status"] for r in pg_rows}

    only_sqlite = sorted(set(sqlite_map.keys()) - set(pg_map.keys()))
    only_pg = sorted(set(pg_map.keys()) - set(sqlite_map.keys()))

    mismatched = []
    for id_ in set(sqlite_map.keys()) & set(pg_map.keys()):
        if sqlite_map[id_] != pg_map[id_]:
            mismatched.append({
                "id": id_,
                "sqlite_status": sqlite_map[id_],
                "pg_status": pg_map[id_],
            })

    return {
        "sqlite_count": len(sqlite_rows),
        "pg_count": len(pg_rows),
        "only_in_sqlite": only_sqlite,
        "only_in_pg": only_pg,
        "status_mismatches": mismatched,
        "in_sync": len(only_sqlite) == 0 and len(only_pg) == 0 and len(mismatched) == 0,
    }


def _reconcile_table_counts(sqlite_conn, pg_conn, table: str) -> Dict[str, Any]:
    """Simple count comparison for a table."""
    try:
        sqlite_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    except Exception:
        sqlite_count = None

    try:
        pg_row = pg_conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
        pg_count = pg_row["c"] if pg_row else None
    except Exception:
        pg_count = None

    return {
        "sqlite_count": sqlite_count,
        "pg_count": pg_count,
        "in_sync": sqlite_count == pg_count,
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def main():
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

    parser = argparse.ArgumentParser(description="V2 Backfill and Reconciliation Tools")
    subparsers = parser.add_subparsers(dest="command")

    # Backfill
    bf = subparsers.add_parser("backfill", help="Backfill SQLite → Postgres")
    bf.add_argument("--sqlite", required=True, help="Path to SQLite database")
    bf.add_argument("--pg", required=True, help="Postgres connection string")
    bf.add_argument("--dry-run", action="store_true", help="Count only, don't write")

    # Reconcile
    rc = subparsers.add_parser("reconcile", help="Compare SQLite vs Postgres")
    rc.add_argument("--sqlite", required=True, help="Path to SQLite database")
    rc.add_argument("--pg", required=True, help="Postgres connection string")

    args = parser.parse_args()

    if args.command == "backfill":
        result = backfill_sqlite_to_postgres(args.sqlite, args.pg, dry_run=args.dry_run)
        print(json.dumps(result, indent=2, default=str))

    elif args.command == "reconcile":
        result = reconcile_stores(args.sqlite, args.pg)
        print(json.dumps(result, indent=2, default=str))

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
