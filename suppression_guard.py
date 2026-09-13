"""Fail-closed suppression guard for V2 CRM execution.

Ensures the suppression list is positively known to be valid before
allowing campaign preparation or execution. Distinguishes between:

- suppression data never synced
- suppression data synced and non-empty
- suppression data synced and legitimately empty (requires explicit acknowledgment)
- suppression data stale
- suppression query/storage unavailable

Any ambiguous state blocks the operation.

Hotfix: addresses fail-open defect where ephemeral SQLite restart
recreates schema but loses suppression rows, making "data lost"
indistinguishable from "nobody is suppressed."
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Dict, Optional, Set, Tuple

log = logging.getLogger("craft.suppression_guard")

# Default: suppression data older than 24 hours is considered stale
DEFAULT_MAX_AGE_HOURS = 24

SENTINEL_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS v2_suppression_sync (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_synced_at TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
    empty_acknowledged INTEGER NOT NULL DEFAULT 0,
    acknowledged_by TEXT,
    acknowledged_at TEXT,
    acknowledged_reason TEXT,
    last_full_refresh_at TEXT,
    last_mutation_at TEXT,
    last_full_refresh_source TEXT,
    last_mutation_source TEXT
);
"""

# Columns added in the dual-timestamp freshness split.  _ensure_sentinel_table()
# uses ALTER TABLE to add any that are missing on existing databases.
_DUAL_TIMESTAMP_COLUMNS = [
    ("last_full_refresh_at", "TEXT"),
    ("last_mutation_at", "TEXT"),
    ("last_full_refresh_source", "TEXT"),
    ("last_mutation_source", "TEXT"),
]


class SuppressionStatus(Enum):
    """Suppression validation states — only HEALTHY and ACKNOWLEDGED_EMPTY allow operations."""
    HEALTHY = "healthy"
    ACKNOWLEDGED_EMPTY = "acknowledged_empty"
    NEVER_SYNCED = "suppression_never_synced"
    STALE = "suppression_stale"
    EMPTY_UNVERIFIED = "suppression_empty_unverified"
    UNAVAILABLE = "suppression_unavailable"
    COUNT_MISMATCH = "suppression_count_mismatch"


def _max_age_hours() -> int:
    """Read configurable freshness threshold from env."""
    try:
        return int(os.environ.get("SUPPRESSION_MAX_AGE_HOURS", str(DEFAULT_MAX_AGE_HOURS)))
    except (ValueError, TypeError):
        return DEFAULT_MAX_AGE_HOURS


class SuppressionGuard:
    """Fail-closed suppression validator.

    Checks the v2_suppression_sync sentinel table to determine whether
    suppression data is trustworthy before allowing operations.
    """

    def __init__(self, db):
        self.db = db
        self._ensure_sentinel_table()

    def _ensure_sentinel_table(self) -> None:
        """Create the sentinel table if it doesn't exist, and migrate schema.

        Handles upgrades from pre-dual-timestamp schema by adding missing
        columns via ALTER TABLE. If the existing sentinel has source
        'mailchimp_full_refresh', backfills last_full_refresh_at from
        last_synced_at. Webhook sources do NOT backfill authoritative
        freshness — only a true full refresh establishes it.
        """
        try:
            self.db.conn.executescript(SENTINEL_TABLE_SCHEMA)
            self.db.conn.commit()
        except Exception as e:
            log.error(f"Failed to create suppression sentinel table: {e}")
            return

        # Migrate: add dual-timestamp columns if absent
        try:
            existing = {
                row[1] for row in
                self.db.conn.execute("PRAGMA table_info(v2_suppression_sync)").fetchall()
            }
            for col_name, col_type in _DUAL_TIMESTAMP_COLUMNS:
                if col_name not in existing:
                    self.db.conn.execute(
                        f"ALTER TABLE v2_suppression_sync ADD COLUMN {col_name} {col_type}"
                    )
                    log.info(f"_ensure_sentinel_table: added column {col_name}")
            self.db.conn.commit()
        except Exception as e:
            log.error(f"Failed to migrate sentinel schema: {e}")
            return

        # Backfill: if legacy sentinel exists with source 'mailchimp_full_refresh',
        # copy last_synced_at → last_full_refresh_at (only if not yet set)
        try:
            row = self.db.conn.execute(
                "SELECT * FROM v2_suppression_sync WHERE id = 1"
            ).fetchone()
            if row is not None:
                row = dict(row)
                if (row.get("source") == "mailchimp_full_refresh"
                        and row.get("last_full_refresh_at") is None
                        and row.get("last_synced_at")):
                    self.db.conn.execute(
                        """UPDATE v2_suppression_sync SET
                               last_full_refresh_at = last_synced_at,
                               last_full_refresh_source = 'mailchimp_full_refresh'
                           WHERE id = 1""",
                    )
                    self.db.conn.commit()
                    log.info("_ensure_sentinel_table: backfilled last_full_refresh_at "
                             "from legacy mailchimp_full_refresh sentinel")
        except Exception as e:
            log.error(f"Failed to backfill dual-timestamp columns: {e}")

    def _actual_suppression_count(self) -> int:
        """Read actual row count from the suppressions table.

        Raises on query failure so callers can return UNAVAILABLE.
        """
        result = self.db.conn.execute(
            "SELECT COUNT(*) AS cnt FROM suppressions"
        ).fetchone()
        return result["cnt"] if result else 0

    def validate(self) -> Tuple[SuppressionStatus, Dict[str, Any]]:
        """Check suppression state. Returns (status, details).

        Only HEALTHY and ACKNOWLEDGED_EMPTY are safe to proceed.
        Everything else must block.

        Cross-checks the sentinel's row_count against the actual
        suppression table to detect silent data loss.
        """
        now = datetime.now(timezone.utc)

        # Step 1: Read the sentinel
        try:
            row = self.db.conn.execute(
                "SELECT * FROM v2_suppression_sync WHERE id = 1"
            ).fetchone()
        except Exception as e:
            return SuppressionStatus.UNAVAILABLE, {
                "reason": f"Cannot read suppression sentinel: {e}",
            }

        if row is None:
            return SuppressionStatus.NEVER_SYNCED, {
                "reason": "Suppression data has never been synced. "
                          "Run a suppression sync before preparing or executing campaigns.",
            }

        row = dict(row)
        last_synced_at = row.get("last_synced_at")
        sentinel_count = row.get("row_count", 0)
        empty_acknowledged = bool(row.get("empty_acknowledged", 0))
        acknowledged_at = row.get("acknowledged_at")
        source = row.get("source", "unknown")
        last_full_refresh_at = row.get("last_full_refresh_at")
        last_mutation_at = row.get("last_mutation_at")

        # Step 2: Check authoritative freshness (last_full_refresh_at only)
        # A webhook mutation alone must NOT satisfy the freshness requirement.
        # If last_full_refresh_at is NULL, no authoritative refresh has ever
        # completed — even if webhooks have been updating last_synced_at.
        if last_full_refresh_at is None:
            # Backward compat: if this is a pre-migration sentinel that was
            # NOT a full refresh, we treat it as never authoritatively synced.
            # Backfill in _ensure_sentinel_table handles the full-refresh case.
            return SuppressionStatus.NEVER_SYNCED, {
                "reason": "No authoritative full refresh has completed. "
                          "Webhook mutations alone do not establish suppression completeness. "
                          "Run POST /api/v2/suppressions/refresh to bootstrap.",
                "last_synced_at": last_synced_at,
                "last_mutation_at": last_mutation_at,
            }

        try:
            refresh_dt = datetime.fromisoformat(last_full_refresh_at)
        except (ValueError, TypeError):
            return SuppressionStatus.UNAVAILABLE, {
                "reason": f"Invalid last_full_refresh_at timestamp: {last_full_refresh_at!r}",
                "last_full_refresh_at": last_full_refresh_at,
                "last_synced_at": last_synced_at,
            }

        max_age = timedelta(hours=_max_age_hours())
        age = now - refresh_dt
        if age > max_age:
            return SuppressionStatus.STALE, {
                "reason": f"Authoritative suppression refresh is {age.total_seconds() / 3600:.1f}h old "
                          f"(threshold: {_max_age_hours()}h). "
                          "Webhook mutations do not extend this window.",
                "last_full_refresh_at": last_full_refresh_at,
                "last_synced_at": last_synced_at,
                "last_mutation_at": last_mutation_at,
                "row_count": sentinel_count,
                "age_hours": round(age.total_seconds() / 3600, 1),
            }

        # Step 3: Verify actual suppression table count matches sentinel
        try:
            actual_count = self._actual_suppression_count()
        except Exception as e:
            return SuppressionStatus.UNAVAILABLE, {
                "reason": f"Cannot read suppression table: {e}",
                "last_synced_at": last_synced_at,
                "sentinel_row_count": sentinel_count,
            }

        if actual_count != sentinel_count:
            return SuppressionStatus.COUNT_MISMATCH, {
                "reason": f"Suppression sentinel says {sentinel_count} rows but "
                          f"actual table has {actual_count} rows. "
                          "This indicates data loss or an unsynchronized write.",
                "sentinel_row_count": sentinel_count,
                "actual_row_count": actual_count,
                "last_synced_at": last_synced_at,
                "last_full_refresh_at": last_full_refresh_at,
                "last_mutation_at": last_mutation_at,
                "source": source,
            }

        # Step 4: Check row count
        if sentinel_count > 0:
            return SuppressionStatus.HEALTHY, {
                "last_synced_at": last_synced_at,
                "last_full_refresh_at": last_full_refresh_at,
                "last_mutation_at": last_mutation_at,
                "row_count": sentinel_count,
                "source": source,
            }

        # Step 5: sentinel_count == 0 — check acknowledgment
        if not empty_acknowledged:
            return SuppressionStatus.EMPTY_UNVERIFIED, {
                "reason": "Suppression list is empty and has not been explicitly acknowledged. "
                          "This could indicate data loss after a restart.",
                "last_synced_at": last_synced_at,
                "last_full_refresh_at": last_full_refresh_at,
                "last_mutation_at": last_mutation_at,
                "row_count": 0,
            }

        # Step 6: Acknowledgment exists — check expiry (24h from acknowledgment)
        try:
            ack_dt = datetime.fromisoformat(acknowledged_at)
        except (ValueError, TypeError):
            return SuppressionStatus.EMPTY_UNVERIFIED, {
                "reason": "Empty-set acknowledgment has invalid timestamp.",
                "last_synced_at": last_synced_at,
                "last_full_refresh_at": last_full_refresh_at,
                "last_mutation_at": last_mutation_at,
                "row_count": 0,
            }

        ack_age = now - ack_dt
        if ack_age > timedelta(hours=24):
            return SuppressionStatus.EMPTY_UNVERIFIED, {
                "reason": f"Empty-set acknowledgment expired ({ack_age.total_seconds() / 3600:.1f}h ago). "
                          "Re-acknowledge if the empty suppression list is still correct.",
                "last_synced_at": last_synced_at,
                "last_full_refresh_at": last_full_refresh_at,
                "last_mutation_at": last_mutation_at,
                "row_count": 0,
                "acknowledged_at": acknowledged_at,
                "acknowledgment_age_hours": round(ack_age.total_seconds() / 3600, 1),
            }

        return SuppressionStatus.ACKNOWLEDGED_EMPTY, {
            "last_synced_at": last_synced_at,
            "last_full_refresh_at": last_full_refresh_at,
            "last_mutation_at": last_mutation_at,
            "row_count": 0,
            "acknowledged_by": row.get("acknowledged_by"),
            "acknowledged_at": acknowledged_at,
            "acknowledged_reason": row.get("acknowledged_reason"),
        }

    def is_valid(self) -> bool:
        """Shorthand: is suppression state safe to proceed?"""
        status, _ = self.validate()
        return status in (SuppressionStatus.HEALTHY, SuppressionStatus.ACKNOWLEDGED_EMPTY)

    def get_suppressions_if_valid(self) -> Tuple[SuppressionStatus, Dict[str, Any], Set[str]]:
        """Validate suppression state AND return the suppression set if valid.

        Returns (status, details, emails). emails is empty if status is not valid.

        Post-fetch verification: after reading emails, verifies that
        len(emails) matches the sentinel row_count. This catches races
        where a mutation happens between validate() and the fetch.
        """
        status, details = self.validate()
        if status not in (SuppressionStatus.HEALTHY, SuppressionStatus.ACKNOWLEDGED_EMPTY):
            return status, details, set()

        # Read sentinel row_count for post-fetch check
        sentinel_count = details.get("row_count", 0)

        # Suppression state is valid — now read the actual emails
        try:
            rows = self.db.conn.execute("SELECT email FROM suppressions").fetchall()
            emails = {r["email"] for r in rows}
        except Exception as e:
            return SuppressionStatus.UNAVAILABLE, {
                "reason": f"Suppression query failed after valid sentinel: {e}",
            }, set()

        # Post-fetch verification: fetched count must match sentinel
        if len(emails) != sentinel_count:
            return SuppressionStatus.COUNT_MISMATCH, {
                "reason": f"Fetched {len(emails)} suppression emails but sentinel "
                          f"says {sentinel_count}. Possible race or data corruption.",
                "sentinel_row_count": sentinel_count,
                "actual_row_count": len(emails),
                "last_synced_at": details.get("last_synced_at"),
                "source": details.get("source", "unknown"),
            }, set()

        return status, details, emails

    def record_mutation(self, source: str = "webhook") -> Dict[str, Any]:
        """Update the sentinel after a successful suppression table mutation.

        Must be called AFTER the underlying suppression INSERT/DELETE
        has been committed. Reads the actual current count from the
        suppressions table and writes it to the sentinel.

        CRITICAL: Updates last_synced_at, last_mutation_at, and
        last_mutation_source ONLY. Does NOT touch last_full_refresh_at.
        A webhook mutation is evidence of one event — it must never
        masquerade as evidence of completeness.

        If actual count > 0 and there was a prior empty-acknowledged
        state, clears the acknowledgment (real data invalidates it).

        Returns details about the update for audit logging.
        """
        now = datetime.now(timezone.utc).isoformat()

        try:
            actual_count = self._actual_suppression_count()
        except Exception as e:
            log.error(f"record_mutation: cannot count suppression rows: {e}")
            return {
                "error": f"Cannot count suppression rows: {e}",
                "source": source,
            }

        try:
            if actual_count > 0:
                # Real rows exist — clear any acknowledgment state
                # Note: last_full_refresh_at is deliberately NOT updated here
                self.db.conn.execute(
                    """INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source,
                           empty_acknowledged, acknowledged_by, acknowledged_at, acknowledged_reason,
                           last_mutation_at, last_mutation_source)
                       VALUES (1, ?, ?, ?, 0, NULL, NULL, NULL, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET
                           last_synced_at = excluded.last_synced_at,
                           row_count = excluded.row_count,
                           source = excluded.source,
                           empty_acknowledged = 0,
                           acknowledged_by = NULL,
                           acknowledged_at = NULL,
                           acknowledged_reason = NULL,
                           last_mutation_at = excluded.last_mutation_at,
                           last_mutation_source = excluded.last_mutation_source""",
                    (now, actual_count, source, now, source),
                )
            else:
                # Zero rows — preserve acknowledgment state, update count and time
                self.db.conn.execute(
                    """INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source,
                           last_mutation_at, last_mutation_source)
                       VALUES (1, ?, 0, ?, ?, ?)
                       ON CONFLICT(id) DO UPDATE SET
                           last_synced_at = excluded.last_synced_at,
                           row_count = 0,
                           source = excluded.source,
                           last_mutation_at = excluded.last_mutation_at,
                           last_mutation_source = excluded.last_mutation_source""",
                    (now, source, now, source),
                )
            self.db.conn.commit()
        except Exception as e:
            log.error(f"record_mutation: cannot update sentinel: {e}")
            return {
                "error": f"Cannot update sentinel: {e}",
                "source": source,
                "actual_count": actual_count,
            }

        return {
            "updated": True,
            "row_count": actual_count,
            "source": source,
            "last_synced_at": now,
            "last_mutation_at": now,
        }

    def record_sync(self, row_count: int, source: str = "manual") -> None:
        """Record a successful suppression sync.

        Replaces any existing sentinel. If row_count > 0, clears any
        prior empty-acknowledged state since real data replaced it.

        This is a generic sync — it updates last_synced_at but does NOT
        set last_full_refresh_at (only refresh_from_mailchimp does that).
        """
        now = datetime.now(timezone.utc).isoformat()

        # If real rows are synced, clear acknowledgment state
        if row_count > 0:
            self.db.conn.execute(
                """INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source,
                       empty_acknowledged, acknowledged_by, acknowledged_at, acknowledged_reason)
                   VALUES (1, ?, ?, ?, 0, NULL, NULL, NULL)
                   ON CONFLICT(id) DO UPDATE SET
                       last_synced_at = excluded.last_synced_at,
                       row_count = excluded.row_count,
                       source = excluded.source,
                       empty_acknowledged = 0,
                       acknowledged_by = NULL,
                       acknowledged_at = NULL,
                       acknowledged_reason = NULL""",
                (now, row_count, source),
            )
        else:
            # Zero rows — preserve acknowledgment state if it exists; update sync time
            self.db.conn.execute(
                """INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source)
                   VALUES (1, ?, 0, ?)
                   ON CONFLICT(id) DO UPDATE SET
                       last_synced_at = excluded.last_synced_at,
                       row_count = 0,
                       source = excluded.source""",
                (now, source),
            )
        self.db.conn.commit()

    def refresh_from_mailchimp(self, mailchimp_client) -> Dict[str, Any]:
        """Perform an authoritative full-refresh of suppressions from Mailchimp.

        This is the bootstrap and ongoing refresh path. It:
        1. Queries Mailchimp for ALL unsubscribed + cleaned members (paginated).
        2. Normalizes emails (lowercase, stripped).
        3. In a single local transaction: replaces the entire suppressions table,
           then updates the sentinel with exact count, timestamp, and source.
        4. If the Mailchimp query fails at any point, returns an error WITHOUT
           touching local state — all-or-nothing from the sentinel's perspective.

        Returns dict with either {"refreshed": True, "row_count": N, ...}
        or {"error": "..."}.
        """
        from datetime import datetime, timezone

        # Step 1: Fetch complete suppressed set from Mailchimp
        log.info("refresh_from_mailchimp: starting authoritative suppression refresh")
        try:
            raw_emails = mailchimp_client.get_suppressed_members()
        except Exception as e:
            log.error(f"refresh_from_mailchimp: Mailchimp query raised: {e}")
            return {"error": f"Mailchimp query failed: {e}"}

        if raw_emails is None:
            log.error("refresh_from_mailchimp: Mailchimp returned None — incomplete data")
            return {"error": "Mailchimp suppression query returned incomplete data. "
                            "Local suppressions NOT updated."}

        # Step 2: Normalize and deduplicate
        normalized = sorted(set(e.lower().strip() for e in raw_emails if e and e.strip()))
        count = len(normalized)
        log.info(f"refresh_from_mailchimp: {count} unique suppressed emails from Mailchimp")

        # Step 3: Atomic local replacement — transaction wraps both tables
        now = datetime.now(timezone.utc).isoformat()
        try:
            # Delete all existing suppressions
            self.db.conn.execute("DELETE FROM suppressions")

            # Insert the authoritative set
            for email in normalized:
                self.db.conn.execute(
                    "INSERT OR IGNORE INTO suppressions (email, reason) "
                    "VALUES (?, 'mailchimp_suppressed')",
                    (email,),
                )

            # Update sentinel — clear any acknowledgment if real rows exist
            # CRITICAL: Set last_full_refresh_at — this IS the authoritative freshness
            if count > 0:
                self.db.conn.execute(
                    """INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source,
                           empty_acknowledged, acknowledged_by, acknowledged_at, acknowledged_reason,
                           last_full_refresh_at, last_full_refresh_source)
                       VALUES (1, ?, ?, 'mailchimp_full_refresh', 0, NULL, NULL, NULL, ?, 'mailchimp_full_refresh')
                       ON CONFLICT(id) DO UPDATE SET
                           last_synced_at = excluded.last_synced_at,
                           row_count = excluded.row_count,
                           source = 'mailchimp_full_refresh',
                           empty_acknowledged = 0,
                           acknowledged_by = NULL,
                           acknowledged_at = NULL,
                           acknowledged_reason = NULL,
                           last_full_refresh_at = excluded.last_full_refresh_at,
                           last_full_refresh_source = 'mailchimp_full_refresh'""",
                    (now, count, now),
                )
            else:
                # Zero suppressions from Mailchimp — preserve acknowledgment state
                self.db.conn.execute(
                    """INSERT INTO v2_suppression_sync (id, last_synced_at, row_count, source,
                           last_full_refresh_at, last_full_refresh_source)
                       VALUES (1, ?, 0, 'mailchimp_full_refresh', ?, 'mailchimp_full_refresh')
                       ON CONFLICT(id) DO UPDATE SET
                           last_synced_at = excluded.last_synced_at,
                           row_count = 0,
                           source = 'mailchimp_full_refresh',
                           last_full_refresh_at = excluded.last_full_refresh_at,
                           last_full_refresh_source = 'mailchimp_full_refresh'""",
                    (now, now),
                )

            self.db.conn.commit()
        except Exception as e:
            # Rollback on any failure — suppressions and sentinel stay untouched
            try:
                self.db.conn.rollback()
            except Exception:
                pass
            log.error(f"refresh_from_mailchimp: local write failed: {e}")
            return {"error": f"Local write failed — suppressions NOT updated: {e}"}

        log.info(f"refresh_from_mailchimp: complete. {count} suppressions, sentinel updated.")
        return {
            "refreshed": True,
            "row_count": count,
            "source": "mailchimp_full_refresh",
            "last_synced_at": now,
            "last_full_refresh_at": now,
        }

    def acknowledge_empty(self, actor: str, reason: str) -> Dict[str, Any]:
        """Explicitly acknowledge that an empty suppression list is legitimate.

        Requirements:
        - actor must be a named person (not empty)
        - reason must be provided
        - acknowledgment expires after 24 hours
        - any later sync with row_count > 0 replaces the acknowledgment

        Returns details about the acknowledgment.
        """
        if not actor or not actor.strip():
            raise ValueError("Acknowledgment requires a named actor")
        if not reason or not reason.strip():
            raise ValueError("Acknowledgment requires a reason")

        now = datetime.now(timezone.utc)

        # Check that sentinel exists (sync must have occurred)
        try:
            row = self.db.conn.execute(
                "SELECT * FROM v2_suppression_sync WHERE id = 1"
            ).fetchone()
        except Exception as e:
            raise RuntimeError(f"Cannot read suppression sentinel: {e}")

        if row is None:
            raise ValueError(
                "Cannot acknowledge empty suppressions: no sync has ever occurred. "
                "Run a suppression sync first."
            )

        row = dict(row)
        if row.get("row_count", 0) > 0:
            raise ValueError(
                f"Suppression list has {row['row_count']} rows — acknowledgment "
                "is only valid when the list is empty."
            )

        self.db.conn.execute(
            """UPDATE v2_suppression_sync SET
                   empty_acknowledged = 1,
                   acknowledged_by = ?,
                   acknowledged_at = ?,
                   acknowledged_reason = ?
               WHERE id = 1""",
            (actor.strip(), now.isoformat(), reason.strip()),
        )
        self.db.conn.commit()

        return {
            "status": "acknowledged",
            "actor": actor.strip(),
            "reason": reason.strip(),
            "acknowledged_at": now.isoformat(),
            "expires_at": (now + timedelta(hours=24)).isoformat(),
        }
