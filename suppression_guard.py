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
    acknowledged_reason TEXT
);
"""


class SuppressionStatus(Enum):
    """Suppression validation states — only HEALTHY and ACKNOWLEDGED_EMPTY allow operations."""
    HEALTHY = "healthy"
    ACKNOWLEDGED_EMPTY = "acknowledged_empty"
    NEVER_SYNCED = "suppression_never_synced"
    STALE = "suppression_stale"
    EMPTY_UNVERIFIED = "suppression_empty_unverified"
    UNAVAILABLE = "suppression_unavailable"


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
        """Create the sentinel table if it doesn't exist."""
        try:
            self.db.conn.executescript(SENTINEL_TABLE_SCHEMA)
            self.db.conn.commit()
        except Exception as e:
            log.error(f"Failed to create suppression sentinel table: {e}")

    def validate(self) -> Tuple[SuppressionStatus, Dict[str, Any]]:
        """Check suppression state. Returns (status, details).

        Only HEALTHY and ACKNOWLEDGED_EMPTY are safe to proceed.
        Everything else must block.
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
        row_count = row.get("row_count", 0)
        empty_acknowledged = bool(row.get("empty_acknowledged", 0))
        acknowledged_at = row.get("acknowledged_at")

        # Step 2: Check freshness
        try:
            synced_dt = datetime.fromisoformat(last_synced_at)
        except (ValueError, TypeError):
            return SuppressionStatus.UNAVAILABLE, {
                "reason": f"Invalid last_synced_at timestamp: {last_synced_at!r}",
                "last_synced_at": last_synced_at,
            }

        max_age = timedelta(hours=_max_age_hours())
        age = now - synced_dt
        if age > max_age:
            return SuppressionStatus.STALE, {
                "reason": f"Suppression data is {age.total_seconds() / 3600:.1f}h old "
                          f"(threshold: {_max_age_hours()}h).",
                "last_synced_at": last_synced_at,
                "row_count": row_count,
                "age_hours": round(age.total_seconds() / 3600, 1),
            }

        # Step 3: Check row count
        if row_count > 0:
            return SuppressionStatus.HEALTHY, {
                "last_synced_at": last_synced_at,
                "row_count": row_count,
                "source": row.get("source", "unknown"),
            }

        # Step 4: row_count == 0 — check acknowledgment
        if not empty_acknowledged:
            return SuppressionStatus.EMPTY_UNVERIFIED, {
                "reason": "Suppression list is empty and has not been explicitly acknowledged. "
                          "This could indicate data loss after a restart.",
                "last_synced_at": last_synced_at,
                "row_count": 0,
            }

        # Step 5: Acknowledgment exists — check expiry (24h from acknowledgment)
        try:
            ack_dt = datetime.fromisoformat(acknowledged_at)
        except (ValueError, TypeError):
            return SuppressionStatus.EMPTY_UNVERIFIED, {
                "reason": "Empty-set acknowledgment has invalid timestamp.",
                "last_synced_at": last_synced_at,
                "row_count": 0,
            }

        ack_age = now - ack_dt
        if ack_age > timedelta(hours=24):
            return SuppressionStatus.EMPTY_UNVERIFIED, {
                "reason": f"Empty-set acknowledgment expired ({ack_age.total_seconds() / 3600:.1f}h ago). "
                          "Re-acknowledge if the empty suppression list is still correct.",
                "last_synced_at": last_synced_at,
                "row_count": 0,
                "acknowledged_at": acknowledged_at,
                "acknowledgment_age_hours": round(ack_age.total_seconds() / 3600, 1),
            }

        return SuppressionStatus.ACKNOWLEDGED_EMPTY, {
            "last_synced_at": last_synced_at,
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
        """
        status, details = self.validate()
        if status not in (SuppressionStatus.HEALTHY, SuppressionStatus.ACKNOWLEDGED_EMPTY):
            return status, details, set()

        # Suppression state is valid — now read the actual emails
        try:
            rows = self.db.conn.execute("SELECT email FROM suppressions").fetchall()
            emails = {r["email"] for r in rows}
        except Exception as e:
            return SuppressionStatus.UNAVAILABLE, {
                "reason": f"Suppression query failed after valid sentinel: {e}",
            }, set()

        return status, details, emails

    def record_sync(self, row_count: int, source: str = "manual") -> None:
        """Record a successful suppression sync.

        Replaces any existing sentinel. If row_count > 0, clears any
        prior empty-acknowledged state since real data replaced it.
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
