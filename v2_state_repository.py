"""V2 State Repository — domain-specific persistence for V2 durable operational state.

This module defines the V2StateRepository protocol (abstract base) and provides
concrete implementations:
    - SQLiteV2StateRepository  — uses the shared SQLite connection (current behavior)
    - PostgresV2StateRepository — uses psycopg3 against a dedicated Postgres database

Architecture boundary:
    - V2 state (interventions, audit log, sends, learning, suppression sentinel)
      → persisted via V2StateRepository
    - Analytics (events, orders, customers, snapshots, ad_spend, suppressions)
      → stays in SQLite, accessed via the existing Database class

The repository is NOT a generic ORM. Each method maps to a specific V2 business
operation. Implementations handle connection management, serialization, and
transaction boundaries internally.
"""

from __future__ import annotations

import abc
import json
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from intervention_model import (
    AUDIT_LOG_SCHEMA,
    CAMPAIGN_SENDS_SCHEMA,
    INTERVENTION_SCHEMA,
    LEARNING_RECORD_SCHEMA,
    TERMINAL_STATES,
    Intervention,
    InterventionStatus,
)

log = logging.getLogger("craft.v2_state")


# Every table V2 operational state depends on. Readiness requires ALL of
# them: v2_send_attempts and v2_send_attempt_recipients are what make the
# duplicate-send guarantee enforceable, so a database without them is not
# healthy for this code even though simpler queries would still work.
_V2_EXPECTED_TABLES = (
    "interventions",
    "intervention_audit_log",
    "v2_campaign_sends",
    "v2_learning_records",
    "v2_suppression_sync",
    "v2_send_attempts",
    "v2_send_attempt_recipients",
)

# Highest migration this code requires:
#   001 base schema
#   002 send attempts (durable claims)
#   003 provider_sent_at (authoritative attribution clock)
REQUIRED_SCHEMA_VERSION = 3


def _iso_utc(value: Any) -> str:
    """Normalise a timestamp to a UTC-aware ISO 8601 string for SQLite.

    SQLite stores timestamps as text and attribution compares them as
    strings, so the representation has to be consistent or range queries
    silently misbehave. Naive input is treated as UTC.

    Raises on None: every caller passing a send timestamp is describing a
    real provider event, and quietly substituting now() there is exactly
    the bug this helper exists to prevent.
    """
    if value is None:
        raise ValueError(
            "A send timestamp is required; refusing to substitute the "
            "current clock for a provider send time."
        )
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return text
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Protocol (Abstract Base)
# ─────────────────────────────────────────────────────────────────────────────


class V2StateRepository(abc.ABC):
    """Domain-specific repository for V2 durable operational state.

    Implementations:
        SQLiteV2StateRepository  — uses the shared SQLite connection (current behavior)
        PostgresV2StateRepository — uses psycopg against a dedicated Postgres database

    Backend selection:
        DATABASE_URL present → Postgres
        DATABASE_URL absent  → SQLite
        Postgres configured but unavailable → fail closed (no silent SQLite fallback)
    """

    # ── Interventions ─────────────────────────────────────────────────────

    @abc.abstractmethod
    def save_intervention(self, intervention: Intervention) -> None:
        """Persist an intervention (insert or update)."""

    @abc.abstractmethod
    def get_intervention(self, intervention_id: str) -> Optional[Intervention]:
        """Retrieve a single intervention by ID."""

    @abc.abstractmethod
    def get_interventions_by_opportunity(self, opportunity_id: str) -> List[Intervention]:
        """All interventions for an opportunity, newest first."""

    @abc.abstractmethod
    def get_interventions_by_event(self, event_id: str) -> List[Intervention]:
        """All interventions for an event, newest first."""

    @abc.abstractmethod
    def list_interventions(self, include_terminal: bool = False) -> List[Intervention]:
        """List interventions, optionally including terminal states."""

    # ── Audit Log ─────────────────────────────────────────────────────────

    @abc.abstractmethod
    def append_audit(
        self,
        intervention_id: str,
        event_id: str,
        action: str,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        actor: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Append an audit log entry. Write-only, append-only."""

    @abc.abstractmethod
    def get_audit_log(self, intervention_id: str) -> List[Dict[str, Any]]:
        """Return all audit entries for an intervention, oldest first."""

    # ── Atomic Transition ─────────────────────────────────────────────────

    @abc.abstractmethod
    def save_intervention_with_audit(
        self,
        intervention: Intervention,
        intervention_id: str,
        event_id: str,
        action: str,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        actor: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Atomically save an intervention and append an audit entry.

        Both succeed or both fail. This is the key improvement over
        the current code where save() and log() are separate commits.
        On failure, both are rolled back.
        """

    # ── Campaign Sends ────────────────────────────────────────────────────

    @abc.abstractmethod
    def record_sends(
        self,
        intervention_id: str,
        campaign_draft_id: str,
        emails: List[str],
    ) -> None:
        """Record sent recipients for attribution tracking.

        Idempotent: duplicate (intervention_id, email) pairs are ignored.

        LEGACY. Stamps rows with the current clock, which is only correct
        when called at the moment of sending. The Phase 2 execution path
        does NOT use this — it goes through promote_attempt_recipients /
        finalize_send_locally, which carry the authoritative
        provider_sent_at so a delayed recovery cannot move the
        attribution window. Retained for pre-Phase-2 callers and tests.
        """

    @abc.abstractmethod
    def get_sends(self, intervention_id: str) -> List[Dict[str, Any]]:
        """Get all send records for an intervention.

        Returns list of dicts with 'email' and 'sent_at' keys.
        """

    # ── Learning Records ──────────────────────────────────────────────────

    @abc.abstractmethod
    def save_learning(self, record: Dict[str, Any]) -> None:
        """Persist a learning record. Upserts on intervention_id."""

    @abc.abstractmethod
    def get_learning(self, intervention_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a learning record by intervention ID."""

    # ── Suppression Sentinel ──────────────────────────────────────────────

    @abc.abstractmethod
    def get_suppression_sentinel(self) -> Optional[Dict[str, Any]]:
        """Read the suppression sync sentinel (single-row table)."""

    @abc.abstractmethod
    def upsert_suppression_sentinel(self, data: Dict[str, Any]) -> None:
        """Insert or update the suppression sync sentinel.

        `data` must include at minimum: last_synced_at, row_count, source.
        Other fields (empty_acknowledged, acknowledged_by, etc.) are optional
        and will be preserved if not provided during update.
        """

    @abc.abstractmethod
    def acknowledge_empty_suppressions(
        self, actor: str, reason: str
    ) -> Dict[str, Any]:
        """Record explicit acknowledgment that empty suppressions are legitimate.

        Returns acknowledgment details including expiry time.
        Raises ValueError if preconditions aren't met.
        """

    # ── Send Attempts ────────────────────────────────────────────────────

    @abc.abstractmethod
    def create_send_attempt(self, attempt: "SendAttempt") -> "SendAttempt":
        """Create a new send attempt (durable claim).

        The partial unique index on (intervention_id, execution_generation)
        prevents duplicate active claims at the database level.

        Returns the attempt with DB-assigned ID.
        Raises DuplicateClaimError if an active/successful attempt exists.
        """

    @abc.abstractmethod
    def update_send_attempt(self, attempt: "SendAttempt") -> None:
        """Persist updated send attempt state (checkpoint).

        Called after each provider step to durably record progress.
        """

    @abc.abstractmethod
    def get_send_attempt(self, attempt_id: int) -> Optional["SendAttempt"]:
        """Retrieve a send attempt by ID."""

    @abc.abstractmethod
    def get_send_attempts(self, intervention_id: str) -> List["SendAttempt"]:
        """All send attempts for an intervention, newest first."""

    @abc.abstractmethod
    def get_active_send_attempt(
        self, intervention_id: str, execution_generation: int
    ) -> Optional["SendAttempt"]:
        """Find the active (non-terminal, non-dry-run) attempt for this
        intervention+generation, if any."""

    @abc.abstractmethod
    def stage_attempt_recipients(
        self, attempt_id: int, emails: List[str]
    ) -> None:
        """Stage recipient emails for a send attempt BEFORE provider send.

        Idempotent: duplicate (attempt_id, email) pairs are ignored.
        """

    @abc.abstractmethod
    def get_attempt_recipients(self, attempt_id: int) -> List[str]:
        """Return staged recipient emails for a send attempt."""

    @abc.abstractmethod
    def promote_attempt_recipients(
        self, attempt_id: int, intervention_id: str, campaign_draft_id: str,
        sent_at: Any,
    ) -> None:
        """Copy staged recipients to v2_campaign_sends for attribution.

        Called only after confirmed_sent or reconciled_sent.
        Idempotent: uses INSERT ... ON CONFLICT DO NOTHING.

        `sent_at` is REQUIRED and must be the authoritative moment the
        PROVIDER sent — attempt.provider_sent_at. It is not optional and
        the repository never substitutes now(): these rows describe a
        historical event, and attribution reads them. Stamping them with
        the time bookkeeping happened would silently move the attribution
        window to whenever recovery ran.
        """

    @abc.abstractmethod
    def finalize_send_locally(
        self,
        attempt_id: int,
        intervention: Intervention,
        event_id: str,
        sent_at: Any,
        audit_action: str,
        audit_metadata: Optional[Dict[str, Any]] = None,
        from_status: Optional[str] = None,
        actor: str = "system",
    ) -> None:
        """Atomically converge local state after a PROVEN provider send.

        In ONE transaction:
          1. promote staged recipients → v2_campaign_sends (idempotent),
             stamped with `sent_at`, the authoritative provider send time
          2. upsert the intervention
          3. append the finalization audit entry

        Contacts no external provider — the send already happened. Safe to
        call repeatedly: recipient promotion conflicts are ignored and the
        intervention upsert is by primary key.

        Callers are responsible for not appending a duplicate audit entry;
        see ExecutionAdapter._finalization_audit_exists.
        """

    # ── Health ────────────────────────────────────────────────────────────

    @abc.abstractmethod
    def health_check(self) -> Dict[str, Any]:
        """Check repository connectivity and return status details."""


# ─────────────────────────────────────────────────────────────────────────────
# SQLite Implementation
# ─────────────────────────────────────────────────────────────────────────────


class SQLiteV2StateRepository(V2StateRepository):
    """SQLite-backed V2 state repository using the shared analytics connection.

    This implementation preserves the existing behavior where V2 tables live
    alongside analytics tables in the same SQLite database. It adds proper
    transaction boundaries for atomic operations (save + audit).

    The `db` parameter is the same Database instance used for analytics reads.
    """

    def __init__(self, db):
        self.db = db
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create V2 tables if they don't exist."""
        self.db.conn.executescript(INTERVENTION_SCHEMA)
        self.db.conn.executescript(AUDIT_LOG_SCHEMA)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        # Suppression sentinel schema — inline to avoid circular import
        self.db.conn.executescript("""
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
        """)
        # Send attempts schema — Phase 2 idempotent execution
        self.db.conn.executescript("""
            CREATE TABLE IF NOT EXISTS v2_send_attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                intervention_id TEXT NOT NULL,
                execution_generation INTEGER NOT NULL DEFAULT 1,
                attempt_status TEXT NOT NULL DEFAULT 'claimed',
                idempotency_key TEXT NOT NULL,
                audience_hash TEXT NOT NULL,
                provider_campaign_id TEXT,
                provider_tag TEXT,
                provider_segment_id INTEGER,
                audience_count INTEGER NOT NULL DEFAULT 0,
                claimed_at TEXT NOT NULL,
                provider_campaign_created_at TEXT,
                audience_configured_at TEXT,
                send_requested_at TEXT,
                completed_at TEXT,
                provider_sent_at TEXT,
                reconciled_at TEXT,
                reconciled_by TEXT,
                reconciliation_detail TEXT,
                error_message TEXT,
                error_detail TEXT,
                is_dry_run INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS v2_send_attempt_recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                send_attempt_id INTEGER NOT NULL,
                email TEXT NOT NULL,
                staged_at TEXT NOT NULL,
                UNIQUE(send_attempt_id, email)
            );
        """)
        # Additive column guard: CREATE TABLE IF NOT EXISTS will not add a
        # column to a table an earlier revision already created, so bring
        # pre-existing dev databases forward explicitly. Mirrors Postgres
        # migration 003.
        self._ensure_column("v2_send_attempts", "provider_sent_at", "TEXT")
        # One blocking attempt, including confirmed sends, per generation.
        # Enforced across connections/processes, not only by a Python precheck.
        # Existing conflicting claims deliberately fail migration closed.
        self.db.conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_send_attempt_blocking
            ON v2_send_attempts (intervention_id, execution_generation)
            WHERE is_dry_run = 0 AND attempt_status NOT IN
                ('failed_pre_send', 'reconciled_not_sent', 'cancelled')
        """)
        self.db.conn.commit()

    def _ensure_column(self, table: str, column: str, decl: str) -> None:
        """Add a column if the table exists and lacks it. No-op otherwise."""
        try:
            cols = {
                r[1] for r in
                self.db.conn.execute(f"PRAGMA table_info({table})").fetchall()
            }
        except Exception:
            return
        if not cols or column in cols:
            return
        try:
            self.db.conn.execute(
                f"ALTER TABLE {table} ADD COLUMN {column} {decl}"
            )
            log.info("Added missing column %s.%s", table, column)
        except Exception as e:  # pragma: no cover - defensive
            log.warning("Could not add column %s.%s: %s", table, column, e)

    # ── Interventions ─────────────────────────────────────────────────────

    def save_intervention(self, intervention: Intervention) -> None:
        evidence_json = (
            json.dumps(intervention.evidence)
            if isinstance(intervention.evidence, dict)
            else intervention.evidence
        )
        self.db.conn.execute(
            """INSERT OR REPLACE INTO interventions
               (id, opportunity_id, event_id, intervention_type, status,
                rationale, audience_definition, expected_revenue, expected_cost,
                expected_net_value, confidence, approval_required, created_at,
                approved_at, executed_at, measurement_window, actual_revenue,
                actual_cost, actual_net_value, outcome_status, evidence,
                campaign_draft_id, measurement_started_at, measurement_ends_at,
                attributed_orders, attributed_tickets, attributed_revenue, sent_count)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                intervention.id,
                intervention.opportunity_id,
                intervention.event_id,
                intervention.intervention_type,
                intervention.status.value,
                intervention.rationale,
                intervention.audience_definition,
                intervention.expected_revenue,
                intervention.expected_cost,
                intervention.expected_net_value,
                intervention.confidence,
                1 if intervention.approval_required else 0,
                intervention.created_at,
                intervention.approved_at,
                intervention.executed_at,
                intervention.measurement_window,
                intervention.actual_revenue,
                intervention.actual_cost,
                intervention.actual_net_value,
                intervention.outcome_status,
                evidence_json,
                intervention.campaign_draft_id,
                intervention.measurement_started_at,
                intervention.measurement_ends_at,
                intervention.attributed_orders,
                intervention.attributed_tickets,
                intervention.attributed_revenue,
                intervention.sent_count,
            ),
        )
        self.db.conn.commit()

    def get_intervention(self, intervention_id: str) -> Optional[Intervention]:
        row = self.db.conn.execute(
            "SELECT * FROM interventions WHERE id = ?", (intervention_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_intervention(row)

    def get_interventions_by_opportunity(self, opportunity_id: str) -> List[Intervention]:
        rows = self.db.conn.execute(
            "SELECT * FROM interventions WHERE opportunity_id = ? ORDER BY created_at DESC",
            (opportunity_id,),
        ).fetchall()
        return [self._row_to_intervention(r) for r in rows]

    def get_interventions_by_event(self, event_id: str) -> List[Intervention]:
        rows = self.db.conn.execute(
            "SELECT * FROM interventions WHERE event_id = ? ORDER BY created_at DESC",
            (event_id,),
        ).fetchall()
        return [self._row_to_intervention(r) for r in rows]

    def list_interventions(self, include_terminal: bool = False) -> List[Intervention]:
        if include_terminal:
            rows = self.db.conn.execute(
                "SELECT * FROM interventions ORDER BY created_at DESC"
            ).fetchall()
        else:
            terminal = tuple(s.value for s in TERMINAL_STATES)
            placeholders = ",".join("?" for _ in terminal)
            rows = self.db.conn.execute(
                f"SELECT * FROM interventions WHERE status NOT IN ({placeholders}) ORDER BY created_at DESC",
                terminal,
            ).fetchall()
        return [self._row_to_intervention(r) for r in rows]

    # ── Audit Log ─────────────────────────────────────────────────────────

    def append_audit(
        self,
        intervention_id: str,
        event_id: str,
        action: str,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        actor: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        meta_json = json.dumps(metadata) if metadata else "{}"
        self.db.conn.execute(
            """INSERT INTO intervention_audit_log
               (intervention_id, event_id, action, from_status, to_status,
                actor, timestamp, metadata, error)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                intervention_id,
                event_id,
                action,
                from_status,
                to_status,
                actor,
                datetime.now(timezone.utc).isoformat(),
                meta_json,
                error,
            ),
        )
        self.db.conn.commit()

    def get_audit_log(self, intervention_id: str) -> List[Dict[str, Any]]:
        rows = self.db.conn.execute(
            "SELECT * FROM intervention_audit_log WHERE intervention_id = ? ORDER BY id ASC",
            (intervention_id,),
        ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            if isinstance(d.get("metadata"), str):
                try:
                    d["metadata"] = json.loads(d["metadata"])
                except (json.JSONDecodeError, TypeError):
                    d["metadata"] = {}
            result.append(d)
        return result

    # ── Atomic Transition ─────────────────────────────────────────────────

    # Single definition of the intervention upsert, shared by
    # save_intervention_with_audit and finalize_send_locally so the two
    # transactional paths cannot drift apart.
    _INTERVENTION_UPSERT_SQL = """INSERT OR REPLACE INTO interventions
                   (id, opportunity_id, event_id, intervention_type, status,
                    rationale, audience_definition, expected_revenue, expected_cost,
                    expected_net_value, confidence, approval_required, created_at,
                    approved_at, executed_at, measurement_window, actual_revenue,
                    actual_cost, actual_net_value, outcome_status, evidence,
                    campaign_draft_id, measurement_started_at, measurement_ends_at,
                    attributed_orders, attributed_tickets, attributed_revenue, sent_count)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""

    @staticmethod
    def _intervention_params(intervention: Intervention, evidence_json):
        return (
            intervention.id,
            intervention.opportunity_id,
            intervention.event_id,
            intervention.intervention_type,
            intervention.status.value,
            intervention.rationale,
            intervention.audience_definition,
            intervention.expected_revenue,
            intervention.expected_cost,
            intervention.expected_net_value,
            intervention.confidence,
            1 if intervention.approval_required else 0,
            intervention.created_at,
            intervention.approved_at,
            intervention.executed_at,
            intervention.measurement_window,
            intervention.actual_revenue,
            intervention.actual_cost,
            intervention.actual_net_value,
            intervention.outcome_status,
            evidence_json,
            intervention.campaign_draft_id,
            intervention.measurement_started_at,
            intervention.measurement_ends_at,
            intervention.attributed_orders,
            intervention.attributed_tickets,
            intervention.attributed_revenue,
            intervention.sent_count,
        )

    def save_intervention_with_audit(
        self,
        intervention: Intervention,
        intervention_id: str,
        event_id: str,
        action: str,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        actor: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Atomically save intervention + append audit in one transaction."""
        evidence_json = (
            json.dumps(intervention.evidence)
            if isinstance(intervention.evidence, dict)
            else intervention.evidence
        )
        meta_json = json.dumps(metadata) if metadata else "{}"
        try:
            self.db.conn.execute(
                self._INTERVENTION_UPSERT_SQL,
                self._intervention_params(intervention, evidence_json),
            )
            self.db.conn.execute(
                """INSERT INTO intervention_audit_log
                   (intervention_id, event_id, action, from_status, to_status,
                    actor, timestamp, metadata, error)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    intervention_id,
                    event_id,
                    action,
                    from_status,
                    to_status,
                    actor,
                    datetime.now(timezone.utc).isoformat(),
                    meta_json,
                    error,
                ),
            )
            self.db.conn.commit()
        except Exception:
            self.db.conn.rollback()
            raise

    # ── Campaign Sends ────────────────────────────────────────────────────

    def record_sends(
        self,
        intervention_id: str,
        campaign_draft_id: str,
        emails: List[str],
    ) -> None:
        # LEGACY path — current clock. Not used by Phase 2 execution;
        # see promote_attempt_recipients for the authoritative-timestamp
        # version that attribution depends on.
        now = datetime.now(timezone.utc).isoformat()
        try:
            for email in emails:
                self.db.conn.execute(
                    """INSERT OR IGNORE INTO v2_campaign_sends
                       (intervention_id, campaign_draft_id, email, sent_at)
                       VALUES (?,?,?,?)""",
                    (intervention_id, campaign_draft_id, email, now),
                )
            self.db.conn.commit()
        except Exception:
            self.db.conn.rollback()
            raise

    def get_sends(self, intervention_id: str) -> List[Dict[str, Any]]:
        rows = self.db.conn.execute(
            """SELECT email, sent_at FROM v2_campaign_sends
               WHERE intervention_id = ? ORDER BY sent_at ASC, email ASC""",
            (intervention_id,),
        ).fetchall()
        return [{"email": r["email"], "sent_at": r["sent_at"]} for r in rows]

    # ── Learning Records ──────────────────────────────────────────────────

    def save_learning(self, record: Dict[str, Any]) -> None:
        self.db.conn.execute(
            """INSERT OR REPLACE INTO v2_learning_records
               (intervention_id, intervention_type, event_id, event_type, city,
                predicted_revenue, attributed_revenue, prediction_error,
                audience_count, sent_count, attributed_orders, attributed_tickets,
                actual_conversion_rate, conversion_assumptions, confidence,
                measurement_window_days, measurement_started_at, measurement_ended_at,
                created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                record["intervention_id"],
                record["intervention_type"],
                record["event_id"],
                record.get("event_type", ""),
                record.get("city", ""),
                record.get("predicted_revenue", 0),
                record.get("attributed_revenue", 0),
                record.get("prediction_error", 0),
                record.get("audience_count", 0),
                record.get("sent_count", 0),
                record.get("attributed_orders", 0),
                record.get("attributed_tickets", 0),
                record.get("actual_conversion_rate"),
                json.dumps(record.get("conversion_assumptions", {})),
                record.get("confidence", 0),
                record.get("measurement_window_days", 7),
                record.get("measurement_started_at"),
                record.get("measurement_ended_at"),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        self.db.conn.commit()

    def get_learning(self, intervention_id: str) -> Optional[Dict[str, Any]]:
        row = self.db.conn.execute(
            "SELECT * FROM v2_learning_records WHERE intervention_id = ?",
            (intervention_id,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if isinstance(d.get("conversion_assumptions"), str):
            try:
                d["conversion_assumptions"] = json.loads(d["conversion_assumptions"])
            except (json.JSONDecodeError, TypeError):
                d["conversion_assumptions"] = {}
        return d

    # ── Suppression Sentinel ──────────────────────────────────────────────

    def get_suppression_sentinel(self) -> Optional[Dict[str, Any]]:
        try:
            row = self.db.conn.execute(
                "SELECT * FROM v2_suppression_sync WHERE id = 1"
            ).fetchone()
            return dict(row) if row else None
        except Exception as e:
            log.error(f"Cannot read suppression sentinel: {e}")
            return None

    def upsert_suppression_sentinel(self, data: Dict[str, Any]) -> None:
        """Upsert the single-row suppression sentinel.

        Supports partial updates: only the keys present in `data` are changed.
        Required for INSERT: last_synced_at, row_count, source.
        """
        now = data.get("last_synced_at", datetime.now(timezone.utc).isoformat())
        row_count = data.get("row_count", 0)
        source = data.get("source", "unknown")

        # Build SET clause for ON CONFLICT — only update keys that are in data
        set_parts = [
            "last_synced_at = excluded.last_synced_at",
            "row_count = excluded.row_count",
            "source = excluded.source",
        ]
        insert_cols = [
            "id", "last_synced_at", "row_count", "source",
        ]
        insert_vals: list = [1, now, row_count, source]

        # Optional fields
        optional_fields = {
            "empty_acknowledged": "empty_acknowledged",
            "acknowledged_by": "acknowledged_by",
            "acknowledged_at": "acknowledged_at",
            "acknowledged_reason": "acknowledged_reason",
            "last_full_refresh_at": "last_full_refresh_at",
            "last_mutation_at": "last_mutation_at",
            "last_full_refresh_source": "last_full_refresh_source",
            "last_mutation_source": "last_mutation_source",
        }
        for key, col in optional_fields.items():
            if key in data:
                insert_cols.append(col)
                insert_vals.append(data[key])
                set_parts.append(f"{col} = excluded.{col}")

        cols_str = ", ".join(insert_cols)
        placeholders = ", ".join("?" for _ in insert_vals)
        sets_str = ", ".join(set_parts)

        self.db.conn.execute(
            f"""INSERT INTO v2_suppression_sync ({cols_str})
                VALUES ({placeholders})
                ON CONFLICT(id) DO UPDATE SET {sets_str}""",
            insert_vals,
        )
        self.db.conn.commit()

    def acknowledge_empty_suppressions(
        self, actor: str, reason: str
    ) -> Dict[str, Any]:
        if not actor or not actor.strip():
            raise ValueError("Acknowledgment requires a named actor")
        if not reason or not reason.strip():
            raise ValueError("Acknowledgment requires a reason")

        now = datetime.now(timezone.utc)

        sentinel = self.get_suppression_sentinel()
        if sentinel is None:
            raise ValueError(
                "Cannot acknowledge empty suppressions: no sync has ever occurred. "
                "Run a suppression sync first."
            )
        if sentinel.get("row_count", 0) > 0:
            raise ValueError(
                f"Suppression list has {sentinel['row_count']} rows — acknowledgment "
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

    # ── Send Attempts ────────────────────────────────────────────────────

    def create_send_attempt(self, attempt: "SendAttempt") -> "SendAttempt":
        from send_attempt_model import (
            SendAttemptStatus, DuplicateClaimError,
            ACTIVE_ATTEMPT_STATES, SUCCESSFUL_SEND_STATES,
        )
        # Check for existing active or successful attempt
        if not attempt.is_dry_run:
            existing = self.db.conn.execute(
                """SELECT id, attempt_status FROM v2_send_attempts
                   WHERE intervention_id = ? AND execution_generation = ?
                   AND is_dry_run = 0
                   AND attempt_status NOT IN (?, ?, ?, ?)""",
                (attempt.intervention_id, attempt.execution_generation,
                 'failed_pre_send', 'reconciled_not_sent', 'cancelled',
                 # terminal-but-ok states checked separately below
                 '__none__'),
            ).fetchall()
            for row in existing:
                status = SendAttemptStatus(row['attempt_status'])
                if status in ACTIVE_ATTEMPT_STATES or status in SUCCESSFUL_SEND_STATES:
                    raise DuplicateClaimError(
                        f"Cannot create claim: existing attempt {row['id']} "
                        f"in state '{row['attempt_status']}'"
                    )

        now = attempt.claimed_at or datetime.now(timezone.utc)
        try:
            cursor = self.db.conn.execute(
                """INSERT INTO v2_send_attempts
                   (intervention_id, execution_generation, attempt_status,
                    idempotency_key, audience_hash, provider_campaign_id,
                    provider_tag, provider_segment_id, audience_count,
                    claimed_at, is_dry_run)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (attempt.intervention_id, attempt.execution_generation,
                 attempt.attempt_status.value, attempt.idempotency_key,
                 attempt.audience_hash, attempt.provider_campaign_id,
                 attempt.provider_tag, attempt.provider_segment_id,
                 attempt.audience_count, now.isoformat(),
                 1 if attempt.is_dry_run else 0),
            )
            self.db.conn.commit()
        except sqlite3.IntegrityError as exc:
            self.db.conn.rollback()
            if 'UNIQUE constraint failed' in str(exc):
                raise DuplicateClaimError('A blocking send claim already exists') from exc
            raise
        attempt.id = cursor.lastrowid
        attempt.claimed_at = now
        return attempt

    def update_send_attempt(self, attempt: "SendAttempt") -> None:
        recon_detail = (
            json.dumps(attempt.reconciliation_detail)
            if attempt.reconciliation_detail else None
        )
        err_detail = (
            json.dumps(attempt.error_detail)
            if attempt.error_detail else None
        )

        def _ts(val):
            """Serialize timestamp — handles both datetime and string."""
            if val is None:
                return None
            if isinstance(val, str):
                return val
            return val.isoformat()

        self.db.conn.execute(
            """UPDATE v2_send_attempts SET
                   attempt_status = ?,
                   provider_campaign_id = ?,
                   provider_tag = ?,
                   provider_segment_id = ?,
                   audience_count = ?,
                   provider_campaign_created_at = ?,
                   audience_configured_at = ?,
                   send_requested_at = ?,
                   completed_at = ?,
                   provider_sent_at = ?,
                   reconciled_at = ?,
                   reconciled_by = ?,
                   reconciliation_detail = ?,
                   error_message = ?,
                   error_detail = ?
               WHERE id = ?""",
            (attempt.attempt_status.value,
             attempt.provider_campaign_id, attempt.provider_tag,
             attempt.provider_segment_id, attempt.audience_count,
             _ts(attempt.provider_campaign_created_at),
             _ts(attempt.audience_configured_at),
             _ts(attempt.send_requested_at),
             _ts(attempt.completed_at),
             _ts(attempt.provider_sent_at),
             _ts(attempt.reconciled_at),
             attempt.reconciled_by, recon_detail,
             attempt.error_message, err_detail,
             attempt.id),
        )
        self.db.conn.commit()

    def get_send_attempt(self, attempt_id: int) -> Optional["SendAttempt"]:
        row = self.db.conn.execute(
            "SELECT * FROM v2_send_attempts WHERE id = ?", (attempt_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_send_attempt(dict(row))

    def get_send_attempts(self, intervention_id: str) -> List["SendAttempt"]:
        rows = self.db.conn.execute(
            """SELECT * FROM v2_send_attempts
               WHERE intervention_id = ?
               ORDER BY id DESC""",
            (intervention_id,),
        ).fetchall()
        return [self._row_to_send_attempt(dict(r)) for r in rows]

    def get_active_send_attempt(
        self, intervention_id: str, execution_generation: int
    ) -> Optional["SendAttempt"]:
        row = self.db.conn.execute(
            """SELECT * FROM v2_send_attempts
               WHERE intervention_id = ? AND execution_generation = ?
               AND is_dry_run = 0
               AND attempt_status NOT IN (
                   'confirmed_sent', 'failed_pre_send',
                   'reconciled_sent', 'reconciled_not_sent', 'cancelled'
               )
               ORDER BY id DESC LIMIT 1""",
            (intervention_id, execution_generation),
        ).fetchone()
        if not row:
            return None
        return self._row_to_send_attempt(dict(row))

    def stage_attempt_recipients(
        self, attempt_id: int, emails: List[str]
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        for email in emails:
            self.db.conn.execute(
                """INSERT OR IGNORE INTO v2_send_attempt_recipients
                   (send_attempt_id, email, staged_at) VALUES (?, ?, ?)""",
                (attempt_id, email.lower().strip(), now),
            )
        self.db.conn.commit()

    def get_attempt_recipients(self, attempt_id: int) -> List[str]:
        rows = self.db.conn.execute(
            """SELECT email FROM v2_send_attempt_recipients
               WHERE send_attempt_id = ? ORDER BY email""",
            (attempt_id,),
        ).fetchall()
        return [r['email'] for r in rows]

    def promote_attempt_recipients(
        self, attempt_id: int, intervention_id: str, campaign_draft_id: str,
        sent_at: Any,
    ) -> None:
        ts = _iso_utc(sent_at)
        emails = self.get_attempt_recipients(attempt_id)
        for email in emails:
            self.db.conn.execute(
                """INSERT OR IGNORE INTO v2_campaign_sends
                   (intervention_id, campaign_draft_id, email, sent_at)
                   VALUES (?, ?, ?, ?)""",
                (intervention_id, campaign_draft_id, email, ts),
            )
        self.db.conn.commit()

    def finalize_send_locally(
        self,
        attempt_id: int,
        intervention: Intervention,
        event_id: str,
        sent_at: Any,
        audit_action: str,
        audit_metadata: Optional[Dict[str, Any]] = None,
        from_status: Optional[str] = None,
        actor: str = "system",
    ) -> None:
        """Promote recipients + save intervention + audit in one transaction."""
        # Two distinct clocks, deliberately kept apart:
        #   sent_ts — when the PROVIDER sent. Attribution reads this.
        #   now     — when this bookkeeping ran. Audit only.
        sent_ts = _iso_utc(sent_at)
        now = datetime.now(timezone.utc).isoformat()
        emails = self.get_attempt_recipients(attempt_id)
        meta_json = json.dumps(audit_metadata) if audit_metadata else "{}"
        evidence_json = (
            json.dumps(intervention.evidence)
            if isinstance(intervention.evidence, dict)
            else intervention.evidence
        )
        try:
            for email in emails:
                # ON CONFLICT DO NOTHING semantics: an already-promoted
                # recipient keeps its ORIGINAL sent_at, so replaying
                # finalization never rewrites attribution timestamps.
                self.db.conn.execute(
                    """INSERT OR IGNORE INTO v2_campaign_sends
                       (intervention_id, campaign_draft_id, email, sent_at)
                       VALUES (?, ?, ?, ?)""",
                    (intervention.id, intervention.campaign_draft_id,
                     email, sent_ts),
                )
            self.db.conn.execute(
                self._INTERVENTION_UPSERT_SQL,
                self._intervention_params(intervention, evidence_json),
            )
            self.db.conn.execute(
                """INSERT INTO intervention_audit_log
                   (intervention_id, event_id, action, from_status, to_status,
                    actor, timestamp, metadata, error)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (intervention.id, event_id, audit_action, from_status,
                 intervention.status.value, actor, now, meta_json, None),
            )
            self.db.conn.commit()
        except Exception:
            self.db.conn.rollback()
            raise

    # ── Health ────────────────────────────────────────────────────────────

    def health_check(self) -> Dict[str, Any]:
        try:
            self.db.conn.execute("SELECT 1").fetchone()
            # Verify V2 tables exist
            tables = []
            for tbl in _V2_EXPECTED_TABLES:
                try:
                    self.db.conn.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                    tables.append(tbl)
                except Exception:
                    pass
            missing = [t for t in _V2_EXPECTED_TABLES if t not in tables]
            ready = not missing
            result = {
                "status": "ok" if ready else "degraded",
                "backend": "sqlite",
                "tables_found": tables,
                "tables_expected": len(_V2_EXPECTED_TABLES),
                "tables_ok": ready,
                "ready": ready,
            }
            if missing:
                result["missing_tables"] = missing
                result["error"] = (
                    f"missing operational tables: {', '.join(missing)}")
            return result
        except Exception as e:
            return {
                "status": "degraded", "backend": "sqlite",
                "ready": False, "error": str(e),
            }

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _row_to_send_attempt(d: dict) -> "SendAttempt":
        """Convert a SQLite row dict to a SendAttempt dataclass."""
        from send_attempt_model import SendAttempt, SendAttemptStatus
        recon = d.get("reconciliation_detail")
        if isinstance(recon, str):
            try:
                recon = json.loads(recon)
            except (json.JSONDecodeError, TypeError):
                recon = None
        err_detail = d.get("error_detail")
        if isinstance(err_detail, str):
            try:
                err_detail = json.loads(err_detail)
            except (json.JSONDecodeError, TypeError):
                err_detail = None
        return SendAttempt(
            id=d["id"],
            intervention_id=d["intervention_id"],
            execution_generation=int(d["execution_generation"]),
            attempt_status=SendAttemptStatus(d["attempt_status"]),
            idempotency_key=d["idempotency_key"],
            audience_hash=d["audience_hash"],
            provider_campaign_id=d.get("provider_campaign_id"),
            provider_tag=d.get("provider_tag"),
            provider_segment_id=int(d["provider_segment_id"]) if d.get("provider_segment_id") is not None else None,
            audience_count=int(d.get("audience_count") or 0),
            claimed_at=d.get("claimed_at"),
            provider_campaign_created_at=d.get("provider_campaign_created_at"),
            audience_configured_at=d.get("audience_configured_at"),
            send_requested_at=d.get("send_requested_at"),
            provider_sent_at=d.get("provider_sent_at"),
            completed_at=d.get("completed_at"),
            reconciled_at=d.get("reconciled_at"),
            reconciled_by=d.get("reconciled_by"),
            reconciliation_detail=recon,
            error_message=d.get("error_message"),
            error_detail=err_detail,
            is_dry_run=bool(d.get("is_dry_run", 0)),
        )

    @staticmethod
    def _row_to_intervention(row) -> Intervention:
        """Convert a SQLite row to an Intervention dataclass."""
        d = dict(row)
        evidence = d.get("evidence", "{}")
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except (json.JSONDecodeError, TypeError):
                evidence = {}
        return Intervention(
            id=d["id"],
            opportunity_id=d["opportunity_id"],
            event_id=d["event_id"],
            intervention_type=d["intervention_type"],
            status=InterventionStatus(d["status"]),
            rationale=d.get("rationale", ""),
            audience_definition=d.get("audience_definition", ""),
            expected_revenue=float(d.get("expected_revenue") or 0),
            expected_cost=float(d.get("expected_cost") or 0),
            expected_net_value=float(d.get("expected_net_value") or 0),
            confidence=float(d.get("confidence") or 0),
            approval_required=bool(d.get("approval_required", 1)),
            created_at=d.get("created_at", ""),
            approved_at=d.get("approved_at"),
            executed_at=d.get("executed_at"),
            measurement_window=int(d.get("measurement_window") or 14),
            actual_revenue=float(d["actual_revenue"]) if d.get("actual_revenue") is not None else None,
            actual_cost=float(d["actual_cost"]) if d.get("actual_cost") is not None else None,
            actual_net_value=float(d["actual_net_value"]) if d.get("actual_net_value") is not None else None,
            outcome_status=d.get("outcome_status"),
            evidence=evidence,
            campaign_draft_id=d.get("campaign_draft_id"),
            measurement_started_at=d.get("measurement_started_at"),
            measurement_ends_at=d.get("measurement_ends_at"),
            attributed_orders=int(d["attributed_orders"]) if d.get("attributed_orders") is not None else None,
            attributed_tickets=int(d["attributed_tickets"]) if d.get("attributed_tickets") is not None else None,
            attributed_revenue=float(d["attributed_revenue"]) if d.get("attributed_revenue") is not None else None,
            sent_count=int(d["sent_count"]) if d.get("sent_count") is not None else None,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Postgres Implementation
# ─────────────────────────────────────────────────────────────────────────────


class PostgresV2StateRepository(V2StateRepository):
    """Postgres-backed V2 state repository using psycopg3.

    Uses a dedicated Postgres database for V2 durable operational state.
    All timestamps are TIMESTAMPTZ (UTC aware). Evidence and metadata
    fields use JSONB. Parameterization uses psycopg's native %(name)s
    or %s placeholders.

    Connection management:
        The `conninfo` (DSN/URL) is stored and connections are created
        per-operation to avoid long-lived connections in the web process.
        For transaction-critical operations (save_intervention_with_audit),
        a single connection + transaction is used.

    Fail-closed:
        If Postgres is unreachable, methods raise — no silent SQLite fallback.
    """

    def __init__(self, conninfo: str, *, autocommit: bool = False):
        """Initialize with a Postgres connection string.

        Args:
            conninfo: psycopg connection string (DSN or URL).
                      e.g. "postgresql://user:pw@host:5432/dbname"
            autocommit: If True, each statement auto-commits (for migration).
        """
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError:
            raise ImportError(
                "psycopg (v3) is required for PostgresV2StateRepository. "
                "Install with: pip install 'psycopg[binary]'"
            )
        self._conninfo = conninfo
        self._autocommit = autocommit
        self._psycopg = psycopg
        self._dict_row = dict_row

    def _connect(self):
        """Create a new connection with dict row factory."""
        return self._psycopg.connect(
            self._conninfo,
            autocommit=self._autocommit,
            row_factory=self._dict_row,
        )

    # ── Timestamp helpers ────────────────────────────────────────────────

    @staticmethod
    def _to_utc(val) -> Optional[datetime]:
        """Convert a value to a UTC-aware datetime for Postgres TIMESTAMPTZ.

        Handles:
            - None → None
            - datetime (aware) → as-is
            - datetime (naive) → assume UTC
            - ISO 8601 string → parse and ensure UTC
        """
        if val is None:
            return None
        if isinstance(val, datetime):
            if val.tzinfo is None:
                return val.replace(tzinfo=timezone.utc)
            return val
        if isinstance(val, str):
            if not val or val.strip() == "":
                return None
            try:
                dt = datetime.fromisoformat(val)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except (ValueError, TypeError):
                return None
        return None

    @staticmethod
    def _ts_to_iso(val) -> Optional[str]:
        """Convert a datetime from Postgres back to ISO string for the Intervention dataclass."""
        if val is None:
            return None
        if isinstance(val, datetime):
            return val.isoformat()
        return str(val)

    @staticmethod
    def _ensure_dict(val) -> dict:
        """Ensure a value is a dict (handles JSONB auto-deserialization and strings)."""
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

    # ── Interventions ─────────────────────────────────────────────────────

    _INTERVENTION_UPSERT = """
        INSERT INTO interventions
           (id, opportunity_id, event_id, intervention_type, status,
            rationale, audience_definition, expected_revenue, expected_cost,
            expected_net_value, confidence, approval_required, created_at,
            approved_at, executed_at, measurement_window, actual_revenue,
            actual_cost, actual_net_value, outcome_status, evidence,
            campaign_draft_id, measurement_started_at, measurement_ends_at,
            attributed_orders, attributed_tickets, attributed_revenue, sent_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (id) DO UPDATE SET
            opportunity_id = EXCLUDED.opportunity_id,
            event_id = EXCLUDED.event_id,
            intervention_type = EXCLUDED.intervention_type,
            status = EXCLUDED.status,
            rationale = EXCLUDED.rationale,
            audience_definition = EXCLUDED.audience_definition,
            expected_revenue = EXCLUDED.expected_revenue,
            expected_cost = EXCLUDED.expected_cost,
            expected_net_value = EXCLUDED.expected_net_value,
            confidence = EXCLUDED.confidence,
            approval_required = EXCLUDED.approval_required,
            created_at = EXCLUDED.created_at,
            approved_at = EXCLUDED.approved_at,
            executed_at = EXCLUDED.executed_at,
            measurement_window = EXCLUDED.measurement_window,
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
            sent_count = EXCLUDED.sent_count
    """

    def _intervention_params(self, intervention: Intervention) -> tuple:
        """Build parameter tuple for intervention upsert."""
        from psycopg.types.json import Json

        evidence = (
            intervention.evidence
            if isinstance(intervention.evidence, dict)
            else self._ensure_dict(intervention.evidence)
        )
        return (
            intervention.id,
            intervention.opportunity_id,
            intervention.event_id,
            intervention.intervention_type,
            intervention.status.value,
            intervention.rationale,
            intervention.audience_definition,
            intervention.expected_revenue,
            intervention.expected_cost,
            intervention.expected_net_value,
            intervention.confidence,
            intervention.approval_required,  # Postgres BOOLEAN natively
            self._to_utc(intervention.created_at),
            self._to_utc(intervention.approved_at),
            self._to_utc(intervention.executed_at),
            intervention.measurement_window,
            intervention.actual_revenue,
            intervention.actual_cost,
            intervention.actual_net_value,
            intervention.outcome_status,
            Json(evidence),
            intervention.campaign_draft_id,
            self._to_utc(intervention.measurement_started_at),
            self._to_utc(intervention.measurement_ends_at),
            intervention.attributed_orders,
            intervention.attributed_tickets,
            intervention.attributed_revenue,
            intervention.sent_count,
        )

    def save_intervention(self, intervention: Intervention) -> None:
        with self._connect() as conn:
            conn.execute(self._INTERVENTION_UPSERT, self._intervention_params(intervention))
            conn.commit()

    def get_intervention(self, intervention_id: str) -> Optional[Intervention]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM interventions WHERE id = %s", (intervention_id,)
            ).fetchone()
        if not row:
            return None
        return self._pg_row_to_intervention(row)

    def get_interventions_by_opportunity(self, opportunity_id: str) -> List[Intervention]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM interventions WHERE opportunity_id = %s ORDER BY created_at DESC",
                (opportunity_id,),
            ).fetchall()
        return [self._pg_row_to_intervention(r) for r in rows]

    def get_interventions_by_event(self, event_id: str) -> List[Intervention]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM interventions WHERE event_id = %s ORDER BY created_at DESC",
                (event_id,),
            ).fetchall()
        return [self._pg_row_to_intervention(r) for r in rows]

    def list_interventions(self, include_terminal: bool = False) -> List[Intervention]:
        with self._connect() as conn:
            if include_terminal:
                rows = conn.execute(
                    "SELECT * FROM interventions ORDER BY created_at DESC"
                ).fetchall()
            else:
                terminal = tuple(s.value for s in TERMINAL_STATES)
                rows = conn.execute(
                    "SELECT * FROM interventions WHERE status != ALL(%s) ORDER BY created_at DESC",
                    (list(terminal),),
                ).fetchall()
        return [self._pg_row_to_intervention(r) for r in rows]

    # ── Audit Log ─────────────────────────────────────────────────────────

    def append_audit(
        self,
        intervention_id: str,
        event_id: str,
        action: str,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        actor: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        from psycopg.types.json import Json

        with self._connect() as conn:
            conn.execute(
                """INSERT INTO intervention_audit_log
                   (intervention_id, event_id, action, from_status, to_status,
                    actor, timestamp, metadata, error)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    intervention_id,
                    event_id,
                    action,
                    from_status,
                    to_status,
                    actor,
                    datetime.now(timezone.utc),
                    Json(metadata or {}),
                    error,
                ),
            )
            conn.commit()

    def get_audit_log(self, intervention_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM intervention_audit_log WHERE intervention_id = %s ORDER BY id ASC",
                (intervention_id,),
            ).fetchall()
        result = []
        for row in rows:
            d = dict(row)
            d["metadata"] = self._ensure_dict(d.get("metadata"))
            # Convert timestamp to ISO string for API compatibility
            if isinstance(d.get("timestamp"), datetime):
                d["timestamp"] = d["timestamp"].isoformat()
            result.append(d)
        return result

    # ── Atomic Transition ─────────────────────────────────────────────────

    def save_intervention_with_audit(
        self,
        intervention: Intervention,
        intervention_id: str,
        event_id: str,
        action: str,
        from_status: Optional[str] = None,
        to_status: Optional[str] = None,
        actor: str = "system",
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> None:
        """Atomically save intervention + append audit in one Postgres transaction."""
        from psycopg.types.json import Json

        conn = self._connect()
        try:
            conn.execute(self._INTERVENTION_UPSERT, self._intervention_params(intervention))
            conn.execute(
                """INSERT INTO intervention_audit_log
                   (intervention_id, event_id, action, from_status, to_status,
                    actor, timestamp, metadata, error)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    intervention_id,
                    event_id,
                    action,
                    from_status,
                    to_status,
                    actor,
                    datetime.now(timezone.utc),
                    Json(metadata or {}),
                    error,
                ),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Campaign Sends ────────────────────────────────────────────────────

    def record_sends(
        self,
        intervention_id: str,
        campaign_draft_id: str,
        emails: List[str],
    ) -> None:
        # LEGACY path — current clock. Not used by Phase 2 execution;
        # see promote_attempt_recipients for the authoritative-timestamp
        # version that attribution depends on.
        now = datetime.now(timezone.utc)
        with self._connect() as conn:
            try:
                for email in emails:
                    conn.execute(
                        """INSERT INTO v2_campaign_sends
                           (intervention_id, campaign_draft_id, email, sent_at)
                           VALUES (%s,%s,%s,%s)
                           ON CONFLICT (intervention_id, email) DO NOTHING""",
                        (intervention_id, campaign_draft_id, email, now),
                    )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def get_sends(self, intervention_id: str) -> List[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT email, sent_at FROM v2_campaign_sends
                   WHERE intervention_id = %s ORDER BY sent_at ASC, email ASC""",
                (intervention_id,),
            ).fetchall()
        return [
            {"email": r["email"], "sent_at": self._ts_to_iso(r["sent_at"])}
            for r in rows
        ]

    # ── Learning Records ──────────────────────────────────────────────────

    def save_learning(self, record: Dict[str, Any]) -> None:
        from psycopg.types.json import Json

        with self._connect() as conn:
            conn.execute(
                """INSERT INTO v2_learning_records
                   (intervention_id, intervention_type, event_id, event_type, city,
                    predicted_revenue, attributed_revenue, prediction_error,
                    audience_count, sent_count, attributed_orders, attributed_tickets,
                    actual_conversion_rate, conversion_assumptions, confidence,
                    measurement_window_days, measurement_started_at, measurement_ended_at,
                    created_at)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (intervention_id) DO UPDATE SET
                    intervention_type = EXCLUDED.intervention_type,
                    event_id = EXCLUDED.event_id,
                    event_type = EXCLUDED.event_type,
                    city = EXCLUDED.city,
                    predicted_revenue = EXCLUDED.predicted_revenue,
                    attributed_revenue = EXCLUDED.attributed_revenue,
                    prediction_error = EXCLUDED.prediction_error,
                    audience_count = EXCLUDED.audience_count,
                    sent_count = EXCLUDED.sent_count,
                    attributed_orders = EXCLUDED.attributed_orders,
                    attributed_tickets = EXCLUDED.attributed_tickets,
                    actual_conversion_rate = EXCLUDED.actual_conversion_rate,
                    conversion_assumptions = EXCLUDED.conversion_assumptions,
                    confidence = EXCLUDED.confidence,
                    measurement_window_days = EXCLUDED.measurement_window_days,
                    measurement_started_at = EXCLUDED.measurement_started_at,
                    measurement_ended_at = EXCLUDED.measurement_ended_at,
                    created_at = EXCLUDED.created_at""",
                (
                    record["intervention_id"],
                    record["intervention_type"],
                    record["event_id"],
                    record.get("event_type", ""),
                    record.get("city", ""),
                    record.get("predicted_revenue", 0),
                    record.get("attributed_revenue", 0),
                    record.get("prediction_error", 0),
                    record.get("audience_count", 0),
                    record.get("sent_count", 0),
                    record.get("attributed_orders", 0),
                    record.get("attributed_tickets", 0),
                    record.get("actual_conversion_rate"),
                    Json(record.get("conversion_assumptions", {})),
                    record.get("confidence", 0),
                    record.get("measurement_window_days", 7),
                    self._to_utc(record.get("measurement_started_at")),
                    self._to_utc(record.get("measurement_ended_at")),
                    datetime.now(timezone.utc),
                ),
            )
            conn.commit()

    def get_learning(self, intervention_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM v2_learning_records WHERE intervention_id = %s",
                (intervention_id,),
            ).fetchone()
        if not row:
            return None
        d = dict(row)
        d["conversion_assumptions"] = self._ensure_dict(d.get("conversion_assumptions"))
        # Convert timestamps to ISO strings for API compatibility
        for ts_field in ("measurement_started_at", "measurement_ended_at", "created_at"):
            d[ts_field] = self._ts_to_iso(d.get(ts_field))
        return d

    # ── Suppression Sentinel ──────────────────────────────────────────────

    def get_suppression_sentinel(self) -> Optional[Dict[str, Any]]:
        try:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM v2_suppression_sync WHERE id = 1"
                ).fetchone()
            if not row:
                return None
            d = dict(row)
            # Convert all timestamps to ISO strings
            ts_fields = [
                "last_synced_at", "acknowledged_at", "last_full_refresh_at",
                "last_mutation_at",
            ]
            for f in ts_fields:
                d[f] = self._ts_to_iso(d.get(f))
            # Convert boolean
            d["empty_acknowledged"] = (
                int(d["empty_acknowledged"]) if isinstance(d["empty_acknowledged"], bool)
                else d["empty_acknowledged"]
            )
            return d
        except Exception as e:
            log.error(f"Cannot read suppression sentinel: {e}")
            return None

    def upsert_suppression_sentinel(self, data: Dict[str, Any]) -> None:
        now_ts = self._to_utc(data.get("last_synced_at")) or datetime.now(timezone.utc)
        row_count = data.get("row_count", 0)
        source = data.get("source", "unknown")

        # Core columns always set
        cols = ["id", "last_synced_at", "row_count", "source"]
        vals: list = [1, now_ts, row_count, source]
        set_parts = [
            "last_synced_at = EXCLUDED.last_synced_at",
            "row_count = EXCLUDED.row_count",
            "source = EXCLUDED.source",
        ]

        # Optional fields
        optional_map = {
            "empty_acknowledged": "empty_acknowledged",
            "acknowledged_by": "acknowledged_by",
            "acknowledged_at": "acknowledged_at",
            "acknowledged_reason": "acknowledged_reason",
            "last_full_refresh_at": "last_full_refresh_at",
            "last_mutation_at": "last_mutation_at",
            "last_full_refresh_source": "last_full_refresh_source",
            "last_mutation_source": "last_mutation_source",
        }
        for key, col in optional_map.items():
            if key in data:
                cols.append(col)
                val = data[key]
                # Convert timestamp strings to datetimes for TIMESTAMPTZ columns
                if col in ("acknowledged_at", "last_full_refresh_at", "last_mutation_at"):
                    val = self._to_utc(val)
                # Convert int (0/1) to bool for Postgres BOOLEAN column
                if col == "empty_acknowledged":
                    val = bool(val)
                vals.append(val)
                set_parts.append(f"{col} = EXCLUDED.{col}")

        cols_str = ", ".join(cols)
        placeholders = ", ".join("%s" for _ in vals)
        sets_str = ", ".join(set_parts)

        with self._connect() as conn:
            conn.execute(
                f"""INSERT INTO v2_suppression_sync ({cols_str})
                    VALUES ({placeholders})
                    ON CONFLICT (id) DO UPDATE SET {sets_str}""",
                vals,
            )
            conn.commit()

    def acknowledge_empty_suppressions(
        self, actor: str, reason: str
    ) -> Dict[str, Any]:
        if not actor or not actor.strip():
            raise ValueError("Acknowledgment requires a named actor")
        if not reason or not reason.strip():
            raise ValueError("Acknowledgment requires a reason")

        now = datetime.now(timezone.utc)

        sentinel = self.get_suppression_sentinel()
        if sentinel is None:
            raise ValueError(
                "Cannot acknowledge empty suppressions: no sync has ever occurred. "
                "Run a suppression sync first."
            )
        if sentinel.get("row_count", 0) > 0:
            raise ValueError(
                f"Suppression list has {sentinel['row_count']} rows — acknowledgment "
                "is only valid when the list is empty."
            )

        with self._connect() as conn:
            conn.execute(
                """UPDATE v2_suppression_sync SET
                       empty_acknowledged = TRUE,
                       acknowledged_by = %s,
                       acknowledged_at = %s,
                       acknowledged_reason = %s
                   WHERE id = 1""",
                (actor.strip(), now, reason.strip()),
            )
            conn.commit()

        return {
            "status": "acknowledged",
            "actor": actor.strip(),
            "reason": reason.strip(),
            "acknowledged_at": now.isoformat(),
            "expires_at": (now + timedelta(hours=24)).isoformat(),
        }

    # ── Send Attempts ────────────────────────────────────────────────────

    def create_send_attempt(self, attempt: "SendAttempt") -> "SendAttempt":
        from send_attempt_model import DuplicateClaimError
        from psycopg.types.json import Json

        now = self._to_utc(attempt.claimed_at) or datetime.now(timezone.utc)
        try:
            with self._connect() as conn:
                row = conn.execute(
                    """INSERT INTO v2_send_attempts
                       (intervention_id, execution_generation, attempt_status,
                        idempotency_key, audience_hash, provider_campaign_id,
                        provider_tag, provider_segment_id, audience_count,
                        claimed_at, is_dry_run)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       RETURNING id""",
                    (attempt.intervention_id, attempt.execution_generation,
                     attempt.attempt_status.value, attempt.idempotency_key,
                     attempt.audience_hash, attempt.provider_campaign_id,
                     attempt.provider_tag, attempt.provider_segment_id,
                     attempt.audience_count, now,
                     attempt.is_dry_run),
                ).fetchone()
                conn.commit()
                attempt.id = row["id"]
                attempt.claimed_at = now
                return attempt
        except Exception as e:
            err_str = str(e)
            if "idx_send_attempts_active_claim" in err_str or \
               "idx_send_attempts_unique_success" in err_str:
                raise DuplicateClaimError(
                    f"Cannot create claim: partial unique index violation "
                    f"for intervention {attempt.intervention_id} "
                    f"gen {attempt.execution_generation}"
                ) from e
            raise

    def update_send_attempt(self, attempt: "SendAttempt") -> None:
        from psycopg.types.json import Json

        with self._connect() as conn:
            conn.execute(
                """UPDATE v2_send_attempts SET
                       attempt_status = %s,
                       provider_campaign_id = %s,
                       provider_tag = %s,
                       provider_segment_id = %s,
                       audience_count = %s,
                       provider_campaign_created_at = %s,
                       audience_configured_at = %s,
                       send_requested_at = %s,
                       completed_at = %s,
                       provider_sent_at = %s,
                       reconciled_at = %s,
                       reconciled_by = %s,
                       reconciliation_detail = %s,
                       error_message = %s,
                       error_detail = %s
                   WHERE id = %s""",
                (attempt.attempt_status.value,
                 attempt.provider_campaign_id, attempt.provider_tag,
                 attempt.provider_segment_id, attempt.audience_count,
                 self._to_utc(attempt.provider_campaign_created_at),
                 self._to_utc(attempt.audience_configured_at),
                 self._to_utc(attempt.send_requested_at),
                 self._to_utc(attempt.completed_at),
                 self._to_utc(attempt.provider_sent_at),
                 self._to_utc(attempt.reconciled_at),
                 attempt.reconciled_by,
                 Json(attempt.reconciliation_detail) if attempt.reconciliation_detail else None,
                 attempt.error_message,
                 Json(attempt.error_detail) if attempt.error_detail else None,
                 attempt.id),
            )
            conn.commit()

    def get_send_attempt(self, attempt_id: int) -> Optional["SendAttempt"]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM v2_send_attempts WHERE id = %s", (attempt_id,)
            ).fetchone()
        if not row:
            return None
        return self._pg_row_to_send_attempt(dict(row))

    def get_send_attempts(self, intervention_id: str) -> List["SendAttempt"]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT * FROM v2_send_attempts
                   WHERE intervention_id = %s ORDER BY id DESC""",
                (intervention_id,),
            ).fetchall()
        return [self._pg_row_to_send_attempt(dict(r)) for r in rows]

    def get_active_send_attempt(
        self, intervention_id: str, execution_generation: int
    ) -> Optional["SendAttempt"]:
        with self._connect() as conn:
            row = conn.execute(
                """SELECT * FROM v2_send_attempts
                   WHERE intervention_id = %s AND execution_generation = %s
                   AND is_dry_run = FALSE
                   AND attempt_status NOT IN (
                       'confirmed_sent', 'failed_pre_send',
                       'reconciled_sent', 'reconciled_not_sent', 'cancelled'
                   )
                   ORDER BY id DESC LIMIT 1""",
                (intervention_id, execution_generation),
            ).fetchone()
        if not row:
            return None
        return self._pg_row_to_send_attempt(dict(row))

    def stage_attempt_recipients(
        self, attempt_id: int, emails: List[str]
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._connect() as conn:
            for email in emails:
                conn.execute(
                    """INSERT INTO v2_send_attempt_recipients
                       (send_attempt_id, email, staged_at)
                       VALUES (%s, %s, %s)
                       ON CONFLICT (send_attempt_id, email) DO NOTHING""",
                    (attempt_id, email.lower().strip(), now),
                )
            conn.commit()

    def get_attempt_recipients(self, attempt_id: int) -> List[str]:
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT email FROM v2_send_attempt_recipients
                   WHERE send_attempt_id = %s ORDER BY email""",
                (attempt_id,),
            ).fetchall()
        return [r["email"] for r in rows]

    def promote_attempt_recipients(
        self, attempt_id: int, intervention_id: str, campaign_draft_id: str,
        sent_at: Any,
    ) -> None:
        ts = self._to_utc(sent_at)
        with self._connect() as conn:
            emails = conn.execute(
                """SELECT email FROM v2_send_attempt_recipients
                   WHERE send_attempt_id = %s""",
                (attempt_id,),
            ).fetchall()
            for r in emails:
                conn.execute(
                    """INSERT INTO v2_campaign_sends
                       (intervention_id, campaign_draft_id, email, sent_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (intervention_id, email) DO NOTHING""",
                    (intervention_id, campaign_draft_id, r["email"], ts),
                )
            conn.commit()

    def finalize_send_locally(
        self,
        attempt_id: int,
        intervention: Intervention,
        event_id: str,
        sent_at: Any,
        audit_action: str,
        audit_metadata: Optional[Dict[str, Any]] = None,
        from_status: Optional[str] = None,
        actor: str = "system",
    ) -> None:
        """Promote recipients + save intervention + audit in ONE transaction.

        Postgres gives us real atomicity here: either the intervention
        advances AND its recipients land in attribution AND the audit
        records it, or none of it happens and the next recovery pass
        retries cleanly.
        """
        from psycopg.types.json import Json

        # Two distinct clocks, deliberately kept apart:
        #   sent_ts — when the PROVIDER sent. Attribution reads this.
        #   now     — when this bookkeeping ran. Audit only.
        sent_ts = self._to_utc(sent_at)
        now = datetime.now(timezone.utc)
        conn = self._connect()
        try:
            rows = conn.execute(
                """SELECT email FROM v2_send_attempt_recipients
                   WHERE send_attempt_id = %s""",
                (attempt_id,),
            ).fetchall()
            for r in rows:
                # DO NOTHING, never DO UPDATE: an already-promoted
                # recipient keeps its ORIGINAL sent_at so replaying
                # finalization cannot rewrite attribution timestamps.
                conn.execute(
                    """INSERT INTO v2_campaign_sends
                       (intervention_id, campaign_draft_id, email, sent_at)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (intervention_id, email) DO NOTHING""",
                    (intervention.id, intervention.campaign_draft_id,
                     r["email"], sent_ts),
                )
            conn.execute(
                self._INTERVENTION_UPSERT,
                self._intervention_params(intervention),
            )
            conn.execute(
                """INSERT INTO intervention_audit_log
                   (intervention_id, event_id, action, from_status, to_status,
                    actor, timestamp, metadata, error)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (intervention.id, event_id, audit_action, from_status,
                 intervention.status.value, actor, now,
                 Json(audit_metadata) if audit_metadata else Json({}), None),
            )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    # ── Health ────────────────────────────────────────────────────────────

    def health_check(self) -> Dict[str, Any]:
        try:
            with self._connect() as conn:
                conn.execute("SELECT 1").fetchone()
                tables = []
                for tbl in _V2_EXPECTED_TABLES:
                    try:
                        conn.execute(f"SELECT 1 FROM {tbl} LIMIT 1")
                        tables.append(tbl)
                    except Exception:
                        pass
                # Check migration version
                try:
                    ver = conn.execute(
                        "SELECT MAX(version) AS v FROM v2_schema_version"
                    ).fetchone()
                    schema_version = ver["v"] if ver else None
                except Exception:
                    schema_version = None

            missing = [t for t in _V2_EXPECTED_TABLES if t not in tables]
            schema_ok = (
                schema_version is not None
                and schema_version >= REQUIRED_SCHEMA_VERSION
            )
            # Fail closed. Previously this accepted any 5 of 7 tables, so
            # a database missing v2_send_attempts — the table the
            # duplicate-send guarantee depends on — still reported
            # healthy. Readiness must mean the idempotency machinery is
            # actually present, not merely that the connection works.
            ready = not missing and schema_ok

            result = {
                "status": "ok" if ready else "degraded",
                "backend": "postgres",
                "tables_found": tables,
                "tables_expected": len(_V2_EXPECTED_TABLES),
                "tables_ok": not missing,
                "schema_version": schema_version,
                "required_schema_version": REQUIRED_SCHEMA_VERSION,
                "schema_ok": schema_ok,
                "ready": ready,
            }
            if missing:
                result["missing_tables"] = missing
            if not ready:
                reasons = []
                if missing:
                    reasons.append(
                        f"missing operational tables: {', '.join(missing)}")
                if not schema_ok:
                    reasons.append(
                        f"schema version {schema_version} < required "
                        f"{REQUIRED_SCHEMA_VERSION}; run migrations")
                result["error"] = "; ".join(reasons)
                log.error("V2 Postgres not ready: %s", result["error"])
            return result
        except Exception as e:
            return {
                "status": "degraded", "backend": "postgres",
                "ready": False, "error": str(e),
            }

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _pg_row_to_send_attempt(d: dict) -> "SendAttempt":
        """Convert a Postgres dict row to a SendAttempt dataclass."""
        from send_attempt_model import SendAttempt, SendAttemptStatus
        ts = PostgresV2StateRepository._ts_to_iso

        recon = d.get("reconciliation_detail")
        if isinstance(recon, str):
            try:
                recon = json.loads(recon)
            except (json.JSONDecodeError, TypeError):
                recon = None
        elif recon is None:
            pass  # keep as None

        err_detail = d.get("error_detail")
        if isinstance(err_detail, str):
            try:
                err_detail = json.loads(err_detail)
            except (json.JSONDecodeError, TypeError):
                err_detail = None

        return SendAttempt(
            id=d["id"],
            intervention_id=d["intervention_id"],
            execution_generation=int(d["execution_generation"]),
            attempt_status=SendAttemptStatus(d["attempt_status"]),
            idempotency_key=d["idempotency_key"],
            audience_hash=d["audience_hash"],
            provider_campaign_id=d.get("provider_campaign_id"),
            provider_tag=d.get("provider_tag"),
            provider_segment_id=int(d["provider_segment_id"]) if d.get("provider_segment_id") is not None else None,
            audience_count=int(d.get("audience_count") or 0),
            claimed_at=d.get("claimed_at"),
            provider_campaign_created_at=d.get("provider_campaign_created_at"),
            audience_configured_at=d.get("audience_configured_at"),
            send_requested_at=d.get("send_requested_at"),
            provider_sent_at=d.get("provider_sent_at"),
            completed_at=d.get("completed_at"),
            reconciled_at=d.get("reconciled_at"),
            reconciled_by=d.get("reconciled_by"),
            reconciliation_detail=recon,
            error_message=d.get("error_message"),
            error_detail=err_detail,
            is_dry_run=bool(d.get("is_dry_run", False)),
        )

    @staticmethod
    def _pg_row_to_intervention(row: Dict[str, Any]) -> Intervention:
        """Convert a Postgres dict row to an Intervention dataclass.

        Handles TIMESTAMPTZ → ISO string conversion and JSONB → dict.
        """
        d = dict(row)
        evidence = d.get("evidence", {})
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except (json.JSONDecodeError, TypeError):
                evidence = {}
        elif evidence is None:
            evidence = {}
        # evidence is already a dict from JSONB auto-deserialization

        # Convert timestamps to ISO strings (Intervention dataclass stores strings)
        def ts(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val.isoformat()
            return str(val)

        return Intervention(
            id=d["id"],
            opportunity_id=d["opportunity_id"],
            event_id=d["event_id"],
            intervention_type=d["intervention_type"],
            status=InterventionStatus(d["status"]),
            rationale=d.get("rationale", ""),
            audience_definition=d.get("audience_definition", ""),
            expected_revenue=float(d.get("expected_revenue") or 0),
            expected_cost=float(d.get("expected_cost") or 0),
            expected_net_value=float(d.get("expected_net_value") or 0),
            confidence=float(d.get("confidence") or 0),
            approval_required=bool(d.get("approval_required", True)),
            created_at=ts(d.get("created_at")) or "",
            approved_at=ts(d.get("approved_at")),
            executed_at=ts(d.get("executed_at")),
            measurement_window=int(d.get("measurement_window") or 14),
            actual_revenue=float(d["actual_revenue"]) if d.get("actual_revenue") is not None else None,
            actual_cost=float(d["actual_cost"]) if d.get("actual_cost") is not None else None,
            actual_net_value=float(d["actual_net_value"]) if d.get("actual_net_value") is not None else None,
            outcome_status=d.get("outcome_status"),
            evidence=evidence,
            campaign_draft_id=d.get("campaign_draft_id"),
            measurement_started_at=ts(d.get("measurement_started_at")),
            measurement_ends_at=ts(d.get("measurement_ends_at")),
            attributed_orders=int(d["attributed_orders"]) if d.get("attributed_orders") is not None else None,
            attributed_tickets=int(d["attributed_tickets"]) if d.get("attributed_tickets") is not None else None,
            attributed_revenue=float(d["attributed_revenue"]) if d.get("attributed_revenue") is not None else None,
            sent_count=int(d["sent_count"]) if d.get("sent_count") is not None else None,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Migration Runner
# ─────────────────────────────────────────────────────────────────────────────


def run_postgres_migrations(conninfo: str, migrations_dir: str = "migrations/v2_postgres") -> int:
    """Run pending Postgres migrations from ordered SQL files.

    Migrations are simple ordered .sql files (001_*.sql, 002_*.sql, ...).
    Each is run in a single transaction. Already-applied versions are skipped
    (safe to re-invoke).

    Returns the number of newly applied migrations.

    Raises on connection failure or SQL errors (fail closed).
    """
    import os
    import re

    import psycopg

    # Find migration files, sorted by version number
    if not os.path.isdir(migrations_dir):
        log.warning(f"Migrations directory not found: {migrations_dir}")
        return 0

    pattern = re.compile(r"^(\d+)_.*\.sql$")
    migration_files = []
    for fname in sorted(os.listdir(migrations_dir)):
        m = pattern.match(fname)
        if m:
            migration_files.append((int(m.group(1)), fname))

    if not migration_files:
        return 0

    applied = 0
    with psycopg.connect(conninfo, autocommit=True) as conn:
        # Ensure schema version table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS v2_schema_version (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                description TEXT NOT NULL
            )
        """)

        # Get already-applied versions
        rows = conn.execute("SELECT version FROM v2_schema_version").fetchall()
        applied_versions = {r["version"] if isinstance(r, dict) else r[0] for r in rows}

        for version, fname in migration_files:
            if version in applied_versions:
                log.info(f"Migration {fname} already applied, skipping")
                continue

            filepath = os.path.join(migrations_dir, fname)
            with open(filepath, "r") as f:
                sql = f.read()

            log.info(f"Applying migration: {fname}")
            # The migration file may contain its own BEGIN/COMMIT,
            # so we run it as-is with autocommit=True
            conn.execute(sql)
            applied += 1
            log.info(f"Migration {fname} applied successfully")

    return applied


# ─────────────────────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────────────────────


def create_v2_repository(
    db=None, database_url: Optional[str] = None
) -> V2StateRepository:
    """Factory: create the appropriate V2StateRepository backend.

    Backend selection:
        database_url present → PostgresV2StateRepository
        database_url absent  → SQLiteV2StateRepository (requires db)
        Postgres configured but unavailable → raise (fail closed)

    Args:
        db: SQLite Database instance (required if database_url is None).
        database_url: Postgres connection string. If provided, Postgres is used.

    Returns:
        V2StateRepository implementation.
    """
    if database_url:
        log.info("V2 state backend: Postgres")
        repo = PostgresV2StateRepository(database_url)
        # Verify connectivity immediately — fail closed
        health = repo.health_check()
        if health["status"] == "degraded":
            raise ConnectionError(
                f"Postgres V2 backend configured but unreachable: {health.get('error')}"
            )
        return repo
    else:
        if db is None:
            raise ValueError(
                "SQLite V2 backend requires a Database instance (db parameter)"
            )
        log.info("V2 state backend: SQLite")
        return SQLiteV2StateRepository(db)
