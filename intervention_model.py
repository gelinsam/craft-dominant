"""Intervention domain model with state machine.

An Intervention is the bridge between a surfaced Opportunity and a concrete
recovery action (e.g. a CRM campaign draft).  It carries the full lifecycle
from initial investigation through execution and measurement.

State machine:
    new → investigated → proposed → approved → executing → measuring → learned
    Any active state may also transition to → rejected | cancelled
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from hashlib import sha256
from typing import Any, Dict, List, Optional

log = logging.getLogger("craft.intervention")


class InterventionStatus(str, Enum):
    NEW = "new"
    INVESTIGATED = "investigated"
    PROPOSED = "proposed"
    APPROVED = "approved"
    EXECUTING = "executing"
    MEASURING = "measuring"
    LEARNED = "learned"
    REJECTED = "rejected"
    CANCELLED = "cancelled"


# Directed graph of legal transitions.
_TRANSITIONS: Dict[InterventionStatus, List[InterventionStatus]] = {
    InterventionStatus.NEW: [InterventionStatus.INVESTIGATED, InterventionStatus.CANCELLED],
    InterventionStatus.INVESTIGATED: [InterventionStatus.PROPOSED, InterventionStatus.CANCELLED],
    InterventionStatus.PROPOSED: [InterventionStatus.APPROVED, InterventionStatus.REJECTED, InterventionStatus.CANCELLED],
    InterventionStatus.APPROVED: [InterventionStatus.EXECUTING, InterventionStatus.CANCELLED],
    InterventionStatus.EXECUTING: [InterventionStatus.MEASURING, InterventionStatus.CANCELLED],
    InterventionStatus.MEASURING: [InterventionStatus.LEARNED, InterventionStatus.CANCELLED],
    # Terminal states have no outgoing edges.
    InterventionStatus.LEARNED: [],
    InterventionStatus.REJECTED: [],
    InterventionStatus.CANCELLED: [],
}

TERMINAL_STATES = frozenset({InterventionStatus.LEARNED, InterventionStatus.REJECTED, InterventionStatus.CANCELLED})


class IllegalTransition(ValueError):
    """Raised when a status transition violates the state machine."""


def validate_transition(current: InterventionStatus, target: InterventionStatus) -> None:
    """Raise IllegalTransition if current → target is not allowed."""
    allowed = _TRANSITIONS.get(current, [])
    if target not in allowed:
        raise IllegalTransition(
            f"Cannot transition from {current.value!r} to {target.value!r}. "
            f"Legal targets: {[s.value for s in allowed]}"
        )


def _intervention_id(opportunity_id: str, intervention_type: str) -> str:
    fingerprint = f"{opportunity_id}|{intervention_type}"
    return sha256(fingerprint.encode("utf-8")).hexdigest()[:16]


@dataclass
class Intervention:
    id: str
    opportunity_id: str
    event_id: str
    intervention_type: str  # e.g. "crm_campaign", "ad_budget_shift", "price_adjust"
    status: InterventionStatus = InterventionStatus.NEW
    rationale: str = ""
    audience_definition: str = ""
    expected_revenue: float = 0.0
    expected_cost: float = 0.0
    expected_net_value: float = 0.0
    confidence: float = 0.0
    approval_required: bool = True
    created_at: str = ""
    approved_at: Optional[str] = None
    executed_at: Optional[str] = None
    measurement_window: int = 14  # days
    actual_revenue: Optional[float] = None
    actual_cost: Optional[float] = None
    actual_net_value: Optional[float] = None
    outcome_status: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    # Link to campaign draft if applicable
    campaign_draft_id: Optional[str] = None
    # Measurement fields
    measurement_started_at: Optional[str] = None
    measurement_ends_at: Optional[str] = None
    attributed_orders: Optional[int] = None
    attributed_tickets: Optional[int] = None
    attributed_revenue: Optional[float] = None
    sent_count: Optional[int] = None

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if isinstance(self.status, str):
            self.status = InterventionStatus(self.status)

    def transition_to(self, target: InterventionStatus) -> None:
        """Move to a new status, enforcing the state machine."""
        validate_transition(self.status, target)
        self.status = target
        now = datetime.now(timezone.utc).isoformat()
        if target == InterventionStatus.APPROVED:
            self.approved_at = now
        elif target == InterventionStatus.EXECUTING:
            self.executed_at = now
        elif target == InterventionStatus.MEASURING:
            self.measurement_started_at = now
            # Default 7-day attribution window
            ends = datetime.now(timezone.utc) + timedelta(days=self.measurement_window)
            self.measurement_ends_at = ends.isoformat()

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATES

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["status"] = self.status.value
        # evidence is already a dict from asdict
        return d

    @classmethod
    def create(
        cls,
        opportunity_id: str,
        event_id: str,
        intervention_type: str,
        **kwargs,
    ) -> "Intervention":
        return cls(
            id=_intervention_id(opportunity_id, intervention_type),
            opportunity_id=opportunity_id,
            event_id=event_id,
            intervention_type=intervention_type,
            **kwargs,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Persistence
# ─────────────────────────────────────────────────────────────────────────────

AUDIT_LOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS intervention_audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intervention_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    action TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT,
    actor TEXT DEFAULT 'system',
    timestamp TEXT NOT NULL,
    metadata TEXT DEFAULT '{}',
    error TEXT,
    FOREIGN KEY (intervention_id) REFERENCES interventions(id)
);
CREATE INDEX IF NOT EXISTS idx_audit_intervention ON intervention_audit_log(intervention_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON intervention_audit_log(timestamp);
"""

CAMPAIGN_SENDS_SCHEMA = """
CREATE TABLE IF NOT EXISTS v2_campaign_sends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intervention_id TEXT NOT NULL,
    campaign_draft_id TEXT NOT NULL,
    email TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    UNIQUE(intervention_id, email)
);
CREATE INDEX IF NOT EXISTS idx_v2_sends_intervention ON v2_campaign_sends(intervention_id);
CREATE INDEX IF NOT EXISTS idx_v2_sends_email ON v2_campaign_sends(email);
"""

LEARNING_RECORD_SCHEMA = """
CREATE TABLE IF NOT EXISTS v2_learning_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    intervention_id TEXT NOT NULL UNIQUE,
    intervention_type TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT DEFAULT '',
    city TEXT DEFAULT '',
    predicted_revenue REAL DEFAULT 0,
    attributed_revenue REAL DEFAULT 0,
    prediction_error REAL DEFAULT 0,
    audience_count INTEGER DEFAULT 0,
    sent_count INTEGER DEFAULT 0,
    attributed_orders INTEGER DEFAULT 0,
    attributed_tickets INTEGER DEFAULT 0,
    actual_conversion_rate REAL,
    conversion_assumptions TEXT DEFAULT '{}',
    confidence REAL DEFAULT 0,
    measurement_window_days INTEGER DEFAULT 7,
    measurement_started_at TEXT,
    measurement_ended_at TEXT,
    created_at TEXT NOT NULL
);
"""

INTERVENTION_SCHEMA = """
CREATE TABLE IF NOT EXISTS interventions (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    intervention_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    rationale TEXT DEFAULT '',
    audience_definition TEXT DEFAULT '',
    expected_revenue REAL DEFAULT 0,
    expected_cost REAL DEFAULT 0,
    expected_net_value REAL DEFAULT 0,
    confidence REAL DEFAULT 0,
    approval_required INTEGER DEFAULT 1,
    created_at TEXT NOT NULL,
    approved_at TEXT,
    executed_at TEXT,
    measurement_window INTEGER DEFAULT 14,
    actual_revenue REAL,
    actual_cost REAL,
    actual_net_value REAL,
    outcome_status TEXT,
    evidence TEXT DEFAULT '{}',
    campaign_draft_id TEXT,
    measurement_started_at TEXT,
    measurement_ends_at TEXT,
    attributed_orders INTEGER,
    attributed_tickets INTEGER,
    attributed_revenue REAL,
    sent_count INTEGER
);
"""


class InterventionStore:
    """SQLite-backed persistence for Interventions."""

    def __init__(self, db):
        self.db = db
        self._ensure_schema()

    def _ensure_schema(self):
        self.db.conn.executescript(INTERVENTION_SCHEMA)
        self.db.conn.executescript(AUDIT_LOG_SCHEMA)
        self.db.conn.executescript(CAMPAIGN_SENDS_SCHEMA)
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.db.conn.commit()

    def save(self, intervention: Intervention) -> None:
        evidence_json = json.dumps(intervention.evidence) if isinstance(intervention.evidence, dict) else intervention.evidence
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

    def get(self, intervention_id: str) -> Optional[Intervention]:
        row = self.db.conn.execute(
            "SELECT * FROM interventions WHERE id = ?", (intervention_id,)
        ).fetchone()
        if not row:
            return None
        return self._row_to_intervention(row)

    def get_by_opportunity(self, opportunity_id: str) -> List[Intervention]:
        rows = self.db.conn.execute(
            "SELECT * FROM interventions WHERE opportunity_id = ? ORDER BY created_at DESC",
            (opportunity_id,),
        ).fetchall()
        return [self._row_to_intervention(r) for r in rows]

    def get_by_event(self, event_id: str) -> List[Intervention]:
        rows = self.db.conn.execute(
            "SELECT * FROM interventions WHERE event_id = ? ORDER BY created_at DESC",
            (event_id,),
        ).fetchall()
        return [self._row_to_intervention(r) for r in rows]

    def list_all(self, include_terminal: bool = False) -> List[Intervention]:
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

    @staticmethod
    def _row_to_intervention(row) -> Intervention:
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
# Audit log
# ─────────────────────────────────────────────────────────────────────────────

class AuditLogger:
    """Append-only audit log for intervention lifecycle events."""

    def __init__(self, db):
        self.db = db
        self.db.conn.executescript(AUDIT_LOG_SCHEMA)
        self.db.conn.commit()

    def log(
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
        """Write an append-only audit record."""
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

    def get_log(self, intervention_id: str) -> List[Dict[str, Any]]:
        """Return all audit entries for an intervention, oldest first."""
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


# ─────────────────────────────────────────────────────────────────────────────
# Learning record store
# ─────────────────────────────────────────────────────────────────────────────

class LearningStore:
    """Persists structured learning records when interventions reach 'learned'."""

    def __init__(self, db):
        self.db = db
        self.db.conn.executescript(LEARNING_RECORD_SCHEMA)
        self.db.conn.commit()

    def save(self, record: Dict[str, Any]) -> None:
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

    def get(self, intervention_id: str) -> Optional[Dict[str, Any]]:
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
