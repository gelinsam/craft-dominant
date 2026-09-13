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
from datetime import datetime, timezone
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

    def __post_init__(self):
        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if isinstance(self.status, str):
            self.status = InterventionStatus(self.status)

    def transition_to(self, target: InterventionStatus) -> None:
        """Move to a new status, enforcing the state machine."""
        validate_transition(self.status, target)
        self.status = target
        if target == InterventionStatus.APPROVED:
            self.approved_at = datetime.now(timezone.utc).isoformat()
        elif target == InterventionStatus.EXECUTING:
            self.executed_at = datetime.now(timezone.utc).isoformat()

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
    campaign_draft_id TEXT
);
"""


class InterventionStore:
    """SQLite-backed persistence for Interventions."""

    def __init__(self, db):
        self.db = db
        self._ensure_schema()

    def _ensure_schema(self):
        self.db.conn.executescript(INTERVENTION_SCHEMA)
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
                campaign_draft_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
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
        )
