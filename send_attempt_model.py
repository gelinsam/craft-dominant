"""
Send Attempt Model — Durable, idempotent execution tracking
=============================================================

A SendAttempt represents a single attempt to execute an intervention
through an external provider (Mailchimp).  The attempt state machine
ensures that:

  1. A durable claim is created BEFORE any provider mutation.
  2. Each irreversible provider step is checkpointed to the DB.
  3. Ambiguous outcomes (network timeout, crash near send) are never
     silently retried — they require explicit reconciliation.
  4. At most one active/successful attempt exists per
     (intervention_id, execution_generation).

State machine::

    claimed
      │
      ├─→ failed_pre_send          (provider setup failed, safe to retry)
      │
      ▼
    provider_campaign_created
      │
      ├─→ failed_pre_send          (audience push failed, campaign exists
      │                              but nothing sent)
      ▼
    audience_configured
      │
      ├─→ failed_pre_send          (pre-send check failed)
      │
      ▼
    send_requested                   ← DANGER ZONE: provider may have sent
      │
      ├─→ confirmed_sent            (provider confirmed success)
      ├─→ ambiguous                  (timeout / crash / unknown response)
      │     │
      │     ├─→ reconciled_sent      (provider says it sent)
      │     └─→ reconciled_not_sent  (provider says it did not send)
      │
      └─→ cancelled                  (cancelled before confirmation)

Terminal states: confirmed_sent, failed_pre_send, reconciled_sent,
                 reconciled_not_sent, cancelled
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, Any, List, FrozenSet


class SendAttemptStatus(str, Enum):
    """Send attempt state machine states."""
    CLAIMED = 'claimed'
    PROVIDER_CAMPAIGN_CREATED = 'provider_campaign_created'
    AUDIENCE_CONFIGURED = 'audience_configured'
    SEND_REQUESTED = 'send_requested'
    CONFIRMED_SENT = 'confirmed_sent'
    FAILED_PRE_SEND = 'failed_pre_send'
    AMBIGUOUS = 'ambiguous'
    RECONCILED_SENT = 'reconciled_sent'
    RECONCILED_NOT_SENT = 'reconciled_not_sent'
    CANCELLED = 'cancelled'


# Legal state transitions
_ATTEMPT_TRANSITIONS: Dict[SendAttemptStatus, FrozenSet[SendAttemptStatus]] = {
    # AMBIGUOUS is reachable from every pre-send state, not just
    # SEND_REQUESTED.  Any provider call can lose its response, and the
    # earlier steps have their own uncertainty: a lost campaign-creation
    # response can leave a campaign at the provider whose ID we never
    # learned, which we can never reconcile.  Forcing those cases into
    # FAILED_PRE_SEND would quietly mark them retryable.
    SendAttemptStatus.CLAIMED: frozenset({
        SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED,
        SendAttemptStatus.FAILED_PRE_SEND,
        SendAttemptStatus.AMBIGUOUS,
        SendAttemptStatus.CANCELLED,
    }),
    SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED: frozenset({
        SendAttemptStatus.AUDIENCE_CONFIGURED,
        SendAttemptStatus.FAILED_PRE_SEND,
        SendAttemptStatus.AMBIGUOUS,
        SendAttemptStatus.CANCELLED,
    }),
    SendAttemptStatus.AUDIENCE_CONFIGURED: frozenset({
        SendAttemptStatus.SEND_REQUESTED,
        SendAttemptStatus.FAILED_PRE_SEND,
        SendAttemptStatus.AMBIGUOUS,
        SendAttemptStatus.CANCELLED,
    }),
    SendAttemptStatus.SEND_REQUESTED: frozenset({
        SendAttemptStatus.CONFIRMED_SENT,
        SendAttemptStatus.AMBIGUOUS,
        # FAILED_PRE_SEND is reachable here ONLY when the provider
        # returned a status code that proves it rejected the request
        # before acting on it (see DEFINITE_SEND_REJECTION_CODES in
        # provider_outcome.py — 400/401/403/404/405/422).  Every other
        # response, including all 5xx and every transport failure, must
        # go to AMBIGUOUS instead.  The state machine cannot enforce
        # that distinction on its own, so the narrow classifier and its
        # tests are what keep this edge honest.
        SendAttemptStatus.FAILED_PRE_SEND,
        SendAttemptStatus.CANCELLED,
    }),
    SendAttemptStatus.AMBIGUOUS: frozenset({
        SendAttemptStatus.RECONCILED_SENT,
        SendAttemptStatus.RECONCILED_NOT_SENT,
    }),
    # Terminal states — no outgoing transitions
    SendAttemptStatus.CONFIRMED_SENT: frozenset(),
    SendAttemptStatus.FAILED_PRE_SEND: frozenset(),
    SendAttemptStatus.RECONCILED_SENT: frozenset(),
    SendAttemptStatus.RECONCILED_NOT_SENT: frozenset(),
    SendAttemptStatus.CANCELLED: frozenset(),
}

TERMINAL_ATTEMPT_STATES = frozenset({
    SendAttemptStatus.CONFIRMED_SENT,
    SendAttemptStatus.FAILED_PRE_SEND,
    SendAttemptStatus.RECONCILED_SENT,
    SendAttemptStatus.RECONCILED_NOT_SENT,
    SendAttemptStatus.CANCELLED,
})

# States that represent a successful send (used for blocking retries)
SUCCESSFUL_SEND_STATES = frozenset({
    SendAttemptStatus.CONFIRMED_SENT,
    SendAttemptStatus.RECONCILED_SENT,
})

# States that are "active" — meaning a send is in progress
ACTIVE_ATTEMPT_STATES = frozenset({
    SendAttemptStatus.CLAIMED,
    SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED,
    SendAttemptStatus.AUDIENCE_CONFIGURED,
    SendAttemptStatus.SEND_REQUESTED,
    SendAttemptStatus.AMBIGUOUS,
})


@dataclass
class SendAttempt:
    """Durable record of one execution attempt for an intervention.

    Created at claim time (before any provider mutation) and updated
    at each checkpoint.
    """
    id: Optional[int]                  # DB-assigned BIGSERIAL
    intervention_id: str
    execution_generation: int
    attempt_status: SendAttemptStatus
    idempotency_key: str
    audience_hash: str

    # Provider state (filled incrementally)
    provider_campaign_id: Optional[str] = None
    provider_tag: Optional[str] = None
    provider_segment_id: Optional[int] = None

    # Audience snapshot
    audience_count: int = 0

    # Timestamps
    claimed_at: Optional[datetime] = None
    provider_campaign_created_at: Optional[datetime] = None
    audience_configured_at: Optional[datetime] = None
    send_requested_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Reconciliation
    reconciled_at: Optional[datetime] = None
    reconciled_by: Optional[str] = None
    reconciliation_detail: Optional[Dict[str, Any]] = None

    # Error tracking
    error_message: Optional[str] = None
    error_detail: Optional[Dict[str, Any]] = None

    # Dry-run flag
    is_dry_run: bool = False

    def transition_to(self, new_status: SendAttemptStatus) -> None:
        """Enforce state machine transitions."""
        allowed = _ATTEMPT_TRANSITIONS.get(self.attempt_status, frozenset())
        if new_status not in allowed:
            raise IllegalAttemptTransition(
                f"Cannot transition send attempt from "
                f"'{self.attempt_status.value}' to '{new_status.value}'. "
                f"Allowed: {sorted(s.value for s in allowed)}"
            )
        self.attempt_status = new_status

        # Set checkpoint timestamp
        now = datetime.now(timezone.utc)
        if new_status == SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED:
            self.provider_campaign_created_at = now
        elif new_status == SendAttemptStatus.AUDIENCE_CONFIGURED:
            self.audience_configured_at = now
        elif new_status == SendAttemptStatus.SEND_REQUESTED:
            self.send_requested_at = now
        elif new_status in TERMINAL_ATTEMPT_STATES:
            self.completed_at = now
            if new_status in (SendAttemptStatus.RECONCILED_SENT,
                              SendAttemptStatus.RECONCILED_NOT_SENT):
                self.reconciled_at = now

    @property
    def is_terminal(self) -> bool:
        return self.attempt_status in TERMINAL_ATTEMPT_STATES

    @property
    def is_active(self) -> bool:
        return self.attempt_status in ACTIVE_ATTEMPT_STATES

    @property
    def is_successful(self) -> bool:
        return self.attempt_status in SUCCESSFUL_SEND_STATES

    @property
    def requires_reconciliation(self) -> bool:
        return self.attempt_status == SendAttemptStatus.AMBIGUOUS

    def to_dict(self) -> Dict[str, Any]:
        """Serialize for API responses. Never includes provider secrets."""
        def _ts(dt):
            if dt is None:
                return None
            if isinstance(dt, str):
                return dt
            return dt.isoformat()

        return {
            'id': self.id,
            'intervention_id': self.intervention_id,
            'execution_generation': self.execution_generation,
            'attempt_status': self.attempt_status.value,
            'idempotency_key': self.idempotency_key,
            'audience_hash': self.audience_hash,
            'provider_campaign_id': self.provider_campaign_id,
            'audience_count': self.audience_count,
            'claimed_at': _ts(self.claimed_at),
            'provider_campaign_created_at': _ts(self.provider_campaign_created_at),
            'audience_configured_at': _ts(self.audience_configured_at),
            'send_requested_at': _ts(self.send_requested_at),
            'completed_at': _ts(self.completed_at),
            'reconciled_at': _ts(self.reconciled_at),
            'reconciled_by': self.reconciled_by,
            'is_dry_run': self.is_dry_run,
            'is_terminal': self.is_terminal,
            'is_active': self.is_active,
            'is_successful': self.is_successful,
            'requires_reconciliation': self.requires_reconciliation,
            'error_message': self.error_message,
        }


class IllegalAttemptTransition(Exception):
    """Raised when a send attempt state machine transition is illegal."""
    pass


class DuplicateClaimError(Exception):
    """Raised when a claim cannot be created because an active/successful
    attempt already exists for this intervention+generation."""
    pass


class AudienceHashMismatch(Exception):
    """Raised when the audience hash at execution time differs from
    what was recorded in an existing claim."""
    pass


# ─────────────────────────────────────────────────────────
# Deterministic identity functions
# ─────────────────────────────────────────────────────────

def compute_audience_hash(emails: List[str]) -> str:
    """Deterministic hash of the exact audience.

    Normalizes (lowercase, strip), sorts, then SHA-256 hashes.
    Two calls with the same logical audience always produce
    the same hash, regardless of input order or whitespace.
    """
    normalized = sorted(set(e.lower().strip() for e in emails))
    payload = '\n'.join(normalized)
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()


def compute_idempotency_key(
    intervention_id: str,
    execution_generation: int,
    campaign_draft_id: str,
    audience_hash: str,
) -> str:
    """Deterministic idempotency key from stable inputs.

    No timestamps, no random data. Same inputs → same key.
    Used to detect whether a retry is logically the same request.
    """
    components = '|'.join([
        intervention_id,
        str(execution_generation),
        campaign_draft_id,
        audience_hash,
    ])
    return hashlib.sha256(components.encode('utf-8')).hexdigest()
