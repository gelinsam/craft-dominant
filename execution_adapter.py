"""Safe CRM execution adapter for V2 interventions.

Handles the approved → executing → measuring path for CRM email campaigns.
Actual external sends are gated behind V2_ENABLE_EXTERNAL_SEND=1.
Without that flag, execution produces a dry-run/preflight result with
no external HTTP side effects.

Phase 2 invariant:
    ONE INTERVENTION MAY PRODUCE AT MOST ONE SUCCESSFUL EXTERNAL SEND
    ATTEMPT FOR A GIVEN EXECUTION GENERATION.  If the provider outcome
    is uncertain, STOP and RECONCILE.  Never blindly retry.

The claim-checkpoint-send pattern:
    1. Create a durable DB claim BEFORE any provider mutation.
    2. After each irreversible provider step, checkpoint state to DB.
    3. If the send outcome is ambiguous (timeout, crash, unknown), mark
       the attempt 'ambiguous' — do NOT retry automatically.
    4. Reconciliation queries the provider for campaign/send state.

Measurement uses deterministic attribution: orders placed by sent recipients
within the attribution window are counted as attributed_revenue (not causal lift).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, List, Optional

from intervention_model import (
    Intervention,
    InterventionStatus,
)
from send_attempt_model import (
    SendAttempt,
    SendAttemptStatus,
    DuplicateClaimError,
    AudienceHashMismatch,
    compute_audience_hash,
    compute_idempotency_key,
    ACTIVE_ATTEMPT_STATES,
    SUCCESSFUL_SEND_STATES,
    TERMINAL_ATTEMPT_STATES,
)
from suppression_guard import SuppressionGuard, SuppressionStatus
from audience_suppression import guard_for_events
from provider_outcome import (
    ProviderSendOutcome,
    ProviderSendStatus,
    ProviderError,
    ReconciliationVerdict,
    classify_reconciliation_status,
)

log = logging.getLogger("craft.execution")


class AmbiguousSendOutcome(Exception):
    """The provider may or may not have sent. Retry is forbidden.

    Distinct from a generic execution failure so the API layer can
    return a non-retryable, reconciliation-required response rather
    than something a client would reasonably retry.
    """

# Default attribution window in days
ATTRIBUTION_WINDOW_DAYS = 7

# Phase 2: single execution generation for now.
# New generation requires explicit future human action.
EXECUTION_GENERATION = 1

# Statuses that mean the intervention has moved past "waiting to run".
# A proven send whose intervention is still APPROVED is the crash
# signature that local finalization repairs.
_POST_EXECUTION_STATUSES = frozenset({
    InterventionStatus.EXECUTING,
    InterventionStatus.MEASURING,
    InterventionStatus.LEARNED,
})


class ExecutionAdapter:
    """Executes approved CRM interventions safely.

    Safety invariants:
    - Execution only from status == approved
    - Suppression list re-checked at execution time
    - Current buyers re-checked at execution time
    - Campaign draft must exist
    - Audience recomputed/validated at execution time (never trust stale prepared audience)
    - External sends gated by V2_ENABLE_EXTERNAL_SEND env flag
    - All state transitions audited
    """

    def __init__(self, db, v2_repo, campaign_engine=None):
        """Initialize with analytics DB and V2 state repository.

        Args:
            db: SQLite Database instance for analytics reads (events, orders,
                buyers, campaigns, suppression email set).
            v2_repo: V2StateRepository for all V2 operational state
                     (interventions, audit, sends, learning, sentinel).
            campaign_engine: Optional CraftCampaignEngine for Mailchimp sends.
        """
        self.db = db
        self.v2_repo = v2_repo
        self.campaign_engine = campaign_engine
        self.suppression_guard = SuppressionGuard(db, v2_repo=v2_repo)

    def execute(self, intervention_id: str, actor: str = "system") -> Dict[str, Any]:
        """Execute an approved CRM intervention.

        Returns a result dict with execution status and details.
        """
        intervention = self.v2_repo.get_intervention(intervention_id)
        if not intervention:
            return {"error": "intervention_not_found"}

        # ── Gate 0: proven send already exists → recover, never resend ──
        # This runs FIRST, before every other gate, because the two
        # crash shapes it repairs would otherwise be unreachable:
        #
        #   * A crash before the intervention advanced leaves it
        #     'approved'; the dry-run gate below would short-circuit
        #     when V2_ENABLE_EXTERNAL_SEND is off, so the send would
        #     never be finalized.
        #   * A crash midway through finalization leaves it 'measuring',
        #     which Gate 1 rejects as an illegal status.
        #
        # Recovery is local-only and cannot send anything, so it is safe
        # to run regardless of the send flag or the current status.
        proven = self._find_successful_attempt(intervention_id)
        if proven:
            if self.is_finalization_complete(proven, intervention):
                return {
                    "error": "already_sent",
                    "message": (
                        f"Intervention {intervention_id} was already "
                        f"successfully sent (attempt {proven.id})."
                    ),
                    "send_attempt": proven.to_dict(),
                }

            # Proven sent but local state is incomplete — converge it.
            # No provider call is made anywhere in this branch.
            log.warning(
                "Recovering unfinalized proven send for %s (attempt %s, "
                "status=%s, intervention=%s)",
                intervention_id, proven.id, proven.attempt_status.value,
                intervention.status.value,
            )
            finalize_result = self.finalize_confirmed_send(
                proven, intervention, actor=actor,
            )
            intervention = self.v2_repo.get_intervention(intervention_id)
            refreshed = self.v2_repo.get_send_attempt(proven.id) or proven
            return {
                "error": "already_sent_recovered",
                "message": (
                    f"Intervention {intervention_id} was already sent "
                    f"(attempt {proven.id}); local state was incomplete and "
                    f"has been recovered without contacting the provider."
                ),
                "recovered": True,
                "finalization": finalize_result,
                "send_attempt": refreshed.to_dict(),
                "intervention": intervention.to_dict() if intervention else None,
            }

        # ── Gate 1: status must be approved ────────────────────────────
        if intervention.status != InterventionStatus.APPROVED:
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_blocked",
                from_status=intervention.status.value,
                actor=actor,
                error=f"Cannot execute from status {intervention.status.value!r}",
            )
            return {
                "error": "illegal_status",
                "message": f"Execution requires status 'approved', got '{intervention.status.value}'",
            }

        # ── Gate 2: campaign draft must exist ──────────────────────────
        if not intervention.campaign_draft_id:
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_blocked",
                from_status=intervention.status.value,
                actor=actor,
                error="No campaign draft linked",
            )
            return {"error": "no_campaign_draft", "message": "Campaign draft is required before execution"}

        campaign = self._get_campaign_draft(intervention.campaign_draft_id)
        if not campaign:
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_blocked",
                from_status=intervention.status.value,
                actor=actor,
                error=f"Campaign draft {intervention.campaign_draft_id} not found in database",
            )
            return {"error": "campaign_draft_missing", "message": "Campaign draft record not found"}

        # ── Gate 3: suppression must be positively valid (fail-closed) ──
        supp_status, supp_details, suppressed = (
            guard_for_events(self.db, [intervention.event_id], self.suppression_guard).get_suppressions_if_valid()
        )
        if supp_status not in (SuppressionStatus.HEALTHY, SuppressionStatus.ACKNOWLEDGED_EMPTY):
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_blocked",
                from_status=intervention.status.value,
                actor=actor,
                error=f"Suppression check failed: {supp_status.value}",
                metadata={
                    "suppression_status": supp_status.value,
                    "last_synced_at": supp_details.get("last_synced_at"),
                    "row_count": supp_details.get("row_count"),
                    "reason": supp_details.get("reason", ""),
                },
            )
            return {
                "error": supp_status.value,
                "message": supp_details.get("reason", "Suppression validation failed — execution blocked for safety"),
            }

        # ── Gate 4: recompute audience at execution time ───────────────
        event = self.db.get_event(intervention.event_id)
        if not event:
            return {"error": "event_not_found"}
        event = dict(event)

        current_buyers = set(self.db.get_event_buyers(intervention.event_id))
        exclude = current_buyers | suppressed

        # Rebuild audience from scratch (never trust stale prepared audience)
        audience_emails = self._build_fresh_audience(intervention.event_id, event, exclude)
        if not audience_emails:
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_blocked",
                from_status=intervention.status.value,
                actor=actor,
                error="Audience is empty after recomputation",
            )
            return {"error": "empty_audience", "message": "No valid recipients after recomputing audience"}

        # ── Gate 5: check external send flag ───────────────────────────
        external_send_enabled = os.environ.get("V2_ENABLE_EXTERNAL_SEND", "0") == "1"

        # Compute audience hash and idempotency key (stable, deterministic)
        audience_hash = compute_audience_hash(audience_emails)
        idempotency_key = compute_idempotency_key(
            intervention_id, EXECUTION_GENERATION,
            intervention.campaign_draft_id, audience_hash,
        )

        if not external_send_enabled:
            # Dry-run: record what would happen, but do not make any
            # external HTTP calls.  Dry-run does NOT create a durable
            # claim that blocks future real execution.
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_dry_run",
                from_status=intervention.status.value,
                actor=actor,
                metadata={
                    "audience_count": len(audience_emails),
                    "audience_hash": audience_hash,
                    "idempotency_key": idempotency_key,
                    "campaign_draft_id": intervention.campaign_draft_id,
                    "reason": "V2_ENABLE_EXTERNAL_SEND is not set",
                },
            )
            return {
                "status": "external_send_disabled",
                "message": "External sending is disabled. Set V2_ENABLE_EXTERNAL_SEND=1 to enable.",
                "dry_run": {
                    "audience_count": len(audience_emails),
                    "audience_hash": audience_hash,
                    "idempotency_key": idempotency_key,
                    "campaign_draft_id": intervention.campaign_draft_id,
                    "subject_line": campaign.get("subject_line", ""),
                    "suppressed_count": len(suppressed),
                    "current_buyers_excluded": len(current_buyers),
                },
            }

        # ── Phase 2: Check for existing send attempt ──────────────────
        # Before creating a new claim, check if one already exists for
        # this intervention + execution generation.
        existing_attempt = self.v2_repo.get_active_send_attempt(
            intervention_id, EXECUTION_GENERATION,
        )
        if existing_attempt:
            if existing_attempt.requires_reconciliation:
                # Ambiguous attempt exists — cannot retry, must reconcile
                return {
                    "error": "reconciliation_required",
                    "message": (
                        f"Send attempt {existing_attempt.id} is in state "
                        f"'ambiguous'. Reconcile before retrying."
                    ),
                    "send_attempt": existing_attempt.to_dict(),
                }
            if existing_attempt.is_active:
                # An in-progress attempt exists — return its state
                return {
                    "error": "attempt_in_progress",
                    "message": (
                        f"Send attempt {existing_attempt.id} is already in "
                        f"progress (state: {existing_attempt.attempt_status.value})."
                    ),
                    "send_attempt": existing_attempt.to_dict(),
                }

        # Note: the successful-attempt check lives in Gate 0 at the top
        # of execute(), so a proven send is detected (and recovered)
        # before any gate can turn it away.

        # ── Phase 2: Claim-before-send ────────────────────────────────
        # Create durable claim BEFORE any provider mutation.
        attempt = SendAttempt(
            id=None,
            intervention_id=intervention_id,
            execution_generation=EXECUTION_GENERATION,
            attempt_status=SendAttemptStatus.CLAIMED,
            idempotency_key=idempotency_key,
            audience_hash=audience_hash,
            audience_count=len(audience_emails),
            is_dry_run=False,
        )
        try:
            attempt = self.v2_repo.create_send_attempt(attempt)
        except DuplicateClaimError as e:
            log.warning(f"Duplicate claim for {intervention_id}: {e}")
            return {
                "error": "duplicate_claim",
                "message": str(e),
            }

        self.v2_repo.append_audit(
            intervention_id, intervention.event_id,
            action="send_attempt_claimed",
            from_status=intervention.status.value,
            actor=actor,
            metadata={
                "send_attempt_id": attempt.id,
                "idempotency_key": idempotency_key,
                "audience_hash": audience_hash,
                "audience_count": len(audience_emails),
                "execution_generation": EXECUTION_GENERATION,
            },
        )

        # Stage recipients BEFORE provider send
        self.v2_repo.stage_attempt_recipients(attempt.id, audience_emails)

        # ── Execute: checkpointed send via Mailchimp ──────────────────
        try:
            send_result = self._send_via_mailchimp_checkpointed(
                attempt, intervention, campaign, audience_emails, actor,
            )
        except AmbiguousSendOutcome as e:
            # The provider may have sent. This is NOT a retryable
            # failure — surface it as its own terminal-until-reconciled
            # condition so no client treats it as "try again".
            log.error(f"Execution ambiguous for {intervention_id}: {e}")
            return {
                "error": "execution_outcome_ambiguous",
                "message": str(e),
                "reconciliation_required": True,
                "provider_campaign_id": attempt.provider_campaign_id,
                "send_attempt": attempt.to_dict(),
            }
        except Exception as e:
            # If we get here, the attempt has been checkpointed at each
            # step. The attempt status tells us exactly where it failed.
            #
            # Guard: if the checkpointed send left the attempt in an
            # ambiguous state, report it as ambiguous even though the
            # error surfaced as a generic exception. The persisted state
            # is the authority on whether a send may have happened.
            log.error(f"Execution failed for {intervention_id}: {e}")
            if attempt.attempt_status == SendAttemptStatus.AMBIGUOUS:
                return {
                    "error": "execution_outcome_ambiguous",
                    "message": str(e),
                    "reconciliation_required": True,
                    "provider_campaign_id": attempt.provider_campaign_id,
                    "send_attempt": attempt.to_dict(),
                }
            return {
                "error": "execution_failed",
                "message": str(e),
                "send_attempt": attempt.to_dict(),
            }

        # ── Local finalization ─────────────────────────────────────────
        # The provider has sent. Everything from here is local
        # bookkeeping, and it runs through the SAME idempotent finalizer
        # the crash-recovery and reconciliation paths use, so all three
        # produce identical state. If the process dies part-way through,
        # the next execute() detects the proven send at Gate 0 and
        # converges what is missing.
        self.finalize_confirmed_send(attempt, intervention, actor=actor)

        # Reload both records from the repository rather than returning
        # the pre-finalization objects still in memory, so the response
        # reports what was actually persisted — measurement window,
        # sent_count and evidence included.
        attempt = self.v2_repo.get_send_attempt(attempt.id) or attempt
        intervention = self.v2_repo.get_intervention(intervention_id) or intervention

        return {
            "status": "executed",
            "intervention_id": intervention_id,
            "sent_count": intervention.sent_count,
            "measurement_window_days": ATTRIBUTION_WINDOW_DAYS,
            "measurement_started_at": intervention.measurement_started_at,
            "measurement_ends_at": intervention.measurement_ends_at,
            "provider_sent_at": attempt.to_dict().get("provider_sent_at"),
            "send_attempt": attempt.to_dict(),
            "intervention": intervention.to_dict(),
        }

    # Clocks are compared with a tolerance rather than for equality:
    # Postgres TIMESTAMPTZ and SQLite ISO text round-trip at different
    # precisions, so sub-second drift is representation noise. Anything
    # larger means two different clocks were used and attribution would
    # be measured against the wrong instant.
    CLOCK_CONSISTENCY_TOLERANCE = timedelta(seconds=1)

    def _resolve_attribution_start(
        self, intervention: Intervention, sent_rows: List[Dict[str, Any]],
    ) -> datetime:
        """The one instant attribution measures from.

        Priority:
          1. The successful attempt's provider_sent_at — canonical, and
             the only source the finalizer also writes into the
             intervention and the recipient rows, so all three agree.
          2. The EARLIEST recipient row. For interventions predating
             Phase 2 there is no attempt, and these rows are the actual
             record of when mail went out. Earliest rather than
             arbitrary, so the answer is deterministic.
          3. measurement_started_at, then executed_at.

        Note the ordering of 2 and 3: measurement_started_at is set by
        the status transition, which for a legacy intervention can be
        later than the send. Preferring it would move those windows
        forward, so the send rows win when no attempt exists.
        """
        canonical: Optional[datetime] = None
        attempt = self._find_successful_attempt(intervention.id)
        provider_sent = (
            self._parse_provider_time(attempt.provider_sent_at)
            if attempt is not None else None
        )
        if provider_sent is not None:
            canonical = provider_sent

        if canonical is None:
            candidates = [
                self._parse_provider_time(r.get("sent_at")) for r in sent_rows
            ]
            candidates = [c for c in candidates if c is not None]
            if candidates:
                canonical = min(candidates)

        if canonical is None:
            canonical = self._parse_provider_time(
                intervention.measurement_started_at)

        if canonical is None:
            canonical = self._parse_provider_time(intervention.executed_at)

        if canonical is None:
            log.warning(
                "No usable send timestamp for %s; falling back to a "
                "full window before now, which may under-attribute.",
                intervention.id,
            )
            return datetime.now(timezone.utc) - timedelta(
                days=ATTRIBUTION_WINDOW_DAYS)

        # Only meaningful once a proven send exists: that is when all
        # three clocks are written from one source and must agree.
        if provider_sent is not None:
            self._warn_on_clock_disagreement(intervention, sent_rows, canonical)
        return canonical

    def _warn_on_clock_disagreement(
        self,
        intervention: Intervention,
        sent_rows: List[Dict[str, Any]],
        canonical: datetime,
    ) -> None:
        """Report, rather than silently tolerate, disagreeing clocks."""
        started = self._parse_provider_time(intervention.measurement_started_at)
        if started and abs(started - canonical) > self.CLOCK_CONSISTENCY_TOLERANCE:
            log.error(
                "Attribution clock disagreement for %s: "
                "measurement_started_at=%s but authoritative send=%s",
                intervention.id, started.isoformat(), canonical.isoformat(),
            )
        for row in sent_rows:
            row_ts = self._parse_provider_time(row.get("sent_at"))
            if row_ts and abs(row_ts - canonical) > self.CLOCK_CONSISTENCY_TOLERANCE:
                log.error(
                    "Attribution clock disagreement for %s: recipient "
                    "send row at %s but authoritative send=%s",
                    intervention.id, row_ts.isoformat(), canonical.isoformat(),
                )
                break

    def attribution_clock_report(self, intervention_id: str) -> Dict[str, Any]:
        """Diagnostic: are all send clocks for this intervention agreed?

        Read-only. Exposes the invariant so a drift can be detected
        without inferring it from attribution numbers.
        """
        intervention = self.v2_repo.get_intervention(intervention_id)
        if not intervention:
            return {"error": "intervention_not_found"}

        attempt = self._find_successful_attempt(intervention_id)
        sent_rows = self.v2_repo.get_sends(intervention_id)

        provider = self._parse_provider_time(
            attempt.provider_sent_at) if attempt else None
        started = self._parse_provider_time(intervention.measurement_started_at)
        row_times = [
            t for t in (self._parse_provider_time(r.get("sent_at"))
                        for r in sent_rows) if t is not None
        ]

        reference = provider or started
        consistent = True
        if reference is not None:
            for other in ([started] if started else []) + row_times:
                if abs(other - reference) > self.CLOCK_CONSISTENCY_TOLERANCE:
                    consistent = False
                    break

        return {
            "intervention_id": intervention_id,
            "provider_sent_at": provider.isoformat() if provider else None,
            "measurement_started_at": started.isoformat() if started else None,
            "recipient_sent_at_min": min(row_times).isoformat() if row_times else None,
            "recipient_sent_at_max": max(row_times).isoformat() if row_times else None,
            "recipient_rows": len(row_times),
            "tolerance_seconds": self.CLOCK_CONSISTENCY_TOLERANCE.total_seconds(),
            "consistent": consistent,
        }

    def measure(self, intervention_id: str, actor: str = "system") -> Dict[str, Any]:
        """Compute attributed outcomes for an executed intervention.

        Attribution method:
        - Only count orders from recipients in v2_campaign_sends
        - Only count orders placed after executed_at
        - Only count orders within the attribution window
        - Label as attributed_revenue, NOT incremental or causal
        """
        intervention = self.v2_repo.get_intervention(intervention_id)
        if not intervention:
            return {"error": "intervention_not_found"}

        if intervention.status != InterventionStatus.MEASURING:
            return {
                "error": "illegal_status",
                "message": f"Measurement requires status 'measuring', got '{intervention.status.value}'",
            }

        # Get sent recipients from v2_repo
        sent_rows = self.v2_repo.get_sends(intervention_id)
        sent_emails = {row["email"] for row in sent_rows}

        if not sent_emails:
            return {"error": "no_sends", "message": "No campaign sends recorded for this intervention"}

        # ── Attribution clock ──────────────────────────────────────────
        # The successful attempt's provider_sent_at is the SINGLE
        # authority for when the campaign reached customers. Recipient
        # rows carry the same value and are treated as evidence, not
        # authority: previously this read sent_rows[0]["sent_at"], which
        # was both an arbitrary row (no ordering) and — before the
        # promotion fix — the time bookkeeping ran rather than the time
        # the provider sent. A send recovered hours after a crash would
        # start its window at recovery time and silently drop every order
        # placed in between.
        executed_dt = self._resolve_attribution_start(intervention, sent_rows)

        window_end = executed_dt + timedelta(days=intervention.measurement_window)
        now = datetime.now(timezone.utc)

        # Query orders from sent recipients after send time, within window
        attributed = self._compute_attribution(
            intervention.event_id, sent_emails, executed_dt, window_end
        )

        # Update intervention
        intervention.attributed_orders = attributed["orders"]
        intervention.attributed_tickets = attributed["tickets"]
        intervention.attributed_revenue = round(attributed["revenue"], 2)
        intervention.actual_revenue = intervention.attributed_revenue
        intervention.actual_cost = 0.0  # Email channel, near-zero cost
        intervention.actual_net_value = intervention.attributed_revenue

        window_complete = now >= window_end

        if window_complete:
            # Transition: measuring → learned
            from_status = intervention.status.value
            intervention.transition_to(InterventionStatus.LEARNED)
            intervention.outcome_status = "measured"

            # Persist learning record
            self._persist_learning(intervention)

            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="measured_complete",
                from_status=from_status,
                to_status=intervention.status.value,
                actor=actor,
                metadata={
                    "attributed_orders": attributed["orders"],
                    "attributed_revenue": attributed["revenue"],
                    "window_complete": True,
                },
            )
        else:
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="measured_partial",
                from_status=intervention.status.value,
                actor=actor,
                metadata={
                    "attributed_orders": attributed["orders"],
                    "attributed_revenue": attributed["revenue"],
                    "window_complete": False,
                    "window_ends_at": window_end.isoformat(),
                },
            )

        self.v2_repo.save_intervention(intervention)

        return {
            "status": "learned" if window_complete else "measuring",
            "window_complete": window_complete,
            "window_ends_at": window_end.isoformat(),
            "predicted": {
                "expected_revenue": intervention.expected_revenue,
                "confidence": intervention.confidence,
            },
            "actual": {
                "attributed_orders": attributed["orders"],
                "attributed_tickets": attributed["tickets"],
                "attributed_revenue": round(attributed["revenue"], 2),
                "sent_count": intervention.sent_count,
            },
            "intervention": intervention.to_dict(),
        }

    # ── Private helpers ────────────────────────────────────────────────

    def _get_campaign_draft(self, campaign_id: str) -> Optional[Dict[str, Any]]:
        try:
            row = self.db.conn.execute(
                "SELECT * FROM campaigns WHERE id = ?", (campaign_id,)
            ).fetchone()
            return dict(row) if row else None
        except Exception:
            return None

    def _build_fresh_audience(self, event_id: str, event: dict, exclude: set) -> List[str]:
        """Rebuild audience from scratch at execution time."""
        emails = set()

        try:
            buyers = list(exclude)  # exclude set already has buyers
            past = self.db.get_past_attendees_not_purchased(
                event_id, event.get("name", ""), limit=50000,
                current_buyer_emails=buyers,
            )
            for c in past:
                if c.get("email") and c["email"] not in exclude:
                    emails.add(c["email"])
        except Exception as e:
            log.warning(f"Past attendees lookup failed during execution: {e}")

        try:
            city = event.get("city", "")
            if city:
                city_prospects = self.db.get_city_prospects(
                    city, exclude_emails=list(exclude | emails), limit=50000
                )
                for c in city_prospects:
                    if c.get("email"):
                        emails.add(c["email"])
        except Exception as e:
            log.warning(f"City prospects lookup failed during execution: {e}")

        return list(emails)

    def _send_via_mailchimp_checkpointed(
        self,
        attempt: SendAttempt,
        intervention: Intervention,
        campaign: Dict[str, Any],
        audience_emails: List[str],
        actor: str,
    ) -> Dict[str, Any]:
        """Checkpointed send through Mailchimp.

        Each irreversible provider step is persisted to the DB BEFORE
        proceeding to the next.  If the process crashes at any point,
        the attempt state tells us exactly where we were and what
        provider resources were created.

        State progression:
            claimed → provider_campaign_created → audience_configured
            → send_requested → confirmed_sent | ambiguous

        Any step may instead go to `ambiguous` if its response is lost.

        ── RETRY INVARIANT ──────────────────────────────────────────
        Every transition into a retryable state below is justified by
        proof that no recipient could have received mail:

          1. Mailchimp not configured  — no provider contact occurred.
          2. ensure_members failed     — consent validation / segment
                                         preparation; no campaign created.
          3. get_tag_segment_id failed — read-only lookup.
          4. create definite failure   — campaign rejected, or created
                                         with no content. A contentless
                                         campaign has no send action
                                         behind it.
          5. send definite failure     — provider returned one of
                                         400/401/403/404/405/422, which
                                         proves it rejected the request
                                         before acting on it.

        Steps 1-3 run before any campaign exists, so a broad `except`
        there is safe.  Steps 4-5 are gated on explicit `is_definite_
        failure` checks, never on a bare exception.  Everything else —
        every 5xx, every timeout, every reset, every unparseable body —
        routes to `ambiguous` and blocks retry until reconciliation.

        Preparation never creates or subscribes contacts. It requires consent
        in the destination audience, creates a fresh attempt-specific segment,
        and verifies its exact membership. A failed preparation can leave an
        unused segment but cannot fall back to the entire audience. Review any
        provider automations triggered by segment/tag changes before enabling
        external sending; this method itself does not dispatch a campaign until
        the checkpointed send action below.
        """
        if not self.campaign_engine or not self.campaign_engine.mailchimp:
            attempt.transition_to(SendAttemptStatus.FAILED_PRE_SEND)
            attempt.error_message = "Mailchimp not configured"
            self.v2_repo.update_send_attempt(attempt)
            raise RuntimeError("Mailchimp not configured — cannot send")

        try:
            mc = self.campaign_engine.mailchimp_for_event(intervention.event_id)
        except Exception as exc:
            attempt.transition_to(SendAttemptStatus.FAILED_PRE_SEND)
            attempt.error_message = 'Verified festival audience mapping required'
            self.v2_repo.update_send_attempt(attempt)
            raise RuntimeError(attempt.error_message) from exc
        tag_name = f"v2-{intervention.id}-{attempt.id}"

        # ── Step 1: Push audience to Mailchimp ────────────────────────
        try:
            member_stats = mc.ensure_members(audience_emails, tag=tag_name)
            if not isinstance(member_stats, dict) or member_stats.get('errors', 1):
                raise RuntimeError('Mailchimp did not confirm the complete audience')
        except Exception as e:
            attempt.transition_to(SendAttemptStatus.FAILED_PRE_SEND)
            attempt.error_message = f"ensure_members failed: {e}"
            self.v2_repo.update_send_attempt(attempt)
            raise RuntimeError(f"Mailchimp audience push failed: {e}") from e

        # ── Step 2: Get segment ID ────────────────────────────────────
        try:
            segment_id = mc.get_tag_segment_id(tag_name)
            if not isinstance(segment_id, int) or isinstance(segment_id, bool) or segment_id <= 0:
                raise RuntimeError('A verified segment is required; whole-audience fallback is forbidden')
        except Exception as e:
            attempt.transition_to(SendAttemptStatus.FAILED_PRE_SEND)
            attempt.error_message = f"get_tag_segment_id failed: {e}"
            self.v2_repo.update_send_attempt(attempt)
            raise RuntimeError(f"Mailchimp segment lookup failed: {e}") from e

        # ── Step 3: Create campaign (IRREVERSIBLE) ────────────────────
        # A campaign that exists but was never sent harms no customer.
        # The danger here is losing its ID: an unnamed campaign cannot
        # be reconciled later, so an uncertain creation must NOT become
        # retryable.
        create_outcome = mc.create_campaign_strict(
            subject=campaign.get("subject_line", ""),
            preview_text=campaign.get("preview_text", ""),
            html=campaign.get("body_html", ""),
            segment_id=segment_id,
        )

        if create_outcome.is_ambiguous:
            # We may have created a campaign we cannot address.  Nothing
            # was sent, but blindly creating another one on retry would
            # accumulate orphans and, worse, hide the unnamed campaign.
            log.error(
                f"AMBIGUOUS campaign creation for attempt {attempt.id}: "
                f"{create_outcome.error_type}"
            )
            attempt.transition_to(SendAttemptStatus.AMBIGUOUS)
            attempt.error_message = (
                create_outcome.error_message or "Campaign creation outcome unknown"
            )
            attempt.error_detail = create_outcome.to_dict()
            self.v2_repo.update_send_attempt(attempt)
            self.v2_repo.append_audit(
                intervention.id, intervention.event_id,
                action="send_attempt_ambiguous",
                actor=actor,
                metadata={
                    "send_attempt_id": attempt.id,
                    "provider_operation": "create_campaign",
                    "provider_campaign_id": create_outcome.provider_campaign_id,
                    "error_type": create_outcome.error_type,
                    "http_status": create_outcome.http_status,
                },
                error="Campaign creation outcome uncertain — manual review required",
            )
            raise RuntimeError(
                f"Campaign creation outcome uncertain for attempt "
                f"{attempt.id}. Manual review required before retrying."
            )

        if create_outcome.is_definite_failure:
            # Proven not created, or created-without-content. Either way
            # nothing was dispatched to any recipient.
            attempt.transition_to(SendAttemptStatus.FAILED_PRE_SEND)
            attempt.error_message = (
                create_outcome.error_message or "Campaign creation failed"
            )
            attempt.error_detail = create_outcome.to_dict()
            if create_outcome.provider_campaign_id:
                # Orphaned shell — record the ID so it can be cleaned up.
                attempt.provider_campaign_id = create_outcome.provider_campaign_id
            self.v2_repo.update_send_attempt(attempt)
            raise RuntimeError(
                f"Mailchimp campaign creation failed: "
                f"{create_outcome.error_type}"
            )

        mc_campaign_id = create_outcome.provider_campaign_id

        # CHECKPOINT: campaign created — persist provider_campaign_id
        attempt.provider_campaign_id = mc_campaign_id
        attempt.provider_tag = tag_name
        attempt.provider_segment_id = segment_id
        attempt.transition_to(SendAttemptStatus.PROVIDER_CAMPAIGN_CREATED)
        self.v2_repo.update_send_attempt(attempt)
        log.info(f"Checkpoint: campaign {mc_campaign_id} created for attempt {attempt.id}")

        # CHECKPOINT: audience configured
        attempt.transition_to(SendAttemptStatus.AUDIENCE_CONFIGURED)
        self.v2_repo.update_send_attempt(attempt)

        # ── Step 4: Send campaign (CRITICAL — THE DANGER ZONE) ────────
        # After this point, the provider may have accepted the send.
        # We transition to send_requested BEFORE calling send, so that
        # if we crash during the call, we know we were in the danger zone.
        attempt.transition_to(SendAttemptStatus.SEND_REQUESTED)
        self.v2_repo.update_send_attempt(attempt)
        log.info(f"Checkpoint: send_requested for attempt {attempt.id}, "
                 f"campaign {mc_campaign_id}")

        # send_campaign_strict never raises for provider conditions: it
        # returns a three-valued outcome.  A bare `except Exception` here
        # would re-introduce the very bug this design removes, so the
        # only exceptions we catch are genuinely unexpected local ones —
        # and those are ALSO treated as ambiguous, because the request
        # had already been dispatched by then.
        try:
            send_outcome = mc.send_campaign_strict(mc_campaign_id)
        except Exception as e:
            log.error(f"AMBIGUOUS: unexpected error during send for attempt "
                      f"{attempt.id}: {type(e).__name__}")
            send_outcome = ProviderSendOutcome(
                status=ProviderSendStatus.AMBIGUOUS,
                provider_campaign_id=mc_campaign_id,
                error_type=type(e).__name__,
                error_message="Unexpected error after send dispatch",
                detail="unexpected_exception_after_dispatch",
            )

        if send_outcome.is_ambiguous:
            # The provider may have accepted and sent the campaign.
            # This state BLOCKS retry until reconciliation proves what
            # actually happened.
            log.error(
                f"AMBIGUOUS send for attempt {attempt.id}, campaign "
                f"{mc_campaign_id}: {send_outcome.error_type}"
            )
            attempt.transition_to(SendAttemptStatus.AMBIGUOUS)
            attempt.error_message = (
                send_outcome.error_message or "Send outcome unknown"
            )
            attempt.error_detail = send_outcome.to_dict()
            self.v2_repo.update_send_attempt(attempt)
            self.v2_repo.append_audit(
                intervention.id, intervention.event_id,
                action="send_attempt_ambiguous",
                actor=actor,
                metadata={
                    "send_attempt_id": attempt.id,
                    "provider_campaign_id": mc_campaign_id,
                    "provider_operation": "send_campaign",
                    "error_type": send_outcome.error_type,
                    "http_status": send_outcome.http_status,
                    "detail": send_outcome.detail,
                },
                error="Provider outcome uncertain — reconciliation required",
            )
            raise AmbiguousSendOutcome(
                f"Send outcome uncertain for campaign {mc_campaign_id}. "
                f"Attempt {attempt.id} marked ambiguous — reconcile before "
                f"retrying."
            )

        if send_outcome.is_definite_failure:
            # The provider returned a status code that PROVES the send
            # was rejected before it was processed.  Only these codes may
            # release the claim and allow a retry.
            attempt.transition_to(SendAttemptStatus.FAILED_PRE_SEND)
            attempt.error_message = (
                send_outcome.error_message
                or f"Provider rejected send for {mc_campaign_id}"
            )
            attempt.error_detail = send_outcome.to_dict()
            self.v2_repo.update_send_attempt(attempt)
            raise RuntimeError(
                f"Mailchimp rejected the send for campaign {mc_campaign_id} "
                f"(HTTP {send_outcome.http_status}) — nothing was sent."
            )

        # CHECKPOINT: confirmed sent
        # Mailchimp's send action returns 204 with no body, so it gives
        # us no send timestamp. The moment we received the success
        # response is the closest defensible answer, and it is recorded
        # here — at the boundary — rather than derived later, so a
        # delayed recovery cannot drift the attribution window forward.
        attempt.provider_sent_at = datetime.now(timezone.utc)
        attempt.transition_to(SendAttemptStatus.CONFIRMED_SENT)
        self.v2_repo.update_send_attempt(attempt)
        log.info(f"Checkpoint: confirmed_sent for attempt {attempt.id}, "
                 f"campaign {mc_campaign_id}")

        self.v2_repo.append_audit(
            intervention.id, intervention.event_id,
            action="send_attempt_confirmed",
            actor=actor,
            metadata={
                "send_attempt_id": attempt.id,
                "provider_campaign_id": mc_campaign_id,
                "audience_count": len(audience_emails),
                "member_stats": member_stats,
            },
        )

        return {
            "send_attempt_id": attempt.id,
            "mailchimp_campaign_id": mc_campaign_id,
            "member_stats": member_stats,
            "tag_name": tag_name,
        }

    # ─────────────────────────────────────────────────────────────────
    # Local finalization after a PROVEN provider send
    # ─────────────────────────────────────────────────────────────────
    #
    # Splitting the send into "provider call" and "local bookkeeping"
    # creates a window: the provider has sent, the attempt says
    # confirmed_sent, but the intervention has not advanced, recipients
    # are still only staged, and no attribution rows exist.  A crash in
    # that window leaves the system duplicate-safe (the successful
    # attempt blocks any resend) but with permanently incomplete local
    # state — measurement would never run.
    #
    # finalize_confirmed_send converges that state.  It is idempotent,
    # it never contacts the provider, and it is the single owner of
    # post-send local work for BOTH the direct-confirmed and the
    # reconciled-confirmed paths, so the two cannot drift.

    FINALIZATION_AUDIT_ACTION = "send_finalized"

    def _find_successful_attempt(
        self, intervention_id: str,
    ) -> Optional[SendAttempt]:
        """Return the proven-sent attempt for this generation, if any.

        Successful means confirmed_sent or reconciled_sent — i.e. the
        provider is known to have sent. Dry runs never count.
        """
        for prev in self.v2_repo.get_send_attempts(intervention_id):
            if (prev.execution_generation == EXECUTION_GENERATION
                    and prev.is_successful and not prev.is_dry_run):
                return prev
        return None

    def _finalization_audit_exists(self, intervention_id: str,
                                   attempt_id: int) -> bool:
        """Has this attempt already been finalized, per the audit log?

        Existence check rather than a DB constraint: the audit table is
        append-only by design, so we keep it that way and simply refuse
        to append a second finalization entry for the same attempt.
        """
        for entry in self.v2_repo.get_audit_log(intervention_id):
            if entry.get("action") != self.FINALIZATION_AUDIT_ACTION:
                continue
            meta = entry.get("metadata") or {}
            if isinstance(meta, dict) and meta.get("send_attempt_id") == attempt_id:
                return True
        return False

    def is_finalization_complete(
        self, attempt: SendAttempt, intervention: Intervention,
    ) -> bool:
        """Is local state fully converged for a proven-sent attempt?

        Deliberately checks several independent indicators. Any single
        one could be true while the rest are missing — that is exactly
        the partial-crash shape this exists to detect.
        """
        if not attempt.is_successful:
            return False

        # 1. Intervention has advanced past approved.
        if intervention.status not in _POST_EXECUTION_STATUSES:
            return False

        # 2. sent_count reflects the staged audience.
        staged = self.v2_repo.get_attempt_recipients(attempt.id)
        if intervention.sent_count != len(staged):
            return False

        # 3. Attribution rows exist for every staged recipient.
        promoted = {
            (s.get("email") or "").lower().strip()
            for s in self.v2_repo.get_sends(intervention.id)
        }
        if not {e.lower().strip() for e in staged}.issubset(promoted):
            return False

        # 4. Measurement window is set.
        if not intervention.measurement_started_at or not intervention.measurement_ends_at:
            return False

        # 5. Finalization is recorded in the audit trail.
        if not self._finalization_audit_exists(intervention.id, attempt.id):
            return False

        return True

    @staticmethod
    def _parse_provider_time(value: Any) -> Optional[datetime]:
        """Parse a provider timestamp into an aware UTC datetime."""
        if not value:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    def _resolve_provider_sent_at(self, attempt: SendAttempt) -> datetime:
        """Best-known moment the provider actually sent.

        Priority:
          1. Already-persisted provider_sent_at (never re-derive; the
             first answer is the closest to the event).
          2. Mailchimp's own send_time from reconciliation.
          3. The moment we received the provider's success response,
             i.e. completed_at on the direct path.
          4. send_requested_at — a lower bound, used only when nothing
             better exists.
          5. Now, if the attempt carries no usable timestamp at all.

        Never the claim time: that precedes the send by the whole
        campaign-setup sequence.
        """
        existing = self._parse_provider_time(attempt.provider_sent_at)
        if existing:
            return existing

        detail = attempt.reconciliation_detail or {}
        if isinstance(detail, dict):
            from_provider = self._parse_provider_time(detail.get("send_time"))
            if from_provider:
                return from_provider

        completed = self._parse_provider_time(attempt.completed_at)
        if completed:
            return completed

        requested = self._parse_provider_time(attempt.send_requested_at)
        if requested:
            return requested

        return datetime.now(timezone.utc)

    def finalize_confirmed_send(
        self,
        attempt: SendAttempt,
        intervention: Intervention,
        actor: str = "system",
    ) -> Dict[str, Any]:
        """Idempotently converge local state for a proven-sent attempt.

        Never contacts the provider. Safe to call repeatedly. Returns a
        dict describing what (if anything) it had to repair.
        """
        if not attempt.is_successful:
            return {
                "error": "attempt_not_successful",
                "message": (
                    f"Finalization requires a proven send; attempt "
                    f"{attempt.id} is '{attempt.attempt_status.value}'."
                ),
            }

        if self.is_finalization_complete(attempt, intervention):
            return {"status": "already_finalized", "repaired": False}

        # Authoritative send time drives the attribution window. Persist
        # it on the attempt so later passes reuse the same answer rather
        # than drifting toward the current clock.
        provider_sent_at = self._resolve_provider_sent_at(attempt)
        if not attempt.provider_sent_at:
            attempt.provider_sent_at = provider_sent_at
            self.v2_repo.update_send_attempt(attempt)

        staged = self.v2_repo.get_attempt_recipients(attempt.id)
        from_status = intervention.status.value

        # Advance the intervention if it has not already moved. A crash
        # may have left it anywhere between approved and measuring.
        if intervention.status == InterventionStatus.APPROVED:
            intervention.transition_to(InterventionStatus.EXECUTING)
        if intervention.status == InterventionStatus.EXECUTING:
            intervention.transition_to(InterventionStatus.MEASURING)

        # transition_to() stamps the window from "now", which for a
        # reconciled send can be hours after the mail actually went out.
        # Overwrite with the authoritative provider send time so
        # attribution measures from when customers received it.
        intervention.executed_at = provider_sent_at.isoformat()
        intervention.measurement_started_at = provider_sent_at.isoformat()
        intervention.measurement_ends_at = (
            provider_sent_at + timedelta(days=ATTRIBUTION_WINDOW_DAYS)
        ).isoformat()
        intervention.measurement_window = ATTRIBUTION_WINDOW_DAYS
        intervention.sent_count = len(staged)
        intervention.evidence["execution_result"] = {
            "send_attempt_id": attempt.id,
            "mailchimp_campaign_id": attempt.provider_campaign_id,
            "provider_sent_at": provider_sent_at.isoformat(),
            "attempt_status": attempt.attempt_status.value,
        }

        audit_already_present = self._finalization_audit_exists(
            intervention.id, attempt.id,
        )

        if audit_already_present:
            # Everything except the audit entry may still need repair,
            # but we must not append a second finalization record.
            self.v2_repo.promote_attempt_recipients(
                attempt.id, intervention.id, intervention.campaign_draft_id,
                sent_at=provider_sent_at,
            )
            self.v2_repo.save_intervention(intervention)
        else:
            # One transaction: promote recipients, save intervention,
            # record finalization.
            self.v2_repo.finalize_send_locally(
                attempt_id=attempt.id,
                intervention=intervention,
                event_id=intervention.event_id,
                # Recipient rows are stamped with when the PROVIDER sent,
                # not when this transaction runs. Attribution reads those
                # rows, so using the transaction clock would move the
                # window to whenever recovery happened to occur.
                sent_at=provider_sent_at,
                audit_action=self.FINALIZATION_AUDIT_ACTION,
                from_status=from_status,
                actor=actor,
                audit_metadata={
                    "send_attempt_id": attempt.id,
                    "attempt_status": attempt.attempt_status.value,
                    "provider_campaign_id": attempt.provider_campaign_id,
                    "provider_sent_at": provider_sent_at.isoformat(),
                    "sent_count": len(staged),
                    "measurement_window_days": ATTRIBUTION_WINDOW_DAYS,
                    "measurement_ends_at": intervention.measurement_ends_at,
                    "execution_generation": attempt.execution_generation,
                },
            )

        log.info(
            "Finalized send attempt %s for %s (status=%s, recipients=%s)",
            attempt.id, intervention.id, intervention.status.value, len(staged),
        )
        return {
            "status": "finalized",
            "repaired": True,
            "sent_count": len(staged),
            "provider_sent_at": provider_sent_at.isoformat(),
        }

    def reconcile_send_attempt(
        self, intervention_id: str, actor: str = "system",
    ) -> Dict[str, Any]:
        """Reconcile an ambiguous send attempt by querying the provider.

        Queries Mailchimp for the campaign status using the stored
        provider_campaign_id.  If the campaign was sent, transitions
        to reconciled_sent and promotes recipients.  If not, transitions
        to reconciled_not_sent.

        This is the ONLY path from ambiguous to a terminal state.
        """
        intervention = self.v2_repo.get_intervention(intervention_id)
        if not intervention:
            return {"error": "intervention_not_found"}

        # Find the ambiguous attempt
        attempts = self.v2_repo.get_send_attempts(intervention_id)
        ambiguous = None
        for a in attempts:
            if (a.execution_generation == EXECUTION_GENERATION
                    and a.attempt_status == SendAttemptStatus.AMBIGUOUS
                    and not a.is_dry_run):
                ambiguous = a
                break

        if not ambiguous:
            return {
                "error": "no_ambiguous_attempt",
                "message": f"No ambiguous send attempt found for {intervention_id}",
            }

        if not ambiguous.provider_campaign_id:
            # We hold no provider campaign ID, so there is nothing to
            # query. The attempt STAYS ambiguous — absence of an ID is
            # not evidence that nothing was created or sent.
            return {
                "error": "no_provider_campaign_id",
                "message": (
                    "Cannot reconcile: no provider_campaign_id recorded. "
                    "A provider campaign may exist that we cannot address. "
                    "Manual review required; retry remains blocked."
                ),
                "reconciliation_required": True,
                "send_attempt": ambiguous.to_dict(),
            }

        # Query provider for campaign status
        if not self.campaign_engine or not self.campaign_engine.mailchimp:
            return {
                "error": "mailchimp_not_configured",
                "message": "Cannot reconcile: Mailchimp not configured",
            }

        mc = self.campaign_engine.mailchimp
        try:
            provider_response = mc.get_campaign_status(
                ambiguous.provider_campaign_id
            )
        except ProviderError as e:
            # We could not ask. That is NOT an answer — the attempt
            # stays ambiguous and stays blocked.
            return {
                "error": "provider_query_failed",
                "message": (
                    f"Could not query Mailchimp for campaign "
                    f"{ambiguous.provider_campaign_id}. Try again later."
                ),
                "reconciliation_required": True,
                "send_attempt": ambiguous.to_dict(),
            }

        provider_result = provider_response.body
        # Note: no `.get(..., "unknown")` default here. A missing status
        # must stay None so the classifier sees absence-of-evidence
        # rather than a string that could accidentally match a rule.
        campaign_status = provider_result.get("status")
        emails_sent = provider_result.get("emails_sent")
        send_time = provider_result.get("send_time")

        verdict = classify_reconciliation_status(
            provider_status=campaign_status,
            emails_sent=emails_sent,
            send_time=send_time,
        )

        ambiguous.reconciliation_detail = {
            "provider_status": campaign_status,
            "emails_sent": emails_sent,
            "send_time": send_time,
            "verdict": verdict.value,
            "query_time": datetime.now(timezone.utc).isoformat(),
        }

        if verdict == ReconciliationVerdict.UNKNOWN:
            # The provider answered, but the answer does not prove
            # whether the send happened. Persist the evidence, keep the
            # attempt ambiguous, and do NOT unlock a retry.
            self.v2_repo.update_send_attempt(ambiguous)
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="send_attempt_reconciliation_inconclusive",
                actor=actor,
                metadata={
                    "send_attempt_id": ambiguous.id,
                    "provider_campaign_id": ambiguous.provider_campaign_id,
                    "provider_status": campaign_status,
                    "verdict": verdict.value,
                },
                error="Provider state does not prove whether the send occurred",
            )
            return {
                "status": "provider_state_still_ambiguous",
                "message": (
                    f"Mailchimp reports campaign status "
                    f"{campaign_status!r}, which does not prove whether the "
                    f"send occurred. The attempt remains ambiguous and "
                    f"retry stays blocked. Manual review required."
                ),
                "provider_status": campaign_status,
                "reconciliation_required": True,
                "send_attempt": ambiguous.to_dict(),
            }

        if verdict == ReconciliationVerdict.SENT:
            # Provider confirms: campaign was sent.
            #
            # Adopt the provider's own send_time as the authoritative
            # moment BEFORE finalizing, so the attribution window starts
            # when customers actually received the email rather than
            # whenever this reconciliation happens to run — which could
            # be hours later.
            provider_time = self._parse_provider_time(send_time)
            if provider_time:
                ambiguous.provider_sent_at = provider_time

            ambiguous.transition_to(SendAttemptStatus.RECONCILED_SENT)
            ambiguous.reconciled_by = actor
            self.v2_repo.update_send_attempt(ambiguous)

            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="send_attempt_reconciled_sent",
                actor=actor,
                metadata={
                    "send_attempt_id": ambiguous.id,
                    "provider_campaign_id": ambiguous.provider_campaign_id,
                    "provider_status": campaign_status,
                    "provider_send_time": send_time,
                },
            )

            # Exactly the same local finalization the direct-confirmed
            # and crash-recovery paths use. One owner for intervention
            # advancement, recipient promotion, measurement setup and
            # the finalization audit, so the paths cannot diverge.
            finalize_result = self.finalize_confirmed_send(
                ambiguous, intervention, actor=actor,
            )
            intervention = self.v2_repo.get_intervention(intervention_id)
            refreshed = self.v2_repo.get_send_attempt(ambiguous.id) or ambiguous

            return {
                "status": "reconciled_sent",
                "message": "Provider confirms campaign was sent.",
                "provider_status": campaign_status,
                "finalization": finalize_result,
                "send_attempt": refreshed.to_dict(),
                "intervention": intervention.to_dict() if intervention else None,
            }
        else:
            # verdict is DEFINITELY_NOT_SENT — the provider proved the
            # campaign is still a draft ('save') with no delivered mail
            # and no send_time. This is the ONLY path that unlocks a
            # retry, and it is reached only via the explicit allowlist
            # in classify_reconciliation_status().
            ambiguous.transition_to(SendAttemptStatus.RECONCILED_NOT_SENT)
            ambiguous.reconciled_by = actor
            self.v2_repo.update_send_attempt(ambiguous)

            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="send_attempt_reconciled_not_sent",
                actor=actor,
                metadata={
                    "send_attempt_id": ambiguous.id,
                    "provider_campaign_id": ambiguous.provider_campaign_id,
                    "provider_status": campaign_status,
                },
            )

            return {
                "status": "reconciled_not_sent",
                "message": (
                    f"Provider proves the campaign was never dispatched "
                    f"(status '{campaign_status}', no delivered mail, no "
                    f"send time). Safe to retry."
                ),
                "provider_status": campaign_status,
                "send_attempt": ambiguous.to_dict(),
            }

    def _record_sends(self, intervention_id: str, campaign_draft_id: str,
                       emails: List[str]) -> None:
        """Record sent recipients for attribution tracking via v2_repo."""
        self.v2_repo.record_sends(intervention_id, campaign_draft_id, emails)

    def _compute_attribution(
        self, event_id: str, sent_emails: set,
        executed_dt: datetime, window_end: datetime,
    ) -> Dict[str, Any]:
        """Count orders from sent recipients within the attribution window.

        Only counts orders where:
        1. The order's email is in the sent_emails set
        2. The order was placed AFTER the send time (executed_dt)
        3. The order was placed BEFORE the window end
        """
        start_ts = executed_dt.isoformat()
        end_ts = window_end.isoformat()

        try:
            rows = self.db.conn.execute(
                """SELECT email, SUM(ticket_count) as tickets, SUM(gross_amount) as revenue,
                          COUNT(*) as order_count
                   FROM orders
                   WHERE event_id = ?
                     AND order_timestamp >= ?
                     AND order_timestamp <= ?
                   GROUP BY email""",
                (event_id, start_ts, end_ts),
            ).fetchall()
        except Exception:
            return {"orders": 0, "tickets": 0, "revenue": 0.0}

        total_orders = 0
        total_tickets = 0
        total_revenue = 0.0

        for row in rows:
            if row["email"] in sent_emails:
                total_orders += int(row["order_count"] or 0)
                total_tickets += int(row["tickets"] or 0)
                total_revenue += float(row["revenue"] or 0)

        return {
            "orders": total_orders,
            "tickets": total_tickets,
            "revenue": round(total_revenue, 2),
        }

    def _persist_learning(self, intervention: Intervention) -> None:
        """Write a learning record via v2_repo when an intervention reaches 'learned'."""
        try:
            event = self.db.get_event(intervention.event_id)
            event = dict(event) if event else {}

            predicted = intervention.expected_revenue
            attributed = intervention.attributed_revenue or 0.0
            error = attributed - predicted

            conversion_assumptions = intervention.evidence.get("campaign_draft", {}).get(
                "conversion_assumptions", {}
            )
            audience_count = conversion_assumptions.get("audience_count", 0)
            actual_rate = (
                (intervention.attributed_orders or 0) / intervention.sent_count
                if intervention.sent_count and intervention.sent_count > 0
                else None
            )

            self.v2_repo.save_learning({
                "intervention_id": intervention.id,
                "intervention_type": intervention.intervention_type,
                "event_id": intervention.event_id,
                "event_type": event.get("event_type", ""),
                "city": event.get("city", ""),
                "predicted_revenue": predicted,
                "attributed_revenue": attributed,
                "prediction_error": round(error, 2),
                "audience_count": audience_count,
                "sent_count": intervention.sent_count or 0,
                "attributed_orders": intervention.attributed_orders or 0,
                "attributed_tickets": intervention.attributed_tickets or 0,
                "actual_conversion_rate": round(actual_rate, 6) if actual_rate is not None else None,
                "conversion_assumptions": conversion_assumptions,
                "confidence": intervention.confidence,
                "measurement_window_days": intervention.measurement_window,
                "measurement_started_at": intervention.measurement_started_at,
                "measurement_ended_at": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            log.error(f"Failed to persist learning record for {intervention.id}: {e}")
