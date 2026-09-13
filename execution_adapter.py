"""Safe CRM execution adapter for V2 interventions.

Handles the approved → executing → measuring path for CRM email campaigns.
Actual external sends are gated behind V2_ENABLE_EXTERNAL_SEND=1.
Without that flag, execution produces a dry-run/preflight result with
no external HTTP side effects.

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
from suppression_guard import SuppressionGuard, SuppressionStatus

log = logging.getLogger("craft.execution")

# Default attribution window in days
ATTRIBUTION_WINDOW_DAYS = 7


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
            self.suppression_guard.get_suppressions_if_valid()
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

        if not external_send_enabled:
            # Dry-run: record what would happen, but do not make any external HTTP calls
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_dry_run",
                from_status=intervention.status.value,
                actor=actor,
                metadata={
                    "audience_count": len(audience_emails),
                    "campaign_draft_id": intervention.campaign_draft_id,
                    "reason": "V2_ENABLE_EXTERNAL_SEND is not set",
                },
            )
            return {
                "status": "external_send_disabled",
                "message": "External sending is disabled. Set V2_ENABLE_EXTERNAL_SEND=1 to enable.",
                "dry_run": {
                    "audience_count": len(audience_emails),
                    "campaign_draft_id": intervention.campaign_draft_id,
                    "subject_line": campaign.get("subject_line", ""),
                    "suppressed_count": len(suppressed),
                    "current_buyers_excluded": len(current_buyers),
                },
            }

        # ── Execute: send via Mailchimp ────────────────────────────────
        try:
            send_result = self._send_via_mailchimp(
                intervention, campaign, audience_emails, actor
            )
        except Exception as e:
            log.error(f"Execution failed for {intervention_id}: {e}")
            self.v2_repo.append_audit(
                intervention_id, intervention.event_id,
                action="execute_failed",
                from_status=intervention.status.value,
                actor=actor,
                error=str(e),
            )
            # Do NOT advance state on failure
            intervention.evidence["execution_error"] = str(e)
            self.v2_repo.save_intervention(intervention)
            return {"error": "execution_failed", "message": str(e)}

        # ── Transition: approved → executing → measuring ───────────────
        from_status = intervention.status.value
        intervention.transition_to(InterventionStatus.EXECUTING)
        intervention.transition_to(InterventionStatus.MEASURING)
        intervention.sent_count = len(audience_emails)
        intervention.evidence["execution_result"] = send_result
        intervention.measurement_window = ATTRIBUTION_WINDOW_DAYS
        self.v2_repo.save_intervention(intervention)

        # Record sends for attribution
        self._record_sends(intervention_id, intervention.campaign_draft_id, audience_emails)

        self.v2_repo.append_audit(
            intervention_id, intervention.event_id,
            action="executed",
            from_status=from_status,
            to_status=intervention.status.value,
            actor=actor,
            metadata={
                "sent_count": len(audience_emails),
                "campaign_draft_id": intervention.campaign_draft_id,
                "mailchimp_campaign_id": send_result.get("mailchimp_campaign_id", ""),
                "measurement_window_days": ATTRIBUTION_WINDOW_DAYS,
                "measurement_ends_at": intervention.measurement_ends_at,
            },
        )

        return {
            "status": "executed",
            "intervention_id": intervention_id,
            "sent_count": len(audience_emails),
            "measurement_window_days": ATTRIBUTION_WINDOW_DAYS,
            "measurement_ends_at": intervention.measurement_ends_at,
            "intervention": intervention.to_dict(),
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
        sent_at_str = sent_rows[0]["sent_at"] if sent_rows else intervention.executed_at

        if not sent_emails:
            return {"error": "no_sends", "message": "No campaign sends recorded for this intervention"}

        # Parse measurement window
        try:
            executed_dt = datetime.fromisoformat(sent_at_str)
        except (ValueError, TypeError):
            executed_dt = datetime.now(timezone.utc) - timedelta(days=ATTRIBUTION_WINDOW_DAYS)

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

    def _send_via_mailchimp(
        self, intervention: Intervention, campaign: Dict[str, Any],
        audience_emails: List[str], actor: str,
    ) -> Dict[str, Any]:
        """Send the campaign through Mailchimp. Raises on failure."""
        if not self.campaign_engine or not self.campaign_engine.mailchimp:
            raise RuntimeError("Mailchimp not configured — cannot send")

        mc = self.campaign_engine.mailchimp
        tag_name = f"v2-{intervention.id}"

        # Push audience to Mailchimp
        member_stats = mc.ensure_members(audience_emails, tag=tag_name)

        # Get segment for tag
        segment_id = mc.get_tag_segment_id(tag_name)

        # Create campaign
        mc_campaign_id = mc.create_campaign(
            subject=campaign.get("subject_line", ""),
            preview_text=campaign.get("preview_text", ""),
            html=campaign.get("body_html", ""),
            segment_id=segment_id,
        )
        if not mc_campaign_id:
            raise RuntimeError("Mailchimp campaign creation failed")

        # Send
        sent_ok = mc.send_campaign(mc_campaign_id)
        if not sent_ok:
            raise RuntimeError(f"Mailchimp send failed for campaign {mc_campaign_id}")

        return {
            "mailchimp_campaign_id": mc_campaign_id,
            "member_stats": member_stats,
            "tag_name": tag_name,
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
