"""Craft Dominant V2 application wrapper.

Builds on the existing Flask application without rewriting it. The V2 layer adds
an authenticated, read-only Command API backed by the deterministic Opportunity
Engine while hardening known null-aggregate behavior in the legacy intelligence
routes.

V2.1 adds: diagnosis, intervention management, and campaign draft preparation.
"""

from __future__ import annotations

import hmac
import logging
import os
from functools import wraps

from flask import jsonify, request

from campaign_adapter import CampaignDraftAdapter
from craft_unified import Database, DecisionEngine, create_app
from diagnosis_engine import DiagnosisEngine
from dominant_agent import OpportunityEngine
from execution_adapter import ExecutionAdapter
from intervention_model import (
    IllegalTransition,
    Intervention,
    InterventionStatus,
)
from v2_backend import init_v2_backend

log = logging.getLogger("craft.v2")


class ResilientDatabase(Database):
    """Small compatibility layer while legacy data paths are being refactored."""

    def get_event_profile_stats(self, event_type: str, city: str) -> dict:
        stats = super().get_event_profile_stats(event_type, city) or {}
        # SQLite AVG returns NULL for an empty group. Legacy intelligence routes
        # round these values directly, so normalize numeric aggregate NULLs here.
        for key in (
            "total_customers",
            "avg_ltv",
            "avg_order_value",
            "avg_tickets",
            "avg_price_sensitivity",
            "avg_social_influence",
            "superspreader_count",
            "vip_count",
            "accelerating_count",
            "dormant_count",
        ):
            if stats.get(key) is None:
                stats[key] = 0
        return stats


def _build_app():
    db = ResilientDatabase(os.environ.get("DB_PATH", "craft_unified.db"))
    app = create_app(db, auto_sync=os.environ.get("CRAFT_AUTO_SYNC", "1") == "1")
    decision_engine = DecisionEngine(db)
    opportunity_engine = OpportunityEngine(db, decision_engine)
    diagnosis_engine = DiagnosisEngine(db, decision_engine)
    # V2 State Repository — backend selected by DATABASE_URL presence
    v2_repo = init_v2_backend(db)

    campaign_adapter = CampaignDraftAdapter(db, v2_repo=v2_repo)
    # Wire up CraftCampaignEngine for Mailchimp sends if available
    _campaign_engine = None
    try:
        from craft_engine import CraftCampaignEngine
        if os.environ.get("MAILCHIMP_API_KEY") and os.environ.get("MAILCHIMP_AUDIENCE_ID"):
            _campaign_engine = CraftCampaignEngine(db, v2_repo=v2_repo)
    except Exception:
        pass
    execution_adapter = ExecutionAdapter(db, v2_repo, _campaign_engine)

    def require_command_auth(fn):
        """Protect V2 business intelligence with a server-side bearer token."""

        @wraps(fn)
        def wrapped(*args, **kwargs):
            expected = os.environ.get("COMMAND_API_KEY", "")
            if not expected:
                return jsonify({
                    "error": "command_api_not_configured",
                    "message": "COMMAND_API_KEY must be configured before enabling V2 Command API.",
                }), 503

            auth = request.headers.get("Authorization", "")
            supplied = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
            if not supplied or not hmac.compare_digest(supplied, expected):
                return jsonify({"error": "unauthorized"}), 401
            return fn(*args, **kwargs)

        return wrapped

    # ─────────────────────────────────────────────────────────────────────
    # Existing V2 endpoints
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/command")
    @require_command_auth
    def command_summary():
        return jsonify(opportunity_engine.command_summary())

    @app.get("/api/v2/opportunities/<event_id>")
    @require_command_auth
    def event_opportunities(event_id: str):
        # Try raw DB lookup first; fall back to grouped portfolio lookup
        resolved = opportunity_engine.resolve_event(event_id)
        if resolved is None:
            return jsonify({"error": "event_not_found"}), 404
        pacing, is_grouped = resolved
        if is_grouped:
            items = [item.to_dict() for item in opportunity_engine.evaluate_pacing(pacing)]
            ctx = opportunity_engine.get_event_context(pacing)
            return jsonify({
                "event_id": event_id,
                "is_grouped": True,
                "logical_event_name": getattr(pacing, "event_name", ""),
                "constituent_event_ids": getattr(pacing, "constituent_event_ids", []),
                "event_date": getattr(pacing, "event_date", ""),
                "city": ctx.get("city", ""),
                "event_type": ctx.get("event_type", ""),
                "opportunities": items,
            })
        items = [item.to_dict() for item in opportunity_engine.evaluate_event(event_id)]
        return jsonify({"event_id": event_id, "opportunities": items})

    # ─────────────────────────────────────────────────────────────────────
    # Diagnosis
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/opportunities/<event_id>/diagnosis")
    @require_command_auth
    def event_diagnosis(event_id: str):
        """Inspect an event's opportunity and return a structured diagnosis."""
        resolved = opportunity_engine.resolve_event(event_id)
        if resolved is None:
            return jsonify({"error": "event_not_found"}), 404

        pacing, is_grouped = resolved
        if is_grouped:
            opps = opportunity_engine.evaluate_pacing(pacing)
        else:
            opps = opportunity_engine.evaluate_event(event_id)

        if not opps:
            return jsonify({
                "event_id": event_id,
                "diagnosis": None,
                "message": "No material opportunity surfaced for this event.",
            })

        opp = opps[0]
        try:
            if is_grouped:
                ctx = opportunity_engine.get_event_context(pacing)
                diagnosis = diagnosis_engine.diagnose_grouped(pacing, opp.to_dict(), ctx)
            else:
                diagnosis = diagnosis_engine.diagnose(event_id, opp.to_dict())
            result = {"event_id": event_id, "diagnosis": diagnosis.to_dict()}
            if is_grouped:
                result["is_grouped"] = True
                result["constituent_event_ids"] = getattr(pacing, "constituent_event_ids", [])
            return jsonify(result)
        except Exception as e:
            log.error(f"Diagnosis failed for {event_id}: {e}")
            return jsonify({"error": "diagnosis_failed", "message": str(e)}), 500

    # ─────────────────────────────────────────────────────────────────────
    # Intervention management
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/interventions")
    @require_command_auth
    def list_interventions():
        include_terminal = request.args.get("include_terminal", "false").lower() == "true"
        items = v2_repo.list_interventions(include_terminal=include_terminal)
        return jsonify({
            "count": len(items),
            "interventions": [i.to_dict() for i in items],
        })

    @app.get("/api/v2/interventions/<intervention_id>")
    @require_command_auth
    def get_intervention(intervention_id: str):
        item = v2_repo.get_intervention(intervention_id)
        if not item:
            return jsonify({"error": "intervention_not_found"}), 404
        return jsonify(item.to_dict())

    # ─────────────────────────────────────────────────────────────────────
    # Prepare: create draft intervention + campaign
    # ─────────────────────────────────────────────────────────────────────

    @app.post("/api/v2/opportunities/<opportunity_id>/prepare")
    @require_command_auth
    def prepare_intervention(opportunity_id: str):
        """Create a draft intervention and campaign from an opportunity.

        This endpoint:
        1. Finds the opportunity via the grouped portfolio (handles both
           raw Eventbrite IDs and grouped timed-entry logical IDs)
        2. Runs diagnosis (grouped or standard)
        3. Creates an Intervention record (status: proposed)
        4. If CRM campaign is recommended, creates a draft campaign
        5. Returns the intervention with campaign details

        DOES NOT send anything externally.
        """
        # Find the opportunity across the full grouped portfolio
        found = opportunity_engine.find_opportunity(opportunity_id)
        if not found:
            return jsonify({"error": "opportunity_not_found"}), 404
        target_opp, target_pacing = found
        is_grouped = target_pacing.event_id != target_opp.event_id or bool(
            getattr(target_pacing, "constituent_event_ids", [])
            and not db.get_event(target_pacing.event_id)
        )

        # Check if intervention already exists for this opportunity
        existing = v2_repo.get_interventions_by_opportunity(opportunity_id)
        active_existing = [i for i in existing if not i.is_terminal]
        if active_existing:
            return jsonify({
                "error": "intervention_exists",
                "message": "An active intervention already exists for this opportunity.",
                "intervention": active_existing[0].to_dict(),
            }), 409

        # Run diagnosis
        opp_dict = target_opp.to_dict()
        event_id = target_opp.event_id
        try:
            if is_grouped:
                ctx = opportunity_engine.get_event_context(target_pacing)
                diagnosis = diagnosis_engine.diagnose_grouped(
                    target_pacing, opp_dict, ctx
                )
            else:
                diagnosis = diagnosis_engine.diagnose(event_id, opp_dict)
        except Exception as e:
            log.error(f"Diagnosis failed during prepare for {event_id}: {e}")
            return jsonify({"error": "diagnosis_failed", "message": str(e)}), 500

        diagnosis_dict = diagnosis.to_dict()
        recommended_type = diagnosis.recommended_intervention or "crm_campaign"

        # Pick the matching intervention option for economics
        option = None
        for opt in diagnosis.intervention_options:
            if opt.intervention_type == recommended_type:
                option = opt
                break
        if not option and diagnosis.intervention_options:
            option = diagnosis.intervention_options[0]
            recommended_type = option.intervention_type

        # Build evidence — include grouped identity data
        intervention_evidence = {
            "diagnosis_summary": {
                "pace_delta_pct": diagnosis.pace_delta_pct,
                "gap_tickets": diagnosis.gap_tickets,
                "revenue_at_risk": diagnosis.revenue_at_risk,
                "root_causes": [
                    {"cause": rc.cause, "confidence": rc.confidence}
                    for rc in diagnosis.root_causes[:3]
                ],
            },
            "opportunity_id": opportunity_id,
        }
        if is_grouped:
            ctx = opportunity_engine.get_event_context(target_pacing)
            intervention_evidence["grouped_event"] = {
                "logical_event_id": event_id,
                "logical_event_name": getattr(target_pacing, "event_name", ""),
                "constituent_event_ids": getattr(target_pacing, "constituent_event_ids", []),
                "event_date": getattr(target_pacing, "event_date", ""),
                "city": ctx.get("city", ""),
                "event_type": ctx.get("event_type", ""),
            }

        # Create intervention
        intervention = Intervention.create(
            opportunity_id=opportunity_id,
            event_id=event_id,
            intervention_type=recommended_type,
            rationale=diagnosis.recommendation_rationale,
            expected_revenue=option.expected_revenue if option else 0,
            expected_cost=option.expected_cost if option else 0,
            expected_net_value=option.expected_net_value if option else 0,
            confidence=option.confidence if option else 0,
            evidence=intervention_evidence,
        )

        # Advance through state machine: new → investigated → proposed
        intervention.transition_to(InterventionStatus.INVESTIGATED)
        intervention.transition_to(InterventionStatus.PROPOSED)

        # If CRM campaign, create draft
        campaign_result = None
        if recommended_type == "crm_campaign":
            try:
                if is_grouped:
                    campaign_result = campaign_adapter.prepare_draft_grouped(
                        intervention, diagnosis_dict,
                        getattr(target_pacing, "constituent_event_ids", []),
                        opportunity_engine.get_event_context(target_pacing),
                    )
                else:
                    campaign_result = campaign_adapter.prepare_draft(
                        intervention, diagnosis_dict
                    )
                intervention.campaign_draft_id = campaign_result["campaign_draft_id"]
                intervention.audience_definition = campaign_result.get("segment_description", "")
                intervention.evidence["campaign_draft"] = {
                    "campaign_id": campaign_result["campaign_draft_id"],
                    "audience_count": campaign_result["audience_count"],
                    "conversion_assumptions": campaign_result["conversion_assumptions"],
                }
            except Exception as e:
                log.error(f"Campaign draft creation failed: {e}")
                # Don't fail the whole prepare — the intervention is still useful
                intervention.evidence["campaign_draft_error"] = str(e)

        v2_repo.save_intervention_with_audit(
            intervention,
            intervention.id, event_id,
            action="prepared",
            from_status="new",
            to_status=intervention.status.value,
            actor="system",
            metadata={
                "opportunity_id": opportunity_id,
                "intervention_type": recommended_type,
                "campaign_draft_id": intervention.campaign_draft_id,
            },
        )

        return jsonify({
            "intervention": intervention.to_dict(),
            "diagnosis_summary": {
                "root_causes": [
                    {"cause": rc.cause, "confidence": rc.confidence, "evidence": rc.evidence}
                    for rc in diagnosis.root_causes
                ],
                "recommended_intervention": diagnosis.recommended_intervention,
                "recommendation_rationale": diagnosis.recommendation_rationale,
                "missing_data": diagnosis.missing_data,
            },
            "campaign_draft": campaign_result,
        }), 201

    # ─────────────────────────────────────────────────────────────────────
    # Approve / Reject
    # ─────────────────────────────────────────────────────────────────────

    @app.post("/api/v2/interventions/<intervention_id>/approve")
    @require_command_auth
    def approve_intervention(intervention_id: str):
        """Approve a proposed intervention. Does NOT trigger execution."""
        item = v2_repo.get_intervention(intervention_id)
        if not item:
            return jsonify({"error": "intervention_not_found"}), 404

        from_status = item.status.value
        try:
            item.transition_to(InterventionStatus.APPROVED)
        except IllegalTransition as e:
            v2_repo.append_audit(
                intervention_id, item.event_id,
                action="approve_rejected",
                from_status=from_status,
                actor="user",
                error=str(e),
            )
            return jsonify({"error": "illegal_transition", "message": str(e)}), 409

        v2_repo.save_intervention_with_audit(
            item,
            intervention_id, item.event_id,
            action="approved",
            from_status=from_status,
            to_status=item.status.value,
            actor="user",
        )
        return jsonify({"status": "approved", "intervention": item.to_dict()})

    @app.post("/api/v2/interventions/<intervention_id>/reject")
    @require_command_auth
    def reject_intervention(intervention_id: str):
        """Reject a proposed intervention."""
        item = v2_repo.get_intervention(intervention_id)
        if not item:
            return jsonify({"error": "intervention_not_found"}), 404

        from_status = item.status.value
        try:
            item.transition_to(InterventionStatus.REJECTED)
        except IllegalTransition as e:
            v2_repo.append_audit(
                intervention_id, item.event_id,
                action="reject_rejected",
                from_status=from_status,
                actor="user",
                error=str(e),
            )
            return jsonify({"error": "illegal_transition", "message": str(e)}), 409

        v2_repo.save_intervention_with_audit(
            item,
            intervention_id, item.event_id,
            action="rejected",
            from_status=from_status,
            to_status=item.status.value,
            actor="user",
        )
        return jsonify({"status": "rejected", "intervention": item.to_dict()})

    # ─────────────────────────────────────────────────────────────────────
    # Execute
    # ─────────────────────────────────────────────────────────────────────

    @app.post("/api/v2/interventions/<intervention_id>/execute")
    @require_command_auth
    def execute_intervention(intervention_id: str):
        """Execute an approved CRM intervention.

        Actual external sends require V2_ENABLE_EXTERNAL_SEND=1.
        Without that flag, returns a dry-run/preflight result.
        """
        result = execution_adapter.execute(intervention_id, actor="user")
        if "error" in result:
            status_code = {
                "intervention_not_found": 404,
                "illegal_status": 409,
                "no_campaign_draft": 422,
                "campaign_draft_missing": 422,
                "suppression_unavailable": 503,
                "suppression_count_mismatch": 503,
                "empty_audience": 422,
                "execution_failed": 500,
                "duplicate_claim": 409,
                "attempt_in_progress": 409,
                "already_sent": 409,
                # Already sent, and local state was repaired on the way
                # through. 409 because the send is not repeatable — but
                # the body carries the recovered state, not a failure.
                "already_sent_recovered": 409,
                "reconciliation_required": 409,
                # 409, never 5xx: a 5xx invites a retry, and a retry here
                # could double-send. The condition is not transient —
                # it clears only when reconciliation proves what the
                # provider did.
                "execution_outcome_ambiguous": 409,
            }.get(result["error"], 400)
            return jsonify(result), status_code

        # external_send_disabled is a 200 — it's not an error, just a gate
        return jsonify(result)

    # ─────────────────────────────────────────────────────────────────────
    # Reconcile
    # ─────────────────────────────────────────────────────────────────────

    @app.post("/api/v2/interventions/<intervention_id>/reconcile")
    @require_command_auth
    def reconcile_intervention(intervention_id: str):
        """Reconcile an ambiguous send attempt by querying the provider.

        Only callable when a send attempt is in 'ambiguous' state.
        Queries Mailchimp for the actual campaign status and resolves
        to reconciled_sent or reconciled_not_sent.
        """
        result = execution_adapter.reconcile_send_attempt(
            intervention_id, actor="user"
        )
        if "error" in result:
            status_code = {
                "intervention_not_found": 404,
                "no_ambiguous_attempt": 404,
                "no_provider_campaign_id": 422,
                "mailchimp_not_configured": 503,
                "provider_query_failed": 502,
            }.get(result["error"], 400)
            return jsonify(result), status_code
        return jsonify(result)

    @app.get("/api/v2/interventions/<intervention_id>/send-attempts")
    @require_command_auth
    def intervention_send_attempts(intervention_id: str):
        """Return all send attempts for an intervention."""
        attempts = v2_repo.get_send_attempts(intervention_id)
        return jsonify({
            "intervention_id": intervention_id,
            "send_attempts": [a.to_dict() for a in attempts],
        })

    # ─────────────────────────────────────────────────────────────────────
    # Measure
    # ─────────────────────────────────────────────────────────────────────

    @app.post("/api/v2/interventions/<intervention_id>/measure")
    @require_command_auth
    def measure_intervention(intervention_id: str):
        """Compute attributed outcomes for an executed CRM intervention.

        Returns predicted vs actual values.
        Attribution is deterministic (order match by email within window),
        NOT causal lift.
        """
        result = execution_adapter.measure(intervention_id, actor="user")
        if "error" in result:
            status_code = {
                "intervention_not_found": 404,
                "illegal_status": 409,
                "no_sends": 422,
            }.get(result["error"], 400)
            return jsonify(result), status_code

        return jsonify(result)

    # ─────────────────────────────────────────────────────────────────────
    # Audit log
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/interventions/<intervention_id>/audit")
    @require_command_auth
    def intervention_audit_log(intervention_id: str):
        """Return the full audit trail for an intervention."""
        item = v2_repo.get_intervention(intervention_id)
        if not item:
            return jsonify({"error": "intervention_not_found"}), 404

        entries = v2_repo.get_audit_log(intervention_id)
        return jsonify({
            "intervention_id": intervention_id,
            "count": len(entries),
            "entries": entries,
        })

    # ─────────────────────────────────────────────────────────────────────
    # Suppression refresh (authoritative Mailchimp sync)
    # ─────────────────────────────────────────────────────────────────────

    @app.post("/api/v2/suppressions/refresh")
    @require_command_auth
    def refresh_suppressions():
        """Trigger an authoritative suppression refresh from Mailchimp.

        Queries Mailchimp for all unsubscribed + cleaned contacts,
        replaces local suppressions table atomically, updates sentinel.

        Returns count and timestamp — never returns email addresses.
        Requires MAILCHIMP_API_KEY and MAILCHIMP_AUDIENCE_ID in env.
        """
        from suppression_guard import SuppressionGuard

        mc_key = os.environ.get("MAILCHIMP_API_KEY")
        mc_audience = os.environ.get("MAILCHIMP_AUDIENCE_ID")
        if not mc_key or not mc_audience:
            return jsonify({
                "error": "mailchimp_not_configured",
                "message": "MAILCHIMP_API_KEY and MAILCHIMP_AUDIENCE_ID must be set.",
            }), 503

        try:
            from craft_engine import MailchimpClient
            mc_client = MailchimpClient(mc_key, mc_audience)
        except Exception as e:
            return jsonify({
                "error": "mailchimp_client_init_failed",
                "message": str(e),
            }), 500

        guard = SuppressionGuard(db, v2_repo=v2_repo)
        result = guard.refresh_from_mailchimp(mc_client)

        if "error" in result:
            return jsonify({
                "error": "refresh_failed",
                "message": result["error"],
            }), 500

        return jsonify({
            "status": "refreshed",
            "row_count": result["row_count"],
            "source": result["source"],
            "last_synced_at": result["last_synced_at"],
            "last_full_refresh_at": result["last_full_refresh_at"],
        })

    # ─────────────────────────────────────────────────────────────────────
    # Health
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/health")
    def v2_health():
        try:
            db.conn.execute("SELECT 1").fetchone()
            analytics_ok = True
        except Exception:
            analytics_ok = False

        v2_health_info = v2_repo.health_check()

        overall = "ok" if analytics_ok and v2_health_info.get("status") == "ok" else "degraded"
        result = {
            "status": overall,
            "command_configured": bool(os.environ.get("COMMAND_API_KEY")),
            "db_path_configured": bool(os.environ.get("DB_PATH")),
            "analytics_backend": "ok" if analytics_ok else "degraded",
            "v2_state_backend": v2_health_info,
        }
        return jsonify(result), 200 if overall == "ok" else 503

    return app


app = _build_app()
