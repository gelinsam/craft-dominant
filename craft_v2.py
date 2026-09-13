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
from intervention_model import (
    IllegalTransition,
    Intervention,
    InterventionStatus,
    InterventionStore,
)

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
    intervention_store = InterventionStore(db)
    campaign_adapter = CampaignDraftAdapter(db)

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
        if db.get_event(event_id) is None:
            return jsonify({"error": "event_not_found"}), 404
        items = [item.to_dict() for item in opportunity_engine.evaluate_event(event_id)]
        return jsonify({"event_id": event_id, "opportunities": items})

    # ─────────────────────────────────────────────────────────────────────
    # Diagnosis
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/opportunities/<event_id>/diagnosis")
    @require_command_auth
    def event_diagnosis(event_id: str):
        """Inspect an event's opportunity and return a structured diagnosis."""
        if db.get_event(event_id) is None:
            return jsonify({"error": "event_not_found"}), 404

        opps = opportunity_engine.evaluate_event(event_id)
        if not opps:
            return jsonify({
                "event_id": event_id,
                "diagnosis": None,
                "message": "No material opportunity surfaced for this event.",
            })

        # Use the top opportunity
        opp = opps[0]
        try:
            diagnosis = diagnosis_engine.diagnose(event_id, opp.to_dict())
            return jsonify({"event_id": event_id, "diagnosis": diagnosis.to_dict()})
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
        items = intervention_store.list_all(include_terminal=include_terminal)
        return jsonify({
            "count": len(items),
            "interventions": [i.to_dict() for i in items],
        })

    @app.get("/api/v2/interventions/<intervention_id>")
    @require_command_auth
    def get_intervention(intervention_id: str):
        item = intervention_store.get(intervention_id)
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
        1. Finds the opportunity by scanning events
        2. Runs diagnosis
        3. Creates an Intervention record (status: proposed)
        4. If CRM campaign is recommended, creates a draft campaign
        5. Returns the intervention with campaign details

        DOES NOT send anything externally.
        """
        # Find the opportunity across all events
        target_opp = None
        for event in db.get_events(upcoming_only=True):
            opps = opportunity_engine.evaluate_event(event["event_id"])
            for opp in opps:
                if opp.opportunity_id == opportunity_id:
                    target_opp = opp
                    break
            if target_opp:
                break

        if not target_opp:
            return jsonify({"error": "opportunity_not_found"}), 404

        # Check if intervention already exists for this opportunity
        existing = intervention_store.get_by_opportunity(opportunity_id)
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
            evidence={
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
            },
        )

        # Advance through state machine: new → investigated → proposed
        intervention.transition_to(InterventionStatus.INVESTIGATED)
        intervention.transition_to(InterventionStatus.PROPOSED)

        # If CRM campaign, create draft
        campaign_result = None
        if recommended_type == "crm_campaign":
            try:
                campaign_result = campaign_adapter.prepare_draft(intervention, diagnosis_dict)
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

        intervention_store.save(intervention)

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
    # Health
    # ─────────────────────────────────────────────────────────────────────

    @app.get("/api/v2/health")
    def v2_health():
        try:
            db.conn.execute("SELECT 1").fetchone()
            return jsonify({
                "status": "ok",
                "command_configured": bool(os.environ.get("COMMAND_API_KEY")),
                "db_path_configured": bool(os.environ.get("DB_PATH")),
            })
        except Exception:
            return jsonify({"status": "degraded"}), 503

    return app


app = _build_app()
