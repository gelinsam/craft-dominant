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
from craft_unified import (
    Database,
    DecisionEngine,
    MetaAdsSync,
    ProfileRebuildBusy,
    command_auth_error,
    auto_sync_enabled,
    create_app,
)
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


#: Tables that constitute the durable analytical plane. These are the counts
#: that must survive a Railway redeploy once the SQLite file lives on a volume.
ANALYTICS_TABLES = (
    "events",
    "orders",
    "customers",
    "customer_event_profiles",
    "daily_snapshots",
    "pacing_curves",
    "ad_spend",
    "suppressions",
)


def external_send_enabled() -> bool:
    """Whether external (real) sending is permitted.

    This is deliberately a mirror of the gate in ExecutionAdapter rather than an
    independent reading of the environment: if the two ever disagreed, health
    would report a comforting lie while the adapter did something else. Only the
    literal string "1" enables sending, so every other value — unset, "0",
    "true", "yes", "" — fails safe to disabled.
    """
    return os.environ.get("V2_ENABLE_EXTERNAL_SEND", "0") == "1"


def analytics_storage_info(db) -> dict:
    """Describe where the analytics SQLite file actually lives.

    Used to prove durability: `directory_is_mount` distinguishes a database on a
    mounted Railway volume from one sitting on the container's ephemeral
    filesystem, which is destroyed on every redeploy. Every probe is defensive —
    diagnostics must never be the reason a health check fails.
    """
    path = getattr(db, "path", None) or os.environ.get("DB_PATH", "craft_unified.db")
    abs_path = os.path.abspath(path)
    directory = os.path.dirname(abs_path) or "."

    info = {
        "db_path": abs_path,
        "db_path_configured": bool(os.environ.get("DB_PATH")),
        "directory": directory,
        "directory_is_mount": False,
        "exists": False,
        "size_bytes": 0,
        "wal_present": False,
        "shm_present": False,
        "journal_mode": None,
    }

    try:
        info["directory_is_mount"] = os.path.ismount(directory)
    except OSError:
        pass

    try:
        if os.path.exists(abs_path):
            info["exists"] = True
            info["size_bytes"] = os.path.getsize(abs_path)
        info["wal_present"] = os.path.exists(abs_path + "-wal")
        info["shm_present"] = os.path.exists(abs_path + "-shm")
    except OSError:
        pass

    try:
        row = db.conn.execute("PRAGMA journal_mode").fetchone()
        if row is not None:
            info["journal_mode"] = row[0]
    except Exception:
        pass

    return info


def analytics_row_counts(db) -> dict:
    """Row count per analytical table, or None where the table is unreadable.

    None is used rather than 0 so a missing/broken table is never mistaken for a
    genuinely empty one — the same false-zero distinction the paid-media work
    depends on.
    """
    counts: dict = {}
    for table in ANALYTICS_TABLES:
        try:
            row = db.conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
            counts[table] = int(row[0]) if row is not None else None
        except Exception:
            counts[table] = None
    return counts


def _build_app():
    db = ResilientDatabase(os.environ.get("DB_PATH", "craft_unified.db"))
    # Single source of truth shared with craft_unified, so the two entry
    # points cannot disagree about whether a startup sync runs.
    app = create_app(db, auto_sync=auto_sync_enabled())
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
            denied = command_auth_error()
            if denied is not None:
                payload, status = denied
                return jsonify(payload), status
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
        requested_audience = (request.get_json(silent=True) or {}).get('audience_id')
        mc_audience = requested_audience or os.environ.get("MAILCHIMP_AUDIENCE_ID")
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

        if requested_audience:
            from audience_suppression import AudienceSuppressionGuard
            try:
                guard = AudienceSuppressionGuard(db, requested_audience)
            except ValueError:
                return jsonify({'error':'invalid_audience_id'}), 400
        elif os.environ.get('MAILCHIMP_EVENT_AUDIENCES'):
            return jsonify({'error':'audience_id_required'}), 400
        else:
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

        # Whether real sending is possible must be directly observable rather
        # than inferred from the absence of evidence. This endpoint is public,
        # so it exposes only the resolved boolean — never the raw environment.
        send_enabled = external_send_enabled()

        storage = analytics_storage_info(db)

        result = {
            "status": overall,
            "command_configured": bool(os.environ.get("COMMAND_API_KEY")),
            "db_path_configured": storage["db_path_configured"],
            "analytics_backend": "ok" if analytics_ok else "degraded",
            "external_send_enabled": send_enabled,
            "execution_mode": "external" if send_enabled else "dry_run",
            # Durability signal only. The path itself is withheld here because
            # this route is unauthenticated; full detail lives behind auth on
            # /api/v2/diagnostics/analytics.
            "analytics_persistent": storage["directory_is_mount"],
            "v2_state_backend": v2_health_info,
        }
        return jsonify(result), 200 if overall == "ok" else 503

    @app.get("/api/v2/diagnostics/mailchimp-audiences")
    @require_command_auth
    def mailchimp_audience_inventory():
        if not _campaign_engine or not _campaign_engine.mailchimp:
            return jsonify({"error": "mailchimp_not_configured"}), 503
        try:
            audiences = _campaign_engine.mailchimp.audience_inventory()
        except Exception:
            log.exception("Mailchimp audience inventory failed")
            return jsonify({"error": "mailchimp_inventory_incomplete"}), 502
        return jsonify({"read_only": True, "audiences": audiences,
                        "audience_count": len(audiences),
                        "external_send_enabled": external_send_enabled(),
                        "routing_status": "single_audience_configuration_requires_mapping"})

    @app.get("/api/v2/diagnostics/analytics")
    @require_command_auth
    def v2_analytics_diagnostics():
        """Authenticated durability diagnostics for the analytical plane.

        Row counts are business data, so they are deliberately kept off the
        public health route. This is the evidence used to prove that analytics
        survive a redeploy: capture counts, restart, compare.
        """
        return jsonify({
            "storage": analytics_storage_info(db),
            "row_counts": analytics_row_counts(db),
        }), 200

    @app.get("/api/v2/diagnostics/meta-assignment")
    @require_command_auth
    def v2_meta_assignment_dry_run():
        """Read-only campaign -> festival-edition assignment report.

        Fetches campaign metadata with GET only, scores it against the current
        events, and returns what the sync WOULD attribute. Requests no insights,
        writes no ad_spend, touches no snapshot, mutates nothing on Meta. This is
        how attribution is reviewed before any spend ingestion runs.
        """
        token = os.environ.get("META_ACCESS_TOKEN")
        accounts = [a.strip() for a in os.environ.get("META_AD_ACCOUNT_ID", "").split(",") if a.strip()]
        if not token or not accounts:
            return jsonify({"error": "meta_not_configured"}), 400

        events = db.get_events(upcoming_only=False)
        combined = {"accounts": [], "totals": {"total_campaigns": 0, "assigned": 0,
                                               "ambiguous": 0, "no_match": 0}}
        include = request.args.get("include", "summary") == "campaigns"
        for acct in accounts:
            try:
                report = MetaAdsSync(token, acct, db).dry_run_assignment(events)
            except Exception as exc:  # diagnostics must never take the app down
                combined["accounts"].append({"account": acct[-4:], "error": str(exc)[:200]})
                continue
            combined["totals"]["total_campaigns"] += report["total_campaigns"]
            for k in ("assigned", "ambiguous", "no_match"):
                combined["totals"][k] += report["counts"][k]
            entry = {
                "account_suffix": acct[-4:],
                "total_campaigns": report["total_campaigns"],
                "counts": report["counts"],
                "distinct_editions_assigned": report["distinct_editions_assigned"],
            }
            if include:
                entry["campaigns"] = report["campaigns"]
            combined["accounts"].append(entry)
        combined["read_only"] = True
        return jsonify(combined), 200

    @app.post("/api/v2/maintenance/rebuild-profiles")
    @require_command_auth
    def v2_rebuild_profiles():
        """Rebuild both customer profile tables from already-persisted orders.

        TEMPORARY VALIDATION SURFACE — see _ProfileRebuildControl. It exists to
        measure PR #11's profile-write batching against the real mounted
        volume without a 70-minute Eventbrite traversal, and without the
        post-sync Meta behaviour PR #10 restored.

        Reaches no external API: it calls the same profile phases the full sync
        calls, over rows already in SQLite. Refuses to start while an Eventbrite
        full sync holds those phases, rather than queueing behind the write
        lock — a rebuild racing a traversal would derive profiles from a
        partially-populated orders table and would time a moving dataset.
        """
        control = getattr(app, "profile_rebuild_control", None)
        if control is None:
            # Fail closed: without the admission gate there is nothing stopping
            # this from overlapping a full sync.
            return jsonify({
                "status": "unavailable",
                "error": "rebuild_control_unavailable",
                "message": "Profile rebuild admission control is not wired up.",
            }), 503
        try:
            result = control.run()
        except ProfileRebuildBusy as busy:
            return jsonify({
                "status": "rejected",
                "reason": busy.reason,
                "message": {
                    "eventbrite_sync_running":
                        "An Eventbrite full sync is running and rebuilds the same "
                        "customer tables. Retry when /api/sync-status reports idle.",
                    "profile_rebuild_running":
                        "A profile rebuild is already in progress.",
                }.get(busy.reason, "Profile rebuild is not available right now."),
            }), 409
        except Exception as exc:
            log.exception("Profile rebuild failed")
            return jsonify({"status": "error", "error": type(exc).__name__,
                            "message": str(exc)[:500]}), 500
        return jsonify(result), 200

    return app


# Production uses the explicit factory. Importing diagnostics or tests must
# never open databases, run migrations, or start ingestion threads.
create_app_v2 = _build_app
