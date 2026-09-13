"""Craft Dominant V2 application wrapper.

Builds on the existing Flask application without rewriting it. The V2 layer adds
an authenticated, read-only Command API backed by the deterministic Opportunity
Engine. Existing Eventbrite/Meta/campaign behavior remains intact.
"""

from __future__ import annotations

import hmac
import os
from functools import wraps

from flask import jsonify, request

from craft_unified import Database, DecisionEngine, create_app
from dominant_agent import OpportunityEngine


def _build_app():
    db = Database(os.environ.get("DB_PATH", "craft_unified.db"))
    app = create_app(db, auto_sync=os.environ.get("CRAFT_AUTO_SYNC", "1") == "1")
    decision_engine = DecisionEngine(db)
    opportunity_engine = OpportunityEngine(db, decision_engine)

    def require_command_auth(fn):
        """Protect V2 business intelligence with a server-side bearer token.

        COMMAND_API_KEY is intentionally separate from EXPORT_API_KEY so a token
        exposed by legacy CSV flows cannot authorize the command surface.
        """

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

    @app.get("/api/v2/command")
    @require_command_auth
    def command_summary():
        return jsonify(opportunity_engine.command_summary())

    @app.get("/api/v2/opportunities/<event_id>")
    @require_command_auth
    def event_opportunities(event_id: str):
        items = [item.to_dict() for item in opportunity_engine.evaluate_event(event_id)]
        if db.get_event(event_id) is None:
            return jsonify({"error": "event_not_found"}), 404
        return jsonify({"event_id": event_id, "opportunities": items})

    @app.get("/api/v2/health")
    def v2_health():
        """Non-sensitive health endpoint suitable for Railway checks."""
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
