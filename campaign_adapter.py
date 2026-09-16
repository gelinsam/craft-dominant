"""Adapter for creating draft CRM campaigns linked to V2 interventions.

Wraps the existing CraftCampaignEngine to produce approval-ready campaign
drafts without sending.  If the campaign engine is unavailable (e.g. missing
API keys), falls back to a minimal draft record so the intervention pipeline
is not blocked.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from intervention_model import Intervention, InterventionStatus
from suppression_guard import SuppressionGuard, SuppressionStatus
from audience_suppression import guard_for_events

log = logging.getLogger("craft.campaign_adapter")

# Conservative conversion assumptions — same as diagnosis_engine for consistency.
EMAIL_OPEN_RATE = 0.22
EMAIL_CLICK_RATE = 0.035
EMAIL_CONVERSION_RATE = 0.012
CHAMPION_MULTIPLIER = 2.5


class CampaignDraftAdapter:
    """Creates draft campaigns linked to V2 interventions.

    Does NOT send emails.  Does NOT push to Mailchimp.
    The draft is saved locally for human review and approval.
    """

    def __init__(self, db, campaign_engine=None, v2_repo=None):
        """
        Args:
            db: Database instance (craft_unified.Database or compatible).
            campaign_engine: Optional CraftCampaignEngine.  Used for Claude-powered
                             copy generation if available; falls back to template
                             copy if not.
            v2_repo: Optional V2StateRepository for sentinel routing.
        """
        self.db = db
        self.campaign_engine = campaign_engine
        self.suppression_guard = SuppressionGuard(db, v2_repo=v2_repo)

    def prepare_draft(
        self,
        intervention: Intervention,
        diagnosis_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a draft campaign record linked to the intervention.

        Returns a dict with campaign details including audience_count and
        conversion assumptions.  The campaign is saved in 'draft' status.

        NEVER sends.
        """
        event_id = intervention.event_id
        event = self.db.get_event(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")
        event = dict(event)

        # Build audience with current-buyer exclusion
        audience_result = self._build_audience(event_id, event)
        audience_emails = audience_result["emails"]
        audience_count = len(audience_emails)
        segment_description = audience_result["description"]
        segment_sql = audience_result["sql"]

        # Conversion modeling — deterministic, no LLM
        avg_price = diagnosis_data.get("avg_ticket_price", 0)
        champions = diagnosis_data.get("crm_champions", 0)
        gap_tickets = diagnosis_data.get("gap_tickets")
        conversions = self._model_conversions(audience_count, champions)
        # Cap conversions at the gap — we can't sell more tickets than are needed
        if gap_tickets is not None and gap_tickets > 0:
            conversions["expected_tickets"] = min(conversions["expected_tickets"], gap_tickets)
        expected_revenue = conversions["expected_tickets"] * avg_price

        # Generate campaign draft ID
        campaign_id = f"v2-{str(uuid.uuid4())[:8]}"

        # Try to use CraftCampaignEngine for Claude-generated copy
        copy_result = self._generate_copy(event, diagnosis_data)

        # Save draft to campaigns table
        try:
            self._ensure_campaigns_table()
            self.db.conn.execute(
                """INSERT INTO campaigns
                   (id, event_id, campaign_type, channel, phase, subject_line,
                    preview_text, body_html, cta_text, cta_url,
                    segment_name, segment_sql, audience_count,
                    status, barrier_addressed, confidence_score,
                    strategic_reasoning, predicted_open_rate,
                    predicted_click_rate, predicted_revenue)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    campaign_id,
                    event_id,
                    "v2_recovery",
                    "email",
                    "recovery",
                    copy_result.get("subject_line", f"{event.get('name', 'Event')} — limited availability"),
                    copy_result.get("preview_text", ""),
                    copy_result.get("body_html", "<p>Draft pending copy generation.</p>"),
                    copy_result.get("cta_text", "Get Tickets"),
                    f"https://www.eventbrite.com/e/{event_id}",
                    segment_description,
                    segment_sql,
                    audience_count,
                    "draft",
                    "pace_recovery",
                    round(intervention.confidence, 2),
                    intervention.rationale,
                    EMAIL_OPEN_RATE,
                    EMAIL_CLICK_RATE,
                    round(expected_revenue, 2),
                ),
            )
            self.db.conn.commit()
        except Exception as e:
            log.error(f"Failed to save campaign draft: {e}")
            raise

        return {
            "campaign_draft_id": campaign_id,
            "event_id": event_id,
            "event_name": event.get("name", ""),
            "status": "draft",
            "audience_count": audience_count,
            "segment_description": segment_description,
            "conversion_assumptions": {
                "email_open_rate": EMAIL_OPEN_RATE,
                "email_click_rate": EMAIL_CLICK_RATE,
                "email_conversion_rate": EMAIL_CONVERSION_RATE,
                "champion_multiplier": CHAMPION_MULTIPLIER,
                "expected_opens": conversions["expected_opens"],
                "expected_clicks": conversions["expected_clicks"],
                "expected_tickets": conversions["expected_tickets"],
                "expected_revenue": round(expected_revenue, 2),
            },
            "copy_source": copy_result.get("source", "template"),
            "subject_line": copy_result.get("subject_line", ""),
        }

    def prepare_draft_grouped(
        self,
        intervention: Intervention,
        diagnosis_data: Dict[str, Any],
        constituent_event_ids: List[str],
        event_context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Create a draft campaign for a grouped (timed-entry) opportunity.

        Uses constituent_event_ids for buyer exclusion across all
        day-scoped slots.  event_context supplies city/event_type/name.

        NEVER sends.
        """
        event_id = intervention.event_id  # logical grouped ID
        event = dict(event_context)

        # Build audience with buyer exclusion across ALL constituent slots
        audience_result = self._build_audience_grouped(
            constituent_event_ids, event
        )
        audience_emails = audience_result["emails"]
        audience_count = len(audience_emails)
        segment_description = audience_result["description"]
        segment_sql = audience_result["sql"]

        # Conversion modeling — deterministic, no LLM
        avg_price = diagnosis_data.get("avg_ticket_price", 0)
        champions = diagnosis_data.get("crm_champions", 0)
        gap_tickets = diagnosis_data.get("gap_tickets")
        conversions = self._model_conversions(audience_count, champions)
        if gap_tickets is not None and gap_tickets > 0:
            conversions["expected_tickets"] = min(
                conversions["expected_tickets"], gap_tickets
            )
        expected_revenue = conversions["expected_tickets"] * avg_price

        campaign_id = f"v2-{str(uuid.uuid4())[:8]}"
        copy_result = self._generate_copy(event, diagnosis_data)

        # Save draft
        try:
            self._ensure_campaigns_table()
            self.db.conn.execute(
                """INSERT INTO campaigns
                   (id, event_id, campaign_type, channel, phase, subject_line,
                    preview_text, body_html, cta_text, cta_url,
                    segment_name, segment_sql, audience_count,
                    status, barrier_addressed, confidence_score,
                    strategic_reasoning, predicted_open_rate,
                    predicted_click_rate, predicted_revenue)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    campaign_id,
                    event_id,
                    "v2_recovery",
                    "email",
                    "recovery",
                    copy_result.get("subject_line", f"{event.get('name', 'Event')} — limited availability"),
                    copy_result.get("preview_text", ""),
                    copy_result.get("body_html", "<p>Draft pending copy generation.</p>"),
                    copy_result.get("cta_text", "Get Tickets"),
                    f"https://www.eventbrite.com/e/{constituent_event_ids[0] if constituent_event_ids else event_id}",
                    segment_description,
                    segment_sql,
                    audience_count,
                    "draft",
                    "pace_recovery",
                    round(intervention.confidence, 2),
                    intervention.rationale,
                    EMAIL_OPEN_RATE,
                    EMAIL_CLICK_RATE,
                    round(expected_revenue, 2),
                ),
            )
            self.db.conn.commit()
        except Exception as e:
            log.error(f"Failed to save grouped campaign draft: {e}")
            raise

        return {
            "campaign_draft_id": campaign_id,
            "event_id": event_id,
            "event_name": event.get("name", ""),
            "status": "draft",
            "audience_count": audience_count,
            "segment_description": segment_description,
            "constituent_event_ids": constituent_event_ids,
            "conversion_assumptions": {
                "email_open_rate": EMAIL_OPEN_RATE,
                "email_click_rate": EMAIL_CLICK_RATE,
                "email_conversion_rate": EMAIL_CONVERSION_RATE,
                "champion_multiplier": CHAMPION_MULTIPLIER,
                "expected_opens": conversions["expected_opens"],
                "expected_clicks": conversions["expected_clicks"],
                "expected_tickets": conversions["expected_tickets"],
                "expected_revenue": round(expected_revenue, 2),
            },
            "copy_source": copy_result.get("source", "template"),
            "subject_line": copy_result.get("subject_line", ""),
        }

    def _build_audience_grouped(
        self, constituent_event_ids: List[str], event: dict
    ) -> Dict[str, Any]:
        """Build audience excluding buyers from ALL constituent event slots."""
        city = event.get("city", "")
        event_type = event.get("event_type", "")

        # Collect buyers across ALL constituent slots
        buyer_set: set = set()
        for eid in constituent_event_ids:
            buyer_set.update(self.db.get_event_buyers(eid))

        # Suppression check — fail-closed invariant
        guard = guard_for_events(self.db, constituent_event_ids, self.suppression_guard)
        supp_status, supp_details, suppressed = guard.get_suppressions_if_valid()
        if supp_status not in (SuppressionStatus.HEALTHY, SuppressionStatus.ACKNOWLEDGED_EMPTY):
            raise ValueError(
                f"Suppression check failed ({supp_status.value}): "
                f"{supp_details.get('reason', 'unknown')}. "
                "Campaign draft blocked to prevent sending to unsubscribed contacts."
            )

        exclude = buyer_set | suppressed

        emails: set = set()
        description_parts = []

        # Past attendees (use first constituent for lookup)
        try:
            first_eid = constituent_event_ids[0] if constituent_event_ids else ""
            past = self.db.get_past_attendees_not_purchased(
                first_eid, event.get("name", ""),
                limit=50000,
                current_buyer_emails=list(buyer_set),
            )
            past_emails = {c["email"] for c in past if c.get("email") not in exclude}
            emails.update(past_emails)
            description_parts.append(f"{len(past_emails)} past attendees")
        except Exception as e:
            log.warning(f"Past attendees lookup failed: {e}")

        # City prospects
        try:
            if city:
                city_prospects = self.db.get_city_prospects(
                    city, exclude_emails=list(exclude | emails), limit=50000
                )
                city_emails = {c["email"] for c in city_prospects if c.get("email")}
                emails.update(city_emails)
                description_parts.append(f"{len(city_emails)} {city} prospects")
        except Exception as e:
            log.warning(f"City prospects lookup failed: {e}")

        description = (
            "Recovery audience: " + ", ".join(description_parts)
            if description_parts
            else "No audience segments available"
        )

        ids_str = ", ".join(f"'{eid}'" for eid in constituent_event_ids)
        cty = city.replace("'", "''")
        etype = event_type.replace("'", "''")
        suppression_source = 'suppressions'
        if getattr(guard, 'audience_id', None):
            suppression_source = f"audience_suppressions WHERE audience_id = '{guard.audience_id}'"
        sql = f"""
            SELECT DISTINCT c.email FROM customers c
            WHERE (c.favorite_city = '{cty}' OR c.event_types LIKE '%{etype}%')
            AND c.email NOT IN (SELECT email FROM orders WHERE event_id IN ({ids_str}))
            AND c.email NOT IN (SELECT email FROM {suppression_source})
        """

        return {
            "emails": list(emails),
            "description": description,
            "sql": sql.strip(),
        }

    def _build_audience(self, event_id: str, event: dict) -> Dict[str, Any]:
        """Identify the most defensible audience, excluding current buyers and suppressions."""
        city = event.get("city", "")
        event_type = event.get("event_type", "")

        # Get current buyers to exclude
        buyers = self.db.get_event_buyers(event_id)
        buyer_set = set(buyers)

        # Get suppressions — fail-closed invariant.
        # Suppression data must be positively validated via the sync sentinel.
        # If suppression state is unknown, stale, or unverified-empty, block.
        guard = guard_for_events(self.db, [event_id], self.suppression_guard)
        supp_status, supp_details, suppressed = guard.get_suppressions_if_valid()
        if supp_status not in (SuppressionStatus.HEALTHY, SuppressionStatus.ACKNOWLEDGED_EMPTY):
            raise ValueError(
                f"Suppression check failed ({supp_status.value}): "
                f"{supp_details.get('reason', 'unknown')}. "
                "Campaign draft blocked to prevent sending to unsubscribed contacts."
            )

        exclude = buyer_set | suppressed

        # Priority 1: Past attendees who haven't bought
        emails = set()
        description_parts = []

        try:
            past = self.db.get_past_attendees_not_purchased(
                event_id, event["name"], limit=50000, current_buyer_emails=buyers
            )
            past_emails = {c["email"] for c in past if c.get("email") not in exclude}
            emails.update(past_emails)
            description_parts.append(f"{len(past_emails)} past attendees")
        except Exception as e:
            log.warning(f"Past attendees lookup failed: {e}")

        # Priority 2: City prospects
        try:
            if city:
                city_prospects = self.db.get_city_prospects(
                    city, exclude_emails=list(exclude | emails), limit=50000
                )
                city_emails = {c["email"] for c in city_prospects if c.get("email")}
                emails.update(city_emails)
                description_parts.append(f"{len(city_emails)} {city} prospects")
        except Exception as e:
            log.warning(f"City prospects lookup failed: {e}")

        description = "Recovery audience: " + ", ".join(description_parts) if description_parts else "No audience segments available"

        # Build corresponding SQL for storage
        eid = event_id.replace("'", "''")
        cty = city.replace("'", "''")
        etype = event_type.replace("'", "''")
        suppression_source = 'suppressions'
        if getattr(guard, 'audience_id', None):
            suppression_source = f"audience_suppressions WHERE audience_id = '{guard.audience_id}'"
        sql = f"""
            SELECT DISTINCT c.email FROM customers c
            WHERE (c.favorite_city = '{cty}' OR c.event_types LIKE '%{etype}%')
            AND c.email NOT IN (SELECT email FROM orders WHERE event_id = '{eid}')
            AND c.email NOT IN (SELECT email FROM {suppression_source})
        """

        return {
            "emails": list(emails),
            "description": description,
            "sql": sql.strip(),
        }

    def _model_conversions(self, audience_count: int, champions: int) -> Dict[str, int]:
        """Deterministic conversion model — no LLM involved.

        Uses one coherent funnel for all segments:
          base_prob = open_rate × click_rate × conversion_rate
          champion_prob = min(base_prob × champion_multiplier, 1.0)

        Non-champions convert at base_prob; champions convert at champion_prob.
        """
        expected_opens = int(audience_count * EMAIL_OPEN_RATE)
        expected_clicks = int(expected_opens * EMAIL_CLICK_RATE)

        base_prob = EMAIL_OPEN_RATE * EMAIL_CLICK_RATE * EMAIL_CONVERSION_RATE
        champion_prob = min(base_prob * CHAMPION_MULTIPLIER, 1.0)

        non_champions = max(0, audience_count - champions)
        base_conversions = int(non_champions * base_prob)
        champion_conversions = int(champions * champion_prob)

        return {
            "expected_opens": expected_opens,
            "expected_clicks": expected_clicks,
            "expected_tickets": base_conversions + champion_conversions,
        }

    def _generate_copy(self, event: dict, diagnosis_data: dict) -> Dict[str, str]:
        """Try Claude-powered copy if campaign engine is available, else use template."""
        # We intentionally do NOT call Claude here in this slice — the campaign
        # engine's generate_campaign() method also triggers phase logging and
        # other side effects we don't want during draft preparation.
        # Return template-based copy that can be edited before approval.
        event_name = event.get("name", "Event")
        days = diagnosis_data.get("days_until", 0)
        tickets = diagnosis_data.get("tickets_sold", 0)
        capacity = diagnosis_data.get("capacity", 0)

        if days <= 7:
            urgency_text = "Last chance"
        elif days <= 14:
            urgency_text = "Almost here"
        elif days <= 30:
            urgency_text = "Coming up"
        else:
            urgency_text = "Mark your calendar"

        return {
            "subject_line": f"{urgency_text}: {event_name}",
            "preview_text": f"Don't miss {event_name} — tickets still available",
            "body_html": f"<p>Draft recovery campaign for {event_name}. Copy generation pending approval.</p>",
            "cta_text": "Get Tickets",
            "source": "template",
        }

    def _ensure_campaigns_table(self):
        """Make sure the campaigns table exists (it's normally created by CraftCampaignEngine)."""
        try:
            self.db.conn.execute("SELECT 1 FROM campaigns LIMIT 1")
        except Exception:
            # Create a minimal schema if the full engine hasn't been initialized
            self.db.conn.executescript("""
                CREATE TABLE IF NOT EXISTS campaigns (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL,
                    campaign_type TEXT NOT NULL,
                    channel TEXT DEFAULT 'email',
                    phase TEXT,
                    subject_line TEXT NOT NULL,
                    preview_text TEXT DEFAULT '',
                    body_html TEXT NOT NULL,
                    cta_text TEXT DEFAULT '',
                    cta_url TEXT DEFAULT '',
                    segment_name TEXT DEFAULT '',
                    segment_sql TEXT DEFAULT '',
                    audience_count INTEGER DEFAULT 0,
                    scheduled_send_at TEXT,
                    status TEXT DEFAULT 'draft',
                    approved_by TEXT,
                    approved_at TEXT,
                    sent_at TEXT,
                    barrier_addressed TEXT DEFAULT '',
                    confidence_score REAL DEFAULT 0,
                    strategic_reasoning TEXT DEFAULT '',
                    predicted_open_rate REAL DEFAULT 0,
                    predicted_click_rate REAL DEFAULT 0,
                    predicted_revenue REAL DEFAULT 0,
                    sends INTEGER DEFAULT 0,
                    opens INTEGER DEFAULT 0,
                    clicks INTEGER DEFAULT 0,
                    conversions INTEGER DEFAULT 0,
                    revenue_attributed REAL DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                );
            """)
            self.db.conn.commit()
