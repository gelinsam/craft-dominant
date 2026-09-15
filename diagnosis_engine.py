"""Opportunity diagnosis engine.

Takes a surfaced Opportunity and inspects all available data to produce a
structured diagnosis: root causes, evidence, ranked interventions, and a
recommendation.  Every factual claim is tied to data passed in — the engine
never invents numbers.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("craft.diagnosis")


@dataclass
class RootCause:
    cause: str
    evidence: List[str]
    confidence: float  # 0..1


@dataclass
class InterventionOption:
    intervention_type: str
    label: str
    rationale: str
    expected_revenue: float
    expected_cost: float
    expected_net_value: float
    confidence: float
    risk: str  # "low" | "medium" | "high"
    prerequisites: List[str]


@dataclass
class Diagnosis:
    event_id: str
    event_name: str
    opportunity_id: str
    # Situation snapshot
    days_until: int
    tickets_sold: int
    capacity: int
    sell_through_pct: float
    avg_ticket_price: float
    recent_velocity: Optional[float]  # tickets/day over last 7 days
    historical_velocity: Optional[float]  # tickets/day at same point in past editions
    pace_delta_pct: float
    gap_tickets: int
    revenue_at_risk: float
    # Spend / efficiency
    meta_spend_total: float
    meta_spend_recent_7d: float
    meta_impressions_recent_7d: int
    meta_clicks_recent_7d: int
    blended_ad_spend_per_ticket: float  # total ad spend / all tickets; 0 if no spend data
    # Audience
    crm_audience_total: int
    crm_past_attendees: int
    crm_champions: int
    crm_at_risk: int
    crm_city_prospects: int
    current_buyers_count: int
    # Historical
    historical_editions: List[Dict[str, Any]]
    current_event_campaigns_sent: int
    # Warnings
    missing_data: List[str]
    # Analysis
    root_causes: List[RootCause]
    intervention_options: List[InterventionOption]
    recommended_intervention: Optional[str]
    recommendation_rationale: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        return d


class DiagnosisEngine:
    """Inspects available data for an opportunity and produces a structured diagnosis."""

    def __init__(self, db, decision_engine):
        self.db = db
        self.decision_engine = decision_engine

    @staticmethod
    def _num(value, default=0.0) -> float:
        try:
            return default if value is None else float(value)
        except (TypeError, ValueError):
            return default

    def diagnose(self, event_id: str, opportunity: Dict[str, Any]) -> Diagnosis:
        """Produce a full diagnosis for an opportunity.

        Args:
            event_id: The event to diagnose.
            opportunity: The Opportunity.to_dict() output from OpportunityEngine.
        """
        event = self.db.get_event(event_id)
        if not event:
            raise ValueError(f"Event {event_id} not found")
        event = dict(event)
        evidence = opportunity.get("evidence", {})

        # ── Core pacing data ────────────────────────────────────────────
        days_until = int(evidence.get("days_until", 0))
        tickets_sold = int(evidence.get("tickets_sold", 0))
        capacity = int(event.get("capacity", 0))
        sell_through = (tickets_sold / capacity * 100) if capacity > 0 else 0
        avg_price = float(evidence.get("avg_ticket_price", 0))
        pace_delta = float(evidence.get("pace_delta_pct", 0))
        gap_tickets = int(evidence.get("gap_tickets", 0))
        revenue_at_risk = float(opportunity.get("revenue_at_risk", 0))

        # ── Velocity ────────────────────────────────────────────────────
        recent_velocity = self._compute_velocity(event_id, days=7)
        historical_velocity = self._compute_historical_velocity(event_id, event, days_until)

        # ── Meta spend ──────────────────────────────────────────────────
        meta_data_status = self._check_meta_data_status(event_id)
        meta_total, meta_7d_spend, meta_7d_impressions, meta_7d_clicks = self._get_meta_spend(event_id)
        blended_ad_spend_per_ticket = self._compute_blended_ad_spend_per_ticket(event_id, meta_total)

        # ── CRM audience ────────────────────────────────────────────────
        audience = self._compute_audience(event_id, event)

        # ── Historical editions ─────────────────────────────────────────
        historical_editions = self._get_historical_editions(event_id, event)
        historical_campaigns = self._count_current_event_campaigns(event_id)

        # ── Missing data warnings ───────────────────────────────────────
        missing = self._detect_missing_data(
            meta_total, recent_velocity, historical_velocity,
            audience, historical_editions, historical_campaigns,
            meta_data_status,
        )

        # ── Root cause analysis ─────────────────────────────────────────
        root_causes = self._analyze_root_causes(
            pace_delta=pace_delta,
            recent_velocity=recent_velocity,
            historical_velocity=historical_velocity,
            meta_total=meta_total,
            meta_7d_spend=meta_7d_spend,
            meta_7d_clicks=meta_7d_clicks,
            blended_ad_spend_per_ticket=blended_ad_spend_per_ticket,
            days_until=days_until,
            sell_through=sell_through,
            audience=audience,
            historical_campaigns=historical_campaigns,
            meta_data_status=meta_data_status,
        )

        # ── Intervention options ────────────────────────────────────────
        intervention_options = self._rank_interventions(
            root_causes=root_causes,
            gap_tickets=gap_tickets,
            avg_price=avg_price,
            days_until=days_until,
            audience=audience,
            meta_total=meta_total,
            blended_ad_spend_per_ticket=blended_ad_spend_per_ticket,
        )

        # Pick recommendation
        recommended = intervention_options[0].intervention_type if intervention_options else None
        rec_rationale = intervention_options[0].rationale if intervention_options else "Insufficient data to recommend an intervention."

        return Diagnosis(
            event_id=event_id,
            event_name=event.get("name", ""),
            opportunity_id=opportunity.get("opportunity_id", ""),
            days_until=days_until,
            tickets_sold=tickets_sold,
            capacity=capacity,
            sell_through_pct=round(sell_through, 1),
            avg_ticket_price=round(avg_price, 2),
            recent_velocity=round(recent_velocity, 2) if recent_velocity is not None else None,
            historical_velocity=round(historical_velocity, 2) if historical_velocity is not None else None,
            pace_delta_pct=round(pace_delta, 1),
            gap_tickets=gap_tickets,
            revenue_at_risk=round(revenue_at_risk, 2),
            meta_spend_total=round(meta_total, 2),
            meta_spend_recent_7d=round(meta_7d_spend, 2),
            meta_impressions_recent_7d=meta_7d_impressions,
            meta_clicks_recent_7d=meta_7d_clicks,
            blended_ad_spend_per_ticket=round(blended_ad_spend_per_ticket, 2),
            crm_audience_total=audience["total"],
            crm_past_attendees=audience["past_attendees"],
            crm_champions=audience["champions"],
            crm_at_risk=audience["at_risk"],
            crm_city_prospects=audience["city_prospects"],
            current_buyers_count=audience["current_buyers"],
            historical_editions=historical_editions,
            current_event_campaigns_sent=historical_campaigns,
            missing_data=missing,
            root_causes=root_causes,
            intervention_options=intervention_options,
            recommended_intervention=recommended,
            recommendation_rationale=rec_rationale,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Grouped-event diagnosis
    # ─────────────────────────────────────────────────────────────────────

    def diagnose_grouped(self, pacing, opportunity: Dict[str, Any],
                         event_context: Dict[str, Any]) -> Diagnosis:
        """Produce a diagnosis for a grouped (timed-entry) opportunity.

        Args:
            pacing: The grouped EventPacing object from analyze_portfolio().
            opportunity: The Opportunity.to_dict() output.
            event_context: An event-like dict with city, event_type, name,
                           capacity — from OpportunityEngine.get_event_context().
        """
        constituent_ids = getattr(pacing, "constituent_event_ids", []) or []
        evidence = opportunity.get("evidence", {})

        # ── Core pacing data (from grouped pacing) ─────────────────────
        days_until = int(evidence.get("days_until", 0))
        tickets_sold = int(evidence.get("tickets_sold", 0))
        capacity = int(event_context.get("capacity", 0))
        sell_through = (tickets_sold / capacity * 100) if capacity > 0 else 0
        avg_price = float(evidence.get("avg_ticket_price", 0))
        pace_delta = float(evidence.get("pace_delta_pct", 0))
        gap_tickets = int(evidence.get("gap_tickets", 0))
        revenue_at_risk = float(opportunity.get("revenue_at_risk", 0))

        # ── Velocity (aggregate across constituent slots) ──────────────
        recent_velocity = self._compute_velocity_for_ids(constituent_ids, days=7)
        historical_velocity = None  # Use pacing's historical comparisons below

        # ── Meta spend (aggregate across constituent slots) ────────────
        meta_data_status = self._check_meta_data_status_for_ids(constituent_ids)
        meta_total, meta_7d_spend, meta_7d_impressions, meta_7d_clicks = (
            self._get_meta_spend_for_ids(constituent_ids)
        )
        blended_ad_spend_per_ticket = (
            (meta_total / tickets_sold) if tickets_sold > 0 and meta_total > 0 else 0.0
        )

        # ── CRM audience (using constituent IDs for buyer exclusion) ───
        audience = self._compute_audience_grouped(constituent_ids, event_context)

        # ── Historical editions (from pacing's pre-computed data) ──────
        historical_editions = getattr(pacing, "historical_comparisons", []) or []

        # ── Campaigns sent (aggregate across constituent slots) ────────
        historical_campaigns = self._count_campaigns_for_ids(constituent_ids)

        # ── Missing data ───────────────────────────────────────────────
        missing = self._detect_missing_data(
            meta_total, recent_velocity, historical_velocity,
            audience, historical_editions, historical_campaigns,
            meta_data_status,
        )

        # ── Root cause analysis ────────────────────────────────────────
        root_causes = self._analyze_root_causes(
            pace_delta=pace_delta,
            recent_velocity=recent_velocity,
            historical_velocity=historical_velocity,
            meta_total=meta_total,
            meta_7d_spend=meta_7d_spend,
            meta_7d_clicks=meta_7d_clicks,
            blended_ad_spend_per_ticket=blended_ad_spend_per_ticket,
            days_until=days_until,
            sell_through=sell_through,
            audience=audience,
            historical_campaigns=historical_campaigns,
            meta_data_status=meta_data_status,
        )

        # ── Intervention options ───────────────────────────────────────
        intervention_options = self._rank_interventions(
            root_causes=root_causes,
            gap_tickets=gap_tickets,
            avg_price=avg_price,
            days_until=days_until,
            audience=audience,
            meta_total=meta_total,
            blended_ad_spend_per_ticket=blended_ad_spend_per_ticket,
        )

        recommended = intervention_options[0].intervention_type if intervention_options else None
        rec_rationale = (
            intervention_options[0].rationale
            if intervention_options
            else "Insufficient data to recommend an intervention."
        )

        return Diagnosis(
            event_id=pacing.event_id,
            event_name=getattr(pacing, "event_name", ""),
            opportunity_id=opportunity.get("opportunity_id", ""),
            days_until=days_until,
            tickets_sold=tickets_sold,
            capacity=capacity,
            sell_through_pct=round(sell_through, 1),
            avg_ticket_price=round(avg_price, 2),
            recent_velocity=round(recent_velocity, 2) if recent_velocity is not None else None,
            historical_velocity=None,
            pace_delta_pct=round(pace_delta, 1),
            gap_tickets=gap_tickets,
            revenue_at_risk=round(revenue_at_risk, 2),
            meta_spend_total=round(meta_total, 2),
            meta_spend_recent_7d=round(meta_7d_spend, 2),
            meta_impressions_recent_7d=meta_7d_impressions,
            meta_clicks_recent_7d=meta_7d_clicks,
            blended_ad_spend_per_ticket=round(blended_ad_spend_per_ticket, 2),
            crm_audience_total=audience["total"],
            crm_past_attendees=audience["past_attendees"],
            crm_champions=audience["champions"],
            crm_at_risk=audience["at_risk"],
            crm_city_prospects=audience["city_prospects"],
            current_buyers_count=audience["current_buyers"],
            historical_editions=historical_editions,
            current_event_campaigns_sent=historical_campaigns,
            missing_data=missing,
            root_causes=root_causes,
            intervention_options=intervention_options,
            recommended_intervention=recommended,
            recommendation_rationale=rec_rationale,
        )

    # ─────────────────────────────────────────────────────────────────────
    # Data gathering helpers
    # ─────────────────────────────────────────────────────────────────────

    def _compute_velocity(self, event_id: str, days: int = 7) -> Optional[float]:
        """Recent ticket velocity from snapshots."""
        try:
            snaps = self.db.get_snapshots(event_id)
            if len(snaps) < 2:
                return None
            recent = snaps[:days]
            if len(recent) < 2:
                return None
            ticket_diff = recent[0]["tickets_cumulative"] - recent[-1]["tickets_cumulative"]
            day_span = max(1, len(recent) - 1)
            return ticket_diff / day_span
        except Exception as e:
            log.warning(f"Velocity computation failed for {event_id}: {e}")
            return None

    def _compute_historical_velocity(
        self, event_id: str, event: dict, days_until: int
    ) -> Optional[float]:
        """What was the daily velocity at this same days-out point in past editions?"""
        try:
            pattern = self.decision_engine._get_pattern(event["name"])
            past_ids = self.db.get_pattern_event_ids(pattern, exclude_ids=[event_id])
            if not past_ids:
                return None
            velocities = []
            for pid in past_ids[:5]:
                snap_at = self.db.get_snapshot_at_days(pid, days_until)
                snap_week_later = self.db.get_snapshot_at_days(pid, days_until + 7)
                if snap_at and snap_week_later:
                    diff = snap_at["tickets_cumulative"] - snap_week_later["tickets_cumulative"]
                    velocities.append(diff / 7.0)
            if not velocities:
                return None
            return sum(velocities) / len(velocities)
        except Exception as e:
            log.warning(f"Historical velocity failed for {event_id}: {e}")
            return None

    def _get_meta_spend(self, event_id: str) -> Tuple[float, float, int, int]:
        """Return (total_spend, last_7d_spend, last_7d_impressions, last_7d_clicks)."""
        total = 0.0
        spend_7d = 0.0
        impressions_7d = 0
        clicks_7d = 0
        try:
            # Paid media is bought per festival edition, so a Saturday view
            # must see the festival's spend even when the canonical storage row
            # is the Sunday one. Without this, V2 reports META SPEND = $0 and
            # "No paid advertising data" for every non-canonical day.
            # Fall back to per-event scope on a store that predates edition
            # reads. Resolving through getattr rather than letting an
            # AttributeError hit the except below matters: that path returns
            # $0, and a false zero here is indistinguishable from "no ads ran".
            if hasattr(self.db, "get_edition_spend"):
                total = self.db.get_edition_spend(event_id)
                edition_ids = self.db.edition_sibling_ids(event_id)
            else:
                total = self.db.get_event_spend(event_id)
                edition_ids = [event_id]
            seven_days_ago = (date.today() - timedelta(days=7)).isoformat()
            marks = ",".join("?" * len(edition_ids))
            rows = self.db.conn.execute(
                f"""SELECT SUM(spend) as s, SUM(impressions) as i, SUM(clicks) as c
                    FROM ad_spend WHERE event_id IN ({marks}) AND spend_date >= ?""",
                (*edition_ids, seven_days_ago),
            ).fetchone()
            if rows:
                spend_7d = self._num(rows["s"])
                impressions_7d = int(self._num(rows["i"]))
                clicks_7d = int(self._num(rows["c"]))
        except Exception as e:
            log.warning(f"Meta spend lookup failed for {event_id}: {e}")
        return total, spend_7d, impressions_7d, clicks_7d

    # Data is "current" if the most recent spend_date is within this many days
    META_FRESHNESS_THRESHOLD_DAYS = 3

    def _check_meta_data_status(self, event_id: str) -> str:
        """Check Meta ad spend data freshness for this event.

        Returns one of:
            'current_has_spend'  - recent data exists with spend > 0
            'current_zero_spend' - recent data exists but all spend is $0
            'stale'              - data exists but latest spend_date is too old
            'no_records'         - ad_spend table exists but no rows for this event
            'unavailable'        - ad_spend table doesn't exist or query failed
        """
        try:
            row = self.db.conn.execute(
                "SELECT MAX(spend_date) as latest_date, "
                "       SUM(spend) as total_spend, "
                "       COUNT(*) as cnt "
                "FROM ad_spend WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if not row or int(row["cnt"]) == 0:
                return "no_records"

            latest_date_str = row["latest_date"]
            if not latest_date_str:
                return "no_records"

            # Check freshness: is the most recent data point within threshold?
            try:
                latest_date = date.fromisoformat(str(latest_date_str))
            except (ValueError, TypeError):
                # Can't parse the date — treat as stale
                return "stale"

            days_since_latest = (date.today() - latest_date).days
            if days_since_latest > self.META_FRESHNESS_THRESHOLD_DAYS:
                return "stale"

            # Data is current — check if there's actual spend
            total_spend = self._num(row["total_spend"])
            if total_spend > 0:
                return "current_has_spend"
            else:
                return "current_zero_spend"
        except Exception:
            return "unavailable"

    def _compute_blended_ad_spend_per_ticket(self, event_id: str, total_spend: float) -> float:
        """Blended ad spend per ticket: total ad spend / all tickets sold.

        Note: this divides total Meta spend by ALL tickets (including organic).
        It is NOT a true CAC — it's a blended cost metric.
        """
        if total_spend <= 0:
            return 0.0
        try:
            tickets = self.db.get_event_tickets(event_id)
            return total_spend / tickets if tickets > 0 else 0.0
        except Exception as e:
            log.warning(f"Blended ad spend computation failed for {event_id}: {e}")
            return 0.0

    def _compute_audience(self, event_id: str, event: dict) -> Dict[str, int]:
        """Count available CRM segments, excluding current buyers.

        past_attendees and city_prospects are deduplicated by email:
        city_prospects excludes both current buyers AND past attendee emails
        so the total is a true unique count.
        """
        result = {
            "total": 0,
            "past_attendees": 0,
            "champions": 0,
            "at_risk": 0,
            "city_prospects": 0,
            "current_buyers": 0,
        }
        try:
            buyers = self.db.get_event_buyers(event_id)
            result["current_buyers"] = len(buyers)
            buyer_set = set(buyers)

            past_attendees = self.db.get_past_attendees_not_purchased(
                event_id, event["name"], limit=50000, current_buyer_emails=buyers
            )
            past_attendee_emails = {c.get("email") for c in past_attendees if c.get("email")}
            result["past_attendees"] = len(past_attendees)
            result["champions"] = len([
                c for c in past_attendees
                if c.get("rfm_segment") in ("champion", "loyal")
            ])
            result["at_risk"] = len([
                c for c in past_attendees
                if c.get("rfm_segment") == "at_risk"
            ])

            city = event.get("city", "")
            if city:
                # Exclude both current buyers AND past attendees to prevent double-counting
                exclude_emails = list(buyer_set | past_attendee_emails)
                city_prospects = self.db.get_city_prospects(
                    city, exclude_emails=exclude_emails, limit=50000
                )
                result["city_prospects"] = len(city_prospects)

            result["total"] = result["past_attendees"] + result["city_prospects"]
        except Exception as e:
            log.warning(f"Audience computation failed for {event_id}: {e}")
        return result

    def _get_historical_editions(self, event_id: str, event: dict) -> List[Dict[str, Any]]:
        """Summary of past editions for comparison."""
        editions = []
        try:
            pattern = self.decision_engine._get_pattern(event["name"])
            past_ids = self.db.get_pattern_event_ids(pattern, exclude_ids=[event_id])
            for pid in past_ids[:5]:
                pe = self.db.get_event(pid)
                if not pe:
                    continue
                pe = dict(pe)
                pt = self.db.get_event_tickets(pid)
                pr = self.db.get_event_revenue(pid)
                pc = pe.get("capacity", 0)
                editions.append({
                    "event_id": pid,
                    "name": pe.get("name", ""),
                    "event_date": pe.get("event_date", ""),
                    "tickets": pt,
                    "revenue": round(pr, 2),
                    "capacity": pc,
                    "sell_through": round(pt / pc * 100, 1) if pc > 0 else 0,
                })
        except Exception as e:
            log.warning(f"Historical editions lookup failed for {event_id}: {e}")
        return editions

    def _count_current_event_campaigns(self, event_id: str) -> int:
        """Count campaigns actually sent for this event (excludes drafts)."""
        try:
            row = self.db.conn.execute(
                "SELECT COUNT(*) as cnt FROM campaigns WHERE event_id = ? AND sent_at IS NOT NULL",
                (event_id,),
            ).fetchone()
            return int(row["cnt"]) if row else 0
        except Exception:
            # campaigns table may not exist if engine hasn't been initialized
            return 0

    def _detect_missing_data(
        self,
        meta_total: float,
        recent_velocity: Optional[float],
        historical_velocity: Optional[float],
        audience: Dict[str, int],
        historical_editions: list,
        historical_campaigns: int,
        meta_data_status: str = "unknown",
    ) -> List[str]:
        warnings = []
        if meta_data_status == "unavailable":
            warnings.append(
                "Meta ad spend data is unavailable (possible expired token, sync failure, "
                "or missing ad_spend table) — cannot assess paid performance."
            )
        elif meta_data_status == "stale":
            warnings.append(
                "Meta ad spend data is stale (last sync is more than "
                f"{self.META_FRESHNESS_THRESHOLD_DAYS} days old) — "
                "paid performance assessment may be outdated."
            )
        elif meta_data_status == "no_records":
            warnings.append(
                "No Meta ad spend recorded for this event — cannot assess paid performance."
            )
        elif meta_data_status == "current_zero_spend":
            warnings.append(
                "Meta ad spend data is current but all recorded spend is $0 — "
                "no active paid campaigns detected."
            )
        # current_has_spend → no warning needed
        if recent_velocity is None:
            warnings.append("Insufficient snapshot data to compute recent ticket velocity.")
        if historical_velocity is None:
            warnings.append("No historical velocity data — cannot compare current vs. past selling rates.")
        if not historical_editions:
            warnings.append("No past editions found — pacing comparison is limited.")
        if audience["total"] == 0:
            warnings.append("CRM audience is empty — email campaign reach may be zero.")
        return warnings

    # ─────────────────────────────────────────────────────────────────────
    # Analysis: root causes
    # ─────────────────────────────────────────────────────────────────────

    def _analyze_root_causes(
        self,
        pace_delta: float,
        recent_velocity: Optional[float],
        historical_velocity: Optional[float],
        meta_total: float,
        meta_7d_spend: float,
        meta_7d_clicks: int,
        blended_ad_spend_per_ticket: float,
        days_until: int,
        sell_through: float,
        audience: dict,
        historical_campaigns: int,
        meta_data_status: str = "unknown",
    ) -> List[RootCause]:
        """Deterministic root cause identification from available signals."""
        causes: List[RootCause] = []

        # 1. Velocity stall
        if recent_velocity is not None and recent_velocity < 1.0:
            evidence = [f"Recent 7-day velocity is {recent_velocity:.1f} tickets/day"]
            if historical_velocity is not None and historical_velocity > 0:
                ratio = recent_velocity / historical_velocity
                evidence.append(
                    f"Historical velocity at this point was {historical_velocity:.1f} tickets/day "
                    f"({ratio:.0%} of historical rate)"
                )
            causes.append(RootCause(
                cause="Ticket velocity has stalled",
                evidence=evidence,
                confidence=min(0.85, 0.6 + (1.0 - min(recent_velocity, 1.0)) * 0.25),
            ))

        # 2. Paid underperformance — only assess when data is current
        if meta_data_status == "current_has_spend" and meta_total > 0:
            if meta_7d_spend > 0 and meta_7d_clicks < 5:
                causes.append(RootCause(
                    cause="Paid ads are spending without driving clicks",
                    evidence=[
                        f"Last 7 days: ${meta_7d_spend:.0f} spent, {meta_7d_clicks} clicks",
                        f"Total campaign spend: ${meta_total:.0f}",
                    ],
                    confidence=0.70,
                ))
            if blended_ad_spend_per_ticket > 0 and blended_ad_spend_per_ticket > 50:
                causes.append(RootCause(
                    cause="Blended ad spend per ticket is high",
                    evidence=[
                        f"Blended ad spend is ${blended_ad_spend_per_ticket:.2f} per ticket "
                        f"(total spend ${meta_total:.0f} / all tickets including organic)",
                    ],
                    confidence=0.60,
                ))
        elif days_until > 14:
            # Distinguish status-aware root causes for missing/stale/zero spend
            if meta_data_status == "unavailable":
                causes.append(RootCause(
                    cause="Meta ad spend data is unavailable",
                    evidence=[
                        "Could not query ad_spend table — possible expired token, sync failure, or missing configuration",
                        f"{days_until} days of runway remain — ad performance cannot be assessed",
                    ],
                    confidence=0.40,
                ))
            elif meta_data_status == "stale":
                causes.append(RootCause(
                    cause="Meta ad spend data is stale",
                    evidence=[
                        f"Last spend data is more than {self.META_FRESHNESS_THRESHOLD_DAYS} days old — "
                        "cannot determine current paid advertising status",
                        f"{days_until} days of runway remain",
                    ],
                    confidence=0.40,
                ))
            elif meta_data_status == "current_zero_spend":
                # Only claim "no paid advertising" when we have current data confirming $0
                causes.append(RootCause(
                    cause="No paid advertising detected",
                    evidence=[
                        "Current Meta data confirms no active ad spend for this event",
                        f"{days_until} days of runway remain",
                    ],
                    confidence=0.50,
                ))
            elif meta_data_status == "no_records":
                causes.append(RootCause(
                    cause="No paid advertising data for this event",
                    evidence=[
                        "No Meta ad spend records found for this event",
                        f"{days_until} days of runway remain",
                    ],
                    confidence=0.45,
                ))

        # 3. Under-marketed (few campaigns sent for current event)
        if historical_campaigns < 2 and days_until > 7:
            causes.append(RootCause(
                cause="Event may be under-marketed",
                evidence=[
                    f"Only {historical_campaigns} campaign(s) sent for this event",
                    f"{days_until} days until event",
                ],
                confidence=0.55,
            ))

        # 4. Untapped CRM audience
        if audience["total"] > 100 and audience["past_attendees"] > 50:
            buyer_to_audience = audience["current_buyers"] / audience["total"] if audience["total"] > 0 else 0
            if buyer_to_audience < 0.10:
                causes.append(RootCause(
                    cause="Large CRM audience is under-activated",
                    evidence=[
                        f"{audience['past_attendees']} past attendees haven't purchased",
                        f"{audience['champions']} are champion/loyal segments",
                        f"Only {buyer_to_audience:.1%} of reachable audience has converted",
                    ],
                    confidence=0.65,
                ))

        # 5. Velocity drop vs. historical
        if recent_velocity is not None and historical_velocity is not None and historical_velocity > 0:
            velocity_ratio = recent_velocity / historical_velocity
            if velocity_ratio < 0.5:
                causes.append(RootCause(
                    cause="Selling rate has dropped significantly vs. historical pattern",
                    evidence=[
                        f"Current: {recent_velocity:.1f} tickets/day vs. historical {historical_velocity:.1f} tickets/day",
                        f"Operating at {velocity_ratio:.0%} of expected rate",
                    ],
                    confidence=min(0.80, 0.55 + (1.0 - velocity_ratio) * 0.3),
                ))

        # Sort by confidence descending
        causes.sort(key=lambda c: c.confidence, reverse=True)
        return causes

    # ─────────────────────────────────────────────────────────────────────
    # Analysis: intervention options
    # ─────────────────────────────────────────────────────────────────────

    # Conservative conversion assumptions — exposed in output for transparency.
    EMAIL_OPEN_RATE = 0.22
    EMAIL_CLICK_RATE = 0.035
    EMAIL_CONVERSION_RATE = 0.012  # click → purchase
    CHAMPION_MULTIPLIER = 2.5  # champions convert at higher rates

    def _rank_interventions(
        self,
        root_causes: List[RootCause],
        gap_tickets: int,
        avg_price: float,
        days_until: int,
        audience: dict,
        meta_total: float,
        blended_ad_spend_per_ticket: float,
    ) -> List[InterventionOption]:
        options: List[InterventionOption] = []

        # Option 1: CRM recovery campaign (always available if audience exists)
        if audience["total"] > 0:
            reachable = audience["past_attendees"] + audience["city_prospects"]
            # Expected conversions from an email campaign — unified funnel model
            expected_opens = int(reachable * self.EMAIL_OPEN_RATE)
            expected_clicks = int(expected_opens * self.EMAIL_CLICK_RATE)

            base_prob = self.EMAIL_OPEN_RATE * self.EMAIL_CLICK_RATE * self.EMAIL_CONVERSION_RATE
            champion_prob = min(base_prob * self.CHAMPION_MULTIPLIER, 1.0)

            non_champions = max(0, reachable - audience["champions"])
            base_conversions = int(non_champions * base_prob)
            champion_conversions = int(audience["champions"] * champion_prob)
            total_conversions = base_conversions + champion_conversions
            # Cap at gap
            total_conversions = min(total_conversions, gap_tickets)
            expected_rev = total_conversions * avg_price
            # Cost is near-zero for owned email channel
            expected_cost = 0.0

            options.append(InterventionOption(
                intervention_type="crm_campaign",
                label="CRM recovery email campaign",
                rationale=(
                    f"Send targeted email to {reachable:,} reachable contacts "
                    f"({audience['champions']} champions, {audience['past_attendees']} past attendees). "
                    f"Estimated {total_conversions} conversions at ${avg_price:.0f} avg ticket."
                ),
                expected_revenue=round(expected_rev, 2),
                expected_cost=expected_cost,
                expected_net_value=round(expected_rev - expected_cost, 2),
                confidence=0.55 if total_conversions > 5 else 0.30,
                risk="low",
                prerequisites=["Mailchimp configured", "Audience data available"],
            ))

        # Option 2: Paid media review (non-financial — flags for human review only)
        # We do NOT model incremental tickets or revenue from ad optimization because
        # any such projection would be fabricated — we have no causal model linking
        # spend changes to ticket outcomes.
        if meta_total > 0 and blended_ad_spend_per_ticket > 0 and days_until > 7:
            options.append(InterventionOption(
                intervention_type="paid_media_review",
                label="Review paid media performance",
                rationale=(
                    f"Current blended ad spend is ${blended_ad_spend_per_ticket:.2f}/ticket "
                    f"(total spend ${meta_total:.0f}). A manual review of targeting, creative, "
                    f"and budget allocation may identify improvements. No automated financial "
                    f"projection is made — this requires human judgment."
                ),
                expected_revenue=0.0,
                expected_cost=0.0,
                expected_net_value=0.0,
                confidence=0.0,
                risk="low",
                prerequisites=["Meta Ads access", "Active ad account"],
            ))

        # Sort by expected_net_value * confidence (same ranking as opportunity engine)
        options.sort(key=lambda o: o.expected_net_value * o.confidence, reverse=True)
        return options

    # ─────────────────────────────────────────────────────────────────────
    # Grouped-event helper methods
    # ─────────────────────────────────────────────────────────────────────

    def _compute_velocity_for_ids(self, event_ids: List[str], days: int = 7) -> Optional[float]:
        """Aggregate recent ticket velocity across multiple constituent event IDs."""
        if not event_ids:
            return None
        total_velocity = 0.0
        found = False
        for eid in event_ids:
            v = self._compute_velocity(eid, days)
            if v is not None:
                total_velocity += v
                found = True
        return total_velocity if found else None

    def _check_meta_data_status_for_ids(self, event_ids: List[str]) -> str:
        """Check Meta ad spend data freshness across constituent event IDs.

        Returns the best status found (most informative), preferring
        'current_has_spend' > 'current_zero_spend' > 'stale' > 'no_records' > 'unavailable'.
        """
        if not event_ids:
            return "unavailable"
        status_priority = {
            "current_has_spend": 5,
            "current_zero_spend": 4,
            "stale": 3,
            "no_records": 2,
            "unavailable": 1,
        }
        best_status = "unavailable"
        best_priority = 0
        for eid in event_ids:
            s = self._check_meta_data_status(eid)
            p = status_priority.get(s, 0)
            if p > best_priority:
                best_status = s
                best_priority = p
        return best_status

    def _get_meta_spend_for_ids(
        self, event_ids: List[str]
    ) -> Tuple[float, float, int, int]:
        """Aggregate Meta spend across constituent event IDs."""
        total = 0.0
        spend_7d = 0.0
        impressions_7d = 0
        clicks_7d = 0
        for eid in event_ids:
            t, s7, i7, c7 = self._get_meta_spend(eid)
            total += t
            spend_7d += s7
            impressions_7d += i7
            clicks_7d += c7
        return total, spend_7d, impressions_7d, clicks_7d

    def _compute_audience_grouped(
        self, event_ids: List[str], event_context: Dict[str, Any]
    ) -> Dict[str, int]:
        """Count CRM audience, excluding buyers from ALL constituent slots.

        Buyers from any constituent event in this logical day-event are
        excluded — if someone bought a Saturday 10am slot ticket, they are
        excluded from the Saturday afternoon slot audience too.
        """
        result = {
            "total": 0,
            "past_attendees": 0,
            "champions": 0,
            "at_risk": 0,
            "city_prospects": 0,
            "current_buyers": 0,
        }
        try:
            # Collect ALL buyers across all constituent slots
            buyer_set = set()
            for eid in event_ids:
                buyer_set.update(self.db.get_event_buyers(eid))
            result["current_buyers"] = len(buyer_set)

            # Use first constituent for past-attendee lookup (same event name)
            first_event = None
            for eid in event_ids:
                row = self.db.get_event(eid)
                if row:
                    first_event = dict(row)
                    break
            if not first_event:
                first_event = event_context

            buyers_list = list(buyer_set)
            past_attendees = self.db.get_past_attendees_not_purchased(
                event_ids[0] if event_ids else "",
                first_event.get("name", ""),
                limit=50000,
                current_buyer_emails=buyers_list,
            )
            past_attendee_emails = {
                c.get("email") for c in past_attendees if c.get("email")
            }
            result["past_attendees"] = len(past_attendees)
            result["champions"] = len([
                c for c in past_attendees
                if c.get("rfm_segment") in ("champion", "loyal")
            ])
            result["at_risk"] = len([
                c for c in past_attendees
                if c.get("rfm_segment") == "at_risk"
            ])

            city = event_context.get("city", "")
            if city:
                exclude_emails = list(buyer_set | past_attendee_emails)
                city_prospects = self.db.get_city_prospects(
                    city, exclude_emails=exclude_emails, limit=50000
                )
                result["city_prospects"] = len(city_prospects)

            result["total"] = result["past_attendees"] + result["city_prospects"]
        except Exception as e:
            log.warning(f"Grouped audience computation failed: {e}")
        return result

    def _count_campaigns_for_ids(self, event_ids: List[str]) -> int:
        """Count campaigns sent for any constituent event ID."""
        if not event_ids:
            return 0
        total = 0
        for eid in event_ids:
            total += self._count_current_event_campaigns(eid)
        return total
