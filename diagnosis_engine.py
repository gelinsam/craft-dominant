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
    cac: float  # cost per acquisition; 0 if no spend data
    # Audience
    crm_audience_total: int
    crm_past_attendees: int
    crm_champions: int
    crm_at_risk: int
    crm_city_prospects: int
    current_buyers_count: int
    # Historical
    historical_editions: List[Dict[str, Any]]
    historical_campaigns_sent: int
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
        meta_total, meta_7d_spend, meta_7d_impressions, meta_7d_clicks = self._get_meta_spend(event_id)
        cac = self._compute_cac(event_id, meta_total)

        # ── CRM audience ────────────────────────────────────────────────
        audience = self._compute_audience(event_id, event)

        # ── Historical editions ─────────────────────────────────────────
        historical_editions = self._get_historical_editions(event_id, event)
        historical_campaigns = self._count_historical_campaigns(event_id)

        # ── Missing data warnings ───────────────────────────────────────
        missing = self._detect_missing_data(
            meta_total, recent_velocity, historical_velocity,
            audience, historical_editions, historical_campaigns,
        )

        # ── Root cause analysis ─────────────────────────────────────────
        root_causes = self._analyze_root_causes(
            pace_delta=pace_delta,
            recent_velocity=recent_velocity,
            historical_velocity=historical_velocity,
            meta_total=meta_total,
            meta_7d_spend=meta_7d_spend,
            meta_7d_clicks=meta_7d_clicks,
            cac=cac,
            days_until=days_until,
            sell_through=sell_through,
            audience=audience,
            historical_campaigns=historical_campaigns,
        )

        # ── Intervention options ────────────────────────────────────────
        intervention_options = self._rank_interventions(
            root_causes=root_causes,
            gap_tickets=gap_tickets,
            avg_price=avg_price,
            days_until=days_until,
            audience=audience,
            meta_total=meta_total,
            cac=cac,
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
            cac=round(cac, 2),
            crm_audience_total=audience["total"],
            crm_past_attendees=audience["past_attendees"],
            crm_champions=audience["champions"],
            crm_at_risk=audience["at_risk"],
            crm_city_prospects=audience["city_prospects"],
            current_buyers_count=audience["current_buyers"],
            historical_editions=historical_editions,
            historical_campaigns_sent=historical_campaigns,
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
            total = self.db.get_event_spend(event_id)
            seven_days_ago = (date.today() - timedelta(days=7)).isoformat()
            rows = self.db.conn.execute(
                """SELECT SUM(spend) as s, SUM(impressions) as i, SUM(clicks) as c
                   FROM ad_spend WHERE event_id = ? AND spend_date >= ?""",
                (event_id, seven_days_ago),
            ).fetchone()
            if rows:
                spend_7d = self._num(rows["s"])
                impressions_7d = int(self._num(rows["i"]))
                clicks_7d = int(self._num(rows["c"]))
        except Exception as e:
            log.warning(f"Meta spend lookup failed for {event_id}: {e}")
        return total, spend_7d, impressions_7d, clicks_7d

    def _compute_cac(self, event_id: str, total_spend: float) -> float:
        """Cost per acquisition: total ad spend / tickets sold."""
        if total_spend <= 0:
            return 0.0
        try:
            tickets = self.db.get_event_tickets(event_id)
            return total_spend / tickets if tickets > 0 else 0.0
        except Exception:
            return 0.0

    def _compute_audience(self, event_id: str, event: dict) -> Dict[str, int]:
        """Count available CRM segments, excluding current buyers."""
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
                city_prospects = self.db.get_city_prospects(
                    city, exclude_emails=buyers, limit=50000
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

    def _count_historical_campaigns(self, event_id: str) -> int:
        """Count campaigns already sent for this event."""
        try:
            row = self.db.conn.execute(
                "SELECT COUNT(*) as cnt FROM campaigns WHERE event_id = ?",
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
    ) -> List[str]:
        warnings = []
        if meta_total <= 0:
            warnings.append("No Meta ad spend data available — cannot assess paid performance or CAC.")
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
        cac: float,
        days_until: int,
        sell_through: float,
        audience: dict,
        historical_campaigns: int,
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

        # 2. Paid underperformance
        if meta_total > 0:
            if meta_7d_spend > 0 and meta_7d_clicks < 5:
                causes.append(RootCause(
                    cause="Paid ads are spending without driving clicks",
                    evidence=[
                        f"Last 7 days: ${meta_7d_spend:.0f} spent, {meta_7d_clicks} clicks",
                        f"Total campaign spend: ${meta_total:.0f}",
                    ],
                    confidence=0.70,
                ))
            if cac > 0 and cac > 50:
                causes.append(RootCause(
                    cause="Customer acquisition cost is high",
                    evidence=[
                        f"CAC is ${cac:.2f} per ticket (total spend ${meta_total:.0f})",
                    ],
                    confidence=0.60,
                ))
        elif meta_total <= 0 and days_until > 14:
            causes.append(RootCause(
                cause="No paid advertising detected",
                evidence=[
                    "No Meta ad spend records found for this event",
                    f"{days_until} days of runway remain",
                ],
                confidence=0.50,
            ))

        # 3. Under-marketed (few campaigns sent)
        if historical_campaigns < 2 and days_until > 7:
            causes.append(RootCause(
                cause="Event may be under-marketed",
                evidence=[
                    f"Only {historical_campaigns} campaign(s) sent so far",
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
        cac: float,
    ) -> List[InterventionOption]:
        options: List[InterventionOption] = []

        # Option 1: CRM recovery campaign (always available if audience exists)
        if audience["total"] > 0:
            reachable = audience["past_attendees"] + audience["city_prospects"]
            # Expected conversions from an email campaign
            expected_opens = int(reachable * self.EMAIL_OPEN_RATE)
            expected_clicks = int(expected_opens * self.EMAIL_CLICK_RATE)
            # Champion boost
            champion_conversions = int(
                audience["champions"] * self.EMAIL_OPEN_RATE * self.EMAIL_CLICK_RATE * self.CHAMPION_MULTIPLIER
            )
            base_conversions = int(expected_clicks * self.EMAIL_CONVERSION_RATE)
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

        # Option 2: Ad budget reallocation (only if we have spend data)
        if meta_total > 0 and cac > 0 and days_until > 7:
            # Model: what if we could reduce CAC by 20% through better targeting?
            improved_cac = cac * 0.80
            additional_budget = min(meta_total * 0.25, gap_tickets * improved_cac)
            additional_tickets = int(additional_budget / improved_cac) if improved_cac > 0 else 0
            additional_tickets = min(additional_tickets, gap_tickets)
            expected_rev = additional_tickets * avg_price

            options.append(InterventionOption(
                intervention_type="ad_budget_shift",
                label="Paid ad optimization / budget shift",
                rationale=(
                    f"Current CAC is ${cac:.2f}. Reallocating or optimizing "
                    f"${additional_budget:.0f} in ad spend could yield ~{additional_tickets} "
                    f"additional tickets at improved targeting."
                ),
                expected_revenue=round(expected_rev, 2),
                expected_cost=round(additional_budget, 2),
                expected_net_value=round(expected_rev - additional_budget, 2),
                confidence=0.40,
                risk="medium",
                prerequisites=["Meta Ads access", "Active ad account"],
            ))

        # Sort by expected_net_value * confidence (same ranking as opportunity engine)
        options.sort(key=lambda o: o.expected_net_value * o.confidence, reverse=True)
        return options
