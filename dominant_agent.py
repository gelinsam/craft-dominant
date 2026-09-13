"""Craft Dominant V2 read-only revenue opportunity engine.

This module turns existing pacing signals into explainable, financially-scored
opportunities. It does not execute external actions. The core rule is simple:
separate revenue at risk from revenue we can reasonably expect to recover.
"""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Dict, List


@dataclass(frozen=True)
class Opportunity:
    opportunity_id: str
    event_id: str
    event_name: str
    opportunity_type: str
    title: str
    rationale: str
    recommended_action: str
    revenue_at_risk: float
    expected_revenue: float
    expected_cost: float
    expected_net_value: float
    confidence: float
    urgency: int
    evidence: Dict[str, Any]
    requires_approval: bool = True

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["confidence_weighted_value"] = round(self.expected_net_value * self.confidence, 2)
        return data


class OpportunityEngine:
    """Rank explainable opportunities without executing external actions."""

    MIN_MODELED_VALUE = 250.0

    def __init__(self, db, decision_engine):
        self.db = db
        self.decision_engine = decision_engine

    @staticmethod
    def _num(value, default=0.0):
        try:
            return default if value is None else float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _recoverable_share(days_until: int) -> float:
        """Conservative recovery assumption based on remaining runway.

        This is intentionally a heuristic, not a claim of causal lift. The value
        is exposed in evidence so it can later be replaced by measured Craft
        intervention performance.
        """
        if days_until >= 42:
            return 0.35
        if days_until >= 21:
            return 0.25
        if days_until >= 8:
            return 0.15
        return 0.08

    @staticmethod
    def _opportunity_id(event_id: str, opportunity_type: str, evidence: Dict[str, Any]) -> str:
        fingerprint = "|".join([
            event_id,
            opportunity_type,
            str(evidence.get("gap_tickets", "")),
            str(evidence.get("pace_delta_pct", "")),
            str(evidence.get("days_until", "")),
        ])
        return sha256(fingerprint.encode("utf-8")).hexdigest()[:16]

    def _avg_ticket_price(self, event_id):
        row = self.db.conn.execute(
            "SELECT COALESCE(SUM(gross_amount),0) revenue, COALESCE(SUM(ticket_count),0) tickets FROM orders WHERE event_id = ?",
            (event_id,),
        ).fetchone()
        if not row:
            return 0.0
        tickets = self._num(row["tickets"])
        revenue = self._num(row["revenue"])
        return revenue / tickets if tickets > 0 else 0.0

    def evaluate_event(self, event_id: str) -> List[Opportunity]:
        event_row = self.db.get_event(event_id)
        if not event_row:
            return []
        event = dict(event_row)
        try:
            pacing = self.decision_engine.analyze_event(event_id)
        except Exception:
            return []
        if not pacing:
            return []

        # Existing DecisionEngine stores pace_vs_historical as a percentage delta:
        # -29 means 29% behind median, +57 means 57% ahead.
        pace_delta = self._num(getattr(pacing, "pace_vs_historical", 0.0))
        avg_price = self._avg_ticket_price(event_id)
        if pace_delta >= -3.0 or avg_price <= 0:
            return []

        sold = self._num(getattr(pacing, "tickets_sold", 0))
        median = self._num(getattr(pacing, "historical_median_at_point", 0))
        days_until = max(0, int(self._num(getattr(pacing, "days_until", 0))))
        gap_tickets = max(0, int(median - sold))
        revenue_at_risk = gap_tickets * avg_price
        recoverable_share = self._recoverable_share(days_until)
        expected_revenue = revenue_at_risk * recoverable_share

        if expected_revenue < self.MIN_MODELED_VALUE:
            return []

        confidence = min(0.80, 0.50 + min(abs(pace_delta) / 100.0, 0.25))
        urgency = max(1, min(10, int(getattr(pacing, "urgency", 5) or 5)))
        evidence = {
            "pace_delta_pct": round(pace_delta, 1),
            "tickets_sold": int(sold),
            "historical_median_at_point": round(median, 1),
            "gap_tickets": gap_tickets,
            "avg_ticket_price": round(avg_price, 2),
            "days_until": days_until,
            "recoverable_share_assumption": recoverable_share,
        }

        opportunity = Opportunity(
            opportunity_id=self._opportunity_id(event_id, "pace_recovery", evidence),
            event_id=event_id,
            event_name=event.get("name", ""),
            opportunity_type="pace_recovery",
            title="Recover pacing gap",
            rationale=(
                f"Event is {abs(pace_delta):.0f}% behind historical median pace, "
                f"putting approximately ${revenue_at_risk:,.0f} of comparable-period revenue at risk."
            ),
            recommended_action="Investigate the cause and prepare a recovery plan for approval.",
            revenue_at_risk=round(revenue_at_risk, 2),
            expected_revenue=round(expected_revenue, 2),
            expected_cost=0.0,
            expected_net_value=round(expected_revenue, 2),
            confidence=round(confidence, 2),
            urgency=urgency,
            evidence=evidence,
            requires_approval=True,
        )
        return [opportunity]

    def evaluate_all(self) -> List[Dict[str, Any]]:
        items = []
        for event in self.db.get_events(upcoming_only=True):
            items.extend(self.evaluate_event(event["event_id"]))
        items.sort(key=lambda item: item.expected_net_value * item.confidence, reverse=True)
        return [item.to_dict() for item in items]

    def command_summary(self) -> Dict[str, Any]:
        items = self.evaluate_all()
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "opportunity_count": len(items),
            "revenue_at_risk": round(sum(i["revenue_at_risk"] for i in items), 2),
            "gross_opportunity": round(sum(i["expected_revenue"] for i in items), 2),
            "net_opportunity": round(sum(i["expected_net_value"] for i in items), 2),
            "confidence_weighted_net": round(sum(i["expected_net_value"] * i["confidence"] for i in items), 2),
            "opportunities": items,
        }
