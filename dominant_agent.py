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

    def _avg_ticket_price_for_ids(self, event_ids: List[str]) -> float:
        """Average ticket price across multiple event IDs.

        Used for grouped timed-entry events whose constituent slot IDs
        are real DB rows, while the grouped event_id is synthetic.
        """
        if not event_ids:
            return 0.0
        placeholders = ",".join("?" * len(event_ids))
        row = self.db.conn.execute(
            f"SELECT COALESCE(SUM(gross_amount),0) revenue, "
            f"COALESCE(SUM(ticket_count),0) tickets "
            f"FROM orders WHERE event_id IN ({placeholders})",
            event_ids,
        ).fetchone()
        if not row:
            return 0.0
        tickets = self._num(row["tickets"])
        revenue = self._num(row["revenue"])
        return revenue / tickets if tickets > 0 else 0.0

    @staticmethod
    def _has_history(pacing) -> bool:
        """True only when pacing has real historical comparison data."""
        median = OpportunityEngine._num(
            getattr(pacing, "historical_median_at_point", 0)
        )
        comparisons = getattr(pacing, "comparison_events", []) or []
        return median > 0 and len(comparisons) > 0

    def _classify_event(self, event_id: str) -> Dict[str, Any]:
        """Return a diagnostic classification explaining why an event does
        or does not produce an opportunity.

        Classifications:
          opportunity      — behind pace with material recovery value
          on_pace          — has history, within or ahead of historical pace
          no_history       — no usable historical comparisons
          no_ticket_price  — average ticket price is zero or negative
          below_materiality — behind pace but recovery below MIN_MODELED_VALUE
          analysis_error   — DecisionEngine could not analyze
          event_not_found  — event_id not in database
        """
        event_row = self.db.get_event(event_id)
        if not event_row:
            return {"classification": "event_not_found",
                    "detail": "Event not in database"}
        try:
            pacing = self.decision_engine.analyze_event(event_id)
        except Exception:
            return {"classification": "analysis_error",
                    "detail": "DecisionEngine raised an exception"}
        if not pacing:
            return {"classification": "analysis_error",
                    "detail": "No pacing data returned"}

        avg_price = self._avg_ticket_price(event_id)
        if avg_price <= 0:
            return {"classification": "no_ticket_price",
                    "detail": "Average ticket price is zero or negative"}

        comparisons = getattr(pacing, "comparison_events", []) or []
        median = self._num(getattr(pacing, "historical_median_at_point", 0))
        has_hist = self._has_history(pacing)

        if not has_hist:
            return {"classification": "no_history",
                    "detail": (f"{len(comparisons)} past edition(s) found, "
                               f"historical median at point = {median}")}

        pace_delta = self._num(getattr(pacing, "pace_vs_historical", 0.0))
        if pace_delta >= -3.0:
            return {"classification": "on_pace",
                    "detail": (f"Pacing {pace_delta:+.1f}% vs historical "
                               f"median ({len(comparisons)} comparison(s))")}

        # Behind pace — check materiality
        sold = self._num(getattr(pacing, "tickets_sold", 0))
        days_until = max(0, int(self._num(getattr(pacing, "days_until", 0))))
        gap_tickets = max(0, int(median - sold))
        revenue_at_risk = gap_tickets * avg_price
        expected_revenue = revenue_at_risk * self._recoverable_share(days_until)

        if expected_revenue < self.MIN_MODELED_VALUE:
            return {"classification": "below_materiality",
                    "detail": (f"Expected recovery ${expected_revenue:,.0f} "
                               f"below ${self.MIN_MODELED_VALUE:,.0f} threshold")}

        return {"classification": "opportunity",
                "detail": (f"{pace_delta:.1f}% behind, "
                           f"${revenue_at_risk:,.0f} at risk")}

    def _classify_pacing(self, pacing) -> Dict[str, Any]:
        """Classify a pre-computed EventPacing object (possibly grouped).

        Same semantics as _classify_event but operates on an already-
        resolved pacing result from analyze_portfolio(), so it never
        calls analyze_event() itself.
        """
        event_ids = getattr(pacing, "constituent_event_ids", []) or []
        if not event_ids:
            event_ids = [pacing.event_id]

        avg_price = self._avg_ticket_price_for_ids(event_ids)
        if avg_price <= 0:
            return {"classification": "no_ticket_price",
                    "detail": "Average ticket price is zero or negative"}

        comparisons = getattr(pacing, "comparison_events", []) or []
        median = self._num(getattr(pacing, "historical_median_at_point", 0))
        has_hist = self._has_history(pacing)

        if not has_hist:
            return {"classification": "no_history",
                    "detail": (f"{len(comparisons)} past edition(s) found, "
                               f"historical median at point = {median}")}

        pace_delta = self._num(getattr(pacing, "pace_vs_historical", 0.0))
        if pace_delta >= -3.0:
            return {"classification": "on_pace",
                    "detail": (f"Pacing {pace_delta:+.1f}% vs historical "
                               f"median ({len(comparisons)} comparison(s))")}

        sold = self._num(getattr(pacing, "tickets_sold", 0))
        days_until = max(0, int(self._num(getattr(pacing, "days_until", 0))))
        gap_tickets = max(0, int(median - sold))
        revenue_at_risk = gap_tickets * avg_price
        expected_revenue = revenue_at_risk * self._recoverable_share(days_until)

        if expected_revenue < self.MIN_MODELED_VALUE:
            return {"classification": "below_materiality",
                    "detail": (f"Expected recovery ${expected_revenue:,.0f} "
                               f"below ${self.MIN_MODELED_VALUE:,.0f} threshold")}

        return {"classification": "opportunity",
                "detail": (f"{pace_delta:.1f}% behind, "
                           f"${revenue_at_risk:,.0f} at risk")}

    def evaluate_pacing(self, pacing) -> List[Opportunity]:
        """Evaluate a pre-computed EventPacing object for opportunities.

        This is the grouped-event counterpart of evaluate_event().  It
        receives an already-resolved pacing result (from analyze_portfolio)
        so that timed-entry grouping and historical matching are done
        exactly once, the same way the existing dashboard does it.
        """
        event_ids = getattr(pacing, "constituent_event_ids", []) or []
        if not event_ids:
            event_ids = [pacing.event_id]

        avg_price = self._avg_ticket_price_for_ids(event_ids)
        if avg_price <= 0:
            return []

        if not self._has_history(pacing):
            return []

        pace_delta = self._num(getattr(pacing, "pace_vs_historical", 0.0))
        if pace_delta >= -3.0:
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

        event_id = pacing.event_id
        event_name = getattr(pacing, "event_name", "")

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
            event_name=event_name,
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

        # --- Separated filters (see _classify_event for semantics) ---
        avg_price = self._avg_ticket_price(event_id)
        if avg_price <= 0:
            return []

        # Distinguish "no historical data" from "on pace".
        # Without history, pace_vs_historical defaults to 0, which is
        # semantically "unknown" — NOT "on pace".  The old compound
        # filter (pace_delta >= -3.0 or avg_price <= 0) conflated the
        # two cases, silently treating every first-year event as healthy.
        if not self._has_history(pacing):
            return []

        pace_delta = self._num(getattr(pacing, "pace_vs_historical", 0.0))
        if pace_delta >= -3.0:
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
        """Evaluate all upcoming events using the same grouped portfolio
        that the existing dashboard uses.

        The dashboard's analyze_portfolio() groups timed-entry slots
        (e.g. six "DC Coffee Festival" slot IDs) into logical day-events
        with properly aggregated tickets, revenue, and historical pacing.
        V2 now consumes that same grouped result so that historical
        comparison operates on the correct aggregation level.
        """
        portfolio = self.decision_engine.analyze_portfolio()
        items = []
        for pacing in portfolio:
            items.extend(self.evaluate_pacing(pacing))
        items.sort(key=lambda item: item.expected_net_value * item.confidence, reverse=True)
        return [item.to_dict() for item in items]

    def command_summary(self) -> Dict[str, Any]:
        portfolio = self.decision_engine.analyze_portfolio()

        items_raw = []
        for pacing in portfolio:
            items_raw.extend(self.evaluate_pacing(pacing))
        items_raw.sort(
            key=lambda item: item.expected_net_value * item.confidence,
            reverse=True,
        )
        items = [item.to_dict() for item in items_raw]

        # --- Data quality diagnostics (over grouped portfolio) ---
        classifications = {}
        event_details = []
        for pacing in portfolio:
            cls = self._classify_pacing(pacing)
            tag = cls["classification"]
            classifications[tag] = classifications.get(tag, 0) + 1
            event_details.append({
                "event_id": pacing.event_id,
                "event_name": getattr(pacing, "event_name", ""),
                "classification": tag,
                "detail": cls["detail"],
            })

        total = len(portfolio)
        no_history = classifications.get("no_history", 0)
        no_price = classifications.get("no_ticket_price", 0)
        errors = classifications.get("analysis_error", 0)
        with_history = total - no_history - no_price - errors - classifications.get("event_not_found", 0)

        warnings = []
        if no_history > 0:
            warnings.append(
                f"{no_history} of {total} events have no historical comparison "
                f"data — opportunity detection requires at least one prior edition"
            )
        if no_price > 0:
            warnings.append(f"{no_price} event(s) have no ticket price data")
        if errors > 0:
            warnings.append(f"{errors} event(s) failed analysis")

        # Zero is trustworthy ONLY when every event had history and
        # none were filtered for data-availability reasons.
        zero_is_trustworthy = (
            len(items) == 0
            and total > 0
            and no_history == 0
            and no_price == 0
            and errors == 0
        )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "opportunity_count": len(items),
            "revenue_at_risk": round(sum(i["revenue_at_risk"] for i in items), 2),
            "gross_opportunity": round(sum(i["expected_revenue"] for i in items), 2),
            "net_opportunity": round(sum(i["expected_net_value"] for i in items), 2),
            "confidence_weighted_net": round(sum(i["expected_net_value"] * i["confidence"] for i in items), 2),
            "opportunities": items,
            "data_quality": {
                "events_evaluated": total,
                "events_with_history": with_history,
                "events_without_history": no_history,
                "coverage_pct": round(with_history / total * 100, 1) if total > 0 else 0,
                "classifications": classifications,
                "event_details": event_details,
                "zero_is_trustworthy": zero_is_trustworthy,
                "warnings": warnings,
            },
        }
