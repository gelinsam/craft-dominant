"""Craft Dominant V2 opportunity engine.

This module converts existing event pacing and CRM data into a ranked queue of
reviewable revenue opportunities. It is intentionally read-only: it does not
send campaigns, change pricing, or alter ad budgets.
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List


@dataclass
class Opportunity:
    event_id: str
    event_name: str
    opportunity_type: str
    title: str
    rationale: str
    expected_revenue: float
    confidence: float
    urgency: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OpportunityEngine:
    """Build a ranked, read-only opportunity queue from Craft's existing data."""

    def __init__(self, db, decision_engine):
        self.db = db
        self.decision_engine = decision_engine

    @staticmethod
    def _num(value: Any, default: float = 0.0) -> float:
        try:
            return default if value is None else float(value)
        except (TypeError, ValueError):
            return default

    def _avg_ticket_price(self, event_id: str) -> float:
        row = self.db.conn.execute(
            """SELECT COALESCE(SUM(gross_amount),0) revenue,
                      COALESCE(SUM(ticket_count),0) tickets
               FROM orders WHERE event_id = ?""",
            (event_id,),
        ).fetchone()
        if not row:
            return 0.0
        tickets = self._num(row['tickets'])
        revenue = self._num(row['revenue'])
        return revenue / tickets if tickets else 0.0

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

        items: List[Opportunity] = []
        avg_price = self._avg_ticket_price(event_id)
        pace = self._num(getattr(pacing, 'pace_vs_historical', 1.0), 1.0)

        if pace < 0.97 and avg_price > 0:
            gap_tickets = max(0, int(self._num(getattr(pacing, 'historical_median_at_point', 0)) - self._num(getattr(pacing, 'tickets_sold', 0))))
            expected_revenue = gap_tickets * avg_price
            if expected_revenue >= 250:
                items.append(Opportunity(
                    event_id=event_id,
                    event_name=event.get('name', ''),
                    opportunity_type='pace_recovery',
                    title='Recover pacing gap',
                    rationale=f"Current pace is {pace:.0%} of historical pace.",
                    expected_revenue=round(expected_revenue, 2),
                    confidence=0.60,
                    urgency=max(1, min(10, int(getattr(pacing, 'urgency', 5) or 5))),
                ))

        return items

    def evaluate_all(self) -> List[Dict[str, Any]]:
        items: List[Opportunity] = []
        for event in self.db.get_events(upcoming_only=True):
            items.extend(self.evaluate_event(event['event_id']))
        items.sort(key=lambda x: x.expected_revenue * x.confidence, reverse=True)
        return [item.to_dict() for item in items]

    def command_summary(self) -> Dict[str, Any]:
        items = self.evaluate_all()
        return {
            'generated_at': datetime.utcnow().isoformat() + 'Z',
            'opportunity_count': len(items),
            'gross_opportunity': round(sum(i['expected_revenue'] for i in items), 2),
            'confidence_weighted_opportunity': round(sum(i['expected_revenue'] * i['confidence'] for i in items), 2),
            'opportunities': items,
        }
