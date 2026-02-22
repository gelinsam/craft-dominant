import os
import sys
import json
import csv
import io
import sqlite3
import hashlib
import logging
import statistics
import re
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from contextlib import contextmanager
from enum import Enum
try:
    import requests
except ImportError:
    requests = None
try:
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger('craft')

def _normalize_event_pattern(name: str, include_season: bool = False) -> str:
    """Single source of truth for event pattern extraction.

    Normalizes event names so 'Philly Cocktail Festival 2025' and 'Philly Cocktail Fest 2026'
    produce the same pattern. Used for matching past editions, timed-entry grouping, and pacing curves.
    """
    name_lower = name.lower()
    name_lower = re.sub(r'20\d{2}', '', name_lower)
    replacements = {
        'philadelphia': 'philly', 'washington dc': 'dc', 'district': 'dc',
        'new york': 'nyc', 'los angeles': 'la', 'san francisco': 'sf', 'san diego': 'sd',
        'festival': 'fest', 'experience': 'exp', 'celebration': 'fest',
        'tasting event': 'tasting', 'pop-up': 'popup', 'pop up': 'popup',
    }
    for old, new in replacements.items():
        name_lower = name_lower.replace(old, new)
    season = ''
    if include_season:
        if 'winter' in name_lower: season = '_winter'
        elif 'spring' in name_lower: season = '_spring'
        elif 'fall' in name_lower: season = '_fall'
    name_lower = re.sub(r'[^a-z\s]', '', name_lower)
    name_lower = '_'.join(name_lower.split())
    name_lower = re.sub(r'_edition|_+', '_', name_lower).strip('_')
    return name_lower + season

def _json_key_match(json_field, key: str) -> bool:
    """Check if key exists in a JSON dict field — safe, no substring false positives."""
    if not key or not json_field:
        return False
    try:
        d = json.loads(json_field) if isinstance(json_field, str) else json_field
        if isinstance(d, dict):
            return key in d
    except (json.JSONDecodeError, TypeError):
        pass
    return False
# =============================================================================
# DATA MODELS
# =============================================================================
class Decision(Enum):
    PIVOT = "pivot"
    PUSH = "push"
    MAINTAIN = "maintain"
    COAST = "coast"
    NOT_STARTED = "not_started"
@dataclass
class Customer:
    """Complete customer record with LTV."""
    email: str
    # Lifetime stats
    total_orders: int = 0
    total_tickets: int = 0
    total_spent: float = 0
    total_events_attended: int = 0
    # Dates
    first_order_date: str = ""
    last_order_date: str = ""
    days_since_last_order: int = 0
    customer_tenure_days: int = 0
    # Averages
    avg_order_value: float = 0
    avg_tickets_per_order: float = 0
    avg_days_between_orders: float = 0
    # Preferences
    favorite_event_type: str = ""
    favorite_city: str = ""
    event_types: Dict[str, int] = field(default_factory=dict)
    cities: Dict[str, int] = field(default_factory=dict)
    # Timing behavior
    avg_days_before_event: float = 0
    timing_segment: str = ""  # super_early_bird, early_bird, planner, spontaneous, last_minute
    # RFM Scoring
    rfm_recency: int = 0      # 1-5
    rfm_frequency: int = 0    # 1-5
    rfm_monetary: int = 0     # 1-5
    rfm_segment: str = ""     # champion, loyal, at_risk, etc.
    # Lifetime Value
    ltv_score: float = 0      # 0-100 composite
    ltv_projected: float = 0  # Projected future value
    # Lists
    events_attended: List[str] = field(default_factory=list)
@dataclass
class EventPacing:
    """Pacing analysis for an event."""
    event_id: str
    event_name: str
    event_date: str
    days_until: int
    # Current state
    tickets_sold: int
    capacity: int
    revenue: float
    ad_spend: float
    sell_through: float
    cac: float
    # Historical comparison
    historical_median_at_point: float
    historical_range: Tuple[float, float]
    pace_vs_historical: float
    comparison_events: List[str]
    comparison_years: List[int]
    # Projection
    projected_final: int
    projected_range: Tuple[int, int]
    confidence: float
    # Decision
    decision: Decision
    urgency: int
    rationale: str
    actions: List[str]
    # Targeting
    high_value_targets: int
    reactivation_targets: int
    # Historical year-by-year comparisons
    historical_comparisons: List[dict] = field(default_factory=list)
    # For timed-entry groups: the real DB event_ids that make up this grouped event
    constituent_event_ids: List[str] = field(default_factory=list)
# =============================================================================
# DATABASE - UNIFIED SCHEMA
# =============================================================================
UNIFIED_SCHEMA = """
-- Events (current and historical)
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    event_type TEXT,
    city TEXT,
    event_date TEXT NOT NULL,
    capacity INTEGER DEFAULT 0,
    status TEXT DEFAULT 'upcoming',  -- upcoming, live, completed
    platform TEXT DEFAULT 'eventbrite',
    meta_campaign_id TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
-- All orders (historical + current)
CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    email TEXT NOT NULL,
    order_timestamp TEXT NOT NULL,
    ticket_count INTEGER DEFAULT 1,
    gross_amount REAL DEFAULT 0,
    net_amount REAL DEFAULT 0,
    ticket_type TEXT,
    promo_code TEXT,
    days_before_event INTEGER,
    FOREIGN KEY (event_id) REFERENCES events(event_id)
);
-- Customers (built from orders)
CREATE TABLE IF NOT EXISTS customers (
    email TEXT PRIMARY KEY,
    total_orders INTEGER DEFAULT 0,
    total_tickets INTEGER DEFAULT 0,
    total_spent REAL DEFAULT 0,
    total_events INTEGER DEFAULT 0,
    first_order_date TEXT,
    last_order_date TEXT,
    days_since_last INTEGER DEFAULT 0,
    tenure_days INTEGER DEFAULT 0,
    avg_order_value REAL DEFAULT 0,
    avg_tickets_per_order REAL DEFAULT 0,
    avg_days_between_orders REAL DEFAULT 0,
    avg_days_before_event REAL DEFAULT 0,
    favorite_event_type TEXT,
    favorite_city TEXT,
    event_types TEXT,  -- JSON
    cities TEXT,       -- JSON
    events_attended TEXT,  -- JSON list
    timing_segment TEXT,
    rfm_r INTEGER DEFAULT 0,
    rfm_f INTEGER DEFAULT 0,
    rfm_m INTEGER DEFAULT 0,
    rfm_segment TEXT,
    ltv_score REAL DEFAULT 0,
    ltv_projected REAL DEFAULT 0,
    updated_at TEXT
);
-- Daily snapshots (for pacing curves)
CREATE TABLE IF NOT EXISTS daily_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    snapshot_date TEXT NOT NULL,
    days_before_event INTEGER NOT NULL,
    tickets_cumulative INTEGER DEFAULT 0,
    revenue_cumulative REAL DEFAULT 0,
    tickets_that_day INTEGER DEFAULT 0,
    revenue_that_day REAL DEFAULT 0,
    orders_that_day INTEGER DEFAULT 0,
    sell_through_pct REAL DEFAULT 0,
    ad_spend_cumulative REAL DEFAULT 0,
    UNIQUE(event_id, snapshot_date)
);
-- Pacing curves (aggregated from past events)
CREATE TABLE IF NOT EXISTS pacing_curves (
    pattern TEXT PRIMARY KEY,
    event_type TEXT,
    source_events TEXT,  -- JSON
    curve_data TEXT,     -- JSON: {days_before: {median, p25, p75, samples}}
    avg_final_sell_through REAL,
    sample_count INTEGER,
    updated_at TEXT
);
-- Ad spend by day
CREATE TABLE IF NOT EXISTS ad_spend (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT,
    campaign_id TEXT,
    campaign_name TEXT,
    spend_date TEXT NOT NULL,
    spend REAL DEFAULT 0,
    impressions INTEGER DEFAULT 0,
    clicks INTEGER DEFAULT 0,
    UNIQUE(event_id, spend_date, campaign_id)
);
-- Analysis cache
CREATE TABLE IF NOT EXISTS analysis_cache (
    event_id TEXT PRIMARY KEY,
    analysis_json TEXT,
    updated_at TEXT
);
-- Indexes
CREATE INDEX IF NOT EXISTS idx_orders_event ON orders(event_id);
CREATE INDEX IF NOT EXISTS idx_orders_email ON orders(email);
CREATE INDEX IF NOT EXISTS idx_orders_timestamp ON orders(order_timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshots_event ON daily_snapshots(event_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_days ON daily_snapshots(days_before_event);
CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(rfm_segment);
CREATE INDEX IF NOT EXISTS idx_customers_ltv ON customers(ltv_score DESC);
CREATE INDEX IF NOT EXISTS idx_events_date ON events(event_date);
"""
class Database:
    """Unified database for all Craft data."""
    def __init__(self, path: str = "craft_unified.db"):
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys = ON")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self._init_schema()
    def _init_schema(self):
        self.conn.executescript(UNIFIED_SCHEMA)
        self.conn.commit()
    @contextmanager
    def transaction(self):
        try:
            yield self.conn
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e
    # === Events ===
    def upsert_event(self, event: dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO events
                (event_id, name, event_type, city, event_date, capacity, status, platform, meta_campaign_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                event['event_id'], event['name'], event.get('event_type'),
                event.get('city'), event['event_date'], event.get('capacity', 0),
                event.get('status', 'upcoming'), event.get('platform', 'eventbrite'),
                event.get('meta_campaign_id')
            ))
    def get_event(self, event_id: str) -> Optional[dict]:
        row = self.conn.execute("SELECT * FROM events WHERE event_id = ?", (event_id,)).fetchone()
        return dict(row) if row else None
    def get_events(self, status: str = None, upcoming_only: bool = False) -> List[dict]:
        query = "SELECT * FROM events"
        params = []
        conditions = []
        if status:
            conditions.append("status = ?")
            params.append(status)
        if upcoming_only:
            conditions.append("event_date >= ?")
            params.append(date.today().isoformat())
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        query += " ORDER BY event_date"
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    def get_past_events(self, pattern: str = None) -> List[dict]:
        query = "SELECT * FROM events WHERE event_date < ? AND status = 'completed'"
        params = [date.today().isoformat()]
        if pattern:
            query += " AND name LIKE ?"
            params.append(f"%{pattern}%")
        query += " ORDER BY event_date DESC"
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    # === Orders ===
    def insert_order(self, order: dict):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO orders
                (order_id, event_id, email, order_timestamp, ticket_count,
                 gross_amount, net_amount, ticket_type, promo_code, days_before_event)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                order['order_id'], order['event_id'], order['email'].lower().strip(),
                order['order_timestamp'], order.get('ticket_count', 1),
                order.get('gross_amount', 0), order.get('net_amount', 0),
                order.get('ticket_type'), order.get('promo_code'),
                order.get('days_before_event')
            ))
    def get_orders_for_event(self, event_id: str) -> List[dict]:
        rows = self.conn.execute(
            "SELECT * FROM orders WHERE event_id = ? ORDER BY order_timestamp",
            (event_id,)
        ).fetchall()
        return [dict(r) for r in rows]
    def get_orders_for_customer(self, email: str) -> List[dict]:
        rows = self.conn.execute("""
            SELECT o.*, e.name as event_name, e.event_type, e.city, e.event_date
            FROM orders o
            JOIN events e ON o.event_id = e.event_id
            WHERE o.email = ?
            ORDER BY o.order_timestamp DESC
        """, (email.lower().strip(),)).fetchall()
        return [dict(r) for r in rows]
    def get_all_emails(self) -> List[str]:
        rows = self.conn.execute("SELECT DISTINCT email FROM orders").fetchall()
        return [r['email'] for r in rows]
    def get_event_purchasers(self, event_id: str) -> List[str]:
        rows = self.conn.execute(
            "SELECT DISTINCT email FROM orders WHERE event_id = ?", (event_id,)
        ).fetchall()
        return [r['email'] for r in rows]
    def get_event_tickets(self, event_id: str) -> int:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(ticket_count), 0) as total FROM orders WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        return row['total'] if row else 0
    def get_event_revenue(self, event_id: str) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(gross_amount), 0) as total FROM orders WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        return row['total'] if row else 0
    # === Customers ===
    def upsert_customer(self, customer: Customer):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO customers
                (email, total_orders, total_tickets, total_spent, total_events,
                 first_order_date, last_order_date, days_since_last, tenure_days,
                 avg_order_value, avg_tickets_per_order, avg_days_between_orders,
                 avg_days_before_event, favorite_event_type, favorite_city,
                 event_types, cities, events_attended, timing_segment,
                 rfm_r, rfm_f, rfm_m, rfm_segment, ltv_score, ltv_projected, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                customer.email, customer.total_orders, customer.total_tickets,
                customer.total_spent, customer.total_events_attended,
                customer.first_order_date, customer.last_order_date,
                customer.days_since_last_order, customer.customer_tenure_days,
                customer.avg_order_value, customer.avg_tickets_per_order,
                customer.avg_days_between_orders, customer.avg_days_before_event,
                customer.favorite_event_type, customer.favorite_city,
                json.dumps(customer.event_types), json.dumps(customer.cities),
                json.dumps(customer.events_attended), customer.timing_segment,
                customer.rfm_recency, customer.rfm_frequency, customer.rfm_monetary,
                customer.rfm_segment, customer.ltv_score, customer.ltv_projected,
                datetime.now().isoformat()
            ))
    def get_customer(self, email: str) -> Optional[Customer]:
        row = self.conn.execute("SELECT * FROM customers WHERE email = ?", (email.lower(),)).fetchone()
        if not row:
            return None
        return Customer(
            email=row['email'],
            total_orders=row['total_orders'],
            total_tickets=row['total_tickets'],
            total_spent=row['total_spent'],
            total_events_attended=row['total_events'],
            first_order_date=row['first_order_date'] or '',
            last_order_date=row['last_order_date'] or '',
            days_since_last_order=row['days_since_last'],
            customer_tenure_days=row['tenure_days'],
            avg_order_value=row['avg_order_value'],
            avg_tickets_per_order=row['avg_tickets_per_order'],
            avg_days_between_orders=row['avg_days_between_orders'],
            avg_days_before_event=row['avg_days_before_event'],
            favorite_event_type=row['favorite_event_type'] or '',
            favorite_city=row['favorite_city'] or '',
            event_types=json.loads(row['event_types'] or '{}'),
            cities=json.loads(row['cities'] or '{}'),
            events_attended=json.loads(row['events_attended'] or '[]'),
            timing_segment=row['timing_segment'] or '',
            rfm_recency=row['rfm_r'],
            rfm_frequency=row['rfm_f'],
            rfm_monetary=row['rfm_m'],
            rfm_segment=row['rfm_segment'] or '',
            ltv_score=row['ltv_score'],
            ltv_projected=row['ltv_projected']
        )
    def get_customers(self, segment: str = None, min_ltv: float = None,
                      limit: int = 100, offset: int = 0,
                      sort_by: str = 'ltv_score', order: str = 'DESC',
                      search: str = None, city: str = None,
                      event_type: str = None) -> List[dict]:
        query = "SELECT * FROM customers WHERE 1=1"
        params = []
        if segment:
            query += " AND rfm_segment = ?"
            params.append(segment)
        if min_ltv is not None:
            query += " AND ltv_score >= ?"
            params.append(min_ltv)
        if search:
            query += " AND email LIKE ?"
            params.append(f"%{search.lower()}%")
        if city:
            query += " AND favorite_city = ?"
            params.append(city)
        if event_type:
            query += " AND favorite_event_type = ?"
            params.append(event_type)
        # Validate sort column
        valid_sorts = ['ltv_score', 'total_spent', 'total_orders', 'days_since_last',
                       'total_events', 'favorite_city', 'favorite_event_type',
                       'avg_order_value', 'total_tickets', 'tenure_days']
        if sort_by not in valid_sorts:
            sort_by = 'ltv_score'
        order = 'DESC' if order.upper() == 'DESC' else 'ASC'
        query += f" ORDER BY {sort_by} {order} LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    def get_customer_count(self, segment: str = None, search: str = None,
                           city: str = None, event_type: str = None) -> int:
        query = "SELECT COUNT(*) as cnt FROM customers WHERE 1=1"
        params = []
        if segment:
            query += " AND rfm_segment = ?"
            params.append(segment)
        if search:
            query += " AND email LIKE ?"
            params.append(f"%{search.lower()}%")
        if city:
            query += " AND favorite_city = ?"
            params.append(city)
        if event_type:
            query += " AND favorite_event_type = ?"
            params.append(event_type)
        row = self.conn.execute(query, params).fetchone()
        return row['cnt'] if row else 0
    def get_distinct_cities(self) -> List[str]:
        rows = self.conn.execute("""
            SELECT DISTINCT favorite_city FROM customers
            WHERE favorite_city IS NOT NULL AND favorite_city != ''
            ORDER BY favorite_city
        """).fetchall()
        return [r['favorite_city'] for r in rows]
    def get_distinct_event_types(self) -> List[str]:
        rows = self.conn.execute("""
            SELECT DISTINCT favorite_event_type FROM customers
            WHERE favorite_event_type IS NOT NULL AND favorite_event_type != ''
            ORDER BY favorite_event_type
        """).fetchall()
        return [r['favorite_event_type'] for r in rows]
    def get_segment_counts(self) -> Dict[str, int]:
        rows = self.conn.execute("""
            SELECT rfm_segment, COUNT(*) as cnt
            FROM customers
            WHERE rfm_segment IS NOT NULL AND rfm_segment != ''
            GROUP BY rfm_segment
        """).fetchall()
        return {r['rfm_segment']: r['cnt'] for r in rows}
    def get_high_value_customers(self, event_type: str = None, city: str = None,
                                 min_ltv: float = 50, limit: int = 500) -> List[dict]:
        """Get high-value customers for targeting, optionally filtered by affinity."""
        query = "SELECT * FROM customers WHERE ltv_score >= ?"
        params = [min_ltv]
        if event_type:
            query += " AND event_types LIKE ?"
            params.append(f'%"{event_type}"%')
        if city:
            query += " AND cities LIKE ?"
            params.append(f'%"{city}"%')
        query += " ORDER BY ltv_score DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]
    def get_at_risk_customers(self, min_orders: int = 2, min_days_inactive: int = 180) -> List[dict]:
        """Get customers who used to be active but haven't purchased recently."""
        rows = self.conn.execute("""
            SELECT * FROM customers
            WHERE total_orders >= ? AND days_since_last >= ?
            ORDER BY total_spent DESC
        """, (min_orders, min_days_inactive)).fetchall()
        return [dict(r) for r in rows]
    # === Intelligence Queries ===
    def get_cross_sell_candidates(self, event_type: str, city: str,
                                   exclude_event_ids: list = None,
                                   exclude_emails: set = None,
                                   limit: int = 2000) -> List[dict]:
        """Find customers who attended OTHER event types in same city — cross-sell targets.
        E.g., Beer Fest buyers who might like Cocktail Fest."""
        rows = self.conn.execute("""
            SELECT DISTINCT o.email, e.event_type, e.city, e.name as event_name
            FROM orders o
            JOIN events e ON o.event_id = e.event_id
            WHERE e.city = ? AND e.event_type != ? AND e.event_type IS NOT NULL
            ORDER BY o.order_timestamp DESC
        """, (city, event_type)).fetchall()
        # Group by email, track which event types they've attended
        email_types = defaultdict(set)
        for r in rows:
            email_types[r['email']].add(r['event_type'])
        # Get customer records in batch
        emails = [e for e in email_types if (not exclude_emails or e not in exclude_emails)][:limit * 2]
        if not emails:
            return []
        cust_map = {}
        batch_size = 500
        for i in range(0, len(emails), batch_size):
            batch = emails[i:i + batch_size]
            ph = ','.join(['?' for _ in batch])
            cust_rows = self.conn.execute(
                f"SELECT * FROM customers WHERE email IN ({ph})", batch
            ).fetchall()
            for c in cust_rows:
                cust_map[c['email']] = dict(c)
        results = []
        for email in emails:
            cust = cust_map.get(email)
            if cust:
                cust['attended_types'] = list(email_types[email])
                results.append(cust)
            if len(results) >= limit:
                break
        results.sort(key=lambda x: -(x.get('ltv_score', 0) or 0))
        return results

    def get_multi_ticket_buyers(self, min_avg_tickets: float = 1.5) -> List[dict]:
        """Find super-spreaders: customers who consistently buy 2+ tickets (bringing friends)."""
        rows = self.conn.execute("""
            SELECT * FROM customers
            WHERE avg_tickets_per_order >= ? AND total_orders >= 2
            ORDER BY avg_tickets_per_order DESC, total_spent DESC
        """, (min_avg_tickets,)).fetchall()
        return [dict(r) for r in rows]

    def get_promo_code_stats(self, event_id: str = None) -> List[dict]:
        """Get promo code usage stats — which codes drive sales and at what discount."""
        if event_id:
            rows = self.conn.execute("""
                SELECT promo_code, COUNT(*) as uses, SUM(ticket_count) as tickets,
                       SUM(gross_amount) as revenue, AVG(gross_amount) as avg_order,
                       COUNT(DISTINCT email) as unique_buyers
                FROM orders
                WHERE event_id = ? AND promo_code IS NOT NULL AND promo_code != ''
                GROUP BY promo_code
                ORDER BY uses DESC
            """, (event_id,)).fetchall()
        else:
            rows = self.conn.execute("""
                SELECT promo_code, COUNT(*) as uses, SUM(ticket_count) as tickets,
                       SUM(gross_amount) as revenue, AVG(gross_amount) as avg_order,
                       COUNT(DISTINCT email) as unique_buyers
                FROM orders
                WHERE promo_code IS NOT NULL AND promo_code != ''
                GROUP BY promo_code
                ORDER BY uses DESC
            """).fetchall()
        return [dict(r) for r in rows]

    def get_purchase_velocity(self, event_id: str) -> dict:
        """Analyze purchase timing patterns for an event's historical buyers."""
        event = self.get_event(event_id)
        if not event:
            return {}
        # Get orders for this event with timing data
        rows = self.conn.execute("""
            SELECT days_before_event, COUNT(*) as order_count, SUM(ticket_count) as tickets,
                   SUM(gross_amount) as revenue
            FROM orders
            WHERE event_id = ? AND days_before_event IS NOT NULL
            GROUP BY days_before_event
            ORDER BY days_before_event DESC
        """, (event_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_churn_risk_customers(self, lookback_window: int = 90) -> List[dict]:
        """Find customers approaching their churn point based on purchase gap patterns.
        If avg_days_between_orders is 120 and they're at 100 days since last, they're at risk."""
        rows = self.conn.execute("""
            SELECT *,
                   CASE WHEN avg_days_between_orders > 0
                        THEN CAST(days_since_last AS REAL) / avg_days_between_orders
                        ELSE 0 END as gap_ratio
            FROM customers
            WHERE total_orders >= 2
              AND avg_days_between_orders > 0
              AND days_since_last >= (avg_days_between_orders * 0.7)
            ORDER BY gap_ratio DESC
        """).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            gap_ratio = d.get('gap_ratio', 0) or 0
            avg_gap = d.get('avg_days_between_orders', 0) or 0
            days_since = d.get('days_since_last', 0) or 0
            # Predict days until churn (when gap_ratio reaches 2.0 = missed 2 cycles)
            churn_threshold = avg_gap * 2.0
            days_until_churn = max(0, int(churn_threshold - days_since))
            d['gap_ratio'] = round(gap_ratio, 2)
            d['days_until_churn'] = days_until_churn
            d['save_window'] = 'critical' if days_until_churn < 14 else 'urgent' if days_until_churn < 30 else 'watch' if days_until_churn < 60 else 'healthy'
            results.append(d)
        return results

    def get_vip_customers(self, min_events: int = 3, min_spent: float = 200,
                           limit: int = 100) -> List[dict]:
        """Identify VIP customers — top spenders with high attendance."""
        rows = self.conn.execute("""
            SELECT * FROM customers
            WHERE total_events >= ? AND total_spent >= ?
            ORDER BY total_spent DESC
            LIMIT ?
        """, (min_events, min_spent, limit)).fetchall()
        return [dict(r) for r in rows]

    def get_event_ticket_types(self, event_id: str) -> List[dict]:
        """Break down ticket types for an event — GA vs VIP vs Early Bird etc."""
        rows = self.conn.execute("""
            SELECT ticket_type, COUNT(*) as orders, SUM(ticket_count) as tickets,
                   SUM(gross_amount) as revenue, AVG(gross_amount) as avg_price,
                   AVG(days_before_event) as avg_days_before
            FROM orders
            WHERE event_id = ? AND ticket_type IS NOT NULL AND ticket_type != ''
            GROUP BY ticket_type
            ORDER BY tickets DESC
        """, (event_id,)).fetchall()
        return [dict(r) for r in rows]

    def get_segment_ticket_preferences(self) -> dict:
        """Which RFM segments prefer which ticket tiers?"""
        rows = self.conn.execute("""
            SELECT c.rfm_segment, o.ticket_type, COUNT(*) as count,
                   AVG(o.gross_amount) as avg_price
            FROM orders o
            JOIN customers c ON lower(o.email) = c.email
            WHERE o.ticket_type IS NOT NULL AND o.ticket_type != ''
              AND c.rfm_segment IS NOT NULL AND c.rfm_segment != ''
            GROUP BY c.rfm_segment, o.ticket_type
            ORDER BY c.rfm_segment, count DESC
        """).fetchall()
        result = defaultdict(list)
        for r in rows:
            result[r['rfm_segment']].append({
                'ticket_type': r['ticket_type'],
                'count': r['count'],
                'avg_price': round(r['avg_price'], 2)
            })
        return dict(result)

    # === Targeting ===
    def get_event_buyers(self, event_id: str) -> set:
        """Get set of emails that have orders for a specific event."""
        rows = self.conn.execute(
            "SELECT DISTINCT lower(email) as email FROM orders WHERE event_id = ?",
            (event_id,)
        ).fetchall()
        return {r['email'] for r in rows}
    def get_pattern_event_ids(self, pattern: str, exclude_ids: list = None) -> List[str]:
        """Get all event IDs matching a pattern name (for finding past editions)."""
        rows = self.conn.execute("SELECT event_id, name FROM events").fetchall()
        matched = []
        for r in rows:
            candidate = _normalize_event_pattern(r['name'], include_season=False)
            if candidate == pattern or pattern in candidate or candidate in pattern:
                if not exclude_ids or r['event_id'] not in exclude_ids:
                    matched.append(r['event_id'])
        return matched
    def get_past_attendees_not_purchased(self, event_id: str, event_name: str,
                                          limit: int = 2000,
                                          current_buyer_emails: set = None,
                                          exclude_event_ids: list = None) -> List[dict]:
        """Find customers who attended past editions of this event but haven't bought this year's.

        Args:
            current_buyer_emails: Pre-computed set of buyer emails to exclude (for grouped events).
                                  If None, looks up buyers for event_id only.
            exclude_event_ids: All current-year event_ids to exclude from "past" events.
                               If None, only excludes event_id.
        """
        # Get current buyers — use provided set or look up single event
        current_buyers = current_buyer_emails if current_buyer_emails is not None else self.get_event_buyers(event_id)
        # Get pattern for this event (no season — match across all seasons)
        pattern = _normalize_event_pattern(event_name, include_season=False)
        # Find all past event IDs with same pattern
        ids_to_exclude = exclude_event_ids or [event_id]
        past_event_ids = self.get_pattern_event_ids(pattern, exclude_ids=ids_to_exclude)
        if not past_event_ids:
            return []
        # Get all past attendee emails
        placeholders = ','.join(['?' for _ in past_event_ids])
        rows = self.conn.execute(f"""
            SELECT DISTINCT lower(o.email) as email,
                   COUNT(DISTINCT o.event_id) as past_editions,
                   SUM(o.gross_amount) as past_spent,
                   MAX(o.order_timestamp) as last_purchase
            FROM orders o
            WHERE o.event_id IN ({placeholders})
            GROUP BY lower(o.email)
            ORDER BY past_spent DESC
        """, past_event_ids).fetchall()
        # Filter out current buyers
        eligible = [r for r in rows if r['email'] not in current_buyers]
        if not eligible:
            return []
        # Batch fetch customer records (fixes N+1 query)
        emails_to_fetch = [r['email'] for r in eligible[:limit * 2]]
        cust_map = {}
        batch_size = 500
        for i in range(0, len(emails_to_fetch), batch_size):
            batch = emails_to_fetch[i:i + batch_size]
            ph = ','.join(['?' for _ in batch])
            cust_rows = self.conn.execute(
                f"SELECT * FROM customers WHERE email IN ({ph})", batch
            ).fetchall()
            for c in cust_rows:
                cust_map[c['email']] = dict(c)
        results = []
        for r in eligible:
            cust = cust_map.get(r['email'])
            if cust:
                cust['past_editions'] = r['past_editions']
                cust['past_event_spent'] = r['past_spent']
                cust['last_event_purchase'] = r['last_purchase']
                results.append(cust)
            if len(results) >= limit:
                break
        return results
    def get_city_prospects(self, city: str, exclude_emails: set = None,
                           limit: int = 1000) -> List[dict]:
        """Get customers in a city who might be interested (bought other events there)."""
        rows = self.conn.execute("""
            SELECT * FROM customers
            WHERE favorite_city = ? AND ltv_score >= 20
            ORDER BY ltv_score DESC
            LIMIT ?
        """, (city, limit)).fetchall()
        results = []
        for r in rows:
            if exclude_emails and r['email'] in exclude_emails:
                continue
            results.append(dict(r))
        return results
    def get_type_prospects(self, event_type: str, city: str = '',
                           exclude_emails: set = None,
                           limit: int = 1000) -> List[dict]:
        """Get customers who like this event type AND are in the same city."""
        if city:
            # Primary: same city + same type (strongest signal)
            rows = self.conn.execute("""
                SELECT * FROM customers
                WHERE (favorite_event_type = ? OR event_types LIKE ?)
                  AND favorite_city = ?
                  AND ltv_score >= 20
                ORDER BY ltv_score DESC
                LIMIT ?
            """, (event_type, f'%"{event_type}"%', city, limit)).fetchall()
        else:
            rows = self.conn.execute("""
                SELECT * FROM customers
                WHERE (favorite_event_type = ? OR event_types LIKE ?)
                  AND ltv_score >= 20
                ORDER BY ltv_score DESC
                LIMIT ?
            """, (event_type, f'%"{event_type}"%', limit)).fetchall()
        results = []
        for r in rows:
            if exclude_emails and r['email'] in exclude_emails:
                continue
            results.append(dict(r))
        return results
    # === Snapshots ===
    def save_snapshot(self, event_id: str, snapshot_date: str, days_before: int,
                      tickets: int, revenue: float, tickets_today: int = 0,
                      revenue_today: float = 0, orders_today: int = 0,
                      sell_through: float = 0, spend: float = 0):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO daily_snapshots
                (event_id, snapshot_date, days_before_event, tickets_cumulative,
                 revenue_cumulative, tickets_that_day, revenue_that_day,
                 orders_that_day, sell_through_pct, ad_spend_cumulative)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (event_id, snapshot_date, days_before, tickets, revenue,
                  tickets_today, revenue_today, orders_today, sell_through, spend))
    def get_snapshots(self, event_id: str) -> List[dict]:
        rows = self.conn.execute("""
            SELECT * FROM daily_snapshots
            WHERE event_id = ?
            ORDER BY days_before_event DESC
        """, (event_id,)).fetchall()
        return [dict(r) for r in rows]
    def get_snapshot_at_days(self, event_id: str, days_before: int) -> Optional[dict]:
        row = self.conn.execute("""
            SELECT * FROM daily_snapshots
            WHERE event_id = ? AND ABS(days_before_event - ?) <= 2
            ORDER BY ABS(days_before_event - ?) LIMIT 1
        """, (event_id, days_before, days_before)).fetchone()
        return dict(row) if row else None
    # === Pacing Curves ===
    def save_curve(self, pattern: str, event_type: str, source_events: List[str],
                   curve_data: dict, avg_final: float):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO pacing_curves
                (pattern, event_type, source_events, curve_data,
                 avg_final_sell_through, sample_count, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                pattern, event_type, json.dumps(source_events),
                json.dumps(curve_data), avg_final, len(source_events),
                datetime.now().isoformat()
            ))
    def get_curve(self, pattern: str) -> Optional[dict]:
        row = self.conn.execute("SELECT * FROM pacing_curves WHERE pattern = ?", (pattern,)).fetchone()
        if not row:
            return None
        return {
            'pattern': row['pattern'],
            'event_type': row['event_type'],
            'source_events': json.loads(row['source_events']),
            'curve_data': {int(k): v for k, v in json.loads(row['curve_data']).items()},
            'avg_final_sell_through': row['avg_final_sell_through'],
            'sample_count': row['sample_count']
        }
    def get_all_curves(self) -> List[dict]:
        rows = self.conn.execute("SELECT pattern FROM pacing_curves").fetchall()
        return [self.get_curve(r['pattern']) for r in rows]
    # === Ad Spend ===
    def save_ad_spend(self, event_id: str, campaign_id: str, campaign_name: str,
                      spend_date: str, spend: float, impressions: int = 0, clicks: int = 0):
        with self.transaction() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO ad_spend
                (event_id, campaign_id, campaign_name, spend_date, spend, impressions, clicks)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (event_id, campaign_id, campaign_name, spend_date, spend, impressions, clicks))
    def get_event_spend(self, event_id: str) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(spend), 0) as total FROM ad_spend WHERE event_id = ?",
            (event_id,)
        ).fetchone()
        return row['total'] if row else 0
    def get_event_spend_at_days_out(self, event_id: str, days_before: int) -> float:
        """Get cumulative ad spend at a specific days-out point from snapshots."""
        row = self.conn.execute("""
            SELECT ad_spend_cumulative FROM daily_snapshots
            WHERE event_id = ? AND ABS(days_before_event - ?) <= 2
            ORDER BY ABS(days_before_event - ?) LIMIT 1
        """, (event_id, days_before, days_before)).fetchone()
        return float(row['ad_spend_cumulative']) if row and row['ad_spend_cumulative'] else 0.0
    def get_event_daily_spend(self, event_id: str):
        """Get all daily spend records for an event."""
        rows = self.conn.execute("""
            SELECT spend_date, spend, campaign_name, impressions, clicks
            FROM ad_spend WHERE event_id = ?
            ORDER BY spend_date ASC
        """, (event_id,)).fetchall()
        return [dict(r) for r in rows]
    def get_meta_sync_status(self) -> dict:
        """Get summary of Meta ad spend data in the system."""
        row = self.conn.execute("""
            SELECT COUNT(DISTINCT event_id) as events,
                   COUNT(DISTINCT campaign_id) as campaigns,
                   COALESCE(SUM(spend), 0) as total_spend,
                   MIN(spend_date) as earliest,
                   MAX(spend_date) as latest
            FROM ad_spend
        """).fetchone()
        return dict(row) if row else {}
# =============================================================================
# EVENTBRITE SYNC
# =============================================================================
class EventbriteSync:
    """Complete Eventbrite API integration."""
    BASE_URL = "https://www.eventbriteapi.com/v3"
    def __init__(self, api_key: str, db: Database):
        self.api_key = api_key
        self.db = db
        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {api_key}'
        self._org_id = None
    def _get(self, endpoint: str, params: dict = None) -> dict:
        import time
        url = f"{self.BASE_URL}{endpoint}"
        response = self.session.get(url, params=params or {}, timeout=30)
        if response.status_code == 429:
            retry = int(response.headers.get('Retry-After', 60))
            log.warning(f"Rate limited, waiting {retry}s")
            time.sleep(retry)
            return self._get(endpoint, params)
        if response.status_code != 200:
            raise Exception(f"API error {response.status_code}: {response.text[:200]}")
        return response.json()
    def _paginate(self, endpoint: str, params: dict = None) -> List[dict]:
        params = params or {}
        results = []
        while True:
            response = self._get(endpoint, params)
            for key in ['events', 'orders', 'attendees', 'organizations']:
                if key in response:
                    results.extend(response[key])
                    break
            pagination = response.get('pagination', {})
            if pagination.get('has_more_items') and pagination.get('continuation'):
                params['continuation'] = pagination['continuation']
            else:
                break
        return results
    def get_org_id(self) -> str:
        if self._org_id:
            return self._org_id
        user = self._get('/users/me/')
        orgs = self._get(f"/users/{user['id']}/organizations/")
        if not orgs.get('organizations'):
            raise Exception("No organizations found")
        self._org_id = orgs['organizations'][0]['id']
        return self._org_id
    def sync_all(self, years_back: int = 2) -> dict:
        """Sync everything: events, orders, build snapshots and customers."""
        results = {'events': 0, 'orders': 0, 'customers': 0, 'curves': 0, 'errors': []}
        cutoff = datetime.now() - timedelta(days=years_back * 365)
        # Get all events
        log.info("Fetching events from Eventbrite...")
        events = self._paginate(
            f'/organizations/{self.get_org_id()}/events/',
            {'status': 'live,started,ended,completed', 'order_by': 'start_desc',
             'expand': 'venue,ticket_availability'}
        )
        log.info(f"Found {len(events)} events")
        for event_data in events:
            try:
                event = self._parse_event(event_data)
                if not event:
                    continue
                event_date = datetime.fromisoformat(event['event_date'])
                if event_date < cutoff:
                    continue
                # Determine status
                if event_date.date() > date.today():
                    event['status'] = 'upcoming'
                elif event_date.date() == date.today():
                    event['status'] = 'live'
                else:
                    event['status'] = 'completed'
                self.db.upsert_event(event)
                results['events'] += 1
                # Get orders
                log.info(f"  Syncing: {event['name']}")
                orders = self._paginate(f"/events/{event['event_id']}/orders/", {'expand': 'attendees'})
                for order_data in orders:
                    order = self._parse_order(order_data, event['event_id'], event_date)
                    if order:
                        self.db.insert_order(order)
                        results['orders'] += 1
                # Build snapshots for completed events
                if event['status'] == 'completed':
                    self._build_snapshots(event['event_id'], event_date.date(), event['capacity'])
            except Exception as e:
                results['errors'].append(str(e))
                log.error(f"  Error: {e}")
        # Build customer profiles
        log.info("Building customer profiles...")
        results['customers'] = self._build_all_customers()
        # Build pacing curves
        log.info("Building pacing curves...")
        results['curves'] = self._build_curves()
        return results
    # Patterns that indicate non-event items (vendor fees, payment links, etc.)
    JUNK_PATTERNS = [
        'vendor fee', 'vendor payment', 'payment link', 'vendor registration',
        'vendor', 'sponsor fee', 'sponsorship payment', 'booth fee',
        'exhibitor fee', 'exhibitor registration', 'vendor app',
        'test event', 'do not use', 'draft event'
    ]
    def _is_junk_event(self, name: str) -> bool:
        """Filter out vendor fees, payment links, and other non-consumer events."""
        name_lower = name.lower().strip()
        for pattern in self.JUNK_PATTERNS:
            if pattern in name_lower:
                return True
        return False
    def _parse_event(self, data: dict) -> Optional[dict]:
        event_id = data.get('id')
        if not event_id:
            return None
        name = data.get('name', {})
        if isinstance(name, dict):
            name = name.get('text', '')
        # Filter out junk events (vendor fees, payment links, etc.)
        if not name or self._is_junk_event(name):
            log.debug(f"Skipping junk event: {name}")
            return None
        start = data.get('start', {})
        event_date = start.get('local') or start.get('utc', '')
        if not event_date:
            return None
        # Parse date
        try:
            event_date = datetime.fromisoformat(event_date.replace('Z', '+00:00')).replace(tzinfo=None)
        except:
            return None
        venue = data.get('venue') or {}
        address = venue.get('address') or {}
        city = address.get('city', '')
        capacity = data.get('capacity') or 0
        if not capacity:
            ticket_avail = data.get('ticket_availability') or {}
            capacity = ticket_avail.get('total_capacity', 0)
        return {
            'event_id': event_id,
            'name': name,
            'event_type': self._infer_type(name),
            'city': city,
            'event_date': event_date.isoformat(),
            'capacity': capacity,
            'platform': 'eventbrite'
        }
    def _parse_order(self, data: dict, event_id: str, event_date: datetime) -> Optional[dict]:
        order_id = data.get('id')
        created = data.get('created', '')
        if not order_id or not created:
            return None
        email = data.get('email', '').lower().strip()
        if not email:
            attendees = data.get('attendees', [])
            if attendees:
                email = attendees[0].get('profile', {}).get('email', '').lower().strip()
        if not email:
            return None
        try:
            order_date = datetime.fromisoformat(created.replace('Z', '+00:00')).replace(tzinfo=None)
            days_before = max(0, (event_date - order_date).days)
        except:
            order_date = datetime.now()
            days_before = 0
        costs = data.get('costs') or {}
        gross = costs.get('gross') or {}
        gross_amount = float(gross.get('major_value') or 0)
        attendees = data.get('attendees', [])
        ticket_count = len(attendees) if attendees else 1
        ticket_type = None
        if attendees:
            ticket_type = attendees[0].get('ticket_class_name')
        return {
            'order_id': order_id,
            'event_id': event_id,
            'email': email,
            'order_timestamp': order_date.isoformat(),
            'ticket_count': ticket_count,
            'gross_amount': gross_amount,
            'ticket_type': ticket_type,
            'promo_code': data.get('promo_code'),
            'days_before_event': days_before
        }
    def _infer_type(self, name: str) -> str:
        name_lower = name.lower()
        for t in ['beer', 'wine', 'cocktail', 'whiskey', 'coffee', 'margarita', 'taco']:
            if t in name_lower:
                return t
        return 'other'
    def _build_snapshots(self, event_id: str, event_date: date, capacity: int):
        """Build daily snapshots from orders."""
        orders = self.db.get_orders_for_event(event_id)
        if not orders:
            return
        daily = defaultdict(list)
        for o in orders:
            try:
                d = datetime.fromisoformat(o['order_timestamp']).date()
                daily[d].append(o)
            except:
                pass
        if not daily:
            return
        cumulative_tickets = 0
        cumulative_revenue = 0
        first_date = min(daily.keys())
        current = first_date
        while current <= event_date:
            days_before = (event_date - current).days
            day_orders = daily.get(current, [])
            tickets_today = sum((o.get('ticket_count') or 1) for o in day_orders)
            revenue_today = sum((o.get('gross_amount') or 0) for o in day_orders)
            cumulative_tickets += tickets_today
            cumulative_revenue += revenue_today
            sell_through = (cumulative_tickets / capacity * 100) if capacity > 0 else 0
            self.db.save_snapshot(
                event_id, current.isoformat(), days_before,
                cumulative_tickets, cumulative_revenue,
                tickets_today, revenue_today, len(day_orders), sell_through
            )
            current += timedelta(days=1)
    def _build_all_customers(self) -> int:
        """Build customer profiles from all orders."""
        emails = self.db.get_all_emails()
        count = 0
        # Get global stats for RFM scoring
        all_customers_data = []
        for email in emails:
            orders = self.db.get_orders_for_customer(email)
            if orders:
                total_spent = sum(o.get('gross_amount', 0) for o in orders)
                last_date = max(o['order_timestamp'] for o in orders)
                try:
                    days_since = (datetime.now() - datetime.fromisoformat(last_date)).days
                except:
                    days_since = 999
                all_customers_data.append({
                    'email': email,
                    'orders': orders,
                    'total_spent': total_spent,
                    'days_since': days_since,
                    'order_count': len(orders)
                })
        # Calculate RFM quintiles
        if all_customers_data:
            recency_values = sorted([c['days_since'] for c in all_customers_data])
            frequency_values = sorted([c['order_count'] for c in all_customers_data])
            monetary_values = sorted([c['total_spent'] for c in all_customers_data])
            def get_quintile(value, sorted_list, reverse=False):
                n = len(sorted_list)
                if n == 0:
                    return 3
                idx = sorted_list.index(value) if value in sorted_list else 0
                pct = idx / n
                if reverse:
                    pct = 1 - pct
                if pct >= 0.8:
                    return 5
                elif pct >= 0.6:
                    return 4
                elif pct >= 0.4:
                    return 3
                elif pct >= 0.2:
                    return 2
                return 1
        for c_data in all_customers_data:
            customer = self._build_customer_profile(
                c_data['email'], c_data['orders'],
                get_quintile(c_data['days_since'], recency_values, reverse=True),
                get_quintile(c_data['order_count'], frequency_values),
                get_quintile(c_data['total_spent'], monetary_values)
            )
            if customer:
                self.db.upsert_customer(customer)
                count += 1
        return count
    def _build_customer_profile(self, email: str, orders: List[dict],
                                 rfm_r: int, rfm_f: int, rfm_m: int) -> Optional[Customer]:
        if not orders:
            return None
        total_orders = len(orders)
        total_tickets = sum(o.get('ticket_count', 1) for o in orders)
        total_spent = sum(o.get('gross_amount', 0) for o in orders)
        # Dates
        timestamps = [o['order_timestamp'] for o in orders]
        first_date = min(timestamps)
        last_date = max(timestamps)
        try:
            first_dt = datetime.fromisoformat(first_date)
            last_dt = datetime.fromisoformat(last_date)
            days_since = (datetime.now() - last_dt).days
            tenure = (datetime.now() - first_dt).days
        except:
            days_since = 0
            tenure = 0
        # Averages
        avg_order = total_spent / total_orders if total_orders > 0 else 0
        avg_tickets = total_tickets / total_orders if total_orders > 0 else 0
        # Days between orders
        if total_orders > 1:
            try:
                sorted_dates = sorted([datetime.fromisoformat(t) for t in timestamps])
                gaps = [(sorted_dates[i+1] - sorted_dates[i]).days for i in range(len(sorted_dates)-1)]
                avg_gap = sum(gaps) / len(gaps) if gaps else 0
            except:
                avg_gap = 0
        else:
            avg_gap = 0
        # Event preferences
        event_types = defaultdict(int)
        cities = defaultdict(int)
        events_attended = []
        days_before_list = []
        for o in orders:
            if o.get('event_type'):
                event_types[o['event_type']] += 1
            if o.get('city'):
                cities[o['city']] += 1
            if o.get('event_name') and o['event_name'] not in events_attended:
                events_attended.append(o['event_name'])
            if o.get('days_before_event') is not None:
                days_before_list.append(o['days_before_event'])
        favorite_type = max(event_types, key=event_types.get) if event_types else ''
        favorite_city = max(cities, key=cities.get) if cities else ''
        # Timing segment
        avg_days_before = sum(days_before_list) / len(days_before_list) if days_before_list else 0
        if avg_days_before >= 45:
            timing = 'super_early_bird'
        elif avg_days_before >= 28:
            timing = 'early_bird'
        elif avg_days_before >= 14:
            timing = 'planner'
        elif avg_days_before >= 7:
            timing = 'spontaneous'
        else:
            timing = 'last_minute'
        # RFM Segment
        if rfm_r >= 4 and rfm_f >= 4 and rfm_m >= 4:
            segment = 'champion'
        elif rfm_r >= 3 and rfm_f >= 3:
            segment = 'loyal'
        elif rfm_r >= 3 and rfm_f <= 2:
            segment = 'potential'
        elif rfm_r <= 2 and rfm_f >= 3 and rfm_m >= 3:
            segment = 'at_risk'
        elif rfm_r <= 2 and rfm_f <= 2:
            segment = 'hibernating'
        else:
            segment = 'other'
        # LTV Score (0-100)
        ltv_score = (
            (rfm_r * 15) +  # Max 75
            (rfm_f * 10) +  # Max 50
            (rfm_m * 15) +  # Max 75
            min(25, total_orders * 5)  # Bonus for order count
        ) / 2.25  # Normalize to ~100
        ltv_score = min(100, max(0, ltv_score))
        # Projected LTV (simple: avg order * expected future orders)
        expected_orders_per_year = 365 / avg_gap if avg_gap > 0 else 1
        ltv_projected = avg_order * expected_orders_per_year * 2  # 2-year projection
        return Customer(
            email=email,
            total_orders=total_orders,
            total_tickets=total_tickets,
            total_spent=total_spent,
            total_events_attended=len(events_attended),
            first_order_date=first_date,
            last_order_date=last_date,
            days_since_last_order=days_since,
            customer_tenure_days=tenure,
            avg_order_value=avg_order,
            avg_tickets_per_order=avg_tickets,
            avg_days_between_orders=avg_gap,
            avg_days_before_event=avg_days_before,
            favorite_event_type=favorite_type,
            favorite_city=favorite_city,
            event_types=dict(event_types),
            cities=dict(cities),
            events_attended=events_attended,
            timing_segment=timing,
            rfm_recency=rfm_r,
            rfm_frequency=rfm_f,
            rfm_monetary=rfm_m,
            rfm_segment=segment,
            ltv_score=ltv_score,
            ltv_projected=ltv_projected
        )
    def _build_curves(self) -> int:
        """Build pacing curves from completed events."""
        events = self.db.get_past_events()
        # Group by pattern
        patterns = defaultdict(list)
        for e in events:
            pattern = self._get_pattern(e['name'])
            patterns[pattern].append(e)
        curves_built = 0
        for pattern, pattern_events in patterns.items():
            if len(pattern_events) < 1:
                continue
            # Collect snapshots
            all_points = defaultdict(list)
            source_events = []
            final_sell_throughs = []
            for event in pattern_events:
                snapshots = self.db.get_snapshots(event['event_id'])
                if not snapshots:
                    continue
                source_events.append(event['name'])
                # Get final sell-through
                if event['capacity'] > 0:
                    final_tickets = self.db.get_event_tickets(event['event_id'])
                    final_sell_throughs.append(final_tickets / event['capacity'] * 100)
                for snap in snapshots:
                    days = snap['days_before_event']
                    st = snap['sell_through_pct']
                    all_points[days].append(st)
            if not all_points or not source_events:
                continue
            # Calculate curve
            curve_data = {}
            for days, values in all_points.items():
                values_sorted = sorted(values)
                n = len(values_sorted)
                curve_data[days] = {
                    'median': values_sorted[n // 2],
                    'p25': values_sorted[max(0, n // 4 - 1)] if n >= 4 else values_sorted[0],
                    'p75': values_sorted[min(n - 1, 3 * n // 4)] if n >= 4 else values_sorted[-1],
                    'samples': n
                }
            avg_final = statistics.mean(final_sell_throughs) if final_sell_throughs else 0
            self.db.save_curve(
                pattern, pattern_events[0].get('event_type', 'other'),
                source_events, curve_data, avg_final
            )
            curves_built += 1
        return curves_built
    def _get_pattern(self, name: str) -> str:
        """Extract pattern from event name."""
        return _normalize_event_pattern(name, include_season=True)
# =============================================================================
# META ADS SYNC
# =============================================================================
class MetaAdsSync:
    """Integrates with Meta Marketing API to pull ad spend data for events."""
    BASE_URL = "https://graph.facebook.com/v24.0"

    # Alias map: campaign abbreviation -> list of event name fragments it could match
    # This allows campaigns named "DBF" to match "District Beer Fest" events, etc.
    EVENT_ALIASES = {
        # Abbreviation -> event name patterns (lowercase)
        'dbf': ['district beer fest'],
        'dcock': ['dc cocktail', 'official dc cocktail'],
        'pcock': ['philly cocktail'],
        'dccf': ['dc coffee'],
        'dcf': ['dc coffee'],
        'acf': ['austin coffee'],
        'pcf': ['philly coffee'],
        'scf': ['seattle coffee', 'sf coffee', 'san francisco coffee'],
        'sfcf': ['san francisco coffee', 'sf coffee'],
        'sdcf': ['san diego coffee'],
        'dalcf': ['dallas coffee'],
        'dal': ['dallas coffee'],
        'dallas': ['dallas coffee'],
        'dcwf': ['dc wine fest'],
        'pwf': ['philly wine fest'],
        'dcmr': ['dc margarita rumble'],
        'nycmr': ['nyc margarita rumble'],
        'dcmm': ['dc margarita march'],
        'nycmm': ['nyc margarita march'],
        'pmm': ['philly margarita march'],
        'dcww': ['dc whiskey walk'],
        'pww': ['philly whiskey walk'],
        'nycww': ['nyc whiskey walk'],
        'pbf': ['philly beer fest'],
        'mermaid': ['mermaid city fest'],
        'bbb': ['bacon beer bourbon', 'bacon, beer, bourbon'],
        'dcwfs': ['dc wine fest spring'],
        'dcwff': ['dc wine fest fall'],
        'pwfs': ['philly wine fest spring'],
        'pwff': ['philly wine fest fall'],
    }

    def __init__(self, access_token: str, ad_account_id: str, db: Database):
        self.access_token = access_token
        self.ad_account_id = ad_account_id.replace('act_', '')
        self.db = db
        self.session = requests.Session()
        self.session.headers['Authorization'] = f'Bearer {access_token}'
    def _api_get(self, url: str, params: dict = None):
        """Make GET request with retry/backoff for rate limits."""
        import time
        params = params or {}
        params['access_token'] = self.access_token
        for attempt in range(3):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 429:
                    wait = int(resp.headers.get('Retry-After', 60 * (attempt + 1)))
                    log.warning(f"Meta API rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                log.error(f"Meta API error (attempt {attempt+1}): {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        return None

    def _generate_keywords(self, event_name: str):
        """Generate search keywords from event name for campaign matching.

        Includes: full name, bigrams, auto-abbreviations, and alias lookups
        so campaigns named 'DBF Winter' match 'District Beer Fest: Winter'.
        """
        cleaned = re.sub(r'\b20\d{2}\b', '', event_name)
        for word in ['spring edition', 'fall edition', 'summer edition', 'winter edition',
                      'edition', 'spring', 'fall', 'summer', 'winter']:
            cleaned = re.sub(r'\b' + word + r'\b', '', cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r'[^\w\s]', '', cleaned)
        cleaned = ' '.join(cleaned.split()).strip().lower()
        if not cleaned:
            return []
        keywords = [cleaned]
        words = cleaned.split()
        if len(words) >= 2:
            for i in range(len(words) - 1):
                keywords.append(f"{words[i]} {words[i+1]}")

        # Auto-generate abbreviations from first letters of each word
        if len(words) >= 2:
            # Full initials: "district beer fest" -> "dbf"
            initials = ''.join(w[0] for w in words if w)
            if len(initials) >= 2:
                keywords.append(initials)
            # Also try without common filler words for shorter abbrevs
            skip_words = {'the', 'of', 'and', 'in', 'at', 'for', 'a', 'an'}
            content_words = [w for w in words if w not in skip_words]
            if len(content_words) >= 2:
                content_initials = ''.join(w[0] for w in content_words if w)
                if content_initials != initials and len(content_initials) >= 2:
                    keywords.append(content_initials)

        # Check alias map: find aliases whose event patterns match this event name
        event_lower = event_name.lower()
        for alias, patterns in self.EVENT_ALIASES.items():
            for pattern in patterns:
                if pattern in event_lower or event_lower in pattern:
                    keywords.append(alias)
                    break

        # Reverse alias lookup: check if any alias IS in the event name
        # (handles cases where we're searching from the campaign side)
        for alias, patterns in self.EVENT_ALIASES.items():
            if alias in event_lower.replace(' ', ''):
                for pattern in patterns:
                    if pattern not in keywords:
                        keywords.append(pattern)

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for kw in keywords:
            if kw not in seen:
                seen.add(kw)
                unique.append(kw)
        return unique

    def _find_campaigns(self, event_name: str):
        """Find Meta campaigns matching an event name.

        Uses two matching strategies:
        1. Forward match: event keywords found in campaign name
        2. Reverse alias match: campaign name words that are known aliases for this event
        """
        keywords = self._generate_keywords(event_name)
        if not keywords:
            return []
        url = f"{self.BASE_URL}/act_{self.ad_account_id}/campaigns"
        params = {'fields': 'id,name,status,objective', 'limit': 200}
        matched = []
        seen_ids = set()
        event_lower = event_name.lower()
        while url:
            data = self._api_get(url, params)
            if not data:
                break
            for campaign in data.get('data', []):
                if campaign['id'] in seen_ids:
                    continue
                cname = campaign['name'].lower()
                match_reason = None
                # Forward match: check if any event keyword appears in campaign name
                for kw in keywords:
                    if kw in cname:
                        match_reason = f"keyword '{kw}'"
                        break
                # Reverse alias match: check if any word in campaign name is a known alias
                if not match_reason:
                    cname_clean = re.sub(r'[^\w\s]', '', cname)
                    for word in cname_clean.split():
                        if word in self.EVENT_ALIASES:
                            for pattern in self.EVENT_ALIASES[word]:
                                if pattern in event_lower or event_lower in pattern:
                                    match_reason = f"reverse alias '{word}'->'{pattern}'"
                                    break
                            if match_reason:
                                break
                if match_reason:
                    matched.append({'id': campaign['id'], 'name': campaign['name'],
                                    'status': campaign.get('status')})
                    seen_ids.add(campaign['id'])
                    log.info(f"  Matched campaign '{campaign['name']}' via {match_reason}")
            paging = data.get('paging', {})
            next_url = paging.get('next')
            if next_url:
                url = next_url
                params = {}
            else:
                break
        log.info(f"Found {len(matched)} Meta campaigns for '{event_name}' (keywords: {keywords[:5]})")
        return matched

    def _fetch_daily_insights(self, campaign_id: str, date_start: str, date_stop: str):
        """Fetch daily spend insights for a campaign."""
        url = f"{self.BASE_URL}/{campaign_id}/insights"
        params = {
            'fields': 'spend,impressions,clicks,date_start,date_stop',
            'time_increment': '1',
            'date_start': date_start,
            'date_stop': date_stop,
            'limit': 500
        }
        insights = []
        while url:
            data = self._api_get(url, params)
            if not data:
                break
            insights.extend(data.get('data', []))
            paging = data.get('paging', {})
            next_url = paging.get('next')
            if next_url:
                url = next_url
                params = {}
            else:
                break
        return insights
    def sync_event_spend(self, event_id: str, event_name: str, event_date_str: str):
        """Sync ad spend from Meta for a single event."""
        try:
            event_date = datetime.fromisoformat(event_date_str).date()
            today = date.today()
            date_start = (event_date - timedelta(days=300)).isoformat()
            date_stop = min(event_date, today).isoformat()
            campaigns = self._find_campaigns(event_name)
            if not campaigns:
                return {'event_id': event_id, 'total_spend': 0, 'campaigns_found': 0, 'days_of_data': 0}
            total_spend = 0.0
            total_days = 0
            for campaign in campaigns:
                insights = self._fetch_daily_insights(campaign['id'], date_start, date_stop)
                for day_data in insights:
                    spend = float(day_data.get('spend', 0))
                    impressions = int(day_data.get('impressions', 0))
                    clicks = int(day_data.get('clicks', 0))
                    spend_date = day_data.get('date_start', '')
                    self.db.save_ad_spend(
                        event_id=event_id, campaign_id=campaign['id'],
                        campaign_name=campaign['name'], spend_date=spend_date,
                        spend=spend, impressions=impressions, clicks=clicks
                    )
                    total_spend += spend
                total_days += len(insights)
            log.info(f"Meta sync for {event_name}: ${total_spend:.2f} across {len(campaigns)} campaigns")
            return {'event_id': event_id, 'total_spend': round(total_spend, 2),
                    'campaigns_found': len(campaigns), 'days_of_data': total_days}
        except Exception as e:
            log.error(f"Meta sync error for {event_id}: {e}")
            return {'event_id': event_id, 'total_spend': 0, 'campaigns_found': 0,
                    'days_of_data': 0, 'error': str(e)}
    def sync_all_events(self, events_list):
        """Sync Meta ad spend for all events."""
        results = {'total_events': len(events_list), 'successful': 0, 'total_spend': 0.0, 'event_results': []}
        for event in events_list:
            result = self.sync_event_spend(event['event_id'], event['name'], event['event_date'])
            results['event_results'].append(result)
            results['total_spend'] += result.get('total_spend', 0)
            if not result.get('error'):
                results['successful'] += 1
        log.info(f"Meta sync complete: {results['successful']}/{results['total_events']} events, ${results['total_spend']:.2f}")
        return results
# =============================================================================
# DECISION ENGINE
# =============================================================================
class DecisionEngine:
    """Makes decisions based on historical pacing."""
    def __init__(self, db: Database):
        self.db = db
        self._curves = {}
        self._load_curves()
    def _load_curves(self):
        curves = self.db.get_all_curves()
        for c in curves:
            if c:
                self._curves[c['pattern']] = c
    def _get_pattern(self, name: str) -> str:
        """Extract pattern from event name."""
        return _normalize_event_pattern(name, include_season=True)
    def analyze_event(self, event_id: str) -> Optional[EventPacing]:
        """Full analysis for an event."""
        event = self.db.get_event(event_id)
        if not event:
            return None
        # Basic info
        event_date = datetime.fromisoformat(event['event_date']).date()
        days_until = (event_date - date.today()).days
        tickets = self.db.get_event_tickets(event_id)
        revenue = self.db.get_event_revenue(event_id)
        spend = self.db.get_event_spend(event_id)
        capacity = event.get('capacity', 500)
        sell_through = (tickets / capacity * 100) if capacity > 0 else 0
        cac = spend / tickets if tickets > 0 else 0
        # Historical comparison
        pattern = self._get_pattern(event['name'])
        curve = self._curves.get(pattern)
        hist_median = 0
        hist_range = (0, 0)
        pace = 0
        comparison_events = []
        comparison_years = []
        projected_final = tickets
        projected_range = (tickets, capacity)
        confidence = 0.5
        if curve and curve['curve_data']:
            # Find closest day in curve for pace calculation
            available_days = sorted(curve['curve_data'].keys(), reverse=True)
            closest_day = None
            for d in available_days:
                if d >= days_until:
                    closest_day = d
                elif closest_day is None:
                    closest_day = d
                    break
            if closest_day is not None and closest_day in curve['curve_data']:
                point = curve['curve_data'][closest_day]
                hist_median = point['median']
                hist_range = (point.get('p25', hist_median), point.get('p75', hist_median))
                if hist_median > 0:
                    pace = ((sell_through - hist_median) / hist_median) * 100
                comparison_events = curve['source_events']
                comparison_years = [int(re.search(r'20\d{2}', e).group())
                                   for e in curve['source_events']
                                   if re.search(r'20\d{2}', e)]
        # Decision
        decision, urgency, rationale, actions = self._decide(
            tickets, capacity, sell_through, pace, cac, days_until, hist_median, comparison_events
        )
        # Get targeting counts
        high_value = len(self.db.get_high_value_customers(
            event_type=event.get('event_type'), city=event.get('city'), min_ltv=50, limit=1000
        ))
        at_risk = len(self.db.get_at_risk_customers(min_orders=2, min_days_inactive=180))
        # Historical year-by-year comparison
        historical_comparisons = []
        all_events = self.db.get_events()
        for pe in all_events:
            if pe['event_id'] == event_id:
                continue
            if self._get_pattern(pe['name']) != pattern:
                continue
            pe_date = datetime.fromisoformat(pe['event_date']).date()
            pe_tickets = self.db.get_event_tickets(pe['event_id'])
            pe_revenue = self.db.get_event_revenue(pe['event_id'])
            pe_capacity = pe.get('capacity', 0)
            pe_sell_through = (pe_tickets / pe_capacity * 100) if pe_capacity > 0 else 0
            pe_spend_total = self.db.get_event_spend(pe['event_id'])
            comp = {
                'event_name': pe['name'],
                'event_date': pe['event_date'],
                'year': pe_date.year,
                'final_tickets': pe_tickets,
                'final_revenue': pe_revenue,
                'capacity': pe_capacity,
                'final_sell_through': round(pe_sell_through, 1),
                'ad_spend_total': round(pe_spend_total, 2),
            }
            snapshot = self.db.get_snapshot_at_days(pe['event_id'], days_until)
            if snapshot:
                spend_at_point = snapshot.get('ad_spend_cumulative', 0) or 0
                comp['at_days_out'] = {
                    'days': snapshot['days_before_event'],
                    'tickets': snapshot['tickets_cumulative'],
                    'revenue': snapshot['revenue_cumulative'],
                    'sell_through': snapshot['sell_through_pct'],
                    'ad_spend': round(float(spend_at_point), 2),
                }
            else:
                comp['at_days_out'] = None
            historical_comparisons.append(comp)
        historical_comparisons.sort(key=lambda x: x['year'])
        # YOY Projection: this_year_tickets / last_year_tickets_at_point * last_year_final
        comps_with_data = [c for c in historical_comparisons
                           if c.get('at_days_out') and c['at_days_out']['tickets'] > 0
                           and c['final_tickets'] > 0]
        if comps_with_data and tickets > 0:
            # Use most recent year for primary projection
            last_year = comps_with_data[-1]
            last_at_point = last_year['at_days_out']['tickets']
            last_final = last_year['final_tickets']
            projected_final = int(tickets / last_at_point * last_final)
            projected_final = max(tickets, projected_final)
            # Range from all available years
            all_projections = []
            for c in comps_with_data:
                at_point = c['at_days_out']['tickets']
                proj = int(tickets / at_point * c['final_tickets'])
                all_projections.append(max(tickets, proj))
            projected_range = (min(all_projections), max(all_projections))
            confidence = 0.9 if len(comps_with_data) >= 3 else 0.75 if len(comps_with_data) >= 2 else 0.6
        return EventPacing(
            event_id=event_id,
            event_name=event['name'],
            event_date=event['event_date'],
            days_until=days_until,
            tickets_sold=tickets,
            capacity=capacity,
            revenue=revenue,
            ad_spend=spend,
            sell_through=sell_through,
            cac=cac,
            historical_median_at_point=hist_median,
            historical_range=hist_range,
            pace_vs_historical=pace,
            comparison_events=comparison_events,
            comparison_years=comparison_years,
            projected_final=projected_final,
            projected_range=projected_range,
            confidence=confidence,
            decision=decision,
            urgency=urgency,
            rationale=rationale,
            actions=actions,
            high_value_targets=high_value,
            reactivation_targets=at_risk,
            historical_comparisons=historical_comparisons
        )
    def _decide(self, tickets: int, capacity: int, sell_through: float,
                pace: float, cac: float, days_until: int, hist_median: float,
                comparison_events: List[str]) -> Tuple[Decision, int, str, List[str]]:
        """Make decision based on historical pacing."""
        target_cac = 12.00
        cac_ok = cac <= target_cac * 1.5 or cac == 0
        context = f" vs historical median {hist_median:.1f}%" if hist_median > 0 else ""
        basis = f"Based on {len(comparison_events)} past events" if comparison_events else "No historical data"
        # PIVOT: Way behind
        if pace < -35:
            urgency = 9 if days_until < 30 else 7
            return (
                Decision.PIVOT, urgency,
                f"Sales {abs(pace):.0f}% behind historical pace{context}. {basis}.",
                [
                    "PAUSE underperforming ad campaigns",
                    "Audit and refresh all creative",
                    "Test flash sale / promo offer",
                    "Try completely different audience",
                    "Consider influencer partnership"
                ]
            )
        # PUSH: Behind but CAC ok
        if pace < -15 and cac_ok and days_until > 7:
            urgency = 7 if days_until < 30 else 5
            bump = min(50, abs(pace))
            return (
                Decision.PUSH, urgency,
                f"Sales {abs(pace):.0f}% behind but CAC ${cac:.2f} acceptable{context}. {basis}.",
                [
                    f"Increase ad budget by {bump:.0f}%",
                    "Expand lookalike audiences",
                    "Add urgency messaging",
                    "Email high-value past attendees",
                    "Increase retargeting frequency"
                ]
            )
        # COAST: Way ahead
        if pace > 25 and sell_through > 30:
            cut = min(40, pace / 2)
            return (
                Decision.COAST, 3,
                f"Sales {pace:.0f}% ahead of historical{context}. {basis}.",
                [
                    f"Reduce ad budget by {cut:.0f}%",
                    "Reallocate budget to struggling events",
                    "Focus on VIP upsells",
                    "Maintain organic only"
                ]
            )
        # MAINTAIN
        return (
            Decision.MAINTAIN, 5 if days_until < 30 else 3,
            f"Tracking within historical norms{context}. {basis}.",
            [
                "Maintain current spend",
                "Continue daily monitoring",
                "Prepare final push for last 2 weeks"
            ]
        )
    def _detect_timed_entry_groups(self, analyses):
        """Detect timed-entry events (same name, within 3 days of each other)."""
        by_pattern = defaultdict(list)
        for a in analyses:
            by_pattern[self._get_pattern(a.event_name)].append(a)
        result = {}
        for pattern, group in by_pattern.items():
            if len(group) < 2:
                continue
            dates = [datetime.fromisoformat(a.event_date).date() for a in group]
            if 0 < (max(dates) - min(dates)).days <= 3:
                result[pattern] = group
        return result
    def _create_day_event(self, pattern, day_analyses, all_analyses_for_pattern):
        """Combine multiple time-slot EventPacing objects for same day into one."""
        import hashlib
        first_date = datetime.fromisoformat(day_analyses[0].event_date).date()
        day_name = first_date.strftime("%A")
        names = [a.event_name for a in all_analyses_for_pattern]
        base_name = names[0]
        if len(names) > 1:
            prefix = names[0]
            for n in names[1:]:
                while not n.startswith(prefix):
                    prefix = prefix[:-1]
            prefix = prefix.rstrip(" -:/")
            if len(prefix) > 10:
                base_name = prefix
        logical_name = f"{base_name} - {day_name}"
        eid = hashlib.md5(f"{logical_name}_{first_date}".encode()).hexdigest()
        total_tickets = sum(a.tickets_sold for a in day_analyses)
        total_revenue = sum(a.revenue for a in day_analyses)
        max_capacity = max(a.capacity for a in day_analyses) if day_analyses else 0
        total_spend = sum(a.ad_spend for a in day_analyses)
        sell_through = (total_tickets / max_capacity * 100) if max_capacity > 0 else 0
        cac_val = (total_spend / total_tickets) if total_tickets > 0 else 0
        days_until = day_analyses[0].days_until
        best_urgency = max(a.urgency for a in day_analyses)
        best_decision = None
        best_rationale = ""
        best_actions = []
        for a in day_analyses:
            if a.urgency == best_urgency:
                best_decision = a.decision
                best_rationale = a.rationale
                best_actions = a.actions
                break
        # Figure out which "day ordinal" this grouped day is within the multi-day event
        # e.g., for a Sat/Sun event, Saturday=Day 1, Sunday=Day 2
        all_current_dates = sorted(set(
            datetime.fromisoformat(a.event_date).date() for a in all_analyses_for_pattern
        ))
        current_day_ordinal = all_current_dates.index(first_date) if first_date in all_current_dates else 0
        total_current_days = len(all_current_dates)

        historical_comparisons = []
        all_events = self.db.get_events()
        # Group ALL past events by (year) first, then by sorted date within that year
        past_by_year = defaultdict(list)
        for pe in all_events:
            if self._get_pattern(pe['name']) != pattern:
                continue
            pe_date = datetime.fromisoformat(pe['event_date']).date()
            is_current = any(pe['event_id'] == a.event_id for a in all_analyses_for_pattern)
            if is_current:
                continue
            past_by_year[pe_date.year].append(pe)
        # For each past year, sort dates and match by day ordinal position
        past_by_date = {}
        for year, year_events in past_by_year.items():
            year_dates = sorted(set(datetime.fromisoformat(e['event_date']).date() for e in year_events))
            # Match current day ordinal to past year's day ordinal
            if current_day_ordinal < len(year_dates):
                target_date = year_dates[current_day_ordinal]
                matching_events = [e for e in year_events
                                   if datetime.fromisoformat(e['event_date']).date() == target_date]
                if matching_events:
                    past_by_date[(year, target_date)] = matching_events
            elif len(year_dates) == 1 and total_current_days > 1 and current_day_ordinal == 0:
                # Past edition was single-day, current is multi-day: only compare with Day 1
                target_date = year_dates[0]
                past_by_date[(year, target_date)] = [e for e in year_events
                    if datetime.fromisoformat(e['event_date']).date() == target_date]

        for (year, pdate), events_on_date in past_by_date.items():
            pd_weekday = pdate.strftime("%A")
            date_tickets = 0
            date_revenue = 0
            date_capacity = 0
            date_spend = 0
            for pe in events_on_date:
                t = self.db.get_event_tickets(pe['event_id'])
                r = self.db.get_event_revenue(pe['event_id'])
                c = pe.get('capacity', 0)
                sp = self.db.get_event_spend(pe['event_id'])
                date_tickets += t
                date_revenue += r
                date_capacity = max(date_capacity, c)
                date_spend += sp
            date_sell = (date_tickets / date_capacity * 100) if date_capacity > 0 else 0
            snap_tickets = 0
            snap_revenue = 0
            snap_spend = 0
            snap_found = False
            for pe in events_on_date:
                s = self.db.get_snapshot_at_days(pe['event_id'], days_until)
                if s:
                    snap_tickets += s['tickets_cumulative']
                    snap_revenue += s['revenue_cumulative']
                    snap_spend += s.get('ad_spend_cumulative', 0) or 0
                    snap_found = True
            snap_sell = round(snap_tickets / date_capacity * 100, 1) if snap_found and date_capacity > 0 else 0
            comp = {
                'event_name': events_on_date[0]['name'],
                'event_date': str(pdate),
                'year': year,
                'day_of_week': pd_weekday,
                'final_tickets': date_tickets,
                'final_revenue': date_revenue,
                'capacity': date_capacity,
                'final_sell_through': round(date_sell, 1),
                'ad_spend_total': round(date_spend, 2),
            }
            if snap_found:
                comp['at_days_out'] = {'days': days_until, 'tickets': snap_tickets, 'revenue': snap_revenue, 'sell_through': snap_sell, 'ad_spend': round(float(snap_spend), 2)}
            else:
                comp['at_days_out'] = None
            historical_comparisons.append(comp)
        historical_comparisons.sort(key=lambda x: x['year'])
        # YOY Projection for grouped events
        grouped_proj_final = total_tickets
        grouped_proj_range = (total_tickets, max_capacity)
        grouped_confidence = 0.5
        comps_with_data = [c for c in historical_comparisons
                           if c.get('at_days_out') and c['at_days_out']['tickets'] > 0
                           and c['final_tickets'] > 0]
        if comps_with_data and total_tickets > 0:
            last_year = comps_with_data[-1]
            last_at_point = last_year['at_days_out']['tickets']
            last_final = last_year['final_tickets']
            grouped_proj_final = int(total_tickets / last_at_point * last_final)
            grouped_proj_final = max(total_tickets, grouped_proj_final)
            all_projections = []
            for c in comps_with_data:
                at_point = c['at_days_out']['tickets']
                proj = int(total_tickets / at_point * c['final_tickets'])
                all_projections.append(max(total_tickets, proj))
            grouped_proj_range = (min(all_projections), max(all_projections))
            grouped_confidence = 0.9 if len(comps_with_data) >= 3 else 0.75 if len(comps_with_data) >= 2 else 0.6
        # Calculate grouped historical median from comparisons at this days-out point
        grouped_hist_median = 0
        grouped_hist_lo = 0
        grouped_hist_hi = 0
        comps_with_snap = [c for c in historical_comparisons if c.get('at_days_out')]
        if comps_with_snap:
            snap_sells = sorted([c['at_days_out']['sell_through'] for c in comps_with_snap])
            n = len(snap_sells)
            grouped_hist_median = snap_sells[n // 2]
            grouped_hist_lo = snap_sells[0]
            grouped_hist_hi = snap_sells[-1]
        # Calculate actual pace for grouped event (not hardcoded 0!)
        grouped_pace = 0
        if grouped_hist_median > 0:
            grouped_pace = ((sell_through - grouped_hist_median) / grouped_hist_median) * 100
        # Re-decide based on grouped data (don't inherit from individual slots)
        grouped_decision, grouped_urgency, grouped_rationale, grouped_actions = self._decide(
            total_tickets, max_capacity, sell_through, grouped_pace,
            cac_val, days_until, grouped_hist_median,
            [c['event_name'] for c in historical_comparisons]
        )
        return EventPacing(
            event_id=eid, event_name=logical_name,
            event_date=day_analyses[0].event_date, days_until=days_until,
            tickets_sold=total_tickets, capacity=max_capacity,
            revenue=total_revenue, ad_spend=total_spend,
            sell_through=round(sell_through, 1), cac=round(cac_val, 2),
            historical_median_at_point=grouped_hist_median,
            historical_range=(grouped_hist_lo, grouped_hist_hi),
            pace_vs_historical=round(grouped_pace, 1),
            comparison_events=[c['event_name'] for c in historical_comparisons],
            comparison_years=[c['year'] for c in historical_comparisons],
            projected_final=grouped_proj_final,
            projected_range=grouped_proj_range,
            confidence=grouped_confidence,
            decision=grouped_decision, urgency=grouped_urgency,
            rationale=grouped_rationale, actions=grouped_actions,
            high_value_targets=max(a.high_value_targets for a in day_analyses) if day_analyses else 0,
            reactivation_targets=max(a.reactivation_targets for a in day_analyses) if day_analyses else 0,
            historical_comparisons=historical_comparisons,
            constituent_event_ids=[a.event_id for a in all_analyses_for_pattern],
        )
    def analyze_portfolio(self) -> List[EventPacing]:
        """Analyze all upcoming events, grouping timed-entry events by day."""
        events = self.db.get_events(upcoming_only=True)
        analyses = []
        for event in events:
            analysis = self.analyze_event(event['event_id'])
            if analysis:
                analyses.append(analysis)
        timed_groups = self._detect_timed_entry_groups(analyses)
        if timed_groups:
            grouped_ids = set()
            for pattern, group in timed_groups.items():
                for a in group:
                    grouped_ids.add(a.event_id)
            ungrouped = [a for a in analyses if a.event_id not in grouped_ids]
            for pattern, group in timed_groups.items():
                by_date = defaultdict(list)
                for a in group:
                    d = datetime.fromisoformat(a.event_date).date()
                    by_date[d].append(a)
                for date, day_group in by_date.items():
                    day_event = self._create_day_event(pattern, day_group, group)
                    ungrouped.append(day_event)
            analyses = ungrouped
        analyses.sort(key=lambda x: (-x.urgency, x.days_until))
        return analyses
# =============================================================================
# FLASK API
# =============================================================================
def create_app(db: Database, auto_sync: bool = False) -> Flask:
    """Create Flask app with all endpoints."""
    import threading
    app = Flask(__name__)
    CORS(app)
    engine = DecisionEngine(db)
    # Sync status tracking
    _sync_state = {'done': False, 'running': auto_sync, 'result': None, 'error': None}
    _portfolio_cache = {'analyses': None, 'ts': 0}  # Cache portfolio analysis for 60s
    def _get_portfolio():
        """Get cached portfolio analysis (avoids re-analyzing on every targeting request)."""
        import time
        now = time.time()
        if _portfolio_cache['analyses'] is None or (now - _portfolio_cache['ts']) > 60:
            _portfolio_cache['analyses'] = engine.analyze_portfolio()
            _portfolio_cache['ts'] = now
        return _portfolio_cache['analyses']
    def _do_background_sync():
        """Run Eventbrite sync in background thread."""
        _sync_state['running'] = True
        try:
            api_key = os.environ.get('EVENTBRITE_API_KEY')
            if not api_key:
                _sync_state['error'] = 'EVENTBRITE_API_KEY not set'
                log.error("EVENTBRITE_API_KEY not set - cannot sync")
                return
            log.info("Starting Eventbrite sync...")
            eb = EventbriteSync(api_key, db)
            result = eb.sync_all(years_back=4)
            _sync_state['result'] = result
            # Reload decision engine curves after sync
            engine._load_curves()
            log.info(f"Sync complete: {result.get('events', 0)} events, "
                     f"{result.get('orders', 0)} orders, "
                     f"{result.get('customers', 0)} customers, "
                     f"{result.get('curves', 0)} curves")
            # Also sync Meta ad spend if credentials are configured
            meta_token = os.environ.get('META_ACCESS_TOKEN')
            meta_accounts_str = os.environ.get('META_AD_ACCOUNT_ID', '')
            meta_accounts = [a.strip() for a in meta_accounts_str.split(',') if a.strip()]
            if meta_token and meta_accounts:
                try:
                    log.info(f"Starting Meta ad spend sync for {len(meta_accounts)} account(s)...")
                    all_events = db.get_events(upcoming_only=False)
                    total_meta_spend = 0
                    for acct_id in meta_accounts:
                        log.info(f"  Syncing Meta account: {acct_id}")
                        meta = MetaAdsSync(meta_token, acct_id, db)
                        meta_result = meta.sync_all_events(all_events)
                        total_meta_spend += meta_result.get('total_spend', 0)
                        log.info(f"  Account {acct_id}: {meta_result.get('successful', 0)} events, "
                                 f"${meta_result.get('total_spend', 0):.2f} spend")
                    log.info(f"Meta sync complete: ${total_meta_spend:.2f} total across {len(meta_accounts)} account(s)")
                except Exception as me:
                    log.error(f"Meta sync error: {me}")
        except Exception as e:
            _sync_state['error'] = str(e)
            log.error(f"Sync error: {e}")
        finally:
            _sync_state['done'] = True
            _sync_state['running'] = False
    # Auto-sync on app creation (for gunicorn deployment)
    if auto_sync:
        threading.Thread(target=_do_background_sync, daemon=True).start()
    def _serialize_pacing(obj):
        """Convert EventPacing dataclass to JSON-safe dict."""
        d = asdict(obj)
        # Convert Decision enum to its string value
        if 'decision' in d:
            d['decision'] = obj.decision.value
        return d
    @app.route('/')
    def home():
        return jsonify({
            'name': 'Craft Dominant API',
            'endpoints': ['/api/dashboard', '/api/events', '/api/customers', '/api/curves',
                          '/api/sync-status'],
            'status': 'running',
            'sync_done': _sync_state['done'],
            'sync_running': _sync_state['running']
        })
    @app.route('/api/health')
    def health():
        return jsonify({
            'status': 'ok',
            'time': datetime.now().isoformat(),
            'sync_done': _sync_state['done'],
            'sync_running': _sync_state['running']
        })
    @app.route('/api/sync-status')
    def sync_status():
        """Check sync progress."""
        return jsonify({
            'done': _sync_state['done'],
            'running': _sync_state['running'],
            'result': _sync_state['result'],
            'error': _sync_state['error']
        })
    @app.route('/api/sync')
    def sync_endpoint():
        """Trigger Eventbrite sync (runs in background)."""
        if _sync_state['running']:
            return jsonify({'status': 'already_running', 'message': 'Sync is already in progress'})
        api_key = os.environ.get('EVENTBRITE_API_KEY')
        if not api_key:
            return jsonify({'error': 'EVENTBRITE_API_KEY not set'}), 500
        # Reset state and run in background
        _sync_state['done'] = False
        _sync_state['running'] = False
        _sync_state['result'] = None
        _sync_state['error'] = None
        threading.Thread(target=_do_background_sync, daemon=True).start()
        return jsonify({'status': 'started', 'message': 'Sync started in background. Poll /api/sync-status for progress.'})
    @app.route('/api/meta-sync')
    def meta_sync_endpoint():
        """Trigger Meta ad spend sync for all events."""
        meta_token = os.environ.get('META_ACCESS_TOKEN')
        meta_accounts_str = os.environ.get('META_AD_ACCOUNT_ID', '')
        meta_accounts = [a.strip() for a in meta_accounts_str.split(',') if a.strip()]
        if not meta_token or not meta_accounts:
            return jsonify({
                'error': 'META_ACCESS_TOKEN and META_AD_ACCOUNT_ID not configured',
                'setup': 'Set these environment variables in Railway to enable Meta ad spend tracking'
            }), 400
        def _do_meta_sync():
            try:
                all_events = db.get_events(upcoming_only=False)
                for acct_id in meta_accounts:
                    log.info(f"Manual Meta sync for account: {acct_id}")
                    meta = MetaAdsSync(meta_token, acct_id, db)
                    result = meta.sync_all_events(all_events)
                    log.info(f"Manual Meta sync complete for {acct_id}: {result}")
            except Exception as e:
                log.error(f"Manual Meta sync error: {e}")
        threading.Thread(target=_do_meta_sync, daemon=True).start()
        return jsonify({'status': 'started', 'message': f'Meta ad spend sync started for {len(meta_accounts)} account(s)'})
    @app.route('/api/meta-status')
    def meta_status():
        """Check Meta ad spend data status."""
        status = db.get_meta_sync_status()
        meta_configured = bool(os.environ.get('META_ACCESS_TOKEN') and os.environ.get('META_AD_ACCOUNT_ID'))
        return jsonify({
            'configured': meta_configured,
            'data': status
        })
    @app.route('/api/meta-debug')
    def meta_debug():
        """Diagnostic: test Meta API connection and campaign matching for one event."""
        meta_token = os.environ.get('META_ACCESS_TOKEN')
        meta_accounts_str = os.environ.get('META_AD_ACCOUNT_ID', '')
        meta_accounts = [a.strip() for a in meta_accounts_str.split(',') if a.strip()]
        if not meta_token or not meta_accounts:
            return jsonify({'error': 'META_ACCESS_TOKEN or META_AD_ACCOUNT_ID not set',
                            'token_set': bool(meta_token),
                            'token_preview': f'{meta_token[:10]}...' if meta_token else None,
                            'accounts': meta_accounts})
        # Test the API with first account
        acct = meta_accounts[0]
        results = {'token_preview': f'{meta_token[:10]}...{meta_token[-5:]}',
                    'accounts': meta_accounts, 'tests': []}
        try:
            meta = MetaAdsSync(meta_token, acct, db)
            # Test 1: Can we reach the API at all?
            test_url = f"{meta.BASE_URL}/act_{meta.ad_account_id}"
            params = {'fields': 'name,account_status', 'access_token': meta_token}
            resp = meta.session.get(test_url, params=params, timeout=15)
            results['tests'].append({
                'test': 'API connection',
                'status_code': resp.status_code,
                'response': resp.json() if resp.status_code == 200 else resp.text[:500],
            })
            # Test 2: Can we list campaigns?
            camp_url = f"{meta.BASE_URL}/act_{meta.ad_account_id}/campaigns"
            camp_params = {'fields': 'id,name,status', 'limit': 5, 'access_token': meta_token}
            camp_resp = meta.session.get(camp_url, params=camp_params, timeout=15)
            camp_data = camp_resp.json() if camp_resp.status_code == 200 else {'error': camp_resp.text[:500]}
            results['tests'].append({
                'test': 'List campaigns',
                'status_code': camp_resp.status_code,
                'campaign_count': len(camp_data.get('data', [])) if isinstance(camp_data, dict) else 0,
                'sample_campaigns': [{'name': c.get('name'), 'status': c.get('status')} for c in (camp_data.get('data', []) if isinstance(camp_data, dict) else [])[:5]],
                'error': camp_data.get('error') if isinstance(camp_data, dict) and 'error' in camp_data else None,
            })
            # Test 3: Try matching a known event
            upcoming = db.get_events(upcoming_only=True)
            if upcoming:
                test_event = upcoming[0]
                keywords = meta._generate_keywords(test_event['name'])
                campaigns_found = meta._find_campaigns(test_event['name'])
                results['tests'].append({
                    'test': f'Campaign match for "{test_event["name"]}"',
                    'keywords_generated': keywords[:10],
                    'campaigns_matched': len(campaigns_found),
                    'matched_names': [c['name'] for c in campaigns_found[:5]],
                })
        except Exception as e:
            results['tests'].append({'test': 'Exception', 'error': str(e)})
        return jsonify(results)
        # === Dashboard ===
    @app.route('/api/dashboard')
    def dashboard():
        """Complete dashboard data."""
        import time
        analyses = engine.analyze_portfolio()
        # Populate cache so targeting/export endpoints don't re-analyze
        _portfolio_cache['analyses'] = analyses
        _portfolio_cache['ts'] = time.time()
        # Portfolio totals
        total_tickets = sum(a.tickets_sold for a in analyses)
        total_capacity = sum(a.capacity for a in analyses)
        total_revenue = sum(a.revenue for a in analyses)
        total_spend = sum(a.ad_spend for a in analyses)
        # Decision counts
        decisions = {}
        for a in analyses:
            d = a.decision.value
            decisions[d] = decisions.get(d, 0) + 1
        # Customer stats
        segments = db.get_segment_counts()
        total_customers = db.get_customer_count()
        return jsonify({
            'portfolio': {
                'total_tickets': total_tickets,
                'total_capacity': total_capacity,
                'total_revenue': total_revenue,
                'total_spend': total_spend,
                'portfolio_cac': total_spend / total_tickets if total_tickets > 0 else 0,
                'event_count': len(analyses)
            },
            'decisions': decisions,
            'events': [_serialize_pacing(a) for a in analyses],
            'customers': {
                'total': total_customers,
                'segments': segments
            },
            'updated_at': datetime.now().isoformat(),
            'sync': {
                'done': _sync_state['done'],
                'running': _sync_state['running'],
                'error': _sync_state['error']
            }
        })
    @app.route('/api/events')
    def events():
        """List all events."""
        upcoming = request.args.get('upcoming', 'true').lower() == 'true'
        events = db.get_events(upcoming_only=upcoming)
        return jsonify(events)
    @app.route('/api/events/<event_id>')
    def event_detail(event_id: str):
        """Single event analysis."""
        analysis = engine.analyze_event(event_id)
        if not analysis:
            return jsonify({'error': 'Event not found'}), 404
        return jsonify(_serialize_pacing(analysis))
    # === CRM ===
    @app.route('/api/customers')
    def customers():
        """List customers with filters, search, sorting."""
        segment = request.args.get('segment')
        min_ltv = request.args.get('min_ltv', type=float)
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        sort_by = request.args.get('sort', 'ltv_score')
        order = request.args.get('order', 'DESC')
        search = request.args.get('search')
        city = request.args.get('city')
        event_type = request.args.get('event_type')
        customers = db.get_customers(segment, min_ltv, limit, offset, sort_by, order,
                                     search, city, event_type)
        total = db.get_customer_count(segment, search, city, event_type)
        return jsonify({
            'customers': customers,
            'total': total,
            'limit': limit,
            'offset': offset
        })
    @app.route('/api/customers/<email>')
    def customer_detail(email: str):
        """Single customer detail with order history."""
        customer = db.get_customer(email)
        if not customer:
            return jsonify({'error': 'Customer not found'}), 404
        orders = db.get_orders_for_customer(email)
        return jsonify({
            'customer': asdict(customer),
            'orders': orders
        })
    @app.route('/api/customers/segments')
    def customer_segments():
        """Segment breakdown."""
        return jsonify(db.get_segment_counts())
    @app.route('/api/customers/cities')
    def customer_cities():
        """Distinct cities from customer data."""
        return jsonify(db.get_distinct_cities())
    @app.route('/api/customers/event-types')
    def customer_event_types():
        """Distinct event types from customer data."""
        return jsonify(db.get_distinct_event_types())
    @app.route('/api/customers/high-value')
    def high_value_customers():
        """High value customers for targeting."""
        event_type = request.args.get('event_type')
        city = request.args.get('city')
        min_ltv = request.args.get('min_ltv', 50, type=float)
        limit = request.args.get('limit', 500, type=int)
        customers = db.get_high_value_customers(event_type, city, min_ltv, limit)
        return jsonify({'customers': customers, 'count': len(customers)})
    @app.route('/api/customers/at-risk')
    def at_risk_customers():
        """At-risk customers for reactivation."""
        min_orders = request.args.get('min_orders', 2, type=int)
        min_inactive = request.args.get('min_inactive', 180, type=int)
        customers = db.get_at_risk_customers(min_orders, min_inactive)
        total_value = sum(c['total_spent'] for c in customers)
        return jsonify({
            'customers': customers,
            'count': len(customers),
            'total_historical_value': total_value
        })
    # === Targeting ===
    @app.route('/api/targeting/<event_id>')
    def targeting(event_id: str):
        """Get targeting audiences with revenue gap, repeat rate, timing intelligence.

        Handles both real DB event_ids and synthetic grouped event_ids from timed-entry grouping.
        Combines buyers from ALL sessions of a timed-entry festival for accurate exclusion.
        """
        event = None
        real_event_ids = [event_id]  # IDs for ticket/revenue counting (same day)
        all_sibling_ids = [event_id]  # ALL session IDs for buyer exclusion
        base_event_name = ''  # Un-grouped name for pattern matching
        events_list = db.get_events(upcoming_only=False)
        # --- Step 1: Resolve event_id (real DB event or synthetic grouped event) ---
        for e in events_list:
            if e['event_id'] == event_id:
                event = e
                base_event_name = e['name']
                break
        if event:
            # Real event found — check for timed-entry siblings (same pattern, within 3 days)
            event_pattern = engine._get_pattern(event['name'])
            event_date = datetime.fromisoformat(event['event_date']).date()
            for e in events_list:
                if e['event_id'] == event_id:
                    continue
                if engine._get_pattern(e['name']) != event_pattern:
                    continue
                e_date = datetime.fromisoformat(e['event_date']).date()
                if abs((e_date - event_date).days) <= 3:
                    all_sibling_ids.append(e['event_id'])
                    if e_date == event_date:
                        real_event_ids.append(e['event_id'])
        else:
            # Not in DB — likely a synthetic grouped event from timed-entry grouping
            for a in _get_portfolio():
                if a.event_id == event_id:
                    real_event_ids = list(a.constituent_event_ids) if a.constituent_event_ids else [event_id]
                    all_sibling_ids = list(real_event_ids)
                    # Get city/type from first real constituent event
                    city = ''
                    event_type = ''
                    for e in events_list:
                        if e['event_id'] in real_event_ids:
                            city = e.get('city', '')
                            event_type = e.get('event_type', '')
                            base_event_name = e['name']  # Real name for pattern matching
                            break
                    event = {
                        'event_id': event_id,
                        'name': a.event_name,
                        'event_date': a.event_date,
                        'capacity': a.capacity,
                        'city': city,
                        'event_type': event_type,
                    }
                    break
        if not event:
            return jsonify({'error': 'Event not found'}), 404
        # --- Step 2: Combine buyers from ALL sessions for accurate exclusion ---
        current_buyers = set()
        for eid in all_sibling_ids:
            current_buyers.update(db.get_event_buyers(eid))
        # Tickets/revenue from same-day sessions only
        current_tickets = sum(db.get_event_tickets(eid) for eid in real_event_ids)
        current_revenue = sum(db.get_event_revenue(eid) for eid in real_event_ids)
        capacity = event.get('capacity', 500)
        avg_ticket_price = current_revenue / current_tickets if current_tickets > 0 else 0
        # Use base_event_name (real name) for pattern matching, not synthetic grouped name
        pattern_name = base_event_name or event['name']
        # ---- REVENUE GAP ANALYSIS ----
        pattern = engine._get_pattern(pattern_name)
        past_event_ids = db.get_pattern_event_ids(pattern, exclude_ids=list(set(all_sibling_ids)))
        # For past editions, group by (year, date) to handle past timed-entry events too
        events_by_id = {e['event_id']: e for e in events_list}
        past_by_year_date = defaultdict(list)
        for pid in past_event_ids:
            pe = events_by_id.get(pid)
            if pe:
                pe_date = datetime.fromisoformat(pe['event_date']).date()
                past_by_year_date[(pe_date.year, pe_date)].append(pe)
        # Aggregate past editions by (year, date) — combine timed-entry slots
        last_year_data = None
        last_year_buyers = set()
        all_past_buyers = set()
        for (yr, dt), pe_group in past_by_year_date.items():
            grp_tickets = sum(db.get_event_tickets(p['event_id']) for p in pe_group)
            grp_revenue = sum(db.get_event_revenue(p['event_id']) for p in pe_group)
            grp_buyers = set()
            for p in pe_group:
                grp_buyers.update(db.get_event_buyers(p['event_id']))
            all_past_buyers.update(grp_buyers)
            grp_capacity = max(p.get('capacity', 0) for p in pe_group)
            if last_year_data is None or yr > last_year_data.get('year', 0):
                last_year_data = {
                    'year': yr,
                    'event_name': pe_group[0]['name'],
                    'tickets': grp_tickets,
                    'revenue': grp_revenue,
                    'buyers': grp_buyers,
                    'capacity': grp_capacity,
                }
                last_year_buyers = grp_buyers
        # Repeat buyer rate
        repeat_buyers = current_buyers & all_past_buyers
        repeat_rate = len(repeat_buyers) / len(all_past_buyers) * 100 if all_past_buyers else 0
        # Revenue gap
        revenue_gap = {}
        if last_year_data:
            tickets_gap = max(0, last_year_data['tickets'] - current_tickets)
            revenue_needed = max(0, last_year_data['revenue'] - current_revenue)
            ly_avg_price = last_year_data['revenue'] / last_year_data['tickets'] if last_year_data['tickets'] > 0 else avg_ticket_price
            revenue_gap = {
                'last_year': last_year_data['year'],
                'last_year_tickets': last_year_data['tickets'],
                'last_year_revenue': round(last_year_data['revenue'], 2),
                'last_year_capacity': last_year_data['capacity'],
                'tickets_gap': tickets_gap,
                'revenue_gap': round(revenue_needed, 2),
                'avg_ticket_price': round(avg_ticket_price or ly_avg_price, 2),
                'pct_of_last_year': round(current_tickets / last_year_data['tickets'] * 100, 1) if last_year_data['tickets'] > 0 else 0,
            }
        # ---- AUDIENCE BUILDING ----
        # 1. Past attendees who haven't purchased (sorted by priority)
        #    Pass combined buyer set and all sibling IDs so grouped events are handled correctly
        past_attendees = db.get_past_attendees_not_purchased(
            event_id, pattern_name, limit=5000,
            current_buyer_emails=current_buyers,
            exclude_event_ids=list(set(all_sibling_ids))
        )
        # Priority score each customer: champions first, then by timing urgency
        days_until = (datetime.fromisoformat(event['event_date']).date() - date.today()).days
        priority_weights = {'champion': 100, 'loyal': 80, 'potential': 60, 'at_risk': 40, 'hibernating': 20, 'other': 30}
        for c in past_attendees:
            seg_score = priority_weights.get(c.get('rfm_segment', 'other'), 30)
            ltv = c.get('ltv_score', 0) or 0
            # Timing urgency: early_birds who haven't bought yet are overdue
            timing = c.get('timing_segment', '')
            timing_urgency = 0
            if timing == 'super_early_bird' and days_until < 60: timing_urgency = 30
            elif timing == 'early_bird' and days_until < 45: timing_urgency = 25
            elif timing == 'planner' and days_until < 30: timing_urgency = 15
            elif timing == 'last_minute' and days_until < 14: timing_urgency = 10
            past_editions = c.get('past_editions', 1) or 1
            c['priority_score'] = seg_score + ltv * 0.3 + timing_urgency + (past_editions * 10)
        past_attendees.sort(key=lambda x: x.get('priority_score', 0), reverse=True)
        past_value = sum(c.get('total_spent', 0) for c in past_attendees)
        # Timing breakdown for past attendees
        timing_breakdown = {}
        for c in past_attendees:
            ts = c.get('timing_segment', 'unknown') or 'unknown'
            if ts not in timing_breakdown:
                timing_breakdown[ts] = {'count': 0, 'overdue': False}
            timing_breakdown[ts]['count'] += 1
        # Mark which timing segments are overdue
        if days_until < 60: timing_breakdown.get('super_early_bird', {}).update({'overdue': True})
        if days_until < 45: timing_breakdown.get('early_bird', {}).update({'overdue': True})
        if days_until < 30: timing_breakdown.get('planner', {}).update({'overdue': True})
        # Segment breakdown for past attendees
        segment_breakdown = {}
        for c in past_attendees:
            seg = c.get('rfm_segment', 'other') or 'other'
            segment_breakdown[seg] = segment_breakdown.get(seg, 0) + 1
        # 2. City prospects
        city_prospects = []
        if event.get('city'):
            all_past_emails = {c['email'] for c in past_attendees}
            all_past_emails.update(current_buyers)
            city_prospects = db.get_city_prospects(
                event['city'], exclude_emails=all_past_emails, limit=1000
            )
        city_value = sum(c.get('total_spent', 0) for c in city_prospects)
        # 3. Event type fans
        type_prospects = []
        if event.get('event_type'):
            all_exclude = {c['email'] for c in past_attendees}
            all_exclude.update(current_buyers)
            all_exclude.update({c['email'] for c in city_prospects})
            type_prospects = db.get_type_prospects(
                event['event_type'], city=event.get('city', ''), exclude_emails=all_exclude, limit=1000
            )
        type_value = sum(c.get('total_spent', 0) for c in type_prospects)
        # 4. At-risk with affinity — require BOTH city AND event type match
        #    Someone who went to "DC Comedy Show" is not a win-back for "DC Beer Fest"
        all_already_listed = {c['email'] for c in past_attendees}
        all_already_listed.update({c['email'] for c in city_prospects})
        all_already_listed.update({c['email'] for c in type_prospects})
        all_already_listed.update(current_buyers)
        at_risk = db.get_at_risk_customers(min_orders=2, min_days_inactive=180)
        at_risk_for_event = []
        for c in at_risk:
            if c['email'] in all_already_listed:
                continue
            ecities = c.get('cities', '{}')
            etypes = c.get('event_types', '{}')
            city_match = _json_key_match(ecities, event.get('city'))
            type_match = _json_key_match(etypes, event.get('event_type'))
            # Must match BOTH city and event type — no random event-goers
            if not (city_match and type_match):
                continue
            at_risk_for_event.append(c)
        at_risk_for_event.sort(key=lambda x: -(x.get('total_spent', 0) or 0))
        at_risk_value = sum(c.get('total_spent', 0) for c in at_risk_for_event)
        # ---- QUICK WIN CALCULATION ----
        # Estimate: if we email top-priority past attendees, how many tickets at historical rebuy rate?
        historical_rebuy_rate = repeat_rate / 100 if repeat_rate > 0 else 0.10  # default 10%
        champion_count = segment_breakdown.get('champion', 0) + segment_breakdown.get('loyal', 0)
        quick_win_emails = champion_count if champion_count > 0 else len(past_attendees)
        quick_win_tickets = int(quick_win_emails * min(historical_rebuy_rate * 1.5, 0.25))  # champions convert 1.5x avg
        quick_win_revenue = round(quick_win_tickets * (avg_ticket_price or 45), 2)
        # ---- TIMING RECOMMENDATIONS ----
        timing_recs = []
        if days_until > 45:
            overdue_early = timing_breakdown.get('super_early_bird', {}).get('count', 0)
            if overdue_early > 0:
                timing_recs.append({
                    'urgency': 'now',
                    'action': f'Email {overdue_early} super-early-bird past attendees — they usually buy 60+ days out',
                    'count': overdue_early,
                    'timing_segments': ['super_early_bird'],
                })
        if days_until > 14 and days_until <= 45:
            overdue_count = sum(
                timing_breakdown.get(ts, {}).get('count', 0)
                for ts in ['super_early_bird', 'early_bird']
            )
            if overdue_count > 0:
                timing_recs.append({
                    'urgency': 'now',
                    'action': f'Email {overdue_count} early birds NOW — they are overdue to buy',
                    'count': overdue_count,
                    'timing_segments': ['super_early_bird', 'early_bird'],
                })
            planner_count = timing_breakdown.get('planner', {}).get('count', 0)
            if planner_count > 0:
                timing_recs.append({
                    'urgency': 'soon',
                    'action': f'{planner_count} planners typically buy 14-28 days out — email this week',
                    'count': planner_count,
                    'timing_segments': ['planner'],
                })
        if days_until <= 14:
            all_overdue = len(past_attendees)
            timing_recs.append({
                'urgency': 'critical',
                'action': f'FINAL PUSH: Email all {all_overdue} past attendees with urgency/scarcity messaging',
                'count': all_overdue,
                'timing_segments': ['all'],
            })
            last_min = timing_breakdown.get('last_minute', {}).get('count', 0) + timing_breakdown.get('spontaneous', {}).get('count', 0)
            if last_min > 0:
                timing_recs.append({
                    'urgency': 'now',
                    'action': f'{last_min} last-minute buyers are entering their buying window',
                    'count': last_min,
                    'timing_segments': ['last_minute', 'spontaneous'],
                })
        return jsonify({
            'event': event,
            'export_token': os.environ.get('EXPORT_API_KEY', ''),
            'current_buyers': len(current_buyers),
            'current_tickets': current_tickets,
            'current_revenue': round(current_revenue, 2),
            'capacity': capacity,
            'days_until': days_until,
            'avg_ticket_price': round(avg_ticket_price, 2),
            'revenue_gap': revenue_gap,
            'repeat_buyers': {
                'count': len(repeat_buyers),
                'total_past_buyers': len(all_past_buyers),
                'rate': round(repeat_rate, 1),
                'last_year_buyers': len(last_year_buyers),
                'rebought_from_last_year': len(current_buyers & last_year_buyers),
                'last_year_rebuy_rate': round(
                    len(current_buyers & last_year_buyers) / len(last_year_buyers) * 100, 1
                ) if last_year_buyers else 0,
            },
            'quick_win': {
                'audience': 'Champion & Loyal past attendees',
                'emails_to_send': quick_win_emails,
                'expected_tickets': quick_win_tickets,
                'expected_revenue': quick_win_revenue,
                'conversion_rate_used': round(min(historical_rebuy_rate * 1.5, 0.25) * 100, 1),
            },
            'timing_recommendations': timing_recs,
            'audiences': {
                'past_attendees': {
                    'label': 'Past Attendees Not Purchased',
                    'description': f'Attended previous editions but no ticket this year — sorted by conversion likelihood',
                    'count': len(past_attendees),
                    'historical_value': round(past_value, 2),
                    'customers': past_attendees[:100],
                    'total_available': len(past_attendees),
                    'segment_breakdown': segment_breakdown,
                    'timing_breakdown': timing_breakdown,
                },
                'city_prospects': {
                    'label': f'{event.get("city", "Local")} Event Fans',
                    'description': f'High-value customers who attend events in {event.get("city", "this city")} but never attended this one',
                    'count': len(city_prospects),
                    'historical_value': round(city_value, 2),
                    'customers': city_prospects[:100],
                    'total_available': len(city_prospects),
                },
                'type_fans': {
                    'label': f'{(event.get("event_type") or "Similar").title()} Lovers',
                    'description': f'Customers who love {event.get("event_type", "this type of")} events but have not attended this one',
                    'count': len(type_prospects),
                    'historical_value': round(type_value, 2),
                    'customers': type_prospects[:100],
                    'total_available': len(type_prospects),
                },
                'at_risk': {
                    'label': 'Win-Back Targets',
                    'description': 'Previously active customers going cold — re-engage before they churn',
                    'count': len(at_risk_for_event),
                    'historical_value': round(at_risk_value, 2),
                    'customers': at_risk_for_event[:100],
                    'total_available': len(at_risk_for_event),
                }
            }
        })
    # === Intelligence Engine ===
    @app.route('/api/intelligence/<event_id>')
    def intelligence(event_id: str):
        """Advanced intelligence for an event: cross-sell, velocity, promo codes,
        super-spreaders, churn prediction, VIP, ticket tiers, cannibalization, competitor radar."""
        # --- Resolve event (same logic as targeting) ---
        event = None
        all_sibling_ids = [event_id]
        base_event_name = ''
        events_list = db.get_events(upcoming_only=False)
        for e in events_list:
            if e['event_id'] == event_id:
                event = e
                base_event_name = e['name']
                break
        if event:
            event_pattern = engine._get_pattern(event['name'])
            event_date = datetime.fromisoformat(event['event_date']).date()
            for e in events_list:
                if e['event_id'] == event_id:
                    continue
                if engine._get_pattern(e['name']) != event_pattern:
                    continue
                e_date = datetime.fromisoformat(e['event_date']).date()
                if abs((e_date - event_date).days) <= 3:
                    all_sibling_ids.append(e['event_id'])
        else:
            for a in _get_portfolio():
                if a.event_id == event_id:
                    all_sibling_ids = list(a.constituent_event_ids) if a.constituent_event_ids else [event_id]
                    for e in events_list:
                        if e['event_id'] in all_sibling_ids:
                            base_event_name = e['name']
                            event = {
                                'event_id': event_id, 'name': a.event_name,
                                'event_date': a.event_date, 'capacity': a.capacity,
                                'city': e.get('city', ''), 'event_type': e.get('event_type', ''),
                            }
                            break
                    break
        if not event:
            return jsonify({'error': 'Event not found'}), 404

        current_buyers = set()
        for eid in all_sibling_ids:
            current_buyers.update(db.get_event_buyers(eid))
        pattern_name = base_event_name or event['name']
        days_until = (datetime.fromisoformat(event['event_date']).date() - date.today()).days
        current_tickets = sum(db.get_event_tickets(eid) for eid in all_sibling_ids)
        current_revenue = sum(db.get_event_revenue(eid) for eid in all_sibling_ids)
        capacity = event.get('capacity', 500)
        avg_ticket_price = current_revenue / current_tickets if current_tickets > 0 else 0

        # ---- 1. CROSS-SELL ENGINE ----
        cross_sell = []
        if event.get('city') and event.get('event_type'):
            cross_sell = db.get_cross_sell_candidates(
                event['event_type'], event['city'],
                exclude_event_ids=all_sibling_ids,
                exclude_emails=current_buyers,
                limit=500
            )
        cross_sell_by_type = defaultdict(int)
        for c in cross_sell:
            for t in c.get('attended_types', []):
                cross_sell_by_type[t] += 1

        # ---- 2. SELL-THROUGH VELOCITY & GAP-CLOSING PLAN ----
        pattern = engine._get_pattern(pattern_name)
        past_event_ids = db.get_pattern_event_ids(pattern, exclude_ids=list(set(all_sibling_ids)))
        # Calculate sell velocity (tickets per day over last 7 days)
        recent_velocity = 0
        for eid in all_sibling_ids:
            snaps = db.get_snapshots(eid)
            if len(snaps) >= 2:
                # Snapshots are ordered by days_before DESC, so first entries are most recent
                recent = [s for s in snaps if s['days_before_event'] <= days_until + 7]
                if len(recent) >= 2:
                    tickets_diff = recent[0]['tickets_cumulative'] - recent[-1]['tickets_cumulative']
                    days_diff = max(1, len(recent))
                    recent_velocity += tickets_diff / days_diff
        tickets_gap = max(0, capacity - current_tickets)
        days_to_sell = int(tickets_gap / recent_velocity) if recent_velocity > 0 else 999
        # Build gap-closing action plan
        past_attendees = db.get_past_attendees_not_purchased(
            event_id, pattern_name, limit=5000,
            current_buyer_emails=current_buyers,
            exclude_event_ids=list(set(all_sibling_ids))
        )
        gap_plan = []
        remaining_gap = tickets_gap
        if remaining_gap > 0 and past_attendees:
            # Segment past attendees by likelihood
            champions = [c for c in past_attendees if c.get('rfm_segment') in ('champion', 'loyal')]
            est_from_champions = int(len(champions) * 0.20)  # 20% conversion for champions
            gap_plan.append({
                'action': f'Email {len(champions)} champion/loyal past attendees',
                'audience': 'past_champions',
                'audience_size': len(champions),
                'expected_tickets': est_from_champions,
                'conversion_rate': '20%',
                'priority': 1,
            })
            remaining_gap -= est_from_champions

        if remaining_gap > 0 and cross_sell:
            est_from_cross = int(len(cross_sell) * 0.05)  # 5% conversion for cross-sell
            gap_plan.append({
                'action': f'Cross-sell to {len(cross_sell)} fans of other event types in {event.get("city", "city")}',
                'audience': 'cross_sell',
                'audience_size': len(cross_sell),
                'expected_tickets': est_from_cross,
                'conversion_rate': '5%',
                'priority': 2,
            })
            remaining_gap -= est_from_cross

        if remaining_gap > 0:
            other_past = [c for c in past_attendees if c.get('rfm_segment') not in ('champion', 'loyal')]
            est_from_others = int(len(other_past) * 0.08)
            gap_plan.append({
                'action': f'Email {len(other_past)} remaining past attendees with urgency',
                'audience': 'past_other',
                'audience_size': len(other_past),
                'expected_tickets': est_from_others,
                'conversion_rate': '8%',
                'priority': 3,
            })
            remaining_gap -= est_from_others

        velocity_alert = {
            'current_velocity': round(recent_velocity, 1),
            'velocity_unit': 'tickets/day',
            'tickets_remaining': tickets_gap,
            'days_at_current_pace': days_to_sell if days_to_sell < 999 else None,
            'will_sell_out': days_to_sell <= days_until if days_to_sell < 999 else False,
            'projected_unsold': max(0, tickets_gap - int(recent_velocity * days_until)) if recent_velocity > 0 else tickets_gap,
            'gap_closing_plan': gap_plan,
            'total_recoverable': sum(p['expected_tickets'] for p in gap_plan),
        }

        # ---- 3. PURCHASE VELOCITY TRIGGERS ----
        # For past editions: how quickly do multi-event buyers purchase after attending?
        post_event_velocity = []
        if past_event_ids:
            ph = ','.join(['?' for _ in past_event_ids])
            rows = db.conn.execute(f"""
                SELECT o1.email,
                       o1.order_timestamp as first_purchase,
                       o2.order_timestamp as next_purchase,
                       o2.event_id as next_event_id,
                       e2.name as next_event_name,
                       CAST(julianday(o2.order_timestamp) - julianday(o1.order_timestamp) AS INTEGER) as days_gap
                FROM orders o1
                JOIN orders o2 ON o1.email = o2.email AND o2.order_timestamp > o1.order_timestamp
                JOIN events e2 ON o2.event_id = e2.event_id
                WHERE o1.event_id IN ({ph})
                  AND o2.event_id NOT IN ({ph})
                ORDER BY days_gap ASC
                LIMIT 5000
            """, past_event_ids + past_event_ids).fetchall()
            # Bucket by timing
            buckets = {'0-7': 0, '8-14': 0, '15-30': 0, '31-60': 0, '61-90': 0, '90+': 0}
            gaps = []
            for r in rows:
                g = r['days_gap'] or 0
                gaps.append(g)
                if g <= 7: buckets['0-7'] += 1
                elif g <= 14: buckets['8-14'] += 1
                elif g <= 30: buckets['15-30'] += 1
                elif g <= 60: buckets['31-60'] += 1
                elif g <= 90: buckets['61-90'] += 1
                else: buckets['90+'] += 1
            optimal_window = None
            if gaps:
                # Median gap = optimal follow-up timing
                gaps.sort()
                optimal_window = gaps[len(gaps) // 2]
            post_event_velocity = {
                'buckets': buckets,
                'total_repeat_purchases': len(rows),
                'optimal_followup_days': optimal_window,
                'recommendation': f'Send follow-up email {optimal_window} days after event for maximum conversion' if optimal_window else 'Not enough data for timing recommendation',
            }

        # ---- 4. PROMO CODE INTELLIGENCE ----
        # Compare promo vs non-promo buyers by segment
        all_event_ids = list(set(all_sibling_ids + past_event_ids[:10]))  # Current + recent past
        promo_stats = []
        for eid in all_event_ids[:5]:  # Limit to 5 events for performance
            stats = db.get_promo_code_stats(eid)
            evt = db.get_event(eid)
            if stats and evt:
                total_revenue = db.get_event_revenue(eid)
                total_orders_count = len(db.get_orders_for_event(eid))
                promo_revenue = sum(s['revenue'] for s in stats)
                promo_orders = sum(s['uses'] for s in stats)
                promo_stats.append({
                    'event_name': evt['name'],
                    'event_id': eid,
                    'total_orders': total_orders_count,
                    'promo_orders': promo_orders,
                    'promo_pct': round(promo_orders / total_orders_count * 100, 1) if total_orders_count > 0 else 0,
                    'promo_revenue': round(promo_revenue, 2),
                    'full_price_revenue': round(total_revenue - promo_revenue, 2),
                    'top_codes': stats[:5],
                })
        # Segment-level promo sensitivity
        promo_by_segment = {}
        rows = db.conn.execute("""
            SELECT c.rfm_segment,
                   COUNT(CASE WHEN o.promo_code IS NOT NULL AND o.promo_code != '' THEN 1 END) as promo_orders,
                   COUNT(*) as total_orders,
                   AVG(CASE WHEN o.promo_code IS NOT NULL AND o.promo_code != '' THEN o.gross_amount END) as avg_promo_price,
                   AVG(CASE WHEN o.promo_code IS NULL OR o.promo_code = '' THEN o.gross_amount END) as avg_full_price
            FROM orders o
            JOIN customers c ON lower(o.email) = c.email
            WHERE c.rfm_segment IS NOT NULL AND c.rfm_segment != ''
            GROUP BY c.rfm_segment
        """).fetchall()
        for r in rows:
            seg = r['rfm_segment']
            total = r['total_orders'] or 1
            promo_by_segment[seg] = {
                'promo_rate': round((r['promo_orders'] or 0) / total * 100, 1),
                'avg_promo_price': round(r['avg_promo_price'] or 0, 2),
                'avg_full_price': round(r['avg_full_price'] or 0, 2),
                'recommendation': 'Skip discounts — they buy at full price' if (r['promo_orders'] or 0) / total < 0.15 else 'Price sensitive — promos drive conversions'
            }

        # ---- 5. SUPER-SPREADERS (+1 GOLD MINE) ----
        spreaders = db.get_multi_ticket_buyers(min_avg_tickets=1.5)
        # Filter to those relevant to this event (city or type match)
        relevant_spreaders = []
        total_plus_ones = 0
        for s in spreaders:
            city_match = _json_key_match(s.get('cities', '{}'), event.get('city'))
            type_match = _json_key_match(s.get('event_types', '{}'), event.get('event_type'))
            if city_match or type_match:
                est_plus_ones = round((s.get('avg_tickets_per_order', 1) - 1) * s.get('total_orders', 1), 0)
                s['estimated_plus_ones'] = int(est_plus_ones)
                total_plus_ones += int(est_plus_ones)
                relevant_spreaders.append(s)
        relevant_spreaders.sort(key=lambda x: -(x.get('avg_tickets_per_order', 0)))
        spreader_intel = {
            'total_spreaders': len(relevant_spreaders),
            'total_estimated_plus_ones': total_plus_ones,
            'top_spreaders': relevant_spreaders[:20],
            'recommendation': f'{len(relevant_spreaders)} super-spreaders brought an est. {total_plus_ones} friends. Offer referral incentives to these buyers.' if relevant_spreaders else 'Not enough multi-ticket buyer data yet.',
            'downloadable': len(relevant_spreaders),
        }

        # ---- 6. TICKET TIER OPTIMIZATION ----
        tier_data = {}
        for eid in all_sibling_ids:
            tiers = db.get_event_ticket_types(eid)
            for t in tiers:
                name = t['ticket_type']
                if name not in tier_data:
                    tier_data[name] = {'orders': 0, 'tickets': 0, 'revenue': 0, 'avg_price': 0, 'avg_days_before': 0, 'count': 0}
                tier_data[name]['orders'] += t['orders']
                tier_data[name]['tickets'] += t['tickets']
                tier_data[name]['revenue'] += t['revenue']
                tier_data[name]['avg_price'] = (tier_data[name]['avg_price'] * tier_data[name]['count'] + t['avg_price']) / (tier_data[name]['count'] + 1)
                tier_data[name]['avg_days_before'] = (tier_data[name]['avg_days_before'] * tier_data[name]['count'] + (t['avg_days_before'] or 0)) / (tier_data[name]['count'] + 1)
                tier_data[name]['count'] += 1
        segment_prefs = db.get_segment_ticket_preferences()
        tier_recommendations = []
        for seg, prefs in segment_prefs.items():
            if prefs:
                top_tier = prefs[0]
                tier_recommendations.append({
                    'segment': seg,
                    'preferred_tier': top_tier['ticket_type'],
                    'avg_price': top_tier['avg_price'],
                    'purchase_count': top_tier['count'],
                })

        # ---- 7. CHURN PREDICTION WITH SAVE WINDOWS ----
        churn_risks = db.get_churn_risk_customers()
        # Filter to event-relevant customers
        event_churn = []
        for c in churn_risks:
            city_match = _json_key_match(c.get('cities', '{}'), event.get('city'))
            type_match = _json_key_match(c.get('event_types', '{}'), event.get('event_type'))
            if city_match or type_match:
                event_churn.append(c)
        churn_by_window = {'critical': [], 'urgent': [], 'watch': []}
        for c in event_churn:
            window = c.get('save_window', 'watch')
            if window in churn_by_window:
                churn_by_window[window].append(c)
        churn_intel = {
            'total_at_risk': len(event_churn),
            'critical': len(churn_by_window['critical']),
            'urgent': len(churn_by_window['urgent']),
            'watch': len(churn_by_window['watch']),
            'critical_customers': churn_by_window['critical'][:20],
            'urgent_customers': churn_by_window['urgent'][:20],
            'total_value_at_risk': round(sum(c.get('total_spent', 0) for c in event_churn), 2),
            'recommendation': f'Email {len(churn_by_window["critical"])} critical-window customers THIS WEEK or lose them. {len(churn_by_window["urgent"])} more in the next 30 days.' if churn_by_window['critical'] else f'{len(churn_by_window["urgent"])} customers in urgent save window.',
        }

        # ---- 8. VIP IDENTIFICATION ----
        vips = db.get_vip_customers(min_events=3, min_spent=200, limit=50)
        # Filter to event-relevant
        relevant_vips = []
        for v in vips:
            city_match = _json_key_match(v.get('cities', '{}'), event.get('city'))
            type_match = _json_key_match(v.get('event_types', '{}'), event.get('event_type'))
            if city_match or type_match:
                already_bought = v['email'] in current_buyers
                v['already_bought'] = already_bought
                relevant_vips.append(v)
        vip_not_bought = [v for v in relevant_vips if not v.get('already_bought')]
        vip_intel = {
            'total_vips': len(relevant_vips),
            'vips_not_bought': len(vip_not_bought),
            'vip_total_value': round(sum(v.get('total_spent', 0) for v in relevant_vips), 2),
            'top_vips': relevant_vips[:20],
            'unbought_vips': vip_not_bought[:20],
            'recommendation': f'{len(vip_not_bought)} VIPs haven\'t bought yet — white-glove outreach, personal invite.' if vip_not_bought else 'All relevant VIPs have purchased!',
        }

        # ---- 9. COMPETITOR RADAR ----
        # Check for OTHER upcoming events in same city within +/- 2 weeks
        competitors = []
        if event.get('city') and event.get('event_date'):
            event_date = datetime.fromisoformat(event['event_date']).date()
            for e in events_list:
                if e['event_id'] in all_sibling_ids:
                    continue
                if e.get('city') != event.get('city'):
                    continue
                e_date = datetime.fromisoformat(e['event_date']).date()
                day_diff = (e_date - event_date).days
                if abs(day_diff) <= 14 and e_date >= date.today():
                    competitors.append({
                        'event_name': e['name'],
                        'event_date': e['event_date'],
                        'event_type': e.get('event_type', ''),
                        'days_apart': day_diff,
                        'same_weekend': abs(day_diff) <= 2,
                    })
        competitor_intel = {
            'competing_events': competitors,
            'count': len(competitors),
            'same_weekend': sum(1 for c in competitors if c.get('same_weekend')),
            'recommendation': f'WARNING: {len(competitors)} competing events in {event.get("city")} within 2 weeks. Differentiate messaging.' if competitors else 'No competing events detected in your calendar.',
        }

        # ---- 10. CANNIBALIZATION DETECTOR ----
        # Check if YOUR OWN events are too close together in same city
        cannibalization = []
        upcoming = db.get_events(upcoming_only=True)
        if event.get('city') and event.get('event_date'):
            event_date = datetime.fromisoformat(event['event_date']).date()
            for e in upcoming:
                if e['event_id'] in all_sibling_ids:
                    continue
                if e.get('city') != event.get('city'):
                    continue
                e_date = datetime.fromisoformat(e['event_date']).date()
                day_diff = (e_date - event_date).days
                if 0 < abs(day_diff) <= 21:  # Within 3 weeks
                    # Check buyer overlap
                    other_buyers = db.get_event_buyers(e['event_id'])
                    overlap = current_buyers & other_buyers
                    cannibalization.append({
                        'event_name': e['name'],
                        'event_date': e['event_date'],
                        'event_type': e.get('event_type', ''),
                        'days_apart': day_diff,
                        'buyer_overlap': len(overlap),
                        'overlap_pct': round(len(overlap) / len(current_buyers) * 100, 1) if current_buyers else 0,
                    })
        cannibal_intel = {
            'risk_events': cannibalization,
            'count': len(cannibalization),
            'recommendation': f'CAUTION: {len(cannibalization)} of your own events are within 3 weeks in {event.get("city")}. Check for audience cannibalization.' if cannibalization else 'No self-cannibalization risk detected.',
        }

        # ---- 11. REVENUE PROJECTOR ----
        # Use historical sell-through curves to project final revenue
        analyses = _get_portfolio()
        this_analysis = None
        for a in analyses:
            if a.event_id == event_id:
                this_analysis = a
                break
        revenue_projection = {
            'current_tickets': current_tickets,
            'current_revenue': round(current_revenue, 2),
            'capacity': capacity,
            'sell_through_pct': round(current_tickets / capacity * 100, 1) if capacity > 0 else 0,
            'projected_final_tickets': this_analysis.projected_final if this_analysis else current_tickets,
            'projected_range': list(this_analysis.projected_range) if this_analysis else [current_tickets, capacity],
            'projected_revenue': round((this_analysis.projected_final if this_analysis else current_tickets) * avg_ticket_price, 2),
            'projected_revenue_range': [
                round(this_analysis.projected_range[0] * avg_ticket_price, 2) if this_analysis else round(current_revenue, 2),
                round(this_analysis.projected_range[1] * avg_ticket_price, 2) if this_analysis else round(capacity * avg_ticket_price, 2),
            ],
            'confidence': this_analysis.confidence if this_analysis else 0.5,
            'avg_ticket_price': round(avg_ticket_price, 2),
        }

        return jsonify({
            'event': event,
            'days_until': days_until,
            'cross_sell': {
                'candidates': len(cross_sell),
                'by_source_type': dict(cross_sell_by_type),
                'top_candidates': [{
                    'email': c['email'],
                    'attended_types': c.get('attended_types', []),
                    'ltv_score': c.get('ltv_score', 0),
                    'total_spent': c.get('total_spent', 0),
                } for c in cross_sell[:20]],
                'recommendation': f'{len(cross_sell)} fans of other events in {event.get("city", "city")} who haven\'t tried {event.get("event_type", "this type")} — prime cross-sell targets.' if cross_sell else 'No cross-sell candidates found.',
            },
            'velocity': velocity_alert,
            'purchase_timing': post_event_velocity,
            'promo_intelligence': {
                'by_event': promo_stats,
                'by_segment': promo_by_segment,
            },
            'super_spreaders': spreader_intel,
            'ticket_tiers': {
                'current_event': tier_data,
                'segment_preferences': tier_recommendations,
            },
            'churn_prediction': churn_intel,
            'vips': vip_intel,
            'competitors': competitor_intel,
            'cannibalization': cannibal_intel,
            'revenue_projection': revenue_projection,
            'export_token': os.environ.get('EXPORT_API_KEY', ''),
        })

    # === Export: Cross-sell and VIP audiences ===
    @app.route('/api/export/intelligence-csv')
    def export_intelligence_csv():
        """Export intelligence audiences (cross-sell, super-spreaders, VIPs, churn) as CSV."""
        export_key = os.environ.get('EXPORT_API_KEY', '')
        if export_key:
            provided = request.args.get('key', '') or request.headers.get('X-Export-Key', '')
            if provided != export_key:
                return jsonify({'error': 'Unauthorized'}), 401
        event_id = request.args.get('event_id')
        audience = request.args.get('audience')  # cross_sell, super_spreaders, vips, churn_critical, churn_urgent
        if not event_id or not audience:
            return jsonify({'error': 'event_id and audience required'}), 400
        # Resolve event
        event = None
        all_sibling_ids = [event_id]
        events_list = db.get_events(upcoming_only=False)
        for e in events_list:
            if e['event_id'] == event_id:
                event = e
                break
        if not event:
            for a in _get_portfolio():
                if a.event_id == event_id:
                    all_sibling_ids = list(a.constituent_event_ids) if a.constituent_event_ids else [event_id]
                    for e in events_list:
                        if e['event_id'] in all_sibling_ids:
                            event = e
                            break
                    break
        if not event:
            return jsonify({'error': 'Event not found'}), 404
        current_buyers = set()
        for eid in all_sibling_ids:
            current_buyers.update(db.get_event_buyers(eid))

        customers_list = []
        if audience == 'cross_sell':
            customers_list = db.get_cross_sell_candidates(
                event.get('event_type', ''), event.get('city', ''),
                exclude_emails=current_buyers, limit=5000)
        elif audience == 'super_spreaders':
            spreaders = db.get_multi_ticket_buyers(min_avg_tickets=1.5)
            for s in spreaders:
                city_match = _json_key_match(s.get('cities', '{}'), event.get('city'))
                type_match = _json_key_match(s.get('event_types', '{}'), event.get('event_type'))
                if city_match or type_match:
                    customers_list.append(s)
        elif audience == 'vips':
            vips = db.get_vip_customers(min_events=3, min_spent=200, limit=200)
            for v in vips:
                city_match = _json_key_match(v.get('cities', '{}'), event.get('city'))
                type_match = _json_key_match(v.get('event_types', '{}'), event.get('event_type'))
                if (city_match or type_match) and v['email'] not in current_buyers:
                    customers_list.append(v)
        elif audience == 'churn_critical':
            churn = db.get_churn_risk_customers()
            for c in churn:
                if c.get('save_window') in ('critical', 'urgent'):
                    city_match = _json_key_match(c.get('cities', '{}'), event.get('city'))
                    type_match = _json_key_match(c.get('event_types', '{}'), event.get('event_type'))
                    if city_match or type_match:
                        customers_list.append(c)
        # Build CSV
        output = io.StringIO()
        fields = ['email', 'favorite_city', 'favorite_event_type', 'rfm_segment',
                  'total_orders', 'total_events', 'total_spent', 'ltv_score',
                  'days_since_last', 'avg_tickets_per_order']
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for c in customers_list:
            writer.writerow(c)
        from flask import Response
        csv_data = output.getvalue()
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', event.get('name', 'event'))
        filename = f"{safe_name}_{audience}.csv"
        return Response(
            csv_data, mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}',
                     'Access-Control-Allow-Origin': '*'})

    @app.route('/api/export/csv')
    def export_csv():
        """Export a targeting audience as CSV."""
        # --- Auth check: require EXPORT_API_KEY if set ---
        export_key = os.environ.get('EXPORT_API_KEY', '')
        if export_key:
            provided = request.args.get('key', '') or request.headers.get('X-Export-Key', '')
            if provided != export_key:
                return jsonify({'error': 'Unauthorized — set EXPORT_API_KEY in Railway and pass ?key= parameter'}), 401
        event_id = request.args.get('event_id')
        audience = request.args.get('audience')  # past_attendees, city_prospects, type_fans, at_risk
        if not event_id or not audience:
            return jsonify({'error': 'event_id and audience required'}), 400
        # --- Resolve event_id (same logic as targeting endpoint) ---
        event = None
        all_sibling_ids = [event_id]
        base_event_name = ''
        events_list = db.get_events(upcoming_only=False)
        for e in events_list:
            if e['event_id'] == event_id:
                event = e
                base_event_name = e['name']
                break
        if event:
            # Real event — find timed-entry siblings
            event_pattern = engine._get_pattern(event['name'])
            event_date = datetime.fromisoformat(event['event_date']).date()
            for e in events_list:
                if e['event_id'] == event_id:
                    continue
                if engine._get_pattern(e['name']) != event_pattern:
                    continue
                e_date = datetime.fromisoformat(e['event_date']).date()
                if abs((e_date - event_date).days) <= 3:
                    all_sibling_ids.append(e['event_id'])
        else:
            # Synthetic grouped event — resolve via cached portfolio analysis
            for a in _get_portfolio():
                if a.event_id == event_id:
                    all_sibling_ids = list(a.constituent_event_ids) if a.constituent_event_ids else [event_id]
                    for e in events_list:
                        if e['event_id'] in all_sibling_ids:
                            base_event_name = e['name']
                            event = {
                                'event_id': event_id, 'name': a.event_name,
                                'event_date': a.event_date, 'capacity': a.capacity,
                                'city': e.get('city', ''), 'event_type': e.get('event_type', ''),
                            }
                            break
                    if not event:
                        event = {'event_id': event_id, 'name': a.event_name,
                                 'event_date': a.event_date, 'capacity': a.capacity,
                                 'city': '', 'event_type': ''}
                    break
        if not event:
            return jsonify({'error': 'Event not found'}), 404
        # Combine buyers from ALL siblings
        current_buyers = set()
        for eid in all_sibling_ids:
            current_buyers.update(db.get_event_buyers(eid))
        pattern_name = base_event_name or event['name']
        # --- Build audience list ---
        timing_filter = request.args.get('timing', '')  # comma-separated timing segments or 'all'
        customers_list = []
        if audience == 'timing':
            # Timing-filtered past attendees (for "DO NOW" / "THIS WEEK" downloads)
            all_past = db.get_past_attendees_not_purchased(
                event_id, pattern_name, limit=10000,
                current_buyer_emails=current_buyers,
                exclude_event_ids=list(set(all_sibling_ids)))
            if timing_filter == 'all':
                customers_list = all_past
            elif timing_filter:
                segments = [s.strip() for s in timing_filter.split(',')]
                customers_list = [c for c in all_past if c.get('timing_segment', '') in segments]
        elif audience == 'past_attendees':
            customers_list = db.get_past_attendees_not_purchased(
                event_id, pattern_name, limit=10000,
                current_buyer_emails=current_buyers,
                exclude_event_ids=list(set(all_sibling_ids)))
        elif audience == 'city_prospects':
            past = db.get_past_attendees_not_purchased(
                event_id, pattern_name, limit=10000,
                current_buyer_emails=current_buyers,
                exclude_event_ids=list(set(all_sibling_ids)))
            exclude = {c['email'] for c in past}
            exclude.update(current_buyers)
            customers_list = db.get_city_prospects(event.get('city', ''), exclude_emails=exclude, limit=10000)
        elif audience == 'type_fans':
            past = db.get_past_attendees_not_purchased(
                event_id, pattern_name, limit=10000,
                current_buyer_emails=current_buyers,
                exclude_event_ids=list(set(all_sibling_ids)))
            exclude = {c['email'] for c in past}
            exclude.update(current_buyers)
            city_p = db.get_city_prospects(event.get('city', ''), exclude_emails=exclude, limit=10000)
            exclude.update({c['email'] for c in city_p})
            customers_list = db.get_type_prospects(event.get('event_type', ''), city=event.get('city', ''), exclude_emails=exclude, limit=10000)
        elif audience == 'at_risk':
            at_risk = db.get_at_risk_customers(min_orders=2, min_days_inactive=180)
            for c in at_risk:
                if c['email'] in current_buyers:
                    continue
                ecities = c.get('cities', '{}')
                etypes = c.get('event_types', '{}')
                if not (_json_key_match(ecities, event.get('city')) and _json_key_match(etypes, event.get('event_type'))):
                    continue
                customers_list.append(c)
        elif audience == 'all':
            past = db.get_past_attendees_not_purchased(
                event_id, pattern_name, limit=10000,
                current_buyer_emails=current_buyers,
                exclude_event_ids=list(set(all_sibling_ids)))
            seen = {c['email'] for c in past}
            seen.update(current_buyers)
            customers_list.extend(past)
            if event.get('city'):
                city_p = db.get_city_prospects(event['city'], exclude_emails=seen, limit=10000)
                customers_list.extend(city_p)
                seen.update({c['email'] for c in city_p})
            if event.get('event_type'):
                type_p = db.get_type_prospects(event['event_type'], city=event.get('city', ''), exclude_emails=seen, limit=10000)
                customers_list.extend(type_p)
                seen.update({c['email'] for c in type_p})
            at_risk = db.get_at_risk_customers(min_orders=2, min_days_inactive=180)
            for c in at_risk:
                if c['email'] not in seen:
                    ecities = c.get('cities', '{}')
                    etypes = c.get('event_types', '{}')
                    if _json_key_match(ecities, event.get('city')) and _json_key_match(etypes, event.get('event_type')):
                        customers_list.append(c)
        # Build CSV
        output = io.StringIO()
        fields = ['email', 'favorite_city', 'favorite_event_type', 'rfm_segment',
                  'total_orders', 'total_events', 'total_spent', 'ltv_score',
                  'days_since_last', 'last_order_date']
        writer = csv.DictWriter(output, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        for c in customers_list:
            writer.writerow(c)
        from flask import Response
        csv_data = output.getvalue()
        safe_name = re.sub(r'[^a-zA-Z0-9]', '_', event['name'])
        filename = f"{safe_name}_{audience}.csv"
        return Response(
            csv_data,
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename={filename}',
                     'Access-Control-Allow-Origin': '*'}
        )
    # === Overlap Analysis ===
    # In-memory cache so CSV export can reference same data
    _overlap_cache = {}

    def _build_overlap_data():
        """Core overlap computation — shared between /api/overlap and CSV export."""
        all_events = db.conn.execute("""
            SELECT e.event_id, e.name, e.event_type, e.city, e.event_date,
                   COUNT(DISTINCT o.email) as attendee_count
            FROM events e
            JOIN orders o ON e.event_id = o.event_id
            GROUP BY e.event_id
            HAVING attendee_count > 0
            ORDER BY e.city, e.event_date DESC
        """).fetchall()
        all_events = [dict(r) for r in all_events]

        # Normalize: group timed-entry slots into one event per pattern+date
        grouped = defaultdict(lambda: {'event_ids': [], 'name': '', 'event_type': '', 'city': '', 'event_date': ''})
        for ev in all_events:
            pattern = _normalize_event_pattern(ev['name'], include_season=True)
            ev_date = ev['event_date'][:10]
            key = f"{pattern}_{ev_date}"
            g = grouped[key]
            g['event_ids'].append(ev['event_id'])
            if not g['name'] or len(ev['name']) < len(g['name']):
                g['name'] = re.sub(r'\s*[-–]\s*\d{1,2}(:\d{2})?\s*(am|pm|AM|PM).*$', '', ev['name']).strip()
            g['event_type'] = ev['event_type'] or g['event_type']
            g['city'] = ev['city'] or g['city']
            g['event_date'] = ev_date

        # Further group by pattern+year (merge multi-day into one edition)
        editions = defaultdict(lambda: {'event_ids': [], 'name': '', 'event_type': '', 'city': '', 'year': ''})
        for key, g in grouped.items():
            pattern = key.rsplit('_', 1)[0]
            year = g['event_date'][:4]
            edition_key = f"{pattern}_{year}"
            ed = editions[edition_key]
            ed['event_ids'].extend(g['event_ids'])
            if not ed['name'] or len(g['name']) < len(ed['name']):
                ed['name'] = g['name']
            ed['event_type'] = g['event_type'] or ed['event_type']
            ed['city'] = g['city'] or ed['city']
            ed['year'] = year

        # Build email sets for each edition
        edition_list = []
        for ekey, ed in editions.items():
            placeholders = ','.join(['?'] * len(ed['event_ids']))
            rows = db.conn.execute(
                f"SELECT DISTINCT email FROM orders WHERE event_id IN ({placeholders})",
                ed['event_ids']
            ).fetchall()
            emails = set(r['email'] for r in rows)
            if len(emails) < 5:
                continue
            edition_list.append({
                'key': ekey,
                'name': ed['name'],
                'event_type': ed['event_type'],
                'city': ed['city'],
                'year': ed['year'],
                'attendee_count': len(emails),
                'emails': emails,
                'event_ids': ed['event_ids']
            })

        # Also build "all-time" editions: merge all years per event pattern per city
        alltime = defaultdict(lambda: {'emails': set(), 'name': '', 'event_type': '', 'city': '', 'years': set(), 'event_ids': []})
        for ed in edition_list:
            pattern = _normalize_event_pattern(ed['name'], include_season=True)
            key = f"{ed['city']}_{pattern}"
            at = alltime[key]
            at['emails'] |= ed['emails']
            at['years'].add(ed['year'])
            at['event_ids'].extend(ed['event_ids'])
            if not at['name'] or len(ed['name']) < len(at['name']):
                at['name'] = ed['name']
            at['event_type'] = ed['event_type'] or at['event_type']
            at['city'] = ed['city'] or at['city']

        alltime_list = []
        for key, at in alltime.items():
            alltime_list.append({
                'key': key,
                'name': at['name'],
                'event_type': at['event_type'],
                'city': at['city'],
                'years': sorted(at['years']),
                'attendee_count': len(at['emails']),
                'emails': at['emails'],
                'event_ids': at['event_ids']
            })

        # Group by city
        by_city = defaultdict(list)
        for at in alltime_list:
            by_city[at['city'] or 'Unknown'].append(at)

        # Calculate pairwise: overlap, gap A→B, gap B→A, retention year-over-year
        city_data = {}
        all_pairs = []
        pair_index = {}  # for CSV export lookups

        for city, city_events in sorted(by_city.items()):
            city_events.sort(key=lambda e: e['attendee_count'], reverse=True)
            pairs = []
            n = len(city_events)
            for i in range(n):
                for j in range(i + 1, n):
                    a = city_events[i]
                    b = city_events[j]
                    overlap_emails = a['emails'] & b['emails']
                    only_a_emails = a['emails'] - b['emails']
                    only_b_emails = b['emails'] - a['emails']
                    overlap_count = len(overlap_emails)
                    only_a = len(only_a_emails)
                    only_b = len(only_b_emails)

                    if overlap_count == 0 and only_a == 0 and only_b == 0:
                        continue

                    pct_of_a = round(overlap_count / a['attendee_count'] * 100, 1) if a['attendee_count'] > 0 else 0
                    pct_of_b = round(overlap_count / b['attendee_count'] * 100, 1) if b['attendee_count'] > 0 else 0
                    same_event = _normalize_event_pattern(a['name'], include_season=True) == _normalize_event_pattern(b['name'], include_season=True)

                    pair_id = f"{city}_{i}_{j}"
                    pair = {
                        'pair_id': pair_id,
                        'event_a': a['name'],
                        'event_a_type': a['event_type'],
                        'event_a_count': a['attendee_count'],
                        'event_a_years': ', '.join(a['years']),
                        'event_b': b['name'],
                        'event_b_type': b['event_type'],
                        'event_b_count': b['attendee_count'],
                        'event_b_years': ', '.join(b['years']),
                        'overlap_count': overlap_count,
                        'only_a_count': only_a,
                        'only_b_count': only_b,
                        'pct_of_a': pct_of_a,
                        'pct_of_b': pct_of_b,
                        'city': city,
                        'same_event': same_event,
                    }
                    # Generate actionable recommendation
                    if same_event:
                        pair['action'] = f"Retention: {pct_of_a}% came back. Target the {only_a} who didn't return."
                        pair['action_type'] = 'retention'
                    elif only_a > only_b:
                        pair['action'] = f"{only_a:,} people went to {a['name']} but NEVER {b['name']}. Push {b['name']} to this list."
                        pair['action_type'] = 'cross_sell_b'
                    else:
                        pair['action'] = f"{only_b:,} people went to {b['name']} but NEVER {a['name']}. Push {a['name']} to this list."
                        pair['action_type'] = 'cross_sell_a'

                    pairs.append(pair)
                    all_pairs.append(pair)
                    # Store email sets for CSV export
                    pair_index[pair_id] = {
                        'overlap': overlap_emails,
                        'only_a': only_a_emails,
                        'only_b': only_b_emails,
                        'event_a': a['name'],
                        'event_b': b['name']
                    }

            pairs.sort(key=lambda p: p['overlap_count'], reverse=True)
            city_data[city] = pairs

        all_pairs.sort(key=lambda p: p['overlap_count'], reverse=True)
        cross_type_pairs = [p for p in all_pairs if not p['same_event']]

        # Year-over-year retention for same events
        retention = []
        # Group edition_list by pattern+city for retention calc
        ret_groups = defaultdict(list)
        for ed in edition_list:
            pattern = _normalize_event_pattern(ed['name'], include_season=True)
            ret_groups[(ed['city'], pattern)].append(ed)
        for (city, pattern), eds in ret_groups.items():
            # Need at least 2 editions with DIFFERENT years
            unique_years = set(e['year'] for e in eds)
            if len(unique_years) < 2:
                continue
            # Merge editions that share the same year (multi-day events)
            by_year = defaultdict(lambda: {'emails': set(), 'name': '', 'year': '', 'event_ids': []})
            for e in eds:
                yr = by_year[e['year']]
                yr['emails'] |= e['emails']
                yr['year'] = e['year']
                yr['event_ids'].extend(e.get('event_ids', []))
                if not yr['name'] or len(e['name']) < len(yr['name']):
                    yr['name'] = e['name']
            years_sorted = sorted(by_year.values(), key=lambda y: y['year'])
            for k in range(len(years_sorted) - 1):
                prev = years_sorted[k]
                curr = years_sorted[k + 1]
                prev_count = len(prev['emails'])
                curr_count = len(curr['emails'])
                if prev_count < 5 or curr_count < 5:
                    continue
                retained = prev['emails'] & curr['emails']
                churned = prev['emails'] - curr['emails']
                new_attendees = curr['emails'] - prev['emails']
                retention.append({
                    'event': curr['name'],
                    'city': city,
                    'prev_year': prev['year'],
                    'curr_year': curr['year'],
                    'prev_count': prev_count,
                    'curr_count': curr_count,
                    'retained': len(retained),
                    'churned': len(churned),
                    'new_attendees': len(new_attendees),
                    'retention_pct': round(len(retained) / prev_count * 100, 1) if prev_count > 0 else 0,
                    'growth_pct': round((curr_count - prev_count) / prev_count * 100, 1) if prev_count > 0 else 0
                })
        retention.sort(key=lambda r: r['churned'], reverse=True)

        # Build heatmap matrix per city (all-time unique events)
        city_matrices = {}
        for city, city_events in by_city.items():
            if len(city_events) < 2:
                continue
            city_events_sorted = sorted(city_events, key=lambda e: e['attendee_count'], reverse=True)
            labels = [e['name'] for e in city_events_sorted]
            matrix = []
            gap_matrix = []  # shows the "target this many" number
            for i, a in enumerate(city_events_sorted):
                row = []
                gap_row = []
                for j, b in enumerate(city_events_sorted):
                    if i == j:
                        row.append(a['attendee_count'])
                        gap_row.append(0)
                    else:
                        overlap = len(a['emails'] & b['emails'])
                        gap = len(a['emails'] - b['emails'])
                        row.append(overlap)
                        gap_row.append(gap)
                matrix.append(row)
                gap_matrix.append(gap_row)
            city_matrices[city] = {
                'labels': labels,
                'matrix': matrix,
                'gap_matrix': gap_matrix,
                'counts': [e['attendee_count'] for e in city_events_sorted],
                'types': [e['event_type'] for e in city_events_sorted]
            }

        # Store in cache for CSV export
        _overlap_cache['pair_index'] = pair_index
        _overlap_cache['city_matrices'] = city_matrices
        _overlap_cache['by_city'] = by_city

        return {
            'cities': sorted(by_city.keys()),
            'pairs_by_city': city_data,
            'top_pairs': all_pairs[:50],
            'top_cross_type': cross_type_pairs[:30],
            'matrices': city_matrices,
            'retention': retention,
            'summary': {
                'total_events': len(alltime_list),
                'total_pairs': len(all_pairs),
                'cross_type_pairs': len(cross_type_pairs),
                'retention_pairs': len(retention),
                'cities_analyzed': len(by_city),
                'highest_overlap': all_pairs[0] if all_pairs else None,
                'highest_cross_type': cross_type_pairs[0] if cross_type_pairs else None,
                'best_retention': retention[0] if retention else None,
                'total_cross_sell_opportunities': sum(p['only_a_count'] + p['only_b_count'] for p in cross_type_pairs)
            }
        }

    @app.route('/api/overlap')
    def overlap_analysis():
        """Attendee overlap, gap audiences, retention, and cross-sell opportunities."""
        try:
            data = _build_overlap_data()
            return jsonify(data)
        except Exception as e:
            log.error(f"Overlap analysis error: {e}")
            import traceback; traceback.print_exc()
            return jsonify({'error': str(e)}), 500

    @app.route('/api/overlap-debug')
    def overlap_debug():
        """Debug: show edition grouping and retention pattern matching."""
        try:
            all_events = db.conn.execute("""
                SELECT e.event_id, e.name, e.event_type, e.city, e.event_date,
                       COUNT(DISTINCT o.email) as attendee_count
                FROM events e
                JOIN orders o ON e.event_id = o.event_id
                GROUP BY e.event_id
                HAVING attendee_count > 0
                ORDER BY e.city, e.name
            """).fetchall()
            all_events = [dict(r) for r in all_events]

            # Show pattern mapping for every event
            pattern_map = []
            for ev in all_events:
                pattern = _normalize_event_pattern(ev['name'], include_season=True)
                pattern_no_season = _normalize_event_pattern(ev['name'], include_season=False)
                year = ev['event_date'][:4]
                pattern_map.append({
                    'name': ev['name'],
                    'city': ev['city'],
                    'year': year,
                    'pattern_with_season': pattern,
                    'pattern_no_season': pattern_no_season,
                    'attendees': ev['attendee_count']
                })

            # Group by city+pattern to show which events would match for retention
            from collections import defaultdict
            ret_groups = defaultdict(list)
            for pm in pattern_map:
                key = f"{pm['city']}|{pm['pattern_with_season']}"
                ret_groups[key].append({'name': pm['name'], 'year': pm['year'], 'attendees': pm['attendees']})

            # Show groups with only 1 year (these are the ones MISSING from retention)
            single_year = {k: v for k, v in ret_groups.items() if len(set(e['year'] for e in v)) == 1}
            multi_year = {k: v for k, v in ret_groups.items() if len(set(e['year'] for e in v)) > 1}

            return jsonify({
                'total_events': len(all_events),
                'total_patterns': len(ret_groups),
                'multi_year_patterns': len(multi_year),
                'single_year_patterns': len(single_year),
                'multi_year': {k: v for k, v in sorted(multi_year.items())},
                'single_year_sample': dict(list(sorted(single_year.items()))[:30]),
                'pattern_map_sample': pattern_map[:50]
            })
        except Exception as e:
            import traceback; traceback.print_exc()
            return jsonify({'error': str(e)}), 500

    @app.route('/api/export/overlap-csv')
    def export_overlap_csv():
        """Download email lists for overlap gap audiences."""
        pair_id = request.args.get('pair_id', '')
        audience = request.args.get('audience', '')  # overlap, only_a, only_b
        city = request.args.get('city', '')
        row_idx = request.args.get('row', '')
        col_idx = request.args.get('col', '')

        if not _overlap_cache.get('pair_index'):
            _build_overlap_data()

        try:
            emails = set()
            filename = 'overlap_audience.csv'

            # Matrix cell export: row event attendees who HAVEN'T been to col event
            if city and row_idx and col_idx:
                row_i = int(row_idx)
                col_j = int(col_idx)
                by_city = _overlap_cache.get('by_city', {})
                city_events = by_city.get(city, [])
                city_events_sorted = sorted(city_events, key=lambda e: e['attendee_count'], reverse=True)
                if row_i < len(city_events_sorted) and col_j < len(city_events_sorted):
                    a = city_events_sorted[row_i]
                    b = city_events_sorted[col_j]
                    if row_i == col_j:
                        emails = a['emails']
                        filename = f"{a['name']}_all_attendees.csv"
                    else:
                        emails = a['emails'] - b['emails']
                        filename = f"{a['name']}_NOT_{b['name']}.csv"

            # Pair-based export
            elif pair_id and audience:
                pair_data = _overlap_cache.get('pair_index', {}).get(pair_id)
                if pair_data:
                    emails = pair_data.get(audience, set())
                    if audience == 'only_a':
                        filename = f"{pair_data['event_a']}_NOT_{pair_data['event_b']}.csv"
                    elif audience == 'only_b':
                        filename = f"{pair_data['event_b']}_NOT_{pair_data['event_a']}.csv"
                    else:
                        filename = f"{pair_data['event_a']}_AND_{pair_data['event_b']}.csv"

            if not emails:
                return jsonify({'error': 'No emails found for this audience'}), 404

            # Build CSV with customer data
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(['email', 'total_orders', 'total_spent', 'total_events', 'favorite_event_type', 'favorite_city', 'last_order_date'])
            for email in sorted(emails):
                cust = db.conn.execute(
                    "SELECT email, total_orders, total_spent, total_events, favorite_event_type, favorite_city, last_order_date FROM customers WHERE email = ?",
                    (email,)
                ).fetchone()
                if cust:
                    writer.writerow([cust['email'], cust['total_orders'], f"{cust['total_spent']:.2f}",
                                     cust['total_events'], cust['favorite_event_type'], cust['favorite_city'],
                                     cust['last_order_date']])
                else:
                    writer.writerow([email, '', '', '', '', '', ''])

            csv_data = output.getvalue()
            filename = re.sub(r'[^a-zA-Z0-9_\-.]', '_', filename)
            from flask import Response
            return Response(csv_data, mimetype='text/csv',
                            headers={'Content-Disposition': f'attachment; filename={filename}',
                                     'Access-Control-Allow-Origin': '*'})
        except Exception as e:
            log.error(f"Overlap CSV export error: {e}")
            return jsonify({'error': str(e)}), 500

    # === Pacing ===
    @app.route('/api/curves')
    def pacing_curves():
        """List all pacing curves."""
        curves = db.get_all_curves()
        return jsonify([c for c in curves if c])
    @app.route('/api/curves/<pattern>')
    def curve_detail(pattern: str):
        """Single curve detail."""
        curve = db.get_curve(pattern)
        if not curve:
            return jsonify({'error': 'Curve not found'}), 404
        return jsonify(curve)
    return app
# =============================================================================
# MAIN
# =============================================================================
class CraftDominant:
    """Main interface - everything unified."""
    def __init__(self, db_path: str = "craft_unified.db"):
        self.db = Database(db_path)
        self.engine = DecisionEngine(self.db)
        self._eventbrite = None
    def sync(self, api_key: str, years_back: int = 2) -> dict:
        """Sync everything from Eventbrite."""
        if not requests:
            return {'error': 'requests library not installed'}
        eb = EventbriteSync(api_key, self.db)
        return eb.sync_all(years_back)
    def sync_meta(self, access_token: str, ad_account_id: str) -> dict:
        """Sync ad spend from Meta Marketing API."""
        if not requests:
            return {'error': 'requests library not installed'}
        meta = MetaAdsSync(access_token, ad_account_id, self.db)
        all_events = self.db.get_events(upcoming_only=False)
        return meta.sync_all_events(all_events)
    def analyze(self, event_id: str = None) -> Any:
        """Analyze one event or portfolio."""
        if event_id:
            return self.engine.analyze_event(event_id)
        return self.engine.analyze_portfolio()
    def get_customer(self, email: str) -> Optional[Customer]:
        return self.db.get_customer(email)
    def get_high_value_customers(self, **kwargs) -> List[dict]:
        return self.db.get_high_value_customers(**kwargs)
    def get_at_risk_customers(self, **kwargs) -> List[dict]:
        return self.db.get_at_risk_customers(**kwargs)
    def serve(self, host: str = '0.0.0.0', port: int = 5000):
        """Start API server."""
        if not HAS_FLASK:
            print("Flask not installed. Run: pip install flask flask-cors")
            return
        app = create_app(self.db)
        print(f"\nCraft Dominant API running on http://{host}:{port}")
        print(f"   Dashboard: http://{host}:{port}/api/dashboard")
        print(f"   Customers: http://{host}:{port}/api/customers")
        print(f"   Events:    http://{host}:{port}/api/events\n")
        app.run(host=host, port=port)
    def print_report(self):
        """Print full portfolio report."""
        analyses = self.engine.analyze_portfolio()
        print("=" * 70)
        print("CRAFT DOMINANT - PORTFOLIO INTELLIGENCE")
        print(f"{datetime.now().strftime('%A, %B %d, %Y')}")
        print("=" * 70)
        # Summary
        total_tickets = sum(a.tickets_sold for a in analyses)
        total_revenue = sum(a.revenue for a in analyses)
        total_spend = sum(a.ad_spend for a in analyses)
        print(f"\nPORTFOLIO")
        print(f"   Events: {len(analyses)}")
        print(f"   Tickets: {total_tickets:,}")
        print(f"   Revenue: ${total_revenue:,.2f}")
        print(f"   Spend: ${total_spend:,.2f}")
        print(f"   CAC: ${total_spend/total_tickets:.2f}" if total_tickets > 0 else "")
        # Decisions
        decisions = {}
        for a in analyses:
            d = a.decision.value
            decisions[d] = decisions.get(d, 0) + 1
        print(f"\nDECISIONS")
        for d, count in decisions.items():
            print(f"   {d.upper()}: {count}")
        # Events
        print(f"\nEVENTS")
        print("-" * 70)
        for a in analyses:
            print(f"\n[{a.decision.value.upper()}] {a.event_name}")
            print(f"   {a.event_date[:10]} ({a.days_until}d) | {a.tickets_sold:,}/{a.capacity:,} ({a.sell_through:.1f}%)")
            if a.historical_median_at_point > 0:
                print(f"   Historical: {a.historical_median_at_point:.1f}% | Pace: {a.pace_vs_historical:+.0f}%")
                print(f"   Projected: {a.projected_final:,} [{a.projected_range[0]:,}-{a.projected_range[1]:,}]")
            print(f"   Spend: ${a.ad_spend:,.2f} | CAC: ${a.cac:.2f}")
            print(f"   {a.high_value_targets} high-value targets | {a.reactivation_targets} reactivation")
            print(f"   -> {a.rationale}")
        # Customer summary
        segments = self.db.get_segment_counts()
        total_customers = self.db.get_customer_count()
        print(f"\nCUSTOMERS")
        print("-" * 70)
        print(f"   Total: {total_customers:,}")
        for seg, count in sorted(segments.items(), key=lambda x: -x[1]):
            print(f"   {seg}: {count:,}")
        at_risk = self.db.get_at_risk_customers()
        if at_risk:
            at_risk_value = sum(c['total_spent'] for c in at_risk)
            print(f"\n   AT RISK: {len(at_risk)} customers (${at_risk_value:,.2f} historical)")
        print("\n" + "=" * 70)
def create_app_with_db(auto_sync: bool = True):
    """Factory function for gunicorn deployment. Auto-syncs on creation."""
    db = Database(os.environ.get('DB_PATH', 'craft_unified.db'))
    return create_app(db, auto_sync=auto_sync)
def main():
    if len(sys.argv) < 2:
        print("""
CRAFT DOMINANT - Unified Intelligence System
Commands:
    sync <api_key>       Sync all data from Eventbrite (events, orders, customers)
    sync <key> --years N Sync N years of history (default: 2)
    report               Print portfolio analysis report
    serve                Start API server for dashboard
    customer <email>     Show customer detail
First time:
    python craft_unified.py sync YOUR_EVENTBRITE_API_KEY --years 2
Then:
    python craft_unified.py report
    python craft_unified.py serve
        """)
        return
    cmd = sys.argv[1].lower()
    craft = CraftDominant()
    if cmd == 'sync':
        if len(sys.argv) < 3:
            print("Usage: sync <eventbrite_api_key> [--years N]")
            return
        api_key = sys.argv[2]
        years = 2
        if '--years' in sys.argv:
            try:
                years = int(sys.argv[sys.argv.index('--years') + 1])
            except:
                pass
        print(f"\nSyncing {years} years of data from Eventbrite...\n")
        result = craft.sync(api_key, years)
        print(f"\nEvents: {result.get('events', 0)}")
        print(f"Orders: {result.get('orders', 0)}")
        print(f"Customers: {result.get('customers', 0)}")
        print(f"Pacing curves: {result.get('curves', 0)}")
        if result.get('errors'):
            print(f"\nErrors: {len(result['errors'])}")
    elif cmd == 'report':
        craft.print_report()
    elif cmd == 'serve':
        craft.serve()
    elif cmd == 'customer':
        if len(sys.argv) < 3:
            print("Usage: customer <email>")
            return
        email = sys.argv[2]
        customer = craft.get_customer(email)
        if not customer:
            print(f"Customer not found: {email}")
            return
        print(f"\n{customer.email}")
        print(f"   Orders: {customer.total_orders} | Tickets: {customer.total_tickets}")
        print(f"   Total spent: ${customer.total_spent:,.2f}")
        print(f"   LTV Score: {customer.ltv_score:.1f}/100")
        print(f"   Segment: {customer.rfm_segment}")
        print(f"   Timing: {customer.timing_segment}")
        print(f"   Favorite type: {customer.favorite_event_type}")
        print(f"   Favorite city: {customer.favorite_city}")
        print(f"   Events: {', '.join(customer.events_attended[:5])}")
        if len(customer.events_attended) > 5:
            print(f"          + {len(customer.events_attended) - 5} more")
    else:
        print(f"Unknown command: {cmd}")
# =============================================================================
# MODULE-LEVEL APP FOR GUNICORN
# =============================================================================
# gunicorn craft_unified:app will use this.
# auto_sync=True starts Eventbrite sync in background immediately.
app = create_app_with_db(auto_sync=True)
if __name__ == "__main__":
    main()
