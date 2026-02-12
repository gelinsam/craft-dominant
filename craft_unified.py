import os
import sys
import json
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
        name_lower = name.lower()
        name_lower = re.sub(r'20\d{2}', '', name_lower)
        replacements = {
            'philadelphia': 'philly', 'washington dc': 'dc', 'district': 'dc',
            'new york': 'nyc', 'los angeles': 'la', 'san francisco': 'sf', 'san diego': 'sd'
        }
        for old, new in replacements.items():
            name_lower = name_lower.replace(old, new)
        season = ''
        if 'winter' in name_lower: season = '_winter'
        elif 'spring' in name_lower: season = '_spring'
        elif 'fall' in name_lower: season = '_fall'
        name_lower = re.sub(r'[^a-z\s]', '', name_lower)
        name_lower = '_'.join(name_lower.split())
        name_lower = re.sub(r'_edition|_+', '_', name_lower).strip('_')
        return name_lower + season
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
        """Same logic as EventbriteSync."""
        name_lower = name.lower()
        name_lower = re.sub(r'20\d{2}', '', name_lower)
        replacements = {
            'philadelphia': 'philly', 'washington dc': 'dc', 'district': 'dc',
            'new york': 'nyc', 'los angeles': 'la', 'san francisco': 'sf', 'san diego': 'sd'
        }
        for old, new in replacements.items():
            name_lower = name_lower.replace(old, new)
        season = ''
        if 'winter' in name_lower: season = '_winter'
        elif 'spring' in name_lower: season = '_spring'
        elif 'fall' in name_lower: season = '_fall'
        name_lower = re.sub(r'[^a-z\s]', '', name_lower)
        name_lower = '_'.join(name_lower.split())
        name_lower = re.sub(r'_edition|_+', '_', name_lower).strip('_')
        return name_lower + season
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
            # Find closest day in curve
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
                # Projection
                if curve['avg_final_sell_through'] > 0:
                    pace_mult = sell_through / hist_median if hist_median > 0 else 1
                    proj_pct = curve['avg_final_sell_through'] * pace_mult
                    projected_final = int(capacity * proj_pct / 100)
                    projected_final = max(tickets, min(capacity, projected_final))
                    low_mult = sell_through / hist_range[1] if hist_range[1] > 0 else pace_mult
                    high_mult = sell_through / hist_range[0] if hist_range[0] > 0 else pace_mult
                    proj_low = int(capacity * curve['avg_final_sell_through'] * low_mult / 100)
                    proj_high = int(capacity * curve['avg_final_sell_through'] * high_mult / 100)
                    projected_range = (
                        max(tickets, min(capacity, proj_low)),
                        max(tickets, min(capacity, proj_high))
                    )
                samples = point.get('samples', 1)
                confidence = 0.9 if samples >= 3 else 0.75 if samples >= 2 else 0.5
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
        historical_comparisons = []
        all_events = self.db.get_events()
        past_by_date = defaultdict(list)
        for pe in all_events:
            if self._get_pattern(pe['name']) != pattern:
                continue
            pe_date = datetime.fromisoformat(pe['event_date']).date()
            is_current = any(pe['event_id'] == a.event_id for a in all_analyses_for_pattern)
            if is_current:
                continue
            past_by_date[(pe_date.year, pe_date)].append(pe)
        for (year, pdate), events_on_date in past_by_date.items():
            pd_weekday = pdate.strftime("%A")
            if pd_weekday != day_name:
                continue
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
        proj_finals = [a.projected_final for a in day_analyses]
        proj_ranges = [a.projected_range for a in day_analyses]
        hist_medians = [a.historical_median_at_point for a in day_analyses if a.historical_median_at_point > 0]
        hist_lo = [a.historical_range[0] for a in day_analyses if a.historical_range[0] > 0]
        hist_hi = [a.historical_range[1] for a in day_analyses if a.historical_range[1] > 0]
        return EventPacing(
            event_id=eid, event_name=logical_name,
            event_date=day_analyses[0].event_date, days_until=days_until,
            tickets_sold=total_tickets, capacity=max_capacity,
            revenue=total_revenue, ad_spend=total_spend,
            sell_through=round(sell_through, 1), cac=round(cac_val, 2),
            historical_median_at_point=sum(hist_medians) if hist_medians else 0,
            historical_range=(sum(hist_lo) if hist_lo else 0, sum(hist_hi) if hist_hi else 0),
            pace_vs_historical=0,
            comparison_events=[c['event_name'] for c in historical_comparisons],
            comparison_years=[c['year'] for c in historical_comparisons],
            projected_final=sum(proj_finals),
            projected_range=(sum(r[0] for r in proj_ranges), sum(r[1] for r in proj_ranges)),
            confidence=max(a.confidence for a in day_analyses) if day_analyses else 0.5,
            decision=best_decision, urgency=best_urgency,
            rationale=best_rationale, actions=best_actions,
            high_value_targets=max(a.high_value_targets for a in day_analyses) if day_analyses else 0,
            reactivation_targets=max(a.reactivation_targets for a in day_analyses) if day_analyses else 0,
            historical_comparisons=historical_comparisons,
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
            result = eb.sync_all(years_back=2)
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
        # === Dashboard ===
    @app.route('/api/dashboard')
    def dashboard():
        """Complete dashboard data."""
        analyses = engine.analyze_portfolio()
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
