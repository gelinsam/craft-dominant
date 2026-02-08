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
                      sort_by: str = 'ltv_score', order: str = 'DESC') -> List[dict]:
        query = "SELECT * FROM customers WHERE 1=1"
        params = []

        if segment:
            query += " AND rfm_segment = ?"
            params.append(segment)

        if min_ltv is not None:
            query += " AND ltv_score >= ?"
            params.append(min_ltv)

        # Validate sort column
        valid_sorts = ['ltv_score', 'total_spent', 'total_orders', 'days_since_last', 'total_events']
        if sort_by not in valid_sorts:
            sort_by = 'ltv_score'

        order = 'DESC' if order.upper() == 'DESC' else 'ASC'
        query += f" ORDER BY {sort_by} {order} LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        rows = self.conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]

    def get_customer_count(self, segment: str = None) -> int:
        query = "SELECT COUNT(*) as cnt FROM customers"
        params = []
        if segment:
            query += " WHERE rfm_segment = ?"
            params.append(segment)
        row = self.conn.execute(query, params).fetchone()
        return row['cnt'] if row else 0

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
                orders = self._paginate(f"/events/{event['event_id']}/orders/", {})

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

    def _parse_event(self, data: dict) -> Optional[dict]:
        event_id = data.get('id')
        if not event_id:
            return None

        name = data.get('name', {})
        if isinstance(name, dict):
            name = name.get('text', '')

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
        gross_amount = float(gross.get('major_value', 0))

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

            tickets_today = sum(o.get('ticket_count', 1) for o in day_orders)
            revenue_today = sum(o.get('gross_amount', 0) for o in day_orders)

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
            reactivation_targets=at_risk
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
                    "🚨 PAUSE underperforming ad campaigns",
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

    def analyze_portfolio(self) -> List[EventPacing]:
        """Analyze all upcoming events."""
        events = self.db.get_events(upcoming_only=True)
        analyses = []

        for event in events:
            analysis = self.analyze_event(event['event_id'])
            if analysis:
                analyses.append(analysis)

        # Sort by urgency
        analyses.sort(key=lambda x: (-x.urgency, x.days_until))
        return analyses


# =============================================================================
# FLASK API
# =============================================================================

def create_app(db: Database) -> Flask:
    """Create Flask app with all endpoints."""
    app = Flask(__name__)
    CORS(app)

    engine = DecisionEngine(db)

    @app.route('/api/health')
    def health():
        return jsonify({'status': 'ok', 'time': datetime.now().isoformat()})

    @app.route('/api/sync')
    def sync_endpoint():
        """Trigger Eventbrite sync via URL."""
        api_key = os.environ.get('EVENTBRITE_API_KEY')
        if not api_key:
            return jsonify({'error': 'EVENTBRITE_API_KEY not set'}), 500

        try:
            eb = EventbriteSync(api_key, db)
            result = eb.sync_all(years_back=2)
            return jsonify({
                'status': 'success',
                'events': result.get('events', 0),
                'orders': result.get('orders', 0),
                'customers': result.get('customers', 0),
                'curves': result.get('curves', 0),
                'errors': len(result.get('errors', []))
            })
        except Exception as e:
            return jsonify({'error': str(e)}), 500

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
            'events': [asdict(a) for a in analyses],
            'customers': {
                'total': total_customers,
                'segments': segments
            },
            'updated_at': datetime.now().isoformat()
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
        return jsonify(asdict(analysis))

    # === CRM ===

    @app.route('/api/customers')
    def customers():
        """List customers with filters."""
        segment = request.args.get('segment')
        min_ltv = request.args.get('min_ltv', type=float)
        limit = request.args.get('limit', 100, type=int)
        offset = request.args.get('offset', 0, type=int)
        sort_by = request.args.get('sort', 'ltv_score')
        order = request.args.get('order', 'DESC')

        customers = db.get_customers(segment, min_ltv, limit, offset, sort_by, order)
        total = db.get_customer_count(segment)

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
        print(f"\n🚀 Craft Dominant API running on http://{host}:{port}")
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

        print(f"\n📊 PORTFOLIO")
        print(f"   Events: {len(analyses)}")
        print(f"   Tickets: {total_tickets:,}")
        print(f"   Revenue: ${total_revenue:,.2f}")
        print(f"   Spend: ${total_spend:,.2f}")
        print(f"   CAC: ${total_spend/total_tickets:.2f}" if total_tickets > 0 else "")

        # Decisions
        emoji = {'pivot': '🚨', 'push': '🚀', 'maintain': '✅', 'coast': '😎', 'not_started': '⏳'}
        decisions = {}
        for a in analyses:
            d = a.decision.value
            decisions[d] = decisions.get(d, 0) + 1

        print(f"\n📋 DECISIONS")
        for d, count in decisions.items():
            print(f"   {emoji.get(d, '❓')} {d.upper()}: {count}")

        # Events
        print(f"\n📈 EVENTS")
        print("-" * 70)

        for a in analyses:
            e = emoji.get(a.decision.value, '❓')
            print(f"\n{e} {a.event_name}")
            print(f"   📅 {a.event_date[:10]} ({a.days_until}d) | {a.tickets_sold:,}/{a.capacity:,} ({a.sell_through:.1f}%)")

            if a.historical_median_at_point > 0:
                print(f"   📊 Historical: {a.historical_median_at_point:.1f}% | Pace: {a.pace_vs_historical:+.0f}%")
                print(f"   📈 Projected: {a.projected_final:,} [{a.projected_range[0]:,}-{a.projected_range[1]:,}]")

            print(f"   💰 Spend: ${a.ad_spend:,.2f} | CAC: ${a.cac:.2f}")
            print(f"   🎯 {a.high_value_targets} high-value targets | {a.reactivation_targets} reactivation")
            print(f"   ➡️ {a.rationale}")

        # Customer summary
        segments = self.db.get_segment_counts()
        total_customers = self.db.get_customer_count()

        print(f"\n👥 CUSTOMERS")
        print("-" * 70)
        print(f"   Total: {total_customers:,}")
        for seg, count in sorted(segments.items(), key=lambda x: -x[1]):
            print(f"   {seg}: {count:,}")

        at_risk = self.db.get_at_risk_customers()
        if at_risk:
            at_risk_value = sum(c['total_spent'] for c in at_risk)
            print(f"\n   ⚠️ AT RISK: {len(at_risk)} customers (${at_risk_value:,.2f} historical)")

        print("\n" + "=" * 70)


def create_app_with_db():
    """Factory function for gunicorn deployment."""
    db = Database(os.environ.get('DB_PATH', 'craft_unified.db'))
    return create_app(db)


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

        print(f"\n✓ Events: {result.get('events', 0)}")
        print(f"✓ Orders: {result.get('orders', 0)}")
        print(f"✓ Customers: {result.get('customers', 0)}")
        print(f"✓ Pacing curves: {result.get('curves', 0)}")

        if result.get('errors'):
            print(f"\n⚠️ Errors: {len(result['errors'])}")

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

        print(f"\n👤 {customer.email}")
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

if __name__ == "__main__":
    main()
