"""
Craft AI Campaign Engine
=========================
The brain + hands of the Craft Hospitality marketing system.

This module plugs into craft_unified.py and adds:
  1. Automated phase detection — watches every event, detects phase transitions
  2. Claude-powered campaign generation — writes emails using full event context
  3. SendGrid execution — sends approved campaigns, tracks performance
  4. Learning loop — post-campaign analysis feeds back into future generation
  5. Campaign approval queue — AI drafts, Sam approves with one click

Integration: Add two lines to craft_unified.py's create_app():
    from craft_engine import CraftCampaignEngine, register_engine_routes
    campaign_engine = CraftCampaignEngine(db, engine)
    register_engine_routes(app, campaign_engine)

Required env vars:
    ANTHROPIC_API_KEY  — Claude API key for campaign generation
    SENDGRID_API_KEY   — SendGrid API key for email sending
    SENDGRID_FROM_EMAIL — Sender address (default: hello@crafthospitality.com)
"""

import os
import json
import uuid
import hmac
import hashlib
import logging
import threading
import time
import re
from datetime import datetime, timedelta, date
from typing import Optional, Dict, List, Any, Tuple
from collections import defaultdict
from contextlib import contextmanager

log = logging.getLogger('craft.engine')

# =============================================================================
# SCHEMA — campaigns, sends, tracking, learnings
# =============================================================================
ENGINE_SCHEMA = """
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
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (event_id) REFERENCES events(event_id)
);
CREATE TABLE IF NOT EXISTS campaign_sends (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id TEXT NOT NULL,
    email TEXT NOT NULL,
    first_name TEXT DEFAULT '',
    sendgrid_message_id TEXT,
    sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'queued',
    opened_at TEXT,
    clicked_at TEXT,
    UNIQUE(campaign_id, email),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
);
CREATE TABLE IF NOT EXISTS email_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sendgrid_message_id TEXT,
    event_type TEXT NOT NULL,
    email TEXT NOT NULL,
    timestamp TEXT,
    url TEXT,
    raw_payload TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS suppressions (
    email TEXT PRIMARY KEY,
    reason TEXT DEFAULT 'unsubscribe',
    suppressed_at TEXT DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS system_learnings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    category TEXT,
    event_type TEXT,
    city TEXT,
    learning TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    data_points INTEGER DEFAULT 0,
    source_campaign_ids TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    is_active INTEGER DEFAULT 1
);
CREATE TABLE IF NOT EXISTS phase_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL,
    phase TEXT NOT NULL,
    triggered_at TEXT DEFAULT CURRENT_TIMESTAMP,
    campaigns_generated INTEGER DEFAULT 0,
    UNIQUE(event_id, phase)
);
CREATE INDEX IF NOT EXISTS idx_campaigns_event ON campaigns(event_id);
CREATE INDEX IF NOT EXISTS idx_campaigns_status ON campaigns(status);
CREATE INDEX IF NOT EXISTS idx_sends_campaign ON campaign_sends(campaign_id);
CREATE INDEX IF NOT EXISTS idx_sends_email ON campaign_sends(email);
CREATE INDEX IF NOT EXISTS idx_sends_msgid ON campaign_sends(sendgrid_message_id);
CREATE INDEX IF NOT EXISTS idx_email_events_msgid ON email_events(sendgrid_message_id);
"""

# =============================================================================
# MARKETING PHASES — from the spec, encoded as code not prompts
# =============================================================================
PHASES = [
    {'name': 'pre_launch',   'days_range': (100, 85), 'barrier': 'availability', 'channels': ['email'], 'max_frequency': 2},
    {'name': 'launch',       'days_range': (84, 70),  'barrier': 'concept',      'channels': ['email'], 'max_frequency': 2},
    {'name': 'momentum',     'days_range': (69, 35),  'barrier': 'social',       'channels': ['email'], 'max_frequency': 4},
    {'name': 'urgency',      'days_range': (34, 14),  'barrier': 'urgency',      'channels': ['email', 'sms'], 'max_frequency': 3},
    {'name': 'final_push',   'days_range': (13, 1),   'barrier': 'urgency',      'channels': ['email', 'sms'], 'max_frequency': 7},
    {'name': 'event_day',    'days_range': (0, 0),    'barrier': None,           'channels': ['social'], 'max_frequency': 0},
    {'name': 'post_event',   'days_range': (-1, -7),  'barrier': 'social',       'channels': ['email'], 'max_frequency': 1},
    {'name': 'reactivation', 'days_range': (-30, -60),'barrier': 'concept',      'channels': ['email'], 'max_frequency': 2},
]

TIMING_RULES = {
    'no_sunday_morning_after_alcohol': True,
    'post_event_send_day': 'tuesday',
    'post_event_send_hour': 10,
    'no_email_during_event': True,
    'sms_vip_only': True,
    'sms_window_start': 9,
    'sms_window_end': 21,
    'max_sms_per_contact_per_month': 2,
}


def get_phase(days_until: int) -> Optional[Dict]:
    """Determine marketing phase from days until event."""
    for p in PHASES:
        lo, hi = p['days_range']
        if hi <= days_until <= lo:
            return p
    if days_until > 100:
        return None  # Not on sale yet
    return None


# =============================================================================
# CLAUDE API CLIENT — direct HTTP, no SDK dependency
# =============================================================================
class ClaudeClient:
    """Minimal Anthropic Messages API client using requests."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.anthropic.com/v1/messages"

    def generate(self, system_prompt: str, user_prompt: str,
                 max_tokens: int = 4000, temperature: float = 0.7) -> Optional[str]:
        """Call Claude and return the text response."""
        try:
            import requests
        except ImportError:
            log.error("requests library required for Claude API")
            return None

        resp = requests.post(
            self.base_url,
            headers={
                'x-api-key': self.api_key,
                'anthropic-version': '2023-06-01',
                'content-type': 'application/json',
            },
            json={
                'model': self.model,
                'max_tokens': max_tokens,
                'temperature': temperature,
                'system': system_prompt,
                'messages': [{'role': 'user', 'content': user_prompt}],
            },
            timeout=60,
        )

        if resp.status_code != 200:
            log.error(f"Claude API error {resp.status_code}: {resp.text[:500]}")
            return None

        data = resp.json()
        content = data.get('content', [])
        if content and content[0].get('type') == 'text':
            return content[0]['text']
        return None

    def generate_json(self, system_prompt: str, user_prompt: str,
                      max_tokens: int = 4000, temperature: float = 0.5) -> Optional[Dict]:
        """Call Claude and parse JSON from the response."""
        text = self.generate(system_prompt, user_prompt, max_tokens, temperature)
        if not text:
            return None

        # Extract JSON from markdown code blocks if present
        json_match = re.search(r'```(?:json)?\s*\n(.*?)\n```', text, re.DOTALL)
        if json_match:
            text = json_match.group(1)

        try:
            return json.loads(text)
        except json.JSONDecodeError:
            # Try to find JSON object in the text
            brace_start = text.find('{')
            brace_end = text.rfind('}')
            if brace_start >= 0 and brace_end > brace_start:
                try:
                    return json.loads(text[brace_start:brace_end + 1])
                except json.JSONDecodeError:
                    pass
            log.error(f"Failed to parse Claude JSON response: {text[:200]}")
            return None


# =============================================================================
# SENDGRID CLIENT — direct HTTP, no SDK
# =============================================================================
class SendGridClient:
    """SendGrid v3 Mail Send API client."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.from_email = os.environ.get('SENDGRID_FROM_EMAIL', 'hello@crafthospitality.com')
        self.from_name = os.environ.get('SENDGRID_FROM_NAME', 'Craft Hospitality')

    def send(self, to_email: str, subject: str, html: str,
             categories: List[str] = None, custom_args: Dict = None) -> Optional[str]:
        """Send one email. Returns SendGrid message ID or None."""
        try:
            import requests
        except ImportError:
            return None

        payload = {
            "personalizations": [{"to": [{"email": to_email}], "custom_args": custom_args or {}}],
            "from": {"email": self.from_email, "name": self.from_name},
            "subject": subject,
            "content": [{"type": "text/html", "value": html}],
            "tracking_settings": {
                "click_tracking": {"enable": True},
                "open_tracking": {"enable": True},
            },
        }
        if categories:
            payload["categories"] = categories[:10]

        unsub = os.environ.get('SENDGRID_UNSUBSCRIBE_GROUP_ID')
        if unsub:
            payload["asm"] = {"group_id": int(unsub)}

        resp = requests.post(
            "https://api.sendgrid.com/v3/mail/send",
            headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
            json=payload,
            timeout=15,
        )

        if resp.status_code in (200, 201, 202):
            msg_id = resp.headers.get('X-Message-Id', '')
            return msg_id
        else:
            log.error(f"SendGrid {resp.status_code}: {resp.text[:300]}")
            return None


# =============================================================================
# THE SYSTEM PROMPT — Craft Hospitality's marketing DNA
# =============================================================================
SYSTEM_PROMPT = """You are the Craft Hospitality AI Marketing Engine. You generate email campaigns that sell tickets to food and beverage festivals.

ABOUT CRAFT HOSPITALITY:
- 30 events/year across 13 US markets (DC, Philly, NYC, Miami, Chicago, LA, Boston, Austin, Dallas, Seattle, SF, San Diego, London)
- Categories: coffee, wine, beer, cocktails, food
- ~100,000 buyer emails with full purchase history
- Mission: become the Live Nation of food & beverage live events

THE 5 BARRIERS TO PURCHASE (every email must address at least one):
1. AVAILABILITY — Am I free that day?
2. SOCIAL — Who is coming with me?
3. CONCEPT — Does this sound amazing?
4. VALUE — Is this worth the money?
5. URGENCY — Why do I need to act right now?

COPY RULES:
- Subject lines: UNDER 12 words. Always.
- Write as if a knowledgeable friend is recommending something, not a brand selling
- Lead with FOMO and specificity over generality
- Use real numbers: "273 tickets left" not "selling fast"
- Use real deadlines: "Price goes up Friday at midnight" not "soon"
- Never use fake urgency — real scarcity only
- Post-event tone: nostalgic, warm, personal — not salesy
- Group buyer messaging always includes a social hook

HARDCODED TIMING RULES:
- NEVER email people while they are at an event
- NEVER send email Sunday morning after alcohol events (hangover)
- Post-event emails send TUESDAY (recovery complete, nostalgic, at work)
- During events: IG stories only (targets outside audience, not attendees)

OUTPUT FORMAT:
Always return valid JSON with these exact fields:
{
  "subject_line": "under 12 words",
  "preview_text": "under 90 chars, complements subject line",
  "body_html": "full email HTML body (no wrapper — we add header/footer)",
  "cta_text": "button text",
  "cta_url": "eventbrite URL or crafthospitality.com URL",
  "barrier_addressed": "availability|social|concept|value|urgency",
  "strategic_reasoning": "2-3 sentences explaining why this campaign for this audience at this time",
  "predicted_open_rate": 0.25,
  "predicted_click_rate": 0.04,
  "confidence_score": 0.8,
  "segment_priority": "which sub-segment to prioritize and why"
}"""


# =============================================================================
# EMAIL TEMPLATE WRAPPER
# =============================================================================
def wrap_email(body_html: str, unsubscribe_url: str = "{{unsubscribe_url}}") -> str:
    """Wrap AI-generated body content in the Craft email frame."""
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<style>
body{{margin:0;padding:0;background:#f8f9fa;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;}}
.c{{max-width:600px;margin:0 auto;background:#fff;}}
.hd{{background:#1a1a2e;padding:28px 24px;text-align:center;}}
.hd h1{{color:#fff;margin:0;font-size:16px;letter-spacing:2px;text-transform:uppercase;}}
.bd{{padding:28px 24px;color:#333;line-height:1.6;font-size:16px;}}
.bd h2{{color:#1a1a2e;margin-top:0;}}
.cta{{display:inline-block;background:#e94560;color:#fff!important;padding:14px 32px;border-radius:6px;text-decoration:none;font-weight:600;font-size:16px;margin:16px 0;}}
.sp{{background:#f0f4f8;padding:16px;border-radius:8px;margin:16px 0;text-align:center;}}
.sp .n{{font-size:28px;font-weight:700;color:#1a1a2e;}}
.ft{{padding:24px;text-align:center;font-size:12px;color:#999;border-top:1px solid #eee;}}
.ft a{{color:#999;}}
</style></head>
<body><div class="c">
<div class="hd"><h1>Craft Hospitality</h1></div>
<div class="bd">{body_html}</div>
<div class="ft">
<p>Craft Hospitality &bull; <a href="https://crafthospitality.com">crafthospitality.com</a></p>
<p><a href="{unsubscribe_url}">Unsubscribe</a> &bull; <a href="https://crafthospitality.com/preferences">Email Preferences</a></p>
</div></div></body></html>"""


# =============================================================================
# CAMPAIGN ENGINE — the brain
# =============================================================================
class CraftCampaignEngine:
    """
    The AI-native campaign engine.

    Watches events → detects phases → calls Claude to generate campaigns →
    queues for approval → sends via SendGrid → tracks performance → learns.
    """

    def __init__(self, db, decision_engine=None):
        self.db = db
        self.decision_engine = decision_engine  # The existing DecisionEngine from craft_unified
        self._init_schema()

        # Initialize clients from env vars
        self._claude = None
        self._sendgrid = None

    def _init_schema(self):
        self.db.conn.executescript(ENGINE_SCHEMA)
        self.db.conn.commit()

    @property
    def claude(self) -> Optional[ClaudeClient]:
        if self._claude is None:
            key = os.environ.get('ANTHROPIC_API_KEY')
            if key:
                self._claude = ClaudeClient(key)
        return self._claude

    @property
    def sendgrid(self) -> Optional[SendGridClient]:
        if self._sendgrid is None:
            key = os.environ.get('SENDGRID_API_KEY')
            if key:
                self._sendgrid = SendGridClient(key)
        return self._sendgrid

    # ─────────────────────────────────────────────────────────
    # PHASE DETECTION — what phase is each event in?
    # ─────────────────────────────────────────────────────────

    def detect_phases(self) -> List[Dict]:
        """Check all active events (upcoming + recently past). Return events needing campaigns."""
        # Get upcoming events
        upcoming = self.db.get_events(upcoming_only=True)

        # Also get recent past events (for post_event + reactivation phases)
        # These phases need events that already happened
        all_events = self.db.get_events()
        today = date.today()
        recent_past = []
        for e in all_events:
            try:
                ed = datetime.fromisoformat(e['event_date'][:10]).date()
                days_ago = (today - ed).days
                if 1 <= days_ago <= 90:  # Past events within 90 days
                    recent_past.append(e)
            except (ValueError, TypeError):
                continue

        # Deduplicate by event_id
        seen = set()
        events = []
        for e in upcoming + recent_past:
            if e['event_id'] not in seen:
                seen.add(e['event_id'])
                events.append(e)

        needs_action = []

        for event in events:
            try:
                event_date = datetime.fromisoformat(event['event_date'][:10]).date()
            except (ValueError, TypeError):
                continue
            days_until = (event_date - today).days
            phase = get_phase(days_until)
            if not phase:
                continue

            # Check if we already generated for this event + phase
            existing = self.db.conn.execute(
                "SELECT id FROM phase_log WHERE event_id = ? AND phase = ?",
                (event['event_id'], phase['name'])
            ).fetchone()

            if existing:
                continue  # Already handled this phase transition

            # Check how many campaigns we've already sent in this phase
            sent_in_phase = self.db.conn.execute("""
                SELECT COUNT(*) as cnt FROM campaigns
                WHERE event_id = ? AND phase = ? AND status IN ('sent', 'approved', 'draft')
            """, (event['event_id'], phase['name'])).fetchone()

            if (sent_in_phase['cnt'] or 0) >= phase['max_frequency']:
                continue  # Already at max frequency for this phase

            needs_action.append({
                'event': event,
                'days_until': days_until,
                'phase': phase,
            })

        return needs_action

    # ─────────────────────────────────────────────────────────
    # CONTEXT BUILDER — assembles everything Claude needs
    # ─────────────────────────────────────────────────────────

    def _build_event_context(self, event: Dict, days_until: int, phase: Dict) -> str:
        """Build the full context prompt for Claude — everything about this event."""
        event_id = event['event_id']
        event_type = event.get('event_type', '')
        city = event.get('city', '')

        # Current sales state
        tickets = self.db.get_event_tickets(event_id)
        revenue = self.db.get_event_revenue(event_id)
        capacity = event.get('capacity', 0)
        sell_through = (tickets / capacity * 100) if capacity > 0 else 0

        # Buyer count and average ticket price
        buyers = self.db.get_event_buyers(event_id)
        avg_price = revenue / tickets if tickets > 0 else 0

        # Historical comparison — what did past editions look like at this point?
        historical_context = ""
        if self.decision_engine:
            try:
                pattern = self.decision_engine._get_pattern(event['name'])
                past_ids = self.db.get_pattern_event_ids(pattern, exclude_ids=[event_id])
                if past_ids:
                    past_data = []
                    for pid in past_ids[:3]:  # Last 3 editions
                        pe = self.db.get_event(pid)
                        if pe:
                            pt = self.db.get_event_tickets(pid)
                            pr = self.db.get_event_revenue(pid)
                            past_data.append(f"  - {pe['name']}: {pt:,} tickets, ${pr:,.0f} revenue, {pe.get('capacity',0):,} cap")
                    if past_data:
                        historical_context = "PAST EDITIONS:\n" + "\n".join(past_data)
            except Exception as e:
                log.warning(f"Historical context error: {e}")

        # Audience segments available
        segment_context = ""
        try:
            # Past attendees not yet purchased
            past_attendees = self.db.get_past_attendees_not_purchased(
                event_id, event['name'], limit=5000,
                current_buyer_emails=buyers
            )
            champions = [c for c in past_attendees if c.get('rfm_segment') in ('champion', 'loyal')]
            at_risk = [c for c in past_attendees if c.get('rfm_segment') == 'at_risk']

            # City + type prospects
            city_count = 0
            type_count = 0
            if city:
                city_prospects = self.db.get_city_prospects(city, exclude_emails=buyers, limit=5000)
                city_count = len(city_prospects)
            if event_type:
                type_prospects = self.db.get_type_prospects(event_type, city=city, exclude_emails=buyers, limit=5000)
                type_count = len(type_prospects)

            segment_context = f"""AVAILABLE AUDIENCES:
  - Past attendees (not yet purchased): {len(past_attendees):,} people
    - Champions/Loyal: {len(champions):,}
    - At-risk/Lapsing: {len(at_risk):,}
    - Other segments: {len(past_attendees) - len(champions) - len(at_risk):,}
  - City prospects ({city}): {city_count:,} people (bought other events in {city})
  - Category fans ({event_type}): {type_count:,} people (attend {event_type} events elsewhere)
  - Current buyers: {len(buyers):,}"""
        except Exception as e:
            log.warning(f"Segment context error: {e}")

        # Active learnings for this event type + city
        learnings_context = ""
        try:
            learnings = self.db.conn.execute("""
                SELECT learning, confidence FROM system_learnings
                WHERE is_active = 1
                AND (event_type IS NULL OR event_type = ? OR event_type = '')
                AND (city IS NULL OR city = ? OR city = '')
                ORDER BY confidence DESC LIMIT 10
            """, (event_type, city)).fetchall()
            if learnings:
                learnings_context = "LEARNINGS FROM PAST CAMPAIGNS:\n" + "\n".join(
                    f"  - {l['learning']} (confidence: {l['confidence']:.0%})" for l in learnings
                )
        except Exception:
            pass

        # Velocity
        velocity_context = ""
        try:
            snaps = self.db.get_snapshots(event_id)
            if len(snaps) >= 2:
                recent = snaps[:7]
                if len(recent) >= 2:
                    daily_vel = (recent[0]['tickets_cumulative'] - recent[-1]['tickets_cumulative']) / max(1, len(recent))
                    velocity_context = f"VELOCITY: {daily_vel:.1f} tickets/day over last {len(recent)} days"
        except Exception:
            pass

        return f"""EVENT: {event['name']}
DATE: {event['event_date'][:10]}
CITY: {city}
TYPE: {event_type}
DAYS UNTIL EVENT: {days_until}
MARKETING PHASE: {phase['name']}
BARRIER TO ADDRESS: {phase['barrier']}

CURRENT STATE:
  Tickets sold: {tickets:,} / {capacity:,} capacity ({sell_through:.1f}% sell-through)
  Revenue: ${revenue:,.0f}
  Average ticket price: ${avg_price:.0f}
  Current buyers: {len(buyers):,}
{velocity_context}

{historical_context}

{segment_context}

{learnings_context}

PHASE GUIDANCE ({phase['name']}):
  Max campaigns this phase: {phase['max_frequency']}
  Channels available: {', '.join(phase['channels'])}
  Primary barrier: {phase['barrier']}

Generate ONE email campaign for this event right now. Target the highest-priority audience segment.
The email should feel like it's from a friend who goes to these events, not a marketing department.
Use real numbers from the data above. Be specific about what makes THIS event worth attending."""

    # ─────────────────────────────────────────────────────────
    # CAMPAIGN GENERATION — Claude writes the email
    # ─────────────────────────────────────────────────────────

    def generate_campaign(self, event: Dict, days_until: int, phase: Dict) -> Optional[Dict]:
        """Use Claude to generate a campaign for an event in a specific phase."""
        if not self.claude:
            log.error("ANTHROPIC_API_KEY not set — cannot generate campaigns")
            return None

        context = self._build_event_context(event, days_until, phase)
        result = self.claude.generate_json(SYSTEM_PROMPT, context, max_tokens=4000, temperature=0.7)

        if not result:
            log.error(f"Claude returned no result for {event['name']}")
            return None

        # Build the segment SQL based on phase and Claude's recommendation
        event_id = event['event_id']
        event_type = event.get('event_type', '')
        city = event.get('city', '')
        segment_sql = self._build_segment_sql(phase, event_id, event_type, city)

        # Count the audience
        audience_count = 0
        try:
            row = self.db.conn.execute(f"SELECT COUNT(*) as cnt FROM ({segment_sql})").fetchone()
            audience_count = row['cnt'] if row else 0
        except Exception as e:
            log.warning(f"Audience count failed: {e}")

        # Wrap the body HTML in our email template
        body_html = result.get('body_html', '')
        full_html = wrap_email(body_html)

        # Calculate send time
        send_at = self._calculate_send_time(phase, event)

        # Create the campaign record
        campaign_id = str(uuid.uuid4())[:12]
        cta_url = result.get('cta_url', '')
        if not cta_url:
            cta_url = f"https://www.eventbrite.com/e/{event_id}"

        # Add UTM tracking
        if '?' in cta_url:
            cta_url += f"&utm_source=craft_ai&utm_medium=email&utm_campaign={campaign_id}"
        else:
            cta_url += f"?utm_source=craft_ai&utm_medium=email&utm_campaign={campaign_id}"

        self.db.conn.execute("""
            INSERT INTO campaigns (id, event_id, campaign_type, phase, subject_line, preview_text,
                body_html, cta_text, cta_url, segment_name, segment_sql, audience_count,
                scheduled_send_at, status, barrier_addressed, confidence_score,
                strategic_reasoning, predicted_open_rate, predicted_click_rate,
                predicted_revenue)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'draft', ?, ?, ?, ?, ?, ?)
        """, (
            campaign_id, event_id, phase['name'], phase['name'],
            result.get('subject_line', f'{event["name"]} — tickets available'),
            result.get('preview_text', ''),
            full_html,
            result.get('cta_text', 'Get Tickets'),
            cta_url,
            result.get('segment_priority', f'{phase["name"]} audience'),
            segment_sql,
            audience_count,
            send_at,
            result.get('barrier_addressed', phase['barrier']),
            result.get('confidence_score', 0.5),
            result.get('strategic_reasoning', ''),
            result.get('predicted_open_rate', 0.2),
            result.get('predicted_click_rate', 0.03),
            result.get('predicted_revenue', 0),
        ))

        # Log the phase transition
        self.db.conn.execute("""
            INSERT OR IGNORE INTO phase_log (event_id, phase, campaigns_generated)
            VALUES (?, ?, 1)
        """, (event_id, phase['name']))

        self.db.conn.commit()

        log.info(f"Campaign generated: {campaign_id} for {event['name']} [{phase['name']}] → {audience_count} recipients")

        return {
            'campaign_id': campaign_id,
            'event_name': event['name'],
            'phase': phase['name'],
            'subject': result.get('subject_line'),
            'audience_count': audience_count,
            'status': 'draft',
        }

    @staticmethod
    def _sql_escape(val: str) -> str:
        """Escape a string for safe use in SQL (prevent injection via event data)."""
        return val.replace("'", "''") if val else ''

    def _build_segment_sql(self, phase: Dict, event_id: str, event_type: str, city: str) -> str:
        """Build the SQL query for the target audience based on marketing phase.

        Returns a self-contained SQL query that can be stored and executed later.
        Excludes current buyers and suppressed (bounced/unsubscribed) emails.
        """
        # Escape all values that go into SQL strings
        eid = self._sql_escape(event_id)
        etype = self._sql_escape(event_type)
        cty = self._sql_escape(city)

        base_exclude = f"""
            email NOT IN (SELECT email FROM orders WHERE event_id = '{eid}')
            AND email NOT IN (SELECT email FROM suppressions)
        """

        if phase['name'] == 'pre_launch':
            # Smallest, most loyal audience — champions & loyal who match type + city
            return f"""
                SELECT DISTINCT c.email FROM customers c
                WHERE c.favorite_city = '{cty}' AND c.event_types LIKE '%{etype}%'
                AND c.rfm_segment IN ('champion', 'loyal')
                AND {base_exclude}
            """
        elif phase['name'] == 'launch':
            # Full city buyer list — everyone who's bought anything in this city
            return f"""
                SELECT DISTINCT c.email FROM customers c
                WHERE c.favorite_city = '{cty}'
                AND {base_exclude}
            """
        elif phase['name'] == 'momentum':
            # Broader: city buyers + category fans
            return f"""
                SELECT DISTINCT c.email FROM customers c
                WHERE (c.favorite_city = '{cty}' OR c.event_types LIKE '%{etype}%')
                AND {base_exclude}
            """
        elif phase['name'] in ('urgency', 'final_push'):
            # Broadest reach: anyone connected to this city or category
            return f"""
                SELECT DISTINCT c.email FROM customers c
                WHERE (c.favorite_city = '{cty}'
                    OR c.cities LIKE '%{cty}%'
                    OR c.event_types LIKE '%{etype}%')
                AND {base_exclude}
            """
        elif phase['name'] == 'post_event':
            # Buyers of this event — thank you / recap email
            return f"""
                SELECT DISTINCT email FROM orders WHERE event_id = '{eid}'
            """
        elif phase['name'] == 'reactivation':
            # Lapsed customers in this city
            return f"""
                SELECT DISTINCT c.email FROM customers c
                WHERE c.favorite_city = '{cty}'
                AND c.days_since_last > 60
                AND c.rfm_segment IN ('at_risk', 'hibernating')
                AND {base_exclude}
            """
        else:
            # Fallback: city buyers
            return f"""
                SELECT DISTINCT c.email FROM customers c
                WHERE c.favorite_city = '{cty}'
                AND {base_exclude}
            """

    def _calculate_send_time(self, phase: Dict, event: Dict) -> str:
        """Calculate the optimal send time respecting timing rules."""
        now = datetime.now()
        event_type = event.get('event_type', '')

        if phase['name'] == 'post_event':
            # Tuesday 10am after the event
            event_date = datetime.fromisoformat(event['event_date'][:10])
            days_until_tuesday = (1 - event_date.weekday()) % 7
            if days_until_tuesday == 0:
                days_until_tuesday = 7
            send = (event_date + timedelta(days=days_until_tuesday)).replace(hour=10, minute=0)
            return send.isoformat()

        # Default: tomorrow at 10am ET (good baseline)
        send = (now + timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0)

        # Don't send Sunday morning after alcohol events
        if send.weekday() == 6 and event_type in ('wine', 'beer', 'cocktails'):
            send += timedelta(days=1)  # Push to Monday

        return send.isoformat()

    # ─────────────────────────────────────────────────────────
    # RUN CYCLE — detect all phases, generate all campaigns
    # ─────────────────────────────────────────────────────────

    def run_cycle(self) -> List[Dict]:
        """Run a full campaign generation cycle. Call this on a cron or manually.
        Detects all phase transitions, generates campaigns for each, returns results.
        """
        needs_action = self.detect_phases()
        results = []

        for item in needs_action:
            try:
                result = self.generate_campaign(
                    event=item['event'],
                    days_until=item['days_until'],
                    phase=item['phase'],
                )
                if result:
                    results.append(result)
            except Exception as e:
                log.error(f"Campaign generation failed for {item['event']['name']}: {e}")
                results.append({
                    'event_name': item['event']['name'],
                    'phase': item['phase']['name'],
                    'error': str(e),
                })

        return results

    # ─────────────────────────────────────────────────────────
    # CAMPAIGN EXECUTION — approve and send
    # ─────────────────────────────────────────────────────────

    def approve(self, campaign_id: str, approved_by: str = 'sam') -> Dict:
        self.db.conn.execute("""
            UPDATE campaigns SET status = 'approved', approved_by = ?, approved_at = ?, updated_at = ?
            WHERE id = ? AND status = 'draft'
        """, (approved_by, datetime.now().isoformat(), datetime.now().isoformat(), campaign_id))
        self.db.conn.commit()
        return {'campaign_id': campaign_id, 'status': 'approved'}

    def reject(self, campaign_id: str) -> Dict:
        self.db.conn.execute("""
            UPDATE campaigns SET status = 'rejected', updated_at = ?
            WHERE id = ? AND status = 'draft'
        """, (datetime.now().isoformat(), campaign_id))
        self.db.conn.commit()
        return {'campaign_id': campaign_id, 'status': 'rejected'}

    def send_campaign(self, campaign_id: str, dry_run: bool = False) -> Dict:
        """Execute a campaign: pull audience from SQL, send each email via SendGrid.

        Args:
            campaign_id: ID of an approved campaign
            dry_run: If True, validates everything but doesn't actually send emails
        """
        if not dry_run and not self.sendgrid:
            return {'error': 'SENDGRID_API_KEY not set'}

        row = self.db.conn.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,)).fetchone()
        if not row:
            return {'error': 'Campaign not found'}
        campaign = dict(row)

        if not dry_run and campaign['status'] not in ('approved',):
            return {'error': f"Campaign status is '{campaign['status']}', must be 'approved'"}

        # Mark sending
        if not dry_run:
            self.db.conn.execute("UPDATE campaigns SET status = 'sending', updated_at = ? WHERE id = ?",
                                 (datetime.now().isoformat(), campaign_id))
            self.db.conn.commit()

        # Pull audience
        try:
            recipients = self.db.conn.execute(campaign['segment_sql']).fetchall()
        except Exception as e:
            if not dry_run:
                self.db.conn.execute("UPDATE campaigns SET status = 'error', updated_at = ? WHERE id = ?",
                                    (datetime.now().isoformat(), campaign_id))
                self.db.conn.commit()
            return {'error': f'Segment SQL failed: {e}'}

        if dry_run:
            return {
                'campaign_id': campaign_id,
                'status': 'dry_run',
                'audience_count': len(recipients),
                'sample_recipients': [r['email'] if hasattr(r, 'keys') else r[0] for r in recipients[:10]],
                'subject_line': campaign['subject_line'],
                'phase': campaign['phase'],
            }

        sent = 0
        failed = 0
        batch_size = 50  # Commit to DB every N sends for crash recovery

        for i, r in enumerate(recipients):
            email = r['email'] if hasattr(r, 'keys') else r[0]

            # Personalize (no first_name in current schema — use friendly fallback)
            subject = campaign['subject_line'].replace('{{first_name}}', 'there')
            html = campaign['body_html'].replace('{{first_name}}', 'there')

            try:
                msg_id = self.sendgrid.send(
                    to_email=email,
                    subject=subject,
                    html=html,
                    categories=[campaign['campaign_type'], campaign['phase'] or '', campaign_id],
                    custom_args={'campaign_id': campaign_id, 'event_id': campaign['event_id']},
                )
            except Exception as e:
                log.error(f"SendGrid error for {email}: {e}")
                msg_id = None

            # Record the send
            status = 'sent' if msg_id else 'failed'
            try:
                self.db.conn.execute("""
                    INSERT OR IGNORE INTO campaign_sends (campaign_id, email, first_name, sendgrid_message_id, status)
                    VALUES (?, ?, ?, ?, ?)
                """, (campaign_id, email, '', msg_id or '', status))
            except Exception as e:
                log.error(f"DB insert error for send record: {e}")

            if msg_id:
                sent += 1
            else:
                failed += 1

            # Batch commit for crash recovery
            if (i + 1) % batch_size == 0:
                self.db.conn.commit()

            # Brief pause every 100 sends to respect SendGrid rate limits
            if (i + 1) % 100 == 0:
                time.sleep(0.5)

        # Final update
        self.db.conn.execute("""
            UPDATE campaigns SET status = 'sent', sends = ?, sent_at = ?, updated_at = ?
            WHERE id = ?
        """, (sent, datetime.now().isoformat(), datetime.now().isoformat(), campaign_id))
        self.db.conn.commit()

        log.info(f"Campaign {campaign_id} sent: {sent} delivered, {failed} failed out of {len(recipients)} recipients")
        return {'campaign_id': campaign_id, 'sent': sent, 'failed': failed, 'total_recipients': len(recipients), 'status': 'sent'}

    # ─────────────────────────────────────────────────────────
    # WEBHOOK PROCESSING — track opens, clicks, bounces
    # ─────────────────────────────────────────────────────────

    def process_sendgrid_events(self, events: List[Dict]) -> Dict:
        """Process SendGrid webhook events."""
        processed = 0
        for ev in events:
            etype = ev.get('event', '')
            email = ev.get('email', '').lower()
            msg_id = ev.get('sg_message_id', '').split('.')[0]
            ts = ev.get('timestamp', '')

            if not email or not etype:
                continue

            self.db.conn.execute("""
                INSERT INTO email_events (sendgrid_message_id, event_type, email, timestamp, url, raw_payload)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (msg_id, etype, email, ts, ev.get('url', ''), json.dumps(ev)))

            if etype == 'open':
                self.db.conn.execute(
                    "UPDATE campaign_sends SET status = 'opened', opened_at = ? WHERE sendgrid_message_id = ? AND status IN ('sent','delivered')",
                    (ts, msg_id))
            elif etype == 'click':
                self.db.conn.execute(
                    "UPDATE campaign_sends SET status = 'clicked', clicked_at = ? WHERE sendgrid_message_id = ?",
                    (ts, msg_id))
            elif etype in ('bounce', 'dropped'):
                self.db.conn.execute("UPDATE campaign_sends SET status = 'bounced' WHERE sendgrid_message_id = ?", (msg_id,))
                self.db.conn.execute("INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'bounce')", (email,))
            elif etype in ('unsubscribe', 'spamreport'):
                self.db.conn.execute("INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, ?)", (email, etype))

            processed += 1

        self.db.conn.commit()
        self._refresh_campaign_metrics()
        return {'processed': processed}

    def _refresh_campaign_metrics(self):
        """Recalc campaign-level open/click counts from sends table."""
        self.db.conn.execute("""
            UPDATE campaigns SET
                opens = (SELECT COUNT(*) FROM campaign_sends WHERE campaign_id = campaigns.id AND status IN ('opened','clicked')),
                clicks = (SELECT COUNT(*) FROM campaign_sends WHERE campaign_id = campaigns.id AND status = 'clicked'),
                updated_at = ?
            WHERE status = 'sent'
        """, (datetime.now().isoformat(),))
        self.db.conn.commit()

    # ─────────────────────────────────────────────────────────
    # LEARNING LOOP — analyze sent campaigns, extract insights
    # ─────────────────────────────────────────────────────────

    def analyze_campaign(self, campaign_id: str) -> Optional[Dict]:
        """Post-campaign analysis using Claude. Call 48h after sending."""
        if not self.claude:
            return None

        campaign = dict(self.db.conn.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,)).fetchone())
        if not campaign or campaign['status'] != 'sent':
            return None

        # Build performance context
        open_rate = campaign['opens'] / campaign['sends'] * 100 if campaign['sends'] > 0 else 0
        click_rate = campaign['clicks'] / campaign['sends'] * 100 if campaign['sends'] > 0 else 0

        # Compare to predicted
        predicted_open = (campaign.get('predicted_open_rate') or 0) * 100
        predicted_click = (campaign.get('predicted_click_rate') or 0) * 100

        prompt = f"""Analyze this campaign's performance and extract learnings.

CAMPAIGN:
  Event: {campaign['event_id']}
  Type: {campaign['campaign_type']}
  Phase: {campaign['phase']}
  Subject: {campaign['subject_line']}
  Barrier: {campaign['barrier_addressed']}

PERFORMANCE:
  Sent: {campaign['sends']:,}
  Opens: {campaign['opens']:,} ({open_rate:.1f}% — predicted {predicted_open:.1f}%)
  Clicks: {campaign['clicks']:,} ({click_rate:.1f}% — predicted {predicted_click:.1f}%)

Provide 2-3 specific, actionable learnings as JSON:
{{
  "learnings": [
    {{"category": "copy|timing|segment", "learning": "specific actionable insight", "confidence": 0.0-1.0}}
  ],
  "what_worked": "1 sentence",
  "what_to_improve": "1 sentence"
}}"""

        result = self.claude.generate_json(
            "You are a data-driven email marketing analyst. Extract specific, actionable learnings from campaign performance data. Be concise.",
            prompt,
            max_tokens=1000,
            temperature=0.3,
        )

        if result and result.get('learnings'):
            event = self.db.get_event(campaign['event_id'])
            for learning in result['learnings']:
                self.db.conn.execute("""
                    INSERT INTO system_learnings (category, event_type, city, learning, confidence, data_points, source_campaign_ids)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    learning.get('category', 'general'),
                    event.get('event_type', '') if event else '',
                    event.get('city', '') if event else '',
                    learning.get('learning', ''),
                    learning.get('confidence', 0.5),
                    campaign['sends'],
                    json.dumps([campaign_id]),
                ))
            self.db.conn.commit()
            log.info(f"Extracted {len(result['learnings'])} learnings from campaign {campaign_id}")

        return result

    # ─────────────────────────────────────────────────────────
    # QUEUE MANAGEMENT
    # ─────────────────────────────────────────────────────────

    def get_queue(self, status: str = None) -> List[Dict]:
        if status:
            rows = self.db.conn.execute(
                "SELECT * FROM campaigns WHERE status = ? ORDER BY created_at DESC", (status,)
            ).fetchall()
        else:
            rows = self.db.conn.execute("""
                SELECT * FROM campaigns
                ORDER BY CASE status WHEN 'draft' THEN 1 WHEN 'approved' THEN 2 WHEN 'sent' THEN 3 ELSE 4 END,
                created_at DESC
            """).fetchall()
        return [dict(r) for r in rows]

    def get_performance_summary(self) -> Dict:
        """Overall campaign performance across all sent campaigns."""
        rows = self.db.conn.execute("""
            SELECT campaign_type,
                   COUNT(*) as campaigns,
                   SUM(sends) as total_sends,
                   SUM(opens) as total_opens,
                   SUM(clicks) as total_clicks,
                   AVG(CASE WHEN sends > 0 THEN opens * 1.0 / sends END) as avg_open_rate,
                   AVG(CASE WHEN sends > 0 THEN clicks * 1.0 / sends END) as avg_click_rate
            FROM campaigns WHERE status = 'sent' GROUP BY campaign_type
        """).fetchall()
        return [dict(r) for r in rows]


# =============================================================================
# BACKGROUND AUTOMATION — runs the cycle on a schedule
# =============================================================================
def start_campaign_scheduler(engine: CraftCampaignEngine, interval_hours: int = 6):
    """Background thread that runs the campaign generation cycle periodically.

    Waits 60s after startup before first run (let Eventbrite sync complete first).
    On error, backs off exponentially up to 1 hour before retrying.
    """
    def _loop():
        # Wait for initial sync to populate events
        time.sleep(60)
        backoff = 0

        while True:
            try:
                log.info("Campaign scheduler: running cycle...")
                results = engine.run_cycle()
                if results:
                    log.info(f"Campaign scheduler: generated {len(results)} campaigns")
                    for r in results:
                        if 'error' in r:
                            log.warning(f"  FAILED: {r.get('event_name')} — {r['error']}")
                        else:
                            log.info(f"  OK: {r.get('event_name')} [{r.get('phase')}] → {r.get('audience_count')} recipients")
                else:
                    log.info("Campaign scheduler: no phase transitions detected")
                backoff = 0  # Reset on success
            except Exception as e:
                log.error(f"Campaign scheduler error: {e}", exc_info=True)
                backoff = min(backoff + 1, 6)  # Max 6 = 2^6 * 60 ~= 1 hour

            sleep_seconds = interval_hours * 3600
            if backoff > 0:
                sleep_seconds = min(sleep_seconds, (2 ** backoff) * 60)
                log.info(f"Campaign scheduler: backing off {sleep_seconds}s after error")

            time.sleep(sleep_seconds)

    t = threading.Thread(target=_loop, daemon=True, name='campaign-scheduler')
    t.start()
    log.info(f"Campaign scheduler started (every {interval_hours}h, first run in 60s)")
    return t


# =============================================================================
# FLASK ROUTES
# =============================================================================
def register_engine_routes(app, engine: CraftCampaignEngine):
    """Register all campaign engine routes on the Flask app."""
    from flask import request, jsonify

    @app.route('/api/campaigns')
    def list_campaigns():
        status = request.args.get('status')
        return jsonify(engine.get_queue(status))

    @app.route('/api/campaigns/<cid>')
    def get_campaign(cid):
        row = engine.db.conn.execute("SELECT * FROM campaigns WHERE id = ?", (cid,)).fetchone()
        if not row:
            return jsonify({'error': 'Not found'}), 404
        return jsonify(dict(row))

    @app.route('/api/campaigns/<cid>/approve', methods=['POST'])
    def approve_campaign(cid):
        return jsonify(engine.approve(cid))

    @app.route('/api/campaigns/<cid>/reject', methods=['POST'])
    def reject_campaign(cid):
        return jsonify(engine.reject(cid))

    @app.route('/api/campaigns/<cid>/send', methods=['POST'])
    def send_campaign(cid):
        return jsonify(engine.send_campaign(cid))

    @app.route('/api/campaigns/<cid>/dry-run', methods=['POST'])
    def dry_run_campaign(cid):
        """Validate a campaign without sending — shows audience count and sample recipients."""
        return jsonify(engine.send_campaign(cid, dry_run=True))

    @app.route('/api/campaigns/<cid>/analyze', methods=['POST'])
    def analyze_campaign(cid):
        result = engine.analyze_campaign(cid)
        if result:
            return jsonify(result)
        return jsonify({'error': 'Analysis failed or campaign not eligible'}), 400

    @app.route('/api/campaigns/generate', methods=['POST'])
    def generate_campaigns():
        """Manually trigger a campaign generation cycle."""
        results = engine.run_cycle()
        return jsonify({'generated': len(results), 'campaigns': results})

    @app.route('/api/campaigns/generate/<event_id>', methods=['POST'])
    def generate_for_event(event_id):
        """Generate a campaign for a specific event (force, regardless of phase log)."""
        event = engine.db.get_event(event_id)
        if not event:
            return jsonify({'error': 'Event not found'}), 404
        days_until = (datetime.fromisoformat(event['event_date'][:10]).date() - date.today()).days
        phase = get_phase(days_until)
        if not phase:
            return jsonify({'error': f'Event is {days_until} days out — no active marketing phase'}), 400
        result = engine.generate_campaign(event, days_until, phase)
        if result:
            return jsonify(result)
        return jsonify({'error': 'Generation failed — check ANTHROPIC_API_KEY'}), 500

    @app.route('/api/campaigns/performance')
    def campaign_performance():
        return jsonify(engine.get_performance_summary())

    @app.route('/api/webhook/sendgrid', methods=['POST'])
    def sendgrid_webhook():
        events = request.get_json() or []
        result = engine.process_sendgrid_events(events)
        return jsonify(result)

    @app.route('/api/learnings')
    def list_learnings():
        rows = engine.db.conn.execute(
            "SELECT * FROM system_learnings WHERE is_active = 1 ORDER BY confidence DESC"
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    @app.route('/api/suppressions')
    def list_suppressions():
        rows = engine.db.conn.execute("SELECT * FROM suppressions ORDER BY suppressed_at DESC LIMIT 100").fetchall()
        return jsonify([dict(r) for r in rows])

    @app.route('/api/engine/status')
    def engine_status():
        """Health check for the campaign engine — shows config status and counts."""
        try:
            campaign_counts = {}
            for status in ('draft', 'approved', 'sent', 'rejected', 'error'):
                row = engine.db.conn.execute(
                    "SELECT COUNT(*) as cnt FROM campaigns WHERE status = ?", (status,)
                ).fetchone()
                campaign_counts[status] = row['cnt'] if row else 0

            suppression_count = engine.db.conn.execute("SELECT COUNT(*) as cnt FROM suppressions").fetchone()
            learning_count = engine.db.conn.execute("SELECT COUNT(*) as cnt FROM system_learnings WHERE is_active = 1").fetchone()

            return jsonify({
                'status': 'ok',
                'claude_configured': bool(os.environ.get('ANTHROPIC_API_KEY')),
                'sendgrid_configured': bool(os.environ.get('SENDGRID_API_KEY')),
                'campaigns': campaign_counts,
                'total_suppressions': suppression_count['cnt'] if suppression_count else 0,
                'active_learnings': learning_count['cnt'] if learning_count else 0,
            })
        except Exception as e:
            return jsonify({'status': 'error', 'message': str(e)}), 500

    @app.route('/api/phases')
    def list_phases():
        """Show current phase status for all upcoming events."""
        events = engine.db.get_events(upcoming_only=True)
        result = []
        for event in events:
            days = (datetime.fromisoformat(event['event_date']).date() - date.today()).days
            phase = get_phase(days)
            # Check if campaign already exists for this phase
            existing = engine.db.conn.execute(
                "SELECT id, status, subject_line FROM campaigns WHERE event_id = ? AND phase = ? ORDER BY created_at DESC LIMIT 1",
                (event['event_id'], phase['name'] if phase else '')
            ).fetchone()
            result.append({
                'event_id': event['event_id'],
                'event_name': event['name'],
                'days_until': days,
                'phase': phase['name'] if phase else 'not_on_sale',
                'barrier': phase['barrier'] if phase else None,
                'campaign_exists': bool(existing),
                'campaign_status': existing['status'] if existing else None,
                'campaign_subject': existing['subject_line'] if existing else None,
            })
        return jsonify(result)

    return engine
