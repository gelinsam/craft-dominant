"""
Craft AI Campaign Engine
=========================
The brain + hands of the Craft Hospitality marketing system.

This module plugs into craft_unified.py and adds:
  1. Automated phase detection — watches every event, detects phase transitions
  2. Claude-powered campaign generation — writes emails using full event context
  3. Mailchimp execution — sends approved campaigns via Mailchimp, tracks performance
  4. Learning loop — post-campaign analysis feeds back into future generation
  5. Campaign approval queue — AI drafts, Sam approves with one click

Integration: Add two lines to craft_unified.py's create_app():
    from craft_engine import CraftCampaignEngine, register_engine_routes
    campaign_engine = CraftCampaignEngine(db, engine)
    register_engine_routes(app, campaign_engine)

Required env vars:
    ANTHROPIC_API_KEY    — Claude API key for campaign generation
    MAILCHIMP_API_KEY    — Mailchimp API key (format: xxx-us16)
    MAILCHIMP_AUDIENCE_ID — Mailchimp Audience/List ID
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
from suppression_guard import SuppressionGuard
from provider_outcome import (
    ProviderResponse,
    ProviderError,
    ProviderTransportError,
    ProviderHTTPError,
    ProviderMalformedResponseError,
    ProviderSendOutcome,
    ProviderSendStatus,
    ProviderCreateOutcome,
    ProviderCreateStatus,
    classify_send_http_error,
)

log = logging.getLogger('craft.engine')

# =============================================================================
# MAILCHIMP WEBHOOK AUTHENTICITY
# =============================================================================
# Mailchimp's Marketing API guide "Synchronize Audience Data with Webhooks"
# documents optional HMAC signing for audience webhooks:
#
#   X-Mailchimp-Signature: t=<unix_seconds>,v1=<64 hex chars>
#   v1 = HMAC-SHA256(key=signing_secret, message="{t}.{raw_body}")
#
# with three rules that are easy to get wrong and are therefore spelled out
# here: verify against the RAW body (parsing or URL-decoding first changes the
# bytes), compare in constant time, and reject anything outside a five-minute
# window.
#
# Scope of that window, stated precisely because it is easy to overclaim: it is
# a FRESHNESS check, not deduplication. It bounds how long a captured delivery
# stays replayable to five minutes; it does not detect the same valid delivery
# arriving twice inside that window. Suppression inserts are idempotent: they
# use INSERT OR IGNORE keyed on email, and the sentinel recomputes the count.
# However, the processor also appends email_events rows; duplicate deliveries
# can duplicate telemetry. This window does not deduplicate those writes or
# guarantee exactly-once processing.

# "reject ... where the timestamp is more than 5 minutes old" — Mailchimp.
MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS = 300

_MC_SIG_TIMESTAMP = re.compile(r'(?:\A|,)\s*t=(\d{1,20})\s*(?=,|\Z)')
_MC_SIG_V1 = re.compile(r'(?:\A|,)\s*v1=([0-9a-fA-F]{64})\s*(?=,|\Z)')


def verify_mailchimp_signature(signing_secret: str, signature_header: str,
                               raw_body: bytes,
                               tolerance_seconds: int = MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS,
                               now: Optional[float] = None) -> Tuple[bool, str]:
    """Verify an X-Mailchimp-Signature header against the raw request body.

    Returns (ok, reason). The reason is safe to log and to return to the
    caller: it names the failure class, never any part of the secret or the
    expected signature.
    """
    if not signing_secret:
        return False, 'signing_secret_not_configured'
    if not signature_header:
        return False, 'missing_signature_header'

    ts_match = _MC_SIG_TIMESTAMP.search(signature_header)
    sig_match = _MC_SIG_V1.search(signature_header)
    if not ts_match or not sig_match:
        # Also catches an unsupported scheme version: a header carrying only
        # v2=... has no v1 to verify, and must not be treated as signed.
        return False, 'malformed_signature_header'

    try:
        timestamp = int(ts_match.group(1))
    except ValueError:
        return False, 'malformed_signature_header'

    current = time.time() if now is None else now
    age = current - timestamp
    if age > tolerance_seconds:
        return False, 'stale_timestamp'
    if age < -tolerance_seconds:
        # A timestamp far in the future is equally not a live delivery.
        return False, 'timestamp_in_future'

    if not isinstance(raw_body, (bytes, bytearray)):
        raw_body = str(raw_body).encode()

    signed_payload = f'{timestamp}.'.encode() + bytes(raw_body)
    expected = hmac.new(signing_secret.encode(), signed_payload,
                        hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected, sig_match.group(1).lower()):
        return False, 'signature_mismatch'
    return True, 'ok'


def normalize_mailchimp_form(form) -> Dict[str, Any]:
    """Turn Mailchimp's form-encoded delivery into the nested dict we consume.

    Mailchimp posts application/x-www-form-urlencoded, not JSON, and expresses
    nesting with bracket notation:

        type=unsubscribe&data[email]=a@b.com&data[merges][FNAME]=Ada

    A flat ``form.to_dict()`` leaves the key as the literal string
    ``data[email]``, so a consumer reading ``payload['data']['email']`` finds
    nothing -- and the handler happily reports success while recording no
    suppression. That is how a genuine unsubscribe gets lost silently, which is
    worse than rejecting it, so the shape is reconstructed here.
    """
    out: Dict[str, Any] = {}
    if not form:
        return out
    for key, value in form.items():
        head, _, rest = key.partition('[')
        if not rest:
            out[key] = value
            continue
        # data[merges][FNAME] -> ['merges', 'FNAME']
        parts = [p for p in re.findall(r'\[([^\[\]]*)\]', '[' + rest) if p != '']
        node = out.setdefault(head, {})
        for part in parts[:-1]:
            nxt = node.get(part)
            if not isinstance(nxt, dict):
                nxt = {}
                node[part] = nxt
            node = nxt
        if parts:
            node[parts[-1]] = value
        else:
            out[head] = value
    return out


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
    mailchimp_campaign_id TEXT DEFAULT '',
    sent_at TEXT DEFAULT CURRENT_TIMESTAMP,
    status TEXT DEFAULT 'queued',
    opened_at TEXT,
    clicked_at TEXT,
    UNIQUE(campaign_id, email),
    FOREIGN KEY (campaign_id) REFERENCES campaigns(id)
);
CREATE TABLE IF NOT EXISTS email_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    mailchimp_campaign_id TEXT DEFAULT '',
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
CREATE INDEX IF NOT EXISTS idx_sends_mc_campaign ON campaign_sends(mailchimp_campaign_id);
CREATE INDEX IF NOT EXISTS idx_email_events_mc_campaign ON email_events(mailchimp_campaign_id);
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

    def __init__(self, api_key: str, model: str = "claude-sonnet-5"):
        self.api_key = api_key
        self.model = model
        self.base_url = "https://api.anthropic.com/v1/messages"

    def generate(self, system_prompt: str, user_prompt: str,
                 max_tokens: int = 4000, temperature: float = 0.7) -> Optional[str]:
        """Call Claude and return text. Legacy temperature argument is ignored.

        Current Claude models reject non-default sampling parameters. Keep the
        Python argument for existing callers, but omit it from the API request.
        """
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
# MAILCHIMP CLIENT — direct HTTP, no SDK
# =============================================================================
class MailchimpClient:
    """Mailchimp Marketing API v3 client.

    Handles: member management, tag-based segmentation, campaign creation & sending.
    All campaigns are sent through Mailchimp so they show up in the Mailchimp dashboard
    with full open/click tracking, unsubscribe handling, and CAN-SPAM compliance.
    """

    def __init__(self, api_key: str, audience_id: str):
        self.api_key = api_key
        self.audience_id = audience_id
        # Extract data center from API key (format: xxx-us16)
        self.dc = api_key.split('-')[-1] if '-' in api_key else 'us16'
        self.base_url = f"https://{self.dc}.api.mailchimp.com/3.0"
        self.from_email = os.environ.get('MAILCHIMP_FROM_EMAIL', 'tickets@crafthospitality.com')
        self.from_name = os.environ.get('MAILCHIMP_FROM_NAME', 'Craft Hospitality')

    def _headers(self) -> Dict:
        return {'Content-Type': 'application/json'}

    def _auth(self) -> Tuple:
        return ('anystring', self.api_key)

    def _request_strict(self, method: str, path: str, data: Dict = None,
                        timeout: int = 30) -> ProviderResponse:
        """Authenticated Mailchimp request that PRESERVES failure kind.

        This is the single HTTP implementation.  Unlike ``_request`` it
        never collapses distinct failures into one sentinel value:

          transport failure (timeout, reset, DNS, socket)
              → ProviderTransportError    — proves NOTHING about whether
                                            the provider acted
          non-2xx HTTP response
              → ProviderHTTPError(status) — caller decides, per
                                            operation, whether that code
                                            proves rejection
          2xx with unparseable body
              → ProviderMalformedResponseError — provider accepted it
          2xx (including 204 No Content)
              → ProviderResponse(body={} when there is no content)

        Any code deciding whether an external send happened MUST use
        this method, never ``_request``.
        """
        try:
            import requests as req
        except ImportError:
            # No transport available: nothing was ever dispatched, so
            # this is a definite local failure, not provider ambiguity.
            raise ProviderHTTPError(
                "requests library required for Mailchimp API",
                http_status=0,
                detail="requests_not_installed",
            )

        url = f"{self.base_url}{path}"

        try:
            resp = req.request(
                method, url,
                auth=self._auth(),
                headers=self._headers(),
                json=data,
                timeout=timeout,
            )
        except Exception as e:
            # The request may or may not have reached Mailchimp, and if
            # it did, Mailchimp may or may not have acted on it.
            error_type = type(e).__name__
            log.error(
                f"Mailchimp {method} {path} → transport failure "
                f"({error_type}) — outcome UNKNOWN"
            )
            raise ProviderTransportError(
                f"Transport failure during {method} {path}: {error_type}",
                error_type=error_type,
            ) from e

        status = resp.status_code

        if status in (200, 201, 204):
            if not resp.content:
                # 204 No Content is the documented success shape for
                # several action endpoints, including campaign send.
                return ProviderResponse(http_status=status, body={})
            try:
                parsed = resp.json()
            except Exception as e:
                log.error(
                    f"Mailchimp {method} {path} → {status} with unparseable "
                    f"body — outcome UNKNOWN"
                )
                raise ProviderMalformedResponseError(
                    f"Unparseable 2xx body from {method} {path}: "
                    f"{type(e).__name__}",
                    http_status=status,
                ) from e
            if not isinstance(parsed, dict):
                parsed = {"_raw": parsed}
            return ProviderResponse(http_status=status, body=parsed)

        # Non-2xx: surface the code so the caller can classify it.
        detail = resp.text[:500] if resp.text else ""
        log.error(f"Mailchimp {method} {path} → {status}: {detail}")
        raise ProviderHTTPError(
            f"Mailchimp {method} {path} returned HTTP {status}",
            http_status=status,
            detail=detail,
        )

    def _request(self, method: str, path: str, data: Dict = None,
                 timeout: int = 30) -> Optional[Dict]:
        """LEGACY lossy wrapper — returns a dict on success, else None.

        WARNING: ``None`` conflates "Mailchimp rejected this" with
        "we never found out".  That distinction is load-bearing for
        anything that mutates customer-visible state, so this method
        MUST NOT be used to decide whether an email was sent.
        Use ``_request_strict`` there.

        Retained unchanged in behaviour for read paths (suppression
        sync, segment lookup, reports) where a lost response and a
        rejection are both simply "no authoritative answer", and both
        correctly fail closed.
        """
        try:
            return self._request_strict(method, path, data, timeout).body
        except ProviderHTTPError:
            return None
        except (ProviderTransportError, ProviderMalformedResponseError):
            # Historically these propagated. Callers of the legacy
            # contract treat None as "no authoritative answer", which is
            # the correct fail-closed reading for read-only paths.
            return None

    # ── Member management ──────────────────────────────────

    def ensure_members(self, emails: List[str], tag: str = None) -> Dict:
        """Require existing consent in this audience; never subscribe buyers.

        Ticket purchase history is targeting evidence, not permission to add a
        person to a different festival's mailing list. Any unknown membership
        blocks preparation before campaign creation.
        """
        expected = {e.lower().strip() for e in emails if e and e.strip()}
        subscribed = self._get_members_by_status('subscribed')
        if subscribed is None or not expected or not expected.issubset(set(subscribed)):
            raise RuntimeError('Audience consent is missing or could not be verified')
        if tag:
            self.tag_members(sorted(expected), tag)
        return {'added': 0, 'updated': len(expected), 'errors': 0}

    def tag_members(self, emails: List[str], tag: str) -> bool:
        """Create a fresh exact segment and verify membership before sending.

        Never merge an earlier attempt's segment: it may contain new buyers
        that the current audience deliberately excluded. Transport uncertainty
        may leave an unused segment, but cannot authorize a campaign.
        """
        expected = sorted({e.lower().strip() for e in emails})
        if not expected:
            raise RuntimeError('Empty segment is not sendable')
        result = self._request_strict('POST', f'/lists/{self.audience_id}/segments', {
            'name': tag, 'static_segment': expected[:500],
        }).body
        segment_id = result.get('id')
        if not isinstance(segment_id, int) or isinstance(segment_id, bool) or segment_id <= 0:
            raise RuntimeError('Mailchimp did not confirm a segment ID')
        for offset in range(500, len(expected), 500):
            self._request_strict('POST', f'/lists/{self.audience_id}/segments/{segment_id}', {
                'members_to_add': expected[offset:offset + 500],
            })
        actual = set()
        offset = 0
        while True:
            page = self._request('GET',
                f'/lists/{self.audience_id}/segments/{segment_id}/members?count=1000&offset={offset}',
                timeout=60)
            if not isinstance(page, dict) or not isinstance(page.get('members'), list):
                raise RuntimeError('Segment membership could not be verified')
            if page.get('total_items') != len(expected):
                raise RuntimeError('Segment membership count does not match approved audience')
            members = page['members']
            for member in members:
                email = member.get('email_address', '').lower().strip()
                if member.get('status') != 'subscribed' or not email or email in actual:
                    raise RuntimeError('Segment contains unverified subscribers')
                actual.add(email)
            offset += len(members)
            if offset == len(expected):
                break
            if not members or offset > len(expected):
                raise RuntimeError('Segment membership pagination incomplete')
        if actual != set(expected):
            raise RuntimeError('Segment membership does not match approved audience')
        if not hasattr(self, '_verified_segments'):
            self._verified_segments = {}
        self._verified_segments[tag] = segment_id
        return True

    # ── Campaign creation & sending ────────────────────────

    def create_campaign_strict(self, subject: str, preview_text: str, html: str,
                               tag: str = None, segment_id: int = None,
                               campaign_title: str = '') -> ProviderCreateOutcome:
        """Create a campaign, returning a lossless outcome.

        Two distinct uncertainty modes exist here:

        1. The POST /campaigns response is lost.  A campaign may now
           exist at Mailchimp whose ID we never learned.  We cannot
           reconcile what we cannot name, so this is AMBIGUOUS with no
           provider_campaign_id.

        2. The POST succeeds but the content PUT fails.  The campaign
           exists and we DO know its ID.  Nothing has been sent — a
           campaign with no content cannot have reached anyone — so this
           is a DEFINITE_FAILURE, but we return the ID so the orphan can
           be cleaned up or reused rather than silently leaked.
        """
        recipients = {'list_id': self.audience_id}

        if segment_id:
            recipients['segment_opts'] = {'saved_segment_id': segment_id}
        elif tag:
            recipients['segment_opts'] = {
                'match': 'all',
                'conditions': [{
                    'condition_type': 'StaticSegment',
                    'field': 'static_segment',
                    'op': 'static_is',
                    'value': tag,
                }],
            }

        data = {
            'type': 'regular',
            'recipients': recipients,
            'settings': {
                'subject_line': subject,
                'preview_text': preview_text or '',
                'title': campaign_title or subject[:50],
                'from_name': self.from_name,
                'reply_to': self.from_email,
                'auto_footer': True,
            },
            'tracking': {
                'opens': True,
                'html_clicks': True,
                'text_clicks': True,
            },
        }
        return self._create_campaign_strict(data, html)

    def _create_campaign_strict(self, data: Dict,
                                html: str) -> ProviderCreateOutcome:
        # ── Step A: create the campaign shell ──────────────────────
        try:
            resp = self._request_strict('POST', '/campaigns', data)
        except ProviderTransportError as e:
            # A campaign may exist that we cannot name.
            log.error("AMBIGUOUS campaign creation: response lost, "
                      "a provider campaign may exist with an unknown ID")
            return ProviderCreateOutcome(
                status=ProviderCreateStatus.AMBIGUOUS,
                error_type=e.error_type,
                error_message=(
                    "Campaign creation response was lost. A campaign may "
                    "exist at the provider with an ID we never received."
                ),
            )
        except ProviderMalformedResponseError as e:
            return ProviderCreateOutcome(
                status=ProviderCreateStatus.AMBIGUOUS,
                http_status=e.http_status,
                error_type="malformed_success_response",
                error_message=(
                    "Campaign creation returned success with an unparseable "
                    "body; the provider campaign ID is unknown."
                ),
            )
        except ProviderHTTPError as e:
            # No campaign was created; nothing was sent.
            return ProviderCreateOutcome(
                status=ProviderCreateStatus.DEFINITE_FAILURE,
                http_status=e.http_status,
                error_type="provider_rejected",
                error_message=f"Campaign creation rejected: HTTP {e.http_status}",
            )

        mc_campaign_id = resp.body.get('id')
        if not mc_campaign_id:
            return ProviderCreateOutcome(
                status=ProviderCreateStatus.AMBIGUOUS,
                http_status=resp.http_status,
                error_type="missing_campaign_id",
                error_message=(
                    "Provider returned success but no campaign ID; a "
                    "campaign may exist that we cannot address."
                ),
            )

        log.info(f"Mailchimp campaign created: {mc_campaign_id}")

        # ── Step B: attach content ─────────────────────────────────
        # A campaign with no content has not been sent to anyone, so any
        # failure here is safe — but we keep the ID either way.
        try:
            self._request_strict(
                'PUT', f'/campaigns/{mc_campaign_id}/content', {'html': html},
            )
        except ProviderError as e:
            log.error(f"Failed to set campaign content for {mc_campaign_id}: "
                      f"{type(e).__name__}")
            return ProviderCreateOutcome(
                status=ProviderCreateStatus.DEFINITE_FAILURE,
                provider_campaign_id=mc_campaign_id,
                error_type="content_set_failed",
                error_message=(
                    f"Campaign {mc_campaign_id} was created but its content "
                    f"could not be set. It was never sent."
                ),
                content_set=False,
            )

        return ProviderCreateOutcome(
            status=ProviderCreateStatus.CREATED,
            provider_campaign_id=mc_campaign_id,
            http_status=resp.http_status,
            content_set=True,
        )

    def send_campaign_strict(self, mc_campaign_id: str,
                             timeout: int = 30) -> ProviderSendOutcome:
        """Send a campaign, returning a lossless three-valued outcome.

        POST /campaigns/{id}/actions/send returns 204 No Content on
        success, so an empty body IS the success signal — we must not
        require JSON to be present.

        Classification:
          2xx                      → CONFIRMED_SENT
          400/401/403/404/405/422  → DEFINITE_FAILURE (rejected pre-processing)
          any other HTTP status    → AMBIGUOUS (incl. every 5xx)
          transport failure        → AMBIGUOUS
          unparseable 2xx body     → AMBIGUOUS (leaning accepted)

        This is the ONLY send method safe for the execution path.
        """
        path = f'/campaigns/{mc_campaign_id}/actions/send'
        try:
            resp = self._request_strict('POST', path, timeout=timeout)
        except ProviderTransportError as e:
            log.error(
                f"AMBIGUOUS send for campaign {mc_campaign_id}: "
                f"{e.error_type} — provider may have accepted the send"
            )
            return ProviderSendOutcome(
                status=ProviderSendStatus.AMBIGUOUS,
                provider_campaign_id=mc_campaign_id,
                error_type=e.error_type,
                error_message=(
                    "Send request was dispatched but no response was "
                    "received. The provider may have sent the campaign."
                ),
                detail="transport_failure_after_dispatch",
            )
        except ProviderHTTPError as e:
            return classify_send_http_error(
                http_status=e.http_status,
                provider_campaign_id=mc_campaign_id,
                detail=e.detail,
            )
        except ProviderMalformedResponseError as e:
            log.error(
                f"AMBIGUOUS send for campaign {mc_campaign_id}: 2xx with "
                f"unparseable body"
            )
            return ProviderSendOutcome(
                status=ProviderSendStatus.AMBIGUOUS,
                http_status=e.http_status,
                provider_campaign_id=mc_campaign_id,
                error_type="malformed_success_response",
                error_message=(
                    "Provider returned success but the body could not be "
                    "parsed. The send was likely accepted."
                ),
                detail="unparseable_2xx_body",
            )

        log.info(f"Mailchimp campaign {mc_campaign_id} sent "
                 f"(HTTP {resp.http_status})")
        return ProviderSendOutcome(
            status=ProviderSendStatus.CONFIRMED_SENT,
            http_status=resp.http_status,
            provider_campaign_id=mc_campaign_id,
        )

    def get_campaign_status(self, mc_campaign_id: str) -> ProviderResponse:
        """Fetch campaign info for reconciliation. Raises on failure.

        Deliberately strict: reconciliation must distinguish "provider
        says X" from "we could not ask", because only the former may
        resolve an ambiguous attempt.
        """
        return self._request_strict('GET', f'/campaigns/{mc_campaign_id}')

    def get_campaign_report(self, mc_campaign_id: str) -> Optional[Dict]:
        """Get campaign performance report from Mailchimp."""
        return self._request('GET', f'/reports/{mc_campaign_id}')

    def get_tag_segment_id(self, tag: str) -> Optional[int]:
        """Only return the exact segment verified during this preparation."""
        return getattr(self, '_verified_segments', {}).get(tag)

    # ── Suppression queries ───────────────────────────────

    def audience_inventory(self) -> List[Dict]:
        """Read every audience's identity and counts, without contact details.

        An incomplete inventory must not appear to prove account-wide coverage.
        This method only issues GETs and never changes subscription status.
        """
        audiences = {}
        offset = 0
        expected = None
        while True:
            response = self._request('GET', f'/lists?count=100&offset={offset}', timeout=60)
            if not isinstance(response, dict) or not isinstance(response.get('lists'), list):
                raise RuntimeError('Mailchimp audience inventory incomplete')
            total = response.get('total_items')
            if not isinstance(total, int) or total < 0 or (expected is not None and total != expected):
                raise RuntimeError('Mailchimp audience inventory changed during pagination')
            expected = total
            rows = response['lists']
            for row in rows:
                audience_id = row.get('id')
                if not audience_id or audience_id in audiences:
                    raise RuntimeError('Mailchimp audience inventory contains missing or duplicate IDs')
                stats = row.get('stats') or {}
                audiences[audience_id] = {
                    'audience_id': audience_id, 'name': row.get('name'),
                    'subscribed': stats.get('member_count'),
                    'unsubscribed': stats.get('unsubscribe_count'),
                    'cleaned': stats.get('cleaned_count'),
                    'legacy_configured_audience': audience_id == self.audience_id,
                }
            offset += len(rows)
            if offset == expected:
                return list(audiences.values())
            if not rows or offset > expected:
                raise RuntimeError('Mailchimp audience inventory pagination truncated')

    def get_suppressed_members(self) -> Optional[List[str]]:
        """Fetch all suppressed members (unsubscribed + cleaned) from Mailchimp.

        Paginates through the full audience for each non-sendable status.
        Returns a list of lowercased email addresses, or None on any error.

        None return means "we could not get the authoritative set" — callers
        must treat this as a failure and NOT update local state.
        """
        suppressed: List[str] = []

        for status in ("unsubscribed", "cleaned"):
            page_emails = self._get_members_by_status(status)
            if page_emails is None:
                # Any pagination failure ⇒ abort entirely
                log.error(f"Failed to fetch {status} members — aborting suppression refresh")
                return None
            suppressed.extend(page_emails)

        return suppressed

    def _get_members_by_status(self, status: str, page_size: int = 1000) -> Optional[List[str]]:
        """Paginate through all audience members with a given status.

        Returns list of lowercased emails, or None on any request failure.
        """
        emails: List[str] = []
        offset = 0
        expected_total = None

        while True:
            resp = self._request(
                'GET',
                f'/lists/{self.audience_id}/members'
                f'?status={status}&count={page_size}&offset={offset}',
                timeout=60,
            )
            if resp is None:
                log.error(f"Mailchimp members request failed: status={status}, offset={offset}")
                return None

            members = resp.get('members')
            total_items = resp.get('total_items')
            if (not isinstance(members, list) or not isinstance(total_items, int)
                    or total_items < 0
                    or (expected_total is not None and total_items != expected_total)):
                return None
            expected_total = total_items
            for m in members:
                addr = m.get('email_address', '').lower().strip()
                if addr:
                    emails.append(addr)

            offset += len(members)

            if offset == total_items:
                break
            if not members or offset > total_items:
                log.error("Mailchimp suppression pagination truncated: status=%s", status)
                return None

            # Brief pause between pages to respect rate limits
            time.sleep(0.2)

        return emails


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

    def __init__(self, db, decision_engine=None, v2_repo=None):
        self.db = db
        self.decision_engine = decision_engine  # The existing DecisionEngine from craft_unified
        self._v2_repo = v2_repo
        self._init_schema()

        # Initialize clients from env vars
        self._claude = None
        self._mailchimp = None

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
    def mailchimp(self) -> Optional[MailchimpClient]:
        if self._mailchimp is None:
            key = os.environ.get('MAILCHIMP_API_KEY')
            audience_id = os.environ.get('MAILCHIMP_AUDIENCE_ID')
            if key and audience_id:
                self._mailchimp = MailchimpClient(key, audience_id)
        return self._mailchimp

    def mailchimp_for_event(self, event_id: str) -> MailchimpClient:
        """Resolve an explicit event destination; never use the legacy default.

        Audience names, city matches, and cross-list membership do not establish
        permission. Every event must have an independently verified mapping.
        """
        from audience_routing import event_audience_id
        audience_id = event_audience_id(event_id)
        if not self.db.get_event(event_id):
            raise RuntimeError('Mapped festival event does not exist')
        key = os.environ.get('MAILCHIMP_API_KEY')
        if not key:
            raise RuntimeError('Mailchimp is not configured')
        return MailchimpClient(key, audience_id)

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

    def validate_campaign(self, campaign_id: str) -> Dict:
        """Validate a campaign and report the audience it would reach.

        This performs no external writes of any kind. It replaces the former
        send_campaign(), whose non-dry-run branch pushed contacts to Mailchimp,
        created a campaign and sent it -- bypassing every V2 safety gate
        (suppression trust, execution-time buyer exclusion, durable send claim,
        provider outcome classification, reconciliation) and reachable over an
        unauthenticated HTTP route.

        Customer email now has exactly one execution path: the V2 intervention
        lifecycle in execution_adapter.ExecutionAdapter.
        """
        row = self.db.conn.execute("SELECT * FROM campaigns WHERE id = ?", (campaign_id,)).fetchone()
        if not row:
            return {'error': 'Campaign not found'}
        campaign = dict(row)

        try:
            recipients = self.db.conn.execute(campaign['segment_sql']).fetchall()
        except Exception as e:
            return {'error': f'Segment SQL failed: {e}'}

        emails = [r['email'] if hasattr(r, 'keys') else r[0] for r in recipients]

        return {
            'campaign_id': campaign_id,
            'status': 'dry_run',
            'audience_count': len(emails),
            'sample_recipients': emails[:10],
            'subject_line': campaign['subject_line'],
            'phase': campaign['phase'],
        }

    # ─────────────────────────────────────────────────────────
    # WEBHOOK PROCESSING — track opens, clicks, bounces
    # ─────────────────────────────────────────────────────────

    def process_mailchimp_webhook(self, data: Dict, audience_id: str = None) -> Dict:
        """Process Mailchimp webhook events (unsubscribe, cleaned, campaign activity).

        After writing a suppression row, updates the suppression sentinel
        via SuppressionGuard.record_mutation() so the fail-closed guard
        stays in sync with actual table state.
        """
        event_type = data.get('type', '')
        email = ''
        suppression_written = False
        ts = datetime.now().isoformat()

        if event_type == 'unsubscribe':
            email = data.get('data', {}).get('email', '').lower()
            if email:
                if audience_id:
                    from audience_suppression import AudienceSuppressionGuard
                    AudienceSuppressionGuard(self.db, audience_id).record_email(email, 'unsubscribe', source='webhook_unsubscribe')
                else:
                    self.db.conn.execute("INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'unsubscribe')", (email,))
                suppression_written = True
        elif event_type == 'cleaned':
            email = data.get('data', {}).get('email', '').lower()
            if email:
                if audience_id:
                    from audience_suppression import AudienceSuppressionGuard
                    AudienceSuppressionGuard(self.db, audience_id).record_email(email, 'bounce', source='webhook_cleaned')
                else:
                    self.db.conn.execute("INSERT OR IGNORE INTO suppressions (email, reason) VALUES (?, 'bounce')", (email,))
                suppression_written = True
        elif event_type == 'campaign':
            # Campaign sent notification — we can pull reports
            mc_campaign_id = data.get('data', {}).get('id', '')
            if mc_campaign_id:
                self.db.conn.execute("""
                    INSERT INTO email_events (mailchimp_campaign_id, event_type, email, timestamp, raw_payload)
                    VALUES (?, ?, ?, ?, ?)
                """, (mc_campaign_id, 'campaign_sent', '', ts, json.dumps(data)))

        if email:
            self.db.conn.execute("""
                INSERT INTO email_events (mailchimp_campaign_id, event_type, email, timestamp, raw_payload)
                VALUES (?, ?, ?, ?, ?)
            """, ('', event_type, email, ts, json.dumps(data)))

        self.db.conn.commit()

        # Update suppression sentinel AFTER successful commit
        if suppression_written and not audience_id:
            try:
                guard = SuppressionGuard(self.db, v2_repo=self._v2_repo)
                guard.record_mutation(source=f"webhook_{event_type}")
            except Exception as e:
                log.error(f"Failed to update suppression sentinel after webhook: {e}")
                # Do not fail the webhook — the suppression row is already committed.
                # The sentinel mismatch will be caught on next validate() call.

        return {'processed': 1, 'type': event_type}

    def sync_campaign_stats(self, campaign_id: str) -> Optional[Dict]:
        """Pull campaign stats from Mailchimp and update local DB.

        Call this periodically (e.g. 24h and 72h after send) to sync
        open/click data from Mailchimp's tracking.
        """
        if not self.mailchimp:
            return None

        # Find the Mailchimp campaign ID from local sends
        row = self.db.conn.execute("""
            SELECT DISTINCT mailchimp_campaign_id FROM campaign_sends
            WHERE campaign_id = ? AND mailchimp_campaign_id != ''
            LIMIT 1
        """, (campaign_id,)).fetchone()

        if not row or not row['mailchimp_campaign_id']:
            return None

        mc_id = row['mailchimp_campaign_id']
        report = self.mailchimp.get_campaign_report(mc_id)

        if report:
            opens = report.get('opens', {}).get('unique_opens', 0)
            clicks = report.get('clicks', {}).get('unique_clicks', 0)
            sends = report.get('emails_sent', 0)

            self.db.conn.execute("""
                UPDATE campaigns SET opens = ?, clicks = ?, sends = ?, updated_at = ?
                WHERE id = ?
            """, (opens, clicks, sends, datetime.now().isoformat(), campaign_id))
            self.db.conn.commit()

            log.info(f"Synced stats for {campaign_id}: {opens} opens, {clicks} clicks, {sends} sends")
            return {'opens': opens, 'clicks': clicks, 'sends': sends}

        return None

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
                # Refresh provider observations independently of campaign generation.
                # The collector only reads Mailchimp and preserves prior good data.
                try:
                    from campaign_feedback import refresh_feedback
                    feedback = refresh_feedback()
                    log.info("Campaign feedback refresh: %s", feedback['status'])
                except Exception:
                    log.warning("Campaign feedback refresh unavailable")
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

    @app.route('/api/campaigns/<cid>/dry-run', methods=['POST'])
    def dry_run_campaign(cid):
        """Validate a campaign without sending — shows audience count and sample recipients."""
        return jsonify(engine.validate_campaign(cid))

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

    @app.route('/api/webhook/mailchimp', methods=['GET', 'POST'])
    def mailchimp_webhook():
        """Mailchimp audience webhook. GET validates the URL, POST receives events.

        This endpoint stays outside the bearer gate because Mailchimp cannot
        send an Authorization header. Unverified, that made it a public write
        into a safety-critical dataset: a POST of
        {"type": "unsubscribe", "data": {"email": ...}} suppressed any address
        and moved the suppression sentinel with it.

        Authenticity is Mailchimp's documented HMAC signature. Per the Marketing
        API guide "Synchronize Audience Data with Webhooks", every delivery on a
        signed webhook carries

            X-Mailchimp-Signature: t=<unix_ts>,v1=<hex>

        where v1 is HMAC-SHA256(signing_secret, "{t}.{raw_body}") over the exact
        request bytes, and deliveries outside a five-minute window are rejected.
        That window bounds replayability; it is not deduplication -- see the
        note on MAILCHIMP_SIGNATURE_TOLERANCE_SECONDS.

        Fails closed. With no signing secret configured the POST is refused,
        never processed unsigned -- signature verification is optional in
        Mailchimp's UI, and an unsigned webhook must not be silently trusted
        here. GET never touches the database, so the URL-validation handshake
        cannot be broken by a misconfigured secret.
        """
        if request.method == 'GET':
            return '', 200  # URL validation handshake; no mutation, no data.

        raw_body = request.get_data(cache=True)
        payload = request.get_json(silent=True)
        if not isinstance(payload, dict):
            payload = normalize_mailchimp_form(request.form)
        audience_id = None
        if os.environ.get('MAILCHIMP_EVENT_AUDIENCES') or os.environ.get('MAILCHIMP_WEBHOOK_SIGNING_SECRETS'):
            # Untrusted list_id selects a key only. The selected key must then
            # authenticate the exact raw body, including that same list_id.
            try:
                keys = json.loads(os.environ.get('MAILCHIMP_WEBHOOK_SIGNING_SECRETS', '{}'))
                audience_id = payload.get('data', {}).get('list_id')
                if not isinstance(audience_id, str) or not re.fullmatch(r'[a-fA-F0-9]{10}', audience_id):
                    raise ValueError('Missing audience identity')
                signing_secret = keys.get(audience_id) if isinstance(keys, dict) else None
            except (ValueError, TypeError, AttributeError):
                return jsonify({'error':'invalid_webhook_audience'}), 401
        else:
            signing_secret = os.environ.get('MAILCHIMP_WEBHOOK_SIGNING_SECRET', '')
        if not isinstance(signing_secret, str) or not signing_secret:
            log.error("Mailchimp webhook POST refused: "
                      "MAILCHIMP_WEBHOOK_SIGNING_SECRET is not set")
            return jsonify({
                'error': 'webhook_signing_secret_not_configured',
                'message': 'MAILCHIMP_WEBHOOK_SIGNING_SECRET must be configured '
                           'before webhook events are accepted.',
            }), 503

        ok, reason = verify_mailchimp_signature(
            signing_secret, request.headers.get('X-Mailchimp-Signature', ''), raw_body)
        if not ok:
            log.warning(f"Mailchimp webhook POST rejected: {reason}")
            return jsonify({'error': 'invalid_signature', 'reason': reason}), 401

        # Optional second factor: a hard-to-guess component in the callback URL,
        # which Mailchimp also recommends. Deliberately a DIFFERENT secret from
        # the signing key -- one is a shared bearer, the other proves payload
        # integrity, and they must not be interchangeable.
        path_secret = os.environ.get('MAILCHIMP_WEBHOOK_PATH_SECRET', '')
        if path_secret:
            supplied = request.args.get('secret', '')
            if not supplied or not hmac.compare_digest(supplied, path_secret):
                log.warning("Mailchimp webhook POST rejected: bad or missing path secret")
                return jsonify({'error': 'unauthorized'}), 401

        result = engine.process_mailchimp_webhook(payload, audience_id=audience_id)
        return jsonify(result)

    @app.route('/api/campaigns/<cid>/sync-stats', methods=['POST'])
    def sync_campaign_stats(cid):
        """Pull latest open/click stats from Mailchimp for a sent campaign."""
        result = engine.sync_campaign_stats(cid)
        if result:
            return jsonify(result)
        return jsonify({'error': 'Could not sync stats — campaign not found or Mailchimp not configured'}), 400

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
                'mailchimp_configured': bool(os.environ.get('MAILCHIMP_API_KEY') and os.environ.get('MAILCHIMP_AUDIENCE_ID')),
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
