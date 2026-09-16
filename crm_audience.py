"""Fresh, festival-scoped CRM candidates. Selection is not marketing consent.

Wraps the existing CRM queries and edition resolver; agents choose documented
criteria, never arbitrary SQL. Provider eligibility must be checked separately.
"""
import csv
import hashlib
import io
from datetime import datetime, timezone


FILTERS = {
    'super_spreaders': {'superspreaders_only': True},
    'vips': {'vips_only': True},
    'churn_critical': {'churn_levels': ['critical', 'urgent']},
    'accelerating': {'momentum': 'accelerating'},
    'dormant': {'momentum': 'dormant'},
    'high_influence': {'min_social_influence': 50},
    'group_buyers': {'group_size': 'large_group'},
}


def email_key(value):
    return value.strip().lower() if isinstance(value, str) else ''


def _past_buyers(db, event, siblings, now):
    """Exact city/type, completed dates, positive observed ticket counts.

    This is purchase evidence, not proof of physical attendance. Never use the
    legacy substring-name matcher, profile totals or a future edition as history.
    """
    dates = []
    for sibling in siblings:
        item = db.get_event(sibling)
        if not item or (item.get('city'), item.get('event_type')) != (event['city'], event['event_type']):
            raise ValueError('Edition sibling has missing or conflicting festival scope')
        dates.append(datetime.fromisoformat(item['event_date']).date())
    if not dates:
        raise ValueError('Edition has no resolved event dates')
    cutoff = min(min(dates), now.astimezone(timezone.utc).date()).isoformat()
    placeholders = ','.join('?' for _ in siblings)
    rows = db.conn.execute(f"""
        SELECT lower(trim(o.email)) AS email,
               COUNT(DISTINCT e.event_id) AS past_event_count,
               MAX(date(e.event_date)) AS last_event_date,
               SUM(o.ticket_count) AS past_ticket_count
        FROM orders o JOIN events e ON e.event_id = o.event_id
        WHERE e.event_type = ? AND e.city = ?
          AND date(e.event_date) < ? AND o.ticket_count > 0
          AND lower(e.name) NOT LIKE '%exhibitor%'
          AND lower(e.name) NOT LIKE '%vendor%'
          AND lower(e.name) NOT LIKE '%sponsor%'
          AND e.event_id NOT IN ({placeholders})
        GROUP BY lower(trim(o.email))
        ORDER BY email LIMIT 50001
    """, [event['event_type'], event['city'], cutoff, *siblings]).fetchall()
    return [dict(row) for row in rows]


def build_crm_audience(db, event_id, segment, purpose='ticket_sales', now=None):
    if segment not in FILTERS and segment not in ('cross_sell', 'past_attendees'):
        raise ValueError('Unsupported CRM audience')
    if purpose not in ('ticket_sales', 'referral'):
        raise ValueError('Unsupported campaign purpose')
    if purpose == 'referral' and segment not in ('super_spreaders', 'high_influence', 'group_buyers'):
        raise ValueError('Referral purpose requires an advocacy audience')
    event = db.get_event(event_id)
    if not event or not event.get('event_type') or not event.get('city'):
        raise ValueError('A known event with festival type and city is required')
    if any(word in (event.get('name') or '').lower() for word in ('exhibitor', 'vendor', 'sponsor')):
        raise ValueError('Business payment events are not customer campaign destinations')
    siblings = db.edition_sibling_ids(event_id)
    if not siblings or event_id not in siblings:
        raise ValueError('Current edition could not be resolved')
    buyers = set()
    for sibling in siblings:
        buyers.update(email_key(email) for email in db.get_event_buyers(sibling))
    if segment == 'past_attendees':
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError('Current time requires a timezone')
        rows = _past_buyers(db, event, siblings, now)
    elif segment == 'cross_sell':
        rows = db.get_cross_sell_candidates(event['event_type'], event['city'],
                                            exclude_emails=buyers, limit=50001)
    else:
        rows = db.get_event_profiles(event['event_type'], event['city'],
                                    limit=50001, **FILTERS[segment])
    if len(rows) > 50000:
        raise ValueError('Audience exceeds supported size; refusing a truncated list')
    selected = {}
    excluded_buyers = set()
    for row in rows:
        email = email_key(row.get('email'))
        if not email or '@' not in email or any(c in email for c in '\r\n'):
            raise ValueError('CRM returned an invalid email; refresh the source data')
        if purpose == 'ticket_sales' and email in buyers:
            excluded_buyers.add(email)
            continue
        selected[email] = dict(row, email=email)
    return {
        'event_id': event_id, 'event_ids': sorted(siblings),
        'event_type': event['event_type'], 'city': event['city'],
        'segment': segment, 'purpose': purpose, 'stage': 'candidates',
        'excluded_current_buyers': len(excluded_buyers),
        'records': [selected[email] for email in sorted(selected)],
    }


def eventbrite_eligible_emails(csv_text):
    """Read Eventbrite's observed subscriber export, with deny winning duplicates."""
    reader = csv.DictReader(io.StringIO(csv_text.lstrip('\ufeff')))
    required = {'Email Address', 'Subscribed? Yes/No', 'Unsubscribed Date', 'Bounced'}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError('Eventbrite export lacks eligibility fields')
    allowed, denied = set(), set()
    for row in reader:
        email = email_key(row.get('Email Address'))
        if not email or '@' not in email:
            raise ValueError('Invalid subscriber record')
        if (row.get('Subscribed? Yes/No') == 'Yes' and row.get('Bounced') == 'No'
                and row.get('Unsubscribed Date') == ''):
            allowed.add(email)
        else:
            denied.add(email)
    return allowed - denied


def recipient_digest(emails):
    """Stable exact-membership fingerprint for preparation/recheck comparison."""
    return hashlib.sha256('\n'.join(sorted({email_key(e) for e in emails})).encode()).hexdigest()
