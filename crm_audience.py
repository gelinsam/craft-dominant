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


def _years_before(day, years):
    # Calendar anniversaries, including February 29.
    try:
        return day.replace(year=day.year-years)
    except ValueError:
        return day.replace(year=day.year-years, day=28)


def _one_and_done_buyers(db, event, now):
    """Observed one-edition buyers who skipped a later completed edition.

    Reuse the production edition resolver: orders and timed-entry days are not
    visits. Missing provider history is not proof of absence; the output stays
    candidate evidence until historical coverage is independently established.
    """
    today = now.astimezone(timezone.utc).date()
    events = [dict(r) for r in db.conn.execute("""
        SELECT event_id, name, event_date, status FROM events
        WHERE city = ? AND event_type = ?
          AND lower(name) NOT LIKE '%exhibitor%'
          AND lower(name) NOT LIKE '%vendor%'
          AND lower(name) NOT LIKE '%sponsor%'
    """, (event['city'], event['event_type'])).fetchall()]
    by_id = {e['event_id']: e for e in events}
    groups, membership = {}, {}
    for e in events:
        eid = e['event_id']
        if eid in membership:
            continue
        ids = frozenset(db.edition_sibling_ids(eid))
        if eid not in ids or not ids.issubset(by_id):
            raise ValueError('Historical edition has unresolved or conflicting scope')
        if ids & membership.keys():
            raise ValueError('Historical editions have inconsistent membership')
        key = tuple(sorted(ids))
        try:
            if any((by_id[i].get('status') or '').lower() in ('cancelled', 'canceled', 'deleted') for i in ids):
                raise ValueError('Cancelled edition is not a return opportunity')
            dates = [datetime.fromisoformat(by_id[i]['event_date']).date() for i in ids]
            groups[key] = (min(dates), max(dates))
        except (ValueError, TypeError):
            groups[key] = None
        membership.update({i:key for i in ids})
    rows = db.conn.execute("""
        SELECT lower(trim(o.email)) AS email, o.event_id,
               SUM(CASE WHEN o.ticket_count > 0 THEN o.ticket_count ELSE 0 END) AS tickets,
               MAX(CASE WHEN o.ticket_count IS NULL THEN 1 ELSE 0 END) AS unknown_quantity
        FROM orders o JOIN events e ON e.event_id=o.event_id
        WHERE e.city=? AND e.event_type=?
          AND lower(e.name) NOT LIKE '%exhibitor%'
          AND lower(e.name) NOT LIKE '%vendor%'
          AND lower(e.name) NOT LIKE '%sponsor%'
        GROUP BY lower(trim(o.email)), o.event_id
    """, (event['city'], event['event_type'])).fetchall()
    people, uncertain = {}, set()
    for row in rows:
        email = email_key(row['email'])
        key = membership[row['event_id']]
        if row['unknown_quantity'] or groups[key] is None:
            uncertain.add(email)
        if row['tickets'] > 0:
            person = people.setdefault(email, {'editions':set(), 'tickets':0})
            person['editions'].add(key)
            person['tickets'] += row['tickets']
    start, end = _years_before(today, 3), _years_before(today, 1)
    selected = []
    for email, person in people.items():
        if email in uncertain or len(person['editions']) != 1:
            continue
        key = next(iter(person['editions']))
        first, last = groups[key]
        if not start <= last <= end:
            continue
        later = [k for k, dates in groups.items()
                 if dates is not None and dates[0] > last and dates[1] < today]
        if not later:
            continue
        selected.append({'email':email, 'purchased_edition_count':1,
                         'last_event_date':last.isoformat(),
                         'past_ticket_count':person['tickets'],
                         'missed_completed_editions':len(later)})
        if len(selected) > 50000:
            raise ValueError('Audience exceeds supported size; refusing a truncated list')
    return selected, {'history_coverage':'stored_records_only',
                      'purchase_window_start':start.isoformat(),
                      'purchase_window_end':end.isoformat(),
                      'edition_count':len(groups)}


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
    if segment not in FILTERS and segment not in ('cross_sell', 'past_attendees', 'one_and_done'):
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
    evidence = {}
    if segment in ('past_attendees', 'one_and_done'):
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            raise ValueError('Current time requires a timezone')
        if segment == 'one_and_done':
            rows, evidence = _one_and_done_buyers(db, event, now)
        else:
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
    quarantined_invalid = 0
    for row in rows:
        email = email_key(row.get('email'))
        if not email or '@' not in email or any(c in email for c in '\r\n'):
            quarantined_invalid += 1
            continue
        if purpose == 'ticket_sales' and email in buyers:
            excluded_buyers.add(email)
            continue
        selected[email] = dict(row, email=email)
    return {
        'event_id': event_id, 'event_ids': sorted(siblings),
        'event_type': event['event_type'], 'city': event['city'],
        'segment': segment, 'purpose': purpose, 'stage': 'candidates',
        'excluded_current_buyers': len(excluded_buyers),
        'quarantined_invalid_email_records': quarantined_invalid,
        'candidate_source_records': len(rows),
        'records': [selected[email] for email in sorted(selected)],
        **evidence,
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
