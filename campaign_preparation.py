"""Draft-only campaign preparation. No provider writes or send capability.

DBOS owns persistence and recovery; Craft owns scope and eligibility decisions.
Browser evidence is explicitly supplied, never inferred from a workflow success.
"""
import csv
import io
from datetime import datetime, timezone, timedelta
from crm_audience import build_crm_audience, eventbrite_eligible_emails, recipient_digest, email_key


def observed_at(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Evidence timestamps require a timezone')
    return stamp


def require_fresh(value, now, max_age=timedelta(hours=1)):
    age = now - observed_at(value)
    if age < timedelta(0) or age > max_age:
        raise ValueError('Evidence is stale or from the future; refresh it')


def prepare_draft(db, request, sources, contact_history, now):
    """Build one immutable candidate package from current CRM and supplied evidence.

    Contact-history completeness must be established by the evidence collector.
    Missing coverage blocks preparation. No empty-history default is allowed.
    """
    if now.tzinfo is None:
        raise ValueError('Current time requires a timezone')
    if not sources or not request.get('run_id'):
        raise ValueError('A run ID and provider evidence are required')
    audience = build_crm_audience(db, request['event_id'], request['segment'], request['purpose'])
    scope = (audience['event_type'], audience['city'])
    allowed, denied = set(), set()
    source_ids = set()
    for source in sources:
        if (source['event_type'], source['city']) != scope:
            raise ValueError('Provider source does not match festival and city')
        if not source.get('list_id') or source['list_id'] in source_ids:
            raise ValueError('Missing or duplicate source list ID')
        source_ids.add(source['list_id'])
        require_fresh(source['observed_at'], now)
        eligible = eventbrite_eligible_emails(source['csv'])
        rows = list(csv.DictReader(io.StringIO(source['csv'].lstrip('\ufeff'))))
        all_emails = {email_key(row['Email Address']) for row in rows}
        allowed.update(eligible)
        denied.update(all_emails - eligible)
    # A denial in any supplied source beats eligibility in another.
    allowed -= denied
    if contact_history.get('complete') is not True:
        raise ValueError('Contact history coverage is incomplete')
    require_fresh(contact_history['observed_at'], now)
    if contact_history.get('active_campaigns'):
        raise ValueError('Active campaigns must be reconciled before preparation')
    if contact_history.get('scope') != 'all_festivals_all_providers':
        raise ValueError('Cross-festival and cross-provider history is required')
    cooldown = request.get('cooldown_days', 7)
    if not isinstance(cooldown, int) or isinstance(cooldown, bool) or not 1 <= cooldown <= 90:
        raise ValueError('Cooldown must be 1–90 days')
    recent = set()
    for item in contact_history['contacts']:
        timestamp = observed_at(item['contacted_at'])
        if timestamp > now:
            raise ValueError('Contact timestamp is in the future')
        if now - timestamp < timedelta(days=cooldown):
            recent.add(email_key(item['email']))
    candidates = {r['email'] for r in audience['records']}
    recipients = sorted((candidates & allowed) - recent)
    if not recipients:
        raise ValueError('No eligible recipients remain')
    output = io.StringIO()
    # Eventbrite treats the header as an invalid subscriber. Upload email-only rows.
    csv.writer(output, lineterminator='\n').writerows([email] for email in recipients)
    return {
        'run_id': request['run_id'], 'event_id': request['event_id'],
        'event_type': scope[0], 'city': scope[1],
        'segment': request['segment'], 'purpose': request['purpose'],
        'prepared_at': now.isoformat(), 'source_list_ids': sorted(source_ids),
        'recipient_count': len(recipients), 'recipient_sha256': recipient_digest(recipients),
        'upload_csv': output.getvalue(), 'state': 'AWAITING_BROWSER_IMPORT',
        'external_send_enabled': False,
    }


def verify_import(package, provider_list_id, export_csv, exported_at, now):
    require_fresh(package['prepared_at'], now)
    require_fresh(exported_at, now)
    if observed_at(exported_at) < observed_at(package['prepared_at']):
        raise ValueError('Verification export predates preparation')
    if not provider_list_id or provider_list_id in package['source_list_ids']:
        raise ValueError('Verify a dedicated destination list, not a source list')
    rows = list(csv.DictReader(io.StringIO(export_csv.lstrip('\ufeff'))))
    emails = eventbrite_eligible_emails(export_csv)
    if (len(rows) != package['recipient_count'] or len(emails) != len(rows)
            or recipient_digest(emails) != package['recipient_sha256']):
        raise ValueError('Provider membership differs from the prepared audience')
    return dict(package, state='DRAFT_AUDIENCE_VERIFIED', provider_list_id=provider_list_id,
                verified_at=now.isoformat())
