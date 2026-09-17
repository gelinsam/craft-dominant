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
    Missing coverage remains a send blocker on the draft. No empty-history
    default is allowed for the known contacts field.
    """
    if now.tzinfo is None:
        raise ValueError('Current time requires a timezone')
    if not sources or not request.get('run_id'):
        raise ValueError('A run ID and provider evidence are required')
    audience = build_crm_audience(db, request['event_id'], request['segment'], request['purpose'], now=now)
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
    provider = request.get('provider', 'eventbrite')
    if provider != 'eventbrite':
        raise ValueError('This preparer requires Eventbrite destination consent; use the Mailchimp adapter for Mailchimp')
    blockers = []
    cross_provider_delivery = set()
    cross_provider_pending = set()
    uncertain_contacts = set()
    if audience.get('history_coverage') == 'stored_records_only':
        blockers.append('purchase_history_coverage_unverified')
    if contact_history.get('complete') is not True:
        blockers.append('contact_history_incomplete')
    require_fresh(contact_history['observed_at'], now)
    reserved = set()
    stale_cross_provider_campaigns = 0
    active = contact_history.get('active_campaigns')
    unresolved = active is None
    if active is not None and not isinstance(active, list):
        raise ValueError('Active campaigns must be an explicit list')
    for campaign in active or []:
        # Legacy ID-only evidence still blocks readiness; it cannot establish
        # that there is no overlap. Partial snapshots can exclude known people.
        if not isinstance(campaign, dict):
            unresolved = True
            continue
        if not campaign.get('campaign_id') or campaign.get('provider') not in ('eventbrite', 'mailchimp'):
            raise ValueError('Active campaign evidence requires provider and ID')
        same_provider = campaign['provider'] == provider
        if same_provider:
            require_fresh(campaign['observed_at'], now)
        else:
            try:
                require_fresh(campaign['observed_at'], now)
            except (ValueError, KeyError):
                stale_cross_provider_campaigns += 1
                continue
        recipients = campaign.get('recipient_emails')
        if not isinstance(recipients, list):
            unresolved = unresolved or same_provider
            continue
        for value in recipients:
            email = email_key(value)
            if not email or '@' not in email or any(c in email for c in '\r\n'):
                raise ValueError('Invalid active campaign recipient')
            (reserved if same_provider else cross_provider_pending).add(email)
        if same_provider and campaign.get('membership_complete') is not True:
            unresolved = True
    if unresolved:
        blockers.append('active_campaigns_unresolved')
    if contact_history.get('scope') not in ('all_festivals_all_providers', f'all_festivals_{provider}'):
        blockers.append('destination_provider_history_incomplete')
    cooldown = request.get('cooldown_days', 7)
    if not isinstance(cooldown, int) or isinstance(cooldown, bool) or not 1 <= cooldown <= 90:
        raise ValueError('Cooldown must be 1–90 days')
    recent = set()
    for item in contact_history['contacts']:
        timestamp = observed_at(item['contacted_at'])
        if timestamp > now:
            raise ValueError('Contact timestamp is in the future')
        email = email_key(item.get('email'))
        if not email or '@' not in email or any(c in email for c in '\r\n'):
            raise ValueError('Invalid contact history record')
        if now - timestamp >= timedelta(days=cooldown):
            continue
        source = item.get('provider')
        status = item.get('status', 'unknown')
        # Acceptance/send attempts are not proof of recipient delivery. Opens
        # and clicks demonstrate engagement, but never infer inbox placement.
        if status in ('delivered', 'opened', 'clicked'):
            if source == provider:
                recent.add(email)
            elif source in ('mailchimp', 'eventbrite'):
                cross_provider_delivery.add(email)
            else:
                uncertain_contacts.add(email)
        elif status in ('failed', 'bounced', 'not_delivered') and source in ('mailchimp', 'eventbrite'):
            # This is contact-frequency evidence only. Independent consent and
            # provider bounce/suppression gates above must still pass.
            continue
        elif source == provider or source not in ('mailchimp', 'eventbrite'):
            uncertain_contacts.add(email)
        else:
            cross_provider_pending.add(email)
    candidates = {r['email'] for r in audience['records']}
    eligible = candidates & allowed
    if eligible & uncertain_contacts:
        blockers.append('delivery_evidence_unresolved')
    recipients = sorted(eligible - recent - reserved)
    if not recipients:
        raise ValueError('No eligible recipients remain')
    output = io.StringIO()
    # Eventbrite treats the header as an invalid subscriber. Upload email-only rows.
    csv.writer(output, lineterminator='\n').writerows([email] for email in recipients)
    return {
        'run_id': request['run_id'], 'event_id': request['event_id'],
        'event_type': scope[0], 'city': scope[1],
        'segment': request['segment'], 'purpose': request['purpose'], 'provider': provider,
        'contact_policy': 'provider_delivery_aware_v1',
        'quarantined_invalid_email_records': audience.get('quarantined_invalid_email_records', 0),
        'cross_provider_delivered_candidates': len(eligible & cross_provider_delivery),
        'cross_provider_pending_candidates': len(eligible & cross_provider_pending),
        'cross_provider_unverified_campaigns': stale_cross_provider_campaigns,
        'unknown_delivery_candidates': len(eligible & uncertain_contacts),
        'prepared_at': now.isoformat(), 'source_list_ids': sorted(source_ids),
        'recipient_count': len(recipients), 'recipient_sha256': recipient_digest(recipients),
        'excluded_recent_contacts': len(eligible & recent),
        'excluded_active_campaign_recipients': len((eligible - recent) & reserved),
        'audience_evidence': {k:audience[k] for k in (
            'history_coverage', 'purchase_window_start', 'purchase_window_end', 'edition_count'
        ) if k in audience},
        'upload_csv': output.getvalue(), 'state': 'AWAITING_BROWSER_IMPORT',
        'sending_blockers': blockers + ['fresh_presend_recheck_required', 'sending_disabled'],
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
