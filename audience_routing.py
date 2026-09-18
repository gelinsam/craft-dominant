"""Explicit festival destinations; no city/name/default audience inference."""
import json
import os
import re


def event_audience_id(event_id, reviewed_audience_id=None):
    try:
        routes = json.loads(os.environ.get('MAILCHIMP_EVENT_AUDIENCES', '{}'))
    except (TypeError, ValueError) as exc:
        raise RuntimeError('Mailchimp event audience mappings are invalid') from exc
    if not isinstance(routes, dict):
        raise RuntimeError('Mailchimp event audience mappings are invalid')
    audience_id = routes.get(str(event_id))
    # Private reviewed draft briefs can carry an explicit destination. They
    # cannot silently replace an existing environment route or infer a city.
    if reviewed_audience_id is not None:
        if audience_id is not None and audience_id != reviewed_audience_id:
            raise RuntimeError('Reviewed draft audience conflicts with the event mapping')
        audience_id = reviewed_audience_id
    if not isinstance(audience_id, str) or not re.fullmatch(r'[a-fA-F0-9]{10}', audience_id):
        raise RuntimeError('This event has no verified Mailchimp audience mapping')
    return audience_id
