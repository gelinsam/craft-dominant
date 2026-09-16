"""Optional DBOS draft runtime, isolated from the production web application.

Importing this module registers functions only. Explicit startup is required.
The caller supplies a read-only database factory; no browser or send tool exists.
"""
from datetime import datetime, timezone
from dbos import DBOS, SetWorkflowID
from campaign_preparation import prepare_draft

_db_factory = None


@DBOS.step()
def build_package(request, sources, contact_history):
    if _db_factory is None:
        raise RuntimeError('Draft worker is not configured')
    return prepare_draft(_db_factory(), request, sources, contact_history,
                         datetime.now(timezone.utc))


@DBOS.workflow()
def prepare_campaign(request, sources, contact_history):
    return build_package(request, sources, contact_history)


def start_runtime(db_factory, system_database_url):
    global _db_factory
    if not system_database_url:
        raise ValueError('An explicit durable database URL is required')
    _db_factory = db_factory
    DBOS(config={'name': 'craft-campaign-drafts',
                 'application_version': 'drafts-v1',
                 'system_database_url': system_database_url})
    DBOS.launch()


def submit(request, sources, contact_history):
    # Retries of one logical request reuse its checkpointed result. A fresh
    # audience requires a NEW run ID, even for identical segment criteria.
    if not request.get('run_id'):
        raise ValueError('An explicit unique run ID is required')
    with SetWorkflowID('craft-draft:' + request['run_id']):
        return DBOS.start_workflow(prepare_campaign, request, sources, contact_history)
