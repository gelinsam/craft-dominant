# Draft campaign workflow: integration candidate

Adopts MIT-licensed DBOS 2.31.1 as a dependency. No vendor code was copied. DBOS
provides checkpoint persistence and restart recovery. Craft retains its existing
CRM and festival-edition resolver. Dittofeed would duplicate CRM/journey storage;
Listmonk would introduce another sending platform. Neither solves the observed
Eventbrite browser workflow, so neither is added.

This is an optional draft worker, not imported by the web application. Production
startup, dependencies, schedules and sending flags are untouched. No send method
or browser credentials are present. Do not enable it as a production service yet.

prepare_draft rebuilds current CRM candidates and intersects explicitly scoped
Eventbrite exports. A denial in any supplied list wins. Evidence older than one
hour, unknown cross-provider contact history, active campaigns, empty audiences,
or wrong city/festival scope block preparation. Seven days is the default contact
cooldown, configurable from 1 to 90 days. A dedicated destination export must match
all recipients exactly. Verification expires with the preparation snapshot.

The evidence collector must establish source mapping and complete contact-history
coverage. Caller-supplied completeness is a contract, not an independently verified
fact. The existing DC pilot cannot be run through this gate yet: active DC sending
and incomplete cross-provider contact history remain unresolved.

Install requirements-workflows.txt in an isolated Python 3.12 worker. Call
start_runtime with a read-only CRM database factory and an explicit system database
URL. Use a dedicated Postgres database in production. DBOS stores workflow inputs
and results, including contact emails; protect and retain that database accordingly.
No public listener is started. Local tests use only synthetic contacts and SQLite.

submit requires a unique run_id for every fresh selection. Retrying the same run
reuses its inputs/result; callers must never reuse an ID with changed criteria.
Recovery of a checkpoint is not a fresh purchase or consent check. Re-run selection
with a new ID after expiration, and recheck immediately before any eventual send.

Browser bridge procedure: prepare package; import headerless upload_csv to a new,
named draft list; export that exact list; verify_import with observed list ID and
export timestamp. If an import result is uncertain, inspect/export before retrying.
DBOS does not make external browser side effects exactly-once. No unattended
Eventbrite adapter, campaign creation, live consent/history collector, send approval
flow, or revenue attribution is implemented by this change.

Validation: unit tests cover cross-list suppression, current-edition purchase
exclusion, contact cooldown, missing history, active campaigns, stale evidence,
wrong scope and exact provider membership. The real SDK integration test kills a
process after a checkpoint and requires restart recovery without rebuilding it.
Merge requires that integration test and architectural review, not just unit tests.
