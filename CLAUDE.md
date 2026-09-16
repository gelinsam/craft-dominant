# Craft Dominant — engineering instructions

Sam Gelin's internal AI operating system for Craft Hospitality.

## Engineering principle: search before building

**Before implementing any substantial new subsystem, search existing project code
and mature open-source solutions first.** Prefer adoption, integration, wrapping,
or extension over greenfield implementation when a proven solution meets the
requirements.

Concentrate custom engineering on Craft-specific business logic, proprietary data
interpretation, differentiated product behaviour, and integrations that truly
require it.

**Any substantial bespoke implementation of commodity infrastructure must document
why existing alternatives were rejected.**

The default order is:

    SEARCH -> EVALUATE -> ADOPT -> CUSTOMIZE -> (only when justified) BUILD

### Two layers, kept distinct

**A. Craft intelligence layer — ours, keep it.** What counts as a festival
edition, historical comparison, ticket pacing, Eventbrite mappings,
Meta-to-festival attribution, customer segmentation, ticket economics,
revenue-at-risk, intervention constraints, and the business rules governing when
and how Craft intervenes. This must stay understandable, testable and
deterministic where possible.

**B. Agent / execution runtime — commodity, prefer proven infrastructure.** Agent
loops, LLM tool orchestration, durable workflow execution, retries and
resumability, long-running jobs, agent memory, subagent orchestration, generic
scheduling, human-in-the-loop frameworks, tracing and observability, evals, MCP
infrastructure, browser agents, generic structured-output plumbing.

Do not blur the two. Craft Dominant is not an agent-framework company; it exists
to make Craft more capable with fewer human hours. Avoid generic plugin systems,
workflow designers, agent registries, premature multi-tenancy, and speculative
abstractions for hypothetical future customers.

## Non-negotiables

- **Data integrity is paramount.** See "Truth layer" below.
- Do not weaken existing test coverage.
- Do not break production.
- Do not enable external sending (`V2_ENABLE_EXTERNAL_SEND` stays `0`,
  `execution_mode` stays `dry_run`) without explicit instruction.
- Do not merge architectural changes until reviewed and tested.

## Truth layer rules (learned the hard way)

A sync must **never overwrite a known value with one it did not observe.**

- An unobserved field is `None`, meaning UNKNOWN — never a default like `1`, `0`
  or `''`. Inventing a default silently replaced real ticket counts with
  plausible wrong ones and nobody noticed for a day.
- Writes **merge** (`ON CONFLICT ... COALESCE(excluded.x, table.x)`), they do not
  `INSERT OR REPLACE`. An *observed* change, including a refund, still applies.
- Parse output carries unknowns and must not be handed to consumers expecting
  real values. Read the merged row back first.
- Provider payloads mark withdrawn records rather than removing them. Check
  refund/cancellation flags; `len(attendees)` is not a ticket count.
- Every sync compares before/after and reports material regressions loudly.
  Stored ticket totals essentially never fall.
- Long-running syncs record durable start/finish state, so an interrupted run
  stays visible after a restart. **Check for an in-flight sync before deploying.**

## Before deploying

1. `GET /api/sync-status` — confirm nothing is running and no run is `interrupted`.
2. Deploying mid-sync kills the traversal and leaves data half-updated.

## Test suite

```
python -m pytest test_dominant_agent.py test_diagnosis.py \
  test_diagnosis_edition_scope.py test_intervention.py test_v2_postgres.py \
  test_meta_spend_correctness.py test_analytics_durability.py \
  test_import_side_effects.py test_customer_profile_performance.py \
  test_post_sync_completion.py test_profile_write_batching.py \
  test_profile_write_concurrency.py test_profile_rebuild_endpoint.py \
  test_meta_festival_edition.py test_meta_spend_pipeline_e2e.py \
  test_meta_edition_shared_reads.py test_meta_alias_tokenization.py \
  test_sync_data_integrity.py test_idempotent_execution.py \
  test_security_exposure.py test_webhook_authenticity.py
```

CI runs these in both the SQLite and Postgres jobs
(`.github/workflows/v2-tests.yml`). The deploy PAT lacks `workflow` scope, so
workflow edits go through the GitHub web UI.
