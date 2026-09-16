# Durable campaign draft preparation

## Reuse decision

Uses MIT-licensed DBOS 2.31.1 for checkpoints and restart recovery. No vendor code
was copied. Craft's existing CRM queries and festival-edition resolver are reused.
Dittofeed would duplicate CRM/journey storage; Listmonk would introduce a different
sending platform. Neither supplies the verified Eventbrite browser workflow.

## Runtime

The dependency is installed with the application, but the web app does not import
or launch DBOS. The on-demand worker has no listener, scheduler or send operation;
DBOS's admin server is explicitly disabled. An operator/browser agent invokes:

    python campaign_worker.py --input INPUT.json --output NEW_OUTPUT.json \
      --analytics-db /data/craft_unified.db \
      --workflow-db-url sqlite:////data/craft_campaign_drafts.sqlite

Use a dedicated protected Postgres system database for concurrent workers. The
single-worker pilot can use a separate SQLite file on the existing durable volume.
DBOS stores email-bearing inputs and results: protect and retain the system
store like CRM data. Output files are created with mode 0600 and never overwritten.
The analytics connection is read-only and holds one consistent snapshot. No
schema setup, ingestion, analytics writes, or application startup is invoked.

INPUT.json contains request, sources and contact_history. Sources are Eventbrite
CSV exports with verified list_id, event_type, city and observed_at. Request
includes run_id, event_id, segment, purpose, and optional cooldown_days (default
7, allowed 1–90). Contact history includes complete, observed_at, scope,
active_campaigns and contacts (email/contacted_at). Never invent completeness.

## Decision boundaries

Fresh CRM candidates intersect provider eligibility. Denials in any supplied list
win. Wrong city/festival, stale/future eligibility, empty recipients, and malformed
data block preparation. Unknown history or active campaigns allow useful drafts
but remain explicit sending blockers. All packages have external_send_enabled=false
and require a fresh pre-send check. No result from this worker authorizes sending.

A dedicated destination export must match all intended recipients exactly. The
preparation and export must be no more than one hour old at verification. Browser
membership verification is not a proof of cross-provider contact-history coverage.

A fresh selection requires a new run_id. Recovery reuses a saved snapshot; it does
not refresh purchases or consent. prepare_and_wait rejects a reused run_id with
changed inputs. Browser operations are outside retryable DBOS steps: a timeout
after an import must be reconciled before retrying, not assumed to mean failure.

## Verified Eventbrite browser procedure

Use the authorized browser session through the computer-use tool. Do not extract
cookies or copy sessions to Railway. This bridge requires that browser to be
available; it is not a server-side Eventbrite marketing API.

1. Read the campaign index and recent delivery reports. The observed DC campaign
   55883602 is Sending and its Delivery report says no delivery information yet.
   Record this as incomplete history with an active campaign, never zero contacts.
2. Read current Craft CRM selection and independently verify provider source-list
   mapping. Export subscriber CSV via Manage subscribers > Download CSV, then the
   generated Download subscribers csv link. Preserve subscription/bounce fields.
3. Run the draft worker against current CRM with fresh evidence. Preserve all hold
   reasons in the resulting package. Use the exact upload_csv recipient set.
4. Search for an existing run-specific list before creating anything. Create a
   dedicated named list, upload the headerless CSV, and wait for import completion.
   An uncertain result requires list inspection/export; never blindly re-import.
5. Export the destination and call campaign_preparation.verify_import. Require
   exact membership, eligibility and row count. Record destination ID, digest and
   observed timestamp with the run. Refresh stale evidence instead of reusing it.
6. Use templates/dc-coffee-super-spreaders.json for the approved DC copy. Preserve
   the user's current draft; create no duplicate campaign when a matching draft
   exists. Verify subject, body, event card, audience ID and recipient count. Stop
   before any Send, Schedule or Send test email action. Do not alter active sends.

The existing source-list pilot is 35083291 (DC coffee 2025), with destination
35155601. User draft campaign 55888508 is DC Super spreaders. IDs are verified
observations for this pilot, not default routing for other festivals.

## Verification and limits

Tests cover current-edition buyers, cross-list suppression, cooldown, missing
history, active campaigns, stale evidence, scope, exact membership, read-only
analytics, private outputs and forced process death after a real DBOS checkpoint.
The integration test must restart without repeating audience construction.

No public marketing campaign API has been established. Browser steps remain
agent-operated. Complete cross-provider contact-history collection and revenue
attribution are unfinished; they must be visible blockers, not implied coverage.
