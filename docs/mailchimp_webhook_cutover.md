# Mailchimp webhook cutover — signed deliveries

Operational runbook for moving `/api/webhook/mailchimp` onto Mailchimp's signed
audience webhooks. **Nothing here has been executed.** Every step is manual and
touches a live provider.

Context: the endpoint previously verified nothing, so any caller could suppress
an arbitrary address and move the suppression sentinel with it. The handler now
requires a valid `X-Mailchimp-Signature` and fails closed when
`MAILCHIMP_WEBHOOK_SIGNING_SECRET` is unset.

## What the code consumes — do not narrow the registration

`process_mailchimp_webhook` (`craft_engine.py:1490-1505`) branches on:

| `type` | Effect |
|---|---|
| `unsubscribe` | writes a suppression with reason `unsubscribe`, updates the sentinel |
| `cleaned` | writes a suppression with reason `bounce`, updates the sentinel |
| `campaign` | writes an `email_events` row |
| anything else | recorded in `email_events` |

So the registration must include **Unsubscribes** and **Cleaned addresses** at
minimum, plus **Campaign sending** if campaign telemetry is wanted. Mailchimp's
own guide walks through "Subscribes and Unsubscribes" as a tutorial example —
that is not this system's configuration, and copying it would silently stop
bounce ingestion.

Also preserve the **event sources**. Mailchimp lets a webhook fire only for
changes made by a subscriber, by an account admin, or via the API. If the
existing webhook has all three and a replacement has only the first, an
unsubscribe performed by an admin in the Mailchimp UI never reaches us and that
person stays in future audiences.

**Before changing anything, record the existing registration**: callback URL,
every checked event type, every checked source, and whether it was created with
signing. `GET /lists/{list_id}/webhooks` returns the current configuration.

## Signing secret

The secret is displayed **once**, at creation, and cannot be retrieved later.

- If the current webhook was created with signing **and the secret was saved**,
  reuse it. No recreation is needed — install it as
  `MAILCHIMP_WEBHOOK_SIGNING_SECRET` and skip to verification.
- If it was created without signing, **or** the secret was not kept, a
  replacement webhook must be created to obtain a new one.

## Cutover, ordered to avoid a silent ingestion gap

The endpoint fails closed, so between deploying the backend and installing a
valid secret **no suppression events are ingested**. External sending is
disabled, so nothing is emailed during that window — but unsubscribes occurring
in it are still lost unless reconciled at the end.

1. **Inspect** the existing webhook and record its full configuration.
2. **Create the replacement signed webhook** pointing at the same callback URL,
   with the same event types and sources. Copy the signing secret immediately.
   Leave the old registration in place temporarily to preserve rollback options.
   The handler accepts only one configured signing key: after the key changes,
   old unsigned deliveries or deliveries signed with the old key are rejected.
   Keeping both registrations does not provide an active ingestion fallback or
   guarantee uninterrupted delivery. Suppression inserts are idempotent, but
   duplicate accepted deliveries can append duplicate email_events telemetry.
3. **Install** `MAILCHIMP_WEBHOOK_SIGNING_SECRET` in Railway.
4. **Deploy** the backend and restart so the variable is live.
   Acceptance before this step depends on the previously deployed handler and
   its configured secrets. Keep the cutover window short and record its bounds.
5. **Verify with a genuine event**: unsubscribe a test contact and confirm the
   suppression row appears with reason `unsubscribe`, the sentinel's `row_count`
   matches the table, and `last_mutation_source` reads `webhook_unsubscribe`.
   Repeat for a bounce if one can be produced safely.
6. **Confirm rejection** — an unsigned POST must return 401 and write nothing.
7. **Delete the old registration** only once 5 and 6 both pass. Keeping it until
   then preserves rollback options, not an active ingestion fallback.
8. **Reconcile** before sending is re-enabled: run
   `POST /api/v2/suppressions/refresh` so the authoritative list is rebuilt from
   Mailchimp. This is what repairs anything missed during the window in step 4 —
   webhook mutations alone never establish completeness, which is why
   `SuppressionGuard.validate()` returns `NEVER_SYNCED` until a full refresh has
   run. Require a successful refresh and a HEALTHY result from the production
   SuppressionGuard before any later decision to enable sending. A failed
   refresh or an unhealthy guard blocks sending; keep sending disabled throughout.

## Optional second factor

`MAILCHIMP_WEBHOOK_PATH_SECRET` adds a hard-to-guess component to the callback
URL, which Mailchimp also recommends. It is a **different** secret from the
signing key and can never substitute for the signature. If used, note that query
strings reach access logs and proxy logs; scrub `secret=` at the logging layer.

## What the signature does and does not give us

It proves the delivery came from Mailchimp and that the body was not altered,
and it bounds replay of a captured delivery to five minutes. It is **not**
deduplication: the same valid delivery arriving twice inside that window is
accepted twice. Suppression inserts are idempotent, but the existing processor
also appends email_events rows, so duplicate deliveries can duplicate telemetry.
The freshness check does not provide delivery deduplication or exactly-once
processing.
