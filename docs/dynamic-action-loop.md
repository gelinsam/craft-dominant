# Dynamic actions and campaign feedback

The existing pacing dashboard remains the home. Its Actions panel reads an authenticated, read-only action plan built with the existing OpportunityEngine and DiagnosisEngine. Timed-entry siblings share an edition action. Rankings remain confidence-weighted estimates, not measured incremental revenue. Sales freshness and integrity warnings are explicit. Active V2 interventions are shown before preparing duplicate work; Eventbrite draft snapshots remain dated observations, not live provider state.

## Reuse decisions

- Reuse Craft's edition resolution, diagnosis, audience routing, suppression and existing six-hour scheduler. Do not replace domain logic with a generic agent framework.
- Adopt Mailchimp's official `mailchimp-marketing` Python SDK, pinned to 3.0.80, for read-only report retrieval: https://github.com/mailchimp/mailchimp-marketing-python . The existing send adapter is unchanged.
- Reuse `lib/campaign-intelligence.mjs` in the browser for comparisons. No duplicate Python ranking implementation and no new Node runtime on Railway.
- Existing DBOS worker remains the durable draft preparation mechanism. No second orchestration service is needed for this bounded read-only refresh.

## Feedback contract

Every existing campaign scheduler cycle reads Mailchimp reports sent in the last 90 days, with validated pagination and a request timeout. A complete snapshot is atomically stored beside DB_PATH on the persistent volume. Failed or partial refreshes preserve the last good snapshot and expose a generic failure without raw provider errors. The private compressed evidence environment variable seeds older history; no campaign/customer data is committed to the public repository.

Unknown metrics never overwrite observed metrics with zero. Observed zero is a valid correction. Existing reviewed content/category labels are preserved. Newly observed campaigns are unverified until their category and creative are reviewed. Eventbrite remains imported partial evidence. These limitations are visible in the dashboard. The browser recalculates eligible comparisons and refreshes once per minute while visible; page refresh does not perform a provider sales sync.

A successful reporting connection does not authorize sending or prove list consent. Meta configuration is displayed as access unverified until live evidence establishes otherwise. Mailchimp audience mapping must be explicit for every edition sibling; no global list fallback.

## Contact policy: provider_delivery_aware_v1

The Eventbrite preparer accepts only the Eventbrite destination because its source exports establish Eventbrite consent. It does not grant Mailchimp consent.

- Same-provider delivered/opened/clicked recipients observe the configured cooldown (default seven days).
- Same-provider scheduled/sending memberships remain exclusions; incomplete membership holds readiness.
- Other-provider deliveries and pending campaigns are advisory context, not blanket exclusions. A Mailchimp send does not prove Eventbrite delivery.
- Same-provider attempted/sent/accepted or unknown-provider observations hold readiness until delivery is clarified. Missing delivery evidence is not a failed delivery.
- Known failed/bounced delivery does not itself count as successful contact. Independent fresh consent, unsubscribe and bounce denials still apply.
- Current-edition buyers and one-and-done purchase-history coverage keep their existing semantics.

## Rollout and verification

Run SQLite and Postgres regression suites, frontend tests and production build. Before deployment confirm `/api/sync-status` has `running: false` and no interrupted runs. After deployment verify authenticated action/evidence responses, anonymous rejection, the first real SDK refresh, preserved pacing totals and dashboard rendering. Both endpoints are read-only and use existing proxy/backend authentication. V2 external sends remain disabled; recommendations cannot send, schedule or alter ads.

## Invalid CRM identities

Candidate selection reuses the existing email validation boundary but quarantines invalid source records instead of aborting all valid candidates. It does not repair or mutate identities. The candidate result and preparation manifest contain `quarantined_invalid_email_records`; CSV responses include `X-CRM-Quarantined-Records` and the exported candidate count. Database lookup failures and truncated queries still fail closed. Provider eligibility exports retain their stricter failure behavior because partial consent evidence must not silently authorize a recipient.
