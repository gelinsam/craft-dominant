# Pacing and actions in one home

`/` remains the main dashboard. The existing pacing cards, charts, decisions and totals are retained. A compact Needs attention strip opens an Actions tab; `/command` retains the detailed intervention workflow and links home.

## Reuse

The home consumes the existing authenticated `/api/command` relay and deterministic OpportunityEngine. It does not duplicate ranking, pricing, targeting or send logic. React/Next.js and the existing Tailwind design system supply the interface. The existing DBOS draft worker (https://github.com/dbos-inc/dbos-transact-py) remains responsible for durable preparation; no additional agent framework, scheduler or license-bearing copied code is introduced for this presentation task. Anthropic's SDK and MCP were evaluated as possible future tool interfaces, but are not needed to render already-computed decisions.

## Evidence and limits

`data/festival-drafts.json` is a checked-in, aggregate-only registry of the September 16, 2026 browser-verified Eventbrite batch. It contains no customer addresses, credentials or send capability. Its links identify exact festival editions, not interchangeable cities/types. It is a dated snapshot, not provider synchronization. Update records only after a real provider check; never advance verification dates during a build or page refresh.

Thirteen drafts have saved recipient selections. NYC has no qualified audience and is blocked. DC Coffee was already scheduled by the owner. San Francisco's displayed active count differs from its eligible export count; the discrepancy is disclosed. Draft evidence older than 24 hours is explicitly labeled for audience refresh. Every draft still requires fresh buyer, consent and recent-contact checks before scheduling. The label is guidance, not a send authorization or enforcement boundary.

Command data refreshes when the dashboard's data timestamp changes or the user requests it. Failed or incomplete responses cannot become an all-clear. Generation time is analysis time, not source sync time. Results for Eventbrite sends are not connected and are not fabricated from draft existence.

No background monitoring, recurring Eventbrite browser creation or causal learning is added in this change. No backend routes, auth boundaries, suppression state, send flags or customer records change. Vercel Deployment Protection remains the human access boundary; COMMAND_API_KEY authenticates the existing frontend relay to Railway.
