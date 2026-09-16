# Past buyers and reserved recipients

The existing CRM audience selector now accepts `past_attendees` for ticket-sales drafts. It means observed positive ticket purchases for completed events of the exact city and festival type, not verified attendance. It queries the existing analytics tables rather than the legacy substring event-name matcher. Dates must precede both today in UTC and the earliest current-edition date. Unknown event dates and zero/unknown ticket quantities do not qualify. All current-edition buyers are excluded using the existing sibling resolver. More than 50,000 candidates fails instead of truncating. Referral purpose remains limited to advocacy segments.

The existing authenticated intelligence CSV export and DBOS preparation worker both reuse this selector. No new route, database, dependency, scheduling service or sending capability is added. This extends Craft-specific selection inside the existing runtime; no replacement framework is warranted.

`contact_history.active_campaigns` can retain legacy campaign IDs, which remain unresolved, or supply observed membership:

```json
{
  "provider": "eventbrite",
  "campaign_id": "provider-id",
  "observed_at": "2026-09-16T16:00:00+00:00",
  "recipient_emails": ["synthetic@example.com"],
  "membership_complete": true
}
```

Include every scheduled/sending campaign across festivals and providers. The surrounding contact-history scope and completeness declarations must describe verified inventory, not optimistic defaults. Membership evidence must be within one hour. Exclude all known members even from partial snapshots; partial/missing membership retains `active_campaigns_unresolved`. A complete list permits overlap resolution only by subtracting its recipients. Past sent campaigns belong in timestamped contact history; do not remove a currently sending campaign's recipients merely because its first batch started more than seven days ago.

Preparation reports disjoint exclusion counts for recent contacts and reserved campaign recipients. These are preparation-time facts only. Consent/source checks, fresh pre-send recheck and external sending disabled remain in force. The user schedules provider drafts; this change does not authorize sending.

Create a new run ID when refreshing evidence. Existing durable packages are immutable historical results and must not be reused as current eligibility. Source membership, send history and current purchases still require fresh observation. Existing scheduled campaigns must not be modified by the draft agent.

Validation includes exact city/type scope despite misleading names, future and unknown dates, refund/unknown quantities, normalized duplicates, other-day buyers, unresolved edition IDs, cross-provider reserved recipients, partial/stale evidence and overlapping exclusions.
