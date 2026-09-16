# One-and-done win-back candidates

Segment `one_and_done` means positive ticket purchases in exactly one known edition of the same city and festival type, with that edition ending between the one-year and three-year calendar anniversaries of the preparation date (UTC, inclusive). At least one later edition must have completed before today. A purchase in any other known edition, including a future one, disqualifies the person. Current-edition buyer exclusion remains in place.

The existing `edition_sibling_ids` resolver is reused for historical editions, so multiple days, timed sessions, group tickets and repeated orders do not imply repeat attendance. Other cities and festival categories do not disqualify or qualify a person. Known cancelled/deleted editions do not count as opportunities; unknown dates and unknown ticket quantities prevent a one-and-done claim for the affected person. Vendor, sponsor and exhibitor payment events are excluded. More than 50,000 candidates fails rather than truncating.

This is an observed purchase-history cohort, not proof of attendance or complete provider history. Output includes `history_coverage: stored_records_only`, calendar window and edition count. The draft worker preserves `purchase_history_coverage_unverified` as a sending blocker. Do not erase that limitation just because an import succeeds. Independently reconcile the relevant editions with provider history before calling lapse verified. Missing historical orders can falsely make a repeat buyer appear one-and-done.

The broader `past_attendees` segment remains available for returning-buyer opportunities, including people who may buy later. It is not synonymous with win-back. NYC is a relaunch per the owner and is not a historical win-back target.

Fresh provider eligibility, cross-provider recent contacts and scheduled/sending membership exclusions still apply. This change adds no sending, scheduling, provider API, runtime or dependency. Use a new durable run ID for fresh preparation. The existing worker, authenticated CSV export and exact-membership verification are reused.
