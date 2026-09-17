# Festival-specific measurement evidence

The existing Eventbrite sync now records private per-event completeness receipts.
An order traversal must contain an orders array and explicit pagination completion;
all source orders must parse uniquely, stored/source counts must match, ticket and
gross fields must be observed, and ticket totals must not decrease. An exception,
unknown value, skipped order, missing sibling or inconsistent count keeps the event
incomplete. Legitimate refunds can therefore still require a later clean refresh.

Receipts are published only after the existing full sync completes and reference
its exact run ID and finish timestamp. No extra scheduler, provider mutations or
database schema is introduced. Receipts contain only event IDs, counts and times.

Scheduled measurement can use a complete current festival edition even when an
unrelated historical event failed. Every sibling session needs a fresh receipt.
The earliest pagination start must be after the measurement window ends before
final measurement. Older/mismatched/interrupted receipts cannot authorize it.
The portfolio-level warning remains visible; this does not mark the full sync clean.

Legacy clean syncs retain their existing gate until event receipts are available.
Warning-bearing legacy syncs remain held. Existing receipt-bearing runs use their
event-specific evidence even if the overall run is clean. Current production holds
are not retroactively cleared by deploying this feature.

This schedules attributed measurement through the existing adapter only. It never
sends, publishes, changes spend or presents attribution as incremental sales lift.
