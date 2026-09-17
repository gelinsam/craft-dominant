# Automatic launch evidence

The existing six-hour maintenance callback now preserves aggregate sales, candidate spend and current creative observations in a private SQLite archive beside DB_PATH. Identical same-day observations are deduplicated. New versions append; previous payloads remain available. A daily Meta refresh reads all creative pages and records observation time, while preserving known fields when the same creative omits them. Failed reads do not advance the current Meta snapshot.

The protected launch view shows archive count, last observation and stale/unavailable states. Capture begins at deployment: it does not reconstruct earlier creative versions, prove an ad was delivering at observation time, or establish causal lift. Old seed creative dates remain unknown. Source timestamps remain attached; sales snapshots reflect stored orders and existing source-quality warnings. Full provider permissions and completeness remain external constraints.

## Reuse decision

Reviewed dlt (incremental extraction), DBOS (durable workflows), and Langfuse (agent evaluation). They address broader needs but are unnecessary for adding version retention to the existing working collector. This change reuses SQLite transactions, Python gzip/SHA-256, existing authenticated Meta transport, existing maintenance scheduling and the protected launch endpoint. No new framework, scheduler or external telemetry service is installed. Future workflow migration should avoid duplicate schedulers.

No customer rows or credentials are archived. Campaign and creative fields are allowlisted. Files are mode 0600, compressed and outside GitHub. Each payload has a 40 MB uncompressed limit; archive retention is append-only and requires future capacity monitoring. No sends, ad mutations or budget changes are performed.

Validation covers preserved versions, retry deduplication, unknown values, excluded sensitive fields, oversized payload rejection, missing/corrupt/stale history, complete creative pagination, and preservation of current evidence after a provider failure.
