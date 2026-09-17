# Tracking readiness

`/tracking` consolidates the retired setup board inside the authenticated application.
The registry and observations live beside `DB_PATH`, never in the public repository.

The existing six-hour maintenance runner reads each registered Meta pixel through
the existing Meta transport. It stores the pixel identity, name, last-fired time,
availability flag and observation time. Provider errors preserve the previous
observation while explicitly marking the current read unavailable. Reads expire
after 24 hours. No provider configuration or advertising spend is changed.

Imported CAPI and website setup labels are historical claims. A successful Meta
read proves visibility only, not browser installation, purchase delivery,
deduplication or attribution. Current event editions without their own imported
reference remain outstanding even when a previous edition had a setup record.

Reuses the existing authenticated proxy, private atomic snapshots, edition identity,
Meta transport and scheduler. Field names follow Meta's official Python SDK
`facebook_business/adobjects/adspixel.py`; no new scheduler or SDK dependency.

The authenticated read-only API is `/api/intelligence/tracking`. Anonymous callers
are denied and responses are not cached. Tests cover boundary authentication,
edition separation, failed reads, stale observations and identity mismatches.
