# Finished social post packages

The existing six-hour maintenance cycle produces a private ready-post snapshot
after refreshing launch action packs. Launch Intelligence displays the actual
photo and caption, with a ZIP containing the unchanged image, caption and tracked
ticket links. No social publishing or ad write is implemented.

Only locally stored, visually reviewed, owned photos marked safe for cross-city
reuse are eligible. A missing photo, changed hash, symlink, unreviewed asset,
different festival category, past event or missing ticket destination cannot
produce a ready post. Houston stays held until its event details exist.

The private catalog is `reusable-reviewed-assets.json` beside `DB_PATH`, with
files in `ready-post-media/<sha256>.jpg`. Business evidence and media are never
committed to this public repository. The catalog is operator-maintained; future
asset ingestion must preserve visual review and category boundaries.

Packages retain weekly identities, refresh from existing event evidence, and
expire after 24 hours without refresh. Downloading does not mark a post published.
The owner reviews the caption and account destination before publishing. Source
engagement is descriptive, not causal sales lift, and selecting a reusable image
does not claim it is the highest-performing creative.

Reuse: existing Flask/Next.js proxy and SSO boundary, Python `zipfile`, existing
atomic snapshots and scheduler. No new scheduler, media editor or agent runtime.
