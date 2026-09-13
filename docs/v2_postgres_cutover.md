# V2 Postgres Cutover Runbook

## Overview

This document covers the cutover of V2 durable operational state from SQLite to Postgres.

**What moves:** interventions, audit log, campaign sends, learning records, suppression sync sentinel
**What stays in SQLite:** events, orders, customers, snapshots, ad_spend, suppressions, campaigns, and all analytics tables

## Prerequisites

- [ ] Postgres instance provisioned (Railway addon or external)
- [ ] `DATABASE_URL` env var ready (format: `postgresql://user:pass@host:port/dbname`)
- [ ] `psycopg[binary]` in requirements.txt (already added)
- [ ] All tests passing on `v2-postgres-foundation` branch

## Cutover Steps

### 1. Provision Postgres

```bash
# Railway: add Postgres addon to the project
# Or: use any Postgres 15+ instance
```

### 2. Run Migrations

Migrations run automatically on startup when `DATABASE_URL` is set. To run manually:

```bash
# From the application directory
python -c "
from v2_state_repository import run_postgres_migrations
import os
run_postgres_migrations(os.environ['DATABASE_URL'])
"
```

### 3. Backfill Existing Data

```bash
# Dry run first
python v2_backfill.py backfill \
  --sqlite craft_unified.db \
  --pg "$DATABASE_URL" \
  --dry-run

# If counts look correct, run for real
python v2_backfill.py backfill \
  --sqlite craft_unified.db \
  --pg "$DATABASE_URL"
```

### 4. Reconcile

```bash
python v2_backfill.py reconcile \
  --sqlite craft_unified.db \
  --pg "$DATABASE_URL"
```

Verify all tables show `in_sync: true`.

### 5. Set Environment Variable

Add `DATABASE_URL` to the Railway/Vercel deployment. The application will automatically use Postgres for V2 state on next startup.

### 6. Verify

```bash
curl -H "Authorization: Bearer $COMMAND_API_KEY" \
  https://craft-dominant-production.up.railway.app/api/v2/health
```

Expected response includes:
```json
{
  "status": "ok",
  "v2_state_backend": {
    "status": "ok",
    "backend": "postgres",
    "tables_ok": true,
    "schema_version": 1
  }
}
```

## Rollback

Remove `DATABASE_URL` from environment. The application falls back to SQLite automatically. No data loss — SQLite still has all V2 tables.

## Architecture Notes

- **Fail closed:** If `DATABASE_URL` is set but Postgres is unreachable, the app will NOT start (no silent SQLite fallback)
- **No cross-database transactions:** Analytical reads complete first, then V2 state is persisted atomically in Postgres
- **Suppression split:** The suppression email list stays in SQLite (analytics-owned). Only the sync sentinel moves to Postgres. `refresh_from_mailchimp()` becomes a two-phase write.
- **Campaigns table stays in SQLite:** It straddles analytics and V2. Only the `campaign_draft_id` linkage in interventions is in Postgres.
