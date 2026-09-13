-- V2 Postgres Phase 1: Durable operational state
-- Migration: 001_initial_schema.sql
--
-- Creates all V2 state tables in Postgres with proper types:
--   - TIMESTAMPTZ for all timestamps (not TEXT)
--   - JSONB for evidence, metadata, conversion_assumptions
--   - UUID-compatible TEXT for IDs (preserving existing SHA-256 hex IDs)
--   - BOOLEAN for approval_required (not INTEGER)
--
-- Tables migrated from SQLite:
--   interventions, intervention_audit_log, v2_campaign_sends,
--   v2_learning_records, v2_suppression_sync
--
-- Tables NOT migrated (stay in SQLite):
--   campaigns (shared with analytics), suppressions (analytics-owned),
--   events, orders, customers, daily_snapshots, pacing_curves,
--   ad_spend, customer_event_profiles, analysis_cache, auto_exports, alert_log

BEGIN;

-- Schema version tracking
CREATE TABLE IF NOT EXISTS v2_schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    description TEXT NOT NULL
);

-- Interventions: core V2 state machine
CREATE TABLE IF NOT EXISTS interventions (
    id TEXT PRIMARY KEY,
    opportunity_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    intervention_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'new'
        CHECK (status IN ('new', 'investigated', 'proposed', 'approved',
                          'executing', 'measuring', 'learned', 'rejected', 'cancelled')),
    rationale TEXT NOT NULL DEFAULT '',
    audience_definition TEXT NOT NULL DEFAULT '',
    expected_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    expected_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    expected_net_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    approval_required BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    approved_at TIMESTAMPTZ,
    executed_at TIMESTAMPTZ,
    measurement_window INTEGER NOT NULL DEFAULT 14,
    actual_revenue DOUBLE PRECISION,
    actual_cost DOUBLE PRECISION,
    actual_net_value DOUBLE PRECISION,
    outcome_status TEXT,
    evidence JSONB NOT NULL DEFAULT '{}',
    campaign_draft_id TEXT,
    measurement_started_at TIMESTAMPTZ,
    measurement_ends_at TIMESTAMPTZ,
    attributed_orders INTEGER,
    attributed_tickets INTEGER,
    attributed_revenue DOUBLE PRECISION,
    sent_count INTEGER
);

CREATE INDEX IF NOT EXISTS idx_interventions_opportunity ON interventions(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_interventions_event ON interventions(event_id);
CREATE INDEX IF NOT EXISTS idx_interventions_status ON interventions(status);
CREATE INDEX IF NOT EXISTS idx_interventions_created ON interventions(created_at);

-- Audit log: append-only intervention lifecycle events
CREATE TABLE IF NOT EXISTS intervention_audit_log (
    id BIGSERIAL PRIMARY KEY,
    intervention_id TEXT NOT NULL REFERENCES interventions(id),
    event_id TEXT NOT NULL,
    action TEXT NOT NULL,
    from_status TEXT,
    to_status TEXT,
    actor TEXT NOT NULL DEFAULT 'system',
    timestamp TIMESTAMPTZ NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_intervention ON intervention_audit_log(intervention_id);
CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON intervention_audit_log(timestamp);

-- Campaign sends: per-recipient send tracking for attribution
CREATE TABLE IF NOT EXISTS v2_campaign_sends (
    id BIGSERIAL PRIMARY KEY,
    intervention_id TEXT NOT NULL,
    campaign_draft_id TEXT NOT NULL,
    email TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    UNIQUE(intervention_id, email)
);

CREATE INDEX IF NOT EXISTS idx_v2_sends_intervention ON v2_campaign_sends(intervention_id);
CREATE INDEX IF NOT EXISTS idx_v2_sends_email ON v2_campaign_sends(email);

-- Learning records: structured outcomes after measurement
CREATE TABLE IF NOT EXISTS v2_learning_records (
    id BIGSERIAL PRIMARY KEY,
    intervention_id TEXT NOT NULL UNIQUE,
    intervention_type TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL DEFAULT '',
    city TEXT NOT NULL DEFAULT '',
    predicted_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    attributed_revenue DOUBLE PRECISION NOT NULL DEFAULT 0,
    prediction_error DOUBLE PRECISION NOT NULL DEFAULT 0,
    audience_count INTEGER NOT NULL DEFAULT 0,
    sent_count INTEGER NOT NULL DEFAULT 0,
    attributed_orders INTEGER NOT NULL DEFAULT 0,
    attributed_tickets INTEGER NOT NULL DEFAULT 0,
    actual_conversion_rate DOUBLE PRECISION,
    conversion_assumptions JSONB NOT NULL DEFAULT '{}',
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    measurement_window_days INTEGER NOT NULL DEFAULT 7,
    measurement_started_at TIMESTAMPTZ,
    measurement_ended_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_learning_event ON v2_learning_records(event_id);

-- Suppression sync sentinel: single-row metadata about suppression freshness
-- The actual suppression email list stays in SQLite (analytics-owned).
-- This sentinel tracks WHEN and HOW the suppression list was last verified.
CREATE TABLE IF NOT EXISTS v2_suppression_sync (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_synced_at TIMESTAMPTZ NOT NULL,
    row_count INTEGER NOT NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
    empty_acknowledged BOOLEAN NOT NULL DEFAULT FALSE,
    acknowledged_by TEXT,
    acknowledged_at TIMESTAMPTZ,
    acknowledged_reason TEXT,
    last_full_refresh_at TIMESTAMPTZ,
    last_mutation_at TIMESTAMPTZ,
    last_full_refresh_source TEXT,
    last_mutation_source TEXT
);

-- Record this migration
INSERT INTO v2_schema_version (version, description)
VALUES (1, 'Initial V2 state schema: interventions, audit log, sends, learning, suppression sentinel')
ON CONFLICT (version) DO NOTHING;

COMMIT;
