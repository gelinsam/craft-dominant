-- V2 Postgres Phase 2: Durable send attempts for idempotent execution
-- Migration: 002_send_attempts.sql
--
-- Adds:
--   v2_send_attempts       — durable claim-before-send tracking
--   v2_send_attempt_recipients — staged audience for crash recovery
--
-- Key invariant: ONE intervention+execution_generation may have AT MOST
-- ONE active/completed send attempt.  The partial unique index enforces
-- this at the database level.
--
-- State machine:
--   claimed → provider_campaign_created → audience_configured
--   → send_requested → confirmed_sent | failed_pre_send | ambiguous
--   → reconciled_sent | reconciled_not_sent | cancelled

BEGIN;

-- ─────────────────────────────────────────────────────────
-- v2_send_attempts: one row per execution attempt
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS v2_send_attempts (
    id BIGSERIAL PRIMARY KEY,
    intervention_id TEXT NOT NULL REFERENCES interventions(id),
    execution_generation INTEGER NOT NULL DEFAULT 1,

    -- State machine
    attempt_status TEXT NOT NULL DEFAULT 'claimed'
        CHECK (attempt_status IN (
            'claimed',
            'provider_campaign_created',
            'audience_configured',
            'send_requested',
            'confirmed_sent',
            'failed_pre_send',
            'ambiguous',
            'reconciled_sent',
            'reconciled_not_sent',
            'cancelled'
        )),

    -- Idempotency
    idempotency_key TEXT NOT NULL,
    audience_hash TEXT NOT NULL,

    -- Provider state (filled incrementally as steps complete)
    provider_campaign_id TEXT,          -- Mailchimp campaign ID
    provider_tag TEXT,                  -- Mailchimp tag used for segmentation
    provider_segment_id INTEGER,        -- Mailchimp segment ID for tag

    -- Audience snapshot
    audience_count INTEGER NOT NULL DEFAULT 0,

    -- Timestamps for each checkpoint
    claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    provider_campaign_created_at TIMESTAMPTZ,
    audience_configured_at TIMESTAMPTZ,
    send_requested_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,           -- When terminal state reached

    -- Reconciliation
    reconciled_at TIMESTAMPTZ,
    reconciled_by TEXT,                 -- 'auto' or actor name
    reconciliation_detail JSONB,       -- Provider query results

    -- Error tracking
    error_message TEXT,
    error_detail JSONB,

    -- Dry-run flag: dry-run attempts do NOT block real execution
    is_dry_run BOOLEAN NOT NULL DEFAULT FALSE
);

-- Critical uniqueness constraint:
-- At most ONE non-terminal, non-dry-run attempt per intervention+generation.
-- Terminal states: confirmed_sent, failed_pre_send, reconciled_sent,
--                  reconciled_not_sent, cancelled
-- This prevents duplicate active claims.
CREATE UNIQUE INDEX IF NOT EXISTS idx_send_attempts_active_claim
    ON v2_send_attempts (intervention_id, execution_generation)
    WHERE attempt_status NOT IN (
        'confirmed_sent', 'failed_pre_send',
        'reconciled_sent', 'reconciled_not_sent', 'cancelled'
    )
    AND is_dry_run = FALSE;

-- Also prevent more than one successful send per intervention+generation.
-- A second confirmed_sent or reconciled_sent is an invariant violation.
CREATE UNIQUE INDEX IF NOT EXISTS idx_send_attempts_unique_success
    ON v2_send_attempts (intervention_id, execution_generation)
    WHERE attempt_status IN ('confirmed_sent', 'reconciled_sent')
    AND is_dry_run = FALSE;

-- Lookup indexes
CREATE INDEX IF NOT EXISTS idx_send_attempts_intervention
    ON v2_send_attempts (intervention_id);
CREATE INDEX IF NOT EXISTS idx_send_attempts_status
    ON v2_send_attempts (attempt_status);
CREATE INDEX IF NOT EXISTS idx_send_attempts_provider_campaign
    ON v2_send_attempts (provider_campaign_id)
    WHERE provider_campaign_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_send_attempts_idempotency
    ON v2_send_attempts (idempotency_key);

-- ─────────────────────────────────────────────────────────
-- v2_send_attempt_recipients: staged audience
-- Populated BEFORE provider send, promoted to v2_campaign_sends
-- after confirmed success.
-- ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS v2_send_attempt_recipients (
    id BIGSERIAL PRIMARY KEY,
    send_attempt_id BIGINT NOT NULL REFERENCES v2_send_attempts(id),
    email TEXT NOT NULL,
    staged_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(send_attempt_id, email)
);

CREATE INDEX IF NOT EXISTS idx_attempt_recipients_attempt
    ON v2_send_attempt_recipients (send_attempt_id);

-- Record this migration
INSERT INTO v2_schema_version (version, description)
VALUES (2, 'Send attempts: durable claim-before-send with partial unique indexes for at-most-once execution')
ON CONFLICT (version) DO NOTHING;

COMMIT;
