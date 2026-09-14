-- V2 Postgres Phase 2b: authoritative provider send timestamp
-- Migration: 003_provider_sent_at.sql
--
-- Adds v2_send_attempts.provider_sent_at — the best-known moment the
-- PROVIDER actually sent the campaign.
--
-- Why a new column rather than reusing an existing one:
--
--   completed_at   is when the attempt reached a terminal state locally.
--                  For a reconciled send that can be hours after the mail
--                  went out, so it is the wrong clock for attribution.
--   send_requested_at
--                  is when we dispatched the request, before we knew
--                  whether it was accepted. It is a lower bound, not a
--                  fact about delivery.
--
-- The attribution window must start from when customers actually
-- received the email, so it needs its own durable field.
--
-- Population priority (see execution_adapter._resolve_provider_sent_at):
--   1. Mailchimp's own `send_time` from the campaign lookup, when
--      reconciliation supplies one.
--   2. The moment we received and persisted the provider's success
--      response on the direct send path.
--   3. Left NULL only when neither is available; callers then fall back
--      to a defensible local timestamp rather than inventing one.
--
-- Nullable by design: pre-existing rows have no recorded provider send
-- time, and backfilling a guess would corrupt attribution truth.

BEGIN;

ALTER TABLE v2_send_attempts
    ADD COLUMN IF NOT EXISTS provider_sent_at TIMESTAMPTZ;

COMMENT ON COLUMN v2_send_attempts.provider_sent_at IS
    'Best-known moment the provider actually sent. Attribution window '
    'starts here. Prefer provider send_time; else provider-confirmation '
    'receipt time. Never the claim or send-request time.';

-- Lets recovery sweeps find successful attempts cheaply.
CREATE INDEX IF NOT EXISTS idx_send_attempts_provider_sent_at
    ON v2_send_attempts (provider_sent_at)
    WHERE provider_sent_at IS NOT NULL;

INSERT INTO v2_schema_version (version, description)
VALUES (3, 'Send attempts: provider_sent_at for authoritative attribution window start')
ON CONFLICT (version) DO NOTHING;

COMMIT;
