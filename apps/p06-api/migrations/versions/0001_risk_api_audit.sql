-- Project 06 owned schema: the financial-read audit trail.
--
-- Scope and ownership (design doc section 8):
--   * Project 06 owns ONLY this schema. It holds no INSERT/UPDATE/DELETE grant
--     on any Project 03 or Project 04 table.
--   * Rows are append-only. There is deliberately no UPDATE or DELETE grant for
--     the application role; retention is handled by dropping whole partitions.
--   * Nothing here stores an account number, a token or a money value. Scope is
--     proven by hash, and results by digest.
--
-- Status: this DDL is the committed shape of the audit store. The shipped build
-- runs the in-memory and JSONL backends (see repositories/audit_store.py); the
-- PostgreSQL backend needs a driver this build does not depend on. Apply this
-- file before wiring that backend.

BEGIN;

CREATE SCHEMA IF NOT EXISTS risk_api;

-- --------------------------------------------------------------------------
-- Access audit, partitioned monthly so retention is a partition drop.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk_api.api_access_audit (
    audit_id              uuid        NOT NULL,
    request_id            text        NOT NULL,
    principal_scope_hash  text        NOT NULL,
    endpoint_id           text        NOT NULL,
    decision              text        NOT NULL,
    result_status         text        NULL,
    denial_family         text        NULL,
    input_digest          text        NULL,
    output_digest         text        NULL,
    data_cut_digest       text        NULL,
    risk_policy_version   text        NULL,
    recon_revision        text        NULL,
    created_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT api_access_audit_pkey PRIMARY KEY (audit_id, created_at),
    CONSTRAINT api_access_audit_decision_chk
        CHECK (decision IN ('ALLOW', 'DENY', 'BLOCK', 'ERROR')),
    CONSTRAINT api_access_audit_status_chk
        CHECK (result_status IS NULL OR result_status IN
            ('READY', 'DEGRADED', 'BLOCKED', 'UNSUPPORTED', 'INCONCLUSIVE')),
    -- A scope hash short enough to be a raw identifier would defeat the point.
    CONSTRAINT api_access_audit_scope_hash_chk
        CHECK (length(principal_scope_hash) >= 32),
    CONSTRAINT api_access_audit_digest_shape_chk
        CHECK (
            (input_digest IS NULL OR input_digest LIKE 'sha256:%') AND
            (output_digest IS NULL OR output_digest LIKE 'sha256:%') AND
            (data_cut_digest IS NULL OR data_cut_digest LIKE 'sha256:%')
        ),
    -- An ALLOW decision that reports no result status would be unexplainable.
    CONSTRAINT api_access_audit_allow_needs_status_chk
        CHECK (decision <> 'ALLOW' OR result_status IS NOT NULL)
) PARTITION BY RANGE (created_at);

CREATE INDEX IF NOT EXISTS ix_api_access_audit_created_at
    ON risk_api.api_access_audit (created_at DESC);
CREATE INDEX IF NOT EXISTS ix_api_access_audit_request
    ON risk_api.api_access_audit (request_id);
CREATE INDEX IF NOT EXISTS ix_api_access_audit_scope
    ON risk_api.api_access_audit (principal_scope_hash, created_at DESC);

-- One starting partition. Operations creates the rolling ones; a missing
-- partition must fail the insert loudly rather than silently route elsewhere,
-- because an unaudited report is not allowed to be sent.
CREATE TABLE IF NOT EXISTS risk_api.api_access_audit_2026_10
    PARTITION OF risk_api.api_access_audit
    FOR VALUES FROM ('2026-10-01 00:00:00+00') TO ('2026-11-01 00:00:00+00');

-- --------------------------------------------------------------------------
-- Supporting evidence rows, one-to-many against an audit record.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS risk_api.api_audit_evidence (
    evidence_id      uuid        NOT NULL PRIMARY KEY,
    audit_id         uuid        NOT NULL,
    audit_created_at timestamptz NOT NULL,
    policy_version   text        NULL,
    recon_revision   text        NULL,
    artifact_hash    text        NULL,
    note             text        NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT api_audit_evidence_hash_shape_chk
        CHECK (artifact_hash IS NULL OR artifact_hash LIKE 'sha256:%'),
    -- Free text is for coarse markers such as 'final_check_passed'.
    CONSTRAINT api_audit_evidence_note_len_chk
        CHECK (note IS NULL OR length(note) <= 200)
);

CREATE INDEX IF NOT EXISTS ix_api_audit_evidence_audit
    ON risk_api.api_audit_evidence (audit_id);

-- --------------------------------------------------------------------------
-- Grants. The application role appends and reads; it cannot rewrite history.
-- --------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'risk_api_app') THEN
        CREATE ROLE risk_api_app NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'risk_api_auditor') THEN
        CREATE ROLE risk_api_auditor NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA risk_api TO risk_api_app, risk_api_auditor;

GRANT SELECT, INSERT ON risk_api.api_access_audit    TO risk_api_app;
GRANT SELECT, INSERT ON risk_api.api_audit_evidence  TO risk_api_app;
REVOKE UPDATE, DELETE, TRUNCATE ON risk_api.api_access_audit   FROM risk_api_app;
REVOKE UPDATE, DELETE, TRUNCATE ON risk_api.api_audit_evidence FROM risk_api_app;

GRANT SELECT ON ALL TABLES IN SCHEMA risk_api TO risk_api_auditor;

-- --------------------------------------------------------------------------
-- Read-side contract against Projects 03 and 04.
--
-- These views are NOT created here: they are owned upstream and must be added
-- as additive, versioned interfaces in those projects. The names and columns
-- below are the contract this build's capability handshake checks for, and the
-- grant must be SELECT only.
--
--   v_risk_epoch_status_v1(source_id, epoch_id, status, revision)
--   v_risk_published_cut_v1(epoch_id, account_scan_id, position_scan_id,
--                           deal_watermark_seq, publication_revision,
--                           accounts_published, positions_published,
--                           deals_complete, published_at_utc)
--   v_risk_reconciliation_v1(epoch_id, scope_hash, recon_revision,
--                            blocking_reason_family)
--
-- Expected upstream grant, to be applied in the Project 03/04 migrations:
--   GRANT SELECT ON sync.v_risk_epoch_status_v1     TO risk_api_app;
--   GRANT SELECT ON sync.v_risk_published_cut_v1    TO risk_api_app;
--   GRANT SELECT ON recon.v_risk_reconciliation_v1  TO risk_api_app;
-- and explicitly no INSERT, UPDATE or DELETE on any upstream object.
-- --------------------------------------------------------------------------

COMMIT;
