-- 003_p05_owned.sql  (PostgreSQL)
--
-- The only objects Project 05 owns. Note what is absent: no table holds a
-- risk figure as truth. The audit table holds digests, versions and statuses
-- so a result can be *re-derived* and compared; the materialized table is a
-- cache whose key contains every version that could change the answer.
--
-- Deliberately not stored: account balances, deal amounts, P&L values. An
-- audit trail that duplicates the money becomes a second source of truth that
-- nobody reconciles, and a disclosure risk at rest (WF5-281).

CREATE TABLE IF NOT EXISTS risk_symbol_catalog (
    catalog_version TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    calc_mode       TEXT NOT NULL,
    base_currency   CHAR(3) NOT NULL,
    quote_currency  CHAR(3) NOT NULL,
    profit_currency CHAR(3) NOT NULL,
    contract_size   NUMERIC(38, 12) NOT NULL CHECK (contract_size > 0),
    tick_size       NUMERIC(38, 12) NOT NULL CHECK (tick_size > 0),
    volume_step     NUMERIC(38, 12) NOT NULL CHECK (volume_step > 0),
    volume_max      NUMERIC(38, 12) NOT NULL CHECK (volume_max > 0),
    approved_hash   TEXT NOT NULL,
    effective_from  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (catalog_version, symbol)
);

CREATE TABLE IF NOT EXISTS risk_calculation_audit (
    calculation_id      UUID PRIMARY KEY,
    -- The idempotency key. Two attempts at the same calculation resolve to one
    -- receipt, which is what makes a crash between COMMIT and the client ACK
    -- recoverable rather than a duplicate (K05-15, WF5-328).
    idempotency_key     CHAR(64) NOT NULL,
    epoch_id            UUID NOT NULL,
    account_scope_hash  CHAR(64) NOT NULL,
    risk_policy_version TEXT NOT NULL,
    symbol_catalog_version TEXT NOT NULL,
    cut_manifest        JSONB NOT NULL,
    input_digest        CHAR(64) NOT NULL,
    output_digest       CHAR(64),
    status              TEXT NOT NULL CHECK (status IN
        ('READY', 'DEGRADED', 'BLOCKED', 'UNSUPPORTED', 'REJECTED')),
    reason_codes        JSONB NOT NULL DEFAULT '[]'::jsonb,
    duration_ms         BIGINT CHECK (duration_ms >= 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_risk_audit_idempotency UNIQUE (idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_risk_audit_epoch_time
    ON risk_calculation_audit (epoch_id, created_at DESC);

CREATE TABLE IF NOT EXISTS risk_materialized_results (
    cache_key     CHAR(64) PRIMARY KEY,
    calculation_id UUID NOT NULL REFERENCES risk_calculation_audit(calculation_id),
    -- Every version that can change the answer is in the key, and repeated
    -- here so an operator can see why two entries differ.
    epoch_id      UUID NOT NULL,
    recon_revision BIGINT NOT NULL,
    quote_snapshot_id TEXT,
    payload       JSONB NOT NULL,
    expires_at    TIMESTAMPTZ NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_risk_cache_expiry
    ON risk_materialized_results (expires_at);

-- ---------------------------------------------------------------------------
-- The writer role may append audit rows and nothing more. It has no rights at
-- all on the Project 03 and Project 04 tables.
-- ---------------------------------------------------------------------------
-- CREATE ROLE risk_writer NOLOGIN;
-- GRANT USAGE ON SCHEMA public TO risk_writer;
-- GRANT SELECT, INSERT ON risk_calculation_audit TO risk_writer;
-- GRANT SELECT, INSERT, DELETE ON risk_materialized_results TO risk_writer;
-- GRANT SELECT ON risk_symbol_catalog TO risk_writer, risk_reader;
-- REVOKE UPDATE, DELETE ON risk_calculation_audit FROM risk_writer;
