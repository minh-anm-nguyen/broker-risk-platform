-- 001_p03_read_model.sql  (PostgreSQL)
--
-- The Project 03 read model, as Project 05 needs to see it. These tables are
-- NOT owned by this project: the file exists so that an integration environment
-- can be stood up and so that the read-only grants at the bottom are testable.
-- A deployment where Project 03 already exists applies only the GRANT section.
--
-- Two columns carry most of the design weight:
--
--   sync_runs.filter_hash       -- 'UNFILTERED' or a digest of the account
--                                  filter. A watermark derived from a filtered
--                                  scan cannot support a claim of global
--                                  completeness, so the reader checks it.
--   sync_runs.terminal_receipt  -- proof the last page arrived. Without it an
--                                  empty result set and a dead paginator are
--                                  the same observation.

CREATE TABLE IF NOT EXISTS source_epochs (
    epoch_id        UUID PRIMARY KEY,
    source_id       TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    generation      BIGINT NOT NULL CHECK (generation >= 0),
    status          TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DEGRADED', 'BLOCKED', 'ARCHIVED')),
    opened_at       TIMESTAMPTZ NOT NULL,
    closed_at       TIMESTAMPTZ,
    -- One (source, run, generation) may exist exactly once. Reusing an epoch
    -- id across generations is the identity collision of WF5-016.
    CONSTRAINT uq_source_epoch_identity UNIQUE (source_id, run_id, generation)
);

CREATE TABLE IF NOT EXISTS sync_runs (
    scan_id           TEXT PRIMARY KEY,
    epoch_id          UUID NOT NULL REFERENCES source_epochs(epoch_id),
    entity            TEXT NOT NULL CHECK (entity IN ('ACCOUNT', 'POSITION', 'DEAL')),
    status            TEXT NOT NULL CHECK (status IN ('STAGED', 'PUBLISHED', 'ABANDONED')),
    filter_hash       TEXT NOT NULL,
    terminal_receipt  BOOLEAN NOT NULL DEFAULT FALSE,
    row_count         BIGINT NOT NULL DEFAULT 0 CHECK (row_count >= 0),
    published_at      TIMESTAMPTZ,
    -- Present only when an upstream freeze produced several entity scans
    -- together; this is the only thing that can raise a cut to COMPATIBLE.
    cut_proof_id      TEXT,
    source_snapshot_seq BIGINT
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_epoch_entity
    ON sync_runs (epoch_id, entity, status, published_at DESC);

CREATE TABLE IF NOT EXISTS sync_checkpoints (
    epoch_id            UUID NOT NULL REFERENCES source_epochs(epoch_id),
    stream              TEXT NOT NULL,
    watermark_seq       BIGINT NOT NULL CHECK (watermark_seq >= 0),
    filter_hash         TEXT NOT NULL,
    terminal            BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at          TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (epoch_id, stream)
);

CREATE TABLE IF NOT EXISTS accounts_current (
    epoch_id          UUID NOT NULL REFERENCES source_epochs(epoch_id),
    account_id        TEXT NOT NULL,
    currency          TEXT NOT NULL,
    balance           NUMERIC(38, 12),
    presence          TEXT NOT NULL CHECK (presence IN ('PRESENT', 'ABSENT')),
    published_scan_id TEXT NOT NULL REFERENCES sync_runs(scan_id),
    payload_hash      CHAR(64) NOT NULL,
    PRIMARY KEY (epoch_id, account_id)
);

CREATE TABLE IF NOT EXISTS positions_current (
    epoch_id          UUID NOT NULL REFERENCES source_epochs(epoch_id),
    position_id       TEXT NOT NULL,
    account_id        TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    side              TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    volume_lots       NUMERIC(38, 12) NOT NULL,
    open_price        NUMERIC(38, 12) NOT NULL,
    source_status     TEXT NOT NULL,
    presence          TEXT NOT NULL CHECK (presence IN ('PRESENT', 'ABSENT')),
    published_scan_id TEXT NOT NULL REFERENCES sync_runs(scan_id),
    payload_hash      CHAR(64) NOT NULL,
    PRIMARY KEY (epoch_id, position_id)
);

CREATE INDEX IF NOT EXISTS idx_positions_account
    ON positions_current (epoch_id, account_id, symbol);

CREATE TABLE IF NOT EXISTS deals (
    epoch_id          UUID NOT NULL REFERENCES source_epochs(epoch_id),
    deal_id           TEXT NOT NULL,
    account_id        TEXT NOT NULL,
    source_seq        BIGINT NOT NULL CHECK (source_seq >= 0),
    entry_kind        TEXT NOT NULL,
    deal_type         TEXT,
    side              TEXT,
    symbol            TEXT,
    -- NULL is meaningful here and must never be coalesced to zero at read
    -- time: a missing money field blocks the component (WF5-005).
    realized_pnl_gross NUMERIC(38, 12),
    commission        NUMERIC(38, 12),
    swap              NUMERIC(38, 12),
    money_currency    TEXT,
    executed_at       TIMESTAMPTZ NOT NULL,
    payload_hash      CHAR(64) NOT NULL,
    PRIMARY KEY (epoch_id, deal_id)
);

-- Supports the hot path: one account, one epoch, a watermark ceiling and a
-- half-open execution window (section 15.5).
CREATE INDEX IF NOT EXISTS idx_deals_account_seq
    ON deals (epoch_id, account_id, source_seq);
CREATE INDEX IF NOT EXISTS idx_deals_account_time
    ON deals (epoch_id, account_id, executed_at);

-- ---------------------------------------------------------------------------
-- Grants. The reader role gets SELECT and nothing else, on purpose: the
-- negative test for this project asserts that an UPDATE on deals is refused by
-- the database, not merely avoided by the code (G05-08, WF5-196, WF5-197).
-- ---------------------------------------------------------------------------
-- CREATE ROLE risk_reader NOLOGIN;
-- GRANT USAGE ON SCHEMA public TO risk_reader;
-- GRANT SELECT ON source_epochs, sync_runs, sync_checkpoints,
--                 accounts_current, positions_current, deals TO risk_reader;
-- REVOKE INSERT, UPDATE, DELETE, TRUNCATE
--     ON source_epochs, sync_runs, sync_checkpoints,
--        accounts_current, positions_current, deals FROM risk_reader;
