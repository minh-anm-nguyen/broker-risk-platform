-- sqlite_harness.sql
--
-- The same read model expressed for SQLite, so the integration suite runs real
-- SQL with no server. Two deliberate differences from the PostgreSQL files:
--
--   * Money and prices are TEXT, not NUMERIC. SQLite's type affinity would
--     hand a NUMERIC column back as a float, and a float that has been through
--     binary representation is already wrong -- no later conversion repairs it.
--     TEXT keeps the exact decimal literal all the way to parse_decimal().
--
--   * Timestamps are TEXT in ISO-8601 with an explicit offset. The reader
--     rejects any timestamp without one, so a naive string here fails loudly
--     instead of being interpreted in the server's local zone.
--
-- Everything else -- keys, the filter_hash and terminal_receipt columns, the
-- indexes the hot path needs -- matches the PostgreSQL schema, because those
-- are the parts the reader's correctness depends on.

CREATE TABLE source_epochs (
    epoch_id    TEXT PRIMARY KEY,
    source_id   TEXT NOT NULL,
    run_id      TEXT NOT NULL,
    generation  INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DEGRADED', 'BLOCKED', 'ARCHIVED')),
    opened_at   TEXT NOT NULL,
    closed_at   TEXT,
    UNIQUE (source_id, run_id, generation)
);

CREATE TABLE sync_runs (
    scan_id             TEXT PRIMARY KEY,
    epoch_id            TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    entity              TEXT NOT NULL CHECK (entity IN ('ACCOUNT', 'POSITION', 'DEAL')),
    status              TEXT NOT NULL CHECK (status IN ('STAGED', 'PUBLISHED', 'ABANDONED')),
    filter_hash         TEXT NOT NULL,
    terminal_receipt    INTEGER NOT NULL DEFAULT 0,
    row_count           INTEGER NOT NULL DEFAULT 0,
    published_at        TEXT,
    cut_proof_id        TEXT,
    source_snapshot_seq INTEGER
);

CREATE INDEX idx_sync_runs_epoch_entity
    ON sync_runs (epoch_id, entity, status, published_at DESC);

CREATE TABLE sync_checkpoints (
    epoch_id      TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    stream        TEXT NOT NULL,
    watermark_seq INTEGER NOT NULL,
    filter_hash   TEXT NOT NULL,
    terminal      INTEGER NOT NULL DEFAULT 0,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (epoch_id, stream)
);

CREATE TABLE accounts_current (
    epoch_id          TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    account_id        TEXT NOT NULL,
    currency          TEXT NOT NULL,
    balance           TEXT,
    presence          TEXT NOT NULL CHECK (presence IN ('PRESENT', 'ABSENT')),
    published_scan_id TEXT NOT NULL REFERENCES sync_runs(scan_id),
    payload_hash      TEXT NOT NULL,
    PRIMARY KEY (epoch_id, account_id)
);

CREATE TABLE positions_current (
    epoch_id          TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    position_id       TEXT NOT NULL,
    account_id        TEXT NOT NULL,
    symbol            TEXT NOT NULL,
    side              TEXT NOT NULL CHECK (side IN ('BUY', 'SELL')),
    volume_lots       TEXT NOT NULL,
    open_price        TEXT NOT NULL,
    source_status     TEXT NOT NULL,
    presence          TEXT NOT NULL CHECK (presence IN ('PRESENT', 'ABSENT')),
    published_scan_id TEXT NOT NULL REFERENCES sync_runs(scan_id),
    payload_hash      TEXT NOT NULL,
    PRIMARY KEY (epoch_id, position_id)
);

CREATE INDEX idx_positions_account
    ON positions_current (epoch_id, account_id, symbol);

CREATE TABLE deals (
    epoch_id           TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    deal_id            TEXT NOT NULL,
    account_id         TEXT NOT NULL,
    source_seq         INTEGER NOT NULL,
    entry_kind         TEXT NOT NULL,
    deal_type          TEXT,
    side               TEXT,
    symbol             TEXT,
    realized_pnl_gross TEXT,
    commission         TEXT,
    swap               TEXT,
    money_currency     TEXT,
    executed_at        TEXT NOT NULL,
    payload_hash       TEXT NOT NULL,
    PRIMARY KEY (epoch_id, deal_id)
);

CREATE INDEX idx_deals_account_seq
    ON deals (epoch_id, account_id, source_seq);
CREATE INDEX idx_deals_account_time
    ON deals (epoch_id, account_id, executed_at);

CREATE TABLE recon_evidence (
    evidence_id            TEXT PRIMARY KEY,
    epoch_id               TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    revision               INTEGER NOT NULL,
    status                 TEXT NOT NULL,
    covered_accounts       TEXT NOT NULL,
    covered_deal_watermark INTEGER,
    scope_known            INTEGER NOT NULL DEFAULT 1,
    created_at             TEXT NOT NULL,
    UNIQUE (epoch_id, revision)
);

CREATE TABLE recon_findings (
    finding_id   TEXT PRIMARY KEY,
    evidence_id  TEXT NOT NULL REFERENCES recon_evidence(evidence_id),
    epoch_id     TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    severity     TEXT NOT NULL CHECK (severity IN ('BLOCKING', 'WARNING', 'INFO')),
    account_id   TEXT,
    symbol       TEXT,
    deal_id      TEXT,
    finding_code TEXT NOT NULL,
    created_at   TEXT NOT NULL
);

CREATE TABLE risk_calculation_audit (
    calculation_id         TEXT PRIMARY KEY,
    idempotency_key        TEXT NOT NULL UNIQUE,
    epoch_id               TEXT NOT NULL,
    account_scope_hash     TEXT NOT NULL,
    risk_policy_version    TEXT NOT NULL,
    symbol_catalog_version TEXT NOT NULL,
    cut_manifest           TEXT NOT NULL,
    input_digest           TEXT NOT NULL,
    output_digest          TEXT,
    status                 TEXT NOT NULL,
    reason_codes           TEXT NOT NULL DEFAULT '[]',
    duration_ms            INTEGER,
    created_at             TEXT NOT NULL
);

CREATE INDEX idx_risk_audit_epoch_time
    ON risk_calculation_audit (epoch_id, created_at DESC);
