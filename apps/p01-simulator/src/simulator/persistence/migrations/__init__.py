"""Schema definition and migration runner.

Design notes that are easy to get wrong and expensive to fix later:

* **Money, prices and volumes are stored as TEXT**, holding the canonical
  Decimal string. SQLite's ``REAL`` is a double; storing ``0.1`` in it and
  reading it back is not ``0.1``. ``NUMERIC`` affinity would silently convert
  some of these strings to floats, so the columns are declared ``TEXT``.
* **Version intervals are half-open** ``[valid_from_seq, valid_to_seq)`` with
  ``NULL`` meaning "still current". Half-open intervals are what make
  "exactly one version visible at seq S" true at the boundary; inclusive
  intervals double-count at the join point (section 3.1).
* **Identity tables exist so foreign keys can exist.** A versioned table's
  primary key includes ``valid_from_seq``, so nothing can reference it. The
  ``*_identity`` tables give deals a real referential target, which turns
  INV-03 from a query you remember to run into a constraint the database
  enforces.
* **A partial unique index enforces netting.** At most one *current* version
  per ``(run_id, account_id, symbol)`` may have ``status='OPEN'``. Closed
  current versions are unconstrained, which is exactly the netting rule.
* Indexes are listed with the query they serve. Each one slows the writer, so
  an index without a named query does not belong here (section 4.1).
"""

from __future__ import annotations

import sqlite3

from ... import SCHEMA_VERSION

_SCHEMA_V1 = """
CREATE TABLE schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
) STRICT;

-- One row per scenario run. `committed_seq` is the single source of truth for
-- how far the event log has durably advanced.
CREATE TABLE runs (
    run_id                    TEXT PRIMARY KEY,
    generation                INTEGER NOT NULL,
    seed                      INTEGER NOT NULL,
    scenario_name             TEXT NOT NULL,
    spec_hash                 TEXT NOT NULL,
    spec_json                 TEXT NOT NULL,
    generator_version         TEXT NOT NULL,
    accounting_policy_version TEXT NOT NULL,
    symbol_catalog_version    TEXT NOT NULL,
    status                    TEXT NOT NULL,
    committed_seq             INTEGER NOT NULL DEFAULT 0,
    virtual_time              TEXT NOT NULL,
    start_virtual_time        TEXT NOT NULL,
    next_position_ordinal     INTEGER NOT NULL DEFAULT 0,
    next_deal_ordinal         INTEGER NOT NULL DEFAULT 0,
    account_count             INTEGER NOT NULL DEFAULT 0,
    deal_count                INTEGER NOT NULL DEFAULT 0,
    open_position_count       INTEGER NOT NULL DEFAULT 0,
    max_open_positions        INTEGER NOT NULL DEFAULT 0,
    max_deals                 INTEGER NOT NULL DEFAULT 0,
    created_at                TEXT NOT NULL,
    replaced_at               TEXT
) STRICT;

-- Single-row pointer to the active run. A reset swaps this one row, which is
-- what makes "the active run changed" an atomic, observable event rather than
-- a race across several tables (section 4.3).
CREATE TABLE active_run (
    id     INTEGER PRIMARY KEY CHECK (id = 1),
    run_id TEXT NOT NULL REFERENCES runs(run_id)
) STRICT;

-- Append-only. `seq` is contiguous from 1 within a run (INV-01); a rolled back
-- transaction consumes no seq because the number is allocated from
-- runs.committed_seq inside the same transaction.
CREATE TABLE event_log (
    run_id       TEXT NOT NULL REFERENCES runs(run_id),
    seq          INTEGER NOT NULL,
    event_id     TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    virtual_time TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    PRIMARY KEY (run_id, seq)
) STRICT;

CREATE UNIQUE INDEX ux_event_log_event_id ON event_log (run_id, event_id);

CREATE TABLE account_identity (
    run_id      TEXT NOT NULL REFERENCES runs(run_id),
    account_id  TEXT NOT NULL,
    created_seq INTEGER NOT NULL,
    PRIMARY KEY (run_id, account_id)
) STRICT;

CREATE TABLE position_identity (
    run_id      TEXT NOT NULL REFERENCES runs(run_id),
    position_id TEXT NOT NULL,
    account_id  TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    created_seq INTEGER NOT NULL,
    PRIMARY KEY (run_id, position_id),
    FOREIGN KEY (run_id, account_id) REFERENCES account_identity (run_id, account_id)
) STRICT;

CREATE TABLE account_versions (
    run_id         TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    valid_from_seq INTEGER NOT NULL,
    valid_to_seq   INTEGER,
    currency       TEXT NOT NULL,
    balance_dec    TEXT NOT NULL,
    credit_dec     TEXT NOT NULL,
    PRIMARY KEY (run_id, account_id, valid_from_seq),
    FOREIGN KEY (run_id, account_id) REFERENCES account_identity (run_id, account_id)
) STRICT;

-- Point lookup of the current version (writer netting/booking path).
CREATE UNIQUE INDEX ux_account_versions_current
    ON account_versions (run_id, account_id)
    WHERE valid_to_seq IS NULL;

CREATE TABLE position_versions (
    run_id         TEXT NOT NULL,
    position_id    TEXT NOT NULL,
    valid_from_seq INTEGER NOT NULL,
    valid_to_seq   INTEGER,
    account_id     TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    side           TEXT NOT NULL,
    volume_dec     TEXT NOT NULL,
    open_price_dec TEXT NOT NULL,
    status         TEXT NOT NULL,
    PRIMARY KEY (run_id, position_id, valid_from_seq),
    FOREIGN KEY (run_id, position_id) REFERENCES position_identity (run_id, position_id)
) STRICT;

CREATE UNIQUE INDEX ux_position_versions_current
    ON position_versions (run_id, position_id)
    WHERE valid_to_seq IS NULL;

-- Netting invariant, enforced by the database: one open position per
-- (account, symbol). Closed current versions are deliberately exempt.
CREATE UNIQUE INDEX ux_position_open_netting_key
    ON position_versions (run_id, account_id, symbol)
    WHERE valid_to_seq IS NULL AND status = 'OPEN';

-- GET /v1/positions?account_id=... ordered by position_id (keyset pagination).
CREATE INDEX ix_position_versions_by_account
    ON position_versions (run_id, account_id, position_id, valid_from_seq);

-- GET /v1/positions?symbol=...
CREATE INDEX ix_position_versions_by_symbol
    ON position_versions (run_id, symbol, position_id, valid_from_seq);

CREATE TABLE mark_versions (
    run_id         TEXT NOT NULL REFERENCES runs(run_id),
    symbol         TEXT NOT NULL,
    valid_from_seq INTEGER NOT NULL,
    valid_to_seq   INTEGER,
    bid_dec        TEXT NOT NULL,
    ask_dec        TEXT NOT NULL,
    market_time    TEXT NOT NULL,
    PRIMARY KEY (run_id, symbol, valid_from_seq)
) STRICT;

CREATE UNIQUE INDEX ux_mark_versions_current
    ON mark_versions (run_id, symbol)
    WHERE valid_to_seq IS NULL;

-- Immutable. There is no UPDATE statement against this table anywhere in the
-- codebase, and no DELETE outside whole-run retention cleanup.
CREATE TABLE deals (
    run_id         TEXT NOT NULL REFERENCES runs(run_id),
    deal_id        TEXT NOT NULL,
    source_seq     INTEGER NOT NULL,
    position_id    TEXT NOT NULL,
    account_id     TEXT NOT NULL,
    symbol         TEXT NOT NULL,
    entry          TEXT NOT NULL,
    side           TEXT NOT NULL,
    volume_dec     TEXT NOT NULL,
    price_dec      TEXT NOT NULL,
    gross_pnl_dec  TEXT NOT NULL,
    commission_dec TEXT NOT NULL,
    swap_dec       TEXT NOT NULL,
    executed_at    TEXT NOT NULL,
    -- Integer companion to executed_at, used for every range comparison.
    -- RFC 3339 strings do not sort chronologically once sub-second precision
    -- appears ('.' < 'Z'), so a text range filter would silently misplace
    -- boundary rows (WF-041, WF-046).
    executed_at_ms INTEGER NOT NULL,
    PRIMARY KEY (run_id, deal_id),
    FOREIGN KEY (run_id, account_id) REFERENCES account_identity (run_id, account_id),
    FOREIGN KEY (run_id, position_id) REFERENCES position_identity (run_id, position_id)
) STRICT;

-- Global and incremental pagination: ORDER BY (source_seq, deal_id).
-- Unique because the MVP emits at most one deal per event.
CREATE UNIQUE INDEX ux_deals_source_order ON deals (run_id, source_seq, deal_id);

-- GET /v1/deals?account_id=...&after_seq=...
CREATE INDEX ix_deals_by_account ON deals (run_id, account_id, source_seq, deal_id);

-- GET /v1/deals?symbol=...
CREATE INDEX ix_deals_by_symbol ON deals (run_id, symbol, source_seq, deal_id);

-- GET /v1/deals?from_time=...&to_time=... Kept despite the writer cost because
-- WF-046 requires time filtering to stay correct when virtual time is not
-- monotonic with source_seq.
CREATE INDEX ix_deals_by_time ON deals (run_id, executed_at_ms, source_seq, deal_id);

-- Written in the same transaction as the effect it describes, so a lost
-- response can never cause a second effect (WF-053, INV-13).
CREATE TABLE idempotency_records (
    scope         TEXT NOT NULL,
    key           TEXT NOT NULL,
    request_hash  TEXT NOT NULL,
    result_seq    INTEGER,
    http_status   INTEGER NOT NULL,
    response_json TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    PRIMARY KEY (scope, key)
) STRICT;

CREATE TABLE fault_rules (
    rule_id      TEXT PRIMARY KEY,
    run_id       TEXT,
    enabled      INTEGER NOT NULL DEFAULT 1,
    layer        TEXT NOT NULL,
    priority     INTEGER NOT NULL DEFAULT 100,
    scope_json   TEXT NOT NULL,
    trigger_json TEXT NOT NULL,
    action_json  TEXT NOT NULL,
    match_count  INTEGER NOT NULL DEFAULT 0,
    activations  INTEGER NOT NULL DEFAULT 0,
    expires_at   TEXT,
    created_at   TEXT NOT NULL
) STRICT;

CREATE TABLE jobs (
    job_id           TEXT PRIMARY KEY,
    run_id           TEXT NOT NULL,
    kind             TEXT NOT NULL,
    status           TEXT NOT NULL,
    target_count     INTEGER NOT NULL DEFAULT 0,
    processed_count  INTEGER NOT NULL DEFAULT 0,
    error_code       TEXT,
    error_message    TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT
) STRICT;

CREATE INDEX ix_jobs_by_run ON jobs (run_id, created_at);
"""


def migrate(conn: sqlite3.Connection) -> int:
    """Create or verify the schema. Returns the schema version in force.

    The whole migration runs in one transaction so a failure part-way leaves
    the database on the previous version rather than on half a schema
    (WF-123). An unexpected newer version is refused instead of guessed at.
    """
    current = _read_schema_version(conn)
    if current == SCHEMA_VERSION:
        return current
    if current > SCHEMA_VERSION:
        raise RuntimeError(
            f"Database schema version {current} is newer than this build supports "
            f"({SCHEMA_VERSION}). Refusing to run against a future schema."
        )

    conn.execute("BEGIN IMMEDIATE")
    try:
        if current == 0:
            conn.executescript(_SCHEMA_V1)
            conn.execute(
                "INSERT INTO schema_meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
        else:  # pragma: no cover - no v2 exists yet
            raise RuntimeError(f"No migration path from schema version {current}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return SCHEMA_VERSION


def _read_schema_version(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='schema_meta'"
    ).fetchone()
    if row is None:
        return 0
    value = conn.execute("SELECT value FROM schema_meta WHERE key='schema_version'").fetchone()
    if value is None:  # pragma: no cover - would mean a partially created schema
        return 0
    return int(value[0])
