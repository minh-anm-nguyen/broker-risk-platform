"""DDL and the claim query, in both dialects, as inspectable text.

Keeping the schema as SQL rather than as an ORM model is a deliberate choice
for this project: design section 6.2 specifies constraints
(``UNIQUE (source_id, epoch_id, dedupe_key)``, the ``CHECK`` that a ``CLAIMED``
row must carry owner/token/lease) that are the actual safety mechanism, and they
should be readable next to the document that requires them.

Two dialect notes.

**Timestamps are fixed-width UTC text in SQLite.** ``2026-10-30T06:00:00.000000Z``
sorts lexicographically in chronological order, so ``next_attempt_at <= ?`` and
``lease_until < ?`` work as range predicates against a plain index. A variable
width format, or a Unix float, would either break ordering or reintroduce
floating point into a correctness predicate. PostgreSQL uses real
``TIMESTAMPTZ``.

**The eligibility guard, not the lock, is what orders an aggregate.** Both
dialects use the same ``NOT EXISTS`` subquery: a row is only claimable when no
earlier version of its aggregate is still unpublished. PostgreSQL adds
``FOR UPDATE SKIP LOCKED`` so several workers can claim disjoint rows; SQLite's
single writer serialises claims by construction. Neither mechanism makes the
*delivered* order safe -- design section 7.2 is explicit that an earlier Kafka
ACK plus an ambiguous DB ACK can still produce a late duplicate -- which is why
the consumer version guard is mandatory rather than an optimisation.
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
# P03-owned tables.
#
# Project 03 is not implemented in this repository, so these are a minimal
# stand-in: just enough of `source_epochs`, `deals`, `sync_runs` and the two
# current-state tables for the atomicity and marker invariants to be exercised
# against real rows. P07 does not own this schema (ADR07-01); in a real
# deployment these columns come from P03's migrations and the shapes must be
# reconciled before the outbox migration is applied.
# --------------------------------------------------------------------------- #

P03_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS source_epochs (
    epoch_id        TEXT PRIMARY KEY,
    source_id       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ACTIVE'
                      CHECK (status IN ('ACTIVE','BLOCKED','RETIRED')),
    opened_at       TEXT NOT NULL,
    UNIQUE (source_id, epoch_id)
);

CREATE TABLE IF NOT EXISTS deals (
    epoch_id        TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    deal_id         TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    position_id     TEXT,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    entry_kind      TEXT NOT NULL,
    volume_lots     TEXT NOT NULL,
    price           TEXT NOT NULL,
    realized_pnl_gross TEXT NOT NULL,
    commission      TEXT NOT NULL,
    swap            TEXT NOT NULL,
    money_currency  TEXT,
    source_seq      INTEGER NOT NULL,
    executed_at     TEXT NOT NULL,
    payload_hash    TEXT NOT NULL,
    PRIMARY KEY (epoch_id, deal_id)
);
CREATE INDEX IF NOT EXISTS idx_deals_account ON deals(epoch_id, account_id);

CREATE TABLE IF NOT EXISTS sync_runs (
    scan_id         TEXT PRIMARY KEY,
    epoch_id        TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    entity          TEXT NOT NULL CHECK (entity IN ('accounts','positions')),
    status          TEXT NOT NULL
                      CHECK (status IN ('STAGED','PUBLISHED','ABORTED','QUARANTINED')),
    snapshot_seq    INTEGER NOT NULL,
    record_count    INTEGER NOT NULL,
    manifest_sha256 TEXT NOT NULL,
    lease_token     TEXT NOT NULL,
    published_at    TEXT
);

CREATE TABLE IF NOT EXISTS accounts_current (
    epoch_id        TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    scan_id         TEXT NOT NULL,
    PRIMARY KEY (epoch_id, account_id)
);

CREATE TABLE IF NOT EXISTS positions_current (
    epoch_id        TEXT NOT NULL,
    position_id     TEXT NOT NULL,
    scan_id         TEXT NOT NULL,
    PRIMARY KEY (epoch_id, position_id)
);

-- Staging. A scan accumulates rows here page by page; "atomic publish" is the
-- single transaction that moves staging to current, flips sync_runs.status and
-- writes the marker. Without a staging area there is no way to have a scan that
-- is half-arrived, and therefore no way to test that a partial scan produces no
-- marker (IT07-04).
CREATE TABLE IF NOT EXISTS accounts_staging (
    scan_id         TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    PRIMARY KEY (scan_id, account_id)
);

CREATE TABLE IF NOT EXISTS positions_staging (
    scan_id         TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    position_id     TEXT NOT NULL,
    PRIMARY KEY (scan_id, position_id)
);
"""

# --------------------------------------------------------------------------- #
# P07-owned tables.
# --------------------------------------------------------------------------- #

OUTBOX_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS outbox_aggregate_heads (
    source_id       TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    aggregate_key   TEXT NOT NULL,
    last_version    INTEGER NOT NULL DEFAULT 0 CHECK (last_version >= 0),
    PRIMARY KEY (source_id, epoch_id, aggregate_key)
);

CREATE TABLE IF NOT EXISTS event_outbox (
    outbox_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL UNIQUE,
    source_id       TEXT NOT NULL,
    epoch_id        TEXT NOT NULL REFERENCES source_epochs(epoch_id),
    dedupe_key      TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    aggregate_key   TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL CHECK (aggregate_version > 0),
    event_type      TEXT NOT NULL,
    schema_version  INTEGER NOT NULL CHECK (schema_version > 0),
    topic           TEXT NOT NULL,
    partition_key   TEXT NOT NULL,
    payload         TEXT NOT NULL,
    payload_sha256  TEXT NOT NULL,
    wire_bytes      BLOB NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (state IN ('PENDING','CLAIMED','PUBLISHED','QUARANTINED')),
    created_at      TEXT NOT NULL,
    next_attempt_at TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_owner     TEXT,
    claim_token     TEXT,
    claim_version   INTEGER NOT NULL DEFAULT 0,
    lease_until     TEXT,
    published_at    TEXT,
    kafka_partition INTEGER,
    kafka_offset    INTEGER,
    last_error_code TEXT,
    quarantine_reason TEXT,
    UNIQUE (source_id, epoch_id, dedupe_key),
    UNIQUE (source_id, epoch_id, aggregate_key, aggregate_version),
    CHECK (state <> 'CLAIMED'
           OR (claim_owner IS NOT NULL AND claim_token IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK (state <> 'PUBLISHED' OR published_at IS NOT NULL),
    CHECK (state <> 'QUARANTINED' OR quarantine_reason IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON event_outbox (next_attempt_at, outbox_id) WHERE state IN ('PENDING','CLAIMED');
CREATE INDEX IF NOT EXISTS idx_outbox_agg_state
    ON event_outbox (source_id, epoch_id, aggregate_key, aggregate_version, state);
CREATE INDEX IF NOT EXISTS idx_outbox_ack_time
    ON event_outbox (published_at) WHERE state = 'PUBLISHED';
-- The backlog gauges group by state on every refresh. Without this the group-by
-- is a heap scan, which at the design's stated 1M-event backlog turns a metrics
-- read into the relay's dominant cost.
CREATE INDEX IF NOT EXISTS idx_outbox_state ON event_outbox (state);

CREATE TABLE IF NOT EXISTS outbox_attempts (
    attempt_id      TEXT PRIMARY KEY,
    event_id        TEXT NOT NULL REFERENCES event_outbox(event_id),
    claim_version   INTEGER NOT NULL,
    outcome         TEXT NOT NULL,
    error_code      TEXT,
    attempted_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_attempts_event
    ON outbox_attempts (event_id, attempted_at);

-- Identity, payload, hash, version and routing are immutable after insert; only
-- the delivery lifecycle columns may change. In PostgreSQL this is a column
-- grant plus a trigger; SQLite has no grants, so the trigger is the whole
-- mechanism. Without it, "the relay cannot alter a Deal amount" would be a
-- convention rather than a constraint.
CREATE TRIGGER IF NOT EXISTS trg_event_outbox_immutable
BEFORE UPDATE ON event_outbox
FOR EACH ROW WHEN (
       NEW.event_id          <> OLD.event_id
    OR NEW.source_id         <> OLD.source_id
    OR NEW.epoch_id          <> OLD.epoch_id
    OR NEW.dedupe_key        <> OLD.dedupe_key
    OR NEW.aggregate_key     <> OLD.aggregate_key
    OR NEW.aggregate_version <> OLD.aggregate_version
    OR NEW.event_type        <> OLD.event_type
    OR NEW.schema_version    <> OLD.schema_version
    OR NEW.topic             <> OLD.topic
    OR NEW.partition_key     <> OLD.partition_key
    OR NEW.payload           <> OLD.payload
    OR NEW.payload_sha256    <> OLD.payload_sha256
    OR NEW.wire_bytes        <> OLD.wire_bytes
    OR NEW.created_at        <> OLD.created_at
)
BEGIN
    SELECT RAISE(ABORT, 'event_outbox identity and payload columns are immutable');
END;
"""

#: Consumer-owned schema. Lives in a *separate database file* by default, which
#: is the point: design section 6.1 forbids a foreign key across the publisher
#: and consumer stores, and the easiest way to honour that is to make such a key
#: impossible to write.
CONSUMER_SQLITE_DDL = """
CREATE TABLE IF NOT EXISTS consumer_processed_events (
    consumer_name   TEXT NOT NULL,
    event_id        TEXT NOT NULL,
    payload_sha256  TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    aggregate_key   TEXT NOT NULL,
    aggregate_version INTEGER NOT NULL,
    topic           TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    INTEGER NOT NULL,
    processed_at    TEXT NOT NULL,
    PRIMARY KEY (consumer_name, event_id)
);

CREATE TABLE IF NOT EXISTS consumer_aggregate_versions (
    consumer_name   TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    aggregate_key   TEXT NOT NULL,
    last_applied_version INTEGER NOT NULL DEFAULT 0,
    blocked_reason  TEXT,
    PRIMARY KEY (consumer_name, source_id, epoch_id, aggregate_key)
);

-- Money is stored as decimal text and summed with Decimal, so the projection
-- stays exactly comparable with the source NUMERIC values (oracle O6).
-- An UNPROVEN-currency Deal is counted but never added to a currency total.
CREATE TABLE IF NOT EXISTS consumer_projection (
    consumer_name   TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    money_currency  TEXT,
    deal_count      INTEGER NOT NULL DEFAULT 0,
    realized_pnl_gross TEXT NOT NULL DEFAULT '0.00',
    commission      TEXT NOT NULL DEFAULT '0.00',
    swap            TEXT NOT NULL DEFAULT '0.00',
    unproven_currency_deal_count INTEGER NOT NULL DEFAULT 0,
    last_event_id   TEXT,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (consumer_name, source_id, epoch_id, account_id)
);

CREATE TABLE IF NOT EXISTS consumer_snapshot_notices (
    consumer_name   TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    epoch_id        TEXT NOT NULL,
    entity          TEXT NOT NULL,
    scan_id         TEXT NOT NULL,
    snapshot_seq    INTEGER NOT NULL,
    record_count    INTEGER NOT NULL,
    noticed_at      TEXT NOT NULL,
    PRIMARY KEY (consumer_name, source_id, epoch_id, entity, scan_id)
);

CREATE TABLE IF NOT EXISTS consumer_quarantine (
    consumer_name   TEXT NOT NULL,
    quarantine_id   TEXT NOT NULL,
    event_id        TEXT,
    topic           TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset    INTEGER NOT NULL,
    source_id       TEXT,
    epoch_id        TEXT,
    aggregate_key   TEXT,
    aggregate_version INTEGER,
    error_code      TEXT NOT NULL,
    reason          TEXT NOT NULL,
    payload_sha256  TEXT,
    quarantined_at  TEXT NOT NULL,
    PRIMARY KEY (consumer_name, quarantine_id)
);
CREATE INDEX IF NOT EXISTS idx_consumer_quarantine_agg
    ON consumer_quarantine (consumer_name, source_id, epoch_id, aggregate_key);
"""

#: Rows eligible for a claim: due-and-pending, or claimed with an expired lease,
#: and with no earlier unpublished version of the same aggregate.
CLAIM_SELECT_SQLITE = """
SELECT o.outbox_id
FROM event_outbox AS o
WHERE (
        (o.state = 'PENDING' AND o.next_attempt_at <= :now)
     OR (o.state = 'CLAIMED' AND o.lease_until < :now)
      )
  AND NOT EXISTS (
        SELECT 1 FROM event_outbox AS prior
        WHERE prior.source_id = o.source_id
          AND prior.epoch_id = o.epoch_id
          AND prior.aggregate_key = o.aggregate_key
          AND prior.aggregate_version < o.aggregate_version
          AND prior.state <> 'PUBLISHED'
      )
ORDER BY o.outbox_id
LIMIT :batch_size
"""

# --------------------------------------------------------------------------- #
# PostgreSQL. Executed only when psycopg and a DSN are available; the shapes
# match design section 6.2 so that the two backends are the same schema.
# --------------------------------------------------------------------------- #

P03_POSTGRES_DDL = """
CREATE TABLE IF NOT EXISTS source_epochs (
    epoch_id        UUID PRIMARY KEY,
    source_id       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'ACTIVE'
                      CHECK (status IN ('ACTIVE','BLOCKED','RETIRED')),
    opened_at       TIMESTAMPTZ NOT NULL,
    UNIQUE (source_id, epoch_id)
);

CREATE TABLE IF NOT EXISTS deals (
    epoch_id        UUID NOT NULL REFERENCES source_epochs(epoch_id),
    deal_id         TEXT NOT NULL,
    account_id      TEXT NOT NULL,
    position_id     TEXT,
    symbol          TEXT NOT NULL,
    side            TEXT NOT NULL,
    entry_kind      TEXT NOT NULL,
    volume_lots     NUMERIC(20,2) NOT NULL,
    price           NUMERIC(20,5) NOT NULL,
    realized_pnl_gross NUMERIC(20,2) NOT NULL,
    commission      NUMERIC(20,2) NOT NULL,
    swap            NUMERIC(20,2) NOT NULL,
    money_currency  TEXT,
    source_seq      BIGINT NOT NULL,
    executed_at     TIMESTAMPTZ NOT NULL,
    payload_hash    CHAR(64) NOT NULL,
    PRIMARY KEY (epoch_id, deal_id)
);
CREATE INDEX IF NOT EXISTS idx_deals_account ON deals(epoch_id, account_id);

CREATE TABLE IF NOT EXISTS sync_runs (
    scan_id         TEXT PRIMARY KEY,
    epoch_id        UUID NOT NULL REFERENCES source_epochs(epoch_id),
    entity          TEXT NOT NULL CHECK (entity IN ('accounts','positions')),
    status          TEXT NOT NULL
                      CHECK (status IN ('STAGED','PUBLISHED','ABORTED','QUARANTINED')),
    snapshot_seq    BIGINT NOT NULL,
    record_count    BIGINT NOT NULL,
    manifest_sha256 CHAR(64) NOT NULL,
    lease_token     TEXT NOT NULL,
    published_at    TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS accounts_current (
    epoch_id        UUID NOT NULL,
    account_id      TEXT NOT NULL,
    scan_id         TEXT NOT NULL,
    PRIMARY KEY (epoch_id, account_id)
);

CREATE TABLE IF NOT EXISTS positions_current (
    epoch_id        UUID NOT NULL,
    position_id     TEXT NOT NULL,
    scan_id         TEXT NOT NULL,
    PRIMARY KEY (epoch_id, position_id)
);

CREATE TABLE IF NOT EXISTS accounts_staging (
    scan_id         TEXT NOT NULL,
    epoch_id        UUID NOT NULL,
    account_id      TEXT NOT NULL,
    PRIMARY KEY (scan_id, account_id)
);

CREATE TABLE IF NOT EXISTS positions_staging (
    scan_id         TEXT NOT NULL,
    epoch_id        UUID NOT NULL,
    position_id     TEXT NOT NULL,
    PRIMARY KEY (scan_id, position_id)
);
"""

OUTBOX_POSTGRES_DDL = """
CREATE TABLE IF NOT EXISTS outbox_aggregate_heads (
    source_id       TEXT NOT NULL,
    epoch_id        UUID NOT NULL,
    aggregate_key   TEXT NOT NULL,
    last_version    BIGINT NOT NULL DEFAULT 0 CHECK (last_version >= 0),
    PRIMARY KEY (source_id, epoch_id, aggregate_key)
);

CREATE TABLE IF NOT EXISTS event_outbox (
    outbox_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id        UUID NOT NULL UNIQUE,
    source_id       TEXT NOT NULL,
    epoch_id        UUID NOT NULL REFERENCES source_epochs(epoch_id),
    dedupe_key      TEXT NOT NULL,
    aggregate_type  TEXT NOT NULL,
    aggregate_key   TEXT NOT NULL,
    aggregate_version BIGINT NOT NULL CHECK (aggregate_version > 0),
    event_type      TEXT NOT NULL,
    schema_version  INTEGER NOT NULL CHECK (schema_version > 0),
    topic           TEXT NOT NULL,
    partition_key   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    payload_sha256  CHAR(64) NOT NULL,
    wire_bytes      BYTEA NOT NULL,
    state           TEXT NOT NULL DEFAULT 'PENDING'
                      CHECK (state IN ('PENDING','CLAIMED','PUBLISHED','QUARANTINED')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    attempt_count   INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_owner     TEXT,
    claim_token     UUID,
    claim_version   BIGINT NOT NULL DEFAULT 0,
    lease_until     TIMESTAMPTZ,
    published_at    TIMESTAMPTZ,
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    last_error_code TEXT,
    quarantine_reason TEXT,
    -- source_id/epoch_id coherence: an event cannot claim the wrong source for
    -- an epoch, enforced by a composite FK rather than by application code.
    FOREIGN KEY (source_id, epoch_id) REFERENCES source_epochs(source_id, epoch_id),
    UNIQUE (source_id, epoch_id, dedupe_key),
    UNIQUE (source_id, epoch_id, aggregate_key, aggregate_version),
    CHECK (state <> 'CLAIMED'
           OR (claim_owner IS NOT NULL AND claim_token IS NOT NULL AND lease_until IS NOT NULL)),
    CHECK (state <> 'PUBLISHED' OR published_at IS NOT NULL),
    CHECK (state <> 'QUARANTINED' OR quarantine_reason IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS idx_outbox_due
    ON event_outbox (next_attempt_at, outbox_id) WHERE state IN ('PENDING','CLAIMED');
CREATE INDEX IF NOT EXISTS idx_outbox_agg_state
    ON event_outbox (source_id, epoch_id, aggregate_key, aggregate_version, state);
CREATE INDEX IF NOT EXISTS idx_outbox_ack_time
    ON event_outbox (published_at) WHERE state = 'PUBLISHED';
-- See the SQLite DDL: the backlog gauges group by state, and PostgreSQL can
-- satisfy this with an index-only scan.
CREATE INDEX IF NOT EXISTS idx_outbox_state ON event_outbox (state);

CREATE TABLE IF NOT EXISTS outbox_attempts (
    attempt_id      UUID PRIMARY KEY,
    event_id        UUID NOT NULL REFERENCES event_outbox(event_id),
    claim_version   BIGINT NOT NULL,
    outcome         TEXT NOT NULL,
    error_code      TEXT,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX IF NOT EXISTS idx_outbox_attempts_event
    ON outbox_attempts (event_id, attempted_at);

CREATE OR REPLACE FUNCTION event_outbox_reject_mutation() RETURNS trigger AS $$
BEGIN
    IF ROW(NEW.event_id, NEW.source_id, NEW.epoch_id, NEW.dedupe_key, NEW.aggregate_key,
           NEW.aggregate_version, NEW.event_type, NEW.schema_version, NEW.topic,
           NEW.partition_key, NEW.payload, NEW.payload_sha256, NEW.wire_bytes, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.event_id, OLD.source_id, OLD.epoch_id, OLD.dedupe_key, OLD.aggregate_key,
           OLD.aggregate_version, OLD.event_type, OLD.schema_version, OLD.topic,
           OLD.partition_key, OLD.payload, OLD.payload_sha256, OLD.wire_bytes, OLD.created_at)
    THEN
        RAISE EXCEPTION 'event_outbox identity and payload columns are immutable';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_event_outbox_immutable ON event_outbox;
CREATE TRIGGER trg_event_outbox_immutable
    BEFORE UPDATE ON event_outbox
    FOR EACH ROW EXECUTE FUNCTION event_outbox_reject_mutation();
"""

#: The claim CTE from design section 7.2, with ``FOR UPDATE SKIP LOCKED`` so
#: that concurrent owners take disjoint rows rather than blocking each other.
CLAIM_UPDATE_POSTGRES = """
WITH next_rows AS (
    SELECT o.outbox_id
    FROM event_outbox o
    WHERE (
            (o.state = 'PENDING' AND o.next_attempt_at <= %(now)s)
         OR (o.state = 'CLAIMED' AND o.lease_until < %(now)s)
          )
      AND NOT EXISTS (
            SELECT 1 FROM event_outbox prior
            WHERE prior.source_id = o.source_id
              AND prior.epoch_id = o.epoch_id
              AND prior.aggregate_key = o.aggregate_key
              AND prior.aggregate_version < o.aggregate_version
              AND prior.state <> 'PUBLISHED'
          )
    ORDER BY o.outbox_id
    LIMIT %(batch_size)s
    FOR UPDATE OF o SKIP LOCKED
)
UPDATE event_outbox o
SET state = 'CLAIMED',
    claim_owner = %(owner)s,
    -- A distinct token per row, not per batch: a stale callback must only be
    -- able to fence the one row it belongs to.
    claim_token = gen_random_uuid(),
    claim_version = o.claim_version + 1,
    lease_until = %(now)s + (%(lease_seconds)s * interval '1 second'),
    attempt_count = o.attempt_count + 1
FROM next_rows n
WHERE o.outbox_id = n.outbox_id
RETURNING o.outbox_id, o.event_id, o.source_id, o.epoch_id, o.dedupe_key,
          o.aggregate_key, o.aggregate_version, o.event_type, o.schema_version,
          o.topic, o.partition_key, o.wire_bytes, o.payload_sha256,
          o.attempt_count, o.claim_owner, o.claim_token, o.claim_version, o.lease_until,
          o.created_at
"""
