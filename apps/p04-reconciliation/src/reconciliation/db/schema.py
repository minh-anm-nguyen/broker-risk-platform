"""Database schema: P03-owned tables plus the additive P04 ``recon_*`` tables.

Design rules enforced here (SYSTEM_DESIGN section 6.2):

* **Money is TEXT, never REAL.** Storing a balance in a floating-point column
  would undo every guarantee in ``canonical.py`` at the storage layer. Every
  monetary column here holds the decimal *string* the source sent.
* **Ownership is physical, not advisory.** ``P03_OWNED_TABLES`` is consumed by
  the connection authoriser in ``engine.py`` so the reconciliation role is
  denied INSERT/UPDATE/DELETE on P03's tables by the engine itself, which is
  how INV04-07 and INV04-18 are tested rather than asserted.
* **Completeness is never a column default.** ``source_complete`` starts false
  and only a validated transition may set it, because no single-table CHECK
  can express "the receipt chain is contiguous, terminal and internally
  consistent" (section 6.2 caveat 1).
* **One terminal page per run**, enforced by a partial unique index, so a
  second terminal receipt is a storage error rather than a silent overwrite.

The reference deployment is PostgreSQL; ``database/migrations/0001_*.sql``
holds that dialect. The statements below are the SQLite form used by this
build, and the structural constraints are deliberately identical so a test
that passes here is testing the same invariants.
"""

from __future__ import annotations

from typing import Final

#: Tables that Project 03 owns. Project 04 may read them and must never write
#: them (section 3.1 "Ownership rule").
P03_OWNED_TABLES: Final[frozenset[str]] = frozenset(
    {
        "sync_sources",
        "source_epochs",
        "sync_runs",
        "sync_checkpoints",
        "deals",
        "accounts_current",
        "positions_current",
        "account_staging",
        "position_staging",
        "repair_intents",
    }
)

#: Tables Project 04 owns and may write.
P04_OWNED_TABLES: Final[frozenset[str]] = frozenset(
    {
        "recon_runs",
        "recon_source_items",
        "recon_target_items",
        "recon_source_page_receipts",
        "recon_triggers",
        "recon_scheduler_leases",
        "recon_findings",
        "recon_plans",
        "recon_actions",
        "recon_approvals",
        "recon_attempts",
        "recon_audit",
    }
)

ENTITIES: Final[tuple[str, ...]] = ("accounts", "positions", "deals")


# ---------------------------------------------------------------------------
# Project 03 owned schema (the counterparty this service audits)
# ---------------------------------------------------------------------------

P03_DDL: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS sync_sources (
      source_id    TEXT PRIMARY KEY,
      display_name TEXT NOT NULL
    )
    """,
    # (source_id, run_id, generation) is the source's own identity; the UUID
    # is P03's handle for it. The UNIQUE constraint is what makes a source
    # reset a *new* epoch instead of a mutation of the old one (section 7.7).
    """
    CREATE TABLE IF NOT EXISTS source_epochs (
      epoch_id          TEXT PRIMARY KEY,
      source_id         TEXT NOT NULL REFERENCES sync_sources(source_id),
      source_run_id     TEXT NOT NULL,
      source_generation INTEGER NOT NULL,
      status            TEXT NOT NULL CHECK (status IN ('ACTIVE','SUPERSEDED','QUARANTINED')),
      created_at        TEXT NOT NULL,
      UNIQUE (source_id, source_run_id, source_generation)
    )
    """,
    # One row per sync attempt. A scan only counts as evidence when
    # status='PUBLISHED' and terminal_receipt=1 (section 2.1).
    """
    CREATE TABLE IF NOT EXISTS sync_runs (
      scan_id          TEXT PRIMARY KEY,
      epoch_id         TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      entity           TEXT NOT NULL CHECK (entity IN ('accounts','positions','deals')),
      snapshot_seq     INTEGER,
      filter_hash      TEXT NOT NULL DEFAULT 'UNFILTERED',
      status           TEXT NOT NULL CHECK (status IN ('RUNNING','PUBLISHED','FAILED','ABANDONED')),
      terminal_receipt INTEGER NOT NULL DEFAULT 0 CHECK (terminal_receipt IN (0,1)),
      record_count     INTEGER,
      started_at       TEXT NOT NULL,
      published_at     TEXT
    )
    """,
    # Deal watermark W and the lease that fences whoever may advance it.
    """
    CREATE TABLE IF NOT EXISTS sync_checkpoints (
      epoch_id      TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      entity        TEXT NOT NULL CHECK (entity IN ('accounts','positions','deals')),
      watermark_seq INTEGER NOT NULL DEFAULT 0 CHECK (watermark_seq >= 0),
      filter_hash   TEXT NOT NULL DEFAULT 'UNFILTERED',
      published     INTEGER NOT NULL DEFAULT 0 CHECK (published IN (0,1)),
      published_scan_id TEXT REFERENCES sync_runs(scan_id),
      lease_token   TEXT,
      lease_version INTEGER NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
      lease_until   TEXT,
      PRIMARY KEY (epoch_id, entity)
    )
    """,
    # Immutable deal history. The PK is (epoch_id, deal_id): the same deal id
    # in two epochs is two different rows, not a collision (INV04-17).
    """
    CREATE TABLE IF NOT EXISTS deals (
      epoch_id           TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      deal_id            TEXT NOT NULL,
      source_seq         INTEGER NOT NULL,
      account_id         TEXT NOT NULL,
      position_id        TEXT NOT NULL,
      symbol             TEXT NOT NULL,
      entry              TEXT NOT NULL,
      side               TEXT NOT NULL,
      volume_lots        TEXT NOT NULL,
      price              TEXT NOT NULL,
      realized_pnl_gross TEXT NOT NULL,
      commission         TEXT NOT NULL,
      swap               TEXT NOT NULL,
      executed_at        TEXT NOT NULL,
      payload_hash       TEXT NOT NULL,
      PRIMARY KEY (epoch_id, deal_id)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_deals_seq ON deals(epoch_id, source_seq)",
    """
    CREATE TABLE IF NOT EXISTS accounts_current (
      epoch_id          TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      account_id        TEXT NOT NULL,
      currency          TEXT NOT NULL,
      balance           TEXT NOT NULL,
      credit            TEXT NOT NULL,
      published_scan_id TEXT NOT NULL REFERENCES sync_runs(scan_id),
      payload_hash      TEXT NOT NULL,
      PRIMARY KEY (epoch_id, account_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS positions_current (
      epoch_id          TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      position_id       TEXT NOT NULL,
      account_id        TEXT NOT NULL,
      symbol            TEXT NOT NULL,
      side              TEXT NOT NULL,
      volume_lots       TEXT NOT NULL,
      open_price        TEXT NOT NULL,
      status            TEXT NOT NULL,
      published_scan_id TEXT NOT NULL REFERENCES sync_runs(scan_id),
      payload_hash      TEXT NOT NULL,
      PRIMARY KEY (epoch_id, position_id)
    )
    """,
    # Staging rows are written during a scan and promoted atomically. They are
    # retained after publish so an audit can use section 7.3 Option B.
    """
    CREATE TABLE IF NOT EXISTS account_staging (
      scan_id      TEXT NOT NULL REFERENCES sync_runs(scan_id),
      account_id   TEXT NOT NULL,
      currency     TEXT NOT NULL,
      balance      TEXT NOT NULL,
      credit       TEXT NOT NULL,
      payload_hash TEXT NOT NULL,
      PRIMARY KEY (scan_id, account_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS position_staging (
      scan_id      TEXT NOT NULL REFERENCES sync_runs(scan_id),
      position_id  TEXT NOT NULL,
      account_id   TEXT NOT NULL,
      symbol       TEXT NOT NULL,
      side         TEXT NOT NULL,
      volume_lots  TEXT NOT NULL,
      open_price   TEXT NOT NULL,
      status       TEXT NOT NULL,
      payload_hash TEXT NOT NULL,
      PRIMARY KEY (scan_id, position_id)
    )
    """,
    # P03's authoritative receipt store for repair intents. The PK on
    # intent_id is what makes a replayed dispatch idempotent (INV04-08).
    """
    CREATE TABLE IF NOT EXISTS repair_intents (
      intent_id         TEXT PRIMARY KEY,
      source_id         TEXT NOT NULL,
      epoch_id          TEXT NOT NULL,
      action_type       TEXT NOT NULL,
      parameters_hash   TEXT NOT NULL,
      status            TEXT NOT NULL CHECK (status IN
                          ('ACCEPTED','RUNNING','COMMITTED','FAILED','REJECTED')),
      reason_code       TEXT,
      receipt_hash      TEXT,
      result_scan_id    TEXT,
      result_watermark  INTEGER,
      execution_count   INTEGER NOT NULL DEFAULT 0 CHECK (execution_count >= 0),
      accepted_at       TEXT NOT NULL,
      completed_at      TEXT
    )
    """,
)


# ---------------------------------------------------------------------------
# Project 04 additive schema (section 6.2)
# ---------------------------------------------------------------------------

P04_DDL: Final[tuple[str, ...]] = (
    """
    CREATE TABLE IF NOT EXISTS recon_runs (
      recon_run_id            TEXT PRIMARY KEY,
      source_id               TEXT NOT NULL REFERENCES sync_sources(source_id),
      source_epoch_id         TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      entity                  TEXT NOT NULL CHECK (entity IN ('accounts','positions','deals')),
      scope                   TEXT NOT NULL DEFAULT 'UNFILTERED',
      scope_hash              TEXT NOT NULL,
      policy_hash             TEXT NOT NULL,
      schema_version          TEXT NOT NULL,
      hash_version            TEXT NOT NULL,
      status                  TEXT NOT NULL CHECK (status IN (
                                'CREATED','CAPTURING_SOURCE','CAPTURING_TARGET','DIFFING',
                                'MATCHED','FINDINGS','PLANNED','INCONCLUSIVE','FAILED','CANCELLED')),
      worker_id               TEXT NOT NULL,
      source_run_id           TEXT,
      source_generation       INTEGER,
      source_snapshot_seq     INTEGER,
      target_snapshot_seq     INTEGER,
      target_published_scan_id TEXT REFERENCES sync_runs(scan_id),
      target_deal_watermark   INTEGER,
      source_complete         INTEGER NOT NULL DEFAULT 0 CHECK (source_complete IN (0,1)),
      target_complete         INTEGER NOT NULL DEFAULT 0 CHECK (target_complete IN (0,1)),
      source_count            INTEGER,
      target_count            INTEGER,
      source_root_hash        TEXT,
      target_root_hash        TEXT,
      reason_code             TEXT,
      verifies_plan_id        TEXT,
      started_at              TEXT NOT NULL,
      deadline_at             TEXT NOT NULL,
      ended_at                TEXT,
      CHECK (source_snapshot_seq IS NULL OR source_snapshot_seq >= 0),
      CHECK (target_snapshot_seq IS NULL OR target_snapshot_seq >= 0)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_recon_runs_epoch ON recon_runs(source_epoch_id, entity, status)",
    # Durable page evidence. source_complete cannot be asserted without a
    # validated terminal receipt and contiguous indices from 0 (section 7.4).
    """
    CREATE TABLE IF NOT EXISTS recon_source_page_receipts (
      recon_run_id TEXT NOT NULL REFERENCES recon_runs(recon_run_id),
      page_index   INTEGER NOT NULL CHECK (page_index >= 0),
      snapshot_seq INTEGER NOT NULL CHECK (snapshot_seq >= 0),
      run_id       TEXT NOT NULL,
      generation   INTEGER NOT NULL,
      item_count   INTEGER NOT NULL CHECK (item_count BETWEEN 0 AND 500),
      page_hash    TEXT NOT NULL,
      cursor_digest TEXT,
      is_terminal  INTEGER NOT NULL CHECK (is_terminal IN (0,1)),
      committed_at TEXT NOT NULL,
      PRIMARY KEY (recon_run_id, page_index)
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS ux_recon_one_terminal_page
      ON recon_source_page_receipts(recon_run_id) WHERE is_terminal = 1
    """,
    # PRIMARY KEY (recon_run_id, item_key) makes a duplicate key across pages
    # a storage error, which the capture layer classifies rather than hides
    # (section 7.4 step 4).
    """
    CREATE TABLE IF NOT EXISTS recon_source_items (
      recon_run_id TEXT NOT NULL REFERENCES recon_runs(recon_run_id),
      page_index   INTEGER NOT NULL CHECK (page_index >= 0),
      item_key     TEXT NOT NULL,
      source_seq   INTEGER,
      payload_hash TEXT NOT NULL,
      PRIMARY KEY (recon_run_id, item_key),
      FOREIGN KEY (recon_run_id, page_index)
        REFERENCES recon_source_page_receipts(recon_run_id, page_index)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_recon_source_page_items ON recon_source_items(recon_run_id, page_index)",
    """
    CREATE TABLE IF NOT EXISTS recon_target_items (
      recon_run_id TEXT NOT NULL REFERENCES recon_runs(recon_run_id),
      item_key     TEXT NOT NULL,
      source_seq   INTEGER,
      payload_hash TEXT NOT NULL,
      PRIMARY KEY (recon_run_id, item_key)
    )
    """,
    # request_id is a client-supplied idempotency key bound to a scope
    # fingerprint, so a retried trigger reuses the run instead of racing a
    # second one (section 6.2, FR04-02).
    """
    CREATE TABLE IF NOT EXISTS recon_triggers (
      request_id          TEXT PRIMARY KEY,
      request_fingerprint TEXT NOT NULL,
      recon_run_id        TEXT NOT NULL UNIQUE REFERENCES recon_runs(recon_run_id),
      created_at          TEXT NOT NULL
    )
    """,
    # P04's scheduler fencing, independent of P03's sync lease (section 6.2).
    """
    CREATE TABLE IF NOT EXISTS recon_scheduler_leases (
      source_epoch_id TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      entity          TEXT NOT NULL CHECK (entity IN ('accounts','positions','deals')),
      scope_hash      TEXT NOT NULL,
      lease_token     TEXT,
      owner_id        TEXT,
      lease_version   INTEGER NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
      lease_until     TEXT,
      PRIMARY KEY (source_epoch_id, entity, scope_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recon_findings (
      finding_id       TEXT PRIMARY KEY,
      recon_run_id     TEXT NOT NULL REFERENCES recon_runs(recon_run_id),
      item_key         TEXT,
      finding_code     TEXT NOT NULL,
      severity         TEXT NOT NULL CHECK (severity IN ('INFO','WARN','ERROR','CRITICAL')),
      confidence       TEXT NOT NULL CHECK (confidence IN ('PROVEN','PROVISIONAL','INCONCLUSIVE')),
      source_hash      TEXT,
      target_hash      TEXT,
      source_seq       INTEGER,
      evidence_hash    TEXT NOT NULL,
      explanation_code TEXT NOT NULL,
      created_at       TEXT NOT NULL,
      UNIQUE (recon_run_id, finding_code, item_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_recon_findings_run ON recon_findings(recon_run_id, finding_code)",
    """
    CREATE TABLE IF NOT EXISTS recon_plans (
      plan_id                TEXT PRIMARY KEY,
      recon_run_id           TEXT NOT NULL REFERENCES recon_runs(recon_run_id),
      plan_hash              TEXT NOT NULL,
      expected_epoch_id      TEXT NOT NULL REFERENCES source_epochs(epoch_id),
      expected_target_scan_id TEXT,
      expected_deal_w        INTEGER,
      expected_lease_version INTEGER,
      policy_hash            TEXT NOT NULL,
      evidence_captured_at   TEXT NOT NULL,
      status                 TEXT NOT NULL CHECK (status IN (
                               'DRAFT','PENDING_APPROVAL','APPROVED','REJECTED',
                               'DISPATCHING','RUNNING','UNKNOWN','VERIFYING',
                               'VERIFIED','FAILED','STALE')),
      reason_code            TEXT,
      expires_at             TEXT NOT NULL,
      created_by             TEXT NOT NULL,
      created_at             TEXT NOT NULL,
      UNIQUE (recon_run_id, plan_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recon_actions (
      action_id       TEXT PRIMARY KEY,
      plan_id         TEXT NOT NULL REFERENCES recon_plans(plan_id),
      action_order    INTEGER NOT NULL CHECK (action_order >= 0),
      action_type     TEXT NOT NULL CHECK (action_type IN (
                        'REQUEST_FULL_ACCOUNT_RESYNC','REQUEST_FULL_POSITION_RESYNC',
                        'REQUEST_DEAL_BACKFILL','MANUAL_INVESTIGATION')),
      intent_id       TEXT NOT NULL UNIQUE,
      status          TEXT NOT NULL,
      reason_code     TEXT,
      parameters_json TEXT NOT NULL,
      parameters_hash TEXT NOT NULL,
      UNIQUE (plan_id, action_order)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recon_approvals (
      approval_id TEXT PRIMARY KEY,
      plan_id     TEXT NOT NULL REFERENCES recon_plans(plan_id),
      actor_id    TEXT NOT NULL,
      role        TEXT NOT NULL,
      decision    TEXT NOT NULL CHECK (decision IN ('APPROVE','REJECT')),
      plan_hash   TEXT NOT NULL,
      decided_at  TEXT NOT NULL,
      UNIQUE (plan_id, actor_id)
    )
    """,
    # One row per dispatch attempt. request_id is unique so a replay after a
    # lost ACK cannot silently create a second attempt record (section 7.6).
    """
    CREATE TABLE IF NOT EXISTS recon_attempts (
      attempt_id   TEXT PRIMARY KEY,
      action_id    TEXT NOT NULL REFERENCES recon_actions(action_id),
      request_id   TEXT NOT NULL UNIQUE,
      status       TEXT NOT NULL,
      receipt_hash TEXT,
      error_code   TEXT,
      created_at   TEXT NOT NULL,
      resolved_at  TEXT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS recon_audit (
      audit_id        TEXT PRIMARY KEY,
      sequence_no     INTEGER NOT NULL,
      recon_run_id    TEXT REFERENCES recon_runs(recon_run_id),
      plan_id         TEXT REFERENCES recon_plans(plan_id),
      event_type      TEXT NOT NULL,
      actor_id        TEXT,
      safe_details    TEXT NOT NULL DEFAULT '{}',
      prev_event_hash TEXT,
      event_hash      TEXT NOT NULL,
      created_at      TEXT NOT NULL
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_recon_audit_seq ON recon_audit(sequence_no)",
)

ALL_DDL: Final[tuple[str, ...]] = P03_DDL + P04_DDL
