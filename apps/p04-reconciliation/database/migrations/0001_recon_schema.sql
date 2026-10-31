-- Project 04 -- additive reconciliation schema (SYSTEM_DESIGN section 6.2)
--
-- STATUS: reference migration. This is the authoritative PostgreSQL form of
-- the schema. It has NOT been executed against a PostgreSQL instance in this
-- repository, because no PostgreSQL server is available in the build
-- environment; the running build uses the SQLite form in
-- src/reconciliation/db/schema.py, whose structural constraints are kept
-- deliberately identical.
--
-- Two differences between the two forms are worth stating plainly:
--
--   1. Role privileges. Here they are real GRANT/REVOKE statements. In the
--      SQLite build the same rule is enforced by a connection authoriser that
--      denies INSERT/UPDATE/DELETE on Project 03's tables (see
--      db/engine.py). The authoriser is an emulation of this section and is
--      not a substitute for it in production.
--
--   2. Snapshot isolation. Section 7.3's target-capture protocol relies on a
--      transaction-scoped read snapshot. PostgreSQL provides it with
--      REPEATABLE READ; SQLite provides it with a WAL read transaction. Both
--      fail closed when the cut moves, which is the property the tests assert.
--
-- Prerequisite: Project 03's schema (sync_sources, source_epochs, sync_runs,
-- sync_checkpoints, deals, accounts_current, positions_current) already
-- exists. This migration adds only recon_* objects and reads P03's.

BEGIN;

-- ---------------------------------------------------------------------------
-- Audit runs
-- ---------------------------------------------------------------------------

CREATE TABLE recon_runs (
  recon_run_id             UUID PRIMARY KEY,
  source_id                TEXT NOT NULL REFERENCES sync_sources(source_id),
  source_epoch_id          UUID NOT NULL REFERENCES source_epochs(epoch_id),
  entity                   TEXT NOT NULL CHECK (entity IN ('accounts','positions','deals')),
  scope                    TEXT NOT NULL DEFAULT 'UNFILTERED',
  scope_hash               CHAR(64) NOT NULL,
  policy_hash              CHAR(64) NOT NULL,
  schema_version           TEXT NOT NULL,
  hash_version             TEXT NOT NULL,
  status                   TEXT NOT NULL CHECK (status IN (
                             'CREATED','CAPTURING_SOURCE','CAPTURING_TARGET','DIFFING',
                             'MATCHED','FINDINGS','PLANNED','INCONCLUSIVE','FAILED','CANCELLED')),
  worker_id                TEXT NOT NULL,
  source_run_id            TEXT,
  source_generation        BIGINT,
  source_snapshot_seq      BIGINT,
  target_snapshot_seq      BIGINT,
  target_published_scan_id UUID REFERENCES sync_runs(scan_id),
  target_deal_watermark    BIGINT,
  source_complete          BOOLEAN NOT NULL DEFAULT FALSE,
  target_complete          BOOLEAN NOT NULL DEFAULT FALSE,
  source_count             BIGINT,
  target_count             BIGINT,
  source_root_hash         CHAR(64),
  target_root_hash         CHAR(64),
  reason_code              TEXT,
  verifies_plan_id         UUID,
  started_at               TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  deadline_at              TIMESTAMPTZ NOT NULL,
  ended_at                 TIMESTAMPTZ,
  CHECK (source_snapshot_seq IS NULL OR source_snapshot_seq >= 0),
  CHECK (target_snapshot_seq IS NULL OR target_snapshot_seq >= 0),
  -- A completeness flag must never be set without the counts and digest that
  -- justify it. The full cross-table proof lives in the application
  -- transition (section 6.2 caveat 1); this is the part a CHECK can express.
  CHECK (NOT source_complete OR (source_count IS NOT NULL AND source_root_hash IS NOT NULL)),
  CHECK (NOT target_complete OR (target_count IS NOT NULL AND target_root_hash IS NOT NULL))
);

CREATE INDEX idx_recon_runs_epoch ON recon_runs(source_epoch_id, entity, status);
CREATE INDEX idx_recon_runs_open ON recon_runs(status)
  WHERE status IN ('CREATED','CAPTURING_SOURCE','CAPTURING_TARGET','DIFFING');

-- ---------------------------------------------------------------------------
-- Source page receipts and manifests
-- ---------------------------------------------------------------------------

-- Durable page evidence. source_complete cannot be asserted without a
-- validated terminal receipt and contiguous indices from 0 (section 7.4).
CREATE TABLE recon_source_page_receipts (
  recon_run_id  UUID NOT NULL REFERENCES recon_runs(recon_run_id) ON DELETE CASCADE,
  page_index    INTEGER NOT NULL CHECK (page_index >= 0),
  snapshot_seq  BIGINT NOT NULL CHECK (snapshot_seq >= 0),
  run_id        TEXT NOT NULL,
  generation    BIGINT NOT NULL,
  item_count    INTEGER NOT NULL CHECK (item_count BETWEEN 0 AND 500),
  page_hash     CHAR(64) NOT NULL,
  cursor_digest CHAR(64),
  is_terminal   BOOLEAN NOT NULL,
  committed_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (recon_run_id, page_index)
);

-- At most one terminal page per scan: a second one is a storage error rather
-- than a silent overwrite.
CREATE UNIQUE INDEX ux_recon_one_terminal_page
  ON recon_source_page_receipts(recon_run_id) WHERE is_terminal;

-- PRIMARY KEY (recon_run_id, item_key) makes a duplicate key across pages a
-- constraint violation the capture layer classifies as SOURCE_DUPLICATE_KEY.
CREATE TABLE recon_source_items (
  recon_run_id UUID NOT NULL REFERENCES recon_runs(recon_run_id) ON DELETE CASCADE,
  page_index   INTEGER NOT NULL CHECK (page_index >= 0),
  item_key     TEXT NOT NULL,
  source_seq   BIGINT,
  payload_hash CHAR(64) NOT NULL,
  PRIMARY KEY (recon_run_id, item_key),
  FOREIGN KEY (recon_run_id, page_index)
    REFERENCES recon_source_page_receipts(recon_run_id, page_index) ON DELETE CASCADE
);

CREATE INDEX idx_recon_source_page_items ON recon_source_items(recon_run_id, page_index);
CREATE INDEX idx_recon_source_seq ON recon_source_items(recon_run_id, source_seq)
  WHERE source_seq IS NOT NULL;

CREATE TABLE recon_target_items (
  recon_run_id UUID NOT NULL REFERENCES recon_runs(recon_run_id) ON DELETE CASCADE,
  item_key     TEXT NOT NULL,
  source_seq   BIGINT,
  payload_hash CHAR(64) NOT NULL,
  PRIMARY KEY (recon_run_id, item_key)
);

CREATE INDEX idx_recon_target_seq ON recon_target_items(recon_run_id, source_seq)
  WHERE source_seq IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Triggers and leases
-- ---------------------------------------------------------------------------

-- Deduplicate audit triggers before creating work. request_id is a
-- client-supplied opaque idempotency key bound to a scope fingerprint, so a
-- retried trigger reuses its run instead of racing a second audit.
CREATE TABLE recon_triggers (
  request_id          TEXT PRIMARY KEY,
  request_fingerprint CHAR(64) NOT NULL,
  recon_run_id        UUID NOT NULL UNIQUE REFERENCES recon_runs(recon_run_id),
  created_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- P04's scheduler fencing, independent of P03's sync_checkpoints lease.
CREATE TABLE recon_scheduler_leases (
  source_epoch_id UUID NOT NULL REFERENCES source_epochs(epoch_id),
  entity          TEXT NOT NULL CHECK (entity IN ('accounts','positions','deals')),
  scope_hash      CHAR(64) NOT NULL,
  lease_token     UUID,
  owner_id        TEXT,
  lease_version   BIGINT NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
  lease_until     TIMESTAMPTZ,
  PRIMARY KEY (source_epoch_id, entity, scope_hash)
);

-- ---------------------------------------------------------------------------
-- Findings
-- ---------------------------------------------------------------------------

CREATE TABLE recon_findings (
  finding_id       UUID PRIMARY KEY,
  recon_run_id     UUID NOT NULL REFERENCES recon_runs(recon_run_id) ON DELETE CASCADE,
  item_key         TEXT,
  finding_code     TEXT NOT NULL,
  severity         TEXT NOT NULL CHECK (severity IN ('INFO','WARN','ERROR','CRITICAL')),
  confidence       TEXT NOT NULL CHECK (confidence IN ('PROVEN','PROVISIONAL','INCONCLUSIVE')),
  source_hash      CHAR(64),
  target_hash      CHAR(64),
  source_seq       BIGINT,
  evidence_hash    CHAR(64) NOT NULL,
  explanation_code TEXT NOT NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (recon_run_id, finding_code, item_key)
);

CREATE INDEX idx_recon_findings_run ON recon_findings(recon_run_id, finding_code);
CREATE INDEX idx_recon_findings_critical ON recon_findings(recon_run_id)
  WHERE severity = 'CRITICAL';

-- ---------------------------------------------------------------------------
-- Plans, actions, approvals, attempts
-- ---------------------------------------------------------------------------

CREATE TABLE recon_plans (
  plan_id                 UUID PRIMARY KEY,
  recon_run_id            UUID NOT NULL REFERENCES recon_runs(recon_run_id),
  plan_hash               CHAR(64) NOT NULL,
  expected_epoch_id       UUID NOT NULL REFERENCES source_epochs(epoch_id),
  expected_target_scan_id UUID,
  expected_deal_w         BIGINT,
  expected_lease_version  BIGINT,
  policy_hash             CHAR(64) NOT NULL,
  evidence_captured_at    TIMESTAMPTZ NOT NULL,
  status                  TEXT NOT NULL CHECK (status IN (
                            'DRAFT','PENDING_APPROVAL','APPROVED','REJECTED',
                            'DISPATCHING','RUNNING','UNKNOWN','VERIFYING',
                            'VERIFIED','FAILED','STALE')),
  reason_code             TEXT,
  expires_at              TIMESTAMPTZ NOT NULL,
  created_by              TEXT NOT NULL,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (recon_run_id, plan_hash)
);

-- The action vocabulary is a closed allowlist. There is deliberately no verb
-- here that edits a value or deletes a row: P04 orchestrates P03, it is not a
-- second writer for synced data (section 8.1).
CREATE TABLE recon_actions (
  action_id       UUID PRIMARY KEY,
  plan_id         UUID NOT NULL REFERENCES recon_plans(plan_id) ON DELETE CASCADE,
  action_order    INTEGER NOT NULL CHECK (action_order >= 0),
  action_type     TEXT NOT NULL CHECK (action_type IN (
                    'REQUEST_FULL_ACCOUNT_RESYNC','REQUEST_FULL_POSITION_RESYNC',
                    'REQUEST_DEAL_BACKFILL','MANUAL_INVESTIGATION')),
  intent_id       TEXT NOT NULL UNIQUE,
  status          TEXT NOT NULL,
  reason_code     TEXT,
  parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  parameters_hash CHAR(64) NOT NULL,
  UNIQUE (plan_id, action_order)
);

CREATE TABLE recon_approvals (
  approval_id UUID PRIMARY KEY,
  plan_id     UUID NOT NULL REFERENCES recon_plans(plan_id) ON DELETE CASCADE,
  actor_id    TEXT NOT NULL,
  role        TEXT NOT NULL,
  decision    TEXT NOT NULL CHECK (decision IN ('APPROVE','REJECT')),
  plan_hash   CHAR(64) NOT NULL,
  decided_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  -- One decision per actor per plan; changing one's mind means a new plan.
  UNIQUE (plan_id, actor_id)
);

CREATE TABLE recon_attempts (
  attempt_id   UUID PRIMARY KEY,
  action_id    UUID NOT NULL REFERENCES recon_actions(action_id) ON DELETE CASCADE,
  request_id   TEXT NOT NULL UNIQUE,
  status       TEXT NOT NULL,
  receipt_hash CHAR(64),
  error_code   TEXT,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  resolved_at  TIMESTAMPTZ
);

-- ---------------------------------------------------------------------------
-- Tamper-evident audit log
-- ---------------------------------------------------------------------------

CREATE TABLE recon_audit (
  audit_id        UUID PRIMARY KEY,
  sequence_no     BIGINT NOT NULL,
  recon_run_id    UUID REFERENCES recon_runs(recon_run_id),
  plan_id         UUID REFERENCES recon_plans(plan_id),
  event_type      TEXT NOT NULL,
  actor_id        TEXT,
  safe_details    JSONB NOT NULL DEFAULT '{}'::jsonb,
  prev_event_hash CHAR(64),
  event_hash      CHAR(64) NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE UNIQUE INDEX ux_recon_audit_seq ON recon_audit(sequence_no);
CREATE INDEX idx_recon_audit_run ON recon_audit(recon_run_id, sequence_no);

COMMIT;


-- ===========================================================================
-- Role privileges (section 3.1 "Ownership rule", INV04-07, INV04-18)
--
-- This is the production form of the privilege separation. The reconciliation
-- role may read every Project 03 table and may write only recon_* tables. A
-- hand-written UPDATE against deals from this role fails with
-- "permission denied for table deals", which is the test INV04-07 requires.
--
-- Run as a superuser or the owner of the Project 03 tables.
-- ===========================================================================

BEGIN;

CREATE ROLE recon_service NOLOGIN;
CREATE ROLE recon_approver NOLOGIN;

-- Read-only on everything Project 03 owns.
GRANT SELECT ON
  sync_sources, source_epochs, sync_runs, sync_checkpoints,
  deals, accounts_current, positions_current
TO recon_service;

-- Explicitly revoke every write path, including via default privileges.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON
  sync_sources, source_epochs, sync_runs, sync_checkpoints,
  deals, accounts_current, positions_current
FROM recon_service;

-- Full control of its own evidence tables.
GRANT SELECT, INSERT, UPDATE ON
  recon_runs, recon_source_page_receipts, recon_source_items, recon_target_items,
  recon_triggers, recon_scheduler_leases, recon_findings, recon_plans,
  recon_actions, recon_attempts, recon_audit
TO recon_service;

-- Evidence is deleted only by the retention sweep, and never for a run whose
-- repair is still in flight (section 3.3 recon.cleanup).
GRANT DELETE ON recon_source_items, recon_target_items TO recon_service;

-- The audit log is append-only for the service: an UPDATE or DELETE here
-- would defeat the hash chain's purpose.
REVOKE UPDATE, DELETE ON recon_audit FROM recon_service;

-- Approvers write decisions and read evidence; they never run audits.
GRANT SELECT ON recon_runs, recon_findings, recon_plans, recon_actions TO recon_approver;
GRANT INSERT ON recon_approvals TO recon_approver;
GRANT UPDATE (status, reason_code) ON recon_plans TO recon_approver;

-- Neither role may change the schema.
REVOKE CREATE ON SCHEMA public FROM recon_service, recon_approver;

COMMIT;


-- ===========================================================================
-- Verification queries for the release gate (section 14, G0/G1)
-- ===========================================================================

-- G0: no run may claim completeness without a valid receipt chain.
-- Expect zero rows.
--
-- SELECT r.recon_run_id
--   FROM recon_runs r
--  WHERE r.source_complete
--    AND (
--      -- no terminal receipt
--      NOT EXISTS (SELECT 1 FROM recon_source_page_receipts p
--                   WHERE p.recon_run_id = r.recon_run_id AND p.is_terminal)
--      -- indices not contiguous from zero
--      OR (SELECT MAX(page_index) + 1 FROM recon_source_page_receipts p
--           WHERE p.recon_run_id = r.recon_run_id)
--         <> (SELECT COUNT(*) FROM recon_source_page_receipts p
--              WHERE p.recon_run_id = r.recon_run_id)
--      -- declared item counts disagree with stored manifest rows
--      OR (SELECT COALESCE(SUM(item_count),0) FROM recon_source_page_receipts p
--           WHERE p.recon_run_id = r.recon_run_id)
--         <> (SELECT COUNT(*) FROM recon_source_items i
--              WHERE i.recon_run_id = r.recon_run_id)
--      -- more than one distinct cut among the receipts
--      OR (SELECT COUNT(DISTINCT (snapshot_seq, run_id, generation))
--            FROM recon_source_page_receipts p
--           WHERE p.recon_run_id = r.recon_run_id) <> 1
--    );

-- G0: no MATCHED run may hold a finding, and none may be incomplete.
-- Expect zero rows.
--
-- SELECT r.recon_run_id FROM recon_runs r
--  WHERE r.status = 'MATCHED'
--    AND (NOT r.source_complete OR NOT r.target_complete
--         OR EXISTS (SELECT 1 FROM recon_findings f WHERE f.recon_run_id = r.recon_run_id));

-- G0: no PROVEN finding may belong to an inconclusive run. Expect zero rows.
--
-- SELECT f.finding_id FROM recon_findings f
--   JOIN recon_runs r ON r.recon_run_id = f.recon_run_id
--  WHERE f.confidence = 'PROVEN' AND r.status = 'INCONCLUSIVE';

-- G1: no plan may be VERIFIED without an independent verification run that
-- itself reached MATCHED. Expect zero rows.
--
-- SELECT p.plan_id FROM recon_plans p
--  WHERE p.status = 'VERIFIED'
--    AND NOT EXISTS (
--      SELECT 1 FROM recon_runs v
--       WHERE v.verifies_plan_id = p.plan_id AND v.status = 'MATCHED');

-- G1: every dispatched action has exactly one intent id and no duplicate
-- successful attempts. Expect zero rows.
--
-- SELECT a.action_id, COUNT(*) AS committed_attempts
--   FROM recon_actions a JOIN recon_attempts t ON t.action_id = a.action_id
--  WHERE t.status = 'COMMITTED'
--  GROUP BY a.action_id HAVING COUNT(*) > 1;
