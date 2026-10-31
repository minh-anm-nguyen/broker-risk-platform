-- 002_p04_evidence.sql  (PostgreSQL)
--
-- The Project 04 reconciliation evidence, as Project 05 needs to read it.
-- Again not owned here; the file documents the contract the quality gate
-- depends on.
--
-- The design decision worth stating: evidence is scoped and versioned.
--
--   covered_accounts / covered_deal_watermark  -- what the verification
--       actually examined. A VERIFIED row for accounts {A} says nothing about
--       account B, and one produced at watermark 100 says nothing about the
--       deals that arrived by watermark 150 (WF5-035, WF5-178).
--
--   revision -- a monotonic counter, not a timestamp. The cache key includes
--       it, so a finding raised between a cache lookup and a response
--       invalidates the cached answer (WF5-208). A wall-clock timestamp cannot
--       do that job reliably across hosts.

CREATE TABLE IF NOT EXISTS recon_evidence (
    evidence_id            UUID PRIMARY KEY,
    epoch_id               UUID NOT NULL REFERENCES source_epochs(epoch_id),
    revision               BIGINT NOT NULL CHECK (revision >= 0),
    status                 TEXT NOT NULL CHECK (status IN
        ('VERIFIED', 'MATCHED', 'INCONCLUSIVE', 'BLOCKED', 'DISPUTED', 'APPROVED', 'COMMITTED')),
    -- APPROVED and COMMITTED are deliberately in the blocking set at the
    -- application layer: a repair that has been approved, or even written, is
    -- not a repair that has been independently verified (WF5-172, WF5-173).
    covered_accounts       TEXT NOT NULL,          -- JSON array of account ids
    covered_deal_watermark BIGINT,
    scope_known            BOOLEAN NOT NULL DEFAULT TRUE,
    created_at             TIMESTAMPTZ NOT NULL,
    CONSTRAINT uq_recon_epoch_revision UNIQUE (epoch_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_recon_latest
    ON recon_evidence (epoch_id, revision DESC);

CREATE TABLE IF NOT EXISTS recon_findings (
    finding_id   UUID PRIMARY KEY,
    evidence_id  UUID NOT NULL REFERENCES recon_evidence(evidence_id),
    epoch_id     UUID NOT NULL REFERENCES source_epochs(epoch_id),
    severity     TEXT NOT NULL CHECK (severity IN ('BLOCKING', 'WARNING', 'INFO')),
    account_id   TEXT,
    symbol       TEXT,
    deal_id      TEXT,
    finding_code TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_recon_findings_scope
    ON recon_findings (epoch_id, severity, account_id);

-- GRANT SELECT ON recon_evidence, recon_findings TO risk_reader;
-- REVOKE INSERT, UPDATE, DELETE, TRUNCATE ON recon_evidence, recon_findings FROM risk_reader;
