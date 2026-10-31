-- Project 08 - PostgreSQL control plane. Design section 5.3.
--
-- Status: the production counterpart of the SQLite schema the test suite
-- exercises. NOT applied to a real server in this repository; release gate G1
-- stays open until a migration and concurrency run against a real PostgreSQL
-- is recorded.
--
-- The design is explicit that DDL alone cannot enforce these invariants
-- (section 5.3: "the DDL alone is not enough"). So this file carries the
-- constraints a database *can* check, and the triggers below close the two
-- gaps where a hand-written UPDATE would otherwise be able to do real damage:
-- publishing a build that was never verified, and mutating an immutable
-- build's identity. Everything else - set equality, the Decimal oracle, read
-- visibility - lives in the application and is covered by its tests.

BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics_control;
SET search_path TO analytics_control, public;

-- ---------------------------------------------------------------------------
-- Builds: many immutable attempts per logical build.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_builds (
    build_id               UUID PRIMARY KEY,
    tenant_id              TEXT NOT NULL,
    source_id              TEXT NOT NULL,
    epoch_id               UUID NOT NULL,
    report_family          TEXT NOT NULL,
    cut_id                 TEXT NOT NULL,
    policy_version         TEXT NOT NULL,
    recon_revision         TEXT NOT NULL,
    source_manifest_sha256 CHAR(64) NOT NULL,
    state                  TEXT NOT NULL CHECK (state IN
        ('REQUESTED','BUILDING','VALIDATING','VERIFIED','PUBLISHED',
         'SUPERSEDED','FAILED','QUARANTINED')),
    lease_token            UUID,
    lease_version          BIGINT NOT NULL DEFAULT 0,
    lease_until            TIMESTAMPTZ,
    evidence_sha256        CHAR(64),
    source_coverage        JSONB NOT NULL DEFAULT '{}'::jsonb,
    attempt_no             INTEGER NOT NULL CHECK (attempt_no > 0),
    failure_reason         TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    -- One attempt number per logical build, so a retry cannot silently
    -- shadow the attempt it replaced.
    UNIQUE (tenant_id, source_id, epoch_id, report_family, cut_id,
            policy_version, attempt_no),
    -- A build may only claim VERIFIED or beyond if it carries an evidence
    -- digest. The matching row in analytics_build_evidence is checked by the
    -- application, since a CHECK cannot look at another table.
    CONSTRAINT verified_requires_evidence CHECK (
        state NOT IN ('VERIFIED','PUBLISHED','SUPERSEDED')
        OR evidence_sha256 IS NOT NULL
    ),
    -- A lease is a token and an expiry together; one without the other is a
    -- half-written claim that fencing logic would misread.
    CONSTRAINT lease_is_complete CHECK (
        (lease_token IS NULL AND lease_until IS NULL)
        OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_builds_scope
    ON analytics_builds (tenant_id, source_id, epoch_id, report_family, state);
CREATE INDEX IF NOT EXISTS idx_builds_lease
    ON analytics_builds (lease_until) WHERE lease_token IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Claims: exactly one active owner per logical build.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_build_claims (
    tenant_id       TEXT NOT NULL,
    source_id       TEXT NOT NULL,
    epoch_id        UUID NOT NULL,
    report_family   TEXT NOT NULL,
    cut_id          TEXT NOT NULL,
    policy_version  TEXT NOT NULL,
    active_build_id UUID REFERENCES analytics_builds(build_id),
    lease_token     UUID,
    lease_version   BIGINT NOT NULL DEFAULT 0,
    lease_until     TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version),
    CONSTRAINT claim_lease_is_complete CHECK (
        (lease_token IS NULL AND lease_until IS NULL)
        OR (lease_token IS NOT NULL AND lease_until IS NOT NULL)
    )
);

-- ---------------------------------------------------------------------------
-- Publication pointer: one current build per family per source epoch.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_publications (
    tenant_id            TEXT NOT NULL,
    source_id            TEXT NOT NULL,
    epoch_id             UUID NOT NULL,
    report_family        TEXT NOT NULL,
    current_build_id     UUID NOT NULL REFERENCES analytics_builds(build_id),
    publication_revision BIGINT NOT NULL CHECK (publication_revision > 0),
    published_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, source_id, epoch_id, report_family)
);

-- Append-only audit of pointer movement, including rollbacks. The revision
-- always increases, even on a rollback, so the trail cannot lie about how
-- many times the pointer moved (INV08-18).
CREATE TABLE IF NOT EXISTS analytics_publication_history (
    history_id           BIGSERIAL PRIMARY KEY,
    tenant_id            TEXT NOT NULL,
    source_id            TEXT NOT NULL,
    epoch_id             UUID NOT NULL,
    report_family        TEXT NOT NULL,
    build_id             UUID NOT NULL REFERENCES analytics_builds(build_id),
    previous_build_id    UUID REFERENCES analytics_builds(build_id),
    publication_revision BIGINT NOT NULL,
    action               TEXT NOT NULL CHECK (action IN ('PUBLISH','ROLLBACK')),
    actor                TEXT NOT NULL CHECK (length(trim(actor)) > 0),
    note                 TEXT,
    recorded_at          TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_pub_history_scope
    ON analytics_publication_history
       (tenant_id, source_id, epoch_id, report_family, history_id);

-- ---------------------------------------------------------------------------
-- Evidence, quarantine, trust and reader pins.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_build_evidence (
    build_id        UUID NOT NULL REFERENCES analytics_builds(build_id),
    evidence_kind   TEXT NOT NULL,
    evidence_sha256 CHAR(64) NOT NULL,
    evidence_json   JSONB NOT NULL,
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (build_id, evidence_kind)
);

CREATE TABLE IF NOT EXISTS analytics_quarantine (
    receipt_id      TEXT PRIMARY KEY,
    tenant_id       TEXT,
    source_id       TEXT,
    epoch_id        UUID,
    topic           TEXT NOT NULL,
    partition       INTEGER NOT NULL CHECK (partition >= 0),
    offset_value    BIGINT NOT NULL CHECK (offset_value >= 0),
    event_id        UUID,
    reason          TEXT NOT NULL,
    detail          JSONB NOT NULL,
    payload_sha256  CHAR(64),
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    resolved        BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_note TEXT,
    -- Idempotent per Kafka coordinate: a replayed poison record must not
    -- multiply receipts (P08-F-09).
    UNIQUE (topic, partition, offset_value),
    -- A resolution without an explanation is how poison quietly disappears.
    CONSTRAINT resolution_has_a_note CHECK (
        resolved = FALSE OR length(trim(coalesce(resolution_note, ''))) > 0
    )
);

CREATE INDEX IF NOT EXISTS idx_quarantine_unresolved
    ON analytics_quarantine (tenant_id, source_id, epoch_id) WHERE resolved = FALSE;

CREATE TABLE IF NOT EXISTS analytics_trust (
    tenant_id      TEXT NOT NULL,
    source_id      TEXT NOT NULL,
    epoch_id       UUID NOT NULL,
    report_family  TEXT NOT NULL,
    cut_id         TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    revision       TEXT NOT NULL,
    verified       BOOLEAN NOT NULL,
    revoked        BOOLEAN NOT NULL DEFAULT FALSE,
    findings       JSONB NOT NULL DEFAULT '[]'::jsonb,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
);

CREATE TABLE IF NOT EXISTS analytics_reader_pins (
    pin_id     TEXT PRIMARY KEY,
    build_id   UUID NOT NULL REFERENCES analytics_builds(build_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_pins_build ON analytics_reader_pins (build_id);

-- ---------------------------------------------------------------------------
-- Triggers for the two invariants a constraint cannot express.
-- ---------------------------------------------------------------------------

-- P08-T-10: an operator with a psql prompt must not be able to force a
-- FAILED or QUARANTINED build to be the current publication. Role grants are
-- the first line of defence; this is the second, because the consequence is a
-- report nobody verified being served as fact.
CREATE OR REPLACE FUNCTION analytics_control.assert_pointer_target_publishable()
RETURNS TRIGGER AS $$
DECLARE
    target_state TEXT;
    target_evidence CHAR(64);
    target_scope RECORD;
BEGIN
    SELECT state, evidence_sha256, tenant_id, source_id, epoch_id, report_family
      INTO target_state, target_evidence, target_scope.tenant_id,
           target_scope.source_id, target_scope.epoch_id, target_scope.report_family
      FROM analytics_control.analytics_builds
     WHERE build_id = NEW.current_build_id;

    IF target_state IS NULL THEN
        RAISE EXCEPTION 'publication points at unknown build %', NEW.current_build_id;
    END IF;
    IF target_state NOT IN ('VERIFIED','PUBLISHED','SUPERSEDED') THEN
        RAISE EXCEPTION
            'refusing to publish build % in state % (must have been VERIFIED)',
            NEW.current_build_id, target_state;
    END IF;
    IF target_evidence IS NULL THEN
        RAISE EXCEPTION
            'refusing to publish build % without an evidence digest',
            NEW.current_build_id;
    END IF;
    IF (target_scope.tenant_id, target_scope.source_id,
        target_scope.epoch_id, target_scope.report_family)
       IS DISTINCT FROM
       (NEW.tenant_id, NEW.source_id, NEW.epoch_id, NEW.report_family) THEN
        RAISE EXCEPTION
            'build % belongs to a different tenant/source/epoch scope',
            NEW.current_build_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_pointer_target_publishable ON analytics_publications;
CREATE TRIGGER trg_pointer_target_publishable
    BEFORE INSERT OR UPDATE ON analytics_publications
    FOR EACH ROW EXECUTE FUNCTION analytics_control.assert_pointer_target_publishable();

-- A build's identity is immutable once created. Only its lifecycle fields may
-- change; rewriting the cut, policy or manifest digest would silently change
-- what a published report was verified against (ADR08-05).
CREATE OR REPLACE FUNCTION analytics_control.assert_build_identity_immutable()
RETURNS TRIGGER AS $$
BEGIN
    IF (NEW.tenant_id, NEW.source_id, NEW.epoch_id, NEW.report_family,
        NEW.cut_id, NEW.policy_version, NEW.source_manifest_sha256,
        NEW.attempt_no, NEW.created_at)
       IS DISTINCT FROM
       (OLD.tenant_id, OLD.source_id, OLD.epoch_id, OLD.report_family,
        OLD.cut_id, OLD.policy_version, OLD.source_manifest_sha256,
        OLD.attempt_no, OLD.created_at) THEN
        RAISE EXCEPTION 'build % identity is immutable', OLD.build_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_build_identity_immutable ON analytics_builds;
CREATE TRIGGER trg_build_identity_immutable
    BEFORE UPDATE ON analytics_builds
    FOR EACH ROW EXECUTE FUNCTION analytics_control.assert_build_identity_immutable();

-- ---------------------------------------------------------------------------
-- Roles. Build mutation, publication and GC are distinct principals
-- (section 5.3). The reader cannot see quarantine payloads at all.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'p08_reader') THEN
        CREATE ROLE p08_reader NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'p08_builder') THEN
        CREATE ROLE p08_builder NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'p08_operator') THEN
        CREATE ROLE p08_operator NOLOGIN;
    END IF;
END
$$;

GRANT USAGE ON SCHEMA analytics_control TO p08_reader, p08_builder, p08_operator;

GRANT SELECT ON analytics_builds, analytics_publications,
                analytics_publication_history, analytics_build_evidence
      TO p08_reader;
GRANT SELECT, INSERT, DELETE ON analytics_reader_pins TO p08_reader;

GRANT SELECT, INSERT, UPDATE ON analytics_builds, analytics_build_claims,
                                analytics_publications, analytics_build_evidence,
                                analytics_trust
      TO p08_builder;
GRANT SELECT, INSERT ON analytics_publication_history TO p08_builder;
GRANT SELECT, INSERT ON analytics_quarantine TO p08_builder;
GRANT USAGE, SELECT ON SEQUENCE analytics_publication_history_history_id_seq TO p08_builder;

GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA analytics_control
      TO p08_operator;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA analytics_control TO p08_operator;

CREATE TABLE IF NOT EXISTS analytics_schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
INSERT INTO analytics_schema_version (version) VALUES (1)
    ON CONFLICT (version) DO NOTHING;

COMMIT;
