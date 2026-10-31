"""Control plane on SQLite, mirroring the ``analytics_control`` PostgreSQL DDL.

SQLite rather than a stubbed dictionary because the guarantees being tested
*are* transactional: a pointer flip that checks four preconditions and then
updates two tables has to be atomic, and a dict cannot fail that honestly.
``BEGIN IMMEDIATE`` plus the same compare-and-set predicates as section 6.5
means a passing concurrency test says something about the real logic.

What SQLite does not give us is spelled out rather than papered over: no
``FOR UPDATE`` (the whole transaction is serialised instead), no LSN, no
replica. Section 5.3's PostgreSQL DDL ships in ``database/postgres`` for
production, and the HA cases (P08-T-12, P08-AF) stay P2 and untested here.

The clock is the control plane's own (section 5.3). A worker passing ``now``
could otherwise declare somebody else's live lease expired.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

from ..canonical import canonical_json, digest_of, ensure_utc, parse_timestamp
from ..clock import Clock, SystemClock
from ..errors import (
    ALLOWED_BUILD_TRANSITIONS,
    AnalyticsError,
    BuildState,
    CompletenessError,
    FencedError,
    IntegrityError,
    Reason,
)
from ..faults.killpoints import FaultPlan, FaultSite, NullFaultPlan
from ..ports.control import (
    BuildRecord,
    ClaimRecord,
    EvidenceRecord,
    PublicationHistoryEntry,
    PublicationRecord,
    QuarantineRecord,
)

SCHEMA_VERSION = 1

_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL);

CREATE TABLE IF NOT EXISTS analytics_builds (
    build_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    report_family TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    recon_revision TEXT NOT NULL,
    source_manifest_sha256 TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN
        ('REQUESTED','BUILDING','VALIDATING','VERIFIED','PUBLISHED',
         'SUPERSEDED','FAILED','QUARANTINED')),
    lease_token TEXT,
    lease_version INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    evidence_sha256 TEXT,
    source_coverage TEXT NOT NULL DEFAULT '{}',
    attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
    failure_reason TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version, attempt_no)
);

CREATE TABLE IF NOT EXISTS analytics_build_claims (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    report_family TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    active_build_id TEXT REFERENCES analytics_builds(build_id),
    lease_token TEXT,
    lease_version INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    PRIMARY KEY (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
);

CREATE TABLE IF NOT EXISTS analytics_publications (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    report_family TEXT NOT NULL,
    current_build_id TEXT NOT NULL REFERENCES analytics_builds(build_id),
    publication_revision INTEGER NOT NULL CHECK (publication_revision > 0),
    published_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, source_id, epoch_id, report_family)
);

CREATE TABLE IF NOT EXISTS analytics_publication_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    report_family TEXT NOT NULL,
    build_id TEXT NOT NULL,
    previous_build_id TEXT,
    publication_revision INTEGER NOT NULL,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    note TEXT,
    recorded_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics_build_evidence (
    build_id TEXT NOT NULL REFERENCES analytics_builds(build_id),
    evidence_kind TEXT NOT NULL,
    evidence_sha256 TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (build_id, evidence_kind)
);

CREATE TABLE IF NOT EXISTS analytics_quarantine (
    receipt_id TEXT PRIMARY KEY,
    tenant_id TEXT,
    source_id TEXT,
    epoch_id TEXT,
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset_value INTEGER NOT NULL,
    event_id TEXT,
    reason TEXT NOT NULL,
    detail TEXT NOT NULL,
    payload_sha256 TEXT,
    recorded_at TEXT NOT NULL,
    resolved INTEGER NOT NULL DEFAULT 0,
    resolution_note TEXT,
    UNIQUE (topic, partition, offset_value)
);

CREATE TABLE IF NOT EXISTS analytics_trust (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    epoch_id TEXT NOT NULL,
    report_family TEXT NOT NULL,
    cut_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    revision TEXT NOT NULL,
    verified INTEGER NOT NULL,
    revoked INTEGER NOT NULL,
    findings TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
);

CREATE TABLE IF NOT EXISTS analytics_reader_pins (
    pin_id TEXT PRIMARY KEY,
    build_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_builds_scope
    ON analytics_builds (tenant_id, source_id, epoch_id, report_family, state);
CREATE INDEX IF NOT EXISTS idx_pins_build ON analytics_reader_pins (build_id);
"""


class SqliteControlPlane:
    """Implements :class:`~analytics.ports.control.ControlPlanePort`."""

    def __init__(
        self,
        path: str = ":memory:",
        *,
        clock: Clock | None = None,
        faults: FaultPlan | None = None,
    ) -> None:
        self._clock = clock or SystemClock()
        self._faults = faults or NullFaultPlan()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA foreign_keys = ON")
        self._conn.execute("PRAGMA journal_mode = WAL" if path != ":memory:" else "PRAGMA synchronous = FULL")
        self._conn.executescript(_DDL)
        if not self._conn.execute("SELECT 1 FROM schema_version").fetchone():
            self._conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # -- transaction helper ------------------------------------------------

    class _Tx:
        """``BEGIN IMMEDIATE`` scope with a modelled lost-ACK window."""

        def __init__(self, owner: "SqliteControlPlane", *, ack_site: str | None = None) -> None:
            self._owner = owner
            self._ack_site = ack_site

        def __enter__(self) -> sqlite3.Connection:
            self._owner._faults.fire(FaultSite.PG_UNAVAILABLE)
            self._owner._lock.acquire()
            self._owner._conn.execute("BEGIN IMMEDIATE")
            return self._owner._conn

        def __exit__(self, exc_type, exc, tb) -> bool:
            try:
                if exc_type is None:
                    self._owner._conn.execute("COMMIT")
                    if self._ack_site is not None:
                        # The commit is durable; the caller is about to be
                        # told nothing. KP08-25 / P08-S-07.
                        self._owner._faults.fire(self._ack_site)
                else:
                    self._owner._conn.execute("ROLLBACK")
            finally:
                self._owner._lock.release()
            return False

    def _now(self) -> datetime:
        return ensure_utc(self._clock.now(), "control_plane_clock")

    # -- builds ------------------------------------------------------------

    def create_build(self, record: BuildRecord) -> BuildRecord:
        now = self._now()
        with self._Tx(self) as conn:
            existing = conn.execute(
                "SELECT build_id FROM analytics_builds WHERE build_id = ?", (record.build_id,)
            ).fetchone()
            if existing is not None:
                # P08-S-10: a retry must mint a new immutable id, never reuse
                # one whose ClickHouse rows may be half-written.
                raise IntegrityError(
                    Reason.BUILD_ID_REUSED,
                    f"build_id {record.build_id} already exists",
                    build_id=record.build_id,
                )
            conn.execute(
                """
                INSERT INTO analytics_builds (
                    build_id, tenant_id, source_id, epoch_id, report_family, cut_id,
                    policy_version, recon_revision, source_manifest_sha256, state,
                    lease_token, lease_version, lease_until, evidence_sha256,
                    source_coverage, attempt_no, failure_reason, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record.build_id,
                    record.tenant_id,
                    record.source_id,
                    record.epoch_id,
                    record.report_family,
                    record.cut_id,
                    record.policy_version,
                    record.recon_revision,
                    record.source_manifest_sha256,
                    str(record.state),
                    record.lease_token,
                    record.lease_version,
                    _iso(record.lease_until),
                    record.evidence_sha256,
                    canonical_json(dict(record.source_coverage)),
                    record.attempt_no,
                    record.failure_reason,
                    _iso(now),
                    _iso(now),
                ),
            )
        return self.get_build(record.build_id)  # type: ignore[return-value]

    def get_build(self, build_id: str) -> BuildRecord | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM analytics_builds WHERE build_id = ?", (build_id,)
            ).fetchone()
        return _build_from_row(row) if row else None

    def list_builds(
        self,
        *,
        tenant_id: str,
        source_id: str | None = None,
        epoch_id: str | None = None,
        report_family: str | None = None,
        states: Sequence[BuildState] | None = None,
    ) -> list[BuildRecord]:
        clauses = ["tenant_id = ?"]
        params: list[Any] = [tenant_id]
        for column, value in (
            ("source_id", source_id),
            ("epoch_id", epoch_id),
            ("report_family", report_family),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if states:
            placeholders = ",".join("?" for _ in states)
            clauses.append(f"state IN ({placeholders})")
            params.extend(str(s) for s in states)
        sql = (
            "SELECT * FROM analytics_builds WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at, attempt_no"
        )
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_build_from_row(row) for row in rows]

    def next_attempt_no(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        report_family: str,
        cut_id: str,
        policy_version: str,
    ) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COALESCE(MAX(attempt_no), 0) AS latest FROM analytics_builds
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version),
            ).fetchone()
        return int(row["latest"]) + 1

    def transition_build(
        self,
        *,
        build_id: str,
        expected_state: BuildState,
        new_state: BuildState,
        lease_token: str | None,
        evidence_sha256: str | None = None,
        source_coverage: Mapping[str, Any] | None = None,
        failure_reason: str | None = None,
    ) -> BuildRecord:
        if new_state not in ALLOWED_BUILD_TRANSITIONS[expected_state]:
            raise AnalyticsError(
                Reason.BUILD_STATE_INVALID,
                f"{expected_state} -> {new_state} is not an allowed transition",
                build_id=build_id,
            )
        now = self._now()
        with self._Tx(self) as conn:
            row = conn.execute(
                "SELECT * FROM analytics_builds WHERE build_id = ?", (build_id,)
            ).fetchone()
            if row is None:
                raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown build", build_id=build_id)
            if row["state"] != str(expected_state):
                raise AnalyticsError(
                    Reason.BUILD_STATE_INVALID,
                    f"build is {row['state']}, expected {expected_state}",
                    build_id=build_id,
                    actual=row["state"],
                )
            if row["lease_token"] is not None and row["lease_token"] != lease_token:
                # INV08-11: a worker that lost its lease cannot move state,
                # even if the state machine would otherwise permit it.
                raise FencedError(
                    Reason.LEASE_HELD_BY_OTHER,
                    "build is owned by a different lease token",
                    build_id=build_id,
                )
            cursor = conn.execute(
                """
                UPDATE analytics_builds
                SET state = ?, updated_at = ?,
                    evidence_sha256 = COALESCE(?, evidence_sha256),
                    source_coverage = COALESCE(?, source_coverage),
                    failure_reason = COALESCE(?, failure_reason)
                WHERE build_id = ? AND state = ?
                """,
                (
                    str(new_state),
                    _iso(now),
                    evidence_sha256,
                    canonical_json(dict(source_coverage)) if source_coverage is not None else None,
                    failure_reason,
                    build_id,
                    str(expected_state),
                ),
            )
            if cursor.rowcount != 1:  # pragma: no cover - serialised by BEGIN IMMEDIATE
                raise AnalyticsError(Reason.BUILD_STATE_INVALID, "state changed concurrently")
        return self.get_build(build_id)  # type: ignore[return-value]

    # -- claims / leases ---------------------------------------------------

    def acquire_claim(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        report_family: str,
        cut_id: str,
        policy_version: str,
        build_id: str,
        lease_token: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ClaimRecord:
        """Take ownership of one logical build.

        ``now`` is accepted for interface symmetry and ignored: expiry is
        judged by the control plane's clock, per section 5.3.
        """
        del now
        current = self._now()
        until = current + timedelta(seconds=lease_seconds)
        key = (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
        with self._Tx(self) as conn:
            row = conn.execute(
                """
                SELECT * FROM analytics_build_claims
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                key,
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO analytics_build_claims (
                        tenant_id, source_id, epoch_id, report_family, cut_id, policy_version,
                        active_build_id, lease_token, lease_version, lease_until
                    ) VALUES (?,?,?,?,?,?,?,?,?,?)
                    """,
                    (*key, build_id, lease_token, 1, _iso(until)),
                )
            else:
                held_until = _parse(row["lease_until"])
                live = (
                    row["lease_token"] is not None
                    and held_until is not None
                    and held_until > current
                )
                if live and row["lease_token"] != lease_token:
                    raise FencedError(
                        Reason.LEASE_HELD_BY_OTHER,
                        "another worker holds a live lease for this cut",
                        active_build_id=row["active_build_id"],
                        lease_until=row["lease_until"],
                    )
                conn.execute(
                    """
                    UPDATE analytics_build_claims
                    SET active_build_id=?, lease_token=?, lease_version=lease_version+1,
                        lease_until=?
                    WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                      AND cut_id=? AND policy_version=? AND lease_version=?
                    """,
                    (build_id, lease_token, _iso(until), *key, row["lease_version"]),
                )
            conn.execute(
                "UPDATE analytics_builds SET lease_token=?, lease_until=?, "
                "lease_version=lease_version+1, updated_at=? WHERE build_id=?",
                (lease_token, _iso(until), _iso(current), build_id),
            )
        return self.get_claim(claim_key=key)  # type: ignore[return-value]

    def renew_claim(
        self,
        *,
        claim_key: tuple[str, str, str, str, str, str],
        lease_token: str,
        lease_seconds: int,
        now: datetime | None = None,
    ) -> ClaimRecord:
        del now
        current = self._now()
        until = current + timedelta(seconds=lease_seconds)
        with self._Tx(self) as conn:
            row = conn.execute(
                """
                SELECT * FROM analytics_build_claims
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                claim_key,
            ).fetchone()
            if row is None or row["lease_token"] != lease_token:
                raise FencedError(Reason.LEASE_LOST, "lease is no longer held by this token")
            held_until = _parse(row["lease_until"])
            if held_until is None or held_until <= current:
                # Renewing an already-expired lease would resurrect a worker
                # another owner may have replaced (P08-S-08).
                raise FencedError(Reason.LEASE_LOST, "lease already expired; re-acquire instead")
            conn.execute(
                """
                UPDATE analytics_build_claims
                SET lease_until=?, lease_version=lease_version+1
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=? AND lease_token=?
                """,
                (_iso(until), *claim_key, lease_token),
            )
            if row["active_build_id"]:
                conn.execute(
                    "UPDATE analytics_builds SET lease_until=? WHERE build_id=?",
                    (_iso(until), row["active_build_id"]),
                )
        return self.get_claim(claim_key=claim_key)  # type: ignore[return-value]

    def release_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str], lease_token: str
    ) -> None:
        """Drop the claim *and* clear the lease on the build it pointed at.

        Clearing only the claim row leaves the build row advertising a lease
        nobody holds. Nothing unsafe follows from that - a stale token still
        fails every CAS - but an operator reading the build would see
        ``LIVE_LEASE``, and the GC policy would report that as the reason a
        finished build is protected. Both are misleading, so the release
        covers both rows.
        """
        with self._Tx(self) as conn:
            row = conn.execute(
                """
                SELECT active_build_id FROM analytics_build_claims
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=? AND lease_token=?
                """,
                (*claim_key, lease_token),
            ).fetchone()
            conn.execute(
                """
                UPDATE analytics_build_claims
                SET lease_token=NULL, lease_until=NULL, lease_version=lease_version+1
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=? AND lease_token=?
                """,
                (*claim_key, lease_token),
            )
            if row is not None and row["active_build_id"]:
                conn.execute(
                    "UPDATE analytics_builds SET lease_token=NULL, lease_until=NULL, "
                    "lease_version=lease_version+1, updated_at=? "
                    "WHERE build_id=? AND lease_token=?",
                    (_iso(self._now()), row["active_build_id"], lease_token),
                )

    def get_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str]
    ) -> ClaimRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM analytics_build_claims
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                claim_key,
            ).fetchone()
        if row is None:
            return None
        return ClaimRecord(
            tenant_id=row["tenant_id"],
            source_id=row["source_id"],
            epoch_id=row["epoch_id"],
            report_family=row["report_family"],
            cut_id=row["cut_id"],
            policy_version=row["policy_version"],
            active_build_id=row["active_build_id"],
            lease_token=row["lease_token"],
            lease_version=row["lease_version"],
            lease_until=_parse(row["lease_until"]),
        )

    def assert_lease_held(
        self,
        *,
        claim_key: tuple[str, str, str, str, str, str],
        lease_token: str,
        now: datetime | None = None,
    ) -> None:
        del now
        claim = self.get_claim(claim_key=claim_key)
        current = self._now()
        if claim is None or claim.lease_token != lease_token:
            raise FencedError(Reason.LEASE_LOST, "lease is not held by this token")
        if claim.lease_until is None or claim.lease_until <= current:
            raise FencedError(
                Reason.LEASE_LOST,
                "lease expired",
                lease_until=_iso(claim.lease_until),
                now=_iso(current),
            )

    # -- publication -------------------------------------------------------

    def get_publication(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> PublicationRecord | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM analytics_publications
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                """,
                (tenant_id, source_id, epoch_id, report_family),
            ).fetchone()
        if row is None:
            return None
        return PublicationRecord(
            tenant_id=row["tenant_id"],
            source_id=row["source_id"],
            epoch_id=row["epoch_id"],
            report_family=row["report_family"],
            current_build_id=row["current_build_id"],
            publication_revision=row["publication_revision"],
            published_at=_parse(row["published_at"]),  # type: ignore[arg-type]
        )

    def publish_pointer(
        self,
        *,
        build_id: str,
        lease_token: str,
        expected_revision: int | None,
        actor: str,
        now: datetime | None = None,
        note: str | None = None,
    ) -> PublicationRecord:
        """Flip the pointer. Section 6.5.

        Every precondition is re-checked *inside* the transaction, because
        each one is a race somebody has lost in production: the build may have
        been quarantined, the lease may have moved, another builder may have
        published, and the evidence may never have been written.
        """
        del now
        current = self._now()
        with self._Tx(self, ack_site=FaultSite.PG_COMMIT_ACK_LOST) as conn:
            build = conn.execute(
                "SELECT * FROM analytics_builds WHERE build_id = ?", (build_id,)
            ).fetchone()
            if build is None:
                raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown build", build_id=build_id)
            if build["state"] != str(BuildState.VERIFIED):
                raise CompletenessError(
                    Reason.BUILD_NOT_VERIFIED,
                    f"only a VERIFIED build may be published; this one is {build['state']}",
                    build_id=build_id,
                    state=build["state"],
                )
            if not build["evidence_sha256"]:
                raise CompletenessError(
                    Reason.EVIDENCE_MISSING,
                    "a build without durable evidence may not be published",
                    build_id=build_id,
                )
            evidence_rows = conn.execute(
                "SELECT COUNT(*) AS n FROM analytics_build_evidence WHERE build_id = ?",
                (build_id,),
            ).fetchone()
            if not evidence_rows["n"]:
                raise CompletenessError(
                    Reason.EVIDENCE_MISSING, "no evidence records stored", build_id=build_id
                )

            claim_key = (
                build["tenant_id"],
                build["source_id"],
                build["epoch_id"],
                build["report_family"],
                build["cut_id"],
                build["policy_version"],
            )
            claim = conn.execute(
                """
                SELECT * FROM analytics_build_claims
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                claim_key,
            ).fetchone()
            if claim is None or claim["lease_token"] != lease_token:
                raise FencedError(
                    Reason.LEASE_LOST, "publisher does not hold the active claim", build_id=build_id
                )
            held_until = _parse(claim["lease_until"])
            if held_until is None or held_until <= current:
                raise FencedError(Reason.LEASE_LOST, "lease expired before publication")
            if claim["active_build_id"] != build_id:
                raise FencedError(
                    Reason.LEASE_LOST,
                    "claim points at a different active build",
                    active_build_id=claim["active_build_id"],
                )

            trust = conn.execute(
                """
                SELECT * FROM analytics_trust
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                claim_key,
            ).fetchone()
            if trust is None:
                raise CompletenessError(
                    Reason.RECONCILIATION_NOT_VERIFIED,
                    "no reconciliation trust record for this cut",
                    build_id=build_id,
                )
            if trust["revoked"] or not trust["verified"]:
                # Section 6.7: P04 can revoke between the build's own check
                # and this moment, so the gate re-reads trust here.
                raise CompletenessError(
                    Reason.RECONCILIATION_REVOKED
                    if trust["revoked"]
                    else Reason.RECONCILIATION_NOT_VERIFIED,
                    "reconciliation trust does not permit publication",
                    revision=trust["revision"],
                )
            if trust["revision"] != build["recon_revision"]:
                raise CompletenessError(
                    Reason.RECONCILIATION_REVOKED,
                    "reconciliation revision moved after the build pinned it",
                    pinned=build["recon_revision"],
                    current=trust["revision"],
                )

            pointer = conn.execute(
                """
                SELECT * FROM analytics_publications
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                """,
                claim_key[:4],
            ).fetchone()
            previous_build_id = pointer["current_build_id"] if pointer else None
            if pointer is None:
                if expected_revision is not None:
                    raise AnalyticsError(
                        Reason.PUBLICATION_REVISION_STALE,
                        "no publication exists yet; expected_revision must be None",
                        expected=expected_revision,
                    )
                revision = 1
                conn.execute(
                    """
                    INSERT INTO analytics_publications (
                        tenant_id, source_id, epoch_id, report_family,
                        current_build_id, publication_revision, published_at
                    ) VALUES (?,?,?,?,?,?,?)
                    """,
                    (*claim_key[:4], build_id, revision, _iso(current)),
                )
            else:
                if expected_revision != pointer["publication_revision"]:
                    # P08-T-04: a stale worker retrying its CAS after someone
                    # else published must lose.
                    raise FencedError(
                        Reason.PUBLICATION_REVISION_STALE,
                        "publication revision changed since it was read",
                        expected=expected_revision,
                        actual=pointer["publication_revision"],
                    )
                revision = pointer["publication_revision"] + 1
                conn.execute(
                    """
                    UPDATE analytics_publications
                    SET current_build_id=?, publication_revision=?, published_at=?
                    WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                      AND publication_revision=?
                    """,
                    (
                        build_id,
                        revision,
                        _iso(current),
                        *claim_key[:4],
                        pointer["publication_revision"],
                    ),
                )
                if previous_build_id and previous_build_id != build_id:
                    conn.execute(
                        "UPDATE analytics_builds SET state=?, updated_at=? "
                        "WHERE build_id=? AND state=?",
                        (
                            str(BuildState.SUPERSEDED),
                            _iso(current),
                            previous_build_id,
                            str(BuildState.PUBLISHED),
                        ),
                    )

            conn.execute(
                "UPDATE analytics_builds SET state=?, updated_at=? WHERE build_id=? AND state=?",
                (str(BuildState.PUBLISHED), _iso(current), build_id, str(BuildState.VERIFIED)),
            )
            conn.execute(
                """
                INSERT INTO analytics_publication_history (
                    tenant_id, source_id, epoch_id, report_family, build_id,
                    previous_build_id, publication_revision, action, actor, note, recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    *claim_key[:4],
                    build_id,
                    previous_build_id,
                    revision,
                    "PUBLISH",
                    actor,
                    note,
                    _iso(current),
                ),
            )
        return self.get_publication(
            tenant_id=claim_key[0],
            source_id=claim_key[1],
            epoch_id=claim_key[2],
            report_family=claim_key[3],
        )  # type: ignore[return-value]

    def rollback_pointer(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        report_family: str,
        target_build_id: str,
        expected_revision: int,
        actor: str,
        now: datetime | None = None,
        note: str | None = None,
    ) -> PublicationRecord:
        """Point back at an earlier build. INV08-18.

        The revision still increases. Rewinding it would make the audit trail
        lie about how many times the pointer moved, and would let a later
        stale CAS succeed.
        """
        del now
        current = self._now()
        with self._Tx(self) as conn:
            pointer = conn.execute(
                """
                SELECT * FROM analytics_publications
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                """,
                (tenant_id, source_id, epoch_id, report_family),
            ).fetchone()
            if pointer is None:
                raise AnalyticsError(Reason.NO_PUBLICATION, "nothing to roll back")
            if pointer["publication_revision"] != expected_revision:
                raise FencedError(
                    Reason.PUBLICATION_REVISION_STALE,
                    "rollback saw a stale revision",
                    expected=expected_revision,
                    actual=pointer["publication_revision"],
                )
            target = conn.execute(
                "SELECT * FROM analytics_builds WHERE build_id=?", (target_build_id,)
            ).fetchone()
            if target is None:
                raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown rollback target")
            if target["state"] not in (
                str(BuildState.SUPERSEDED),
                str(BuildState.PUBLISHED),
                str(BuildState.VERIFIED),
            ):
                raise CompletenessError(
                    Reason.BUILD_NOT_VERIFIED,
                    "rollback target was never verified",
                    state=target["state"],
                )
            if (
                target["tenant_id"],
                target["source_id"],
                target["epoch_id"],
                target["report_family"],
            ) != (tenant_id, source_id, epoch_id, report_family):
                raise AnalyticsError(
                    Reason.FORBIDDEN_SCOPE,
                    "rollback target belongs to a different tenant/source/epoch scope",
                    build_id=target_build_id,
                )
            revision = pointer["publication_revision"] + 1
            conn.execute(
                """
                UPDATE analytics_publications
                SET current_build_id=?, publication_revision=?, published_at=?
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND publication_revision=?
                """,
                (
                    target_build_id,
                    revision,
                    _iso(current),
                    tenant_id,
                    source_id,
                    epoch_id,
                    report_family,
                    expected_revision,
                ),
            )
            conn.execute(
                "UPDATE analytics_builds SET state=?, updated_at=? WHERE build_id=? AND state=?",
                (
                    str(BuildState.SUPERSEDED),
                    _iso(current),
                    pointer["current_build_id"],
                    str(BuildState.PUBLISHED),
                ),
            )
            conn.execute(
                "UPDATE analytics_builds SET state=?, updated_at=? WHERE build_id=?",
                (str(BuildState.PUBLISHED), _iso(current), target_build_id),
            )
            conn.execute(
                """
                INSERT INTO analytics_publication_history (
                    tenant_id, source_id, epoch_id, report_family, build_id,
                    previous_build_id, publication_revision, action, actor, note, recorded_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    tenant_id,
                    source_id,
                    epoch_id,
                    report_family,
                    target_build_id,
                    pointer["current_build_id"],
                    revision,
                    "ROLLBACK",
                    actor,
                    note,
                    _iso(current),
                ),
            )
        return self.get_publication(
            tenant_id=tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
        )  # type: ignore[return-value]

    def publication_history(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> list[PublicationHistoryEntry]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM analytics_publication_history
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                ORDER BY history_id
                """,
                (tenant_id, source_id, epoch_id, report_family),
            ).fetchall()
        return [
            PublicationHistoryEntry(
                tenant_id=row["tenant_id"],
                source_id=row["source_id"],
                epoch_id=row["epoch_id"],
                report_family=row["report_family"],
                build_id=row["build_id"],
                previous_build_id=row["previous_build_id"],
                publication_revision=row["publication_revision"],
                action=row["action"],
                actor=row["actor"],
                note=row["note"],
                recorded_at=_parse(row["recorded_at"]),  # type: ignore[arg-type]
            )
            for row in rows
        ]

    # -- evidence ----------------------------------------------------------

    def put_evidence(
        self,
        *,
        build_id: str,
        evidence_kind: str,
        evidence_json: Mapping[str, Any],
        now: datetime | None = None,
    ) -> EvidenceRecord:
        del now
        current = self._now()
        payload = dict(evidence_json)
        sha = digest_of(payload)
        with self._Tx(self) as conn:
            conn.execute(
                """
                INSERT INTO analytics_build_evidence (
                    build_id, evidence_kind, evidence_sha256, evidence_json, recorded_at
                ) VALUES (?,?,?,?,?)
                ON CONFLICT (build_id, evidence_kind) DO UPDATE SET
                    evidence_sha256=excluded.evidence_sha256,
                    evidence_json=excluded.evidence_json,
                    recorded_at=excluded.recorded_at
                """,
                (build_id, evidence_kind, sha, canonical_json(payload), _iso(current)),
            )
        return EvidenceRecord(
            build_id=build_id,
            evidence_kind=evidence_kind,
            evidence_sha256=sha,
            evidence_json=payload,
            recorded_at=current,
        )

    def get_evidence(self, *, build_id: str) -> list[EvidenceRecord]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM analytics_build_evidence WHERE build_id=? ORDER BY evidence_kind",
                (build_id,),
            ).fetchall()
        return [
            EvidenceRecord(
                build_id=row["build_id"],
                evidence_kind=row["evidence_kind"],
                evidence_sha256=row["evidence_sha256"],
                evidence_json=json.loads(row["evidence_json"]),
                recorded_at=_parse(row["recorded_at"]),  # type: ignore[arg-type]
            )
            for row in rows
        ]

    # -- quarantine --------------------------------------------------------

    def record_quarantine(self, record: QuarantineRecord) -> QuarantineRecord:
        with self._Tx(self) as conn:
            existing = conn.execute(
                "SELECT * FROM analytics_quarantine WHERE topic=? AND partition=? AND offset_value=?",
                (record.topic, record.partition, record.offset),
            ).fetchone()
            if existing is not None:
                # P08-F-09: replayed poison must not multiply receipts, so the
                # first receipt wins and is returned unchanged.
                return _quarantine_from_row(existing)
            conn.execute(
                """
                INSERT INTO analytics_quarantine (
                    receipt_id, tenant_id, source_id, epoch_id, topic, partition, offset_value,
                    event_id, reason, detail, payload_sha256, recorded_at, resolved, resolution_note
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    record.receipt_id,
                    record.tenant_id,
                    record.source_id,
                    record.epoch_id,
                    record.topic,
                    record.partition,
                    record.offset,
                    record.event_id,
                    record.reason,
                    canonical_json(dict(record.detail)),
                    record.payload_sha256,
                    _iso(record.recorded_at),
                    1 if record.resolved else 0,
                    record.resolution_note,
                ),
            )
        return record

    def list_quarantine(
        self,
        *,
        tenant_id: str | None = None,
        source_id: str | None = None,
        epoch_id: str | None = None,
        unresolved_only: bool = True,
    ) -> list[QuarantineRecord]:
        clauses: list[str] = []
        params: list[Any] = []
        for column, value in (
            ("tenant_id", tenant_id),
            ("source_id", source_id),
            ("epoch_id", epoch_id),
        ):
            if value is not None:
                clauses.append(f"{column} = ?")
                params.append(value)
        if unresolved_only:
            clauses.append("resolved = 0")
        sql = "SELECT * FROM analytics_quarantine"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY topic, partition, offset_value"
        with self._lock:
            rows = self._conn.execute(sql, params).fetchall()
        return [_quarantine_from_row(row) for row in rows]

    def resolve_quarantine(self, *, receipt_id: str, note: str) -> QuarantineRecord:
        with self._Tx(self) as conn:
            row = conn.execute(
                "SELECT * FROM analytics_quarantine WHERE receipt_id=?", (receipt_id,)
            ).fetchone()
            if row is None:
                raise AnalyticsError(Reason.FIELD_INVALID, "unknown quarantine receipt")
            conn.execute(
                "UPDATE analytics_quarantine SET resolved=1, resolution_note=? WHERE receipt_id=?",
                (note, receipt_id),
            )
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM analytics_quarantine WHERE receipt_id=?", (receipt_id,)
            ).fetchone()
        return _quarantine_from_row(row)

    # -- trust -------------------------------------------------------------

    def put_trust(
        self,
        *,
        cut_key: tuple[str, str, str, str, str, str],
        revision: str,
        verified: bool,
        revoked: bool = False,
        findings: Sequence[str] = (),
        now: datetime | None = None,
    ) -> None:
        del now
        current = self._now()
        with self._Tx(self) as conn:
            conn.execute(
                """
                INSERT INTO analytics_trust (
                    tenant_id, source_id, epoch_id, report_family, cut_id, policy_version,
                    revision, verified, revoked, findings, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
                DO UPDATE SET revision=excluded.revision, verified=excluded.verified,
                              revoked=excluded.revoked, findings=excluded.findings,
                              updated_at=excluded.updated_at
                """,
                (
                    *cut_key,
                    revision,
                    1 if verified else 0,
                    1 if revoked else 0,
                    canonical_json(list(findings)),
                    _iso(current),
                ),
            )

    def get_trust(
        self, *, cut_key: tuple[str, str, str, str, str, str]
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT * FROM analytics_trust
                WHERE tenant_id=? AND source_id=? AND epoch_id=? AND report_family=?
                  AND cut_id=? AND policy_version=?
                """,
                cut_key,
            ).fetchone()
        if row is None:
            return None
        return {
            "revision": row["revision"],
            "verified": bool(row["verified"]),
            "revoked": bool(row["revoked"]),
            "findings": json.loads(row["findings"]),
            "updated_at": row["updated_at"],
        }

    # -- reader pins -------------------------------------------------------

    def register_reader_pin(self, *, build_id: str, pin_id: str, now: datetime | None = None) -> None:
        del now
        with self._Tx(self) as conn:
            conn.execute(
                "INSERT OR REPLACE INTO analytics_reader_pins (pin_id, build_id, created_at) "
                "VALUES (?,?,?)",
                (pin_id, build_id, _iso(self._now())),
            )

    def release_reader_pin(self, *, pin_id: str) -> None:
        with self._Tx(self) as conn:
            conn.execute("DELETE FROM analytics_reader_pins WHERE pin_id=?", (pin_id,))

    def active_reader_pins(self, *, build_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM analytics_reader_pins WHERE build_id=?", (build_id,)
            ).fetchone()
        return int(row["n"])


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    return ensure_utc(value).isoformat()


def _parse(value: str | None) -> datetime | None:
    if value is None:
        return None
    return parse_timestamp(value)


def _build_from_row(row: sqlite3.Row) -> BuildRecord:
    return BuildRecord(
        build_id=row["build_id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=row["epoch_id"],
        report_family=row["report_family"],
        cut_id=row["cut_id"],
        policy_version=row["policy_version"],
        recon_revision=row["recon_revision"],
        source_manifest_sha256=row["source_manifest_sha256"],
        state=BuildState(row["state"]),
        attempt_no=row["attempt_no"],
        lease_token=row["lease_token"],
        lease_version=row["lease_version"],
        lease_until=_parse(row["lease_until"]),
        evidence_sha256=row["evidence_sha256"],
        source_coverage=json.loads(row["source_coverage"]),
        created_at=_parse(row["created_at"]),
        updated_at=_parse(row["updated_at"]),
        failure_reason=row["failure_reason"],
    )


def _quarantine_from_row(row: sqlite3.Row) -> QuarantineRecord:
    return QuarantineRecord(
        receipt_id=row["receipt_id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=row["epoch_id"],
        topic=row["topic"],
        partition=row["partition"],
        offset=row["offset_value"],
        event_id=row["event_id"],
        reason=row["reason"],
        detail=json.loads(row["detail"]),
        payload_sha256=row["payload_sha256"],
        recorded_at=_parse(row["recorded_at"]),  # type: ignore[arg-type]
        resolved=bool(row["resolved"]),
        resolution_note=row["resolution_note"],
    )
