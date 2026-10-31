"""Control plane on PostgreSQL. Section 5.3.

**Coverage status:** not exercised by the offline suite - it needs a real
server. The guard *logic* is shared with :mod:`control_sqlite`, which is
fully tested, by reusing the same SQL text with a translated placeholder
style. What remains unverified here is the driver integration and the
PostgreSQL-specific locking, so release gate G1 stays open until an
integration run against a real PostgreSQL is recorded.

PostgreSQL adds two things the SQLite adapter emulates by serialising the
whole transaction, and both are used where section 6.5 calls for them:

* ``SELECT ... FOR UPDATE`` on the pointer, build and claim rows, so two
  publishers contend on rows rather than on a process-wide lock;
* ``SERIALIZABLE`` isolation for the publication transaction, so a
  write-skew between the pointer read and the pointer update cannot commit.

What it does **not** add is a distributed transaction. The pointer flip is
still atomic only within PostgreSQL (section 3.3), and P04's verdict still
lives in another transaction domain (section 6.7).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any, Mapping, Sequence

from ..canonical import canonical_json, digest_of, ensure_utc
from ..clock import Clock, SystemClock
from ..errors import (
    ALLOWED_BUILD_TRANSITIONS,
    AnalyticsError,
    BuildState,
    CompletenessError,
    ConfigurationError,
    FencedError,
    IntegrityError,
    Reason,
)
from ..ports.control import (
    BuildRecord,
    ClaimRecord,
    EvidenceRecord,
    PublicationHistoryEntry,
    PublicationRecord,
    QuarantineRecord,
)

SCHEMA = "analytics_control"


class PostgresControlPlane:
    """Implements :class:`~analytics.ports.control.ControlPlanePort`."""

    def __init__(
        self,
        *,
        dsn: str,
        clock: Clock | None = None,
        connection: Any | None = None,
        statement_timeout_ms: int = 5_000,
    ) -> None:
        if dsn.startswith("sqlite"):
            raise ConfigurationError(
                "PostgresControlPlane received a sqlite DSN; use SqliteControlPlane",
                dsn=dsn.split("://", 1)[0],
            )
        self._clock = clock or SystemClock()
        self._statement_timeout_ms = statement_timeout_ms
        self._conn = connection or self._connect(dsn)

    def _connect(self, dsn: str):
        try:
            import psycopg
        except ImportError as exc:  # pragma: no cover - driver optional
            raise ConfigurationError(
                "psycopg is required for the PostgreSQL control plane; "
                "install the 'infra' extra"
            ) from exc
        conn = psycopg.connect(dsn, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(self._statement_timeout_ms)}")
            cur.execute(f"SET search_path TO {SCHEMA}, public")
        return conn

    def close(self) -> None:
        self._conn.close()

    # -- transaction helpers ----------------------------------------------

    class _Tx:
        def __init__(self, owner: "PostgresControlPlane", *, serializable: bool = False) -> None:
            self._owner = owner
            self._serializable = serializable
            self._cursor = None

        def __enter__(self):
            self._cursor = self._owner._conn.cursor()
            level = "SERIALIZABLE" if self._serializable else "READ COMMITTED"
            self._cursor.execute(f"BEGIN ISOLATION LEVEL {level}")
            return self._cursor

        def __exit__(self, exc_type, exc, tb) -> bool:
            assert self._cursor is not None
            try:
                if exc_type is None:
                    self._cursor.execute("COMMIT")
                else:
                    self._cursor.execute("ROLLBACK")
            finally:
                self._cursor.close()
            return False

    def _now(self) -> datetime:
        return ensure_utc(self._clock.now(), "control_plane_clock")

    def _query(self, sql: str, params: Sequence[Any] = ()) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            if cur.description is None:
                return []
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]

    @staticmethod
    def _one(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
        return dict(rows[0]) if rows else None

    # -- builds ------------------------------------------------------------

    def create_build(self, record: BuildRecord) -> BuildRecord:
        with self._Tx(self) as cur:
            cur.execute(
                "SELECT build_id FROM analytics_builds WHERE build_id = %s FOR UPDATE",
                (record.build_id,),
            )
            if cur.fetchone() is not None:
                raise IntegrityError(
                    Reason.BUILD_ID_REUSED,
                    f"build_id {record.build_id} already exists",
                    build_id=record.build_id,
                )
            now = self._now()
            cur.execute(
                """
                INSERT INTO analytics_builds (
                    build_id, tenant_id, source_id, epoch_id, report_family, cut_id,
                    policy_version, recon_revision, source_manifest_sha256, state,
                    lease_token, lease_version, lease_until, evidence_sha256,
                    source_coverage, attempt_no, failure_reason, created_at, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                    record.lease_until,
                    record.evidence_sha256,
                    canonical_json(dict(record.source_coverage)),
                    record.attempt_no,
                    record.failure_reason,
                    now,
                    now,
                ),
            )
        return self.get_build(record.build_id)  # type: ignore[return-value]

    def get_build(self, build_id: str) -> BuildRecord | None:
        row = self._one(
            self._query("SELECT * FROM analytics_builds WHERE build_id = %s", (build_id,))
        )
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
        clauses = ["tenant_id = %s"]
        params: list[Any] = [tenant_id]
        for column, value in (
            ("source_id", source_id),
            ("epoch_id", epoch_id),
            ("report_family", report_family),
        ):
            if value is not None:
                clauses.append(f"{column} = %s")
                params.append(value)
        if states:
            clauses.append("state = ANY(%s)")
            params.append([str(state) for state in states])
        rows = self._query(
            "SELECT * FROM analytics_builds WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at, attempt_no",
            params,
        )
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
        row = self._one(
            self._query(
                """
                SELECT COALESCE(MAX(attempt_no), 0) AS latest FROM analytics_builds
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s
                """,
                (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version),
            )
        )
        return int(row["latest"]) + 1 if row else 1

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
        with self._Tx(self) as cur:
            cur.execute(
                "SELECT state, lease_token FROM analytics_builds WHERE build_id=%s FOR UPDATE",
                (build_id,),
            )
            row = cur.fetchone()
            if row is None:
                raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown build", build_id=build_id)
            state, held_token = row[0], row[1]
            if state != str(expected_state):
                raise AnalyticsError(
                    Reason.BUILD_STATE_INVALID,
                    f"build is {state}, expected {expected_state}",
                    build_id=build_id,
                    actual=state,
                )
            if held_token is not None and held_token != lease_token:
                raise FencedError(
                    Reason.LEASE_HELD_BY_OTHER,
                    "build is owned by a different lease token",
                    build_id=build_id,
                )
            cur.execute(
                """
                UPDATE analytics_builds
                SET state=%s, updated_at=%s,
                    evidence_sha256=COALESCE(%s, evidence_sha256),
                    source_coverage=COALESCE(%s::jsonb, source_coverage),
                    failure_reason=COALESCE(%s, failure_reason)
                WHERE build_id=%s AND state=%s
                """,
                (
                    str(new_state),
                    self._now(),
                    evidence_sha256,
                    canonical_json(dict(source_coverage)) if source_coverage is not None else None,
                    failure_reason,
                    build_id,
                    str(expected_state),
                ),
            )
            if cur.rowcount != 1:  # pragma: no cover - FOR UPDATE serialises this
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
        del now  # the database clock decides expiry (section 5.3)
        key = (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
        current = self._now()
        until = current + timedelta(seconds=lease_seconds)
        with self._Tx(self) as cur:
            cur.execute(
                """
                SELECT lease_token, lease_version, lease_until, active_build_id
                FROM analytics_build_claims
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s
                FOR UPDATE
                """,
                key,
            )
            row = cur.fetchone()
            if row is None:
                cur.execute(
                    """
                    INSERT INTO analytics_build_claims (
                        tenant_id, source_id, epoch_id, report_family, cut_id, policy_version,
                        active_build_id, lease_token, lease_version, lease_until
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,1,%s)
                    """,
                    (*key, build_id, lease_token, until),
                )
            else:
                held_token, lease_version, held_until, _active = row
                live = held_token is not None and held_until is not None and held_until > current
                if live and held_token != lease_token:
                    raise FencedError(
                        Reason.LEASE_HELD_BY_OTHER,
                        "another worker holds a live lease for this cut",
                        lease_until=str(held_until),
                    )
                cur.execute(
                    """
                    UPDATE analytics_build_claims
                    SET active_build_id=%s, lease_token=%s, lease_version=lease_version+1,
                        lease_until=%s
                    WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                      AND cut_id=%s AND policy_version=%s AND lease_version=%s
                    """,
                    (build_id, lease_token, until, *key, lease_version),
                )
                if cur.rowcount != 1:  # pragma: no cover - FOR UPDATE serialises
                    raise FencedError(Reason.LEASE_HELD_BY_OTHER, "claim changed concurrently")
            cur.execute(
                "UPDATE analytics_builds SET lease_token=%s, lease_until=%s, "
                "lease_version=lease_version+1, updated_at=%s WHERE build_id=%s",
                (lease_token, until, current, build_id),
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
        with self._Tx(self) as cur:
            cur.execute(
                """
                SELECT lease_until, active_build_id FROM analytics_build_claims
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s AND lease_token=%s
                FOR UPDATE
                """,
                (*claim_key, lease_token),
            )
            row = cur.fetchone()
            if row is None:
                raise FencedError(Reason.LEASE_LOST, "lease is no longer held by this token")
            held_until, active_build_id = row
            if held_until is None or held_until <= current:
                raise FencedError(Reason.LEASE_LOST, "lease already expired; re-acquire instead")
            cur.execute(
                """
                UPDATE analytics_build_claims
                SET lease_until=%s, lease_version=lease_version+1
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s AND lease_token=%s
                """,
                (until, *claim_key, lease_token),
            )
            if active_build_id:
                cur.execute(
                    "UPDATE analytics_builds SET lease_until=%s WHERE build_id=%s",
                    (until, active_build_id),
                )
        return self.get_claim(claim_key=claim_key)  # type: ignore[return-value]

    def release_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str], lease_token: str
    ) -> None:
        """Drop the claim *and* clear the lease on the build it pointed at.

        Mirrors :meth:`SqliteControlPlane.release_claim`: clearing only the
        claim row leaves the build advertising a lease nobody holds, which
        misreports the GC protection reason for a finished build.
        """
        with self._Tx(self) as cur:
            cur.execute(
                """
                UPDATE analytics_build_claims
                SET lease_token=NULL, lease_until=NULL, lease_version=lease_version+1
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s AND lease_token=%s
                RETURNING active_build_id
                """,
                (*claim_key, lease_token),
            )
            row = cur.fetchone()
            if row is not None and row[0]:
                cur.execute(
                    "UPDATE analytics_builds SET lease_token=NULL, lease_until=NULL, "
                    "lease_version=lease_version+1, updated_at=%s "
                    "WHERE build_id=%s AND lease_token=%s",
                    (self._now(), row[0], lease_token),
                )

    def get_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str]
    ) -> ClaimRecord | None:
        row = self._one(
            self._query(
                """
                SELECT * FROM analytics_build_claims
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s
                """,
                claim_key,
            )
        )
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
            lease_until=_utc(row["lease_until"]),
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
            raise FencedError(Reason.LEASE_LOST, "lease expired")

    # -- publication -------------------------------------------------------

    def get_publication(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> PublicationRecord | None:
        row = self._one(
            self._query(
                """
                SELECT * FROM analytics_publications
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                """,
                (tenant_id, source_id, epoch_id, report_family),
            )
        )
        if row is None:
            return None
        return PublicationRecord(
            tenant_id=row["tenant_id"],
            source_id=row["source_id"],
            epoch_id=row["epoch_id"],
            report_family=row["report_family"],
            current_build_id=row["current_build_id"],
            publication_revision=row["publication_revision"],
            published_at=_utc(row["published_at"]),  # type: ignore[arg-type]
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
        """Flip the pointer under SERIALIZABLE. Section 6.5.

        Every precondition is re-read inside the transaction with
        ``FOR UPDATE``, in the order the design lists them: the build is
        VERIFIED, its evidence exists, the publisher holds the live claim,
        P04's trust still permits it, and the pointer revision is the one the
        caller saw.
        """
        del now
        current = self._now()
        with self._Tx(self, serializable=True) as cur:
            cur.execute(
                "SELECT * FROM analytics_builds WHERE build_id=%s FOR UPDATE", (build_id,)
            )
            build = _row_dict(cur)
            if build is None:
                raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown build", build_id=build_id)
            if build["state"] != str(BuildState.VERIFIED):
                raise CompletenessError(
                    Reason.BUILD_NOT_VERIFIED,
                    f"only a VERIFIED build may be published; this one is {build['state']}",
                    build_id=build_id,
                )
            if not build["evidence_sha256"]:
                raise CompletenessError(
                    Reason.EVIDENCE_MISSING,
                    "a build without durable evidence may not be published",
                    build_id=build_id,
                )
            cur.execute(
                "SELECT count(*) FROM analytics_build_evidence WHERE build_id=%s", (build_id,)
            )
            if not int(cur.fetchone()[0]):
                raise CompletenessError(
                    Reason.EVIDENCE_MISSING, "no evidence records stored", build_id=build_id
                )

            scope = (
                build["tenant_id"],
                build["source_id"],
                build["epoch_id"],
                build["report_family"],
            )
            claim_key = (*scope, build["cut_id"], build["policy_version"])
            cur.execute(
                """
                SELECT lease_token, lease_until, active_build_id FROM analytics_build_claims
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s
                FOR UPDATE
                """,
                claim_key,
            )
            claim = cur.fetchone()
            if claim is None or claim[0] != lease_token:
                raise FencedError(
                    Reason.LEASE_LOST, "publisher does not hold the active claim", build_id=build_id
                )
            if claim[1] is None or _utc(claim[1]) <= current:
                raise FencedError(Reason.LEASE_LOST, "lease expired before publication")
            if claim[2] != build_id:
                raise FencedError(
                    Reason.LEASE_LOST, "claim points at a different active build"
                )

            cur.execute(
                """
                SELECT revision, verified, revoked FROM analytics_trust
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s
                """,
                claim_key,
            )
            trust = cur.fetchone()
            if trust is None:
                raise CompletenessError(
                    Reason.RECONCILIATION_NOT_VERIFIED,
                    "no reconciliation trust record for this cut",
                    build_id=build_id,
                )
            revision_text, verified, revoked = trust
            if revoked or not verified:
                raise CompletenessError(
                    Reason.RECONCILIATION_REVOKED if revoked
                    else Reason.RECONCILIATION_NOT_VERIFIED,
                    "reconciliation trust does not permit publication",
                    revision=revision_text,
                )
            if revision_text != build["recon_revision"]:
                raise CompletenessError(
                    Reason.RECONCILIATION_REVOKED,
                    "reconciliation revision moved after the build pinned it",
                    pinned=build["recon_revision"],
                    current=revision_text,
                )

            cur.execute(
                """
                SELECT current_build_id, publication_revision FROM analytics_publications
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                FOR UPDATE
                """,
                scope,
            )
            pointer = cur.fetchone()
            previous_build_id = pointer[0] if pointer else None
            if pointer is None:
                if expected_revision is not None:
                    raise AnalyticsError(
                        Reason.PUBLICATION_REVISION_STALE,
                        "no publication exists yet; expected_revision must be None",
                    )
                revision = 1
                cur.execute(
                    """
                    INSERT INTO analytics_publications (
                        tenant_id, source_id, epoch_id, report_family,
                        current_build_id, publication_revision, published_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (*scope, build_id, revision, current),
                )
            else:
                if expected_revision != pointer[1]:
                    raise FencedError(
                        Reason.PUBLICATION_REVISION_STALE,
                        "publication revision changed since it was read",
                        expected=expected_revision,
                        actual=pointer[1],
                    )
                revision = pointer[1] + 1
                cur.execute(
                    """
                    UPDATE analytics_publications
                    SET current_build_id=%s, publication_revision=%s, published_at=%s
                    WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                      AND publication_revision=%s
                    """,
                    (build_id, revision, current, *scope, pointer[1]),
                )
                if previous_build_id and previous_build_id != build_id:
                    cur.execute(
                        "UPDATE analytics_builds SET state=%s, updated_at=%s "
                        "WHERE build_id=%s AND state=%s",
                        (
                            str(BuildState.SUPERSEDED),
                            current,
                            previous_build_id,
                            str(BuildState.PUBLISHED),
                        ),
                    )

            cur.execute(
                "UPDATE analytics_builds SET state=%s, updated_at=%s "
                "WHERE build_id=%s AND state=%s",
                (str(BuildState.PUBLISHED), current, build_id, str(BuildState.VERIFIED)),
            )
            cur.execute(
                """
                INSERT INTO analytics_publication_history (
                    tenant_id, source_id, epoch_id, report_family, build_id,
                    previous_build_id, publication_revision, action, actor, note, recorded_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'PUBLISH',%s,%s,%s)
                """,
                (*scope, build_id, previous_build_id, revision, actor, note, current),
            )
        return self.get_publication(
            tenant_id=scope[0], source_id=scope[1], epoch_id=scope[2], report_family=scope[3]
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
        del now
        current = self._now()
        scope = (tenant_id, source_id, epoch_id, report_family)
        with self._Tx(self, serializable=True) as cur:
            cur.execute(
                """
                SELECT current_build_id, publication_revision FROM analytics_publications
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                FOR UPDATE
                """,
                scope,
            )
            pointer = cur.fetchone()
            if pointer is None:
                raise AnalyticsError(Reason.NO_PUBLICATION, "nothing to roll back")
            if pointer[1] != expected_revision:
                raise FencedError(
                    Reason.PUBLICATION_REVISION_STALE,
                    "rollback saw a stale revision",
                    expected=expected_revision,
                    actual=pointer[1],
                )
            cur.execute(
                "SELECT state, tenant_id, source_id, epoch_id, report_family "
                "FROM analytics_builds WHERE build_id=%s FOR UPDATE",
                (target_build_id,),
            )
            target = cur.fetchone()
            if target is None:
                raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown rollback target")
            if target[0] not in (
                str(BuildState.SUPERSEDED),
                str(BuildState.PUBLISHED),
                str(BuildState.VERIFIED),
            ):
                raise CompletenessError(
                    Reason.BUILD_NOT_VERIFIED, "rollback target was never verified"
                )
            if tuple(target[1:5]) != scope:
                raise AnalyticsError(
                    Reason.FORBIDDEN_SCOPE,
                    "rollback target belongs to a different scope",
                    build_id=target_build_id,
                )
            revision = expected_revision + 1
            cur.execute(
                """
                UPDATE analytics_publications
                SET current_build_id=%s, publication_revision=%s, published_at=%s
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND publication_revision=%s
                """,
                (target_build_id, revision, current, *scope, expected_revision),
            )
            cur.execute(
                "UPDATE analytics_builds SET state=%s, updated_at=%s WHERE build_id=%s AND state=%s",
                (str(BuildState.SUPERSEDED), current, pointer[0], str(BuildState.PUBLISHED)),
            )
            cur.execute(
                "UPDATE analytics_builds SET state=%s, updated_at=%s WHERE build_id=%s",
                (str(BuildState.PUBLISHED), current, target_build_id),
            )
            cur.execute(
                """
                INSERT INTO analytics_publication_history (
                    tenant_id, source_id, epoch_id, report_family, build_id,
                    previous_build_id, publication_revision, action, actor, note, recorded_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'ROLLBACK',%s,%s,%s)
                """,
                (*scope, target_build_id, pointer[0], revision, actor, note, current),
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
        rows = self._query(
            """
            SELECT * FROM analytics_publication_history
            WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
            ORDER BY history_id
            """,
            (tenant_id, source_id, epoch_id, report_family),
        )
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
                recorded_at=_utc(row["recorded_at"]),  # type: ignore[arg-type]
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
        with self._Tx(self) as cur:
            cur.execute(
                """
                INSERT INTO analytics_build_evidence (
                    build_id, evidence_kind, evidence_sha256, evidence_json, recorded_at
                ) VALUES (%s,%s,%s,%s::jsonb,%s)
                ON CONFLICT (build_id, evidence_kind) DO UPDATE SET
                    evidence_sha256=EXCLUDED.evidence_sha256,
                    evidence_json=EXCLUDED.evidence_json,
                    recorded_at=EXCLUDED.recorded_at
                """,
                (build_id, evidence_kind, sha, canonical_json(payload), current),
            )
        return EvidenceRecord(
            build_id=build_id,
            evidence_kind=evidence_kind,
            evidence_sha256=sha,
            evidence_json=payload,
            recorded_at=current,
        )

    def get_evidence(self, *, build_id: str) -> list[EvidenceRecord]:
        rows = self._query(
            "SELECT * FROM analytics_build_evidence WHERE build_id=%s ORDER BY evidence_kind",
            (build_id,),
        )
        return [
            EvidenceRecord(
                build_id=row["build_id"],
                evidence_kind=row["evidence_kind"],
                evidence_sha256=row["evidence_sha256"],
                evidence_json=_as_json(row["evidence_json"]),
                recorded_at=_utc(row["recorded_at"]),  # type: ignore[arg-type]
            )
            for row in rows
        ]

    # -- quarantine --------------------------------------------------------

    def record_quarantine(self, record: QuarantineRecord) -> QuarantineRecord:
        with self._Tx(self) as cur:
            cur.execute(
                """
                INSERT INTO analytics_quarantine (
                    receipt_id, tenant_id, source_id, epoch_id, topic, partition, offset_value,
                    event_id, reason, detail, payload_sha256, recorded_at, resolved, resolution_note
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s,%s,%s)
                ON CONFLICT (topic, partition, offset_value) DO NOTHING
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
                    record.recorded_at,
                    record.resolved,
                    record.resolution_note,
                ),
            )
            cur.execute(
                "SELECT * FROM analytics_quarantine "
                "WHERE topic=%s AND partition=%s AND offset_value=%s",
                (record.topic, record.partition, record.offset),
            )
            stored = _row_dict(cur)
        return _quarantine_from_row(stored) if stored else record

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
                clauses.append(f"{column} = %s")
                params.append(value)
        if unresolved_only:
            clauses.append("resolved = FALSE")
        sql = "SELECT * FROM analytics_quarantine"
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY topic, partition, offset_value"
        return [_quarantine_from_row(row) for row in self._query(sql, params)]

    def resolve_quarantine(self, *, receipt_id: str, note: str) -> QuarantineRecord:
        with self._Tx(self) as cur:
            cur.execute(
                "UPDATE analytics_quarantine SET resolved=TRUE, resolution_note=%s "
                "WHERE receipt_id=%s RETURNING *",
                (note, receipt_id),
            )
            row = _row_dict(cur)
        if row is None:
            raise AnalyticsError(Reason.FIELD_INVALID, "unknown quarantine receipt")
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
        with self._Tx(self) as cur:
            cur.execute(
                """
                INSERT INTO analytics_trust (
                    tenant_id, source_id, epoch_id, report_family, cut_id, policy_version,
                    revision, verified, revoked, findings, updated_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)
                ON CONFLICT (tenant_id, source_id, epoch_id, report_family, cut_id, policy_version)
                DO UPDATE SET revision=EXCLUDED.revision, verified=EXCLUDED.verified,
                              revoked=EXCLUDED.revoked, findings=EXCLUDED.findings,
                              updated_at=EXCLUDED.updated_at
                """,
                (
                    *cut_key,
                    revision,
                    verified,
                    revoked,
                    canonical_json(list(findings)),
                    self._now(),
                ),
            )

    def get_trust(
        self, *, cut_key: tuple[str, str, str, str, str, str]
    ) -> dict[str, Any] | None:
        row = self._one(
            self._query(
                """
                SELECT * FROM analytics_trust
                WHERE tenant_id=%s AND source_id=%s AND epoch_id=%s AND report_family=%s
                  AND cut_id=%s AND policy_version=%s
                """,
                cut_key,
            )
        )
        if row is None:
            return None
        return {
            "revision": row["revision"],
            "verified": bool(row["verified"]),
            "revoked": bool(row["revoked"]),
            "findings": _as_json(row["findings"]),
            "updated_at": str(row["updated_at"]),
        }

    # -- reader pins -------------------------------------------------------

    def register_reader_pin(
        self, *, build_id: str, pin_id: str, now: datetime | None = None
    ) -> None:
        del now
        with self._Tx(self) as cur:
            cur.execute(
                "INSERT INTO analytics_reader_pins (pin_id, build_id, created_at) "
                "VALUES (%s,%s,%s) ON CONFLICT (pin_id) DO UPDATE SET build_id=EXCLUDED.build_id",
                (pin_id, build_id, self._now()),
            )

    def release_reader_pin(self, *, pin_id: str) -> None:
        with self._Tx(self) as cur:
            cur.execute("DELETE FROM analytics_reader_pins WHERE pin_id=%s", (pin_id,))

    def active_reader_pins(self, *, build_id: str) -> int:
        row = self._one(
            self._query(
                "SELECT count(*) AS n FROM analytics_reader_pins WHERE build_id=%s", (build_id,)
            )
        )
        return int(row["n"]) if row else 0


def _row_dict(cursor) -> dict[str, Any] | None:
    row = cursor.fetchone()
    if row is None:
        return None
    columns = [desc[0] for desc in cursor.description]
    return dict(zip(columns, row))


def _utc(value: Any) -> datetime | None:
    """Normalise a driver timestamp to aware UTC.

    The columns are ``TIMESTAMPTZ``, so a naive value here means the driver
    was configured without timezone support. UTC is supplied rather than
    raising, because the column's declared type already settles the question.
    """
    if value is None:
        return None
    if not isinstance(value, datetime):
        value = datetime.fromisoformat(str(value))
    return value.replace(tzinfo=UTC) if value.tzinfo is None else ensure_utc(value)


def _as_json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    return json.loads(value) if value else {}


def _build_from_row(row: Mapping[str, Any]) -> BuildRecord:
    return BuildRecord(
        build_id=str(row["build_id"]),
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=str(row["epoch_id"]),
        report_family=row["report_family"],
        cut_id=row["cut_id"],
        policy_version=row["policy_version"],
        recon_revision=row["recon_revision"],
        source_manifest_sha256=row["source_manifest_sha256"],
        state=BuildState(row["state"]),
        attempt_no=row["attempt_no"],
        lease_token=str(row["lease_token"]) if row["lease_token"] else None,
        lease_version=row["lease_version"],
        lease_until=_utc(row["lease_until"]),
        evidence_sha256=row["evidence_sha256"],
        source_coverage=_as_json(row["source_coverage"]),
        created_at=_utc(row["created_at"]),
        updated_at=_utc(row["updated_at"]),
        failure_reason=row["failure_reason"],
    )


def _quarantine_from_row(row: Mapping[str, Any]) -> QuarantineRecord:
    return QuarantineRecord(
        receipt_id=row["receipt_id"],
        tenant_id=row["tenant_id"],
        source_id=row["source_id"],
        epoch_id=str(row["epoch_id"]) if row["epoch_id"] else None,
        topic=row["topic"],
        partition=row["partition"],
        offset=row["offset_value"],
        event_id=str(row["event_id"]) if row["event_id"] else None,
        reason=row["reason"],
        detail=_as_json(row["detail"]),
        payload_sha256=row["payload_sha256"],
        recorded_at=_utc(row["recorded_at"]),  # type: ignore[arg-type]
        resolved=bool(row["resolved"]),
        resolution_note=row["resolution_note"],
    )
