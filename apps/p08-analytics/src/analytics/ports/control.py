"""The PostgreSQL control-plane port: builds, claims, pointers, evidence.

This is the only component in P08 with transactional authority. ClickHouse
holds the rows; the control plane decides which rows a reader is allowed to
believe. Section 3.3 is blunt that the pointer flip is atomic *within
PostgreSQL* and is not a distributed transaction, so the port exposes
compare-and-set operations with explicit expected values rather than a
``save()`` that would paper over the race.

Three guards appear in nearly every signature:

* ``lease_token`` - proves the caller still owns the build (INV08-11).
* ``expected_revision`` - proves the caller saw the current pointer before
  trying to replace it (P08-T-04).
* ``expected_state`` - proves the transition is one the state machine allows.

A method without these would be a method that lets a stale worker publish.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable

from ..errors import BuildState


@dataclass(frozen=True, slots=True)
class BuildRecord:
    """One immutable build attempt. Mirrors ``analytics_control.analytics_builds``."""

    build_id: str
    tenant_id: str
    source_id: str
    epoch_id: str
    report_family: str
    cut_id: str
    policy_version: str
    recon_revision: str
    source_manifest_sha256: str
    state: BuildState
    attempt_no: int
    lease_token: str | None = None
    lease_version: int = 0
    lease_until: datetime | None = None
    evidence_sha256: str | None = None
    source_coverage: Mapping[str, Any] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None
    failure_reason: str | None = None

    @property
    def claim_key(self) -> tuple[str, str, str, str, str, str]:
        return (
            self.tenant_id,
            self.source_id,
            self.epoch_id,
            self.report_family,
            self.cut_id,
            self.policy_version,
        )


@dataclass(frozen=True, slots=True)
class ClaimRecord:
    """The single active owner for one logical build. Section 5.3."""

    tenant_id: str
    source_id: str
    epoch_id: str
    report_family: str
    cut_id: str
    policy_version: str
    active_build_id: str | None
    lease_token: str | None
    lease_version: int
    lease_until: datetime | None


@dataclass(frozen=True, slots=True)
class PublicationRecord:
    """The pointer a reader resolves before every query."""

    tenant_id: str
    source_id: str
    epoch_id: str
    report_family: str
    current_build_id: str
    publication_revision: int
    published_at: datetime


@dataclass(frozen=True, slots=True)
class PublicationHistoryEntry:
    """Append-only audit of pointer movement, including rollbacks (INV08-18)."""

    tenant_id: str
    source_id: str
    epoch_id: str
    report_family: str
    build_id: str
    publication_revision: int
    action: str
    actor: str
    recorded_at: datetime
    previous_build_id: str | None = None
    note: str | None = None


@dataclass(frozen=True, slots=True)
class QuarantineRecord:
    """A durable poison receipt. Section 6.1 step 3.

    Written *before* the stream may advance past the record. The receipt is
    the difference between "we isolated a bad event" and "we dropped one"
    (INV08-21).
    """

    receipt_id: str
    tenant_id: str | None
    source_id: str | None
    epoch_id: str | None
    topic: str
    partition: int
    offset: int
    event_id: str | None
    reason: str
    detail: Mapping[str, Any]
    payload_sha256: str | None
    recorded_at: datetime
    resolved: bool = False
    resolution_note: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceRecord:
    build_id: str
    evidence_kind: str
    evidence_sha256: str
    evidence_json: Mapping[str, Any]
    recorded_at: datetime


@runtime_checkable
class ControlPlanePort(Protocol):
    """Transactional authority over build state and publication."""

    # -- builds ------------------------------------------------------------

    def create_build(self, record: BuildRecord) -> BuildRecord:
        """Insert a new attempt. Reusing a ``build_id`` is an error (P08-S-10)."""

    def get_build(self, build_id: str) -> BuildRecord | None: ...

    def list_builds(
        self,
        *,
        tenant_id: str,
        source_id: str | None = None,
        epoch_id: str | None = None,
        report_family: str | None = None,
        states: Sequence[BuildState] | None = None,
    ) -> Sequence[BuildRecord]: ...

    def next_attempt_no(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        report_family: str,
        cut_id: str,
        policy_version: str,
    ) -> int: ...

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
        """CAS on ``(state, lease_token)``. Raises on a stale owner or an
        illegal transition; never silently no-ops."""

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
        now: datetime,
    ) -> ClaimRecord:
        """Take ownership, or raise if a live lease is held elsewhere.

        Expiry is evaluated against the control plane's own clock, not the
        caller's, so a worker with a skewed clock cannot talk itself into
        believing a lease has lapsed (P08-S-08).
        """

    def renew_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str], lease_token: str,
        lease_seconds: int, now: datetime,
    ) -> ClaimRecord: ...

    def release_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str], lease_token: str
    ) -> None: ...

    def get_claim(
        self, *, claim_key: tuple[str, str, str, str, str, str]
    ) -> ClaimRecord | None: ...

    def assert_lease_held(
        self, *, claim_key: tuple[str, str, str, str, str, str], lease_token: str, now: datetime
    ) -> None:
        """Raise :class:`FencedError` unless the lease is held and unexpired."""

    # -- publication -------------------------------------------------------

    def get_publication(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> PublicationRecord | None: ...

    def publish_pointer(
        self,
        *,
        build_id: str,
        lease_token: str,
        expected_revision: int | None,
        actor: str,
        now: datetime,
        note: str | None = None,
    ) -> PublicationRecord:
        """Flip the pointer in one transaction. Section 6.5.

        ``expected_revision=None`` means "there must be no pointer yet", which
        is how a first publication stays safe against two concurrent builders.
        """

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
        now: datetime,
        note: str | None = None,
    ) -> PublicationRecord:
        """Return the pointer to an earlier verified build without rewriting
        history: the revision still moves forward (INV08-18)."""

    def publication_history(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> Sequence[PublicationHistoryEntry]: ...

    # -- evidence ----------------------------------------------------------

    def put_evidence(
        self, *, build_id: str, evidence_kind: str, evidence_json: Mapping[str, Any], now: datetime
    ) -> EvidenceRecord: ...

    def get_evidence(self, *, build_id: str) -> Sequence[EvidenceRecord]: ...

    # -- quarantine --------------------------------------------------------

    def record_quarantine(self, record: QuarantineRecord) -> QuarantineRecord:
        """Idempotent on ``(topic, partition, offset)``: a replayed poison
        record must not multiply receipts (P08-F-09)."""

    def list_quarantine(
        self,
        *,
        tenant_id: str | None = None,
        source_id: str | None = None,
        epoch_id: str | None = None,
        unresolved_only: bool = True,
    ) -> Sequence[QuarantineRecord]: ...

    def resolve_quarantine(self, *, receipt_id: str, note: str) -> QuarantineRecord: ...

    # -- reconciliation trust snapshot ------------------------------------

    def put_trust(
        self, *, cut_key: tuple[str, str, str, str, str, str], revision: str, verified: bool,
        revoked: bool, findings: Sequence[str], now: datetime,
    ) -> None: ...

    def get_trust(
        self, *, cut_key: tuple[str, str, str, str, str, str]
    ) -> Mapping[str, Any] | None: ...

    # -- reader bookkeeping ------------------------------------------------

    def register_reader_pin(self, *, build_id: str, pin_id: str, now: datetime) -> None:
        """Note that a cursor is reading a build, so GC cannot delete it
        underneath the reader (INV08-19, P08-T-08)."""

    def release_reader_pin(self, *, pin_id: str) -> None: ...

    def active_reader_pins(self, *, build_id: str) -> int: ...
