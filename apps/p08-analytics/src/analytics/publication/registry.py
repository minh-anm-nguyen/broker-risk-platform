"""Resolving the publication pointer, and deciding freshness honestly.

The pointer is read once per request and the returned ``build_id`` is bound
for the whole response. That is what prevents the torn read of P08-T-02: a
builder flipping the pointer mid-query must not change which build the rows
in flight came from.

Freshness is computed here rather than by the caller, because the interesting
part is the *downgrade*: a verified publication older than the target is
``STALE``, not ``READY`` with a stale timestamp attached. Section 8.1 again -
the status has to carry the bad news, since nobody reads the timestamp.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from ..clock import Clock
from ..errors import (
    AnalyticsError,
    BuildState,
    CompletenessError,
    QueryError,
    Reason,
    ReportStatus,
)
from ..ports.control import BuildRecord, ControlPlanePort, PublicationHistoryEntry


@dataclass(frozen=True, slots=True)
class ResolvedPublication:
    """The pointer, the build behind it, and what status a reader gets."""

    build_id: str
    publication_revision: int
    published_at: datetime
    cut_id: str
    policy_version: str
    recon_revision: str
    epoch_id: str
    status: ReportStatus
    freshness_seconds: int
    evidence_sha256: str | None
    attempt_no: int

    @property
    def is_readable(self) -> bool:
        """``STALE`` data is still real data; ``BLOCKED`` is not."""
        return self.status in (ReportStatus.READY, ReportStatus.STALE)


class PublicationRegistry:
    """Read-side view of the control plane."""

    def __init__(
        self,
        control: ControlPlanePort,
        *,
        clock: Clock,
        freshness_target_seconds: int = 300,
    ) -> None:
        self._control = control
        self._clock = clock
        self._freshness_target = freshness_target_seconds

    def resolve(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> ResolvedPublication:
        """Resolve the current pointer, or refuse with a reason."""
        pointer = self._control.get_publication(
            tenant_id=tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
        )
        if pointer is None:
            raise CompletenessError(
                Reason.NO_PUBLICATION,
                "no verified publication exists for this scope yet",
                source_id=source_id,
                report_family=report_family,
            )
        build = self._control.get_build(pointer.current_build_id)
        if build is None:  # pragma: no cover - FK prevents this
            raise AnalyticsError(
                Reason.BUILD_NOT_FOUND,
                "publication points at a build that no longer exists",
                build_id=pointer.current_build_id,
            )
        if build.state not in (BuildState.PUBLISHED, BuildState.SUPERSEDED):
            # A pointer at a FAILED or QUARANTINED build means somebody wrote
            # the control tables by hand (P08-T-10). Fail closed.
            raise CompletenessError(
                Reason.BUILD_STATE_INVALID,
                "publication points at a build that is not in a published state",
                build_id=build.build_id,
                state=str(build.state),
            )
        if (build.tenant_id, build.source_id, build.epoch_id) != (
            tenant_id,
            source_id,
            epoch_id,
        ):  # pragma: no cover - the pointer key enforces this
            raise CompletenessError(
                Reason.FORBIDDEN_SCOPE,
                "publication build belongs to a different scope",
                build_id=build.build_id,
            )

        age = int((self._clock.now() - pointer.published_at).total_seconds())
        status = ReportStatus.READY if age <= self._freshness_target else ReportStatus.STALE
        if not build.evidence_sha256:  # pragma: no cover - publication gate requires it
            status = ReportStatus.BLOCKED
        return ResolvedPublication(
            build_id=build.build_id,
            publication_revision=pointer.publication_revision,
            published_at=pointer.published_at,
            cut_id=build.cut_id,
            policy_version=build.policy_version,
            recon_revision=build.recon_revision,
            epoch_id=build.epoch_id,
            status=status,
            freshness_seconds=max(0, age),
            evidence_sha256=build.evidence_sha256,
            attempt_no=build.attempt_no,
        )

    def resolve_explicit_build(
        self, *, tenant_id: str, build_id: str
    ) -> ResolvedPublication:
        """Resolve a historical build named explicitly by the caller.

        Permitted by section 8.2 so an auditor can reproduce an old report,
        but only for a build that was genuinely verified, and only inside the
        caller's own tenant - a ``build_id`` is not a capability token.
        """
        build = self._control.get_build(build_id)
        if build is None or build.tenant_id != tenant_id:
            raise CompletenessError(
                Reason.BUILD_NOT_FOUND, "unknown build for this tenant", build_id=build_id
            )
        if build.state not in (
            BuildState.PUBLISHED,
            BuildState.SUPERSEDED,
            BuildState.VERIFIED,
        ):
            raise CompletenessError(
                Reason.BUILD_NOT_VERIFIED,
                "that build was never verified",
                build_id=build_id,
                state=str(build.state),
            )
        published_at = build.updated_at or self._clock.now()
        pointer = self._control.get_publication(
            tenant_id=build.tenant_id,
            source_id=build.source_id,
            epoch_id=build.epoch_id,
            report_family=build.report_family,
        )
        is_current = pointer is not None and pointer.current_build_id == build_id
        age = int((self._clock.now() - published_at).total_seconds())
        return ResolvedPublication(
            build_id=build.build_id,
            publication_revision=pointer.publication_revision if pointer else 0,
            published_at=published_at,
            cut_id=build.cut_id,
            policy_version=build.policy_version,
            recon_revision=build.recon_revision,
            epoch_id=build.epoch_id,
            # An explicitly requested superseded build is historical by
            # definition, so it is never advertised as READY.
            status=ReportStatus.READY if is_current and age <= self._freshness_target
            else ReportStatus.STALE,
            freshness_seconds=max(0, age),
            evidence_sha256=build.evidence_sha256,
            attempt_no=build.attempt_no,
        )

    def rollback(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        report_family: str,
        target_build_id: str,
        actor: str,
        note: str,
    ) -> ResolvedPublication:
        """Point back at an earlier verified build. INV08-18."""
        if not note.strip():
            raise QueryError(Reason.FIELD_INVALID, "a rollback requires an operator note")
        pointer = self._control.get_publication(
            tenant_id=tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
        )
        if pointer is None:
            raise CompletenessError(Reason.NO_PUBLICATION, "nothing to roll back")
        self._control.rollback_pointer(
            tenant_id=tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
            target_build_id=target_build_id,
            expected_revision=pointer.publication_revision,
            actor=actor,
            now=self._clock.now(),
            note=note,
        )
        return self.resolve(
            tenant_id=tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
        )

    def history(
        self, *, tenant_id: str, source_id: str, epoch_id: str, report_family: str
    ) -> Sequence[PublicationHistoryEntry]:
        return self._control.publication_history(
            tenant_id=tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
        )

    def evidence(self, *, tenant_id: str, build_id: str) -> list[dict]:
        """Evidence for a build, scoped to the caller's tenant."""
        build = self._control.get_build(build_id)
        if build is None or build.tenant_id != tenant_id:
            raise CompletenessError(
                Reason.BUILD_NOT_FOUND, "unknown build for this tenant", build_id=build_id
            )
        return [
            {
                "evidence_kind": record.evidence_kind,
                "evidence_sha256": record.evidence_sha256,
                "evidence_json": record.evidence_json,
                "recorded_at": record.recorded_at.isoformat(),
            }
            for record in self._control.get_evidence(build_id=build_id)
        ]

    def pin_reader(self, *, build_id: str, pin_id: str) -> None:
        """Mark a build as being read, so GC leaves it alone. P08-T-08."""
        self._control.register_reader_pin(
            build_id=build_id, pin_id=pin_id, now=self._clock.now()
        )

    def unpin_reader(self, *, pin_id: str) -> None:
        self._control.release_reader_pin(pin_id=pin_id)

    def current_build(self, build_record: BuildRecord) -> bool:
        pointer = self._control.get_publication(
            tenant_id=build_record.tenant_id,
            source_id=build_record.source_id,
            epoch_id=build_record.epoch_id,
            report_family=build_record.report_family,
        )
        return pointer is not None and pointer.current_build_id == build_record.build_id
