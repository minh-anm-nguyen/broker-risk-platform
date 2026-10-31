"""Garbage collection for abandoned builds, with hard protections.

GC is the one maintenance task that can destroy a published report, so it is
written as *plan then execute*: :meth:`GarbageCollector.plan` decides and
explains, and nothing is deleted without a plan. The protections, each from a
specific scenario:

* The currently published build is never collectable (INV08-19, G08-19).
* A build with an open reader pin is never collectable, because a cursor
  mid-pagination is still reading it (P08-T-08).
* A build acquired by a live lease is never collectable, even if it looks
  abandoned - that is the GC-versus-new-build race of KP08-28.
* ``QUARANTINED`` builds are retained for forensics regardless of age: they
  are the evidence of an integrity incident.

A build that *was* published and has since been superseded is retained too,
until it ages out, because rollback needs it (RB08-04).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Mapping, Sequence

from ..clock import Clock
from ..errors import AnalyticsError, BuildState, Reason
from ..faults.killpoints import FaultPlan, KillPoint, NullFaultPlan
from ..ports.clickhouse import ClickHousePort
from ..ports.control import BuildRecord, ControlPlanePort


@dataclass(frozen=True, slots=True)
class GcPlan:
    """What GC intends to do, and why it is leaving the rest alone."""

    collectable: tuple[str, ...]
    protected: Mapping[str, str] = field(default_factory=dict)

    def as_wire(self) -> dict[str, object]:
        return {"collectable": list(self.collectable), "protected": dict(self.protected)}


class GarbageCollector:
    def __init__(
        self,
        *,
        control: ControlPlanePort,
        clickhouse: ClickHousePort,
        clock: Clock,
        retain_superseded_seconds: int = 7 * 24 * 3600,
        faults: FaultPlan | None = None,
    ) -> None:
        self._control = control
        self._ch = clickhouse
        self._clock = clock
        self._retain_superseded = retain_superseded_seconds
        self._faults = faults or NullFaultPlan()

    def plan(
        self, *, tenant_id: str, source_id: str | None = None, epoch_id: str | None = None
    ) -> GcPlan:
        builds = self._control.list_builds(
            tenant_id=tenant_id, source_id=source_id, epoch_id=epoch_id
        )
        collectable: list[str] = []
        protected: dict[str, str] = {}
        now = self._clock.now()

        for build in builds:
            reason = self._protection_reason(build, now)
            if reason is None:
                collectable.append(build.build_id)
            else:
                protected[build.build_id] = reason
        return GcPlan(collectable=tuple(sorted(collectable)), protected=protected)

    def _protection_reason(self, build: BuildRecord, now) -> str | None:
        pointer = self._control.get_publication(
            tenant_id=build.tenant_id,
            source_id=build.source_id,
            epoch_id=build.epoch_id,
            report_family=build.report_family,
        )
        if pointer is not None and pointer.current_build_id == build.build_id:
            return "CURRENT_PUBLICATION"
        if build.state is BuildState.PUBLISHED:
            # State says published even though the pointer moved on: treat the
            # disagreement as a reason to keep the rows, not to delete them.
            return "MARKED_PUBLISHED"
        if build.state is BuildState.QUARANTINED:
            return "QUARANTINE_FORENSICS"
        if self._control.active_reader_pins(build_id=build.build_id) > 0:
            return "ACTIVE_READER_PIN"
        if build.lease_token and build.lease_until and build.lease_until > now:
            return "LIVE_LEASE"
        if build.state is BuildState.SUPERSEDED:
            updated = build.updated_at or now
            if (now - updated) < timedelta(seconds=self._retain_superseded):
                return "SUPERSEDED_RETENTION_WINDOW"
        if build.state in (BuildState.REQUESTED, BuildState.BUILDING, BuildState.VALIDATING):
            if build.lease_until and build.lease_until > now:
                return "LIVE_LEASE"
        if build.state is BuildState.VERIFIED:
            # Verified but unpublished is a rollback candidate; it is only
            # collected once it is superseded or explicitly failed.
            return "VERIFIED_AWAITING_PUBLICATION"
        return None

    def collect(
        self,
        *,
        tenant_id: str,
        source_id: str | None = None,
        epoch_id: str | None = None,
        plan: GcPlan | None = None,
        actor: str = "gc",
    ) -> dict[str, int]:
        """Delete the rows of collectable builds. Re-checks every protection.

        The re-check is not redundant: KP08-28 injects a new build acquiring a
        lease between the plan and the delete, and a GC that trusted a stale
        plan would remove rows from under it.
        """
        if not actor:
            raise AnalyticsError(Reason.UNAUTHENTICATED, "gc requires an actor")
        chosen = plan or self.plan(tenant_id=tenant_id, source_id=source_id, epoch_id=epoch_id)
        deleted: dict[str, int] = {}
        now = self._clock.now()
        for build_id in chosen.collectable:
            self._faults.fire(KillPoint.GC_DELETING_WHILE_BUILD_ACQUIRED, build_id=build_id)
            build = self._control.get_build(build_id)
            if build is None:  # pragma: no cover - concurrent delete
                continue
            reason = self._protection_reason(build, now)
            if reason is not None:
                # Became protected after planning. Skipping is the whole point
                # of re-checking (G08-19 expects GC_PROTECTED, not a delete).
                continue
            deleted[build_id] = self._ch.delete_build(build_id=build_id)
        return deleted

    def assert_collectable(self, build_id: str) -> None:
        """Raise ``GC_PROTECTED`` if this build may not be deleted.

        Used by the operator CLI so a manual delete gets the same answer as
        the automated sweep.
        """
        build = self._control.get_build(build_id)
        if build is None:
            raise AnalyticsError(Reason.BUILD_NOT_FOUND, "unknown build", build_id=build_id)
        reason = self._protection_reason(build, self._clock.now())
        if reason is not None:
            raise AnalyticsError(
                Reason.GC_PROTECTED,
                f"build is protected from collection: {reason}",
                build_id=build_id,
                protection=reason,
            )
