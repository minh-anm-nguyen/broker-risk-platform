"""Operator replay: dry run first, named approver always, same ``event_id``.

Design section 10.6 sets the shape of this: audited, role-gated, dry-run counts
and manifest before anything moves, and a replay never changes the ``event_id``
for the same fact. The last clause is what makes replay safe at all -- the
downstream dedup that absorbs a crash duplicate is the same mechanism that
absorbs a deliberate replay, and it only works if the identity is preserved.
So there is no API here that re-mints an event.

:func:`plan_replay` is separated from :func:`execute_replay` so the manifest can
be read, counted and approved by a human before any row changes state. The
manifest hash is over the sorted event ids, so approving a plan and then
executing a different set of rows is detectable rather than merely unlikely.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..canonical import payload_hash
from ..errors import IntegrityIncident
from ..observability.logging import get_logger
from ..persistence.ports import EventState

_log = get_logger("ops.replay")


@dataclass(frozen=True, slots=True)
class ReplayPlan:
    """What a replay would touch, as a reviewable manifest."""

    event_ids: tuple[str, ...]
    aggregates: tuple[tuple[str, str, str], ...]
    manifest_sha256: str
    scope: str

    @property
    def count(self) -> int:
        return len(self.event_ids)


@dataclass(slots=True)
class ReplayOutcome:
    requeued: int = 0
    skipped: tuple[str, ...] = ()
    approved_by: str = ""
    executed_at: str = ""
    notes: list[str] = field(default_factory=list)


def plan_replay(
    store: Any,
    *,
    source_id: str | None = None,
    epoch_id: str | None = None,
    aggregate_key: str | None = None,
    event_ids: tuple[str, ...] | None = None,
) -> ReplayPlan:
    """Build the manifest of quarantined rows a replay would requeue.

    Only ``QUARANTINED`` rows are eligible. A ``PENDING`` row needs no replay
    (the relay will take it), and a ``PUBLISHED`` row is already in the broker
    -- republishing it from here would create a duplicate for no operational
    reason, and the way to redeliver to a consumer is to move that consumer's
    offset, not to produce again.
    """
    selected = []
    for record in store.iter_records(state=EventState.QUARANTINED):
        if source_id is not None and record.source_id != source_id:
            continue
        if epoch_id is not None and record.epoch_id != epoch_id:
            continue
        if aggregate_key is not None and record.aggregate_key != aggregate_key:
            continue
        if event_ids is not None and str(record.event_id) not in event_ids:
            continue
        selected.append(record)

    ids = tuple(sorted(str(record.event_id) for record in selected))
    aggregates = tuple(
        sorted({(record.source_id, record.epoch_id, record.aggregate_key) for record in selected})
    )
    scope = ", ".join(
        part
        for part in (
            f"source={source_id}" if source_id else "",
            f"epoch={epoch_id}" if epoch_id else "",
            f"aggregate={aggregate_key}" if aggregate_key else "",
            f"explicit_ids={len(event_ids)}" if event_ids else "",
        )
        if part
    ) or "all quarantined rows"
    return ReplayPlan(
        event_ids=ids,
        aggregates=aggregates,
        manifest_sha256=payload_hash({"event_ids": list(ids)}),
        scope=scope,
    )


def execute_replay(
    store: Any,
    plan: ReplayPlan,
    *,
    approved_by: str,
    now: datetime,
    second_approver: str | None = None,
    require_two_approvers: bool = False,
) -> ReplayOutcome:
    """Requeue the planned rows, re-verifying the manifest first.

    The manifest is recomputed from the plan's own id list and compared with the
    hash recorded when the plan was built. That catches a plan object that was
    edited between approval and execution; it does not catch the database having
    changed underneath, which is why each row is also requeued conditionally on
    still being ``QUARANTINED``.
    """
    if not approved_by:
        raise IntegrityIncident("a replay requires a named approver for the audit trail")
    if require_two_approvers:
        # Financially sensitive replays get the two-person rule from section
        # 10.6. Two *distinct* people -- one name entered twice is one person.
        if not second_approver:
            raise IntegrityIncident("this replay requires a second approver")
        if second_approver == approved_by:
            raise IntegrityIncident("the second approver must be a different person")
    recomputed = payload_hash({"event_ids": list(plan.event_ids)})
    if recomputed != plan.manifest_sha256:
        raise IntegrityIncident(
            "replay manifest hash does not match its event id list; the plan was modified "
            "after it was built and must be rebuilt and re-approved"
        )

    outcome = ReplayOutcome(approved_by=approved_by, executed_at=now.isoformat())
    skipped: list[str] = []
    for event_id in plan.event_ids:
        changed = store.requeue_quarantined(
            event_id=uuid.UUID(event_id), now=now, approved_by=approved_by
        )
        if changed:
            outcome.requeued += 1
        else:
            # No longer quarantined: someone else acted, or it was published.
            skipped.append(event_id)
    outcome.skipped = tuple(skipped)
    if skipped:
        outcome.notes.append(
            f"{len(skipped)} row(s) were no longer QUARANTINED at execution time and were left alone"
        )
    _log.info(
        "replay executed",
        scope=plan.scope,
        planned=plan.count,
        requeued=outcome.requeued,
        skipped=len(skipped),
        approved_by=approved_by,
        second_approver=second_approver,
        manifest_sha256=plan.manifest_sha256,
    )
    return outcome


def backlog_drain_estimate(*, pending: int, publish_rate: float, ingest_rate: float = 0.0) -> float | None:
    """Seconds to clear a backlog, or ``None`` when it will never clear.

    Design section 16.2: the figure that matters is
    ``drain_rate = publish_rate - ingest_rate``, and a non-positive drain rate
    means **no convergence**. Returning ``None`` rather than a large number is
    the point -- "infinity" rounded to a plausible-looking ETA is how a backlog
    that is growing gets reported as a backlog that is clearing slowly.
    """
    drain_rate = publish_rate - ingest_rate
    if drain_rate <= 0:
        return None
    return pending / drain_rate
