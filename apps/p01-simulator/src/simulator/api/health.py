"""Health endpoints.

Three distinct questions, deliberately not conflated (WF-112, section 4.3):

* ``/health/live`` - is the process running? Nothing more. It performs no
  query and holds no lock, so it still answers while every data slot is
  occupied (WF-056), and it is exempt from fault injection so a chaos run
  cannot fake a dead process.
* ``/health/ready`` - can this instance serve *consistent* reads? A process
  whose projections disagree with its event log is alive and not ready, and
  saying otherwise is the failure section 4.3 exists to prevent.
* Correctness is neither of these. It is answered by the invariant suite, not
  by a probe.

Neither route requires a token: a readiness probe that needs a secret is a
readiness probe nobody configures correctly.
"""

from __future__ import annotations

from fastapi import APIRouter, Request

from .. import SCHEMA_VERSION, __version__
from ..domain.models import RunStatus
from .deps import get_context
from .schemas import LivenessOut, ReadinessOut

router = APIRouter(tags=["health"])


@router.get("/health/live", response_model=LivenessOut, summary="Process liveness")
async def liveness() -> LivenessOut:
    return LivenessOut(status="alive", version=__version__)


@router.get("/health/ready", response_model=ReadinessOut, summary="Readiness for consistent reads")
async def readiness(request: Request) -> ReadinessOut:
    context = get_context(request)
    reasons: list[str] = []

    if not context.writer.running:
        reasons.append("writer_not_running")
    if context.coordinator.recovery_required:
        reasons.append("recovery_required")

    run = None
    try:
        run = context.repository.active_run()
    except Exception:  # noqa: BLE001 - an unreadable database is a reason, not a 500
        reasons.append("database_unreadable")

    if run is not None and run.status is RunStatus.READ_ONLY_REPAIR_REQUIRED:
        reasons.append("run_requires_repair")

    queue_depth = context.writer.queue_depth
    capacity = context.writer.queue_capacity
    if capacity and queue_depth >= capacity:
        # Full queue is a real refusal of writes, so readiness reflects it even
        # though reads are still being served.
        reasons.append("writer_queue_full")

    return ReadinessOut(
        ready=not reasons,
        reasons=reasons,
        run_id=run.run_id if run else None,
        generation=run.generation if run else None,
        run_status=run.status.value if run else None,
        committed_seq=run.committed_seq if run else 0,
        schema_version=SCHEMA_VERSION,
        writer_running=context.writer.running,
        writer_queue_depth=queue_depth,
        writer_queue_capacity=capacity,
        active_snapshots=context.snapshots.active_count,
        requests_in_flight=context.admission.in_flight,
        recovery_required=context.coordinator.recovery_required,
        faults_armed=context.faults.armed,
        active_fault_rules=len(context.faults.snapshot()),
    )
