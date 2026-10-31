"""Scenario lifecycle coordinator: runs, resets, stops, clock advances, jobs.

Three concurrency rules hold this together:

1. **Every write goes through the writer thread.** Even run creation and job
   bookkeeping are submitted as write units, so there is genuinely one writer
   rather than "one writer plus a few places that also write".
2. **Lifecycle transitions are serialized by one lock.** Two operators racing
   ``start`` against ``reset`` must produce one winner and one clear conflict,
   not two half-applied transitions (WF-008, WF-049, WF-059).
3. **A reset is one transaction.** The new run row, the old run's ``REPLACED``
   status and the ``active_run`` pointer all move together, so no reader can
   observe a generation that is half swapped (WF-038).

Generation itself runs on its own thread. It is CPU- and storage-bound, and
running it on the event loop would make every read request wait behind it.
"""

from __future__ import annotations

import asyncio
import itertools
import sqlite3
import threading
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, TypeVar

from .. import GENERATOR_VERSION
from ..canonical import format_timestamp, parse_timestamp
from ..domain.accounting import ACCOUNTING_POLICY_VERSION
from ..domain.models import (
    SYMBOL_CATALOG_VERSION,
    ClosePosition,
    ConflictError,
    IdempotencyConflictError,
    IncreasePosition,
    JobStatus,
    NotFoundError,
    OpenPosition,
    RunStatus,
    ServiceNotReadyError,
    Side,
    SimulatorError,
    ValidationError,
    WriterOverloadedError,
)

#: Intents that produce exactly one deal. Account creation and price ticks do
#: not, so counting "anything that is not a tick" would overstate progress.
_DEAL_PRODUCING_INTENTS = (OpenPosition, IncreasePosition, ClosePosition)
from ..observability.logging import get_logger
from ..observability.metrics import Metrics, NullMetrics
from ..persistence import event_store as es
from ..persistence.connection import ReadConnectionPool
from ..persistence.writer import WriteCommand, WriterService
from ..settings import Settings
from .generator import (
    GeneratorMirror,
    OpenPositionIndex,
    ScenarioGenerator,
    ScenarioSpec,
    validate_spec,
)

logger = get_logger("coordinator")

T = TypeVar("T")

#: Idempotency scope for operations that happen before a run exists.
GLOBAL_SCOPE = "__global__"

#: How often a long generation job persists its progress. Every event would
#: turn one transaction per event into two.
_PROGRESS_EVERY = 2_000


class TooManyJobsError(SimulatorError):
    code = "TOO_MANY_JOBS"
    retryable = True


@dataclass(frozen=True, slots=True)
class JobSummary:
    job_id: str
    run_id: str
    kind: str
    status: JobStatus
    target_count: int
    processed_count: int
    error_code: str | None
    error_message: str | None
    cancel_requested: bool
    created_at: str
    started_at: str | None
    finished_at: str | None

    @property
    def unit(self) -> str:
        """What ``processed_count`` counts, which differs per job kind."""
        return "deals" if self.kind == "generate" else "events"

    def to_payload(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "run_id": self.run_id,
            "kind": self.kind,
            "status": self.status.value,
            "unit": self.unit,
            "target_count": self.target_count,
            "processed_count": self.processed_count,
            "error_code": self.error_code,
            "error_message": self.error_message,
            "cancel_requested": self.cancel_requested,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "status_url": f"/admin/v1/jobs/{self.job_id}",
        }


def job_from_row(row: sqlite3.Row) -> JobSummary:
    return JobSummary(
        job_id=row["job_id"],
        run_id=row["run_id"],
        kind=row["kind"],
        status=JobStatus(row["status"]),
        target_count=int(row["target_count"]),
        processed_count=int(row["processed_count"]),
        error_code=row["error_code"],
        error_message=row["error_message"],
        cancel_requested=bool(row["cancel_requested"]),
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


# ---------------------------------------------------------------------------
# Idempotency for write units
# ---------------------------------------------------------------------------


def lookup_unit_idempotency(
    conn: sqlite3.Connection, scope: str, key: str | None, request_hash: str
) -> dict[str, Any] | None:
    if not key:
        return None
    row = conn.execute(
        "SELECT request_hash, response_json FROM idempotency_records WHERE scope = ? AND key = ?",
        (scope, key),
    ).fetchone()
    if row is None:
        return None
    if row["request_hash"] != request_hash:
        raise IdempotencyConflictError(
            "Idempotency-Key was already used with a different request body",
            details={"idempotency_key": key},
        )
    import json

    return json.loads(row["response_json"])


def store_unit_idempotency(
    conn: sqlite3.Connection,
    scope: str,
    key: str | None,
    request_hash: str,
    response: dict[str, Any],
    *,
    http_status: int = 202,
) -> None:
    if not key:
        return
    import json

    conn.execute(
        "INSERT INTO idempotency_records "
        "(scope, key, request_hash, result_seq, http_status, response_json, created_at) "
        "VALUES (?, ?, ?, NULL, ?, ?, ?)",
        (
            scope,
            key,
            request_hash,
            http_status,
            json.dumps(response, separators=(",", ":")),
            format_timestamp(datetime.now(UTC)),
        ),
    )


# ---------------------------------------------------------------------------
# Coordinator
# ---------------------------------------------------------------------------


class ScenarioCoordinator:
    def __init__(
        self,
        settings: Settings,
        writer: WriterService,
        pool: ReadConnectionPool,
        *,
        metrics: Metrics | NullMetrics | None = None,
    ) -> None:
        self._settings = settings
        self._writer = writer
        self._pool = pool
        self._metrics = metrics or NullMetrics()
        self._lock = asyncio.Lock()
        self._job_thread: threading.Thread | None = None
        self._job_cancel = threading.Event()
        self._active_job_id: str | None = None
        self._recovery_required = False
        self._recovery_detail: str | None = None

    # -- lifecycle ---------------------------------------------------------

    def startup(self) -> None:
        """Reconcile state left behind by a previous process.

        Two things can be inconsistent after an unclean stop: a job row still
        marked ``RUNNING`` by a process that no longer exists, and projections
        that disagree with the event log. The first is corrected; the second is
        *not* silently repaired - the run is flagged for operator repair and
        readiness goes false, because rebuilding financial projections without
        a human deciding to is exactly the behaviour section 4.3 forbids.
        """
        with self._pool.acquire(timeout=5.0) as conn:
            stale = [
                row["job_id"]
                for row in conn.execute(
                    "SELECT job_id FROM jobs WHERE status IN ('PENDING', 'RUNNING')"
                )
            ]
            active_run_id = es.load_active_run_id(conn)
            detail = (
                es.check_event_log_consistency(conn, active_run_id) if active_run_id else None
            )

        for job_id in stale:
            self._writer_unit(
                lambda conn, job_id=job_id: conn.execute(
                    "UPDATE jobs SET status = ?, error_code = ?, error_message = ?, finished_at = ? "
                    "WHERE job_id = ?",
                    (
                        JobStatus.FAILED.value,
                        "PROCESS_RESTARTED",
                        "job was interrupted by a process restart",
                        format_timestamp(datetime.now(UTC)),
                        job_id,
                    ),
                ),
                label="reconcile_stale_job",
            )
            logger.warning("marked interrupted job as failed", extra={"job_id": job_id})

        if detail:
            self._recovery_required = True
            self._recovery_detail = detail
            self._metrics.recovery_required.set(1)
            self._writer_unit(
                lambda conn: conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ? AND status <> ?",
                    (
                        RunStatus.READ_ONLY_REPAIR_REQUIRED.value,
                        active_run_id,
                        RunStatus.REPLACED.value,
                    ),
                ),
                label="flag_repair_required",
            )
            logger.error(
                "event log and projections disagree; run flagged for operator repair",
                extra={"run_id": active_run_id, "detail": detail},
            )
        else:
            self._metrics.recovery_required.set(0)

    def shutdown(self, *, timeout: float = 5.0) -> None:
        self._job_cancel.set()
        thread = self._job_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)

    @property
    def recovery_required(self) -> bool:
        return self._recovery_required

    @property
    def recovery_detail(self) -> str | None:
        return self._recovery_detail

    # -- reads -------------------------------------------------------------

    def active_run(self) -> es.RunSummary | None:
        with self._pool.acquire(timeout=5.0) as conn:
            run_id = es.load_active_run_id(conn)
            return es.load_run_summary(conn, run_id) if run_id else None

    def require_active_run(self) -> es.RunSummary:
        run = self.active_run()
        if run is None:
            raise ServiceNotReadyError(
                "no active run; create one with POST /admin/v1/runs",
                details={"hint": "POST /admin/v1/runs"},
            )
        return run

    def run(self, run_id: str) -> es.RunSummary | None:
        with self._pool.acquire(timeout=5.0) as conn:
            return es.load_run_summary(conn, run_id)

    def job(self, job_id: str) -> JobSummary | None:
        with self._pool.acquire(timeout=5.0) as conn:
            row = conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
            return job_from_row(row) if row is not None else None

    def job_running(self) -> bool:
        thread = self._job_thread
        return thread is not None and thread.is_alive()

    # -- transitions -------------------------------------------------------

    async def create_run(
        self,
        spec: ScenarioSpec,
        *,
        idempotency_key: str | None,
        request_hash: str,
    ) -> dict[str, Any]:
        validate_spec(spec, limits=self._settings)
        async with self._lock:

            def unit(conn: sqlite3.Connection) -> dict[str, Any]:
                # The idempotency lookup comes first, before the
                # already-running check. The realistic retry happens *while*
                # the first job is still running - the client lost the response
                # seconds ago - and answering that with TOO_MANY_JOBS instead
                # of the original job would defeat the key entirely (WF-003).
                replay = lookup_unit_idempotency(conn, GLOBAL_SCOPE, idempotency_key, request_hash)
                if replay is not None:
                    return {**replay, "replayed": True}
                if self.job_running():
                    raise TooManyJobsError(
                        "a generation job is already running",
                        details={
                            "max_active_generators": self._settings.max_active_generators
                        },
                    )
                active_run_id = es.load_active_run_id(conn)
                if active_run_id:
                    active = es.load_run_summary(conn, active_run_id)
                    if active is not None and not active.status.is_terminal:
                        raise ConflictError(
                            "an active run already exists; use reset to start a new generation",
                            details={
                                "active_run_id": active.run_id,
                                "generation": active.generation,
                                "status": active.status.value,
                            },
                        )
                generation = self._next_generation(conn)
                run_id = f"run_{generation:04d}"
                self._insert_run(conn, run_id, generation, spec)
                conn.execute(
                    "INSERT INTO active_run (id, run_id) VALUES (1, ?) "
                    "ON CONFLICT(id) DO UPDATE SET run_id = excluded.run_id",
                    (run_id,),
                )
                job_id = self._new_job_id()
                self._insert_job(
                    conn, job_id, run_id, kind="generate", target_count=spec.deal_count
                )
                response = {
                    "job_id": job_id,
                    "run_id": run_id,
                    "generation": generation,
                    "status": JobStatus.PENDING.value,
                    "status_url": f"/admin/v1/jobs/{job_id}",
                }
                store_unit_idempotency(
                    conn, GLOBAL_SCOPE, idempotency_key, request_hash, response
                )
                return {**response, "replayed": False}

            result = await self._writer.execute_unit(unit, label="create_run")
            if not result["replayed"]:
                self._start_job(result["job_id"], result["run_id"], spec, mode="full")
            return result

    async def reset(
        self,
        *,
        spec: ScenarioSpec | None,
        expected_generation: int | None,
        idempotency_key: str | None,
        request_hash: str,
    ) -> dict[str, Any]:
        if spec is not None:
            validate_spec(spec, limits=self._settings)
        async with self._lock:
            # Cancel any generation first: letting an old job keep writing into
            # a run that is about to be replaced is how generations get mixed.
            await self._cancel_running_job(timeout=10.0)

            captured: dict[str, Any] = {}

            def unit(conn: sqlite3.Connection) -> dict[str, Any]:
                replay = lookup_unit_idempotency(conn, GLOBAL_SCOPE, idempotency_key, request_hash)
                if replay is not None:
                    return {**replay, "replayed": True}
                active_run_id = es.load_active_run_id(conn)
                if active_run_id is None:
                    raise ConflictError(
                        "there is no active run to reset",
                        details={"hint": "POST /admin/v1/runs"},
                    )
                active = es.load_run_summary(conn, active_run_id)
                assert active is not None
                if expected_generation is not None and active.generation != expected_generation:
                    # WF-105: a stale operator must not reset a run they are
                    # no longer looking at.
                    raise ConflictError(
                        "generation guard does not match the active run",
                        details={
                            "expected_generation": expected_generation,
                            "active_generation": active.generation,
                        },
                    )
                effective_spec = spec if spec is not None else active.spec
                captured["spec"] = effective_spec
                generation = self._next_generation(conn)
                run_id = f"run_{generation:04d}"
                self._insert_run(conn, run_id, generation, effective_spec)
                now = format_timestamp(datetime.now(UTC))
                conn.execute(
                    "UPDATE runs SET status = ?, replaced_at = ? WHERE run_id = ?",
                    (RunStatus.REPLACED.value, now, active.run_id),
                )
                conn.execute("UPDATE active_run SET run_id = ? WHERE id = 1", (run_id,))
                job_id = self._new_job_id()
                self._insert_job(
                    conn,
                    job_id,
                    run_id,
                    kind="generate",
                    target_count=effective_spec.deal_count,
                )
                response = {
                    "job_id": job_id,
                    "run_id": run_id,
                    "generation": generation,
                    "previous_run_id": active.run_id,
                    "previous_generation": active.generation,
                    "status": JobStatus.PENDING.value,
                    "status_url": f"/admin/v1/jobs/{job_id}",
                }
                store_unit_idempotency(
                    conn, GLOBAL_SCOPE, idempotency_key, request_hash, response
                )
                return {**response, "replayed": False}

            result = await self._writer.execute_unit(unit, label="reset_run")
            if not result["replayed"]:
                self._start_job(
                    result["job_id"], result["run_id"], captured["spec"], mode="full"
                )
            return result

    async def stop_run(self, run_id: str, *, timeout: float = 5.0) -> dict[str, Any]:
        """Quiesce generation, then freeze the run.

        The order matters and is easy to get wrong: marking the run
        ``STOPPING`` *before* the generator has stopped makes the writer reject
        the generator's already-submitted commands, which fails the job with a
        run conflict instead of cancelling it. The operator asked for a stop,
        so a stop is what the job status must report (WF-050).

        If the generator does not finish within the deadline the run is left
        ``STOPPING`` rather than claimed as stopped, and the response says
        ``drained: false`` - "still draining" is an honest answer, "stopped" at
        that point would not be.
        """
        async with self._lock:
            run = self.run(run_id)
            if run is None:
                raise NotFoundError(f"Run {run_id!r} does not exist", details={"run_id": run_id})
            if run.status.is_terminal:
                # Idempotent by design: stopping a stopped run is a no-op
                # success, not a conflict.
                return {
                    "run_id": run_id,
                    "status": run.status.value,
                    "drained": True,
                    "job": self._job_payload(),
                }

            drained = await self._cancel_running_job(timeout=timeout)

            def finish(conn: sqlite3.Connection) -> str:
                current = es.load_run_summary(conn, run_id)
                if current is None:  # pragma: no cover - checked above
                    raise NotFoundError(
                        f"Run {run_id!r} does not exist", details={"run_id": run_id}
                    )
                if current.status.is_terminal:
                    return current.status.value
                status = RunStatus.STOPPED if drained else RunStatus.STOPPING
                conn.execute(
                    "UPDATE runs SET status = ? WHERE run_id = ?", (status.value, run_id)
                )
                return status.value

            status = await self._writer.execute_unit(finish, label="stop_run")
            return {
                "run_id": run_id,
                "status": status,
                "drained": drained,
                "job": self._job_payload(),
            }

    async def advance(
        self,
        run_id: str,
        *,
        events: int,
        clock_steps: int,
        idempotency_key: str | None,
        request_hash: str,
    ) -> dict[str, Any]:
        if events < 0 or clock_steps < 0:
            raise ValidationError(
                "events and clock_steps must not be negative",
                details={"events": events, "clock_steps": clock_steps},
            )
        if events > self._settings.max_advance_events:
            raise ValidationError(
                "events exceeds the configured maximum per advance call",
                details={"maximum": self._settings.max_advance_events},
            )
        async with self._lock:

            def unit(conn: sqlite3.Connection) -> dict[str, Any]:
                # Same ordering as create_run: a replay must be served even
                # while the job it refers to is still running.
                replay = lookup_unit_idempotency(conn, run_id, idempotency_key, request_hash)
                if replay is not None:
                    return {**replay, "replayed": True}
                if events and self.job_running():
                    raise TooManyJobsError(
                        "a generation job is already running",
                        details={
                            "max_active_generators": self._settings.max_active_generators
                        },
                    )
                run = es.load_run_summary(conn, run_id)
                if run is None:
                    raise NotFoundError(f"Run {run_id!r} does not exist", details={"run_id": run_id})
                if not run.status.accepts_writes:
                    raise ConflictError(
                        f"run {run_id} is {run.status.value} and does not accept writes",
                        details={"run_id": run_id, "status": run.status.value},
                    )
                if clock_steps:
                    advanced = run.virtual_time + timedelta(
                        milliseconds=self._settings.virtual_time_step_ms * clock_steps
                    )
                    conn.execute(
                        "UPDATE runs SET virtual_time = ? WHERE run_id = ?",
                        (format_timestamp(advanced), run_id),
                    )
                else:
                    advanced = run.virtual_time
                response: dict[str, Any] = {
                    "run_id": run_id,
                    "generation": run.generation,
                    "virtual_time": format_timestamp(advanced),
                    "clock_steps": clock_steps,
                    "events_requested": events,
                    "job_id": None,
                    "status": JobStatus.SUCCEEDED.value,
                }
                if events:
                    job_id = self._new_job_id()
                    self._insert_job(
                        conn, job_id, run_id, kind="advance", target_count=events
                    )
                    response["job_id"] = job_id
                    response["status"] = JobStatus.PENDING.value
                    response["status_url"] = f"/admin/v1/jobs/{job_id}"
                store_unit_idempotency(conn, run_id, idempotency_key, request_hash, response)
                return {**response, "replayed": False, "_spec": run.spec.to_canonical()}

            result = await self._writer.execute_unit(unit, label="advance_run")
            spec_payload = result.pop("_spec", None)
            if not result["replayed"] and result["job_id"]:
                run = self.run(run_id)
                assert run is not None
                self._start_job(
                    result["job_id"], run_id, run.spec, mode="advance", max_events=events
                )
            return result

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        async with self._lock:
            summary = self.job(job_id)
            if summary is None:
                raise NotFoundError(f"Job {job_id!r} does not exist", details={"job_id": job_id})
            if summary.status.is_terminal:
                return summary.to_payload()
            if self._active_job_id == job_id:
                await self._cancel_running_job(timeout=10.0)
            else:
                self._writer_unit(
                    lambda conn: conn.execute(
                        "UPDATE jobs SET cancel_requested = 1 WHERE job_id = ?", (job_id,)
                    ),
                    label="request_job_cancel",
                )
            refreshed = self.job(job_id)
            return refreshed.to_payload() if refreshed else summary.to_payload()

    # -- helpers -----------------------------------------------------------

    def _job_payload(self) -> dict[str, Any] | None:
        if self._active_job_id is None:
            return None
        summary = self.job(self._active_job_id)
        return summary.to_payload() if summary else None

    async def _cancel_running_job(self, *, timeout: float) -> bool:
        """Request cancellation and wait. Returns True when the job finished."""
        thread = self._job_thread
        if thread is None or not thread.is_alive():
            return True
        job_id = self._active_job_id
        if job_id:
            self._writer_unit(
                lambda conn: conn.execute(
                    "UPDATE jobs SET cancel_requested = 1 WHERE job_id = ?", (job_id,)
                ),
                label="request_job_cancel",
            )
        self._job_cancel.set()
        await asyncio.to_thread(thread.join, timeout)
        return not thread.is_alive()

    @staticmethod
    def _next_generation(conn: sqlite3.Connection) -> int:
        row = conn.execute("SELECT COALESCE(MAX(generation), 0) FROM runs").fetchone()
        return int(row[0]) + 1

    @staticmethod
    def _new_job_id() -> str:
        # Job ids are deliberately not derived from the seed: they are
        # operational identifiers and are excluded from the canonical digest.
        return f"job-{uuid.uuid4().hex[:12]}"

    def _insert_run(
        self, conn: sqlite3.Connection, run_id: str, generation: int, spec: ScenarioSpec
    ) -> None:
        import json

        max_open = min(
            spec.max_open_positions or self._settings.max_open_positions_per_run,
            self._settings.max_open_positions_per_run,
        )
        # `max_deals` is a safety ceiling for the run, not the generation
        # target. The target lives in the spec and in the job's target_count;
        # conflating the two would make `advance` fail the moment the initial
        # scenario finished, which is exactly what it exists to continue.
        max_deals = self._settings.max_deals_per_run
        conn.execute(
            "INSERT INTO runs (run_id, generation, seed, scenario_name, spec_hash, spec_json, "
            " generator_version, accounting_policy_version, symbol_catalog_version, status, "
            " committed_seq, virtual_time, start_virtual_time, next_position_ordinal, "
            " next_deal_ordinal, account_count, deal_count, open_position_count, "
            " max_open_positions, max_deals, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, 0, 0, 0, 0, 0, ?, ?, ?)",
            (
                run_id,
                generation,
                spec.seed,
                spec.scenario_name,
                spec.digest,
                json.dumps(spec.to_canonical(), separators=(",", ":"), sort_keys=True),
                spec.generator_version,
                ACCOUNTING_POLICY_VERSION,
                SYMBOL_CATALOG_VERSION,
                RunStatus.RUNNING.value,
                format_timestamp(spec.start_virtual_time),
                format_timestamp(spec.start_virtual_time),
                max_open,
                max_deals,
                format_timestamp(datetime.now(UTC)),
            ),
        )

    @staticmethod
    def _insert_job(
        conn: sqlite3.Connection, job_id: str, run_id: str, *, kind: str, target_count: int
    ) -> None:
        conn.execute(
            "INSERT INTO jobs (job_id, run_id, kind, status, target_count, processed_count, created_at) "
            "VALUES (?, ?, ?, ?, ?, 0, ?)",
            (
                job_id,
                run_id,
                kind,
                JobStatus.PENDING.value,
                target_count,
                format_timestamp(datetime.now(UTC)),
            ),
        )

    def _writer_unit(
        self, fn: Callable[[sqlite3.Connection], T], *, label: str, attempts: int = 200
    ) -> T:
        """Run a write unit, tolerating transient writer-queue saturation.

        Job bookkeeping must not be lost merely because the event queue is
        temporarily full of the job's own commands.
        """
        for attempt in range(attempts):
            try:
                return self._writer.execute_unit_sync(fn, label=label, timeout=30.0)
            except WriterOverloadedError:
                if attempt == attempts - 1:
                    raise
                time.sleep(0.01)
        raise WriterOverloadedError("writer remained saturated", details={"label": label})

    # -- generation job ----------------------------------------------------

    def _start_job(
        self,
        job_id: str,
        run_id: str,
        spec: ScenarioSpec,
        *,
        mode: str,
        max_events: int | None = None,
    ) -> None:
        self._job_cancel = threading.Event()
        self._active_job_id = job_id
        thread = threading.Thread(
            target=self._generation_worker,
            args=(job_id, run_id, spec, mode, max_events),
            name=f"sim-gen-{job_id}",
            daemon=True,
        )
        self._job_thread = thread
        thread.start()

    def _generation_worker(
        self,
        job_id: str,
        run_id: str,
        spec: ScenarioSpec,
        mode: str,
        max_events: int | None,
    ) -> None:
        cancel = self._job_cancel
        started = time.perf_counter()
        processed = 0
        self._writer_unit(
            lambda conn: conn.execute(
                "UPDATE jobs SET status = ?, started_at = ? WHERE job_id = ?",
                (JobStatus.RUNNING.value, format_timestamp(datetime.now(UTC)), job_id),
            ),
            label="job_running",
        )
        try:
            generator = ScenarioGenerator(
                spec, max_open_positions=self._settings.max_open_positions_per_run
            )
            if mode == "full":
                stream: Iterator[Any] = itertools.chain(
                    generator.header_intents(),
                    generator.trading_intents(max_deals=spec.deal_count),
                )
            else:
                mirror, trading_index = self._rebuild_mirror(run_id, generator)
                generator.load_mirror(mirror)
                stream = generator.trading_intents(
                    start_index=trading_index, max_events=max_events
                )
            processed = self._pump(job_id, run_id, stream, mode=mode, cancel=cancel)
        except BaseException as exc:  # noqa: BLE001 - the job owns the failure
            code = getattr(exc, "code", type(exc).__name__)
            message = str(exc)[:500]
            logger.error(
                "generation job failed",
                extra={"job_id": job_id, "run_id": run_id, "error_code": code},
                exc_info=exc,
            )
            self._finish_job(job_id, JobStatus.FAILED, processed, code=code, message=message)
            self._metrics.generation_jobs.labels(status=JobStatus.FAILED.value).inc()
        else:
            status = JobStatus.CANCELLED if cancel.is_set() else JobStatus.SUCCEEDED
            self._finish_job(job_id, status, processed)
            self._metrics.generation_jobs.labels(status=status.value).inc()
        finally:
            self._metrics.generation_job_duration.observe(time.perf_counter() - started)

    def _finish_job(
        self,
        job_id: str,
        status: JobStatus,
        processed: int,
        *,
        code: str | None = None,
        message: str | None = None,
    ) -> None:
        self._writer_unit(
            lambda conn: conn.execute(
                "UPDATE jobs SET status = ?, processed_count = ?, error_code = ?, "
                "error_message = ?, finished_at = ? WHERE job_id = ?",
                (
                    status.value,
                    processed,
                    code,
                    message,
                    format_timestamp(datetime.now(UTC)),
                    job_id,
                ),
            ),
            label="job_finish",
        )

    def _pump(
        self,
        job_id: str,
        run_id: str,
        stream: Iterator[Any],
        *,
        mode: str,
        cancel: threading.Event,
    ) -> int:
        """Submit intents with bounded in-flight depth.

        The in-flight cap is the generator respecting the writer's own
        backpressure. Submitting 100k futures and hoping would allocate 100k
        pending results and defeat the point of a bounded queue.
        """
        in_flight: deque[Any] = deque()
        limit = max(16, min(512, self._settings.max_pending_events // 4))
        events = 0
        deals = 0
        for intent in stream:
            if cancel.is_set():
                break
            future = self._submit_with_backpressure(run_id, intent, in_flight)
            in_flight.append(future)
            if len(in_flight) >= limit:
                self._collect(in_flight, keep=limit // 2)
            events += 1
            if isinstance(intent, _DEAL_PRODUCING_INTENTS):
                deals += 1
            if events % _PROGRESS_EVERY == 0:
                self._collect(in_flight, keep=0)
                count = deals if mode == "full" else events
                self._writer_unit(
                    lambda conn, count=count: conn.execute(
                        "UPDATE jobs SET processed_count = ? WHERE job_id = ?", (count, job_id)
                    ),
                    label="job_progress",
                )
        self._collect(in_flight, keep=0)
        return deals if mode == "full" else events

    def _submit_with_backpressure(
        self, run_id: str, intent: Any, in_flight: deque[Any]
    ) -> Any:
        attempts = 0
        while True:
            try:
                return self._writer.submit_command(
                    WriteCommand(run_id=run_id, intent=intent)
                )
            except WriterOverloadedError:
                # The queue is full of our own work. Drain it rather than spin.
                if in_flight:
                    self._collect(in_flight, keep=0)
                    continue
                attempts += 1
                if attempts > 5_000:
                    raise
                time.sleep(0.002)

    @staticmethod
    def _collect(in_flight: deque[Any], *, keep: int) -> None:
        """Resolve futures, propagating the first failure.

        A failed command must fail the job: the generator's in-memory mirror
        assumes every submitted intent was applied, and continuing past a
        failure would generate the rest of the dataset against state that no
        longer matches the database (WF-012).
        """
        while len(in_flight) > keep:
            in_flight.popleft().result(timeout=120.0)

    def _rebuild_mirror(
        self, run_id: str, generator: ScenarioGenerator
    ) -> tuple[GeneratorMirror, int]:
        """Reload generator state from the database to continue a run.

        ``advance`` continues the same seeded stream, so the trading index is
        derived from ``committed_seq``. Manually injected admin events shift
        that index, which means a run that mixes generation with manual events
        is no longer digest-comparable to a pure one - documented rather than
        hidden.
        """
        with self._pool.acquire(timeout=5.0) as conn:
            accounts = [
                row["account_id"]
                for row in conn.execute(
                    "SELECT account_id FROM account_identity WHERE run_id = ? ORDER BY account_id",
                    (run_id,),
                )
            ]
            open_positions = {
                (row["account_id"], row["symbol"]): (
                    Side(row["side"]),
                    Decimal(row["volume_dec"]),
                )
                for row in conn.execute(
                    "SELECT account_id, symbol, side, volume_dec FROM position_versions "
                    "WHERE run_id = ? AND valid_to_seq IS NULL AND status = 'OPEN'",
                    (run_id,),
                )
            }
            prices = {
                row["symbol"]: (Decimal(row["bid_dec"]), Decimal(row["ask_dec"]))
                for row in conn.execute(
                    "SELECT symbol, bid_dec, ask_dec FROM mark_versions "
                    "WHERE run_id = ? AND valid_to_seq IS NULL",
                    (run_id,),
                )
            }
            run = es.load_run_summary(conn, run_id)
        assert run is not None
        trading_index = max(0, run.committed_seq - generator.header_event_count)
        return (
            GeneratorMirror(accounts, OpenPositionIndex(open_positions), prices),
            trading_index,
        )
