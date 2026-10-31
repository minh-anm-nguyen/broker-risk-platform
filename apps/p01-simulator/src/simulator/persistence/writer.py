"""The single writer: one thread, one bounded queue, one transaction at a time.

Why a dedicated writer thread rather than writing from request handlers:

* ``seq`` must be contiguous (INV-01). Allocating it as ``committed_seq + 1``
  inside the transaction that consumes it means a rollback consumes no number,
  and a single writer means no two transactions race for the same one.
* SQLite calls are blocking. Issuing them from an ``async def`` handler would
  stall the event loop and make ``/health/live`` slow for reasons that have
  nothing to do with health (WF-093).
* Backpressure needs somewhere to live. A bounded queue is the one place that
  can say "no" early, with a 503, instead of growing memory until the kernel
  decides for us (WF-090).

**Batching contract.** Several commands may share one transaction. If any
command fails for a storage reason the whole batch rolls back and each command
is then retried in its own transaction, so every caller learns its own outcome
rather than "someone in your batch failed". A *domain* rejection is different:
the engine validates before writing anything, so a rejected command leaves the
transaction clean and the rest of the batch proceeds (WF-012).

**No future is resolved before COMMIT.** That is what makes the retry path safe
and what stops a caller from acting on a write that later rolled back.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import queue
import sqlite3
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

from ..canonical import format_timestamp, parse_timestamp
from ..domain import engine as eng
from ..domain.models import (
    ConflictError,
    EventType,
    IdempotencyConflictError,
    Intent,
    NotFoundError,
    PositionStatus,
    RunStatus,
    ServiceNotReadyError,
    SimulatorError,
    StorageError,
    WriterOverloadedError,
)
from ..observability.logging import get_logger
from ..observability.metrics import Metrics, NullMetrics
from ..settings import Settings
from . import event_store as es
from .connection import open_connection
from .failpoints import (
    AFTER_COMMIT,
    BEFORE_COMMIT,
    STORAGE_FULL,
    STORAGE_READONLY,
    FailpointController,
    InjectedStorageFailure,
)

logger = get_logger("writer")

T = TypeVar("T")

_RUN_COLUMNS = (
    "run_id, generation, status, committed_seq, virtual_time, next_position_ordinal, "
    "next_deal_ordinal, account_count, deal_count, open_position_count, "
    "max_open_positions, max_deals"
)


# ---------------------------------------------------------------------------
# Mutable run state, loaded once per transaction
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class RunState:
    """In-transaction view of a run's counters and clock.

    Held in memory for the duration of one transaction and flushed once, so a
    500-command batch performs one UPDATE of the run row instead of 500. On
    rollback the object is discarded and reloaded from the database, which is
    why nothing may be cached across transactions.
    """

    run_id: str
    generation: int
    status: RunStatus
    committed_seq: int
    virtual_time: datetime
    next_position_ordinal: int
    next_deal_ordinal: int
    account_count: int
    deal_count: int
    open_position_count: int
    max_open_positions: int
    max_deals: int
    dirty: bool = False

    @classmethod
    def load(cls, conn: sqlite3.Connection, run_id: str) -> "RunState":
        row = conn.execute(
            f"SELECT {_RUN_COLUMNS} FROM runs WHERE run_id = ?", (run_id,)
        ).fetchone()
        if row is None:
            raise NotFoundError(f"Run {run_id!r} does not exist", details={"run_id": run_id})
        return cls(
            run_id=row["run_id"],
            generation=int(row["generation"]),
            status=RunStatus(row["status"]),
            committed_seq=int(row["committed_seq"]),
            virtual_time=parse_timestamp(row["virtual_time"]),
            next_position_ordinal=int(row["next_position_ordinal"]),
            next_deal_ordinal=int(row["next_deal_ordinal"]),
            account_count=int(row["account_count"]),
            deal_count=int(row["deal_count"]),
            open_position_count=int(row["open_position_count"]),
            max_open_positions=int(row["max_open_positions"]),
            max_deals=int(row["max_deals"]),
        )

    def flush(self, conn: sqlite3.Connection) -> None:
        if not self.dirty:
            return
        conn.execute(
            "UPDATE runs SET committed_seq = ?, virtual_time = ?, next_position_ordinal = ?, "
            "next_deal_ordinal = ?, account_count = ?, deal_count = ?, open_position_count = ? "
            "WHERE run_id = ?",
            (
                self.committed_seq,
                format_timestamp(self.virtual_time),
                self.next_position_ordinal,
                self.next_deal_ordinal,
                self.account_count,
                self.deal_count,
                self.open_position_count,
                self.run_id,
            ),
        )


class _TransactionStateView:
    """:class:`~simulator.domain.engine.StateView` over an open transaction.

    Reads go straight to SQLite. Inside a transaction SQLite already sees the
    writes made earlier in the same batch, so there is no separate
    pending-mutation cache that could drift out of step with the rows.
    """

    __slots__ = ("_conn", "_state")

    def __init__(self, conn: sqlite3.Connection, state: RunState) -> None:
        self._conn = conn
        self._state = state

    def get_account(self, account_id: str):
        return es.load_current_account(self._conn, self._state.run_id, account_id)

    def get_open_position(self, account_id: str, symbol: str):
        return es.load_current_open_position(self._conn, self._state.run_id, account_id, symbol)

    def get_mark(self, symbol: str):
        return es.load_current_mark(self._conn, self._state.run_id, symbol)

    def open_position_count(self) -> int:
        return self._state.open_position_count

    def deal_count(self) -> int:
        return self._state.deal_count

    def allocate_position_ordinal(self) -> int:
        self._state.next_position_ordinal += 1
        self._state.dirty = True
        return self._state.next_position_ordinal

    def allocate_deal_ordinal(self) -> int:
        self._state.next_deal_ordinal += 1
        self._state.dirty = True
        return self._state.next_deal_ordinal


# ---------------------------------------------------------------------------
# Public task and result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WriteCommand:
    run_id: str
    intent: Intent
    idempotency_key: str | None = None
    #: Digest of the canonical request. Replaying a key with a different
    #: request is a conflict, not a replay (WF-004).
    request_hash: str | None = None


@dataclass(frozen=True, slots=True)
class WriteResult:
    run_id: str
    generation: int
    seq: int
    event_id: str
    event_type: str
    virtual_time: str
    deal_id: str | None = None
    #: True when this response was served from the idempotency record rather
    #: than by applying a new effect.
    replayed: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "generation": self.generation,
            "seq": self.seq,
            "event_id": self.event_id,
            "event_type": self.event_type,
            "virtual_time": self.virtual_time,
            "deal_id": self.deal_id,
        }


@dataclass(slots=True)
class _CommandTask:
    command: WriteCommand
    future: "concurrent.futures.Future[WriteResult]"


@dataclass(slots=True)
class _UnitTask:
    label: str
    fn: Callable[[sqlite3.Connection], Any]
    future: "concurrent.futures.Future[Any]"


@dataclass(slots=True)
class _Outcome:
    result: WriteResult | None = None
    error: BaseException | None = None
    event_type: str | None = None
    rejection_code: str | None = None


# ---------------------------------------------------------------------------
# Writer service
# ---------------------------------------------------------------------------


class WriterService:
    """Serializes every write in the process onto one thread."""

    def __init__(
        self,
        settings: Settings,
        *,
        metrics: Metrics | NullMetrics | None = None,
        failpoints: FailpointController | None = None,
    ) -> None:
        self._settings = settings
        self._metrics = metrics or NullMetrics()
        self._failpoints = failpoints or FailpointController()
        self._queue: queue.Queue[_CommandTask | _UnitTask] = queue.Queue(
            maxsize=settings.max_pending_events
        )
        self._thread: threading.Thread | None = None
        self._running = False
        self._stop_requested = False
        self._drain = True
        self._lock = threading.Lock()

    # -- lifecycle ---------------------------------------------------------

    def start(self) -> None:
        with self._lock:
            if self._running:
                return
            self._stop_requested = False
            self._running = True
            self._thread = threading.Thread(target=self._worker, name="sim-writer", daemon=True)
            self._thread.start()

    def stop(self, *, drain: bool = True, timeout: float = 10.0) -> None:
        """Stop the writer.

        With ``drain=True`` queued commands are applied first, bounded by
        ``timeout``; anything still queued afterwards is failed explicitly
        rather than silently dropped, so a caller never believes a write
        happened because nobody said otherwise (WF-050, WF-124).
        """
        with self._lock:
            if not self._running:
                return
            self._drain = drain
            self._stop_requested = True
            thread = self._thread
        if thread is not None:
            thread.join(timeout=timeout)
        with self._lock:
            self._running = False
            self._thread = None
        self._fail_remaining(
            ServiceNotReadyError("writer stopped before this write was applied")
        )

    @property
    def running(self) -> bool:
        return self._running

    @property
    def queue_depth(self) -> int:
        return self._queue.qsize()

    @property
    def queue_capacity(self) -> int:
        return self._queue.maxsize

    def _fail_remaining(self, error: SimulatorError) -> None:
        while True:
            try:
                task = self._queue.get_nowait()
            except queue.Empty:
                return
            if not task.future.done():
                task.future.set_exception(error)

    # -- submission --------------------------------------------------------

    def _enqueue(self, task: _CommandTask | _UnitTask) -> None:
        if not self._running or self._stop_requested:
            raise ServiceNotReadyError("writer is not accepting writes")
        try:
            self._queue.put_nowait(task)
        except queue.Full as exc:
            # The whole point of a bounded queue: reject early and cheaply
            # rather than accept work we have no capacity to do.
            self._metrics.writer_queue_rejected.inc()
            raise WriterOverloadedError(
                "writer queue is full; retry after backoff",
                details={"queue_capacity": self._queue.maxsize},
            ) from exc
        self._metrics.writer_queue_depth.set(self._queue.qsize())

    def submit_command(self, command: WriteCommand) -> "concurrent.futures.Future[WriteResult]":
        future: "concurrent.futures.Future[WriteResult]" = concurrent.futures.Future()
        self._enqueue(_CommandTask(command=command, future=future))
        return future

    def submit_unit(
        self, fn: Callable[[sqlite3.Connection], T], *, label: str
    ) -> "concurrent.futures.Future[T]":
        """Run an arbitrary write unit in its own transaction on the writer thread.

        Run creation, reset, job bookkeeping and clock advances all go through
        here so that there is genuinely one writer, not "one writer plus a few
        places that also write".
        """
        future: "concurrent.futures.Future[T]" = concurrent.futures.Future()
        self._enqueue(_UnitTask(label=label, fn=fn, future=future))
        return future

    async def execute(self, command: WriteCommand) -> WriteResult:
        return await asyncio.wrap_future(self.submit_command(command))

    async def execute_unit(self, fn: Callable[[sqlite3.Connection], T], *, label: str) -> T:
        return await asyncio.wrap_future(self.submit_unit(fn, label=label))

    def execute_sync(self, command: WriteCommand, timeout: float | None = 30.0) -> WriteResult:
        return self.submit_command(command).result(timeout=timeout)

    def execute_unit_sync(
        self, fn: Callable[[sqlite3.Connection], T], *, label: str, timeout: float | None = 30.0
    ) -> T:
        return self.submit_unit(fn, label=label).result(timeout=timeout)

    # -- worker ------------------------------------------------------------

    def _worker(self) -> None:
        conn = open_connection(self._settings)
        pending_unit: _UnitTask | None = None
        try:
            while True:
                if pending_unit is not None:
                    task: _CommandTask | _UnitTask | None = pending_unit
                    pending_unit = None
                else:
                    try:
                        task = self._queue.get(timeout=0.05)
                    except queue.Empty:
                        if self._stop_requested:
                            return
                        continue

                if self._stop_requested and not self._drain:
                    task.future.set_exception(
                        ServiceNotReadyError("writer is draining and rejected this write")
                    )
                    continue

                if isinstance(task, _UnitTask):
                    self._run_unit(conn, task)
                    self._metrics.writer_queue_depth.set(self._queue.qsize())
                    continue

                batch: list[_CommandTask] = [task]
                while len(batch) < self._settings.writer_batch_size:
                    try:
                        nxt = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if isinstance(nxt, _UnitTask):
                        # Units are never batched with domain events: they do
                        # arbitrary work and must not share a rollback.
                        pending_unit = nxt
                        break
                    batch.append(nxt)
                self._process_batch(conn, batch)
                self._metrics.writer_queue_depth.set(self._queue.qsize())
        except BaseException:  # pragma: no cover - defensive
            logger.exception("writer thread terminated unexpectedly")
            raise
        finally:
            conn.close()

    def _safe_rollback(self, conn: sqlite3.Connection) -> None:
        try:
            if conn.in_transaction:
                conn.rollback()
        except sqlite3.Error:  # pragma: no cover - rollback of a dead conn
            logger.warning("rollback failed")

    def _run_unit(self, conn: sqlite3.Connection, task: _UnitTask) -> None:
        started = time.perf_counter()
        try:
            conn.execute("BEGIN IMMEDIATE")
            result = task.fn(conn)
            self._failpoints.check(BEFORE_COMMIT)
            conn.commit()
            self._failpoints.check(AFTER_COMMIT)
        except BaseException as exc:
            self._safe_rollback(conn)
            task.future.set_exception(self._translate(exc))
        else:
            self._metrics.writer_commit_duration.observe(time.perf_counter() - started)
            task.future.set_result(result)

    def _process_batch(self, conn: sqlite3.Connection, batch: list[_CommandTask]) -> None:
        started = time.perf_counter()
        try:
            outcomes = self._apply_batch(conn, batch)
        except BaseException as exc:
            self._safe_rollback(conn)
            self._metrics.writer_batches.labels(outcome="retried_individually").inc()
            logger.warning(
                "writer batch failed; retrying each command in its own transaction",
                extra={"batch_size": len(batch), "error_type": type(exc).__name__},
            )
            if len(batch) == 1:
                # Already isolated: retrying would just fail again.
                task = batch[0]
                self._metrics.writer_batches.labels(outcome="failed").inc()
                if not task.future.done():
                    task.future.set_exception(self._translate(exc))
                return
            for task in batch:
                if not task.future.done():
                    self._apply_single(conn, task)
            return

        self._metrics.writer_batches.labels(outcome="committed").inc()
        self._metrics.writer_commit_duration.observe(time.perf_counter() - started)
        self._resolve(batch, outcomes)

    def _apply_single(self, conn: sqlite3.Connection, task: _CommandTask) -> None:
        try:
            outcomes = self._apply_batch(conn, [task])
        except BaseException as exc:
            self._safe_rollback(conn)
            self._metrics.writer_batches.labels(outcome="failed").inc()
            task.future.set_exception(self._translate(exc))
        else:
            self._metrics.writer_batches.labels(outcome="committed").inc()
            self._resolve([task], outcomes)

    def _resolve(self, batch: list[_CommandTask], outcomes: list[_Outcome]) -> None:
        """Publish results only after the transaction committed."""
        for task, outcome in zip(batch, outcomes, strict=True):
            if task.future.done():
                continue
            if outcome.rejection_code:
                self._metrics.domain_rejections.labels(code=outcome.rejection_code).inc()
            if outcome.event_type:
                self._metrics.writer_commits.labels(event_type=outcome.event_type).inc()
            if outcome.error is not None:
                task.future.set_exception(outcome.error)
            else:
                assert outcome.result is not None
                self._metrics.last_committed_seq.labels(run_id=outcome.result.run_id).set(
                    outcome.result.seq
                )
                task.future.set_result(outcome.result)

    def _apply_batch(self, conn: sqlite3.Connection, batch: list[_CommandTask]) -> list[_Outcome]:
        conn.execute("BEGIN IMMEDIATE")
        states: dict[str, RunState] = {}
        outcomes: list[_Outcome] = []
        applied_seqs: list[int] = []

        for task in batch:
            try:
                result, event_type = self._apply_command(conn, states, task.command)
            except SimulatorError as exc:
                # The engine validates before it writes, so a domain rejection
                # leaves this transaction exactly as it was. The rest of the
                # batch is unaffected (WF-012).
                outcomes.append(_Outcome(error=exc, rejection_code=exc.code))
                continue
            outcomes.append(_Outcome(result=result, event_type=event_type))
            applied_seqs.append(result.seq)

        for state in states.values():
            state.flush(conn)

        for seq in applied_seqs:
            self._failpoints.check(BEFORE_COMMIT, seq=seq)
        conn.commit()
        for seq in applied_seqs:
            self._failpoints.check(AFTER_COMMIT, seq=seq)
        return outcomes

    def _apply_command(
        self,
        conn: sqlite3.Connection,
        states: dict[str, RunState],
        command: WriteCommand,
    ) -> tuple[WriteResult, str | None]:
        state = states.get(command.run_id)
        if state is None:
            state = RunState.load(conn, command.run_id)
            states[command.run_id] = state

        # 1. Idempotency replay. Read-only, and it happens before anything
        #    else so a retry of a committed write never re-validates against
        #    state that has moved on since (WF-053).
        if command.idempotency_key:
            replay = self._lookup_idempotent(conn, command)
            if replay is not None:
                return replay, None

        # 2. A terminal run accepts no further events (INV-14).
        if not state.status.accepts_writes:
            raise ConflictError(
                f"run {state.run_id} is {state.status.value} and does not accept writes",
                details={"run_id": state.run_id, "status": state.status.value},
            )

        # 3. Allocate the sequence number from committed state. Because this
        #    happens inside the transaction, a rollback consumes no number and
        #    INV-01 contiguity holds.
        seq = state.committed_seq + 1
        virtual_time = state.virtual_time

        # 4. Validate and compute. Pure with respect to storage: this either
        #    raises before any write, or returns everything to be written.
        view = _TransactionStateView(conn, state)
        limits = eng.EngineLimits(
            max_open_positions=state.max_open_positions,
            max_deals=state.max_deals,
        )
        applied = eng.apply_intent(
            view, command.intent, seq=seq, virtual_time=virtual_time, limits=limits
        )

        # 5. Write. The event, every projection version, the deal and the
        #    idempotency record all land in this one transaction (section 2.2).
        #
        #    The storage failpoints fire here, between validation and the first
        #    write, so a simulated ENOSPC or read-only mount aborts a
        #    transaction that was otherwise about to succeed - which is the
        #    only interesting place for it to happen (WF-080, WF-081).
        self._failpoints.check(STORAGE_FULL, seq=seq)
        self._failpoints.check(STORAGE_READONLY, seq=seq)
        event_id = eng.event_id_for(seq)
        created_at = datetime.now(UTC)
        es.insert_event(
            conn,
            run_id=state.run_id,
            seq=seq,
            event_id=event_id,
            event_type=applied.event_type,
            payload=applied.payload,
            virtual_time=virtual_time,
            created_at=created_at,
        )
        for mutation in applied.mutations:
            self._write_mutation(conn, state, seq, mutation)
        if applied.deal is not None:
            es.insert_deal(conn, state.run_id, applied.deal)
            state.deal_count += 1

        state.committed_seq = seq
        state.virtual_time = virtual_time + timedelta(
            milliseconds=self._settings.virtual_time_step_ms
        )
        state.dirty = True

        result = WriteResult(
            run_id=state.run_id,
            generation=state.generation,
            seq=seq,
            event_id=event_id,
            event_type=applied.event_type.value,
            virtual_time=format_timestamp(virtual_time),
            deal_id=applied.deal.deal_id if applied.deal is not None else None,
        )
        if command.idempotency_key:
            conn.execute(
                "INSERT INTO idempotency_records "
                "(scope, key, request_hash, result_seq, http_status, response_json, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    state.run_id,
                    command.idempotency_key,
                    command.request_hash or "",
                    seq,
                    201,
                    json.dumps(result.to_payload(), separators=(",", ":")),
                    format_timestamp(created_at),
                ),
            )
        return result, applied.event_type.value

    def _lookup_idempotent(
        self, conn: sqlite3.Connection, command: WriteCommand
    ) -> WriteResult | None:
        row = conn.execute(
            "SELECT request_hash, response_json FROM idempotency_records WHERE scope = ? AND key = ?",
            (command.run_id, command.idempotency_key),
        ).fetchone()
        if row is None:
            return None
        if row["request_hash"] != (command.request_hash or ""):
            # Same key, different request. Returning the old result would be a
            # lie and applying the new one would break the key's meaning.
            raise IdempotencyConflictError(
                "Idempotency-Key was already used with a different request body",
                details={"idempotency_key": command.idempotency_key},
            )
        stored = json.loads(row["response_json"])
        return WriteResult(
            run_id=stored["run_id"],
            generation=int(stored["generation"]),
            seq=int(stored["seq"]),
            event_id=stored["event_id"],
            event_type=stored["event_type"],
            virtual_time=stored["virtual_time"],
            deal_id=stored.get("deal_id"),
            replayed=True,
        )

    def _write_mutation(
        self, conn: sqlite3.Connection, state: RunState, seq: int, mutation: eng.Mutation
    ) -> None:
        if isinstance(mutation, eng.AccountMutation):
            es.write_account_version(
                conn, state.run_id, seq, mutation.account, is_new=mutation.is_new
            )
            if mutation.is_new:
                state.account_count += 1
            return
        if isinstance(mutation, eng.PositionMutation):
            es.write_position_version(
                conn, state.run_id, seq, mutation.position, is_new=mutation.is_new
            )
            was_open = mutation.previous_status is PositionStatus.OPEN
            now_open = mutation.position.status is PositionStatus.OPEN
            if now_open and not was_open:
                state.open_position_count += 1
            elif was_open and not now_open:
                state.open_position_count -= 1
            return
        es.write_mark_version(conn, state.run_id, seq, mutation.mark)

    # -- error translation -------------------------------------------------

    def _translate(self, exc: BaseException) -> BaseException:
        """Map an infrastructure failure onto the public error contract.

        The distinction that matters: a ``StorageError`` is retryable and says
        nothing about whether the write landed, which is precisely what the
        idempotency key is for. An ``IntegrityError`` is not retryable at all,
        because it means the engine allowed something the schema forbids - a
        bug in this service, not a client problem.
        """
        if isinstance(exc, SimulatorError):
            return exc
        if isinstance(exc, InjectedStorageFailure):
            kind = {
                STORAGE_FULL: "disk_full",
                STORAGE_READONLY: "readonly",
            }.get(exc.name, "injected")
            self._metrics.storage_errors.labels(kind=kind).inc()
            return StorageError(
                "storage write failed",
                details={"source": "failpoint", "failpoint": exc.name, "kind": kind},
            )
        if isinstance(exc, sqlite3.IntegrityError):
            self._metrics.invariant_failures.labels(invariant_id="DB_CONSTRAINT").inc()
            logger.error("storage integrity constraint violated", exc_info=exc)
            return SimulatorError(
                "storage integrity constraint violated",
                details={"cause": str(exc)[:200]},
            )
        if isinstance(exc, sqlite3.Error):
            text = str(exc).lower()
            if "locked" in text or "busy" in text:
                self._metrics.sqlite_busy.inc()
                return StorageError(
                    "database is busy; retry after backoff", details={"cause": "SQLITE_BUSY"}
                )
            if "full" in text:
                self._metrics.storage_errors.labels(kind="disk_full").inc()
                return StorageError("storage is full", details={"cause": "ENOSPC"})
            if "readonly" in text or "read-only" in text or "attempt to write" in text:
                self._metrics.storage_errors.labels(kind="readonly").inc()
                return StorageError("storage is read-only", details={"cause": "EROFS"})
            self._metrics.storage_errors.labels(kind="other").inc()
            logger.error("storage operation failed", exc_info=exc)
            return StorageError(
                "storage operation failed", details={"cause": type(exc).__name__}
            )
        return exc
