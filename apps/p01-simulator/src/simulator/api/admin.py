"""The admin / control plane (``/admin/v1/*``).

Separated from the data plane for one reason: the data plane is handed to the
system under test, and nothing the system under test can do should be able to
reset a run or arm a fault (WF-101). Everything here requires the admin token.

``Idempotency-Key`` is **mandatory** on every operation that creates state.
These are precisely the calls whose response can be lost, and a retry without
a key would create a second run or a second event (WF-003, WF-053). Replaying
a key returns the original outcome; reusing it with a different body is a
409 rather than a surprise (WF-004).

Nothing here is an order-entry API. ``POST /runs/{id}/events`` applies one
already-decided domain event so a test can construct an exact state; there is
no order book, no routing and no OMS (section 3.2).
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import psutil
from fastapi import APIRouter, Depends, Path, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, PlainTextResponse

from ..canonical import parse_timestamp
from ..domain import invariants as inv
from ..domain.models import (
    ACCOUNT_CURRENCY,
    AdjustAccount,
    ClosePosition,
    CreateAccount,
    IncreasePosition,
    Intent,
    NotFoundError,
    OpenPosition,
    Side,
    UpdateMark,
    ValidationError,
)
from ..faults import rules as fault_rules
from ..generation.generator import ScenarioSpec
from ..observability.logging import bind_run_id, get_logger
from ..persistence.writer import WriteCommand
from .deps import (
    AppContext,
    get_context,
    optional_idempotency_key,
    request_digest,
    require_admin_plane,
    require_idempotency_key,
)
from .schemas import (
    AdvanceRequest,
    ArmFaultsRequest,
    CreateRunRequest,
    EventAppliedOut,
    InjectEventRequest,
    InvariantReportOut,
    JobOut,
    ResetRequest,
    RunAcceptedOut,
)

logger = get_logger("api.admin")

router = APIRouter(
    prefix="/admin/v1", tags=["admin"], dependencies=[Depends(require_admin_plane)]
)


def _spec_from_request(payload: CreateRunRequest) -> ScenarioSpec:
    try:
        start = parse_timestamp(payload.start_virtual_time)
    except ValueError as exc:
        raise ValidationError(
            "start_virtual_time must be an RFC 3339 timestamp",
            details={"field": "start_virtual_time", "example": "2026-10-30T00:00:00Z"},
        ) from exc
    return ScenarioSpec(
        scenario_name=payload.scenario_name,
        seed=payload.seed,
        start_virtual_time=start,
        account_count=payload.account_count,
        deal_count=payload.deal_count,
        max_open_positions=payload.max_open_positions,
        symbols=tuple(payload.symbols),
        initial_balance_usd=payload.initial_balance_usd,
        generator_version=payload.generator_version,
        execution_mode=payload.execution_mode,
        fault_profile=payload.fault_profile,
    )


def _after_generation_change(context: AppContext, run_id: str, previous_run_id: str | None) -> None:
    """Re-point process-local state at the new generation.

    Rules and snapshot leases belonging to a replaced run are released here:
    leaving them would let a fault configured for the previous generation fire
    against the new one, and leave leases nobody can ever use (WF-038).
    """
    run = context.coordinator.run(run_id)
    if run is not None:
        context.faults.set_seed(run.seed)
    if previous_run_id:
        dropped_rules = context.faults.clear(run_id=previous_run_id)
        dropped_leases = context.snapshots.drop_run(previous_run_id)
        if dropped_rules or dropped_leases:
            logger.info(
                "released state belonging to the replaced run",
                extra={
                    "previous_run_id": previous_run_id,
                    "fault_rules_dropped": dropped_rules,
                    "snapshot_leases_dropped": dropped_leases,
                },
            )


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------


@router.post("/runs", response_model=RunAcceptedOut, status_code=202, summary="Create and start a run")
async def create_run(request: Request, payload: CreateRunRequest) -> JSONResponse:
    context = get_context(request)
    key = require_idempotency_key(request)
    digest = request_digest("POST /admin/v1/runs", payload.model_dump(mode="json"))
    result = await context.coordinator.create_run(
        _spec_from_request(payload), idempotency_key=key, request_hash=digest
    )
    _after_generation_change(context, result["run_id"], None)
    bind_run_id(result["run_id"])
    return JSONResponse(status_code=202, content=RunAcceptedOut(**result).model_dump(mode="json"))


@router.get("/runs/current", summary="Describe the active run")
async def current_run(request: Request) -> dict[str, Any]:
    context = get_context(request)
    run = context.coordinator.active_run()
    if run is None:
        raise NotFoundError("there is no active run", details={"hint": "POST /admin/v1/runs"})
    bind_run_id(run.run_id)
    payload = run.to_payload()
    # The spec is echoed back because a reproducibility claim is only
    # checkable if the inputs are visible; no secret ever reaches it.
    payload["spec"] = run.spec_payload
    return payload


@router.get("/runs/{run_id}", summary="Describe a run by id")
async def get_run(request: Request, run_id: str = Path(max_length=64)) -> dict[str, Any]:
    context = get_context(request)
    run = context.coordinator.run(run_id)
    if run is None:
        raise NotFoundError(f"run {run_id!r} does not exist", details={"run_id": run_id})
    payload = run.to_payload()
    payload["spec"] = run.spec_payload
    return payload


@router.post("/runs/{run_id}/reset", response_model=RunAcceptedOut, status_code=202, summary="Start a new generation")
async def reset_run(
    request: Request, payload: ResetRequest, run_id: str = Path(max_length=64)
) -> JSONResponse:
    context = get_context(request)
    key = require_idempotency_key(request)
    digest = request_digest(
        f"POST /admin/v1/runs/{run_id}/reset", payload.model_dump(mode="json")
    )
    spec = _spec_from_request(payload.spec) if payload.spec is not None else None
    result = await context.coordinator.reset(
        spec=spec,
        expected_generation=payload.expected_generation,
        idempotency_key=key,
        request_hash=digest,
    )
    _after_generation_change(context, result["run_id"], result.get("previous_run_id"))
    return JSONResponse(status_code=202, content=RunAcceptedOut(**result).model_dump(mode="json"))


@router.post("/runs/{run_id}/stop", summary="Stop generation for a run")
async def stop_run(request: Request, run_id: str = Path(max_length=64)) -> dict[str, Any]:
    context = get_context(request)
    return await context.coordinator.stop_run(run_id)


@router.post("/runs/{run_id}/advance", summary="Advance the virtual clock and/or generate events")
async def advance_run(
    request: Request, payload: AdvanceRequest, run_id: str = Path(max_length=64)
) -> dict[str, Any]:
    context = get_context(request)
    key = require_idempotency_key(request)
    digest = request_digest(
        f"POST /admin/v1/runs/{run_id}/advance", payload.model_dump(mode="json")
    )
    return await context.coordinator.advance(
        run_id,
        events=payload.events,
        clock_steps=payload.clock_steps,
        idempotency_key=key,
        request_hash=digest,
    )


@router.post(
    "/runs/{run_id}/events",
    response_model=EventAppliedOut,
    status_code=201,
    summary="Apply one structured domain event (test instrumentation)",
)
async def inject_event(
    request: Request, payload: InjectEventRequest, run_id: str = Path(max_length=64)
) -> EventAppliedOut:
    context = get_context(request)
    key = require_idempotency_key(request)
    digest = request_digest(
        f"POST /admin/v1/runs/{run_id}/events", payload.model_dump(mode="json")
    )
    intent = _intent_from_request(payload)
    result = await context.writer.execute(
        WriteCommand(
            run_id=run_id, intent=intent, idempotency_key=key, request_hash=digest
        )
    )
    bind_run_id(result.run_id)
    return EventAppliedOut(
        run_id=result.run_id,
        generation=result.generation,
        seq=result.seq,
        event_id=result.event_id,
        event_type=result.event_type,
        virtual_time=result.virtual_time,
        deal_id=result.deal_id,
        replayed=result.replayed,
    )


def _require(payload: InjectEventRequest, field: str) -> Any:
    value = getattr(payload, field)
    if value is None:
        raise ValidationError(
            f"{field} is required for kind={payload.kind}",
            details={"field": field, "kind": payload.kind},
        )
    return value


def _intent_from_request(payload: InjectEventRequest) -> Intent:
    kind = payload.kind
    commission = payload.commission if payload.commission is not None else Decimal("0.00")
    swap = payload.swap if payload.swap is not None else Decimal("0.00")
    if kind == "CREATE_ACCOUNT":
        return CreateAccount(
            account_id=_require(payload, "account_id"),
            currency=payload.currency or ACCOUNT_CURRENCY,
            balance=payload.balance if payload.balance is not None else Decimal("0.00"),
            credit=payload.credit if payload.credit is not None else Decimal("0.00"),
        )
    if kind == "ADJUST_ACCOUNT":
        return AdjustAccount(
            account_id=_require(payload, "account_id"),
            amount=_require(payload, "amount"),
            reason=_require(payload, "reason"),
        )
    if kind == "UPDATE_MARK":
        return UpdateMark(
            symbol=_require(payload, "symbol"),
            bid=_require(payload, "bid"),
            ask=_require(payload, "ask"),
        )
    if kind == "OPEN_POSITION":
        return OpenPosition(
            account_id=_require(payload, "account_id"),
            symbol=_require(payload, "symbol"),
            side=Side(_require(payload, "side")),
            volume_lots=_require(payload, "volume_lots"),
            price=_require(payload, "price"),
            commission=commission,
            swap=swap,
        )
    if kind == "INCREASE_POSITION":
        return IncreasePosition(
            account_id=_require(payload, "account_id"),
            symbol=_require(payload, "symbol"),
            side=Side(_require(payload, "side")),
            volume_lots=_require(payload, "volume_lots"),
            price=_require(payload, "price"),
            commission=commission,
            swap=swap,
        )
    return ClosePosition(
        account_id=_require(payload, "account_id"),
        symbol=_require(payload, "symbol"),
        volume_lots=_require(payload, "volume_lots"),
        price=_require(payload, "price"),
        commission=commission,
        swap=swap,
    )


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


@router.get("/jobs/{job_id}", response_model=JobOut, summary="Job status")
async def get_job(request: Request, job_id: str = Path(max_length=64)) -> JobOut:
    context = get_context(request)
    job = context.coordinator.job(job_id)
    if job is None:
        raise NotFoundError(f"job {job_id!r} does not exist", details={"job_id": job_id})
    return JobOut(**job.to_payload())


@router.post("/jobs/{job_id}/cancel", response_model=JobOut, summary="Request job cancellation")
async def cancel_job(request: Request, job_id: str = Path(max_length=64)) -> JobOut:
    context = get_context(request)
    return JobOut(**await context.coordinator.cancel_job(job_id))


# ---------------------------------------------------------------------------
# Faults
# ---------------------------------------------------------------------------


@router.post("/faults", status_code=201, summary="Declare a deliberate fault")
async def create_fault(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    context = get_context(request)
    key = optional_idempotency_key(request)
    run = context.coordinator.active_run()
    rule_id = str(payload.get("rule_id") or "").strip() or f"fault-{os.urandom(6).hex()}"
    if context.faults.get(rule_id) is not None:
        from ..domain.models import ConflictError

        raise ConflictError(
            f"fault rule {rule_id!r} already exists",
            details={"rule_id": rule_id, "hint": "DELETE it first or choose another id"},
        )
    rule = fault_rules.build_rule(
        payload,
        rule_id=rule_id,
        run_id=run.run_id if run else None,
        max_delay_ms=context.settings.max_fault_delay_ms,
    )
    context.faults.add(rule)
    if run is not None:
        context.faults.set_seed(run.seed)

    def persist(conn: Any) -> None:
        fault_rules.insert_rule(conn, rule)

    await context.writer.execute_unit(persist, label="insert_fault_rule")
    logger.warning(
        "fault rule declared",
        extra={"rule_id": rule.rule_id, "mode": rule.action.mode.value, "idempotency_key": bool(key)},
    )
    return rule.to_payload()


@router.get("/faults", summary="List declared faults with activation counts")
async def list_faults(request: Request) -> dict[str, Any]:
    context = get_context(request)
    return {
        "armed": context.faults.armed,
        "max_fault_rules": context.settings.max_fault_rules,
        "items": context.faults.list_payload(),
    }


@router.delete("/faults/{rule_id}", summary="Remove one fault (idempotent)")
async def delete_fault(request: Request, rule_id: str = Path(max_length=64)) -> dict[str, Any]:
    context = get_context(request)
    removed = context.faults.remove(rule_id)

    def persist(conn: Any) -> int:
        return fault_rules.delete_rule(conn, rule_id)

    await context.writer.execute_unit(persist, label="delete_fault_rule")
    # Idempotent on purpose: deleting an absent rule is success, so a cleanup
    # path in a test teardown never has to care whether it already ran.
    return {"rule_id": rule_id, "removed": removed}


@router.delete("/faults", summary="Remove every fault")
async def clear_faults(request: Request) -> dict[str, Any]:
    context = get_context(request)
    removed = context.faults.clear()

    def persist(conn: Any) -> int:
        return fault_rules.delete_all_rules(conn)

    await context.writer.execute_unit(persist, label="delete_all_fault_rules")
    return {"removed": removed}


@router.post("/faults/arm", summary="Arm or disarm every fault at once")
async def arm_faults(request: Request, payload: ArmFaultsRequest) -> dict[str, Any]:
    context = get_context(request)
    context.faults.set_armed(payload.armed)
    logger.warning("fault engine master switch changed", extra={"armed": payload.armed})
    return {"armed": context.faults.armed, "active_fault_rules": len(context.faults.snapshot())}


# ---------------------------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------------------------


@router.get(
    "/diagnostics/invariants",
    response_model=InvariantReportOut,
    summary="Run the storage-scope invariant suite",
)
async def diagnostics_invariants(
    request: Request,
    run_id: str | None = None,
    scope: str = "fast",
) -> InvariantReportOut:
    """Run the invariant suite.

    ``scope=fast`` (the default) runs only the aggregate checks and skips the
    canonical digest, which keeps it inside a sane HTTP timeout on any dataset
    size. ``scope=full`` adds the checks that walk every deal and position plus
    the digest; on a 100k-deal run that was measured at tens of seconds, so CI
    and release gates should use ``simulator verify`` rather than this endpoint
    (section 5.2). A fast sweep never reports ``storage_scope_ok``.
    """
    context = get_context(request)
    if scope not in {inv.FAST_SCOPE, inv.FULL_SCOPE}:
        raise ValidationError(
            "scope must be 'fast' or 'full'",
            details={"field": "scope", "supported": [inv.FAST_SCOPE, inv.FULL_SCOPE]},
        )
    run = context.coordinator.run(run_id) if run_id else context.coordinator.active_run()
    if run is None:
        raise NotFoundError(
            "no run to check", details={"run_id": run_id} if run_id else {"hint": "POST /admin/v1/runs"}
        )

    def sweep() -> tuple[list[inv.InvariantResult], str, dict[str, int]]:
        # Even the fast sweep issues several queries, so it runs in a worker
        # thread rather than on the event loop (WF-093).
        with context.pool.acquire(timeout=30.0) as conn:
            results = inv.run_all(conn, run.run_id, scope=scope)
            if scope == inv.FULL_SCOPE:
                digest, counts = inv.canonical_state_digest(conn, run.run_id)
            else:
                digest, counts = "", {}
        return results, digest, counts

    results, digest, counts = await run_in_threadpool(sweep)
    for item in results:
        if item.status == inv.FAIL:
            context.metrics.invariant_failures.labels(invariant_id=item.invariant_id).inc()
            logger.error(
                "invariant failed",
                extra={"invariant_id": item.invariant_id, "detail": item.detail},
            )
    return InvariantReportOut(
        run_id=run.run_id,
        generation=run.generation,
        committed_seq=run.committed_seq,
        scope=scope,
        # Empty on a fast sweep rather than a stale value: a digest that was
        # not computed must not look like one that was.
        canonical_digest=digest,
        record_counts=counts,
        summary=inv.summarize(results),
        results=[item.to_payload() for item in results],
    )


@router.get("/metrics", summary="Prometheus metrics")
async def metrics(request: Request) -> Response:
    context = get_context(request)
    if not context.settings.metrics_enabled:
        return PlainTextResponse("metrics are disabled\n", status_code=404)

    # Gauges that describe "right now" are sampled at scrape time rather than
    # maintained continuously, which keeps the hot paths free of stat() calls.
    database = context.settings.db_path
    try:
        context.metrics.db_file_bytes.set(database.stat().st_size if database.exists() else 0)
        wal = database.with_suffix(database.suffix + "-wal")
        context.metrics.sqlite_wal_bytes.set(wal.stat().st_size if wal.exists() else 0)
    except OSError:
        pass
    try:
        context.metrics.resident_memory_bytes.set(psutil.Process().memory_info().rss)
    except Exception:  # noqa: BLE001 - metrics must never fail a request
        pass
    context.metrics.writer_queue_depth.set(context.writer.queue_depth)
    context.metrics.snapshot_active.set(context.snapshots.active_count)
    context.metrics.requests_in_flight.labels(plane="data").set(context.admission.in_flight)

    body, content_type = context.metrics.render()
    return Response(content=body, media_type=content_type)
