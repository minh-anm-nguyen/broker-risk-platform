"""The connector's own HTTP surface: health, metrics, and a read-only probe.

Nothing here calls the driver. A health handler that reached upstream would
make liveness depend on the source being alive, and an orchestrator would then
restart the connector *because the broker was down* - turning one outage into
two. ``/health/live`` answers from process state only; ``/health/ready``
answers from session state that the supervisor and its watchdog maintain in
the background (INV-13, WF2-P-01).

``/health/ready`` returns 503 when the connector will not serve a read. That
includes the quarantined case: liveness stays 200 because the process is fine,
while readiness is false because a driver call is stuck and no further call
will be dispatched. Reporting 200 on both would be the dishonest combination
section 5.4 warns about.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import fields, is_dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, AsyncIterator

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from . import CONTRACT_VERSION, __version__
from .client import Connector
from .errors import ERROR_CODES, ConnectorError
from .ports import Entity, ReadPlan

__all__ = ["create_app"]


def create_app(connector: Connector, *, connect_on_startup: bool = True) -> FastAPI:
    """Build the ASGI app around an existing connector."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if connect_on_startup:
            try:
                await connector.connect()
            except ConnectorError:
                # Startup does not fail on an unreachable source: readiness
                # reports it and the supervisor keeps trying inside its
                # backoff budget. Crashing the process instead would restart
                # it into the same outage (WF2-O-05).
                pass
        try:
            yield
        finally:
            await connector.close()

    app = FastAPI(
        title="MT5 Integration Connector",
        version=__version__,
        description=(
            "Read-only async port over a blocking broker driver. Owns no durable "
            "checkpoint: committing one belongs to the sync service (ADR-207)."
        ),
        lifespan=lifespan,
    )
    app.state.connector = connector

    @app.get("/health/live", summary="Process liveness")
    async def live() -> dict[str, Any]:
        return {
            "status": "alive" if connector.supervisor.live() else "draining",
            "version": __version__,
        }

    @app.get("/health/ready", summary="Readiness to serve reads")
    async def ready() -> Response:
        health = connector.supervisor.health()
        lag = connector.loop_monitor.summary()
        health["event_loop_lag"] = {
            "samples": lag.samples,
            "p50_ms": round(lag.p50_ms, 3),
            "p95_ms": round(lag.p95_ms, 3),
            "p99_ms": round(lag.p99_ms, 3),
            "max_ms": round(lag.max_ms, 3),
        }
        return JSONResponse(health, status_code=200 if health["ready"] else 503)

    @app.get("/metrics", summary="Prometheus metrics")
    async def metrics() -> Response:
        return Response(
            content=connector.metrics.render(),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/v1/contract", summary="The handoff contract this build publishes")
    async def contract() -> dict[str, Any]:
        capabilities = connector.supervisor.capabilities
        return {
            "connector_version": __version__,
            "contract_version": CONTRACT_VERSION,
            "entities": [entity.value for entity in Entity],
            "error_codes": list(ERROR_CODES),
            "owns_durable_checkpoint": False,
            "checkpoint_note": (
                "the connector reports a checkpoint candidate only; committing one "
                "after the pages are persisted belongs to the sync service"
            ),
            "negotiated_capabilities": (
                None
                if capabilities is None
                else {
                    "contract_version": capabilities.contract_version,
                    "source_kind": capabilities.source_kind,
                    "supported_entities": sorted(
                        entity.value for entity in capabilities.supported_entities
                    ),
                    "snapshot_cursor": capabilities.snapshot_cursor,
                    "incremental_deals_after_seq": capabilities.incremental_deals_after_seq,
                    "stable_snapshot_across_entities": (
                        capabilities.stable_snapshot_across_entities
                    ),
                    "max_page_size": capabilities.max_page_size,
                }
            ),
        }

    # ---------------------------------------------------------------- reads ---
    #
    # The data surface the sync service consumes. Until these existed, Project
    # 03 pointed its adapter at the simulator directly and everything this
    # component provides - bounded admission, the dedicated worker that
    # isolates a blocking call, worker epochs, late-response fencing, the
    # SUSPECT session state - was bypassed (contract gap CG-12). Project 03
    # already requested exactly these four paths, so the fix is to serve them
    # rather than to change the consumer.
    #
    # Every read goes through ``connector.read_page``, which is the whole
    # point: the isolation lives in that call, not in these handlers. A handler
    # that reached for a driver directly would reintroduce the problem.

    @app.get("/v1/meta", summary="Source identity and negotiated capabilities")
    async def meta() -> Any:
        """The shape the sync service reads before a scan.

        ``snapshot_seq`` is obtained by reading one page rather than being
        cached or defaulted. The sync service requires an integer here and
        falls back to ``0`` when the field is absent, so an honest refusal is
        better than either: a reported ``0`` would claim a cut at the beginning
        of time, and a stale cached value would let a scan believe it had a
        newer cut than the pages it is about to read.
        """
        identity = connector.supervisor.identity
        capabilities = connector.supervisor.capabilities
        if identity is None or capabilities is None:
            return JSONResponse(
                status_code=503,
                content=_problem(
                    "SOURCE_UNAVAILABLE",
                    "no source session is established; the connector has not "
                    "completed a handshake",
                ),
            )

        # One page, one item. A read, not an advance: snapshot_seq is the
        # source's event-log upper bound and observing it does not move it.
        try:
            probe = await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=1))
        except ConnectorError as exc:
            return JSONResponse(
                status_code=_READ_STATUS.get(getattr(exc, "code", ""), 502),
                content=_problem(
                    getattr(exc, "code", "INTERNAL"),
                    f"cannot establish the source cut: {exc}",
                ),
            )
        if not isinstance(probe.snapshot_seq, int):
            return JSONResponse(
                status_code=502,
                content=_problem(
                    "SOURCE_CONTRACT_ERROR",
                    "the source did not report an integer snapshot_seq, so this "
                    "connector cannot describe a cut",
                ),
            )

        return {
            "run_id": identity.run_id,
            "generation": identity.generation,
            "snapshot_seq": probe.snapshot_seq,
            "worker_epoch": identity.worker_epoch,
            "source_id": identity.source_id,
            "source_kind": identity.source_kind,
            "contract_version": identity.contract_version,
            "capabilities": {
                "supported_entities": sorted(
                    entity.value for entity in capabilities.supported_entities
                ),
                "snapshot_cursor": capabilities.snapshot_cursor,
                "incremental_deals_after_seq": capabilities.incremental_deals_after_seq,
                # Reported as declared, never as hoped. The sync service refuses
                # a cross-entity claim without it.
                "stable_snapshot_across_entities": (
                    capabilities.stable_snapshot_across_entities
                ),
                "max_page_size": capabilities.max_page_size,
            },
        }

    async def _serve_page(
        entity: Entity,
        *,
        limit: int,
        cursor: str | None,
        after_seq: int | None = None,
    ) -> Any:
        plan = ReadPlan(entity=entity, limit=limit, cursor=cursor, after_seq=after_seq)
        try:
            page = await connector.read_page(plan)
        except ConnectorError as exc:
            # The connector's taxonomy is preserved rather than flattened into
            # a 500. A consumer that cannot tell a timeout from a schema error
            # cannot decide whether retrying is safe.
            return JSONResponse(
                status_code=_READ_STATUS.get(getattr(exc, "code", ""), 502),
                content=_problem(getattr(exc, "code", "INTERNAL"), str(exc)),
            )
        return _page_body(page)

    @app.get("/v1/accounts", summary="One page of accounts")
    async def accounts(limit: int = 100, cursor: str | None = None) -> Any:
        return await _serve_page(Entity.ACCOUNTS, limit=limit, cursor=cursor)

    @app.get("/v1/positions", summary="One page of positions")
    async def positions(limit: int = 100, cursor: str | None = None) -> Any:
        return await _serve_page(Entity.POSITIONS, limit=limit, cursor=cursor)

    @app.get("/v1/deals", summary="One page of deals")
    async def deals(
        limit: int = 100, cursor: str | None = None, after_seq: int | None = None
    ) -> Any:
        return await _serve_page(
            Entity.DEALS, limit=limit, cursor=cursor, after_seq=after_seq
        )

    return app


#: Transport-level mapping for the read endpoints. Anything unlisted becomes a
#: 502: the connector could not complete the read and the caller should not
#: have to guess whether that was its own fault.
_READ_STATUS: dict[str, int] = {
    "SOURCE_UNAVAILABLE": 503,
    "SOURCE_TIMEOUT": 504,
    "ADMISSION_REJECTED": 429,
    "DEADLINE_EXCEEDED": 504,
    "VALIDATION_FAILED": 502,
    "SCHEMA_ERROR": 502,
    "UNAUTHORIZED": 502,
}

_RETRYABLE = frozenset(
    {"SOURCE_UNAVAILABLE", "SOURCE_TIMEOUT", "ADMISSION_REJECTED", "DEADLINE_EXCEEDED"}
)


def _problem(code: str, message: str) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "retryable": code in _RETRYABLE}}


def _page_body(page: Any) -> dict[str, Any]:
    """Project a validated page into the wire shape the sync service parses.

    ``next_cursor`` is always present, even when null. The sync service reads
    it with a membership test on purpose: a body missing the key is a schema
    error, while an explicit null is a terminal page, and collapsing the two
    would let a truncated response end a scan and silently skip the rest.
    """
    return {
        "run_id": page.identity.run_id,
        "generation": page.identity.generation,
        "snapshot_seq": page.snapshot_seq,
        "items": [_encode(item) for item in page.items],
        "next_cursor": page.next_cursor,
        # Deliberately NOT sent as `filter_scope`. That key is the sync
        # service's own scope vocabulary and it compares the value against the
        # scope it asked for; putting this connector's provenance digest there
        # looks like a filtered scan and is rejected as FILTER_SCOPE_VIOLATION.
        # Omitting it lets the consumer apply its own default for an unfiltered
        # read, which is what this is, while the digest stays available for
        # tracing under a name that means what it says.
        "filter_digest": page.provenance.filter_digest,
        "worker_epoch": page.identity.worker_epoch,
        "page_index": page.page_index,
    }


def _encode(item: Any) -> Any:
    """Serialise one DTO without losing exactness.

    Two conversions matter and both are easy to get wrong:

    * ``Decimal`` becomes a plain decimal string. Not a JSON number - that
      would arrive as a float on the far side and import binary rounding error
      into money. Not ``str(value)`` either, because ``Decimal("1E+2")``
      renders as ``"1E+2"`` and no parser downstream of here accepts exponents.
    * ``datetime`` becomes ISO-8601 with an explicit UTC offset. A naive
      timestamp is a timestamp that is wrong somewhere, and the sync service
      refuses one outright, so raising here is better than shipping it.
    """
    if is_dataclass(item) and not isinstance(item, type):
        return {f.name: _encode(getattr(item, f.name)) for f in fields(item)}
    if isinstance(item, Decimal):
        return format(item, "f")
    if isinstance(item, datetime):
        if item.tzinfo is None:
            raise ValueError("refusing to serialise a naive datetime")
        return item.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(item, (list, tuple)):
        return [_encode(entry) for entry in item]
    if isinstance(item, dict):
        return {str(key): _encode(value) for key, value in item.items()}
    return item
