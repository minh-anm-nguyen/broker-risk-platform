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
from typing import Any, AsyncIterator

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from . import CONTRACT_VERSION, __version__
from .client import Connector
from .errors import ERROR_CODES, ConnectorError
from .ports import Entity

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

    return app
