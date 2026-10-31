"""Application factory, middleware stack and lifespan.

The middleware order is a design decision, not an accident:

1. **Observability (outermost).** Assigns the request id, caps the request
   body, applies admission control and records the *final* status. It must be
   outermost so that an injected fault is still counted as the status the
   client actually received.
2. **Fault injection.** Inside observability, outside routing, so it can
   replace or mutate a handler's response without the handler knowing - and so
   it can never touch the database.
3. **Routing**, with authentication and rate limiting as route dependencies.

Everything long-lived is built in the lifespan and stored on ``app.state`` as
one context object. No module-level singletons: a test can build fifty
independent applications against fifty temporary databases in one process.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from . import GENERATOR_VERSION, __version__
from .api import admin as admin_api
from .api import data as data_api
from .api import health as health_api
from .api.deps import AppContext
from .api.errors import ERROR_CODES, error_response, install_exception_handlers
from .domain.models import PayloadTooLargeError, ServiceNotReadyError, SimulatorError
from .faults import rules as fault_rules
from .faults.matcher import FaultEngine
from .faults.middleware import FaultInjectionMiddleware
from .generation.coordinator import ScenarioCoordinator
from .observability.logging import (
    bind_request_id,
    bind_run_id,
    configure_logging,
    get_logger,
)
from .observability.metrics import Metrics, NullMetrics
from .persistence.connection import ProcessLock, ReadConnectionPool, initialize_database
from .persistence.cursors import CursorCodec, SnapshotRegistry
from .persistence.failpoints import controller_from_settings
from .persistence.read_repository import ReadRepository
from .persistence.writer import WriterService
from .security.auth import Authenticator
from .security.rate_limit import AdmissionController, RateLimiter
from .settings import Settings, assert_single_worker_configuration

logger = get_logger("app")

_DESCRIPTION = f"""
Deterministic synthetic broker data source for the *Broker Risk Reporting and
MT5 Integration* learning roadmap (mini-project 01 of 09).

**This is not the MT5 Manager API.** The REST surface here is a simulated test
contract. `run_id`, `generation`, `snapshot_seq` and `source_seq` are
simulator semantics and carry no claim that a real MT5 Manager SDK exposes
anything equivalent. Project 02's adapter is what hides the difference.

Conventions:

* every money, price and volume value is a JSON **string** holding a canonical
  decimal; floats are rejected on input
* timestamps are RFC 3339 UTC; business time comes from the scenario's virtual
  clock, not the host clock
* `after_seq` is **exclusive**; time ranges are `[from_time, to_time)`
* `next_cursor` is opaque and signed; `null` means this snapshot has no further
  pages, not that no more data will ever exist
* error codes: {", ".join(ERROR_CODES)}

Generator version `{GENERATOR_VERSION}`: a seed only reproduces a dataset
within one generator version.
""".strip()

#: Paths subject to admission control and data-plane metrics.
_DATA_PREFIX = "/v1"
#: Paths exempt from admission control. Liveness must answer while every data
#: slot is taken, and the control plane must stay reachable to disarm faults.
_EXEMPT_PREFIXES = ("/health", "/admin")


def _route_label(scope: dict[str, Any]) -> str:
    """Low-cardinality metric label.

    The route *template* where the router resolved one, otherwise a coarse
    prefix. Never the resolved URL: one time series per account id would take
    a metrics backend down (section 11.1).
    """
    route = scope.get("route")
    path_format = getattr(route, "path", None)
    if isinstance(path_format, str) and path_format:
        return path_format
    path = scope.get("path", "/")
    parts = [part for part in path.split("/") if part][:2]
    return "/" + "/".join(parts) if parts else "/"


class ObservabilityMiddleware:
    """Request id, body cap, admission control and request metrics."""

    def __init__(self, app: Any, *, context_getter: Any) -> None:
        self.app = app
        self._context_getter = context_getter

    async def __call__(self, scope: dict[str, Any], receive: Any, send: Any) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        context: AppContext = self._context_getter()
        path: str = scope.get("path", "")
        request_id = self._request_id(scope)
        bind_request_id(request_id)
        bind_run_id(None)
        scope.setdefault("state", {})["request_id"] = request_id

        plane = "admin" if path.startswith("/admin") else ("data" if path.startswith(_DATA_PREFIX) else "health")
        started = time.perf_counter()
        status_holder = {"status": 500}

        async def send_wrapper(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_holder["status"] = message["status"]
                headers = list(message.get("headers") or [])
                headers.append((b"x-request-id", request_id.encode("ascii")))
                if not any(key.lower() == b"cache-control" for key, _ in headers):
                    # Synthetic data, but a cached financial payload is a trap
                    # for any consumer that assumes freshness.
                    headers.append((b"cache-control", b"no-store"))
                message = {**message, "headers": headers}
            await send(message)

        try:
            guarded_receive = await self._bounded_receive(scope, receive, context)
            if path.startswith(_EXEMPT_PREFIXES) or not path.startswith(_DATA_PREFIX):
                await self.app(scope, guarded_receive, send_wrapper)
            else:
                async with context.admission.slot():
                    context.metrics.requests_in_flight.labels(plane=plane).set(
                        context.admission.in_flight
                    )
                    await self.app(scope, guarded_receive, send_wrapper)
        except SimulatorError as exc:
            # Raised by admission control or the body cap, i.e. before the
            # route's exception handlers exist to catch it.
            reason = "admission" if isinstance(exc, ServiceNotReadyError) else "body_size"
            context.metrics.requests_rejected.labels(reason=reason, plane=plane).inc()
            response = error_response(
                code=exc.code,
                message=exc.message,
                request_id=request_id,
                retryable=exc.retryable,
                details=exc.details,
            )
            await response(scope, receive, send_wrapper)
        finally:
            duration = time.perf_counter() - started
            label = _route_label(scope)
            context.metrics.http_duration.labels(
                route=label, method=scope.get("method", "GET")
            ).observe(duration)
            context.metrics.http_requests.labels(
                route=label, status=str(status_holder["status"]), plane=plane
            ).inc()
            context.metrics.requests_in_flight.labels(plane=plane).set(
                context.admission.in_flight
            )
            bind_request_id(None)

    @staticmethod
    def _request_id(scope: dict[str, Any]) -> str:
        for key, value in scope.get("headers") or ():
            if key.lower() == b"x-request-id":
                candidate = value.decode("latin-1")[:64].strip()
                # Echoed when supplied so a caller can correlate, but bounded
                # and never trusted for anything but correlation.
                if candidate:
                    return candidate
        return f"req-{uuid.uuid4().hex[:16]}"

    @staticmethod
    async def _bounded_receive(
        scope: dict[str, Any], receive: Any, context: AppContext
    ) -> Any:
        """Cap the request body and hand the route a replay of it.

        The body is read here rather than lazily in the route for one reason:
        FastAPI wraps *any* exception from ``request.body()`` into a generic
        400, so a limit enforced during the route's read surfaces as "error
        parsing the body" instead of 413. Enforcing it before the route runs
        keeps the error contract honest (WF-091).

        A Content-Length check alone would not be enough either - a chunked
        request declares no length - so the bytes are counted as they arrive
        and reading stops one byte past the limit. Only methods that can carry
        a body are read at all, so a GET pays nothing.

        Buffering is bounded by design: control-plane payloads are capped at a
        few hundred KiB, and the data plane has no request bodies.
        """
        limit = context.settings.max_request_body_bytes
        if scope.get("method", "GET") in {"GET", "HEAD", "OPTIONS"}:
            return receive

        for key, value in scope.get("headers") or ():
            if key.lower() == b"content-length":
                try:
                    declared = int(value)
                except ValueError:
                    continue
                if declared > limit:
                    raise PayloadTooLargeError(
                        "request body exceeds the permitted size",
                        details={"maximum_bytes": limit, "declared_bytes": declared},
                    )

        chunks: list[bytes] = []
        total = 0
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] == "http.disconnect":
                more_body = False
                break
            chunk = message.get("body") or b""
            total += len(chunk)
            if total > limit:
                raise PayloadTooLargeError(
                    "request body exceeds the permitted size",
                    details={"maximum_bytes": limit},
                )
            chunks.append(chunk)
            more_body = bool(message.get("more_body"))

        body = b"".join(chunks)
        delivered = False

        async def replay() -> dict[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        return replay


def create_app(
    settings: Settings | None = None, *, lock_process: bool | None = None
) -> FastAPI:
    active_settings = settings or Settings()
    configure_logging(active_settings.log_level, active_settings.log_format)
    assert_single_worker_configuration()

    should_lock = (
        active_settings.enforce_single_process_lock if lock_process is None else lock_process
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        lock: ProcessLock | None = None
        if should_lock:
            lock = ProcessLock(active_settings.lock_path)
            lock.acquire()
        schema_version = initialize_database(active_settings)
        metrics: Metrics | NullMetrics = (
            Metrics() if active_settings.metrics_enabled else NullMetrics()
        )
        pool = ReadConnectionPool(active_settings)
        failpoints = controller_from_settings(
            active_settings.failpoint,
            active_settings.failpoint_at_seq,
            active_settings.failpoint_mode,
        )
        writer = WriterService(active_settings, metrics=metrics, failpoints=failpoints)
        writer.start()
        coordinator = ScenarioCoordinator(active_settings, writer, pool, metrics=metrics)
        faults = FaultEngine(active_settings, metrics=metrics)
        context = AppContext(
            settings=active_settings,
            metrics=metrics,
            pool=pool,
            writer=writer,
            coordinator=coordinator,
            repository=ReadRepository(pool, active_settings),
            codec=CursorCodec(active_settings.signing_keys()),
            snapshots=SnapshotRegistry(active_settings, metrics=metrics),
            faults=faults,
            authenticator=Authenticator(active_settings),
            rate_limiter=RateLimiter(active_settings),
            admission=AdmissionController(active_settings),
            failpoints=failpoints,
            schema_version=schema_version,
            started_at=datetime.now(UTC),
            process_lock=lock,
        )
        app.state.context = context

        coordinator.startup()
        with pool.acquire(timeout=10.0) as conn:
            faults.load(fault_rules.load_rules(conn))
        run = coordinator.active_run()
        if run is not None:
            faults.set_seed(run.seed)
        logger.info(
            "simulator started",
            extra={
                "schema_version": schema_version,
                "active_run_id": run.run_id if run else None,
                "fault_rules": len(faults.snapshot()),
                "recovery_required": coordinator.recovery_required,
            },
        )
        try:
            yield
        finally:
            coordinator.shutdown()
            # Drain rather than drop: a queued write that is silently discarded
            # would leave a caller believing it succeeded (WF-124).
            writer.stop(drain=True, timeout=10.0)
            pool.close()
            if lock is not None:
                lock.release()
            logger.info("simulator stopped")

    app = FastAPI(
        title="MT5 Broker Simulator",
        version=__version__,
        description=_DESCRIPTION,
        lifespan=lifespan,
        openapi_url="/openapi.json",
        docs_url="/docs",
        redoc_url=None,
    )
    install_exception_handlers(app)

    app.add_middleware(
        FaultInjectionMiddleware,
        engine_getter=lambda: app.state.context.faults,
        settings=active_settings,
        metrics_getter=lambda: app.state.context.metrics,
    )
    app.add_middleware(
        ObservabilityMiddleware, context_getter=lambda: app.state.context
    )

    app.include_router(health_api.router)
    app.include_router(data_api.router)
    app.include_router(admin_api.router)
    return app


app_factory = create_app
