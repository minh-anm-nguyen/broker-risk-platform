"""Application factory.

``create_app`` takes an already-assembled context, which is what lets the test
suite build a differently-faulted instance without monkeypatching the module.

The middleware does three things in order: assign a server-generated request
id, time the request, and record it under a *route template* label. It also
applies the sensitive header set to responses that did not come from the
helpers, so a header policy violation is impossible to introduce by adding a
route.
"""

from __future__ import annotations

import time

from fastapi import FastAPI, Request
from fastapi.responses import Response

from .api.deps import AppContext, new_request_id
from .api.errors import register_exception_handlers, sensitive_headers
from .api.health import health_router, internal_router
from .api.v1 import accounts, calculations, exposure, positions, reports
from .lifespan import build_context, lifespan
from .observability.metrics import LabelPolicyError
from .settings import Settings, get_settings

__all__ = ["create_app", "asgi_app"]

API_TITLE = "Broker Risk Reporting API"
API_VERSION = "1.0.0"


def create_app(*, context: AppContext | None = None, settings: Settings | None = None) -> FastAPI:
    """Build the ASGI application around a context."""
    resolved_settings = settings or (context.settings if context else get_settings())
    application = FastAPI(
        title=API_TITLE,
        version=API_VERSION,
        description=(
            "Read-only broker risk reporting. Monetary values are JSON strings; every "
            "financial payload carries its data cut, policy versions and digests."
        ),
        lifespan=lifespan,
        openapi_url="/openapi.json",
        docs_url=None,
        redoc_url=None,
    )
    application.state.context = context or build_context(settings=resolved_settings)
    application.state.logger = application.state.context.logger

    register_exception_handlers(application)
    application.include_router(health_router)
    application.include_router(internal_router)
    base = resolved_settings.api_base_path
    application.include_router(accounts.router, prefix=base)
    application.include_router(positions.router, prefix=base)
    application.include_router(exposure.router, prefix=base)
    application.include_router(reports.router, prefix=base)
    application.include_router(calculations.router, prefix=base)

    @application.middleware("http")
    async def _observe(request: Request, call_next) -> Response:
        context_obj: AppContext = request.app.state.context
        request.state.request_id = _assign_request_id(request, context_obj)
        started = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - started

        for header, value in sensitive_headers(request.state.request_id).items():
            response.headers.setdefault(header, value)

        try:
            context_obj.metrics.observe_request(
                route=_route_template(request),
                method=request.method,
                status=response.status_code,
                duration_seconds=elapsed,
            )
            context_obj.metrics.set_gauges(
                inflight=context_obj.admission.stats.inflight,
                heavy_queue_depth=context_obj.admission.stats.queued_heavy,
                heartbeat_p99=context_obj.workers.heartbeat.p99_lag_seconds,
            )
            context_obj.metrics.add_worker_cpu(0.0)
        except LabelPolicyError:
            # A label policy breach must not break the response; it is a bug
            # in instrumentation, surfaced by the metrics tests.
            pass
        return response

    return application


def _assign_request_id(request: Request, context: AppContext) -> str:
    """Server-generated, unless a trusted proxy header is explicitly configured."""
    trusted_header = context.settings.trusted_proxy_request_id_header
    if trusted_header:
        supplied = request.headers.get(trusted_header)
        if supplied and _is_safe_request_id(supplied):
            return supplied
    return new_request_id()


def _is_safe_request_id(value: str) -> bool:
    return 8 <= len(value) <= 64 and all(ch.isalnum() or ch in "-_" for ch in value)


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    if not path:
        return "unmatched"
    return path.lower()



def asgi_app() -> FastAPI:
    """Entrypoint for ``uvicorn risk_api.main:asgi_app --factory``."""
    return create_app()
