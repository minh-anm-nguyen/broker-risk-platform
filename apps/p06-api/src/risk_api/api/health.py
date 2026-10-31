"""Health and internal diagnostics.

``/health/live`` answers from the event loop alone. It must not touch the
database: a transient projection-store outage that restarts every replica turns
a degraded service into no service.

``/health/ready`` runs bounded checks on the things a request genuinely needs,
and reports coarse reasons only. A blocking finding for one broker degrades
that broker's reports; it does not take the instance out of rotation.

``/internal/metrics`` is mounted on a separate router and is not part of the
public ``/api/v1`` surface. In a deployment it binds to a restricted listener;
here it additionally requires the ops permission.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import Response

from ..authz.principal_context import Permission
from ..authz.scope_policy import require_permission
from ..contracts.problem_details import ApiProblem
from .deps import authenticate, get_context
from .errors import json_response, request_id_of, sensitive_headers

__all__ = ["health_router", "internal_router"]

health_router = APIRouter(tags=["health"])
internal_router = APIRouter(tags=["internal"])


@health_router.get("/health/live", summary="Process liveness")
async def live(request: Request) -> Response:
    """True whenever the event loop is servicing requests."""
    context = get_context(request)
    return json_response(
        {"status": "live", "service": context.settings.service_name},
        status_code=200,
        request_id=request_id_of(request),
    )


@health_router.get("/health/ready", summary="Instance readiness")
async def ready(request: Request) -> Response:
    """Bounded checks on credentials, projections, audit sink and capacity."""
    context = get_context(request)
    checks: dict[str, bool] = {}

    checks["identity_configured"] = bool(context.verifier.key_ids)
    checks["read_views_compatible"] = context.views.capability().is_compatible()
    try:
        checks["projection_store"] = await context.views.health()
    except Exception:
        checks["projection_store"] = False
    try:
        checks["audit_sink"] = await context.audit.health()
    except Exception:
        checks["audit_sink"] = False
    checks["admission"] = context.admission.healthy()
    checks["workers"] = context.workers.healthy()

    ready_now = all(checks.values())
    body = {
        "status": "ready" if ready_now else "not_ready",
        # Coarse names only: no secrets, no hostnames, no connection strings.
        "checks": {name: ("ok" if value else "failed") for name, value in sorted(checks.items())},
    }
    return json_response(
        body, status_code=200 if ready_now else 503, request_id=request_id_of(request)
    )


@internal_router.get("/internal/metrics", summary="Prometheus metrics")
async def metrics(request: Request) -> Response:
    """Metrics scrape. Not publicly routable in a deployment."""
    context = get_context(request)
    if not context.settings.expose_internal_metrics:
        # Hide existence rather than advertising a disabled internal route.
        raise ApiProblem("RESOURCE_NOT_FOUND", "no such resource")

    principal = authenticate(request, context)
    require_permission(
        principal, Permission.READ_INTERNAL_METRICS, denial_family="METRICS_NOT_PERMITTED"
    )

    context.metrics.set_gauges(
        inflight=context.admission.stats.inflight,
        heavy_queue_depth=context.admission.stats.queued_heavy,
        heartbeat_p99=context.workers.heartbeat.p99_lag_seconds,
    )
    return Response(
        content=context.metrics.render(),
        status_code=200,
        media_type="text/plain; version=0.0.4; charset=utf-8",
        headers=sensitive_headers(request_id_of(request)),
    )
