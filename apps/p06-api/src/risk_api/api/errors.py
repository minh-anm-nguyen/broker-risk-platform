"""Error rendering and the response header policy.

Every refusal leaves through here, so the taxonomy, the media type and the
privacy headers are applied in exactly one place. A handler that formatted its
own error would eventually forget ``no-store``.

Nothing in a problem body is derived from an exception message that the service
did not write itself: a stack trace, a SQL fragment or a driver string never
reaches a client.
"""

from __future__ import annotations

import json
from typing import Any, Mapping

from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import Response
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..contracts.problem_details import ApiProblem, problem_body
from ..observability.logging import log_event
from ..repositories.audit_store import AuditUnavailable
from ..repositories.read_views import DatabaseUnavailable

__all__ = [
    "PROBLEM_MEDIA_TYPE",
    "sensitive_headers",
    "problem_response",
    "json_response",
    "register_exception_handlers",
    "request_id_of",
]

PROBLEM_MEDIA_TYPE = "application/problem+json"

# Starlette status codes we map onto the documented taxonomy.
_STATUS_TO_CODE: Mapping[int, str] = {
    401: "UNAUTHENTICATED",
    403: "ACCESS_DENIED",
    404: "RESOURCE_NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    406: "NOT_ACCEPTABLE",
    413: "REQUEST_TOO_LARGE",
    414: "URI_TOO_LONG",
    415: "UNSUPPORTED_MEDIA_TYPE",
    422: "INVALID_REQUEST",
    429: "RATE_LIMITED",
    500: "INTERNAL_ERROR",
    503: "NOT_READY",
}


def request_id_of(request: Request) -> str:
    """The server-generated request id for this request.

    Always server-generated: a client cannot forge correlation by sending its
    own ``X-Request-ID`` unless a trusted proxy header is explicitly
    configured.
    """
    return getattr(request.state, "request_id", "req-unknown")


def sensitive_headers(request_id: str, *, extra: Mapping[str, str] | None = None) -> dict[str, str]:
    """Headers applied to every financial success, partial and error body."""
    headers = {
        "Cache-Control": "private, no-store",
        "Pragma": "no-cache",
        "X-Content-Type-Options": "nosniff",
        "X-Request-ID": request_id,
        "Vary": "Authorization",
        "Referrer-Policy": "no-referrer",
    }
    if extra:
        headers.update(extra)
    return headers


def problem_response(problem: ApiProblem, request_id: str) -> Response:
    """Render an ``ApiProblem`` as RFC 9457."""
    body = problem_body(problem, request_id)
    headers = sensitive_headers(request_id, extra=problem.headers)
    if problem.code == "UNAUTHENTICATED":
        headers.setdefault("WWW-Authenticate", 'Bearer realm="risk-reporting-api"')
    if problem.code == "METHOD_NOT_ALLOWED" and "Allow" not in headers:
        headers["Allow"] = "GET, POST, HEAD, OPTIONS"
    return Response(
        content=json.dumps(body, separators=(",", ":"), sort_keys=True),
        status_code=problem.status,
        media_type=PROBLEM_MEDIA_TYPE,
        headers=headers,
    )


def json_response(payload: Any, *, status_code: int, request_id: str) -> Response:
    """Render a success body with the sensitive header set."""
    return Response(
        content=json.dumps(payload, separators=(",", ":"), sort_keys=True, allow_nan=False),
        status_code=status_code,
        media_type="application/json",
        headers=sensitive_headers(request_id),
    )


def register_exception_handlers(app: Any) -> None:
    """Install handlers so no exception escapes as an untyped 500."""

    @app.exception_handler(ApiProblem)
    async def _api_problem(request: Request, exc: ApiProblem) -> Response:
        request_id = request_id_of(request)
        context = getattr(request.app.state, "context", None)
        if context is not None:
            _record_denial(context, request, exc)
        return problem_response(exc, request_id)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> Response:
        # Pydantic detail can quote submitted values; summarize instead.
        fields = sorted({".".join(str(p) for p in err.get("loc", ())[1:]) for err in exc.errors()})
        fields = [field for field in fields if field]
        detail = "the request failed schema validation"
        if fields:
            detail = f"{detail}: {', '.join(fields[:8])}"
        return problem_response(ApiProblem("INVALID_REQUEST", detail), request_id_of(request))

    @app.exception_handler(StarletteHTTPException)
    async def _http(request: Request, exc: StarletteHTTPException) -> Response:
        code = _STATUS_TO_CODE.get(exc.status_code, "INTERNAL_ERROR")
        problem = ApiProblem(code, _safe_http_detail(exc), status_override=exc.status_code)
        headers: dict[str, str] = {}
        if exc.status_code == 405 and getattr(exc, "headers", None):
            allow = (exc.headers or {}).get("Allow")
            if allow:
                headers["Allow"] = allow
        problem.headers.update(headers)
        return problem_response(problem, request_id_of(request))

    @app.exception_handler(DatabaseUnavailable)
    async def _db_unavailable(request: Request, exc: DatabaseUnavailable) -> Response:
        problem = ApiProblem(
            "DB_UNAVAILABLE",
            "the projection store is unavailable",
            reason_codes=("DB_UNAVAILABLE",),
        )
        return problem_response(problem, request_id_of(request))

    @app.exception_handler(AuditUnavailable)
    async def _audit_unavailable(request: Request, exc: AuditUnavailable) -> Response:
        problem = ApiProblem(
            "AUDIT_UNAVAILABLE",
            "the mandatory access audit sink is unavailable",
            reason_codes=("AUDIT_PERSISTENCE_FAILED",),
        )
        return problem_response(problem, request_id_of(request))

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception) -> Response:
        request_id = request_id_of(request)
        logger = getattr(request.app.state, "logger", None)
        if logger is not None:
            import logging

            log_event(
                logger,
                logging.ERROR,
                "unhandled_exception",
                request_id=request_id,
                route=_route_template(request),
                exception_type=type(exc).__name__,
            )
        # Redacted incident, and definitively no financial payload.
        return problem_response(
            ApiProblem("INTERNAL_ERROR", "an unexpected internal error occurred"), request_id
        )


def _safe_http_detail(exc: StarletteHTTPException) -> str:
    detail = exc.detail if isinstance(exc.detail, str) else ""
    if not detail or len(detail) > 200:
        return "the request could not be served"
    lowered = detail.lower()
    if any(token in lowered for token in ("select ", "traceback", "psycopg", "asyncpg", "/usr/")):
        return "the request could not be served"
    return detail


def _route_template(request: Request) -> str:
    route = request.scope.get("route")
    path = getattr(route, "path", None)
    return path or "unmatched"


def _record_denial(context: Any, request: Request, problem: ApiProblem) -> None:
    """Count a denial for the BOLA-probing signal, with a coarse family label."""
    metrics = getattr(context, "metrics", None)
    if metrics is None:
        return
    if problem.status not in (401, 403, 404):
        return
    family = problem.reason_codes[0].lower() if problem.reason_codes else problem.code.lower()
    try:
        metrics.observe_denial(denial_family=family, route=_route_template(request))
    except Exception:  # pragma: no cover - metrics must never break a response
        return
