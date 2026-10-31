"""The error contract and its exception handlers (SYSTEM_DESIGN section 5.3).

``code`` is the stable part of this contract. A connector branches on it to
decide whether to retry, re-sync or give up, so the mapping below is an API
surface: renaming a code is a breaking change.

The distinctions that actually drive client behaviour:

* **400 vs 410.** ``INVALID_CURSOR`` means "you constructed this wrong, fix the
  request"; ``SNAPSHOT_EXPIRED`` / ``RUN_REPLACED`` mean "this snapshot is gone,
  start a new sync". A client that confuses them either retries forever or
  resyncs unnecessarily.
* **409 vs 422.** A conflict is about state that may change
  (``IDEMPOTENCY_CONFLICT``); an unprocessable entity is about a request that
  will never be accepted as written.
* **503 and ``retryable``.** A storage error says nothing about whether the
  write landed. That is what the idempotency key is for, and the error body
  says so rather than implying failure.

One deliberate exception: the ``malformed_json`` fault does *not* follow this
contract. That is the point of it - it exists so a downstream parser can be
tested against a response that is not well formed (section 5.3).
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from ..domain.models import SimulatorError
from ..observability.logging import current_request_id, get_logger

logger = get_logger("api.errors")

#: Code to HTTP status. Codes absent here fall back to 500, which is the right
#: default: an unmapped code means an error path nobody classified.
CODE_TO_STATUS: dict[str, int] = {
    "INVALID_CURSOR": 400,
    "INVALID_FILTER": 400,
    "UNAUTHORIZED": 401,
    "FORBIDDEN": 403,
    "NOT_FOUND": 404,
    "RUN_CONFLICT": 409,
    "IDEMPOTENCY_CONFLICT": 409,
    "ACCOUNT_ALREADY_EXISTS": 409,
    "POSITION_ALREADY_OPEN": 409,
    "SNAPSHOT_EXPIRED": 410,
    "RUN_REPLACED": 410,
    "PAYLOAD_TOO_LARGE": 413,
    "VALIDATION_FAILED": 422,
    "UNSUPPORTED_OPERATION": 422,
    "CLOSE_VOLUME_EXCEEDS_POSITION": 422,
    "OPEN_POSITION_LIMIT_REACHED": 422,
    "DEAL_LIMIT_REACHED": 422,
    "RATE_LIMITED": 429,
    "TOO_MANY_JOBS": 429,
    "SNAPSHOT_CAPACITY_REACHED": 429,
    "INTERNAL_ERROR": 500,
    "SERVICE_NOT_READY": 503,
    "WRITER_OVERLOADED": 503,
    "STORAGE_ERROR": 503,
    "SIMULATED_UPSTREAM_TIMEOUT": 504,
}

#: Published in the OpenAPI schema so a connector can enumerate what it must
#: handle instead of discovering codes in production.
ERROR_CODES: tuple[str, ...] = tuple(sorted(CODE_TO_STATUS))

_RETRY_AFTER_STATUSES = frozenset({429, 503})


def problem_payload(
    *,
    code: str,
    message: str,
    request_id: str | None = None,
    run_id: str | None = None,
    retryable: bool = False,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "retryable": retryable,
            "request_id": request_id,
            "run_id": run_id,
            "details": details or {},
        }
    }


def error_response(
    *,
    code: str,
    message: str,
    status_code: int | None = None,
    request_id: str | None = None,
    run_id: str | None = None,
    retryable: bool = False,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    status = status_code if status_code is not None else CODE_TO_STATUS.get(code, 500)
    payload = problem_payload(
        code=code,
        message=message,
        request_id=request_id,
        run_id=run_id,
        retryable=retryable,
        details=details,
    )
    headers = {"Cache-Control": "no-store"}
    retry_after = (details or {}).get("retry_after_seconds")
    if status in _RETRY_AFTER_STATUSES:
        # RFC 9110: give the client a concrete number rather than make it guess
        # a backoff that may well be wrong in both directions.
        headers["Retry-After"] = str(int(retry_after) if retry_after is not None else 1)
    return JSONResponse(status_code=status, content=payload, headers=headers)


def install_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(SimulatorError)
    async def _handle_simulator_error(request: Request, exc: SimulatorError) -> JSONResponse:
        status = CODE_TO_STATUS.get(exc.code, 500)
        if status >= 500:
            logger.error(
                "request failed", extra={"error_code": exc.code, "route": request.url.path}
            )
        return error_response(
            code=exc.code,
            message=exc.message,
            request_id=current_request_id(),
            run_id=getattr(request.state, "run_id", None),
            retryable=exc.retryable,
            details=exc.details,
        )

    @app.exception_handler(RequestValidationError)
    async def _handle_validation_error(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        # Pydantic's own structure is reshaped into our envelope so a client
        # has exactly one error format to parse.
        fields = [
            {
                "location": ".".join(str(part) for part in error.get("loc", ())),
                "message": error.get("msg", ""),
                "type": error.get("type", ""),
            }
            for error in exc.errors()[:20]
        ]
        return error_response(
            code="VALIDATION_FAILED",
            message="request validation failed",
            request_id=current_request_id(),
            run_id=getattr(request.state, "run_id", None),
            details={"fields": fields},
        )

    @app.exception_handler(StarletteHTTPException)
    async def _handle_http_exception(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = {
            401: "UNAUTHORIZED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            405: "VALIDATION_FAILED",
            413: "PAYLOAD_TOO_LARGE",
            429: "RATE_LIMITED",
        }.get(exc.status_code, "INTERNAL_ERROR" if exc.status_code >= 500 else "VALIDATION_FAILED")
        return error_response(
            code=code,
            message=str(exc.detail),
            status_code=exc.status_code,
            request_id=current_request_id(),
            run_id=getattr(request.state, "run_id", None),
            retryable=exc.status_code in _RETRY_AFTER_STATUSES,
        )

    @app.exception_handler(Exception)
    async def _handle_unexpected(request: Request, exc: Exception) -> JSONResponse:
        # An unexpected exception is a bug. It is logged with its type and
        # message but the response carries neither, so a stack trace or a
        # fragment of a query can never leak through the API.
        logger.error(
            "unhandled exception", extra={"route": request.url.path}, exc_info=exc
        )
        return error_response(
            code="INTERNAL_ERROR",
            message="an unexpected error occurred",
            request_id=current_request_id(),
            run_id=getattr(request.state, "run_id", None),
        )
