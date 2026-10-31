"""Application context and shared FastAPI dependencies.

Everything long-lived hangs off one :class:`AppContext` stored on
``app.state``. That keeps the app factory testable - a test builds a context
against a temporary database and gets a fully independent application, with no
module-level singletons to reset between cases.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from fastapi import Request

from ..canonical import canonical_json
from ..domain.models import PayloadTooLargeError, ValidationError
from ..faults.matcher import FaultEngine
from ..generation.coordinator import ScenarioCoordinator
from ..observability.metrics import Metrics, NullMetrics
from ..persistence.connection import ProcessLock, ReadConnectionPool
from ..persistence.cursors import CursorCodec, SnapshotRegistry
from ..persistence.failpoints import FailpointController
from ..persistence.read_repository import ReadRepository
from ..persistence.writer import WriterService
from ..security.auth import Authenticator, Plane, Principal
from ..security.rate_limit import AdmissionController, RateLimiter
from ..settings import Settings


@dataclass(slots=True)
class AppContext:
    settings: Settings
    metrics: Metrics | NullMetrics
    pool: ReadConnectionPool
    writer: WriterService
    coordinator: ScenarioCoordinator
    repository: ReadRepository
    codec: CursorCodec
    snapshots: SnapshotRegistry
    faults: FaultEngine
    authenticator: Authenticator
    rate_limiter: RateLimiter
    admission: AdmissionController
    failpoints: FailpointController
    schema_version: int
    started_at: datetime
    process_lock: ProcessLock | None = None


def get_context(request: Request) -> AppContext:
    return request.app.state.context


async def require_data_plane(request: Request) -> Principal:
    return _authorize(request, Plane.DATA)


async def require_admin_plane(request: Request) -> Principal:
    return _authorize(request, Plane.ADMIN)


def _authorize(request: Request, plane: Plane) -> Principal:
    context = get_context(request)
    principal = context.authenticator.authenticate(
        request.headers.get("authorization"), required=plane
    )
    # Rate limiting is per principal, which is only knowable after
    # authentication. Unauthenticated floods are bounded by the body-size cap
    # and by admission control, both of which run before this.
    context.rate_limiter.check(f"{principal.plane.value}:{principal.token_id}")
    request.state.principal = principal
    return principal


def resolve_limit(context: AppContext, limit: int | None) -> int:
    """Clamp-free limit resolution: an over-limit request is an error.

    Silently clamping ``limit=100000000`` to 500 would let a client believe it
    had received everything (WF-040, WF-091).
    """
    if limit is None:
        return context.settings.default_page_size
    if limit < 1 or limit > context.settings.max_page_size:
        raise ValidationError(
            "limit is outside the permitted range",
            details={
                "field": "limit",
                "minimum": 1,
                "maximum": context.settings.max_page_size,
            },
        )
    return limit


def require_idempotency_key(request: Request) -> str:
    """Mandatory for control-plane writes (section 5.2).

    Required rather than optional because the operation it guards is exactly
    the one whose response can be lost: without a key, a retry after a dropped
    response would create a second run or a second event (WF-053).
    """
    key = request.headers.get("idempotency-key")
    if not key or not key.strip():
        raise ValidationError(
            "the Idempotency-Key header is required for this operation",
            details={"header": "Idempotency-Key"},
        )
    key = key.strip()
    if len(key) > 128:
        raise ValidationError(
            "Idempotency-Key must not exceed 128 characters",
            details={"header": "Idempotency-Key", "maximum": 128},
        )
    return key


def optional_idempotency_key(request: Request) -> str | None:
    key = request.headers.get("idempotency-key")
    if key is None:
        return None
    return require_idempotency_key(request)


def request_digest(route: str, payload: Any) -> str:
    """Digest binding an idempotency key to the request it was used with.

    The route is part of the input so the same key cannot be reused across two
    different operations and be mistaken for a replay.
    """
    return hashlib.sha256(
        canonical_json({"route": route, "payload": payload}).encode("utf-8")
    ).hexdigest()


def enforce_body_size(request: Request, context: AppContext) -> None:
    declared = request.headers.get("content-length")
    if declared is None:
        return
    try:
        length = int(declared)
    except ValueError as exc:
        raise ValidationError("Content-Length is not an integer") from exc
    if length > context.settings.max_request_body_bytes:
        raise PayloadTooLargeError(
            "request body exceeds the permitted size",
            details={"maximum_bytes": context.settings.max_request_body_bytes},
        )
