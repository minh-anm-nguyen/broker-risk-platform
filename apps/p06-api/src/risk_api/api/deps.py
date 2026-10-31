"""Request-scoped wiring: identity, scope, admission and the report pipeline.

``run_report`` is the shared spine for the financial endpoints. Each router
supplies its permission, its component set and its workload class; the order of
checks is identical everywhere, which is the only way the "zero engine calls
for an unauthorized request" invariant can hold across five routes.
"""

from __future__ import annotations

import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Sequence

from fastapi import Request

from ..admission.budgets import Deadline, RequestBudget
from ..admission.quotas import PrincipalQuotas
from ..admission.scheduler import AdmissionController, WorkloadClass
from ..app.cut_capture import CutCapturer
from ..app.engine_port import RiskEnginePort
from ..app.pagination import CursorCodec
from ..app.risk_service import ReportEnvelope, ReportRequest, RiskReportService
from ..authn.jwt_verifier import JwtVerifier, TokenError
from ..authz.principal_context import Principal, RequestedFilters, ScopePolicy
from ..authz.scope_policy import ScopePolicyRepository, authorize_scope, require_permission
from ..contracts.problem_details import ApiProblem
from ..contracts.requests import enforce_query_allowlist
from ..contracts.versioning import resolve_policy_versions
from ..execution.cpu_worker import CpuWorkerPool
from ..faults import FaultRegistry
from ..observability.logging import log_event
from ..observability.metrics import Metrics
from ..repositories.audit_store import AuditStore
from ..repositories.read_views import InMemoryReadViews
from ..settings import Settings

__all__ = [
    "AppContext",
    "get_context",
    "new_request_id",
    "authenticate",
    "resolve_policy",
    "run_report",
    "assert_accept_json",
    "assert_json_content_type",
    "EpochView",
]


@dataclass(slots=True)
class AppContext:
    """Everything a request needs, assembled once at startup."""

    settings: Settings
    verifier: JwtVerifier
    policies: ScopePolicyRepository
    views: InMemoryReadViews
    capturer: CutCapturer
    engine: RiskEnginePort
    audit: AuditStore
    metrics: Metrics
    admission: AdmissionController
    quotas: PrincipalQuotas
    cursors: CursorCodec
    service: RiskReportService
    workers: CpuWorkerPool
    faults: FaultRegistry
    logger: logging.Logger
    clock: Callable[[], float]
    wall_clock: Callable[[], int]
    budget: RequestBudget

    def epochs(self) -> "EpochView":
        return EpochView(self.views)


class EpochView:
    """Adapts the read views to the epoch resolver the authorizer expects."""

    def __init__(self, views: InMemoryReadViews) -> None:
        self._views = views

    async def active_epoch(self, source_id: str) -> str | None:
        world = self._views.world
        if world.source_id != source_id:
            return None
        return world.epoch_id if world.epoch_status == "ACTIVE" else None

    async def epoch_status(self, source_id: str, epoch_id: str) -> str | None:
        world = self._views.world
        if world.source_id != source_id or world.epoch_id != epoch_id:
            return None
        return world.epoch_status


def get_context(request: Request) -> AppContext:
    context = getattr(request.app.state, "context", None)
    if context is None:  # pragma: no cover - a misconfigured app
        raise ApiProblem("NOT_READY", "the service is not initialised")
    return context


def new_request_id() -> str:
    return f"req-{uuid.uuid4().hex[:16]}"


# ----------------------------------------------------------------- identity


def authenticate(request: Request, context: AppContext) -> Principal:
    """Verify the bearer token and build the trusted principal.

    Every failure is the same opaque ``401``: a prober must not be able to
    distinguish a bad signature from a wrong audience. The real reason goes to
    the structured log.
    """
    header = request.headers.get("authorization")
    if not header:
        raise ApiProblem(
            "UNAUTHENTICATED",
            "a bearer credential is required",
            reason_codes=("MISSING_CREDENTIAL",),
        )
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise ApiProblem(
            "UNAUTHENTICATED",
            "the Authorization header must use the Bearer scheme",
            reason_codes=("MALFORMED_CREDENTIAL",),
        )
    try:
        verified = context.verifier.verify(token.strip(), now=context.wall_clock())
    except TokenError as exc:
        log_event(
            context.logger,
            logging.INFO,
            "authentication_failed",
            request_id=getattr(request.state, "request_id", None),
            reason=exc.reason,
        )
        raise ApiProblem(
            "UNAUTHENTICATED",
            "the supplied credential is not valid for this API",
            reason_codes=("INVALID_CREDENTIAL",),
        ) from exc

    # A client-supplied tenant header is never trusted. The claim decides.
    if request.headers.get("x-tenant-id") and request.headers["x-tenant-id"] != (
        verified.tenant_id or ""
    ):
        log_event(
            context.logger,
            logging.WARNING,
            "tenant_header_ignored",
            request_id=getattr(request.state, "request_id", None),
        )
    if verified.tenant_id is None:
        raise ApiProblem(
            "ACCESS_DENIED",
            "the credential carries no tenant binding",
            reason_codes=("MISSING_TENANT_CLAIM",),
        )
    return Principal(
        principal_id=verified.subject,
        tenant_id=verified.tenant_id,
        roles=_roles_from(verified.raw_claims),
        token_scopes=verified.scopes,
        key_id=verified.key_id,
        token_id=verified.token_id,
    )


def _roles_from(claims: Mapping[str, Any]) -> tuple[str, ...]:
    raw = claims.get("roles", ())
    if isinstance(raw, str):
        return tuple(part for part in raw.split() if part)
    if isinstance(raw, list) and all(isinstance(item, str) for item in raw):
        return tuple(raw)
    return ()


async def resolve_policy(principal: Principal, context: AppContext) -> ScopePolicy:
    """Load the server-side policy record. A missing record denies."""
    policy = await context.policies.resolve(principal.principal_id)
    if policy is None:
        raise ApiProblem(
            "ACCESS_DENIED",
            "no access policy is configured for this principal",
            reason_codes=("MISSING_POLICY",),
        )
    return policy


# ------------------------------------------------------------- negotiation


def assert_accept_json(request: Request) -> None:
    """Refuse a client that will not accept JSON.

    A financial body is never served as some other type just because the
    client asked for it.
    """
    accept = request.headers.get("accept")
    if not accept or accept.strip() == "*/*":
        return
    acceptable = {part.split(";")[0].strip().lower() for part in accept.split(",")}
    if acceptable & {"application/json", "application/*", "*/*", "application/problem+json"}:
        return
    raise ApiProblem(
        "NOT_ACCEPTABLE", "this API only represents reports as application/json"
    )


def assert_json_content_type(request: Request, *, reject_encoding: bool) -> None:
    """Require a JSON request body and refuse compressed bodies in P0."""
    content_type = (request.headers.get("content-type") or "").split(";")[0].strip().lower()
    if content_type != "application/json":
        raise ApiProblem(
            "UNSUPPORTED_MEDIA_TYPE",
            "request bodies must be sent as application/json",
        )
    if reject_encoding and request.headers.get("content-encoding"):
        # Decompression is an unbounded cost before validation; not in P0.
        raise ApiProblem(
            "UNSUPPORTED_MEDIA_TYPE",
            "compressed request bodies are not accepted by this version",
        )


def assert_query_budget(request: Request, settings: Settings) -> None:
    """Bound the URL and query string before parsing anything."""
    raw_path = request.scope.get("raw_path") or b""
    url_length = len(raw_path) or len(str(request.url))
    if url_length > settings.max_url_chars:
        raise ApiProblem("URI_TOO_LONG", "the request URI exceeds the configured limit")
    query = request.scope.get("query_string") or b""
    if len(query) > settings.max_query_chars:
        raise ApiProblem("INVALID_REQUEST", "the query string exceeds the configured limit")


# --------------------------------------------------------------- the spine


async def run_report(
    *,
    request: Request,
    context: AppContext,
    endpoint_id: str,
    permission: str,
    components: Sequence[str],
    allowed_query_keys: Iterable[str],
    source_id: str,
    account_ids: Sequence[str],
    allow_partial: bool,
    workload: WorkloadClass,
    aggregate: bool = False,
    symbol: str | None = None,
    epoch_id: str | None = None,
    period_start_utc: str | None = None,
    period_end_utc: str | None = None,
    valuation_as_of: str | None = None,
    requested_risk_policy: str | None = None,
    requested_symbol_catalog: str | None = None,
) -> ReportEnvelope:
    """Authenticate, authorize, admit, compute, verify, audit and serialize."""
    settings = context.settings
    assert_query_budget(request, settings)
    assert_accept_json(request)
    enforce_query_allowlist(request.query_params.keys(), frozenset(allowed_query_keys))

    principal = authenticate(request, context)
    require_permission(principal, permission, denial_family="ENDPOINT_NOT_PERMITTED")
    policy = await resolve_policy(principal, context)

    risk_policy_version, symbol_catalog_version = resolve_policy_versions(
        requested_risk_policy=requested_risk_policy,
        requested_symbol_catalog=requested_symbol_catalog,
        installed_risk_policy=settings.risk_policy_version,
        installed_symbol_catalog=settings.symbol_catalog_version,
        allowed_risk_policies=settings.allowed_risk_policy_versions,
        allowed_symbol_catalogs=settings.allowed_symbol_catalog_versions,
    )

    scope = await authorize_scope(
        principal=principal,
        policy=policy,
        filters=RequestedFilters(
            source_id=source_id,
            account_ids=tuple(account_ids),
            symbol=symbol,
            epoch_id=epoch_id,
        ),
        epochs=context.epochs(),
        allow_pinned_epoch=settings.allow_pinned_epoch_requests,
        aggregate=aggregate,
        max_accounts=settings.max_accounts_per_request,
    )

    report_request = ReportRequest(
        endpoint_id=endpoint_id,
        scope=scope,
        components=tuple(components),
        allow_partial=allow_partial,
        risk_policy_version=risk_policy_version,
        symbol_catalog_version=symbol_catalog_version,
        api_schema_version=settings.api_schema_version,
        result_contract_version=settings.result_contract_version,
        request_id=getattr(request.state, "request_id", new_request_id()),
        period_start_utc=period_start_utc,
        period_end_utc=period_end_utc,
        valuation_as_of=valuation_as_of,
        workload=workload,
    )

    deadline = Deadline.after(settings.request_deadline_seconds, clock=context.clock)
    async with context.admission.admit(
        tenant_id=scope.tenant_id, workload=workload, deadline=deadline
    ):
        envelope = await context.service.produce(report_request, deadline=deadline)

    log_event(
        context.logger,
        logging.INFO,
        "report_served",
        request_id=report_request.request_id,
        endpoint_id=endpoint_id,
        status=envelope.result.status,
        partial=envelope.partial,
        scope_hash=scope.audit_scope_hash(),
        audit_id=envelope.audit_id,
    )
    return envelope


def wall_clock_seconds() -> int:
    return int(time.time())
