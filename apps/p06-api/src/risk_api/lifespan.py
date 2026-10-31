"""Dependency assembly and lifecycle.

``build_context`` is the single composition root: the app, the tests and the
scripts all construct the service the same way, so a test cannot accidentally
exercise a different wiring than production.

The read-view capability handshake runs here, at startup. Detecting a Project
03/04 contract drift during a financial request would be far too late.
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable, Mapping

from fastapi import FastAPI
from risk_engine import RiskEngine

from .admission.budgets import RequestBudget, monotonic_clock
from .admission.quotas import PrincipalQuotas
from .admission.scheduler import AdmissionController
from .api.deps import AppContext, wall_clock_seconds
from .app.cut_capture import CutCapturer
from .app.engine_port import BoundedRiskEngineAdapter, RiskEnginePort
from .app.pagination import CursorCodec
from .app.risk_service import RiskReportService
from .authn.jwt_verifier import JwtVerifier
from .authz.principal_context import ScopePolicy
from .authz.scope_policy import InMemoryScopePolicyRepository, ScopePolicyRepository
from .contracts.versioning import assert_read_view_capability
from .execution.cpu_worker import CpuWorkerPool
from .faults import FaultRegistry
from .observability.logging import configure_logging
from .observability.metrics import Metrics
from .repositories.audit_store import AuditStore, InMemoryAuditStore, JsonlAuditStore
from .repositories.read_views import InMemoryReadViews, SourceWorld, build_demo_world
from .settings import Settings, get_settings

__all__ = ["build_context", "lifespan", "default_policies"]


def default_policies(source_id: str = "sim-broker-a") -> dict[str, ScopePolicy]:
    """A small, explicit policy set for local development.

    Two analysts with disjoint accounts, so a BOLA attempt has a real victim
    to try to reach, plus an ops reader that can see metrics and nothing else.
    """
    return {
        "analyst-a": ScopePolicy(
            principal_id="analyst-a",
            tenant_id="tenant-a",
            roles=("risk_analyst",),
            revision="policy-r1",
            accounts_by_source={source_id: frozenset({"00042"})},
            aggregate_sources=frozenset({source_id}),
        ),
        "analyst-b": ScopePolicy(
            principal_id="analyst-b",
            tenant_id="tenant-b",
            roles=("risk_analyst",),
            revision="policy-r1",
            accounts_by_source={source_id: frozenset({"00043"})},
            aggregate_sources=frozenset({source_id}),
        ),
        "viewer-a": ScopePolicy(
            principal_id="viewer-a",
            tenant_id="tenant-a",
            roles=("risk_viewer",),
            revision="policy-r1",
            accounts_by_source={source_id: frozenset({"00042"})},
        ),
        "ops-a": ScopePolicy(
            principal_id="ops-a",
            tenant_id="tenant-a",
            roles=("ops_reader",),
            revision="policy-r1",
            accounts_by_source={},
        ),
    }


def build_context(
    *,
    settings: Settings | None = None,
    world: SourceWorld | None = None,
    policies: Mapping[str, ScopePolicy] | None = None,
    policy_repository: ScopePolicyRepository | None = None,
    engine: RiskEnginePort | None = None,
    audit: AuditStore | None = None,
    clock: Callable[[], float] | None = None,
    wall_clock: Callable[[], int] | None = None,
    faults: FaultRegistry | None = None,
) -> AppContext:
    """Assemble every port the API needs."""
    settings = settings or get_settings()
    logger = configure_logging(level=settings.log_level)
    fault_registry = faults or FaultRegistry(enabled=settings.faults_enabled)
    metrics = Metrics()

    verifier = JwtVerifier(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        keys=settings.jwt_keys,
        allowed_algorithms=settings.jwt_allowed_algorithms,
        clock_skew_seconds=settings.jwt_clock_skew_seconds,
        max_lifetime_seconds=settings.jwt_max_lifetime_seconds,
    )

    source_world = world or build_demo_world()
    views = InMemoryReadViews(
        source_world,
        pool_size=settings.db_pool_size,
        pool_timeout_seconds=settings.db_pool_timeout_seconds,
        statement_timeout_ms=settings.db_statement_timeout_ms,
        faults=fault_registry,
    )
    # Fail fast on a drifted read-side contract.
    assert_read_view_capability(views.capability())

    repository = policy_repository or InMemoryScopePolicyRepository(
        dict(policies or default_policies(source_world.source_id))
    )

    workers = CpuWorkerPool(max_workers=settings.max_heavy_executing)
    engine_port = engine or BoundedRiskEngineAdapter(
        engine=RiskEngine(
            risk_policy_version=settings.risk_policy_version,
            reporting_currency=settings.reporting_currency,
        ),
        workers=workers,
        faults=fault_registry,
    )

    audit_store = audit or _build_audit_store(settings, fault_registry)

    budget = RequestBudget(
        total_seconds=settings.request_deadline_seconds,
        capture_seconds=settings.capture_budget_seconds,
        compute_seconds=settings.compute_budget_seconds,
        final_check_seconds=settings.final_check_budget_seconds,
    )
    monotonic = clock or monotonic_clock
    quotas = PrincipalQuotas(
        rate_per_second=settings.rate_limit_per_second,
        burst=settings.rate_limit_burst,
        clock=monotonic,
    )
    admission = AdmissionController(
        quotas=quotas,
        max_heavy_executing=settings.max_heavy_executing,
        max_heavy_queued=settings.max_heavy_queued,
        max_cheap_concurrent=settings.max_cheap_concurrent,
        heavy_weight=settings.heavy_request_weight,
        cheap_weight=settings.cheap_request_weight,
        retry_after_seconds=settings.retry_after_seconds,
    )
    capturer = CutCapturer(
        views=views,
        faults=fault_registry,
        default_realized_window_days=settings.default_realized_window_days,
    )
    service = RiskReportService(
        capturer=capturer,
        engine=engine_port,
        audit=audit_store,
        metrics=metrics,
        budget=budget,
        audit_required=settings.audit_required,
        max_response_bytes=settings.max_response_bytes,
        reporting_currency=settings.reporting_currency,
        faults=fault_registry,
        max_recapture_attempts=settings.max_recapture_attempts,
    )
    cursors = CursorCodec(
        signing_key=settings.cursor_signing_key,
        key_id=settings.cursor_key_id,
        ttl_seconds=settings.cursor_ttl_seconds,
    )

    return AppContext(
        settings=settings,
        verifier=verifier,
        policies=repository,
        views=views,
        capturer=capturer,
        engine=engine_port,
        audit=audit_store,
        metrics=metrics,
        admission=admission,
        quotas=quotas,
        cursors=cursors,
        service=service,
        workers=workers,
        faults=fault_registry,
        logger=logger,
        clock=monotonic,
        wall_clock=wall_clock or wall_clock_seconds,
        budget=budget,
    )


def _build_audit_store(settings: Settings, faults: FaultRegistry) -> AuditStore:
    if settings.audit_backend == "jsonl":
        return JsonlAuditStore(settings.audit_path, faults=faults)
    return InMemoryAuditStore(faults=faults)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start the worker pool and heartbeat; drain them on shutdown."""
    context: AppContext = app.state.context
    context.workers.start()
    app.state.logger = context.logger
    app.state.started_at = time.time()
    try:
        yield
    finally:
        await context.workers.shutdown()
