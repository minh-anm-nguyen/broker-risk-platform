"""Composition root: assemble the three P08 apps from settings.

The design splits P08 into three deployables - the Kafka consumer, the build
worker and the read-only query service (section 16.1). They share the control
plane and ClickHouse, so a single :class:`Stack` builds the shared pieces once
and hands out the workers.

``build_lab_stack`` is used by the test suite, the CLI and the benchmarks. It
wires the deterministic adapters; ``build_production_stack`` wires the real
drivers and refuses to start if the configuration would be unsafe. The code
under test is the same either way - only the adapters differ, which is the
point of the ports (see ``ports/__init__.py``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .adapters.clickhouse_memory import InMemoryClickHouse
from .adapters.control_sqlite import SqliteControlPlane
from .adapters.kafka_memory import InMemoryBroker, InMemoryConsumer
from .build.worker import BuildWorker, SourceEvidenceProvider
from .clock import Clock, ManualClock, SystemClock
from .contracts.policy import AccountingPolicy, default_p0_policy
from .errors import ConfigurationError
from .faults.killpoints import FaultPlan, NullFaultPlan
from .ingest.consumer import IngestWorker
from .observability.logging import configure_logging
from .observability.metrics import Metrics
from .ports.clickhouse import ClickHousePort
from .ports.control import ControlPlanePort
from .ports.kafka import ConsumerPort, PartitionKey
from .publication.gc import GarbageCollector
from .publication.registry import PublicationRegistry
from .query.api import create_app
from .query.auth import Authenticator, Scope
from .query.cursors import CursorCodec
from .query.service import AnalyticsQueryService
from .settings import Settings


@dataclass(slots=True)
class Stack:
    """Everything wired together, with the shared pieces shared."""

    settings: Settings
    clock: Clock
    metrics: Metrics
    faults: FaultPlan
    clickhouse: ClickHousePort
    control: ControlPlanePort
    policy: AccountingPolicy
    registry: PublicationRegistry
    query: AnalyticsQueryService
    authenticator: Authenticator
    gc: GarbageCollector
    broker: InMemoryBroker | None = None
    consumer: ConsumerPort | None = None

    def ingest_worker(self, consumer: ConsumerPort | None = None) -> IngestWorker:
        target = consumer or self.consumer
        if target is None:
            raise ConfigurationError("no consumer is wired into this stack")
        return IngestWorker(
            consumer=target,
            clickhouse=self.clickhouse,
            control=self.control,
            settings=self.settings,
            clock=self.clock,
            metrics=self.metrics,
            faults=self.faults,
        )

    def build_worker(
        self, source: SourceEvidenceProvider, *, actor: str = "build-worker"
    ) -> BuildWorker:
        return BuildWorker(
            clickhouse=self.clickhouse,
            control=self.control,
            source=source,
            policy=self.policy,
            settings=self.settings,
            clock=self.clock,
            metrics=self.metrics,
            faults=self.faults,
            actor=actor,
        )

    def asgi_app(self):
        return create_app(
            service=self.query,
            authenticator=self.authenticator,
            metrics=self.metrics,
            ready_probe=self.ready_probe,
        )

    def ready_probe(self) -> dict[str, Any]:
        """Readiness that actually checks something.

        A process answering ``ready`` while its storage is unreachable just
        attracts traffic it cannot serve, so the probe touches ClickHouse and
        reports the failure reason.
        """
        try:
            stats = self.clickhouse.storage_stats()
        except Exception as exc:  # pragma: no cover - adapter specific
            return {"ready": False, "reason": "CLICKHOUSE_UNAVAILABLE", "detail": str(exc)}
        return {
            "ready": True,
            "tables": dict(stats.table_rows),
            "disk_free_bytes": stats.disk_free_bytes,
        }

    def close(self) -> None:
        closer = getattr(self.control, "close", None)
        if callable(closer):
            closer()


def _assemble(
    *,
    settings: Settings,
    clock: Clock,
    clickhouse: ClickHousePort,
    control: ControlPlanePort,
    policy: AccountingPolicy,
    metrics: Metrics,
    faults: FaultPlan,
    authenticator: Authenticator,
    broker: InMemoryBroker | None = None,
    consumer: ConsumerPort | None = None,
) -> Stack:
    registry = PublicationRegistry(
        control,
        clock=clock,
        freshness_target_seconds=settings.query_freshness_target_seconds,
    )
    query = AnalyticsQueryService(
        clickhouse=clickhouse,
        registry=registry,
        policy=policy,
        settings=settings,
        clock=clock,
        cursors=CursorCodec(
            signing_key=settings.cursor_signing_key or "lab-cursor-key-not-for-production",
            key_id=settings.cursor_signing_key_id,
        ),
        metrics=metrics,
        faults=faults,
    )
    collector = GarbageCollector(
        control=control, clickhouse=clickhouse, clock=clock, faults=faults
    )
    return Stack(
        settings=settings,
        clock=clock,
        metrics=metrics,
        faults=faults,
        clickhouse=clickhouse,
        control=control,
        policy=policy,
        registry=registry,
        query=query,
        authenticator=authenticator,
        gc=collector,
        broker=broker,
        consumer=consumer,
    )


def build_lab_stack(
    *,
    settings: Settings | None = None,
    clock: Clock | None = None,
    faults: FaultPlan | None = None,
    policy: AccountingPolicy | None = None,
    partitions: int = 1,
    topic: str | None = None,
    assignment: Sequence[PartitionKey] | None = None,
    control_path: str = ":memory:",
    disk_capacity_rows: int = 5_000_000,
    reader_token: str = "lab-reader-token",
    operator_token: str = "lab-operator-token",
    tenant_id: str = "lab-tenant",
) -> Stack:
    """Wire the deterministic stack used by tests, the CLI and benchmarks."""
    settings = settings or Settings(
        env="test",
        tenant_map=(f"broker.deals.v1:sim-broker-01={tenant_id}",),
    )
    clock = clock or ManualClock()
    faults = faults or FaultPlan()
    metrics = Metrics()
    policy = policy or default_p0_policy()

    topic_name = topic or settings.kafka_topic_allowlist[0]
    broker = InMemoryBroker(faults=faults)
    broker.create_topic(topic_name, partitions)
    consumer = InMemoryConsumer(
        broker,
        group_id=settings.kafka_group_id,
        assignment=list(assignment) if assignment is not None else broker.partitions_of(topic_name),
        faults=faults,
    )
    clickhouse = InMemoryClickHouse(
        faults=faults, clock=clock, disk_capacity_rows=disk_capacity_rows
    )
    control = SqliteControlPlane(control_path, clock=clock, faults=faults)

    authenticator = Authenticator()
    authenticator.register(
        reader_token, principal=f"reader@{tenant_id}", scope=Scope(tenant_id=tenant_id)
    )
    authenticator.register(
        operator_token,
        principal=f"operator@{tenant_id}",
        scope=Scope(tenant_id=tenant_id, operator=True),
    )

    return _assemble(
        settings=settings,
        clock=clock,
        clickhouse=clickhouse,
        control=control,
        policy=policy,
        metrics=metrics,
        faults=faults,
        authenticator=authenticator,
        broker=broker,
        consumer=consumer,
    )


def build_production_stack(settings: Settings | None = None) -> Stack:
    """Wire the real drivers. Imports them lazily and fails closed.

    Called only from the CLI entrypoints. A missing driver is a configuration
    error with a clear message rather than an ``ImportError`` from somewhere
    deep inside a request handler.
    """
    settings = settings or Settings()
    configure_logging(settings.log_level)
    if settings.env not in ("docker", "prod"):
        raise ConfigurationError(
            "build_production_stack requires env=docker or env=prod; "
            "use build_lab_stack for local work",
            env=settings.env,
        )

    from .adapters.clickhouse_http import HttpClickHouse
    from .adapters.control_postgres import PostgresControlPlane

    clock = SystemClock()
    clickhouse = HttpClickHouse(
        url=settings.ch_url,
        database=settings.ch_database,
        max_query_duration_ms=settings.ch_max_query_duration_ms,
        version_pin=settings.ch_server_version_pin,
    )
    control = PostgresControlPlane(dsn=settings.control_dsn, clock=clock)
    return _assemble(
        settings=settings,
        clock=clock,
        clickhouse=clickhouse,
        control=control,
        policy=default_p0_policy(),
        metrics=Metrics(),
        # Production never carries an armed fault plan.
        faults=NullFaultPlan(),
        authenticator=Authenticator.from_settings(settings),
    )


def app():  # pragma: no cover - ASGI entrypoint
    """``uvicorn analytics.main:app --factory``."""
    return build_production_stack().asgi_app()
