"""One wiring of the whole system, shared by the CLI, the demos and the tests.

Assembling writer, store, relay, broker and consumer by hand in every test is
how test setups drift apart until two tests are quietly exercising different
systems. This module owns the wiring once. Tests reach past it when they need
something unusual -- a second relay owner, a separate producer identity -- and
those are exactly the cases worth seeing spelled out at the call site.

The publisher and the consumer get *separate* :class:`Metrics` registries by
default. They are different processes in a real deployment, and sharing a
registry would let a test assert on a counter without noticing which side
incremented it.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .broker.embedded import EmbeddedConsumer, EmbeddedKafka, EmbeddedProducer
from .clock import Clock, RealClock
from .consumer.inbox import ConsumerStore
from .consumer.reference import ReferenceConsumer
from .faults.hooks import HOOKS, HookRegistry
from .health import HealthMonitor
from .observability.metrics import Metrics
from .p03.sync_service import SyncService
from .persistence.sqlite_store import SqliteOutboxStore
from .relay.publisher import Publisher
from .relay.scheduler import RelayWorker
from .settings import Settings, load_settings
from .writer.outbox_writer import OutboxWriter


@dataclass(slots=True)
class Pipeline:
    """Every component, already connected."""

    settings: Settings
    clock: Clock
    hooks: HookRegistry
    store: SqliteOutboxStore
    writer: OutboxWriter
    sync: SyncService
    broker: EmbeddedKafka
    producer: EmbeddedProducer
    publisher: Publisher
    worker: RelayWorker
    consumer_store: ConsumerStore
    kafka_consumer: EmbeddedConsumer
    consumer: ReferenceConsumer
    health: HealthMonitor
    relay_metrics: Metrics
    consumer_metrics: Metrics
    source_id: str
    synchronous: str = "FULL"
    extra: dict[str, Any] = field(default_factory=dict)

    def run_relay(self, *, max_cycles: int = 10_000) -> Any:
        return self.worker.run_until_idle(max_cycles=max_cycles)

    def run_consumer(self, *, max_rounds: int = 1000) -> Any:
        return self.consumer.run_until_idle(max_rounds=max_rounds)

    def pump(self, *, max_cycles: int = 10_000, max_rounds: int = 1000) -> tuple[Any, Any]:
        """Drive the relay to quiescence, then the consumer. The common case."""
        return self.run_relay(max_cycles=max_cycles), self.run_consumer(max_rounds=max_rounds)

    def new_relay(self, owner: str, *, metrics: Metrics | None = None) -> tuple[Publisher, RelayWorker]:
        """A second relay owner sharing the store and broker.

        Its own store handle and its own producer, because that is what a
        standby process has. Sharing the producer would make the takeover tests
        meaningless -- the "old" worker's queued record would be served by the
        new worker's poll.
        """
        store = SqliteOutboxStore(
            self.settings.sqlite_path, clock=self.clock, synchronous=self.synchronous
        )
        producer = self.broker.producer(queue_max_bytes=self.settings.producer_buffer_max_bytes)
        publisher = Publisher(
            store,
            producer,
            settings=self.settings,
            clock=self.clock,
            metrics=metrics or Metrics.build(),
            hooks=self.hooks,
            owner=owner,
        )
        worker = RelayWorker(publisher, settings=self.settings, clock=self.clock, sleep=lambda _s: None)
        self.extra.setdefault("standbys", []).append((store, producer))
        return publisher, worker

    def close(self) -> None:
        self.store.close()
        self.consumer_store.close()
        self.producer.close()
        self.kafka_consumer.close()
        for store, producer in self.extra.get("standbys", []):
            store.close()
            producer.close()


def build_pipeline(
    *,
    root: str | Path,
    clock: Clock | None = None,
    hooks: HookRegistry | None = None,
    settings: Settings | None = None,
    source_id: str = "sim-broker-01",
    subscribe: bool = True,
    synchronous: str = "FULL",
    **setting_overrides: Any,
) -> Pipeline:
    """Build and migrate a complete pipeline rooted at ``root``.

    ``synchronous`` is passed through to both SQLite stores; see
    :class:`~outbox.persistence.sqlite_store.SqliteOutboxStore` for why a test
    suite may lower it.
    """
    base = Path(root)
    base.mkdir(parents=True, exist_ok=True)
    active_clock = clock or RealClock()
    active_hooks = hooks or HOOKS
    resolved = settings or load_settings(
        sqlite_path=str(base / "publisher.sqlite3"),
        source_allowlist=(source_id,),
        **setting_overrides,
    )

    store = SqliteOutboxStore(resolved.sqlite_path, clock=active_clock, synchronous=synchronous)
    # This builder is P07's self-contained lab: it also constructs the
    # EmbeddedKafka broker and the SyncService stand-in that plays the part of
    # Project 03. It therefore owns the P03 stand-in schema and asks for it
    # explicitly. A deployment sharing Project 03's database must not use this
    # builder (contract gap CG-07).
    store.migrate(include_p03_standin=True)

    writer = OutboxWriter(
        source_allowlist=resolved.source_allowlist,
        clock=active_clock,
        max_event_wire_bytes=resolved.max_event_wire_bytes,
        hooks=active_hooks,
    )
    sync = SyncService(store, writer, source_id=source_id, clock=active_clock, hooks=active_hooks)

    broker = EmbeddedKafka()
    producer = broker.producer(queue_max_bytes=resolved.producer_buffer_max_bytes)
    relay_metrics = Metrics.build()
    publisher = Publisher(
        store,
        producer,
        settings=resolved,
        clock=active_clock,
        metrics=relay_metrics,
        hooks=active_hooks,
    )
    worker = RelayWorker(publisher, settings=resolved, clock=active_clock, sleep=lambda _s: None)

    consumer_store = ConsumerStore(base / "consumer.sqlite3", clock=active_clock, synchronous=synchronous)
    consumer_store.migrate()
    kafka_consumer = broker.consumer(group=resolved.consumer_group)
    consumer_metrics = Metrics.build()
    consumer = ReferenceConsumer(
        kafka_consumer,
        consumer_store,
        settings=resolved,
        clock=active_clock,
        metrics=consumer_metrics,
        hooks=active_hooks,
    )
    if subscribe:
        consumer.subscribe()

    health = HealthMonitor(store, producer, settings=resolved, clock=active_clock)

    return Pipeline(
        settings=resolved,
        clock=active_clock,
        hooks=active_hooks,
        store=store,
        writer=writer,
        sync=sync,
        broker=broker,
        producer=producer,
        publisher=publisher,
        worker=worker,
        consumer_store=consumer_store,
        kafka_consumer=kafka_consumer,
        consumer=consumer,
        health=health,
        relay_metrics=relay_metrics,
        consumer_metrics=consumer_metrics,
        source_id=source_id,
        synchronous=synchronous,
    )
