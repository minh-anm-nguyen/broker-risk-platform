"""The ClickHouse port: append-only raw, immutable build tables.

The protocol is deliberately narrow. P08 does not need arbitrary SQL from the
application layer; it needs exactly the few shapes section 6.3 describes, each
with its scope bound. A port that exposed ``execute(sql)`` would make
INV08-17 - every read is scoped - unenforceable by construction.

One asymmetry is encoded in the return type. :class:`InsertResult` can say
"proven", and it can raise :class:`~analytics.errors.AmbiguousWriteError`, but
there is no third option that means "probably fine". Section 3.2: on an
uncertain ACK the only safe action is to replay.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping, Protocol, Sequence, runtime_checkable


@dataclass(frozen=True, slots=True)
class RawObservation:
    """One row of ``broker_events_raw``. Section 5.2.

    An *observation*, not a Deal: the same Deal legitimately appears here more
    than once after a Kafka replay (INV08-01, INV08-23), which is why the
    table is plain ``MergeTree`` and nothing ever sums it directly.
    """

    tenant_id: str
    source_id: str
    epoch_id: str
    event_id: str
    dedupe_key: str
    kafka_record_key: str
    event_type: str
    schema_version: int
    payload_sha256: str
    deal_payload_hash: str | None
    deal_id: str | None
    account_id: str
    source_seq: int
    aggregate_version: int
    event_time: datetime
    executed_at: datetime | None
    kafka_topic: str
    kafka_partition: int
    kafka_offset: int
    payload_json: str
    ingested_at: datetime | None = None

    @property
    def scope(self) -> tuple[str, str, str]:
        return (self.tenant_id, self.source_id, self.epoch_id)

    @property
    def coordinate(self) -> tuple[str, int, int]:
        return (self.kafka_topic, self.kafka_partition, self.kafka_offset)


@dataclass(frozen=True, slots=True)
class DealFactRow:
    """One row of ``analytics_deals_build``: exactly one per Deal per build."""

    build_id: str
    tenant_id: str
    source_id: str
    epoch_id: str
    deal_id: str
    account_id: str
    symbol: str
    side: str
    entry_kind: str
    source_seq: int
    executed_at: datetime
    money_currency: str
    realized_pnl_gross: Decimal
    commission: Decimal
    swap: Decimal
    volume_lots: Decimal
    fee_coverage: str
    deal_payload_hash: str
    event_id: str


@dataclass(frozen=True, slots=True)
class DailyRollupRow:
    """One row of ``analytics_daily_build``, grouped per section 6.4."""

    build_id: str
    tenant_id: str
    source_id: str
    epoch_id: str
    trade_day: date
    account_id: str
    symbol: str
    money_currency: str
    deal_count: int
    realized_pnl_gross: Decimal
    commission: Decimal
    swap: Decimal
    volume_lots: Decimal


@dataclass(frozen=True, slots=True)
class InsertResult:
    """Outcome of a *proven* insert. Ambiguity is raised, never returned."""

    rows_written: int
    table: str
    #: Monotonic marker of the write, used by the read-visibility probe before
    #: a pointer flip (INV08-09, P08-T-01).
    write_marker: int


@dataclass(frozen=True, slots=True)
class EventIdGroup:
    """Per-``event_id`` observation summary. Section 6.3, first query."""

    event_id: str
    physical_observations: int
    distinct_hashes: int
    hashes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class DealHashGroup:
    """Per-Deal observation summary. Section 6.3, second query."""

    deal_id: str
    physical_observations: int
    distinct_deal_hashes: int
    hashes: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class StorageStats:
    """What ``system.parts`` would tell an operator. Section 13.5."""

    table_rows: Mapping[str, int] = field(default_factory=dict)
    active_parts: Mapping[str, int] = field(default_factory=dict)
    bytes_on_disk: int = 0
    disk_free_bytes: int = 0
    peak_memory_bytes: int = 0


@runtime_checkable
class ClickHousePort(Protocol):
    """Operations P08 performs against ClickHouse. Nothing wider."""

    def insert_raw(self, rows: Sequence[RawObservation], *, deadline_ms: int) -> InsertResult:
        """Append raw observations.

        Raises :class:`AmbiguousWriteError` when durability is unproven and
        :class:`RetriableError`/:class:`PoisonError` for refused batches. A
        return value always means the rows are durable.
        """

    def insert_deal_facts(self, rows: Sequence[DealFactRow], *, deadline_ms: int) -> InsertResult: ...

    def insert_daily_rollups(
        self, rows: Sequence[DailyRollupRow], *, deadline_ms: int
    ) -> InsertResult: ...

    def scan_raw_deals(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        max_source_seq: int,
        max_write_marker: int,
    ) -> Iterable[RawObservation]:
        """Raw Deal observations inside a *pinned* cut.

        ``max_write_marker`` is what makes the scan repeatable while ingestion
        continues: without it two SELECTs in one build can disagree
        (P08-R-06).
        """

    def event_id_conflicts(
        self, *, tenant_id: str, source_id: str, epoch_id: str, max_write_marker: int
    ) -> Sequence[EventIdGroup]: ...

    def deal_hash_conflicts(
        self, *, tenant_id: str, source_id: str, epoch_id: str, max_write_marker: int
    ) -> Sequence[DealHashGroup]: ...

    def count_deal_facts(self, *, build_id: str, tenant_id: str) -> tuple[int, int]:
        """``(physical_rows, unique_deal_ids)`` - both, because either alone lies."""

    def read_deal_facts(
        self, *, build_id: str, tenant_id: str, source_id: str, epoch_id: str
    ) -> Iterable[DealFactRow]: ...

    def read_daily_rollups(
        self,
        *,
        build_id: str,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        from_day: date | None = None,
        to_day: date | None = None,
        accounts: Sequence[str] | None = None,
    ) -> Iterable[DailyRollupRow]: ...

    def delete_build(self, *, build_id: str) -> int:
        """Drop an abandoned build's rows. Guarded by the GC policy, not here."""

    def storage_stats(self) -> StorageStats: ...

    def visible_write_marker(self, *, reader: str = "default") -> int:
        """Highest write marker the named read path can currently see.

        Models replica lag: a value below the writer's marker means a reader
        cannot yet see the build, and the publication gate must refuse rather
        than assume global visibility (INV08-24, P08-T-07).
        """
