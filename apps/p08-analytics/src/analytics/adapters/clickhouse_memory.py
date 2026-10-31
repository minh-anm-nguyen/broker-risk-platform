"""Deterministic in-process model of the ClickHouse tables P08 uses.

This is a model of the *engine's observable contract*, not a stub that
returns canned answers. The properties it reproduces are the ones P08's
correctness arguments depend on:

* **Append-only with duplicates.** ``MergeTree`` has no uniqueness
  constraint; ``ORDER BY`` is a sparse index (section 5.1). Inserting the same
  Deal twice yields two rows, so a build that forgot to dedup fails here
  exactly as it would in production.
* **Write markers and reader lag.** A row is durable when the writer is
  acknowledged, but a *reader* may not see it yet. ``visible_write_marker``
  models that gap, which is what makes INV08-09's read-visibility probe
  testable rather than theoretical.
* **Ambiguous acknowledgement.** The nastiest real failure is "persisted, but
  the client never learned". Here that is one scheduled fault
  (``CH_INSERT_PERSISTED_RESPONSE_DROPPED``) instead of a race nobody can
  reproduce on demand.
* **Bounded resources.** Disk capacity, per-insert parts and a query row
  budget are finite, so overload surfaces as a controlled error (INV08-20).

Rows are stored unsorted and scans sort explicitly. Any code that silently
depended on insertion order would be relying on something real ClickHouse
does not promise.
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime
from typing import Iterable, Sequence

from ..canonical import is_sha256_hex
from ..clock import Clock
from ..errors import PoisonError, Reason, RetriableError
from ..faults.killpoints import FaultPlan, FaultSite, NullFaultPlan
from ..ports.clickhouse import (
    DailyRollupRow,
    DealFactRow,
    DealHashGroup,
    EventIdGroup,
    InsertResult,
    RawObservation,
    StorageStats,
)

RAW_TABLE = "broker_analytics.broker_events_raw"
FACTS_TABLE = "broker_analytics.analytics_deals_build"
DAILY_TABLE = "broker_analytics.analytics_daily_build"


@dataclass(slots=True)
class _StoredRaw:
    row: RawObservation
    write_marker: int


@dataclass(slots=True)
class _StoredFact:
    row: DealFactRow
    write_marker: int


@dataclass(slots=True)
class _StoredDaily:
    row: DailyRollupRow
    write_marker: int


class InMemoryClickHouse:
    """Implements :class:`~analytics.ports.clickhouse.ClickHousePort`."""

    def __init__(
        self,
        *,
        faults: FaultPlan | None = None,
        disk_capacity_rows: int = 5_000_000,
        max_query_rows: int = 20_000_000,
        clock: Clock | None = None,
    ) -> None:
        self._faults = faults or NullFaultPlan()
        self._lock = threading.RLock()
        self._raw: list[_StoredRaw] = []
        self._facts: list[_StoredFact] = []
        self._daily: list[_StoredDaily] = []
        self._write_marker = 0
        self._parts: dict[str, int] = defaultdict(int)
        self._disk_capacity_rows = disk_capacity_rows
        self._max_query_rows = max_query_rows
        self._clock = clock
        #: Per-reader visibility ceiling. Absent means "fully caught up",
        #: which is the single-node P0 deployment.
        self._reader_markers: dict[str, int] = {}
        self._peak_memory_bytes = 0
        self._rows_read_total = 0

    # -- helpers -----------------------------------------------------------

    def _now(self) -> datetime:
        if self._clock is not None:
            return self._clock.now()
        return datetime.now(UTC)

    def _next_marker(self) -> int:
        self._write_marker += 1
        return self._write_marker

    @property
    def write_marker(self) -> int:
        return self._write_marker

    def _total_rows(self) -> int:
        return len(self._raw) + len(self._facts) + len(self._daily)

    def _check_capacity(self, incoming: int, table: str) -> None:
        if self._total_rows() + incoming > self._disk_capacity_rows:
            # Section 8.4: a full disk stops ingestion and keeps the Kafka
            # offset. It never drops the batch.
            raise RetriableError(
                Reason.STORAGE_FULL,
                "clickhouse storage capacity exhausted",
                table=table,
                capacity_rows=self._disk_capacity_rows,
            )

    def _budget(self, rows_scanned: int) -> None:
        self._rows_read_total += rows_scanned
        if rows_scanned > self._max_query_rows:
            raise RetriableError(
                Reason.BUDGET_EXCEEDED,
                "query would read more rows than the configured budget",
                rows=rows_scanned,
                budget=self._max_query_rows,
            )
        # Crude but honest: the model charges memory for what a GROUP BY would
        # have to hold, so P08-R-10 can assert a bounded failure.
        self._peak_memory_bytes = max(self._peak_memory_bytes, rows_scanned * 256)

    def _insert_guard(self, table: str, rows: Sequence[object], deadline_ms: int) -> None:
        self._faults.fire(FaultSite.CH_UNAVAILABLE, table=table, rows=rows)
        self._faults.fire(FaultSite.CH_DISK_FULL, table=table, rows=rows)
        if deadline_ms <= 0:
            raise RetriableError(Reason.DEADLINE_EXCEEDED, "insert deadline already elapsed")

    # -- inserts -----------------------------------------------------------

    def insert_raw(self, rows: Sequence[RawObservation], *, deadline_ms: int = 5_000) -> InsertResult:
        with self._lock:
            self._insert_guard(RAW_TABLE, rows, deadline_ms)
            for row in rows:
                self._validate_raw(row)
            self._check_capacity(len(rows), RAW_TABLE)

            # ``CH_INSERT_REJECT`` models a server-side type error: nothing is
            # persisted, so the caller may safely keep its offset (P08-H-01).
            self._faults.fire(FaultSite.CH_INSERT_REJECT, table=RAW_TABLE, rows=rows)

            marker = self._next_marker()
            partial = self._faults.armed(FaultSite.CH_INSERT_PARTIAL)
            accepted = rows[: max(1, len(rows) // 2)] if partial and len(rows) > 1 else rows
            for row in accepted:
                stored = row if row.ingested_at is not None else replace(row, ingested_at=self._now())
                self._raw.append(_StoredRaw(stored, marker))
            self._parts[RAW_TABLE] += 1

            if partial:
                self._faults.fire(FaultSite.CH_INSERT_PARTIAL, table=RAW_TABLE, rows=rows)

            # Fired *after* persistence: the rows are in the table and the
            # client is told nothing. Section 3.2's crash window, on demand.
            self._faults.fire(FaultSite.CH_INSERT_AMBIGUOUS, table=RAW_TABLE, rows=rows)
            return InsertResult(rows_written=len(accepted), table=RAW_TABLE, write_marker=marker)

    def _validate_raw(self, row: RawObservation) -> None:
        """Reject what the DDL cannot. Section 5.2's "hard gates".

        ``FixedString(64)`` accepts any 64 bytes, so the hash shape is checked
        here; and a Deal observation without a Deal fingerprint would silently
        become un-dedupable, so it is refused rather than stored.
        """
        if not is_sha256_hex(row.payload_sha256):
            raise PoisonError(
                Reason.HASH_MALFORMED, "payload_sha256 must be 64 hex chars", event_id=row.event_id
            )
        if row.event_type == "DealRecorded":
            if not row.deal_id:
                raise PoisonError(Reason.FIELD_MISSING, "DealRecorded raw row needs deal_id")
            if not is_sha256_hex(row.deal_payload_hash):
                raise PoisonError(
                    Reason.HASH_MALFORMED,
                    "DealRecorded raw row needs a 64-hex deal_payload_hash",
                    deal_id=row.deal_id,
                )
        if row.executed_at is not None and row.executed_at.tzinfo is None:
            raise PoisonError(Reason.FIELD_INVALID, "executed_at must be timezone-aware")

    def insert_deal_facts(
        self, rows: Sequence[DealFactRow], *, deadline_ms: int = 30_000
    ) -> InsertResult:
        with self._lock:
            self._insert_guard(FACTS_TABLE, rows, deadline_ms)
            self._check_capacity(len(rows), FACTS_TABLE)
            self._faults.fire(FaultSite.CH_INSERT_REJECT, table=FACTS_TABLE, rows=rows)
            marker = self._next_marker()
            for row in rows:
                self._facts.append(_StoredFact(row, marker))
            self._parts[FACTS_TABLE] += 1
            self._faults.fire(FaultSite.CH_INSERT_AMBIGUOUS, table=FACTS_TABLE, rows=rows)
            return InsertResult(rows_written=len(rows), table=FACTS_TABLE, write_marker=marker)

    def insert_daily_rollups(
        self, rows: Sequence[DailyRollupRow], *, deadline_ms: int = 30_000
    ) -> InsertResult:
        with self._lock:
            self._insert_guard(DAILY_TABLE, rows, deadline_ms)
            self._check_capacity(len(rows), DAILY_TABLE)
            self._faults.fire(FaultSite.CH_INSERT_REJECT, table=DAILY_TABLE, rows=rows)
            marker = self._next_marker()
            for row in rows:
                self._daily.append(_StoredDaily(row, marker))
            self._parts[DAILY_TABLE] += 1
            self._faults.fire(FaultSite.CH_INSERT_AMBIGUOUS, table=DAILY_TABLE, rows=rows)
            return InsertResult(rows_written=len(rows), table=DAILY_TABLE, write_marker=marker)

    # -- reads -------------------------------------------------------------

    def _read_guard(self, scanned: int) -> None:
        self._faults.fire(FaultSite.CH_UNAVAILABLE, operation="read")
        self._faults.fire(FaultSite.CH_QUERY_TIMEOUT, operation="read", rows=scanned)
        self._budget(scanned)

    def scan_raw_deals(
        self,
        *,
        tenant_id: str,
        source_id: str,
        epoch_id: str,
        max_source_seq: int,
        max_write_marker: int,
    ) -> list[RawObservation]:
        with self._lock:
            self._read_guard(len(self._raw))
            selected = [
                stored.row
                for stored in self._raw
                if stored.row.scope == (tenant_id, source_id, epoch_id)
                and stored.row.event_type == "DealRecorded"
                and stored.row.source_seq <= max_source_seq
                and stored.write_marker <= max_write_marker
            ]
        # Sorted by a total key so the caller gets a repeatable order without
        # depending on physical layout (INV08-22, P08-R-11).
        selected.sort(key=lambda r: (r.deal_id or "", r.event_id, r.coordinate))
        return selected

    def event_id_conflicts(
        self, *, tenant_id: str, source_id: str, epoch_id: str, max_write_marker: int
    ) -> list[EventIdGroup]:
        with self._lock:
            self._read_guard(len(self._raw))
            groups: dict[str, list[str]] = defaultdict(list)
            for stored in self._raw:
                row = stored.row
                if (
                    row.scope == (tenant_id, source_id, epoch_id)
                    and row.event_type == "DealRecorded"
                    and stored.write_marker <= max_write_marker
                ):
                    groups[row.event_id].append(row.payload_sha256)
        out = []
        for event_id, hashes in groups.items():
            distinct = sorted(set(hashes))
            out.append(
                EventIdGroup(
                    event_id=event_id,
                    physical_observations=len(hashes),
                    distinct_hashes=len(distinct),
                    hashes=tuple(distinct),
                )
            )
        out.sort(key=lambda g: g.event_id)
        return out

    def deal_hash_conflicts(
        self, *, tenant_id: str, source_id: str, epoch_id: str, max_write_marker: int
    ) -> list[DealHashGroup]:
        with self._lock:
            self._read_guard(len(self._raw))
            groups: dict[str, list[str]] = defaultdict(list)
            for stored in self._raw:
                row = stored.row
                if (
                    row.scope == (tenant_id, source_id, epoch_id)
                    and row.event_type == "DealRecorded"
                    and row.deal_id
                    and stored.write_marker <= max_write_marker
                ):
                    groups[row.deal_id].append(row.deal_payload_hash or "")
        out = []
        for deal_id, hashes in groups.items():
            distinct = sorted(set(hashes))
            out.append(
                DealHashGroup(
                    deal_id=deal_id,
                    physical_observations=len(hashes),
                    distinct_deal_hashes=len(distinct),
                    hashes=tuple(distinct),
                )
            )
        out.sort(key=lambda g: g.deal_id)
        return out

    def count_deal_facts(self, *, build_id: str, tenant_id: str) -> tuple[int, int]:
        with self._lock:
            self._read_guard(len(self._facts))
            rows = [
                stored.row
                for stored in self._facts
                if stored.row.build_id == build_id and stored.row.tenant_id == tenant_id
            ]
        return len(rows), len({row.deal_id for row in rows})

    def read_deal_facts(
        self, *, build_id: str, tenant_id: str, source_id: str, epoch_id: str
    ) -> list[DealFactRow]:
        with self._lock:
            self._read_guard(len(self._facts))
            visible = self._visible_marker("default")
            rows = [
                stored.row
                for stored in self._facts
                if stored.row.build_id == build_id
                and (stored.row.tenant_id, stored.row.source_id, stored.row.epoch_id)
                == (tenant_id, source_id, epoch_id)
                and stored.write_marker <= visible
            ]
        rows.sort(key=lambda r: (r.account_id, r.executed_at, r.deal_id))
        return rows

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
    ) -> list[DailyRollupRow]:
        account_filter = set(accounts) if accounts else None
        with self._lock:
            self._read_guard(len(self._daily))
            visible = self._visible_marker("default")
            rows = []
            for stored in self._daily:
                row = stored.row
                if row.build_id != build_id:
                    continue
                if (row.tenant_id, row.source_id, row.epoch_id) != (tenant_id, source_id, epoch_id):
                    continue
                if stored.write_marker > visible:
                    continue
                if from_day is not None and row.trade_day < from_day:
                    continue
                if to_day is not None and row.trade_day > to_day:
                    continue
                if account_filter is not None and row.account_id not in account_filter:
                    continue
                rows.append(row)
        rows.sort(key=lambda r: (r.trade_day, r.account_id, r.symbol, r.money_currency))
        return rows

    def delete_build(self, *, build_id: str) -> int:
        with self._lock:
            before = len(self._facts) + len(self._daily)
            self._facts = [s for s in self._facts if s.row.build_id != build_id]
            self._daily = [s for s in self._daily if s.row.build_id != build_id]
            return before - (len(self._facts) + len(self._daily))

    # -- operations / observability ---------------------------------------

    def storage_stats(self) -> StorageStats:
        with self._lock:
            return StorageStats(
                table_rows={
                    RAW_TABLE: len(self._raw),
                    FACTS_TABLE: len(self._facts),
                    DAILY_TABLE: len(self._daily),
                },
                active_parts=dict(self._parts),
                bytes_on_disk=self._total_rows() * 256,
                disk_free_bytes=max(0, self._disk_capacity_rows - self._total_rows()) * 256,
                peak_memory_bytes=self._peak_memory_bytes,
            )

    def _visible_marker(self, reader: str) -> int:
        self._faults.fire(FaultSite.CH_READER_LAG, reader=reader)
        return self._reader_markers.get(reader, self._write_marker)

    def visible_write_marker(self, *, reader: str = "default") -> int:
        with self._lock:
            return self._visible_marker(reader)

    # -- test/ops controls -------------------------------------------------

    def set_reader_lag(self, *, reader: str = "default", visible_marker: int) -> None:
        """Pin a reader behind the writer, modelling replica lag (P08-T-07)."""
        with self._lock:
            self._reader_markers[reader] = visible_marker

    def clear_reader_lag(self, *, reader: str = "default") -> None:
        with self._lock:
            self._reader_markers.pop(reader, None)

    def set_disk_capacity_rows(self, rows: int) -> None:
        with self._lock:
            self._disk_capacity_rows = rows

    def optimize(self) -> None:
        """Collapse parts, as a background merge would.

        Exists so a test can prove a total is already correct *before* any
        merge runs and unchanged after one (P08-G-10, P08-U-06). The row
        contents are untouched: a plain ``MergeTree`` merge removes nothing.
        """
        with self._lock:
            for table in list(self._parts):
                self._parts[table] = 1 if self._table_rows(table) else 0

    def _table_rows(self, table: str) -> int:
        if table == RAW_TABLE:
            return len(self._raw)
        if table == FACTS_TABLE:
            return len(self._facts)
        if table == DAILY_TABLE:
            return len(self._daily)
        return 0

    def raw_row_count(self) -> int:
        with self._lock:
            return len(self._raw)

    def all_raw(self) -> list[RawObservation]:
        with self._lock:
            return [stored.row for stored in self._raw]

    def rows_read_total(self) -> int:
        return self._rows_read_total
