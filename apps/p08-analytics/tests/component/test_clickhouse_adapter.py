"""The ClickHouse model's own contract. What-if domains H, I, J, AB, AD, AE.

The properties asserted here are the ones P08's correctness arguments lean
on: duplicates survive, an ambiguous ACK can leave rows behind, readers can
lag the writer, and resource limits produce controlled errors.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from analytics.adapters.clickhouse_memory import (
    DAILY_TABLE,
    FACTS_TABLE,
    RAW_TABLE,
    InMemoryClickHouse,
)
from analytics.clock import ManualClock
from analytics.errors import AmbiguousWriteError, PoisonError, Reason, RetriableError
from analytics.faults import FaultPlan
from analytics.faults.killpoints import FaultSite
from analytics.ports.clickhouse import DailyRollupRow, DealFactRow, RawObservation

pytestmark = pytest.mark.component

HASH_A = "a" * 64
HASH_B = "b" * 64
NOW = datetime(2026, 10, 30, 9, 0, tzinfo=UTC)


def raw(
    *,
    event_id: str = "e1",
    deal_id: str | None = "D-1",
    payload_hash: str = HASH_A,
    deal_hash: str | None = HASH_A,
    offset: int = 0,
    source_seq: int = 1,
    tenant: str = "t",
    event_type: str = "DealRecorded",
    executed_at: datetime | None = NOW,
) -> RawObservation:
    return RawObservation(
        tenant_id=tenant,
        source_id="s",
        epoch_id="e",
        event_id=event_id,
        dedupe_key="d",
        kafka_record_key="k",
        event_type=event_type,
        schema_version=1,
        payload_sha256=payload_hash,
        deal_payload_hash=deal_hash,
        deal_id=deal_id,
        account_id="A-1",
        source_seq=source_seq,
        aggregate_version=source_seq,
        event_time=NOW,
        executed_at=executed_at,
        kafka_topic="broker.deals.v1",
        kafka_partition=0,
        kafka_offset=offset,
        payload_json="{}",
    )


def fact(*, build_id="b1", deal_id="D-1", tenant="t") -> DealFactRow:
    return DealFactRow(
        build_id=build_id,
        tenant_id=tenant,
        source_id="s",
        epoch_id="e",
        deal_id=deal_id,
        account_id="A-1",
        symbol="EURUSD",
        side="BUY",
        entry_kind="OUT",
        source_seq=1,
        executed_at=NOW,
        money_currency="USD",
        realized_pnl_gross=Decimal("1.00"),
        commission=Decimal("-0.10"),
        swap=Decimal("0.00"),
        volume_lots=Decimal("1.00"),
        fee_coverage="COMMISSION_AND_SWAP_ONLY",
        deal_payload_hash=HASH_A,
        event_id="e1",
    )


def daily(*, build_id="b1", tenant="t") -> DailyRollupRow:
    return DailyRollupRow(
        build_id=build_id,
        tenant_id=tenant,
        source_id="s",
        epoch_id="e",
        trade_day=NOW.date(),
        account_id="A-1",
        symbol="EURUSD",
        money_currency="USD",
        deal_count=1,
        realized_pnl_gross=Decimal("1.00"),
        commission=Decimal("-0.10"),
        swap=Decimal("0.00"),
        volume_lots=Decimal("1.00"),
    )


@pytest.fixture
def ch() -> InMemoryClickHouse:
    return InMemoryClickHouse(clock=ManualClock())


class TestAppendOnlyRaw:
    def test_duplicates_are_stored_not_collapsed(self, ch):
        """``MergeTree`` has no uniqueness constraint. INV08-23, section 5.1.

        A build that forgot to dedup therefore fails here exactly as it would
        in production.
        """
        ch.insert_raw([raw(offset=0)])
        ch.insert_raw([raw(offset=5)])
        assert ch.raw_row_count() == 2

    def test_ingested_at_is_filled_in(self, ch):
        ch.insert_raw([raw()])
        assert ch.all_raw()[0].ingested_at is not None

    def test_scan_is_ordered_independently_of_insertion(self, ch):
        ch.insert_raw([raw(deal_id="D-3", offset=0, source_seq=3)])
        ch.insert_raw([raw(deal_id="D-1", offset=1, source_seq=1)])
        ch.insert_raw([raw(deal_id="D-2", offset=2, source_seq=2)])
        rows = ch.scan_raw_deals(
            tenant_id="t", source_id="s", epoch_id="e", max_source_seq=99, max_write_marker=99
        )
        assert [r.deal_id for r in rows] == ["D-1", "D-2", "D-3"]

    def test_scan_respects_the_source_watermark(self, ch):
        ch.insert_raw([raw(deal_id="D-1", source_seq=1), raw(deal_id="D-2", source_seq=9, offset=1)])
        rows = ch.scan_raw_deals(
            tenant_id="t", source_id="s", epoch_id="e", max_source_seq=5, max_write_marker=99
        )
        assert [r.deal_id for r in rows] == ["D-1"]

    def test_scan_respects_the_pinned_write_marker(self, ch):
        """P08-R-06: a pinned marker keeps the scan repeatable mid-ingest."""
        ch.insert_raw([raw(deal_id="D-1")])
        pinned = ch.visible_write_marker()
        ch.insert_raw([raw(deal_id="D-2", offset=1)])
        rows = ch.scan_raw_deals(
            tenant_id="t", source_id="s", epoch_id="e", max_source_seq=99,
            max_write_marker=pinned,
        )
        assert [r.deal_id for r in rows] == ["D-1"]

    def test_scope_isolates_tenants(self, ch):
        ch.insert_raw([raw(tenant="t"), raw(tenant="rival", offset=1)])
        rows = ch.scan_raw_deals(
            tenant_id="t", source_id="s", epoch_id="e", max_source_seq=99, max_write_marker=99
        )
        assert len(rows) == 1
        assert rows[0].tenant_id == "t"

    def test_markers_are_excluded_from_deal_scans(self, ch):
        ch.insert_raw(
            [raw(event_type="AccountSnapshotPublished", deal_id=None, deal_hash=None,
                 executed_at=None)]
        )
        rows = ch.scan_raw_deals(
            tenant_id="t", source_id="s", epoch_id="e", max_source_seq=99, max_write_marker=99
        )
        assert rows == []


class TestInsertValidation:
    def test_rejects_a_malformed_payload_hash(self, ch):
        """``FixedString(64)`` proves nothing about hashing. Section 5.2."""
        with pytest.raises(PoisonError) as excinfo:
            ch.insert_raw([raw(payload_hash="short")])
        assert excinfo.value.reason is Reason.HASH_MALFORMED

    def test_rejects_a_deal_row_without_a_deal_hash(self, ch):
        with pytest.raises(PoisonError) as excinfo:
            ch.insert_raw([raw(deal_hash=None)])
        assert excinfo.value.reason is Reason.HASH_MALFORMED

    def test_rejects_a_deal_row_without_a_deal_id(self, ch):
        with pytest.raises(PoisonError) as excinfo:
            ch.insert_raw([raw(deal_id=None)])
        assert excinfo.value.reason is Reason.FIELD_MISSING

    def test_rejects_a_naive_executed_at(self, ch):
        with pytest.raises(PoisonError) as excinfo:
            ch.insert_raw([raw(executed_at=datetime(2026, 10, 30, 9, 0))])
        assert excinfo.value.reason is Reason.FIELD_INVALID

    def test_nothing_is_stored_when_a_row_is_refused(self, ch):
        with pytest.raises(PoisonError):
            ch.insert_raw([raw(), raw(offset=1, payload_hash="bad")])
        assert ch.raw_row_count() == 0


class TestAckAmbiguity:
    def test_ambiguous_ack_can_leave_rows_persisted(self):
        """Section 3.2's worst case: durable write, lost response.

        The caller must see an error *and* the rows must be there, because
        that is the real failure mode. Committing the offset here would be the
        one unrecoverable mistake available.
        """
        plan = FaultPlan()
        plan.raise_at(FaultSite.CH_INSERT_AMBIGUOUS, AmbiguousWriteError())
        ch = InMemoryClickHouse(faults=plan, clock=ManualClock())
        with pytest.raises(AmbiguousWriteError):
            ch.insert_raw([raw()])
        assert ch.raw_row_count() == 1
        plan.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)

    def test_server_rejection_stores_nothing(self):
        """P08-H-01: a refused batch keeps the Kafka offset safe."""
        plan = FaultPlan()
        plan.raise_at(
            FaultSite.CH_INSERT_REJECT, PoisonError(Reason.INSERT_REJECTED, "type mismatch")
        )
        ch = InMemoryClickHouse(faults=plan, clock=ManualClock())
        with pytest.raises(PoisonError):
            ch.insert_raw([raw()])
        assert ch.raw_row_count() == 0

    def test_partial_insert_is_visible_as_a_short_write(self):
        """P08-E-07: an unknown subset landed, so the batch is uncertain."""
        plan = FaultPlan()
        plan.call_at(FaultSite.CH_INSERT_PARTIAL, lambda ctx: None)
        ch = InMemoryClickHouse(faults=plan, clock=ManualClock())
        result = ch.insert_raw([raw(offset=i, deal_id=f"D-{i}") for i in range(4)])
        assert result.rows_written < 4
        plan.assert_fired(FaultSite.CH_INSERT_PARTIAL)

    def test_unavailable_backend_is_retriable(self):
        plan = FaultPlan()
        plan.raise_at(
            FaultSite.CH_UNAVAILABLE, RetriableError(Reason.BACKEND_UNAVAILABLE, "down")
        )
        ch = InMemoryClickHouse(faults=plan, clock=ManualClock())
        with pytest.raises(RetriableError):
            ch.insert_raw([raw()])

    def test_elapsed_deadline_is_refused(self, ch):
        with pytest.raises(RetriableError) as excinfo:
            ch.insert_raw([raw()], deadline_ms=0)
        assert excinfo.value.reason is Reason.DEADLINE_EXCEEDED


class TestReaderVisibility:
    def test_a_lagging_reader_sees_less_than_the_writer(self, ch):
        """P08-T-07: replica lag, as a deterministic condition."""
        ch.insert_deal_facts([fact()])
        ch.set_reader_lag(visible_marker=0)
        assert ch.read_deal_facts(build_id="b1", tenant_id="t", source_id="s", epoch_id="e") == []
        ch.clear_reader_lag()
        assert len(
            ch.read_deal_facts(build_id="b1", tenant_id="t", source_id="s", epoch_id="e")
        ) == 1

    def test_visible_marker_tracks_the_writer_by_default(self, ch):
        ch.insert_raw([raw()])
        assert ch.visible_write_marker() == ch.write_marker


class TestConflictQueries:
    def test_event_id_group_reports_distinct_hashes(self, ch):
        ch.insert_raw([raw(event_id="e1", payload_hash=HASH_A)])
        ch.insert_raw([raw(event_id="e1", payload_hash=HASH_B, offset=1)])
        groups = ch.event_id_conflicts(
            tenant_id="t", source_id="s", epoch_id="e", max_write_marker=99
        )
        assert groups[0].physical_observations == 2
        assert groups[0].distinct_hashes == 2

    def test_identical_redelivery_reports_one_hash(self, ch):
        ch.insert_raw([raw(offset=0)])
        ch.insert_raw([raw(offset=1)])
        groups = ch.event_id_conflicts(
            tenant_id="t", source_id="s", epoch_id="e", max_write_marker=99
        )
        assert groups[0].physical_observations == 2
        assert groups[0].distinct_hashes == 1

    def test_deal_hash_group_reports_distinct_content(self, ch):
        ch.insert_raw([raw(event_id="e1", deal_hash=HASH_A)])
        ch.insert_raw([raw(event_id="e2", deal_hash=HASH_B, offset=1)])
        groups = ch.deal_hash_conflicts(
            tenant_id="t", source_id="s", epoch_id="e", max_write_marker=99
        )
        assert groups[0].distinct_deal_hashes == 2


class TestFactCounts:
    def test_reports_physical_and_unique_separately(self, ch):
        """Either number alone can agree with a wrong set. Section 6.3."""
        ch.insert_deal_facts([fact(deal_id="D-1"), fact(deal_id="D-1")])
        assert ch.count_deal_facts(build_id="b1", tenant_id="t") == (2, 1)

    def test_counts_are_scoped_to_the_build(self, ch):
        ch.insert_deal_facts([fact(build_id="b1"), fact(build_id="b2", deal_id="D-2")])
        assert ch.count_deal_facts(build_id="b1", tenant_id="t") == (1, 1)


class TestResourceBounds:
    def test_full_disk_stops_ingestion_without_dropping(self, ch):
        """Section 8.4: stop and keep the offset; never drop the batch."""
        ch.set_disk_capacity_rows(1)
        ch.insert_raw([raw()])
        with pytest.raises(RetriableError) as excinfo:
            ch.insert_raw([raw(offset=1)])
        assert excinfo.value.reason is Reason.STORAGE_FULL
        assert ch.raw_row_count() == 1

    def test_query_row_budget_is_enforced(self):
        ch = InMemoryClickHouse(clock=ManualClock(), max_query_rows=2)
        ch.insert_raw([raw(offset=i, deal_id=f"D-{i}") for i in range(3)])
        with pytest.raises(RetriableError) as excinfo:
            ch.scan_raw_deals(
                tenant_id="t", source_id="s", epoch_id="e", max_source_seq=99,
                max_write_marker=99,
            )
        assert excinfo.value.reason is Reason.BUDGET_EXCEEDED

    def test_storage_stats_report_parts_per_table(self, ch):
        ch.insert_raw([raw()])
        ch.insert_raw([raw(offset=1)])
        ch.insert_deal_facts([fact()])
        ch.insert_daily_rollups([daily()])
        stats = ch.storage_stats()
        assert stats.active_parts[RAW_TABLE] == 2
        assert stats.active_parts[FACTS_TABLE] == 1
        assert stats.table_rows[DAILY_TABLE] == 1

    def test_optimize_collapses_parts_without_removing_rows(self, ch):
        """A plain ``MergeTree`` merge removes nothing. ADR08-02.

        This is what lets a test prove a total is already correct before any
        merge and unchanged after one (P08-U-06).
        """
        ch.insert_raw([raw(offset=0)])
        ch.insert_raw([raw(offset=1)])
        before = ch.raw_row_count()
        ch.optimize()
        assert ch.storage_stats().active_parts[RAW_TABLE] == 1
        assert ch.raw_row_count() == before


class TestDeleteBuild:
    def test_removes_only_the_named_build(self, ch):
        ch.insert_deal_facts([fact(build_id="b1"), fact(build_id="b2", deal_id="D-2")])
        ch.insert_daily_rollups([daily(build_id="b1"), daily(build_id="b2")])
        removed = ch.delete_build(build_id="b1")
        assert removed == 2
        assert ch.count_deal_facts(build_id="b2", tenant_id="t") == (1, 1)
        assert ch.count_deal_facts(build_id="b1", tenant_id="t") == (0, 0)
