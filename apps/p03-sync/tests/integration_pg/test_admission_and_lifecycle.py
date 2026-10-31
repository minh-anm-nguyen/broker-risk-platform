"""Admission, budgets, cancellation and backpressure (groups M, N, Q)."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from trading_sync.application.coordinator import SyncCoordinator
from trading_sync.application.killpoints import KillPointCrash
from trading_sync.application.sync_deals import DealSyncService
from trading_sync.config import load_settings
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import AdmissionRejectedError, ScanBudgetExceededError

pytestmark = pytest.mark.integration


async def watermark(sink, epoch) -> int:
    return await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED)


@pytest.mark.whatif("WF3-148", "WF3-235")
async def test_admission_rejects_beyond_capacity_instead_of_queueing(
    sink, settings, deal_service, snapshot_service, source
) -> None:
    """A thousand triggers must not become a thousand scans.

    Rejecting *is* the containment. An unbounded queue would accept them all
    and move the problem somewhere less visible, where it surfaces as an OOM
    rather than as a refused request (INV-15).
    """
    tight = load_settings(
        database_url=settings.database_url.get_secret_value(),
        sync={"worker_concurrency": 1, "scan_queue_capacity": 2},
    )
    coordinator = SyncCoordinator(sink, tight, deal_service, snapshot_service)
    source.add_deal("D1")

    coordinator._waiting = tight.sync.scan_queue_capacity
    with pytest.raises(AdmissionRejectedError) as caught:
        await coordinator._run_admitted(source, Entity.DEALS)
    assert caught.value.safe_details["capacity"] == 2
    assert caught.value.retryable is True

    # With the room free again, the same request is admitted and succeeds.
    coordinator._waiting = 0
    result = await coordinator.sync_entity(source, Entity.DEALS)
    assert result.ok


@pytest.mark.whatif("WF3-150", "WF3-047")
async def test_a_scan_that_outlives_its_budget_aborts_recoverably(
    sink, settings, source, epoch, guard, metrics, barriers
) -> None:
    """The budget is deliberately shorter than the source cursor TTL.

    Reaching the TTL mid-scan wastes the whole attempt, so the scan stops
    itself first and the next one restarts from the durable watermark.
    """
    clock = iter([0.0, 0.0, 10_000.0, 10_001.0, 10_002.0])
    ticking = DealSyncService(
        sink,
        settings,
        guard,
        metrics=metrics,
        barriers=barriers,
        monotonic=lambda: next(clock, 10_004.0),
    )
    source.page_size = 1
    for index in range(4):
        source.add_deal(f"D{index}")

    with pytest.raises(ScanBudgetExceededError) as caught:
        await ticking.run_once(source)
    assert caught.value.retryable is True
    assert await watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-146", "WF3-211")
async def test_the_source_is_never_read_ahead_of_the_sink(
    deal_service, source, sessions
) -> None:
    """One page in flight, always.

    The source counts pages served, and that count must equal the pages
    committed. The bound is what stops a connector faster than the database
    from growing an in-memory backlog.
    """
    source.page_size = 1
    for index in range(8):
        source.add_deal(f"D{index}")

    await deal_service.run_once(source)

    async with sessions() as session:
        receipts = (
            await session.execute(text("SELECT count(*) FROM sync_page_receipts"))
        ).scalar()
    assert source.pages_served["deals"] == receipts


@pytest.mark.whatif("WF3-157", "WF3-162")
async def test_cancellation_after_a_page_is_received_never_reports_success(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """A SIGTERM arriving mid-scan drains; it does not conclude.

    Cancellation cannot undo an already committed page, and it must not be
    mistaken for completion: the watermark stays put and the scan is recorded
    as aborted (design 18.4).
    """
    source.page_size = 1
    for index in range(5):
        source.add_deal(f"D{index}")

    async def cancel_now() -> None:
        raise asyncio.CancelledError()

    barriers.arm_action("K01_after_source_response", cancel_now, times=1)
    with pytest.raises(asyncio.CancelledError):
        await deal_service.run_once(source)
    barriers.assert_crossed("K01_after_source_response")

    assert await watermark(sink, epoch) == 0
    async with sessions() as session:
        status = (
            await session.execute(
                text("SELECT status FROM sync_runs ORDER BY started_at DESC LIMIT 1")
            )
        ).scalar()
    assert status == "ABORTED"


@pytest.mark.whatif("WF3-159")
async def test_a_crash_inside_the_page_transaction_leaves_no_half_page(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """Receipt written, COMMIT never reached: PostgreSQL rolls back both.

    Page rows and their receipt share one transaction precisely so this window
    cannot produce a receipt that overstates what was stored.
    """
    source.page_size = 3
    for index in range(6):
        source.add_deal(f"D{index}")

    barriers.arm_crash("K05_after_receipt_before_commit")
    with pytest.raises(KillPointCrash):
        await deal_service.run_once(source)
    barriers.assert_crossed("K05_after_receipt_before_commit")

    async with sessions() as session:
        deals = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
        receipts = (
            await session.execute(text("SELECT count(*) FROM sync_page_receipts"))
        ).scalar()
    assert deals == 0
    assert receipts == 0
    assert await watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-163")
async def test_a_committed_page_survives_the_source_failing_afterwards(
    deal_service, source, sink, epoch, sessions
) -> None:
    """The sink transaction depends only on the validated local batch.

    A connector restarting after page one committed cannot retroactively
    affect that page; it only stops the scan from completing.
    """
    source.page_size = 2
    for index in range(6):
        source.add_deal(f"D{index}")
    source.inject(Fault(mode="unavailable", at_page=1, entity=Entity.DEALS))

    with pytest.raises(Exception):
        await deal_service.run_once(source)
    source.assert_fired("unavailable")

    async with sessions() as session:
        stored = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
    assert stored == 2
    assert await watermark(sink, epoch) == 0

    source.clear_faults()
    await deal_service.run_once(source)
    async with sessions() as session:
        rows = (await session.execute(text("SELECT deal_id, payload_hash FROM deals"))).all()
    assert {row[0]: row[1] for row in rows} == source.oracle_deals()


@pytest.mark.whatif("WF3-197")
async def test_a_replay_is_not_recorded_as_new_inserts(
    deal_service, source, sessions
) -> None:
    """Replay work and stored facts are counted separately.

    That separation is what lets an operator see "we re-read this history"
    without it looking like the history grew.
    """
    for index in range(3):
        source.add_deal(f"D{index}")
    await deal_service.run_once(source)

    # A deliberate backfill: rewind the mark and read the window again.
    async with sessions() as session:
        await session.execute(
            text("UPDATE sync_checkpoints SET watermark = 0 WHERE entity = 'deals'")
        )
        await session.commit()
    await deal_service.run_once(source)

    async with sessions() as session:
        total = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
        inserted = (
            await session.execute(
                text("SELECT sum(record_count) FROM sync_runs WHERE entity='deals'")
            )
        ).scalar()
    assert total == 3
    assert int(inserted) == 3, "the replay was miscounted as new inserts"


@pytest.mark.whatif("WF3-200", "WF3-152")
async def test_repeated_cursor_expiry_is_bounded_by_the_retry_budget(
    sink, settings, deal_service, snapshot_service, source
) -> None:
    """A hot retry loop on 410 is what the budget exists to stop.

    Each attempt looks like a fresh first attempt, so an attempt counter alone
    would never trip. The sliding window is what eventually refuses.
    """
    coordinator = SyncCoordinator(
        sink, settings, deal_service, snapshot_service, sleep=_no_sleep
    )
    source.page_size = 1
    for index in range(4):
        source.add_deal(f"D{index}")
    source.inject(Fault(mode="snapshot_expired", at_page=0, times=99))

    result = await coordinator.sync_entity(source, Entity.DEALS)

    assert not result.ok
    assert result.reason_code == "SNAPSHOT_EXPIRED"
    # attempts are bounded by scan_retry_attempts, not by the source's mood
    assert result.attempts <= settings.sync.scan_retry_attempts + 1
    assert source.fired("snapshot_expired") == result.attempts


async def _no_sleep(_delay: float) -> None:
    """Collapse backoff so the test measures policy, not wall-clock patience."""
    return None
