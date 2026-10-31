"""Deal persistence: idempotency, conflicts and isolation (groups C, I).

The two outcomes this layer must keep apart are INV-04 and INV-05. The same
deal arriving twice with the same content is a no-op; the same deal arriving
with different money is an incident that stops the scan and leaves the stored
value alone. Collapsing them in either direction is a serious failure: treat a
conflict as a duplicate and a commission gets rewritten, treat a duplicate as
a conflict and every replay becomes an outage.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import ImmutableDealConflict, ReceiptConflict

pytestmark = pytest.mark.integration


async def _lease_and_scan(sink, epoch, *, watermark: int = 0):
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=watermark)
    return lease, scan


async def _count(sessions, sql: str, **params) -> int:
    async with sessions() as session:
        return int((await session.execute(text(sql), params)).scalar() or 0)


@pytest.mark.whatif("WF3-006")
async def test_a_page_of_deals_is_stored_with_a_receipt(sink, epoch, sessions) -> None:
    lease, scan = await _lease_and_scan(sink, epoch)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1), deal_payload("D2", 5)],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    result = await sink.persist_deal_page(lease, scan, page)
    assert result.inserted == 2
    assert await _count(sessions, "SELECT count(*) FROM deals") == 2
    assert await _count(sessions, "SELECT count(*) FROM sync_page_receipts") == 1


@pytest.mark.whatif("WF3-027", "WF3-169")
async def test_replaying_a_scan_inserts_nothing_new(sink, epoch, sessions) -> None:
    """Three identical replays leave one row per deal.

    This is the property that makes crash recovery safe: a restart re-reads
    from the durable watermark, and whatever was already written absorbs the
    replay without duplicating (section 10.2, fixture G08).
    """
    for _ in range(3):
        lease, scan = await _lease_and_scan(sink, epoch)
        page = make_page(
            deal_scan_state(),
            [deal_payload("D1", 1), deal_payload("D2", 2)],
            index=0,
            snapshot_seq=10,
            terminal=True,
        )
        await sink.persist_deal_page(lease, scan, page)
        await sink.release_lease(lease)

    assert await _count(sessions, "SELECT count(*) FROM deals") == 2
    async with sessions() as session:
        hashes = (
            await session.execute(text("SELECT DISTINCT payload_hash FROM deals"))
        ).scalars().all()
    assert len(hashes) == 2


@pytest.mark.whatif("WF3-028", "WF3-098")
async def test_same_deal_id_with_different_money_is_refused(sink, epoch, sessions) -> None:
    """The stored value wins and the scan stops (INV-05).

    ``ON CONFLICT DO UPDATE`` here would rewrite the commission and leave no
    evidence, which is the single highest-RPN failure in the design's FMEA.
    """
    lease, scan = await _lease_and_scan(sink, epoch)
    first = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1, commission="-2.50")],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await sink.persist_deal_page(lease, scan, first)
    await sink.release_lease(lease)

    lease2, scan2 = await _lease_and_scan(sink, epoch)
    conflicting = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1, commission="-25.00")],
        index=0,
        snapshot_seq=11,
        terminal=True,
    )
    with pytest.raises(ImmutableDealConflict) as caught:
        await sink.persist_deal_page(lease2, scan2, conflicting)

    async with sessions() as session:
        stored = (
            await session.execute(
                text("SELECT commission FROM deals WHERE deal_id = 'D1'")
            )
        ).scalar()
    assert Decimal(stored) == Decimal("-2.50")
    # The incident names the key by digest, never by value (design 11.6).
    assert "source_key_hash" in caught.value.safe_details
    assert "D1" not in str(caught.value)


@pytest.mark.whatif("WF3-028", "WF3-102")
async def test_a_refused_page_commits_nothing_at_all(sink, epoch, sessions) -> None:
    """One bad row rolls back the whole page, including its receipt.

    The conflict has to come from a row a *previous* scan stored: within one
    scan the validator catches a repeated key first, which is the earlier and
    better place to catch it. Here the page is internally consistent and only
    collides with history, so the refusal happens in the sink.

    The new deal sharing that page is the point of the test. A half-committed
    page would store it and leave a receipt chain claiming more was persisted
    than actually was -- and the completeness proof rests on that chain.
    """
    first_lease, first_scan = await _lease_and_scan(sink, epoch)
    await sink.persist_deal_page(
        first_lease,
        first_scan,
        make_page(
            deal_scan_state(),
            [deal_payload("D1", 1, commission="-2.50")],
            index=0,
            snapshot_seq=10,
            terminal=True,
        ),
    )
    await sink.publish_deal_checkpoint(
        first_lease, first_scan, old_watermark=0, new_watermark=10
    )
    await sink.release_lease(first_lease)

    lease, scan = await _lease_and_scan(sink, epoch, watermark=10)
    bad_page = make_page(
        deal_scan_state(start_watermark=10),
        [deal_payload("D2", 11), deal_payload("D1", 12, commission="-99.00")],
        index=0,
        snapshot_seq=20,
        terminal=True,
    )
    with pytest.raises(ImmutableDealConflict):
        await sink.persist_deal_page(lease, scan, bad_page)

    # D1 untouched, D2 never stored, no receipt for the refused page.
    assert await _count(sessions, "SELECT count(*) FROM deals") == 1
    assert await _count(sessions, "SELECT count(*) FROM deals WHERE deal_id='D2'") == 0
    assert (
        await _count(
            sessions,
            "SELECT count(*) FROM sync_page_receipts WHERE scan_id = :scan",
            scan=scan.scan_id,
        )
        == 0
    )
    assert await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED) == 10


@pytest.mark.whatif("WF3-053")
async def test_recommitting_an_identical_page_is_acknowledged_not_reapplied(
    sink, epoch, sessions
) -> None:
    """How an ambiguous COMMIT resolves (design 6.5).

    The connection dropped after PostgreSQL committed but before the
    acknowledgement arrived, so the caller does not know what happened. The
    retry finds a receipt with an identical hash and is told so, rather than
    inserting again or being told it failed.
    """
    lease, scan = await _lease_and_scan(sink, epoch)
    page = make_page(
        deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=True
    )
    first = await sink.persist_deal_page(lease, scan, page)
    assert first.already_committed is False

    replay = await sink.persist_deal_page(lease, scan, page)
    assert replay.already_committed is True
    assert replay.inserted == 0
    assert await _count(sessions, "SELECT count(*) FROM sync_page_receipts") == 1


@pytest.mark.whatif("WF3-059")
async def test_same_page_index_with_different_content_is_a_receipt_conflict(
    sink, epoch
) -> None:
    """An honest replay is byte-identical; a different hash means the source
    changed its answer, so the original receipt stands."""
    lease, scan = await _lease_and_scan(sink, epoch)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=False
        ),
    )
    with pytest.raises(ReceiptConflict):
        await sink.persist_deal_page(
            lease,
            scan,
            make_page(
                deal_scan_state(),
                [deal_payload("D2", 2)],
                index=0,
                snapshot_seq=10,
                terminal=False,
            ),
        )


@pytest.mark.whatif("WF3-097")
async def test_two_workers_inserting_the_same_deal_yield_one_row(
    sink, epoch, sessions
) -> None:
    """Concurrent identical inserts resolve to exactly one row, no leaked error.

    The unique key does the work; both transactions see a consistent result
    because the content comparison is taken under a row share lock.
    """
    lease, scan = await _lease_and_scan(sink, epoch)
    page = make_page(
        deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=True
    )
    second_lease, second_scan = lease, await sink.create_scan(lease, start_watermark=0)

    results = await asyncio.gather(
        sink.persist_deal_page(lease, scan, page),
        sink.persist_deal_page(second_lease, second_scan, page),
        return_exceptions=True,
    )
    assert await _count(sessions, "SELECT count(*) FROM deals WHERE deal_id='D1'") == 1
    for outcome in results:
        if isinstance(outcome, BaseException):
            assert getattr(outcome, "reason_code", None) in {
                "DB_TRANSIENT",
                "IMMUTABLE_DEAL_CONFLICT",
                "STALE_SCAN",
            }


@pytest.mark.whatif("WF3-100", "WF3-181")
async def test_injection_shaped_identifiers_round_trip_exactly(sink, epoch, sessions) -> None:
    """Ids reach SQL only as bound parameters, so these are data, not syntax.

    The payload below would be catastrophic under string interpolation; here it
    is simply an account id that comes back byte-identical.
    """
    hostile = "A1'; DROP TABLE deals; --"
    lease, scan = await _lease_and_scan(sink, epoch)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D'1", 1, account_id=hostile)],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await sink.persist_deal_page(lease, scan, page)

    async with sessions() as session:
        row = (
            await session.execute(
                text("SELECT deal_id, account_id FROM deals")
            )
        ).one()
    assert row[0] == "D'1"
    assert row[1] == hostile
    assert await _count(sessions, "SELECT count(*) FROM deals") == 1


@pytest.mark.whatif("WF3-015", "WF3-182")
async def test_the_same_deal_id_in_two_epochs_is_two_rows(sink, source, sessions) -> None:
    """Epoch isolation, the reason the primary key is composite.

    After a simulator reset the ids start over. Keyed on ``deal_id`` alone, the
    first replayed id would collide with the previous run's history and one
    run's money would overwrite the other's (INV-14).
    """
    first_identity = await source.read_meta(source.source_id)
    first_epoch = await sink.register_epoch(first_identity)
    await sink.approve_epoch(first_epoch.epoch_id, reason="TEST")

    lease, scan = await _lease_and_scan(sink, first_epoch)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(),
            [deal_payload("D1", 1, commission="-2.50")],
            index=0,
            snapshot_seq=10,
            terminal=True,
        ),
    )
    await sink.release_lease(lease)

    source.reset(run_id="run_0002", generation=0)
    # The previous epoch's lease was released above; the new epoch has its own
    # checkpoint rows, so this acquisition is for a different scope entirely.
    second_identity = await source.read_meta(source.source_id)
    second_epoch = await sink.register_epoch(second_identity)
    await sink.approve_epoch(second_epoch.epoch_id, reason="TEST")

    lease2, scan2 = await _lease_and_scan(sink, second_epoch)
    await sink.persist_deal_page(
        lease2,
        scan2,
        make_page(
            deal_scan_state(),
            [deal_payload("D1", 1, commission="-7.75")],
            index=0,
            snapshot_seq=10,
            terminal=True,
            run_id="run_0002",
        ),
    )

    async with sessions() as session:
        rows = (
            await session.execute(
                text("SELECT epoch_id, commission FROM deals WHERE deal_id='D1' ORDER BY commission")
            )
        ).all()
    assert len(rows) == 2
    assert {Decimal(row[1]) for row in rows} == {Decimal("-7.75"), Decimal("-2.50")}
