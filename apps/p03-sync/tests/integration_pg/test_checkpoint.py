"""Terminal checkpoint guards (design 6.4, group E, INV-02, INV-18).

The watermark is the one number that can cause permanent, undetectable data
loss. Once it moves past a snapshot, nothing will ever re-read that window. So
the publication transaction asserts everything at once, and each test here
removes exactly one of those preconditions and shows the watermark holds.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import (
    FilterScopeError,
    IncompleteScanError,
    StaleScanError,
)

pytestmark = pytest.mark.integration


async def _lease(sink, epoch, entity=Entity.DEALS, scope=FILTER_UNFILTERED):
    return await sink.acquire_lease(epoch.epoch_id, entity, scope, ttl_seconds=30)


async def _watermark(sink, epoch) -> int:
    return await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED)


@pytest.mark.whatif("WF3-006", "WF3-025")
async def test_a_complete_scan_advances_the_watermark_to_the_snapshot(
    sink, epoch, sessions
) -> None:
    """``W`` becomes ``S``, not the highest deal sequence.

    The snapshot bound is the correct value even though the last deal sits at
    5, because non-deal events consumed 6 through 12 and that window has now
    been read (design 6.2, fixture G03).
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1), deal_payload("D2", 5)],
        index=0,
        snapshot_seq=12,
        terminal=True,
    )
    await sink.persist_deal_page(lease, scan, page)
    await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=12)

    assert await _watermark(sink, epoch) == 12
    async with sessions() as session:
        status = (
            await session.execute(
                text("SELECT status FROM sync_runs WHERE scan_id = :s"),
                {"s": scan.scan_id},
            )
        ).scalar()
    assert status == "PUBLISHED"


@pytest.mark.whatif("WF3-040")
async def test_an_empty_but_complete_scan_may_advance(sink, epoch) -> None:
    """Zero new deals still proves the window was read."""
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    page = make_page(deal_scan_state(), [], index=0, snapshot_seq=40, terminal=True)
    await sink.persist_deal_page(lease, scan, page)
    await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=40)
    assert await _watermark(sink, epoch) == 40


@pytest.mark.whatif("WF3-054", "WF3-061")
async def test_a_scan_without_a_terminal_page_cannot_publish(sink, epoch) -> None:
    """The "99 of 100 pages" case, which must be a failure and not a near-miss.

    Every page committed cleanly; only the proof of completeness is missing.
    Advancing here would skip whatever page 100 held, forever (INV-02).
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=50, terminal=False
        ),
    )
    with pytest.raises(IncompleteScanError) as caught:
        await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=50)
    assert caught.value.safe_details["terminal_pages"] == 0
    assert await _watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-057")
async def test_a_scan_with_no_pages_at_all_cannot_publish(sink, epoch) -> None:
    """A watermark set without any receipt has no evidence behind it.

    This is the shape of an operator reaching in to "catch up" a lagging
    source: the guard rejects it because the proof is missing, not because the
    number looks wrong (WF3-057).
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    with pytest.raises(IncompleteScanError):
        await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=99)
    assert await _watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-058", "WF3-171")
async def test_a_filtered_scan_cannot_drive_the_global_watermark(sink, epoch) -> None:
    """A scan of one account says nothing about the rest of the dataset.

    Letting it advance the global watermark would skip every deal outside the
    filter (INV-18).
    """
    scoped = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    narrowed = scoped.__class__(
        epoch_id=scoped.epoch_id,
        entity=scoped.entity,
        filter_scope="account_id=A1",
        token=scoped.token,
        version=scoped.version,
        expires_at=scoped.expires_at,
        watermark=scoped.watermark,
    )
    scan = await sink.create_scan(scoped, start_watermark=0)
    with pytest.raises(FilterScopeError):
        await sink.publish_deal_checkpoint(
            narrowed, scan, old_watermark=0, new_watermark=10
        )
    assert await _watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-055")
async def test_the_watermark_never_moves_backwards(sink, epoch) -> None:
    """Monotonicity, checked before anything is attempted (INV-02)."""
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=30, terminal=True
        ),
    )
    await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=30)

    with pytest.raises(StaleScanError):
        await sink.publish_deal_checkpoint(lease, scan, old_watermark=30, new_watermark=20)
    assert await _watermark(sink, epoch) == 30


@pytest.mark.whatif("WF3-068")
async def test_compare_and_set_rejects_a_scan_whose_watermark_moved(
    sink, epoch, sessions
) -> None:
    """A scan's completeness proof only applies to the window it started from.

    If the watermark moved while this scan ran, its snapshot no longer sits
    immediately above the current position, so publishing it could leave a hole
    between the two.
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=30, terminal=True
        ),
    )
    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE sync_checkpoints SET watermark = 25 WHERE epoch_id = :e "
                "AND entity = 'deals'"
            ),
            {"e": epoch.epoch_id},
        )
        await session.commit()

    with pytest.raises(StaleScanError):
        await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=30)
    assert await _watermark(sink, epoch) == 25


@pytest.mark.whatif("WF3-060")
async def test_watermark_and_scan_status_move_in_one_transaction(
    sink, epoch, sessions
) -> None:
    """No split state between ``W`` and ``PUBLISHED``.

    The SQL oracle in design 15.3 asserts the inverse: a published scan id on a
    checkpoint whose run is not PUBLISHED. That query must return nothing.
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=30, terminal=True
        ),
    )
    await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=30)

    async with sessions() as session:
        inconsistent = (
            await session.execute(
                text(
                    "SELECT count(*) FROM sync_checkpoints c "
                    "JOIN sync_runs r ON r.scan_id = c.published_scan_id "
                    "WHERE c.published_scan_id IS NOT NULL AND r.status <> 'PUBLISHED'"
                )
            )
        ).scalar()
    assert inconsistent == 0


@pytest.mark.whatif("WF3-056", "WF3-196", "WF3-203")
async def test_publication_writes_its_audit_row_in_the_same_transaction(
    sink, epoch, sessions
) -> None:
    """An audit trail that can be missing for a checkpoint that happened is not
    an audit trail (WF3-203)."""
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=30, terminal=True
        ),
    )
    await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=30)

    async with sessions() as session:
        row = (
            await session.execute(
                text(
                    "SELECT event_type, safe_details FROM sync_audit "
                    "WHERE scan_id = :s AND event_type = 'DEAL_CHECKPOINT_PUBLISHED'"
                ),
                {"s": scan.scan_id},
            )
        ).mappings().one()
    assert row["safe_details"]["watermark_before"] == 0
    assert row["safe_details"]["watermark_after"] == 30


@pytest.mark.whatif("WF3-028")
async def test_a_quarantined_scan_cannot_publish(sink, epoch) -> None:
    """Unresolved integrity evidence blocks the watermark.

    Publishing while a conflict from this scan sits unexamined would bless a
    dataset we have already said we do not trust.
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=30, terminal=True
        ),
    )
    await sink.quarantine(
        epoch.epoch_id,
        scan.scan_id,
        Entity.DEALS,
        reason_code="IMMUTABLE_DEAL_CONFLICT",
        source_key="D1",
    )
    with pytest.raises(IncompleteScanError):
        await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=30)
    assert await _watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-049")
async def test_receipt_chain_with_a_hole_cannot_publish(sink, epoch, sessions) -> None:
    """Indices must run 0..n-1 with nothing missing.

    A hole means a page was never committed, so the terminal page proves only
    that the source stopped sending -- not that we stored what it sent
    (design 15.3 SQL oracle).
    """
    lease = await _lease(sink, epoch)
    scan = await sink.create_scan(lease, start_watermark=0)
    state = deal_scan_state()
    for index, terminal in ((0, False), (1, False), (2, True)):
        await sink.persist_deal_page(
            lease,
            scan,
            make_page(
                state,
                [deal_payload(f"D{index}", index + 1)],
                index=index,
                snapshot_seq=30,
                terminal=terminal,
            ),
        )
    async with sessions() as session:
        await session.execute(
            text("DELETE FROM sync_page_receipts WHERE scan_id = :s AND page_index = 1"),
            {"s": scan.scan_id},
        )
        await session.commit()

    with pytest.raises(IncompleteScanError):
        await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=30)
    assert await _watermark(sink, epoch) == 0
