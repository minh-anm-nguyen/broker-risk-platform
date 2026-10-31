"""Account and position snapshot publication (groups F and G, INV-06..INV-08).

Three properties are under test, and all three are about what a *reader* can
observe:

* a scan that did not finish changes nothing, so a reader keeps seeing the last
  complete snapshot (INV-06);
* publication is one transaction, so a reader sees the old set or the new set
  and never a mixture (INV-07);
* a row missing from a complete snapshot becomes ``ABSENT``, never ``CLOSED``.
  We know it was not in the listing; only the source knows whether the position
  closed (INV-08).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.support import (
    account_payload,
    make_page,
    position_payload,
    snapshot_scan_state,
)
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import DuplicateKeyConflict, FilterScopeError, StaleScanError

pytestmark = pytest.mark.integration


async def _stage_and_publish(
    sink, epoch, entity, pages, *, snapshot_seq: int, publish: bool = True
):
    """Run one full snapshot scan: lease, stage every page, then promote."""
    lease = await sink.acquire_lease(
        epoch.epoch_id, entity, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(
        lease, start_watermark=await sink.read_watermark(epoch.epoch_id, entity, FILTER_UNFILTERED)
    )
    state = snapshot_scan_state(entity)
    for index, (items, terminal) in enumerate(pages):
        page = make_page(
            state, items, index=index, snapshot_seq=snapshot_seq, terminal=terminal
        )
        await sink.stage_snapshot_page(lease, scan, page)
    if not publish:
        return lease, scan, None
    await sink.mark_scan_status(scan, "STAGED", snapshot_seq=snapshot_seq)
    result = await sink.publish_snapshot(lease, scan, snapshot_seq=snapshot_seq)
    await sink.release_lease(lease)
    return lease, scan, result


async def _rows(sessions, sql: str, **params):
    async with sessions() as session:
        return (await session.execute(text(sql), params)).mappings().all()


@pytest.mark.whatif("WF3-007")
async def test_a_complete_snapshot_publishes_every_row(sink, epoch, sessions) -> None:
    _, _, result = await _stage_and_publish(
        sink,
        epoch,
        Entity.ACCOUNTS,
        [([account_payload("A1"), account_payload("A2", balance="250.75")], True)],
        snapshot_seq=100,
    )
    assert result.rows_published == 2
    rows = await _rows(
        sessions, "SELECT account_id, balance, presence, snapshot_seq FROM accounts_current ORDER BY account_id"
    )
    assert [row["account_id"] for row in rows] == ["A1", "A2"]
    assert Decimal(rows[1]["balance"]) == Decimal("250.75")
    assert {row["presence"] for row in rows} == {"PRESENT"}
    assert {row["snapshot_seq"] for row in rows} == {100}


@pytest.mark.whatif("WF3-007")
async def test_an_empty_but_complete_snapshot_is_publishable(sink, epoch, sessions) -> None:
    """A source with no accounts is a valid answer, provided the scan is complete."""
    _, _, result = await _stage_and_publish(
        sink, epoch, Entity.ACCOUNTS, [([], True)], snapshot_seq=5
    )
    assert result.rows_published == 0
    assert await _rows(sessions, "SELECT * FROM accounts_current") == []


@pytest.mark.whatif("WF3-061", "WF3-232")
async def test_an_unfinished_scan_leaves_the_projection_untouched(
    sink, epoch, sessions
) -> None:
    """The partial-snapshot hazard, which staging exists to prevent.

    Page one of two has been staged durably. A reader must still see the
    previous complete snapshot, not 50% of a new one (fixture G14).
    """
    await _stage_and_publish(
        sink,
        epoch,
        Entity.ACCOUNTS,
        [([account_payload("A1", balance="100.00")], True)],
        snapshot_seq=10,
    )
    lease, scan, _ = await _stage_and_publish(
        sink,
        epoch,
        Entity.ACCOUNTS,
        [([account_payload("A1", balance="999.00")], False)],
        snapshot_seq=20,
        publish=False,
    )
    # Scoped to this scan: staging from the earlier published scan is retained
    # until the retention sweep, so an unscoped count would include it.
    staged = await _rows(
        sessions,
        "SELECT count(*) AS n FROM account_staging WHERE scan_id = :scan",
        scan=scan.scan_id,
    )
    assert staged[0]["n"] == 1

    current = await _rows(sessions, "SELECT balance, snapshot_seq FROM accounts_current")
    assert Decimal(current[0]["balance"]) == Decimal("100.00")
    assert current[0]["snapshot_seq"] == 10


@pytest.mark.whatif("WF3-065", "WF3-073")
async def test_a_row_missing_from_a_new_snapshot_becomes_absent_not_closed(
    sink, epoch, sessions
) -> None:
    """The INV-08 case, stated precisely.

    P2 was in snapshot 10 and is gone from snapshot 20. All we may record is
    that it was absent. Its ``source_status`` stays as the source last reported
    it, because inferring CLOSED would invent a business fact.
    """
    await _stage_and_publish(
        sink,
        epoch,
        Entity.POSITIONS,
        [([position_payload("P1"), position_payload("P2")], True)],
        snapshot_seq=10,
    )
    await _stage_and_publish(
        sink, epoch, Entity.POSITIONS, [([position_payload("P1")], True)], snapshot_seq=20
    )

    rows = await _rows(
        sessions,
        "SELECT position_id, presence, source_status, snapshot_seq FROM positions_current "
        "ORDER BY position_id",
    )
    by_id = {row["position_id"]: row for row in rows}
    assert by_id["P1"]["presence"] == "PRESENT"
    assert by_id["P2"]["presence"] == "ABSENT"
    assert by_id["P2"]["source_status"] == "OPEN"
    assert by_id["P2"]["snapshot_seq"] == 20


@pytest.mark.whatif("WF3-074")
async def test_a_source_reported_closed_status_is_preserved(sink, epoch, sessions) -> None:
    """CLOSED is recorded when and only when the source says so."""
    await _stage_and_publish(
        sink,
        epoch,
        Entity.POSITIONS,
        [([position_payload("P1", source_status="CLOSED")], True)],
        snapshot_seq=10,
    )
    rows = await _rows(
        sessions, "SELECT presence, source_status FROM positions_current"
    )
    assert rows[0]["source_status"] == "CLOSED"
    assert rows[0]["presence"] == "PRESENT"


@pytest.mark.whatif("WF3-064", "WF3-075")
async def test_publication_records_the_snapshot_it_came_from(sink, epoch, sessions) -> None:
    """A published snapshot is current *as of S*, not as of wall-clock now.

    The balance may already have moved on the source. Recording the sequence
    is what lets a consumer reason about freshness instead of assuming the
    value is live (section 10.6).
    """
    await _stage_and_publish(
        sink,
        epoch,
        Entity.ACCOUNTS,
        [([account_payload("A1", balance="100.00")], True)],
        snapshot_seq=200,
    )
    await _stage_and_publish(
        sink,
        epoch,
        Entity.ACCOUNTS,
        [([account_payload("A1", balance="150.00")], True)],
        snapshot_seq=201,
    )
    rows = await _rows(
        sessions, "SELECT balance, snapshot_seq FROM accounts_current WHERE account_id='A1'"
    )
    assert Decimal(rows[0]["balance"]) == Decimal("150.00")
    assert rows[0]["snapshot_seq"] == 201


@pytest.mark.whatif("WF3-062", "WF3-076")
async def test_conflicting_duplicate_keys_in_one_snapshot_are_refused(
    sink, epoch, sessions
) -> None:
    """Two balances for one account means the snapshot is not a coherent view.

    Rejecting beats picking one, because picking one is a silent, arbitrary
    choice about somebody's money.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    state = snapshot_scan_state(Entity.ACCOUNTS)
    await sink.stage_snapshot_page(
        lease,
        scan,
        make_page(
            state, [account_payload("A1", balance="1.00")], index=0, snapshot_seq=10, terminal=False
        ),
    )
    # A fresh state, so the validator does not catch the repeat first; the
    # collision then happens inside the staging transaction.
    second = snapshot_scan_state(Entity.ACCOUNTS)
    second.expected_page_index = 1
    with pytest.raises(DuplicateKeyConflict):
        await sink.stage_snapshot_page(
            lease,
            scan,
            make_page(
                second,
                [account_payload("A1", balance="2.00")],
                index=1,
                snapshot_seq=10,
                terminal=True,
            ),
        )
    assert await _rows(sessions, "SELECT * FROM accounts_current") == []


@pytest.mark.whatif("WF3-066", "WF3-079", "WF3-224")
async def test_a_filtered_scan_cannot_publish_a_full_snapshot(sink, epoch) -> None:
    """Absence is only inferable from a complete listing.

    A ``status=OPEN`` listing says nothing about positions outside the filter,
    so publishing it would mark every closed position absent on no evidence.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.POSITIONS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    page = make_page(
        snapshot_scan_state(Entity.POSITIONS),
        [position_payload("P1")],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await sink.stage_snapshot_page(lease, scan, page)
    await sink.mark_scan_status(scan, "STAGED", snapshot_seq=10)

    narrowed = lease.__class__(
        epoch_id=lease.epoch_id,
        entity=lease.entity,
        filter_scope="status=OPEN",
        token=lease.token,
        version=lease.version,
        expires_at=lease.expires_at,
        watermark=lease.watermark,
    )
    with pytest.raises(FilterScopeError):
        await sink.publish_snapshot(narrowed, scan, snapshot_seq=10)


@pytest.mark.whatif("WF3-068")
async def test_an_older_scan_cannot_overwrite_a_newer_publication(
    sink, epoch, sessions
) -> None:
    """A slow scan of snapshot 100 finishing after 110 published must not win.

    Allowing it would roll the current projection backwards in time, which no
    consumer can detect from the data alone.
    """
    old_lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    old_scan = await sink.create_scan(old_lease, start_watermark=0)
    await sink.stage_snapshot_page(
        old_lease,
        old_scan,
        make_page(
            snapshot_scan_state(Entity.ACCOUNTS),
            [account_payload("A1", balance="100.00")],
            index=0,
            snapshot_seq=100,
            terminal=True,
        ),
    )
    await sink.mark_scan_status(old_scan, "STAGED", snapshot_seq=100)
    await sink.release_lease(old_lease)

    await _stage_and_publish(
        sink,
        epoch,
        Entity.ACCOUNTS,
        [([account_payload("A1", balance="110.00")], True)],
        snapshot_seq=110,
    )

    newer_lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    stale = newer_lease.__class__(
        epoch_id=newer_lease.epoch_id,
        entity=newer_lease.entity,
        filter_scope=newer_lease.filter_scope,
        token=newer_lease.token,
        version=newer_lease.version,
        expires_at=newer_lease.expires_at,
        watermark=newer_lease.watermark,
    )
    with pytest.raises(StaleScanError):
        await sink.publish_snapshot(stale, old_scan, snapshot_seq=100)

    rows = await _rows(
        sessions, "SELECT balance, snapshot_seq FROM accounts_current WHERE account_id='A1'"
    )
    assert Decimal(rows[0]["balance"]) == Decimal("110.00")
    assert rows[0]["snapshot_seq"] == 110


@pytest.mark.whatif("WF3-067", "WF3-207")
async def test_concurrent_readers_see_old_or_new_never_a_mixture(
    sink, epoch, sessions
) -> None:
    """INV-07, exercised with a reader racing the promotion.

    Every observation must be internally consistent: all three accounts at the
    old balance or all three at the new. A single mixed read would mean the
    promotion was not atomic, and a consumer computing exposure across those
    rows would produce a number that was never true.
    """
    accounts = [account_payload(f"A{i}", balance="100.00") for i in range(3)]
    await _stage_and_publish(sink, epoch, Entity.ACCOUNTS, [(accounts, True)], snapshot_seq=10)

    updated = [account_payload(f"A{i}", balance="500.00") for i in range(3)]
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=10)
    await sink.stage_snapshot_page(
        lease,
        scan,
        make_page(
            snapshot_scan_state(Entity.ACCOUNTS),
            updated,
            index=0,
            snapshot_seq=20,
            terminal=True,
        ),
    )
    await sink.mark_scan_status(scan, "STAGED", snapshot_seq=20)

    observations: list[set[str]] = []

    async def reader() -> None:
        for _ in range(60):
            rows = await _rows(
                sessions, "SELECT balance FROM accounts_current ORDER BY account_id"
            )
            if rows:
                observations.append({Decimal(row["balance"]) for row in rows})
            await asyncio.sleep(0)

    reads = asyncio.create_task(reader())
    await sink.publish_snapshot(lease, scan, snapshot_seq=20)
    await reads

    assert observations, "reader never managed a read"
    old_set = {Decimal("100.00")}
    new_set = {Decimal("500.00")}
    for seen in observations:
        assert seen in (old_set, new_set), f"hybrid snapshot observed: {seen}"
