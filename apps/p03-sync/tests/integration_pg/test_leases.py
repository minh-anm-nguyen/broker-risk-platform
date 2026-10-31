"""Lease acquisition, heartbeat and fencing (design group K, INV-09).

The lease is what makes "one scan per scope" true in the presence of a paused
process, a slow network or a restarted container. Its guarantee is narrow and
specific: a worker that lost the lease may have *already committed pages* --
those are idempotent and harmless -- but it must not publish, because its view
of the snapshot is no longer authoritative.

Every decision here is made by the database. The lease predicate uses
``clock_timestamp()``, so a worker with a skewed clock cannot talk itself into
ownership (WF3-126).
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text

from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import LeaseLostError, StaleScanError

pytestmark = pytest.mark.integration


@pytest.mark.whatif("WF3-121")
async def test_only_one_worker_wins_a_free_lease(sink, epoch) -> None:
    """A single conditional UPDATE resolves the race; the loser gets nothing.

    No application lock is involved, which matters because an application lock
    would not survive the loser being in a different process.
    """
    first, second = await asyncio.gather(
        sink.acquire_lease(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30),
        sink.acquire_lease(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30),
    )
    winners = [lease for lease in (first, second) if lease is not None]
    assert len(winners) == 1


@pytest.mark.whatif("WF3-121")
async def test_a_second_acquisition_is_refused_while_the_lease_is_live(sink, epoch) -> None:
    held = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert held is not None
    assert (
        await sink.acquire_lease(
            epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
        )
        is None
    )


@pytest.mark.whatif("WF3-122")
async def test_takeover_after_expiry_fences_the_old_worker(sink, epoch, sessions) -> None:
    """The central fencing case: A pauses past its TTL, B takes over, A returns.

    A's lease version is stale, so its commit guard refuses. Had it been
    allowed to publish, it would have written a checkpoint derived from a scan
    B has since superseded.
    """
    worker_a = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert worker_a is not None

    # Expire A's lease by moving its deadline into the past, which is what a
    # 40-second pause would achieve without the test waiting 40 seconds.
    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE sync_checkpoints SET lease_until = clock_timestamp() "
                "- interval '1 second' WHERE epoch_id = :epoch"
            ),
            {"epoch": epoch.epoch_id},
        )
        await session.commit()

    worker_b = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert worker_b is not None
    assert worker_b.version > worker_a.version

    with pytest.raises(LeaseLostError):
        await sink.heartbeat_lease(worker_a, ttl_seconds=30)
    with pytest.raises(LeaseLostError):
        await sink.create_scan(worker_a, start_watermark=0)


@pytest.mark.whatif("WF3-123", "WF3-129")
async def test_a_fenced_worker_cannot_commit_a_page(sink, epoch, sessions) -> None:
    """A page already in hand must not reach the sink after a takeover.

    The guard is at the commit, deliberately later than the fetch, because the
    fetch happened while the lease was still valid.
    """
    worker_a = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(worker_a, start_watermark=0)
    page = make_page(
        deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=True
    )

    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE sync_checkpoints SET lease_until = clock_timestamp() "
                "- interval '1 second' WHERE epoch_id = :epoch"
            ),
            {"epoch": epoch.epoch_id},
        )
        await session.commit()
    worker_b = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert worker_b is not None

    with pytest.raises(LeaseLostError):
        await sink.persist_deal_page(worker_a, scan, page)

    async with sessions() as session:
        count = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
    assert count == 0


@pytest.mark.whatif("WF3-127")
async def test_a_fenced_worker_cannot_publish_a_checkpoint(sink, epoch, sessions) -> None:
    """Zero stale terminal checkpoints, even after a full successful scan."""
    worker_a = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(worker_a, start_watermark=0)
    page = make_page(
        deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=True
    )
    await sink.persist_deal_page(worker_a, scan, page)

    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE sync_checkpoints SET lease_token = gen_random_uuid(), "
                "lease_version = lease_version + 1, "
                "lease_until = clock_timestamp() + interval '30 seconds' "
                "WHERE epoch_id = :epoch"
            ),
            {"epoch": epoch.epoch_id},
        )
        await session.commit()

    with pytest.raises(LeaseLostError):
        await sink.publish_deal_checkpoint(
            worker_a, scan, old_watermark=0, new_watermark=10
        )
    assert await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED) == 0


@pytest.mark.whatif("WF3-125")
async def test_heartbeat_extends_the_same_lease_without_bumping_the_version(
    sink, epoch
) -> None:
    """A renewal is not a takeover.

    Bumping the version on heartbeat would invalidate the holder's own fencing
    token and make every long scan fail at its final commit.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    renewed = await sink.heartbeat_lease(lease, ttl_seconds=30)
    assert renewed.version == lease.version
    assert renewed.token == lease.token
    assert renewed.expires_at >= lease.expires_at


@pytest.mark.whatif("WF3-126")
async def test_lease_deadline_comes_from_the_database_clock(sink, epoch, sessions) -> None:
    """A worker's own clock never decides ownership.

    With a two-minute skew, a worker trusting its own clock would either hold a
    dead lease or surrender a live one.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    async with sessions() as session:
        row = (
            await session.execute(
                text(
                    "SELECT lease_until, clock_timestamp() AS now FROM sync_checkpoints "
                    "WHERE epoch_id = :epoch AND entity = 'deals'"
                ),
                {"epoch": epoch.epoch_id},
            )
        ).mappings().one()
    remaining = (row["lease_until"] - row["now"]).total_seconds()
    assert 0 < remaining <= 30
    assert lease.expires_at == row["lease_until"]


@pytest.mark.whatif("WF3-161")
async def test_release_by_a_fenced_worker_does_not_free_the_new_owner(
    sink, epoch, sessions
) -> None:
    """A departing worker must not clear someone else's claim on its way out.

    A container restarting while its lease is still valid is the common case;
    an unconditional release would hand the scope to a third worker while the
    rightful owner is mid-scan.
    """
    worker_a = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    async with sessions() as session:
        await session.execute(
            text(
                "UPDATE sync_checkpoints SET lease_until = clock_timestamp() "
                "- interval '1 second' WHERE epoch_id = :epoch"
            ),
            {"epoch": epoch.epoch_id},
        )
        await session.commit()
    worker_b = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )

    await sink.release_lease(worker_a)

    # B still owns it: A's release matched no row.
    assert (
        await sink.acquire_lease(
            epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
        )
        is None
    )
    assert await sink.heartbeat_lease(worker_b, ttl_seconds=30) is not None


@pytest.mark.whatif("WF3-124")
async def test_leases_are_independent_per_entity(sink, epoch) -> None:
    """Accounts, positions and deals are separate scopes.

    One scan per entity is the concurrency rule, not one scan per source, so a
    deal scan must not block an account scan (design 8.4).
    """
    deals = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    accounts = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert deals is not None and accounts is not None
    assert deals.entity is not accounts.entity


@pytest.mark.whatif("WF3-018", "WF3-019")
async def test_a_blocked_epoch_clears_the_lease_and_stops_commits(
    sink, epoch
) -> None:
    """Fencing an epoch must reach an in-flight worker at its next guard.

    Waiting for the heartbeat to notice would leave a window in which the
    worker could still publish against a dataset we have disowned.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.block_epoch(epoch.epoch_id, reason_code="SOURCE_EPOCH_CHANGED")

    page = make_page(
        deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=True
    )
    with pytest.raises((LeaseLostError, StaleScanError, Exception)) as caught:
        await sink.persist_deal_page(lease, scan, page)
    assert caught.value.reason_code in {
        "SOURCE_EPOCH_BLOCKED",
        "LEASE_LOST",
    }
