"""Retention, quotas and quarantine volume (groups M and O)."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from tests.support import account_payload, make_page, snapshot_scan_state
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, EpochStatus
from trading_sync.errors import EpochBlockedError, StorageFullError
from trading_sync.infrastructure.repository import MAX_QUARANTINE_ROWS_PER_SCAN
from trading_sync.infrastructure.retention import RetentionPolicy, StagingSweeper

pytestmark = pytest.mark.integration


async def _stage(sink, epoch, *, terminal: bool, snapshot_seq: int = 10):
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.stage_snapshot_page(
        lease,
        scan,
        make_page(
            snapshot_scan_state(Entity.ACCOUNTS),
            [account_payload("A1")],
            index=0,
            snapshot_seq=snapshot_seq,
            terminal=True,
        ),
    )
    if terminal:
        await sink.mark_scan_status(scan, "STAGED", snapshot_seq=snapshot_seq)
        await sink.publish_snapshot(lease, scan, snapshot_seq=snapshot_seq)
    await sink.release_lease(lease)
    return scan


@pytest.mark.whatif("WF3-174", "WF3-071")
async def test_the_sweep_never_touches_a_live_scan(sink, epoch, sessions) -> None:
    """A scan still running, or being re-run, keeps its staging.

    Eligibility and deletion share one transaction, so a scan restarting
    between the two cannot lose rows it is about to reuse.
    """
    live = await _stage(sink, epoch, terminal=False)
    sweeper = StagingSweeper(sessions, RetentionPolicy(retention_seconds=0))

    result = await sweeper.sweep(Entity.ACCOUNTS)

    assert result.rows_removed == 0
    assert result.protected_scans >= 1
    async with sessions() as session:
        remaining = (
            await session.execute(
                text("SELECT count(*) FROM account_staging WHERE scan_id = :s"),
                {"s": live.scan_id},
            )
        ).scalar()
    assert remaining == 1


@pytest.mark.whatif("WF3-154")
async def test_the_sweep_clears_staging_of_a_finished_scan_past_retention(
    sink, epoch, sessions
) -> None:
    """Finished and old enough: the rows go, in bounded batches."""
    published = await _stage(sink, epoch, terminal=True)
    async with sessions() as session:
        # Age the scan rather than wait out the retention window.
        await session.execute(
            text(
                "UPDATE sync_runs SET ended_at = clock_timestamp() "
                "- interval '2 days' WHERE scan_id = :s"
            ),
            {"s": published.scan_id},
        )
        await session.commit()

    sweeper = StagingSweeper(sessions, RetentionPolicy(retention_seconds=3600))
    result = await sweeper.sweep(Entity.ACCOUNTS)

    assert result.rows_removed == 1
    assert result.scans_cleared == 1
    async with sessions() as session:
        current = (
            await session.execute(text("SELECT count(*) FROM accounts_current"))
        ).scalar()
        run = (
            await session.execute(
                text("SELECT status FROM sync_runs WHERE scan_id = :s"),
                {"s": published.scan_id},
            )
        ).scalar()
    # The published projection and the scan metadata both survive: staging is
    # evidence, not the data itself.
    assert current == 1
    assert run == "PUBLISHED"


@pytest.mark.whatif("WF3-154")
async def test_recent_evidence_survives_even_for_an_aborted_scan(
    sink, epoch, sessions
) -> None:
    """An incident investigated an hour later still has its rows.

    Aborting is exactly when staged rows matter most, so recency protects them
    regardless of status.
    """
    aborted = await _stage(sink, epoch, terminal=False)
    await sink.mark_scan_status(aborted, "ABORTED", reason_code="UPSTREAM_TIMEOUT")

    sweeper = StagingSweeper(sessions, RetentionPolicy(retention_seconds=24 * 3600))
    result = await sweeper.sweep(Entity.ACCOUNTS)

    assert result.rows_removed == 0
    async with sessions() as session:
        remaining = (
            await session.execute(text("SELECT count(*) FROM account_staging"))
        ).scalar()
    assert remaining == 1


@pytest.mark.whatif("WF3-149", "WF3-232")
async def test_a_staging_quota_breach_refuses_the_next_snapshot(
    sink, epoch, sessions
) -> None:
    """Refusing keeps the last complete projection serving reads.

    Starting anyway risks running out of space during the promotion, which is
    the one transaction that must not fail halfway.
    """
    await _stage(sink, epoch, terminal=True)
    sweeper = StagingSweeper(sessions, RetentionPolicy(max_staging_rows=1))

    with pytest.raises(StorageFullError) as caught:
        await sweeper.assert_quota_available(Entity.ACCOUNTS)
    assert caught.value.safe_details["max_staging_rows"] == 1
    assert caught.value.checkpoint_safe is False

    async with sessions() as session:
        current = (
            await session.execute(text("SELECT count(*) FROM accounts_current"))
        ).scalar()
    assert current == 1, "the previously published snapshot must still be served"


@pytest.mark.whatif("WF3-151")
async def test_a_quarantine_flood_is_capped_and_summarised(sink, epoch, sessions) -> None:
    """A malformed batch can produce one incident per record.

    The incident stays visible, but its volume stops growing: past the cap one
    summary row is written and the rest are dropped. An unbounded quarantine
    table turns a data-quality problem into a disk problem.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=60
    )
    scan = await sink.create_scan(lease, start_watermark=0)

    for index in range(MAX_QUARANTINE_ROWS_PER_SCAN + 25):
        await sink.quarantine(
            epoch.epoch_id,
            scan.scan_id,
            Entity.DEALS,
            reason_code="INVALID_MONEY",
            source_key=f"D{index}",
        )

    async with sessions() as session:
        total = (
            await session.execute(
                text("SELECT count(*) FROM quarantine_items WHERE scan_id = :s"),
                {"s": scan.scan_id},
            )
        ).scalar()
        summary = (
            await session.execute(
                text(
                    "SELECT safe_details FROM quarantine_items "
                    "WHERE reason_code = 'QUARANTINE_FLOOD_TRUNCATED'"
                )
            )
        ).mappings().all()
    assert total == MAX_QUARANTINE_ROWS_PER_SCAN + 1
    assert len(summary) == 1
    assert summary[0]["safe_details"]["original_reason_code"] == "INVALID_MONEY"


@pytest.mark.whatif("WF3-170", "WF3-023")
async def test_an_archived_epoch_cannot_be_scanned(sink, source, sessions) -> None:
    """A backfill must target an archived epoch explicitly and read-only.

    Approving a newer epoch archives the previous one. An archived epoch is not
    a committable target, so a backfill job pointed at it fails closed instead
    of writing into history or moving the active checkpoint.
    """
    identity = await source.read_meta(source.source_id)
    old = await sink.register_epoch(identity)
    await sink.approve_epoch(old.epoch_id, reason="TEST")

    source.reset(run_id="run_0002", generation=0)
    new_identity = await source.read_meta(source.source_id)
    new = await sink.register_epoch(new_identity)
    await sink.approve_epoch(new.epoch_id, reason="REBOOTSTRAP")

    async with sessions() as session:
        status = (
            await session.execute(
                text("SELECT status FROM source_epochs WHERE epoch_id = :e"),
                {"e": old.epoch_id},
            )
        ).scalar()
    assert status == str(EpochStatus.ARCHIVED)

    lease = await sink.acquire_lease(
        old.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    # The lease row still exists; the epoch guard sits at the commit, which is
    # where a stale or archived target is actually refused.
    assert lease is not None
    with pytest.raises(EpochBlockedError):
        await sink.create_scan(lease, start_watermark=0)

    assert await sink.read_watermark(new.epoch_id, Entity.DEALS, FILTER_UNFILTERED) == 0
