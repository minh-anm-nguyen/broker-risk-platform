"""Compound fault campaigns and the remaining P0 cases (groups T, S, A, B, F, G).

The design calls these "critical triples": combinations where each guard works
alone but their interaction is where a real incident lives. The pattern in
every test is the same -- compose two or three faults and check that the guards
fail *closed* together rather than one of them papering over another.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from tests.support import account_payload, make_page, snapshot_scan_state
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, EpochStatus
from trading_sync.errors import (
    FilterScopeError,
    InvalidMoneyError,
    LeaseLostError,
    SchemaError,
    SourceEpochChangedError,
)

pytestmark = [pytest.mark.e2e, pytest.mark.integration]


async def watermark(sink, epoch, entity=Entity.DEALS) -> int:
    return await sink.read_watermark(epoch.epoch_id, entity, FILTER_UNFILTERED)


@pytest.mark.whatif("WF3-230")
async def test_a_lease_lost_during_a_410_retry_leaves_one_owner(
    deal_service, source, sink, epoch, sessions
) -> None:
    """Worker A hits 410; worker B takes the lease; A must not come back.

    Two recovery mechanisms overlap here -- retry-from-watermark and lease
    fencing -- and the hazard is A treating its retry as still authorised.
    B restarts from the same durable watermark, so nothing is lost either way.
    """
    source.page_size = 2
    for index in range(6):
        source.add_deal(f"D{index}")

    source.inject(Fault(mode="snapshot_expired", at_page=1, entity=Entity.DEALS))
    with pytest.raises(Exception):
        await deal_service.run_once(source)
    source.assert_fired("snapshot_expired")
    assert await watermark(sink, epoch) == 0

    # B takes over cleanly while A is between attempts.
    worker_b = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert worker_b is not None

    source.clear_faults()
    # A's retry finds the scope owned and yields rather than competing.
    outcome = await deal_service.run_once(source)
    assert outcome.reason_code == "LEASE_HELD_ELSEWHERE"
    assert await watermark(sink, epoch) == 0

    await sink.release_lease(worker_b)
    final = await deal_service.run_once(source)
    assert final.status.value == "PUBLISHED"

    async with sessions() as session:
        rows = (await session.execute(text("SELECT deal_id, payload_hash FROM deals"))).all()
    assert {row[0]: row[1] for row in rows} == source.oracle_deals()


@pytest.mark.whatif("WF3-233")
async def test_bad_money_on_the_last_page_preserves_every_earlier_page(
    deal_service, source, sink, epoch, sessions
) -> None:
    """199 good pages and one poisoned value: the watermark holds.

    The committed pages stay -- they are valid and idempotent -- but the scan
    cannot publish, so no consumer sees a dataset that silently stops short.
    After the source is repaired, a replay completes exactly (fixture G13).
    """
    source.page_size = 1
    for index in range(5):
        source.add_deal(f"D{index}")
    source.inject(Fault(mode="excess_scale", at_page=4, entity=Entity.DEALS))

    with pytest.raises(InvalidMoneyError):
        await deal_service.run_once(source)
    source.assert_fired("excess_scale")

    async with sessions() as session:
        stored = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
    assert stored == 4  # the four clean pages
    assert await watermark(sink, epoch) == 0

    source.clear_faults()
    await deal_service.run_once(source)
    async with sessions() as session:
        rows = (await session.execute(text("SELECT deal_id, payload_hash FROM deals"))).all()
    assert {row[0]: row[1] for row in rows} == source.oracle_deals()
    assert await watermark(sink, epoch) == source.committed_seq


@pytest.mark.whatif("WF3-234")
async def test_a_crash_during_checkpoint_plus_a_takeover_keeps_the_mark_monotonic(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """The highest-risk composition: a dying publisher and a new owner.

    Either the old watermark or the new one is correct; anything between them,
    or a regression, would mean a skipped or double-counted window.
    """
    for index in range(4):
        source.add_deal(f"D{index}")

    barriers.arm_crash("K09_after_checkpoint_update_before_commit")
    with pytest.raises(BaseException):
        await deal_service.run_once(source)
    barriers.assert_crossed("K09_after_checkpoint_update_before_commit")
    after_crash = await watermark(sink, epoch)
    assert after_crash == 0

    barriers.clear()
    worker_b = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    assert worker_b is not None
    await sink.release_lease(worker_b)

    await deal_service.run_once(source)
    final = await watermark(sink, epoch)
    assert final == source.committed_seq
    assert final >= after_crash


@pytest.mark.whatif("WF3-236")
async def test_a_filtered_position_scan_and_a_reset_both_fail_closed(
    snapshot_service, source, sink, epoch, sessions
) -> None:
    """Two independent guards, neither relying on the other.

    A filtered scan cannot mark rows absent, and a reset epoch cannot publish
    at all. Composed, no absence is recorded and the old epoch is fenced.
    """
    source.set_position("P1")
    source.set_position("P2")
    await snapshot_service.run_once(source, Entity.POSITIONS)

    source.inject(Fault(mode="filter_scope", at_page=0, entity=Entity.POSITIONS))
    with pytest.raises(FilterScopeError):
        await snapshot_service.run_once(source, Entity.POSITIONS)
    source.assert_fired("filter_scope")

    async with sessions() as session:
        absent = (
            await session.execute(
                text("SELECT count(*) FROM positions_current WHERE presence = 'ABSENT'")
            )
        ).scalar()
    assert absent == 0, "a filtered scan marked rows absent"

    source.clear_faults()
    source.reset(run_id="run_0003", generation=0)
    with pytest.raises(SourceEpochChangedError):
        await snapshot_service.run_once(source, Entity.POSITIONS)

    async with sessions() as session:
        status = (
            await session.execute(
                text("SELECT status FROM source_epochs WHERE run_id = 'run_0001'")
            )
        ).scalar()
    assert status == str(EpochStatus.BLOCKED)


@pytest.mark.whatif("WF3-078", "WF3-219")
async def test_a_position_whose_account_is_absent_is_still_stored(
    snapshot_service, source, sessions
) -> None:
    """No fabricated account, and no dropped position.

    The entities have independent cuts, so a position arriving before its
    account appears is normal rather than corrupt. Inventing the account would
    create a row the source never sent; dropping the position would lose real
    data.
    """
    source.set_position("P1", account_id="A_UNSEEN")
    await snapshot_service.run_once(source, Entity.POSITIONS)

    async with sessions() as session:
        position = (
            await session.execute(
                text("SELECT account_id, presence FROM positions_current")
            )
        ).one()
        accounts = (
            await session.execute(text("SELECT count(*) FROM accounts_current"))
        ).scalar()
    assert position[0] == "A_UNSEEN"
    assert position[1] == "PRESENT"
    assert accounts == 0, "an account row was fabricated"


@pytest.mark.whatif("WF3-220")
async def test_deal_commissions_never_adjust_a_published_balance(
    deal_service, snapshot_service, source, sessions
) -> None:
    """The source owns its own money; we never recompute it.

    A derived balance would be a second, conflicting answer, and the
    discrepancy would be attributed to this service rather than investigated
    at the source (section 10.12).
    """
    source.set_account("A1", balance="1000.00")
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    for index in range(5):
        source.add_deal(f"D{index}", account_id="A1", commission="-10.00")
    await deal_service.run_once(source)

    async with sessions() as session:
        balance = (
            await session.execute(
                text("SELECT balance FROM accounts_current WHERE account_id='A1'")
            )
        ).scalar()
    assert Decimal(balance) == Decimal("1000.00")


@pytest.mark.whatif("WF3-063")
async def test_an_identical_duplicate_account_across_pages_is_idempotent(
    sink, epoch
) -> None:
    """Same account, same content, two pages: one row, absorbed quietly.

    Only the conflicting case is an error; refusing the identical case would
    turn a harmless upstream quirk into an outage.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.ACCOUNTS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    first_state = snapshot_scan_state(Entity.ACCOUNTS)
    await sink.stage_snapshot_page(
        lease,
        scan,
        make_page(
            first_state, [account_payload("A1")], index=0, snapshot_seq=10, terminal=False
        ),
    )
    repeat_state = snapshot_scan_state(Entity.ACCOUNTS)
    repeat_state.expected_page_index = 1
    result = await sink.stage_snapshot_page(
        lease,
        scan,
        make_page(
            repeat_state, [account_payload("A1")], index=1, snapshot_seq=10, terminal=True
        ),
    )
    assert result.inserted == 0

    await sink.mark_scan_status(scan, "STAGED", snapshot_seq=10)
    publication = await sink.publish_snapshot(lease, scan, snapshot_seq=10)
    assert publication.rows_published == 1


@pytest.mark.whatif("WF3-005")
async def test_enrollment_is_idempotent_across_restarts(sink, source, sessions) -> None:
    """A retried startup converges on exactly one set of rows.

    Source, epoch and all three checkpoints are created in one transaction, so
    a crash during startup cannot leave an epoch with checkpoints missing.
    """
    identity = await source.read_meta(source.source_id)
    for _ in range(3):
        await sink.enroll_source(source.source_id, str(identity.source_kind))
        await sink.register_epoch(identity)

    async with sessions() as session:
        sources = (await session.execute(text("SELECT count(*) FROM sync_sources"))).scalar()
        epochs = (await session.execute(text("SELECT count(*) FROM source_epochs"))).scalar()
        checkpoints = (
            await session.execute(text("SELECT count(*) FROM sync_checkpoints"))
        ).scalar()
    assert sources == 1
    assert epochs == 1
    assert checkpoints == 3  # accounts, positions and deals, all at zero


@pytest.mark.whatif("WF3-016")
async def test_a_reconnect_keeping_the_same_run_does_not_reset_the_watermark(
    deal_service, source, sink, epoch
) -> None:
    """Connector lifecycle is not source identity (AS-13).

    A worker restart rotates the connector's own epoch, which says nothing
    about the data. Treating that as a source reset would force a full re-read
    of history on every reconnect.
    """
    for index in range(3):
        source.add_deal(f"D{index}")
    await deal_service.run_once(source)
    established = await watermark(sink, epoch)
    assert established > 0

    reads_before = source.meta_reads
    source.add_deal("D_after_reconnect")
    outcome = await deal_service.run_once(source)

    assert source.meta_reads > reads_before  # identity was re-verified
    assert outcome.watermark_before == established  # and found unchanged
    assert outcome.watermark_after == source.committed_seq


@pytest.mark.whatif("WF3-199", "WF3-020")
async def test_an_epoch_reset_is_audited_once_with_both_identities(
    deal_service, source, sessions
) -> None:
    """A reset is a critical event, and the audit must name what changed.

    The same ``source_id`` now reporting a different run is indistinguishable
    from a different broker behind the same configuration, so it demands an
    explicit review rather than a silent merge (WF3-020).
    """
    source.add_deal("D1")
    await deal_service.run_once(source)

    source.reset(run_id="run_0009", generation=2)
    with pytest.raises(SourceEpochChangedError):
        await deal_service.run_once(source)

    async with sessions() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT event_type, safe_details FROM sync_audit WHERE event_type IN "
                    "('EPOCH_BLOCKED', 'SOURCE_EPOCH_DISCOVERED') ORDER BY created_at"
                )
            )
        ).mappings().all()
    kinds = [row["event_type"] for row in rows]
    assert "EPOCH_BLOCKED" in kinds
    discovered = [row for row in rows if row["event_type"] == "SOURCE_EPOCH_DISCOVERED"]
    assert len(discovered) == 1
    assert discovered[0]["safe_details"]["previous_epoch"] == "run_0001:G0"
    assert discovered[0]["safe_details"]["observed_epoch"] == "run_0009:G2"
