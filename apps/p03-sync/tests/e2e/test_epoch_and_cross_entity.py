"""Source resets, conflicts and cross-entity freshness (groups B, S)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, EpochStatus
from trading_sync.errors import (
    ImmutableDealConflict,
    SourceCapabilityError,
    SourceContractError,
    SourceEpochChangedError,
)

pytestmark = [pytest.mark.e2e, pytest.mark.integration]


async def watermark(sink, epoch, entity=Entity.DEALS) -> int:
    return await sink.read_watermark(epoch.epoch_id, entity, FILTER_UNFILTERED)


@pytest.mark.whatif("WF3-013", "WF3-014", "WF3-231")
async def test_a_source_reset_fences_the_old_epoch_and_stops(
    deal_service, source, sink, epoch, sessions
) -> None:
    """A reset is deliberately not self-healing.

    Bootstrapping the new epoch means a full re-read and a second copy of
    history, so it takes an explicit approval rather than happening inside a
    retry loop. Meanwhile the old epoch is fenced, so nothing can commit
    against the dataset we have just disowned (design 8.3).
    """
    source.add_deal("D1")
    await deal_service.run_once(source)
    first_watermark = await watermark(sink, epoch)
    assert first_watermark > 0

    source.reset(run_id="run_0002", generation=0)
    source.add_deal("D1")  # the id repeats in the new run

    with pytest.raises(SourceEpochChangedError) as caught:
        await deal_service.run_once(source)
    assert caught.value.safe_details["previous_epoch"] == "run_0001:G0"

    async with sessions() as session:
        rows = (
            await session.execute(
                text("SELECT run_id, generation, status FROM source_epochs ORDER BY created_at")
            )
        ).all()
    statuses = {(row[0], row[1]): row[2] for row in rows}
    assert statuses[("run_0001", 0)] == str(EpochStatus.BLOCKED)
    assert statuses[("run_0002", 0)] == str(EpochStatus.DISCOVERED)
    # The old watermark survives as history; it is never reused.
    assert await watermark(sink, epoch) == first_watermark


@pytest.mark.whatif("WF3-015", "WF3-231")
async def test_after_approval_the_new_epoch_is_isolated_from_the_old(
    deal_service, source, sink, epoch, sessions
) -> None:
    """Repeated deal ids across runs become two rows, each with its own money.

    Keyed on ``deal_id`` alone this is an overwrite; keyed on
    ``(epoch_id, deal_id)`` it is simply two facts (INV-14).
    """
    source.add_deal("D1", commission="-2.50")
    await deal_service.run_once(source)

    source.reset(run_id="run_0002", generation=0)
    source.add_deal("D1", commission="-9.99")
    with pytest.raises(SourceEpochChangedError):
        await deal_service.run_once(source)

    async with sessions() as session:
        new_epoch_id = (
            await session.execute(
                text("SELECT epoch_id FROM source_epochs WHERE run_id = 'run_0002'")
            )
        ).scalar()
    await sink.approve_epoch(new_epoch_id, reason="OPERATOR_REBOOTSTRAP")

    outcome = await deal_service.run_once(source)
    assert outcome.status.value == "PUBLISHED"

    async with sessions() as session:
        rows = (
            await session.execute(
                text("SELECT commission FROM deals WHERE deal_id = 'D1'")
            )
        ).scalars().all()
    assert {Decimal(value) for value in rows} == {Decimal("-9.99"), Decimal("-2.50")}
    assert await sink.read_watermark(new_epoch_id, Entity.DEALS, FILTER_UNFILTERED) > 0


@pytest.mark.whatif("WF3-017")
async def test_a_page_from_a_different_run_is_refused_mid_scan(
    deal_service, source, sink, epoch
) -> None:
    """A stale worker delivering a previous run's page after a handshake.

    The page validator catches pages that disagree with each other; the epoch
    guard catches a page that disagrees with the epoch we hold the watermark
    for. Either way the scan dies and the watermark holds.
    """
    source.page_size = 2
    for index in range(6):
        source.add_deal(f"D{index}")
    source.inject(Fault(mode="epoch_change", at_page=1, entity=Entity.DEALS))

    with pytest.raises(Exception) as caught:
        await deal_service.run_once(source)
    source.assert_fired("epoch_change")
    assert caught.value.reason_code in {"MIXED_SNAPSHOT", "SOURCE_EPOCH_CHANGED"}
    assert await watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-028", "WF3-223")
async def test_a_changed_deal_amount_is_quarantined_and_blocks_the_watermark(
    deal_service, source, sink, epoch, sessions
) -> None:
    """A source "correction" is held for review, not applied.

    Note what it takes to reach this path at all. Once a deal is below the
    watermark it is never re-read, because ``after_seq`` is exclusive -- so an
    amendment to settled history is invisible to incremental sync and is
    Project 04's problem, not this layer's. The conflict surfaces here because
    the first scan aborted before publishing, leaving D1 durable with the
    watermark still at zero, so the replay reads it again.

    When it does surface: the original value stands, the incident is recorded
    under a hashed key rather than the id, the scan is QUARANTINED, and the
    watermark does not move -- so nothing downstream consumes a dataset we
    already know is disputed (fixture G10, INV-05).
    """
    source.page_size = 1
    source.add_deal("D1", commission="-2.50")
    source.add_deal("D2", commission="-1.00")

    # Abort after page 0 commits, so D1 is durable but nothing is published.
    source.inject(Fault(mode="timeout", at_page=1, entity=Entity.DEALS))
    with pytest.raises(Exception):
        await deal_service.run_once(source)
    source.assert_fired("timeout")
    assert await watermark(sink, epoch) == 0

    source.clear_faults()
    source.amend_deal("D1", commission="-25.00")

    with pytest.raises(ImmutableDealConflict):
        await deal_service.run_once(source)

    async with sessions() as session:
        stored = (
            await session.execute(text("SELECT commission FROM deals WHERE deal_id='D1'"))
        ).scalar()
        quarantine = (
            await session.execute(
                text("SELECT reason_code, source_key_hash, safe_details FROM quarantine_items")
            )
        ).mappings().all()
        scan_status = (
            await session.execute(
                text("SELECT status FROM sync_runs ORDER BY started_at DESC LIMIT 1")
            )
        ).scalar()
    assert Decimal(stored) == Decimal("-2.50")
    assert quarantine[0]["reason_code"] == "IMMUTABLE_DEAL_CONFLICT"
    assert quarantine[0]["source_key_hash"] is not None
    assert "D1" not in str(quarantine[0]["safe_details"])
    assert scan_status == "QUARANTINED"
    assert await watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-217", "WF3-221")
async def test_entities_keep_independent_freshness(
    deal_service, snapshot_service, source, sink, epoch
) -> None:
    """Three entities, three cuts, and no pretence of a global one.

    Accounts at one sequence, positions at another and deals at a third is the
    normal case rather than an error. What would be wrong is reporting them as
    one consistent state (design 7.5, ADR-005).
    """
    source.set_account("A1")
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    accounts_at = await watermark(sink, epoch, Entity.ACCOUNTS)

    source.set_position("P1")
    await snapshot_service.run_once(source, Entity.POSITIONS)
    positions_at = await watermark(sink, epoch, Entity.POSITIONS)

    source.add_deal("D1")
    await deal_service.run_once(source)
    deals_at = await watermark(sink, epoch, Entity.DEALS)

    assert accounts_at < positions_at < deals_at
    checkpoints = {row["entity"]: row for row in await sink.read_checkpoints(epoch.epoch_id)}
    assert checkpoints["accounts"]["watermark"] == accounts_at
    assert checkpoints["positions"]["watermark"] == positions_at
    assert checkpoints["deals"]["watermark"] == deals_at


@pytest.mark.whatif("WF3-073", "WF3-218")
async def test_a_closed_position_keeps_its_deals_and_becomes_absent(
    deal_service, snapshot_service, source, sink, epoch, sessions
) -> None:
    """Deal history outlives the position it refers to (ADR-014, fixture G07)."""
    source.set_position("P1")
    source.add_deal("D1", position_id="P1")
    await snapshot_service.run_once(source, Entity.POSITIONS)
    await deal_service.run_once(source)

    source.remove_position("P1")
    await snapshot_service.run_once(source, Entity.POSITIONS)

    async with sessions() as session:
        position = (
            await session.execute(
                text(
                    "SELECT presence, source_status FROM positions_current "
                    "WHERE position_id='P1'"
                )
            )
        ).one()
        deal = (
            await session.execute(text("SELECT position_id FROM deals WHERE deal_id='D1'"))
        ).scalar()
    assert position[0] == "ABSENT"
    assert position[1] == "OPEN"  # never rewritten to CLOSED on absence alone
    assert deal == "P1"


@pytest.mark.whatif("WF3-008", "WF3-180")
async def test_a_source_without_a_stable_sequence_cannot_drive_incremental_sync(
    deal_service, source
) -> None:
    """No watermark can be defined over a sequence nobody promised to keep.

    Refusing is the correct answer; the alternative is a watermark that looks
    authoritative and is not.
    """
    source.supports_incremental_deals = False
    with pytest.raises(SourceCapabilityError):
        await deal_service.run_once(source)


@pytest.mark.whatif("WF3-001", "WF3-002")
async def test_meta_without_a_usable_identity_blocks_bootstrap(deal_service, source) -> None:
    """A synthetic ``run_id`` would let two different runs share a watermark."""
    source.run_id = ""
    with pytest.raises(SourceContractError):
        await deal_service.run_once(source)

    source.run_id = "run_0001"
    source.generation = -1
    with pytest.raises(SourceContractError):
        await deal_service.run_once(source)
