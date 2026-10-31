"""Crash windows around every commit boundary (design 17.3, group E, G3).

Each test arms one named barrier, lets the scan die there, then restarts the
scan and checks the two things that matter: the watermark never skipped, and
the deal set matches the source oracle exactly.

Every test also asserts the barrier was actually crossed. A crash test whose
injector never fired proves nothing, and reporting it as PASS would be worse
than reporting nothing at all (WF3-241, design 15.6).
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from trading_sync.application.killpoints import KillPointCrash
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity

pytestmark = [pytest.mark.crash, pytest.mark.integration]


def seed(source, count: int = 5) -> None:
    """A small history with sequence gaps, so replay is not trivially ordered."""
    for index in range(count):
        source.add_deal(f"D{index}", commission=f"-{index + 1}.50")
        source.advance_events(2)


async def watermark(sink, epoch) -> int:
    return await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED)


async def stored_deals(sessions) -> dict[str, str]:
    async with sessions() as session:
        rows = (
            await session.execute(text("SELECT deal_id, payload_hash FROM deals"))
        ).all()
    return {row[0]: row[1] for row in rows}


@pytest.mark.whatif("WF3-049", "WF3-050")
async def test_crash_before_the_first_source_call_changes_nothing(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """Nothing fetched, nothing written, watermark untouched."""
    seed(source)
    barriers.arm_crash("K00_before_source_call")
    with pytest.raises(KillPointCrash):
        await deal_service.run_once(source)

    barriers.assert_crossed("K00_before_source_call")
    assert await watermark(sink, epoch) == 0
    assert await stored_deals(sessions) == {}


@pytest.mark.whatif("WF3-051", "WF3-052")
async def test_crash_after_validation_before_commit_leaves_no_rows(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """The page was validated and then lost. PostgreSQL rolled nothing forward.

    No half page is visible, because the page never reached a transaction.
    """
    seed(source)
    barriers.arm_crash("K02_after_page_validate")
    with pytest.raises(KillPointCrash):
        await deal_service.run_once(source)

    barriers.assert_crossed("K02_after_page_validate")
    assert await watermark(sink, epoch) == 0
    assert await stored_deals(sessions) == {}


@pytest.mark.whatif("WF3-053", "WF3-229")
async def test_crash_after_page_commit_before_ack_replays_without_duplicating(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """The ambiguous-COMMIT case (design 6.5, section 10.2).

    PostgreSQL committed the page; the process died before learning that. The
    restart cannot tell whether the commit landed, so it replays from the
    durable watermark. The durable receipt and the unique key together make the
    replay a no-op, and the watermark only moves once the whole snapshot is
    proven read.
    """
    seed(source)
    barriers.arm_crash("K06_after_commit_before_ack")
    with pytest.raises(KillPointCrash):
        await deal_service.run_once(source)
    barriers.assert_crossed("K06_after_commit_before_ack")

    # The page is durable even though the caller never learned so.
    assert len(await stored_deals(sessions)) == 5
    # But the watermark has not moved: completeness was never proven.
    assert await watermark(sink, epoch) == 0

    barriers.clear()
    outcome = await deal_service.run_once(source)

    assert await stored_deals(sessions) == source.oracle_deals()
    assert await watermark(sink, epoch) == source.committed_seq
    assert outcome.watermark_after == source.committed_seq
    async with sessions() as session:
        total = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
    assert total == 5


@pytest.mark.whatif("WF3-054", "WF3-055")
async def test_crash_after_the_last_page_before_the_checkpoint(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """Section 10.3: every deal is durable, the watermark is not.

    This is the window that makes replay mandatory rather than optional, and
    the window in which a "we already have everything" shortcut would skip the
    proof.
    """
    seed(source)
    barriers.arm_crash("K07_after_last_page_commit")
    with pytest.raises(KillPointCrash):
        await deal_service.run_once(source)
    barriers.assert_crossed("K07_after_last_page_commit")

    assert len(await stored_deals(sessions)) == 5
    assert await watermark(sink, epoch) == 0

    barriers.clear()
    await deal_service.run_once(source)
    assert await watermark(sink, epoch) == source.committed_seq
    assert await stored_deals(sessions) == source.oracle_deals()


@pytest.mark.whatif("WF3-055", "WF3-060")
async def test_crash_inside_the_checkpoint_transaction_rolls_it_back(
    deal_service, source, sink, epoch, sessions, barriers
) -> None:
    """The watermark update and the scan status move together or not at all.

    Dying between the two would leave a checkpoint pointing at a scan that
    never reached PUBLISHED, which the design's SQL oracle treats as corruption.
    """
    seed(source)
    barriers.arm_crash("K09_after_checkpoint_update_before_commit")
    with pytest.raises(KillPointCrash):
        await deal_service.run_once(source)
    barriers.assert_crossed("K09_after_checkpoint_update_before_commit")

    assert await watermark(sink, epoch) == 0
    async with sessions() as session:
        split = (
            await session.execute(
                text(
                    "SELECT count(*) FROM sync_checkpoints c "
                    "JOIN sync_runs r ON r.scan_id = c.published_scan_id "
                    "WHERE r.status <> 'PUBLISHED'"
                )
            )
        ).scalar()
    assert split == 0

    barriers.clear()
    await deal_service.run_once(source)
    assert await watermark(sink, epoch) == source.committed_seq
