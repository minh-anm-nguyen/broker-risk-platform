"""Crash windows around snapshot staging and promotion (K11..K15, INV-06/07)."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from trading_sync.application.killpoints import KillPointCrash
from trading_sync.domain.snapshots import Entity

pytestmark = [pytest.mark.crash, pytest.mark.integration]


async def current_accounts(sessions) -> dict[str, Decimal]:
    async with sessions() as session:
        rows = (
            await session.execute(
                text("SELECT account_id, balance, presence FROM accounts_current")
            )
        ).all()
    return {row[0]: Decimal(row[1]) for row in rows}


@pytest.mark.whatif("WF3-061", "WF3-158")
async def test_crash_before_promotion_keeps_the_previous_snapshot(
    snapshot_service, source, sessions, epoch, barriers
) -> None:
    """Staged rows are durable and invisible; the reader keeps the old set.

    This is the whole reason staging exists. Without it, the dead scan would
    have left a mixture of two snapshots in the live projection.
    """
    source.set_account("A1", balance="100.00")
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    assert await current_accounts(sessions) == {"A1": Decimal("100.00")}

    source.set_account("A1", balance="900.00")
    source.set_account("A2", balance="50.00")
    barriers.arm_crash("K13_before_snapshot_promote")
    with pytest.raises(KillPointCrash):
        await snapshot_service.run_once(source, Entity.ACCOUNTS)
    barriers.assert_crossed("K13_before_snapshot_promote")

    assert await current_accounts(sessions) == {"A1": Decimal("100.00")}
    async with sessions() as session:
        staged = (
            await session.execute(text("SELECT count(*) FROM account_staging"))
        ).scalar()
    assert staged >= 2  # durable, and invisible to any reader


@pytest.mark.whatif("WF3-067", "WF3-158")
async def test_crash_midway_through_promotion_rolls_the_whole_promotion_back(
    snapshot_service, source, sessions, epoch, barriers
) -> None:
    """Design 7.3: the upsert and the absent-marking share one transaction.

    Dying between them would leave new balances beside stale presence flags.
    PostgreSQL rolls the pair back together, so the reader still sees a
    complete older snapshot (INV-07, WF3-067).
    """
    for name in ("A1", "A2", "A3"):
        source.set_account(name, balance="10.00")
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    before = await current_accounts(sessions)
    assert before == {"A1": Decimal("10.00"), "A2": Decimal("10.00"), "A3": Decimal("10.00")}

    for name in ("A1", "A2", "A3"):
        source.set_account(name, balance="777.00")
    barriers.arm_crash("K14_mid_snapshot_promote_transaction")
    with pytest.raises(KillPointCrash):
        await snapshot_service.run_once(source, Entity.ACCOUNTS)
    barriers.assert_crossed("K14_mid_snapshot_promote_transaction")

    after = await current_accounts(sessions)
    assert after == before, "a partially promoted snapshot became visible"

    barriers.clear()
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    assert await current_accounts(sessions) == {
        "A1": Decimal("777.00"),
        "A2": Decimal("777.00"),
        "A3": Decimal("777.00"),
    }


@pytest.mark.whatif("WF3-158", "WF3-119")
async def test_crash_after_promotion_commit_leaves_the_new_snapshot_published(
    snapshot_service, source, sessions, epoch, barriers
) -> None:
    """The mirror case: the commit landed, the acknowledgement did not.

    The publication is durable and complete, so recovery must not undo it or
    re-promote stale staging over it.
    """
    source.set_position("P1", volume_lots="0.10")
    await snapshot_service.run_once(source, Entity.POSITIONS)

    source.set_position("P1", volume_lots="0.50")
    barriers.arm_crash("K15_after_snapshot_commit_before_ack")
    with pytest.raises(KillPointCrash):
        await snapshot_service.run_once(source, Entity.POSITIONS)
    barriers.assert_crossed("K15_after_snapshot_commit_before_ack")

    async with sessions() as session:
        rows = (
            await session.execute(
                text("SELECT volume_lots, presence FROM positions_current WHERE position_id='P1'")
            )
        ).one()
    assert Decimal(rows[0]) == Decimal("0.50")
    assert rows[1] == "PRESENT"


@pytest.mark.whatif("WF3-021", "WF3-166")
async def test_staging_from_a_dead_scan_cannot_promote_itself_later(
    snapshot_service, source, sessions, epoch, barriers
) -> None:
    """INV-21: unpublished staging must not become active after a restart.

    The scan never reached ``STAGED`` in the database, so a later publisher has
    no basis to promote it. Status is read from the database, never inferred
    from leftover rows.
    """
    source.set_account("A1", balance="1.00")
    await snapshot_service.run_once(source, Entity.ACCOUNTS)

    source.set_account("A1", balance="2.00")
    barriers.arm_crash("K12_after_snapshot_stage_page")
    with pytest.raises(KillPointCrash):
        await snapshot_service.run_once(source, Entity.ACCOUNTS)
    barriers.assert_crossed("K12_after_snapshot_stage_page")

    async with sessions() as session:
        statuses = (
            await session.execute(
                text("SELECT status FROM sync_runs WHERE entity='accounts' ORDER BY started_at")
            )
        ).scalars().all()
    assert "PUBLISHED" in statuses
    assert statuses[-1] in {"FETCHING", "ABORTED"}
    assert await current_accounts(sessions) == {"A1": Decimal("1.00")}
