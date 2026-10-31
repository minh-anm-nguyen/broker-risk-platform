"""End-to-end sync against the fake source, checked against its oracle.

This is the G2 correctness gate. The assertions compare the database against
ground truth read straight from the source's event log -- never against the
paged endpoint the scan used, because a fault that drops records would corrupt
that expectation too and the test would pass while data went missing
(design 15.2, WF3-141).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import (
    ImmutableDealConflict,
    IncompleteScanError,
    SnapshotExpiredError,
    SourceCapabilityError,
    SourceEpochChangedError,
    UpstreamTimeoutError,
)

pytestmark = [pytest.mark.e2e, pytest.mark.integration]


async def stored_deals(sessions) -> dict[str, str]:
    async with sessions() as session:
        rows = (await session.execute(text("SELECT deal_id, payload_hash FROM deals"))).all()
    return {row[0]: row[1] for row in rows}


async def watermark(sink, epoch, entity=Entity.DEALS) -> int:
    return await sink.read_watermark(epoch.epoch_id, entity, FILTER_UNFILTERED)


@pytest.mark.whatif("WF3-205", "WF3-025")
async def test_initial_sync_matches_the_oracle_exactly(
    deal_service, snapshot_service, source, sink, epoch, sessions
) -> None:
    """All three entities, with sequence gaps between deals.

    Set equality against the oracle is the only assertion that actually proves
    no record was skipped; row counts alone would miss a substitution.
    """
    source.set_account("A1", balance="5000.00")
    source.set_account("A2", balance="125.50")
    source.set_position("P1", volume_lots="0.25")
    for index in range(7):
        source.add_deal(f"D{index}", commission=f"-{index + 1}.25")
        source.advance_events(3)  # non-deal events, so deal sequences have gaps

    await deal_service.run_once(source)
    await snapshot_service.run_once(source, Entity.ACCOUNTS)
    await snapshot_service.run_once(source, Entity.POSITIONS)

    assert await stored_deals(sessions) == source.oracle_deals()
    assert await watermark(sink, epoch) == source.committed_seq

    async with sessions() as session:
        accounts = (
            await session.execute(
                text("SELECT account_id, balance FROM accounts_current ORDER BY account_id")
            )
        ).all()
        positions = (
            await session.execute(
                text("SELECT position_id, volume_lots FROM positions_current")
            )
        ).all()
    assert [row[0] for row in accounts] == ["A1", "A2"]
    assert Decimal(accounts[1][1]) == Decimal("125.50")
    assert Decimal(positions[0][1]) == Decimal("0.25")


@pytest.mark.whatif("WF3-206")
async def test_a_poll_with_no_new_deals_still_advances_safely(
    deal_service, source, sink, epoch, sessions
) -> None:
    """Non-deal events moved the source sequence; the window was read and is empty.

    The watermark may advance on that evidence alone. Refusing to would make
    the reported lag grow forever on a quiet source (design 6.2).
    """
    source.add_deal("D1")
    first = await deal_service.run_once(source)
    assert first.watermark_after == source.committed_seq

    source.advance_events(25)  # account and mark events only
    second = await deal_service.run_once(source)

    assert second.records == 0
    assert second.watermark_after == source.committed_seq
    assert second.watermark_after > first.watermark_after
    assert len(await stored_deals(sessions)) == 1


@pytest.mark.whatif("WF3-169")
async def test_repeated_full_replay_is_idempotent(
    deal_service, source, sink, epoch, sessions
) -> None:
    """An operator re-running a completed scan must not change anything.

    Hash and count identical before and after, which is what makes a manual
    re-run a safe operation rather than a gamble.
    """
    for index in range(4):
        source.add_deal(f"D{index}")
    await deal_service.run_once(source)
    before = await stored_deals(sessions)
    before_watermark = await watermark(sink, epoch)

    for _ in range(3):
        await deal_service.run_once(source)

    assert await stored_deals(sessions) == before
    assert await watermark(sink, epoch) == before_watermark


@pytest.mark.whatif("WF3-037", "WF3-135")
async def test_a_mid_scan_cursor_expiry_leaves_the_watermark_and_recovers(
    deal_service, source, sink, epoch, sessions
) -> None:
    """410 on page two: abort, hold the watermark, then succeed on a fresh scan.

    Page one is already durable and harmless. What must not happen is the
    watermark advancing on a partial read, or the retry resuming the dead
    cursor and skipping the rest of the snapshot.
    """
    source.page_size = 2
    for index in range(6):
        source.add_deal(f"D{index}")

    source.inject(Fault(mode="snapshot_expired", at_page=1, entity=Entity.DEALS))
    with pytest.raises(SnapshotExpiredError):
        await deal_service.run_once(source)
    source.assert_fired("snapshot_expired")

    assert await watermark(sink, epoch) == 0
    partial = await stored_deals(sessions)
    assert len(partial) == 2  # page one committed, which is safe

    source.clear_faults()
    await deal_service.run_once(source)

    assert await stored_deals(sessions) == source.oracle_deals()
    assert await watermark(sink, epoch) == source.committed_seq


@pytest.mark.whatif("WF3-133")
async def test_a_connector_timeout_aborts_without_advancing(
    deal_service, source, sink, epoch
) -> None:
    source.page_size = 2
    for index in range(5):
        source.add_deal(f"D{index}")
    source.inject(Fault(mode="timeout", at_page=1, entity=Entity.DEALS))

    with pytest.raises(UpstreamTimeoutError):
        await deal_service.run_once(source)
    source.assert_fired("timeout")
    assert await watermark(sink, epoch) == 0


@pytest.mark.whatif("WF3-141")
async def test_a_source_that_claims_a_false_terminal_is_caught_only_by_the_oracle(
    deal_service, source, sink, epoch, sessions
) -> None:
    """The documented limit of client-side verification (design 6.6).

    The source reports page one as terminal while later pages exist. Every
    client-side rule passes: provenance is consistent, the sequence is
    monotonic, the chain is complete and terminated. The scan "succeeds" and
    the watermark advances.

    Only the oracle shows records are missing. That is why completeness is a
    harness-level assertion and why this case can never be certified by
    validation alone -- and why Project 04's reconciliation exists.
    """
    source.page_size = 2
    for index in range(6):
        source.add_deal(f"D{index}")
    source.inject(Fault(mode="false_terminal", at_page=0, entity=Entity.DEALS))

    outcome = await deal_service.run_once(source)
    source.assert_fired("false_terminal")
    assert outcome.status.value == "PUBLISHED"

    persisted = await stored_deals(sessions)
    oracle = source.oracle_deals()
    assert persisted != oracle, "the fault did not actually withhold records"
    assert set(persisted) < set(oracle)
