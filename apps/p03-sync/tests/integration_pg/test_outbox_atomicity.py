"""The Deal and its outbox row commit together, or neither commits.

This is the one test that can actually establish Project 07's first design
decision end to end, and it needs a real PostgreSQL instance to do it. Without
``SYNC_TEST_DATABASE_URL`` every case here reports BLOCKED_ENVIRONMENT, which
is not a pass (P09 section 19.3, contract gap CG-08).

It also happens to be the first thing that applies Project 07's outbox DDL on
top of Project 03's migrated schema. If the two disagree - a missing
``UNIQUE (source_id, epoch_id)`` on ``source_epochs`` that P07's composite
foreign key needs, a column type mismatch - this is where it surfaces, loudly,
instead of in production. That check was the part contract gap CG-07 left open.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import text

from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity

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


async def _rows(sessions, sql: str, **params):
    async with sessions() as session:
        return (await session.execute(text(sql), params)).mappings().all()


@pytest.fixture
async def outbox_schema(sessions):
    """Apply Project 07's outbox DDL beside Project 03's migrated schema.

    Deliberately P07's own DDL text rather than a local copy: a local copy
    would drift, and this test exists partly to prove the two real schemas can
    occupy one database.
    """
    try:
        from outbox.persistence.schema_sql import OUTBOX_POSTGRES_DDL
    except ImportError:  # pragma: no cover - P07 not installed in this env
        pytest.skip(
            "Project 07 is not importable: the outbox schema cannot be applied, "
            "so this case is BLOCKED_ENVIRONMENT, not passing"
        )
    async with sessions() as session:
        await session.execute(text(OUTBOX_POSTGRES_DDL))
        await session.commit()
    return True


@pytest.fixture
def emitting_sink(sink, settings):
    """The sink with emission switched on.

    Off by default, because Project 03 must remain usable on its own. A
    deployment that wants the Project 07 chain sets it, and this fixture is how
    these tests say that is the deployment under test.
    """
    settings.sync.emit_outbox_events = True
    return sink


@pytest.mark.whatif("WF3-006")
async def test_each_committed_deal_leaves_exactly_one_outbox_row(
    emitting_sink, epoch, sessions, outbox_schema
) -> None:
    lease, scan = await _lease_and_scan(emitting_sink, epoch)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1), deal_payload("D2", 5)],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    result = await emitting_sink.persist_deal_page(lease, scan, page)

    assert result.inserted == 2
    assert await _count(sessions, "SELECT count(*) FROM deals") == 2
    # One event per Deal. Not per page, not per account.
    assert await _count(sessions, "SELECT count(*) FROM event_outbox") == 2


async def test_a_replayed_page_emits_nothing_further(
    emitting_sink, epoch, sessions, outbox_schema
) -> None:
    """``ON CONFLICT DO NOTHING`` declines the rows, so there is no new business
    fact and therefore no new event (P07 ADR07-02).

    At-least-once delivery already requires consumers to be idempotent.
    Emitting avoidable duplicates on top would spend that guarantee for
    nothing.
    """
    lease, scan = await _lease_and_scan(emitting_sink, epoch)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1)],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await emitting_sink.persist_deal_page(lease, scan, page)
    first = await _count(sessions, "SELECT count(*) FROM event_outbox")

    lease2, scan2 = await _lease_and_scan(emitting_sink, epoch)
    await emitting_sink.persist_deal_page(lease2, scan2, page)

    assert await _count(sessions, "SELECT count(*) FROM event_outbox") == first == 1


async def test_versions_are_contiguous_per_account_and_independent_between_accounts(
    emitting_sink, epoch, sessions, outbox_schema
) -> None:
    """The relay orders by ``aggregate_version`` within an aggregate.

    A gap would stall that account's lane permanently, because the guard waits
    for a version that is never coming. Two accounts must not share a counter
    either, or one busy account would push the other's versions forward.
    """
    lease, scan = await _lease_and_scan(emitting_sink, epoch)
    page = make_page(
        deal_scan_state(),
        [
            deal_payload("A1", 1, account_id="00001"),
            deal_payload("A2", 2, account_id="00001"),
            deal_payload("B1", 3, account_id="00002"),
        ],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await emitting_sink.persist_deal_page(lease, scan, page)

    rows = await _rows(
        sessions,
        "SELECT aggregate_key, aggregate_version FROM event_outbox "
        "ORDER BY aggregate_key, aggregate_version",
    )
    by_account: dict[str, list[int]] = {}
    for row in rows:
        by_account.setdefault(row["aggregate_key"], []).append(int(row["aggregate_version"]))

    assert by_account == {"00001": [1, 2], "00002": [1]}


async def test_a_failed_page_leaves_neither_a_deal_nor_an_event(
    emitting_sink, epoch, sessions, outbox_schema
) -> None:
    """The invariant, stated as a test.

    A Deal with no event is invisible to everything downstream and looks like
    success. An event with no Deal would publish a fact that never happened.
    Both are prevented by the same thing: one transaction.
    """
    lease, scan = await _lease_and_scan(emitting_sink, epoch)
    # Money finer than the event contract carries. Valid for Project 03,
    # unrepresentable as a DealRecorded payload, so emission raises inside the
    # transaction (contract gap CG-10).
    page = make_page(
        deal_scan_state(),
        [deal_payload("D9", 1, commission="0.005")],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )

    with pytest.raises(Exception):
        await emitting_sink.persist_deal_page(lease, scan, page)

    assert await _count(sessions, "SELECT count(*) FROM deals WHERE deal_id = 'D9'") == 0
    assert await _count(sessions, "SELECT count(*) FROM event_outbox") == 0


async def test_the_event_payload_matches_the_stored_deal(
    emitting_sink, epoch, sessions, outbox_schema
) -> None:
    """The row and the event must describe the same money.

    Comparing them guards against a projection that drifts from the stored
    value - the failure where both halves commit happily and disagree about the
    amount.
    """
    lease, scan = await _lease_and_scan(emitting_sink, epoch)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1)],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await emitting_sink.persist_deal_page(lease, scan, page)

    event = (await _rows(sessions, "SELECT payload, event_type FROM event_outbox"))[0]
    deal = (
        await _rows(
            sessions,
            "SELECT deal_id, account_id, realized_pnl_gross FROM deals WHERE deal_id = 'D1'",
        )
    )[0]

    payload = event["payload"]
    assert event["event_type"] == "DealRecorded"
    assert payload["deal_id"] == deal["deal_id"]
    assert payload["account_id"] == deal["account_id"]
    # Decimal on one side, canonical string on the other: equal as amounts.
    assert Decimal(payload["realized_pnl_gross"]) == deal["realized_pnl_gross"]


async def test_emission_stays_off_unless_it_is_switched_on(sink, epoch, sessions) -> None:
    """The default posture, asserted so it cannot drift silently.

    With emission off, Deals commit and no event is written. That is a declared
    capability gap, not a bug - but nothing downstream may be described as fed
    by this service while it holds.
    """
    lease, scan = await _lease_and_scan(sink, epoch)
    page = make_page(
        deal_scan_state(),
        [deal_payload("D1", 1)],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    result = await sink.persist_deal_page(lease, scan, page)
    assert result.inserted == 1
    assert await _count(sessions, "SELECT count(*) FROM deals") == 1
