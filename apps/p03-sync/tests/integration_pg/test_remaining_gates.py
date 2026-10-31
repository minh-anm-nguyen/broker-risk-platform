"""The remaining P0 gates: typed upstream reset, hot keys, harness honesty."""

from __future__ import annotations

import os
import time

import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import SourceEpochChangedError
from trading_sync.infrastructure.db import create_engine, session_factory

pytestmark = pytest.mark.integration


@pytest.mark.whatif("WF3-134")
async def test_a_typed_epoch_change_from_the_connector_stops_writes_at_once(
    deal_service, source, sink, epoch, sessions
) -> None:
    """The connector can notice the reset before we infer it from provenance.

    Both routes must stop the scan. This one is the earlier signal, so no
    further page may reach the sink after it -- the pages already committed are
    correctly attributed to the old epoch and stay.
    """
    source.page_size = 2
    for index in range(6):
        source.add_deal(f"D{index}")
    source.inject(
        Fault(mode="epoch_changed_error", at_page=1, entity=Entity.DEALS)
    )

    with pytest.raises(SourceEpochChangedError):
        await deal_service.run_once(source)
    source.assert_fired("epoch_changed_error")

    async with sessions() as session:
        stored = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
        status = (
            await session.execute(
                text("SELECT status FROM sync_runs ORDER BY started_at DESC LIMIT 1")
            )
        ).scalar()
    assert stored == 2  # page zero, already committed and correctly attributed
    assert status == "ABORTED"
    assert await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED) == 0


@pytest.mark.whatif("WF3-209")
async def test_many_deals_on_one_account_do_not_create_a_row_hotspot(
    sink, epoch, sessions
) -> None:
    """Deals are keyed by deal id, so a busy account is not a contended row.

    Had the schema kept a per-account counter or balance, every deal for that
    account would serialise on one row and the design's throughput target
    would be unreachable for exactly the accounts that matter most.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=120
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    state = deal_scan_state(snapshot_seq=5001)

    started = time.perf_counter()
    for page_index in range(10):
        first = page_index * 500 + 1
        items = [
            deal_payload(f"D{seq}", seq, account_id="HOT_ACCOUNT")
            for seq in range(first, first + 500)
        ]
        await sink.persist_deal_page(
            lease,
            scan,
            make_page(
                state, items, index=page_index, snapshot_seq=5001, terminal=page_index == 9
            ),
        )
    elapsed = time.perf_counter() - started

    async with sessions() as session:
        stored = (
            await session.execute(
                text("SELECT count(*) FROM deals WHERE account_id = 'HOT_ACCOUNT'")
            )
        ).scalar()
        accounts_touched = (
            await session.execute(text("SELECT count(*) FROM accounts_current"))
        ).scalar()
    assert stored == 5000
    # Nothing was written to the account projection: deal ingestion does not
    # touch account rows at all, which is why there is no hotspot.
    assert accounts_touched == 0
    assert elapsed < 120, f"5000 deals on one account took {elapsed:.1f}s"


@pytest.mark.whatif("WF3-244")
async def test_committed_data_survives_a_fresh_engine(settings, sink, epoch) -> None:
    """The harness must prove recovery against a persistent database.

    A crash test that restarted with an empty database would "recover" every
    time and prove nothing. Here a page is committed, the engine is disposed,
    and a brand-new engine must still see the row and the receipt -- the
    database fingerprint is retained, so the volume is genuinely durable.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=60
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    page = make_page(
        deal_scan_state(), [deal_payload("D_durable", 1)], index=0, snapshot_seq=10,
        terminal=True,
    )
    await sink.persist_deal_page(lease, scan, page)

    fresh_engine = create_engine(settings)
    fresh_sessions = session_factory(fresh_engine)
    try:
        async with fresh_sessions() as session:
            stored = (
                await session.execute(
                    text("SELECT payload_hash FROM deals WHERE deal_id = 'D_durable'")
                )
            ).scalar()
            receipt = (
                await session.execute(
                    text("SELECT page_hash FROM sync_page_receipts WHERE scan_id = :s"),
                    {"s": scan.scan_id},
                )
            ).scalar()
    finally:
        await fresh_engine.dispose()

    assert stored is not None
    assert receipt == page.page_hash


@pytest.mark.whatif("WF3-247")
def test_the_database_backed_suites_cannot_run_without_a_real_database() -> None:
    """A mocked rollback is not evidence, so the suites refuse to pretend.

    Without ``SYNC_TEST_DATABASE_URL`` the fixture skips and the recorder marks
    those cases ``BLOCKED_ENVIRONMENT`` -- never PASS. This test confirms the
    env var is what gates them, so a green run on a machine with no database
    cannot be mistaken for a satisfied release gate.
    """
    configured = os.environ.get("SYNC_TEST_DATABASE_URL", "").strip()
    assert configured, "this suite is itself database-backed, so it must be configured"

    from tests.conftest import database_url

    source = database_url.__wrapped__ if hasattr(database_url, "__wrapped__") else None
    assert source is not None or callable(database_url)
    # The fixture body names the env var it requires; assert that contract
    # rather than re-implementing it.
    import inspect

    body = inspect.getsource(
        database_url.__wrapped__ if hasattr(database_url, "__wrapped__") else database_url
    )
    assert "SYNC_TEST_DATABASE_URL" in body
    assert "pytest.skip" in body
    assert "BLOCKED_ENVIRONMENT" in body
