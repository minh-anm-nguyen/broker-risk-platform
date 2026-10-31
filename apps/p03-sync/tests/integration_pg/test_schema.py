"""Schema, constraint and privilege assertions against the migrated database.

The migration builds tables from the shared metadata, which removes the drift
risk of a second hand-written copy but moves the burden here: these tests are
what actually guarantee the live schema carries every constraint the design
depends on. A unique constraint quietly missing from a migration is WF3-245,
and a review cannot be relied on to catch it.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _scalar(sessions, sql: str, **params) -> object:
    async with sessions() as session:
        return (await session.execute(text(sql), params)).scalar()


@pytest.mark.whatif("WF3-245")
async def test_every_required_unique_key_exists(sessions) -> None:
    """These keys are what make idempotency and epoch isolation possible.

    Without ``pk_deals`` a replay inserts duplicates; without
    ``uq_source_epoch`` two simulator runs can share an epoch row.
    """
    required = {
        ("deals", "pk_deals"),
        ("source_epochs", "uq_source_epoch"),
        ("sync_checkpoints", "pk_sync_checkpoints"),
        ("sync_page_receipts", "pk_sync_page_receipts"),
        ("accounts_current", "pk_accounts_current"),
        ("positions_current", "pk_positions_current"),
        ("account_staging", "pk_account_staging"),
        ("position_staging", "pk_position_staging"),
    }
    async with sessions() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT rel.relname, con.conname FROM pg_constraint con "
                    "JOIN pg_class rel ON rel.oid = con.conrelid "
                    "WHERE con.contype IN ('p', 'u')"
                )
            )
        ).all()
    present = {(row[0], row[1]) for row in rows}
    assert required <= present, f"missing: {sorted(required - present)}"


@pytest.mark.whatif("WF3-245")
async def test_deals_primary_key_includes_the_epoch(sessions) -> None:
    """Deal ids repeat across simulator runs, so the epoch has to be in the key.

    Keyed on ``deal_id`` alone, a reset source's first replayed id would
    collide with history from the previous run (INV-14, WF3-015).
    """
    async with sessions() as session:
        columns = (
            await session.execute(
                text(
                    "SELECT a.attname FROM pg_constraint con "
                    "JOIN pg_class rel ON rel.oid = con.conrelid "
                    "JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = ANY(con.conkey) "
                    "WHERE rel.relname = 'deals' AND con.contype = 'p' ORDER BY a.attname"
                )
            )
        ).scalars().all()
    assert list(columns) == ["deal_id", "epoch_id"]


@pytest.mark.whatif("WF3-088", "WF3-089")
async def test_money_columns_are_exact_numeric(sessions) -> None:
    """Any float column here would silently round money (INV-12)."""
    async with sessions() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT table_name, column_name, data_type, numeric_precision, numeric_scale "
                    "FROM information_schema.columns WHERE table_schema='public' "
                    "AND column_name IN ('balance','credit','volume_lots','price',"
                    "'open_price','realized_pnl_gross','commission','swap')"
                )
            )
        ).all()
    assert rows, "expected money columns to exist"
    for table, column, data_type, precision, scale in rows:
        assert data_type == "numeric", f"{table}.{column} is {data_type}"
        assert (precision, scale) == (38, 12), f"{table}.{column} is ({precision},{scale})"


@pytest.mark.whatif("WF3-091")
async def test_all_timestamp_columns_carry_a_time_zone(sessions) -> None:
    """A ``timestamp without time zone`` column would discard the offset we
    went to the trouble of validating."""
    async with sessions() as session:
        naive = (
            await session.execute(
                text(
                    "SELECT table_name, column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND data_type='timestamp without time zone'"
                )
            )
        ).all()
    assert naive == []


@pytest.mark.whatif("WF3-034")
async def test_identifier_columns_are_text_not_numeric(sessions) -> None:
    """A numeric id column loses leading zeros and merges '0001' with '1'."""
    async with sessions() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT table_name, column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema='public' AND column_name IN "
                    "('deal_id','account_id','position_id','source_id','run_id')"
                )
            )
        ).all()
    for table, column, data_type in rows:
        if column == "run_id" or column.endswith("_id"):
            if column == "generation":
                continue
            assert data_type == "text", f"{table}.{column} is {data_type}"


@pytest.mark.whatif("WF3-101", "WF3-104")
async def test_database_refuses_to_update_a_stored_deal(sessions, sink, epoch) -> None:
    """Immutability is enforced by PostgreSQL, not only by application code.

    This is the invariant most expensive to lose and the easiest to lose by
    accident: one stray ``ON CONFLICT DO UPDATE`` on a money column would
    rewrite a commission with no trace. With the trigger in place, application
    code cannot violate it even by mistake (INV-05, FMEA row 2).
    """
    from tests.support import deal_payload, deal_scan_state, make_page
    from trading_sync.domain.identifiers import FILTER_UNFILTERED
    from trading_sync.domain.snapshots import Entity

    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    state = deal_scan_state()
    page = make_page(
        state,
        [deal_payload("D1", 1, commission="-2.50")],
        index=0,
        snapshot_seq=10,
        terminal=True,
    )
    await sink.persist_deal_page(lease, scan, page)

    async with sessions() as session:
        with pytest.raises(Exception) as caught:
            await session.execute(
                text("UPDATE deals SET commission = -25.00 WHERE deal_id = 'D1'")
            )
            await session.commit()
        assert "immutable" in str(caught.value).lower()
        await session.rollback()

    stored = await _scalar(sessions, "SELECT commission FROM deals WHERE deal_id='D1'")
    assert str(stored) == "-2.500000000000"


@pytest.mark.whatif("WF3-101")
async def test_database_refuses_to_delete_a_stored_deal(sessions, sink, epoch) -> None:
    """Deleting history is not a repair, so the row path is closed too.

    Retention sweeps are a separate, authorised task; the ingestion role cannot
    remove a deal (design 11.1, WF3-178).
    """
    from tests.support import deal_payload, deal_scan_state, make_page
    from trading_sync.domain.identifiers import FILTER_UNFILTERED
    from trading_sync.domain.snapshots import Entity

    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    page = make_page(
        deal_scan_state(), [deal_payload("D9", 1)], index=0, snapshot_seq=10, terminal=True
    )
    await sink.persist_deal_page(lease, scan, page)

    async with sessions() as session:
        with pytest.raises(Exception):
            await session.execute(text("DELETE FROM deals WHERE deal_id='D9'"))
            await session.commit()
        await session.rollback()

    assert await _scalar(sessions, "SELECT count(*) FROM deals WHERE deal_id='D9'") == 1


@pytest.mark.whatif("WF3-184")
async def test_application_role_has_no_ddl_privilege(sessions) -> None:
    """DDL belongs to the migration owner alone (design 11.1).

    Skipped when the role is absent, because a developer database without role
    setup is a legitimate configuration -- but where the split exists it is
    asserted.
    """
    exists = await _scalar(
        sessions, "SELECT 1 FROM pg_roles WHERE rolname = 'sync_app'"
    )
    if not exists:
        pytest.skip("sync_app role is not provisioned in this database")
    can_create = await _scalar(
        sessions, "SELECT has_schema_privilege('sync_app', 'public', 'CREATE')"
    )
    assert can_create is False
    can_insert = await _scalar(
        sessions, "SELECT has_table_privilege('sync_app', 'deals', 'INSERT')"
    )
    assert can_insert is True
    can_delete = await _scalar(
        sessions, "SELECT has_table_privilege('sync_app', 'deals', 'DELETE')"
    )
    assert can_delete is False
