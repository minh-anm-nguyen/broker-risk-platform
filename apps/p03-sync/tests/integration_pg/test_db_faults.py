"""Database failure modes, produced by the real server (design group J).

These faults are induced rather than mocked: a terminated backend, a genuine
lock timeout, a genuine deadlock between two transactions. The design is blunt
that a mocked rollback is not evidence (WF3-247), and the classification these
tests check is what the retry policy keys off -- misclassify a deadlock as
permanent and sync stalls; misclassify a conflict as transient and it retries
forever.
"""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.config import load_settings
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import DatabaseUnavailableError, TransientDatabaseError
from trading_sync.infrastructure.db import (
    RETRYABLE_SQLSTATES,
    classify_db_error,
    create_engine,
    session_factory,
)

pytestmark = pytest.mark.integration


@pytest.mark.whatif("WF3-109")
async def test_an_unreachable_database_is_reported_not_ready(settings) -> None:
    """Readiness must be false, and the failure must be classified.

    Starting scans against an unreachable database would churn leases and
    produce a stream of errors in place of one clear degraded signal.
    """
    broken = load_settings(
        database_url="postgresql+asyncpg://postgres@localhost:1/nonexistent",
        postgres={"pool_timeout_seconds": 2.0},
    )
    engine = create_engine(broken)
    factory = session_factory(engine)
    try:
        # A refused connect arrives as a bare OSError from the driver, with no
        # SQLSTATE at all, so the classifier has to recognise it structurally.
        with pytest.raises((SQLAlchemyError, OSError)) as caught:
            async with factory() as session:
                await session.execute(text("SELECT 1"))
        classified = classify_db_error(caught.value)
        assert isinstance(classified, DatabaseUnavailableError)
        assert classified.retryable is True
        assert classified.checkpoint_safe is False
    finally:
        await engine.dispose()


@pytest.mark.whatif("WF3-110", "WF3-111")
async def test_a_terminated_backend_is_classified_as_unavailable(sessions) -> None:
    """A dropped connection proves nothing about a COMMIT either way.

    The classification deliberately does not claim to know whether the
    transaction landed. That is why recovery re-reads durable state instead of
    guessing from the error (design 6.5).
    """
    async with sessions() as victim:
        pid = (await victim.execute(text("SELECT pg_backend_pid()"))).scalar()
        async with sessions() as killer:
            await killer.execute(
                text("SELECT pg_terminate_backend(:pid)"), {"pid": pid}
            )
            await killer.commit()
        with pytest.raises((SQLAlchemyError, OSError)) as caught:
            await victim.execute(text("SELECT 1"))
    classified = classify_db_error(caught.value)
    assert isinstance(classified, DatabaseUnavailableError)
    assert "commit outcome is unknown" in classified.message


@pytest.mark.whatif("WF3-111", "WF3-222")
async def test_durable_state_resolves_an_ambiguous_commit(sink, epoch, sessions) -> None:
    """The recovery procedure from design 6.5, exercised directly.

    After an ambiguous COMMIT the only authority is the database. The receipt
    with its content hash says whether the page landed, and the checkpoint says
    whether the scan published -- neither of which a lost worker's log could
    answer. Project 04 reads the same state to decide where to resume
    (WF3-222).
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=30
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    page = make_page(
        deal_scan_state(), [deal_payload("D1", 1)], index=0, snapshot_seq=10, terminal=True
    )
    await sink.persist_deal_page(lease, scan, page)

    state = await sink.recover_page_state(scan.scan_id)
    assert state["run"]["status"] == "FETCHING"
    assert [receipt["page_index"] for receipt in state["receipts"]] == [0]
    assert state["receipts"][0]["page_hash"] == page.page_hash
    assert state["receipts"][0]["is_terminal"] is True
    # Page durable, checkpoint not yet moved: replay from the durable mark.
    assert state["checkpoint"]["watermark"] == 0
    assert state["checkpoint"]["published_scan_id"] is None

    await sink.publish_deal_checkpoint(lease, scan, old_watermark=0, new_watermark=10)
    after = await sink.recover_page_state(scan.scan_id)
    assert after["run"]["status"] == "PUBLISHED"
    assert after["checkpoint"]["watermark"] == 10
    assert after["checkpoint"]["published_scan_id"] == scan.scan_id


@pytest.mark.whatif("WF3-116")
async def test_a_lock_held_past_lock_timeout_fails_fast(sessions, sink, epoch) -> None:
    """A waiter gives up instead of blocking indefinitely.

    Without ``lock_timeout`` a stuck publisher would hold its peer for as long
    as it lived, and the symptom would be a hung worker rather than a reported
    error (design 9.3).
    """
    async with sessions() as holder:
        await holder.execute(
            text(
                "SELECT 1 FROM sync_checkpoints WHERE epoch_id = :e AND entity = 'deals' "
                "FOR UPDATE"
            ),
            {"e": epoch.epoch_id},
        )
        async with sessions() as waiter:
            await waiter.execute(text("SET LOCAL lock_timeout = '150ms'"))
            with pytest.raises(SQLAlchemyError) as caught:
                await waiter.execute(
                    text(
                        "SELECT 1 FROM sync_checkpoints WHERE epoch_id = :e "
                        "AND entity = 'deals' FOR UPDATE"
                    ),
                    {"e": epoch.epoch_id},
                )
            await waiter.rollback()
        await holder.rollback()
    # 55P03 lock_not_available: a real error, and deliberately not retryable --
    # an immediate retry would just queue behind the same lock.
    classified = classify_db_error(caught.value)
    assert not isinstance(classified, TransientDatabaseError)


@pytest.mark.whatif("WF3-114")
async def test_a_real_deadlock_is_classified_as_retryable(sessions, sink, source) -> None:
    """Two transactions locking two rows in opposite order (SQLSTATE 40P01).

    PostgreSQL kills one of them. The survivor commits, and the victim's error
    must be classified retryable so the *whole* transaction is replayed -- a
    partial retry would not be idempotent (design 9.2, WF3-115).
    """
    identity = await source.read_meta(source.source_id)
    first = await sink.register_epoch(identity)
    await sink.approve_epoch(first.epoch_id, reason="TEST")

    async with sessions() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT entity FROM sync_checkpoints WHERE epoch_id = :e "
                    "ORDER BY entity"
                ),
                {"e": first.epoch_id},
            )
        ).scalars().all()
    assert len(rows) >= 2
    left, right = rows[0], rows[1]

    lock_sql = text(
        "SELECT 1 FROM sync_checkpoints WHERE epoch_id = :e AND entity = :entity FOR UPDATE"
    )
    both_took_first = asyncio.Event()
    taken = 0

    async def worker(first_entity: str, second_entity: str) -> BaseException | None:
        nonlocal taken
        try:
            async with sessions() as session:
                await session.execute(
                    lock_sql, {"e": first.epoch_id, "entity": first_entity}
                )
                taken += 1
                if taken >= 2:
                    both_took_first.set()
                await both_took_first.wait()
                await session.execute(
                    lock_sql, {"e": first.epoch_id, "entity": second_entity}
                )
                await session.commit()
            return None
        except SQLAlchemyError as exc:
            return exc

    outcomes = await asyncio.gather(worker(left, right), worker(right, left))
    failures = [exc for exc in outcomes if exc is not None]
    assert len(failures) == 1, "expected exactly one deadlock victim"

    classified = classify_db_error(failures[0])
    assert isinstance(classified, TransientDatabaseError)
    assert classified.retryable is True
    assert classified.safe_details["sqlstate"] in RETRYABLE_SQLSTATES


@pytest.mark.whatif("WF3-103", "WF3-097")
async def test_content_comparison_is_stable_under_concurrency(
    sink, epoch, sessions
) -> None:
    """The conflict check reads the stored row under a share lock.

    Without it, a concurrent writer could change the row between the read and
    the decision, and the same replay could be judged a duplicate once and a
    conflict the next time -- an intermittent money alert, which is worse than
    either verdict consistently.
    """
    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=60
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    await sink.persist_deal_page(
        lease,
        scan,
        make_page(
            deal_scan_state(),
            [deal_payload("D1", 1, commission="-2.50")],
            index=0,
            snapshot_seq=10,
            terminal=True,
        ),
    )

    identical = make_page(
        deal_scan_state(), [deal_payload("D1", 1, commission="-2.50")], index=0,
        snapshot_seq=10, terminal=True,
    )
    # Scans are created first: an await inside a generator expression would
    # build an async generator rather than the coroutines gather expects.
    scans = [await sink.create_scan(lease, start_watermark=0) for _ in range(4)]
    results = await asyncio.gather(
        *(sink.persist_deal_page(lease, scan, identical) for scan in scans),
        return_exceptions=True,
    )
    for outcome in results:
        if isinstance(outcome, BaseException):
            assert getattr(outcome, "reason_code", None) in {
                "DB_TRANSIENT",
                "STALE_SCAN",
            }, f"unexpected failure: {outcome!r}"

    async with sessions() as session:
        count = (
            await session.execute(text("SELECT count(*) FROM deals WHERE deal_id='D1'"))
        ).scalar()
    assert count == 1


@pytest.mark.whatif("WF3-128")
async def test_each_transaction_gets_its_own_session(sessions) -> None:
    """Sessions are never shared across tasks.

    ``AsyncSession`` is not safe to share, and sharing one shows up as
    corrupted-looking data rather than an obvious error -- so the factory
    returns a fresh session per call by construction (design 8.4).
    """
    async with sessions() as first, sessions() as second:
        assert first is not second
        first_pid = (await first.execute(text("SELECT pg_backend_pid()"))).scalar()
        second_pid = (await second.execute(text("SELECT pg_backend_pid()"))).scalar()
    assert first_pid != second_pid
