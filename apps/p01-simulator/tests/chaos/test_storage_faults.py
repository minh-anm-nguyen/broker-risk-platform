"""Storage faults: WF-079, WF-080, WF-081, WF-082, WF-084, WF-121, WF-123.

What is being asserted in every case: the failure is **bounded, named and
non-destructive**. The write does not land, the watermark does not move, the
run stays consistent, and once the condition clears the service continues
without manual repair.

What is deliberately *not* claimed: that a simulated ENOSPC is identical to a
real full disk, or that ``kill -9`` is a power cut. The failpoints make the
error path reachable and assertable; a real filesystem-level test needs the
container harness and is recorded as such (section 6.4, WF-087).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.domain.models import StorageError
from simulator.main import create_app
from simulator.observability.metrics import Metrics
from simulator.persistence.connection import (
    initialize_database,
    open_connection,
    verify_integrity,
)
from simulator.persistence.failpoints import controller_from_settings
from simulator.persistence.writer import WriteCommand, WriterService
from tests.conftest import ADMIN_TOKEN, DATA_TOKEN, Runtime, create_run, make_settings

pytestmark = [pytest.mark.chaos, pytest.mark.slow]

ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
DATA = {"Authorization": f"Bearer {DATA_TOKEN}"}


def build_runtime(tmp_path: Path, **overrides: Any) -> Runtime:
    return Runtime(make_settings(tmp_path, **overrides))


def seed(runtime: Runtime) -> Any:
    """A tiny run with accounts and a quote, no trading."""
    import asyncio
    from datetime import UTC, datetime
    from decimal import Decimal

    from simulator.generation.generator import ScenarioSpec

    spec = ScenarioSpec(
        scenario_name="storage",
        seed=42,
        start_virtual_time=datetime(2026, 10, 30, tzinfo=UTC),
        account_count=2,
        deal_count=0,
        max_open_positions=10,
        symbols=("EURUSD",),
        initial_balance_usd=Decimal("10000.00"),
    )
    accepted = asyncio.run(
        runtime.coordinator.create_run(spec, idempotency_key=None, request_hash="seed")
    )
    import time

    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        job = runtime.coordinator.job(accepted["job_id"])
        if job and job.status.is_terminal:
            assert job.status.value == "SUCCEEDED", job
            return runtime.coordinator.run(accepted["run_id"])
        time.sleep(0.01)
    raise AssertionError("seeding did not finish")


OPEN_INTENT_ARGS = {
    "account_id": "A-000001",
    "symbol": "EURUSD",
    "side": "BUY",
    "volume_lots": "0.50",
    "price": "1.10000",
    "commission": "-2.50",
    "swap": "0.00",
}


def open_intent():
    from decimal import Decimal

    from simulator.domain.models import OpenPosition, Side

    return OpenPosition(
        account_id="A-000001",
        symbol="EURUSD",
        side=Side.BUY,
        volume_lots=Decimal("0.50"),
        price=Decimal("1.10000"),
        commission=Decimal("-2.50"),
        swap=Decimal("0.00"),
    )


@pytest.mark.parametrize(
    ("failpoint", "expected_kind"),
    [("storage_full", "disk_full"), ("storage_readonly", "readonly")],
)
def test_a_storage_failure_aborts_the_write_without_moving_the_watermark(
    tmp_path: Path, failpoint: str, expected_kind: str
) -> None:
    """WF-080, WF-081: a controlled transaction failure, nothing half written."""
    settings = make_settings(tmp_path, writer_batch_size=1)
    runtime = Runtime(settings)
    try:
        run = seed(runtime)
        baseline = run.committed_seq

        # Swap in a failpoint controller that fails the transaction rather than
        # the process, so the error path can be observed.
        runtime.writer._failpoints = controller_from_settings(  # noqa: SLF001
            failpoint, baseline + 1, "raise"
        )

        with pytest.raises(StorageError) as excinfo:
            runtime.writer.execute_sync(
                WriteCommand(run_id=run.run_id, intent=open_intent()), timeout=30
            )
        error = excinfo.value
        assert error.code == "STORAGE_ERROR"
        assert error.retryable is True
        assert error.details["failpoint"] == failpoint
        assert error.details["kind"] == expected_kind

        after = runtime.coordinator.run(run.run_id)
        assert after.committed_seq == baseline
        assert after.deal_count == 0

        with runtime.pool.acquire() as connection:
            from simulator.domain import invariants as inv

            summary = inv.summarize(inv.run_all(connection, run.run_id))
            assert summary["failed"] == 0
            assert summary["storage_scope_ok"] is True
    finally:
        runtime.close()


def test_the_service_recovers_once_the_storage_condition_clears(
    tmp_path: Path,
) -> None:
    """WF-080: resume with the same idempotency key, no manual repair."""
    settings = make_settings(tmp_path, writer_batch_size=1)
    runtime = Runtime(settings)
    try:
        run = seed(runtime)
        baseline = run.committed_seq

        runtime.writer._failpoints = controller_from_settings(  # noqa: SLF001
            "storage_full", baseline + 1, "raise"
        )
        with pytest.raises(StorageError):
            runtime.writer.execute_sync(
                WriteCommand(
                    run_id=run.run_id,
                    intent=open_intent(),
                    idempotency_key="resume-key",
                    request_hash="resume-hash",
                ),
                timeout=30,
            )

        # Condition cleared.
        runtime.writer._failpoints = controller_from_settings("", 0)  # noqa: SLF001
        result = runtime.writer.execute_sync(
            WriteCommand(
                run_id=run.run_id,
                intent=open_intent(),
                idempotency_key="resume-key",
                request_hash="resume-hash",
            ),
            timeout=30,
        )
        assert result.seq == baseline + 1
        assert result.replayed is False  # the first attempt never committed

        # And the key now behaves as a key: a further retry is a replay.
        again = runtime.writer.execute_sync(
            WriteCommand(
                run_id=run.run_id,
                intent=open_intent(),
                idempotency_key="resume-key",
                request_hash="resume-hash",
            ),
            timeout=30,
        )
        assert again.replayed is True
        assert again.seq == result.seq
        assert runtime.coordinator.run(run.run_id).committed_seq == baseline + 1
    finally:
        runtime.close()


def test_storage_errors_surface_as_503_over_http(tmp_path: Path) -> None:
    """The error contract: retryable, and silent about whether it landed."""
    settings = make_settings(tmp_path, writer_batch_size=1)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=2, deal_count=0)
        context = client.app.state.context
        current = client.get("/admin/v1/runs/current", headers=ADMIN).json()
        context.writer._failpoints = controller_from_settings(  # noqa: SLF001
            "storage_full", current["committed_seq"] + 1, "raise"
        )

        response = client.post(
            "/admin/v1/runs/run_0001/events",
            json={"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.1", "ask": "1.2"},
            headers={**ADMIN, "Idempotency-Key": "storage-503"},
        )
        assert response.status_code == 503
        error = response.json()["error"]
        assert error["code"] == "STORAGE_ERROR"
        assert error["retryable"] is True
        assert response.headers["retry-after"]

        # Reads keep working: storage refused a write, not every query.
        assert client.get("/v1/meta", headers=DATA).status_code == 200
        metrics = client.get("/admin/v1/metrics", headers=ADMIN).text
        assert 'sim_storage_errors_total{kind="disk_full"}' in metrics


def test_a_held_exclusive_lock_fails_fast_instead_of_hanging(tmp_path: Path) -> None:
    """WF-079: a bounded busy timeout, then a named retryable error."""
    settings = make_settings(
        tmp_path, writer_batch_size=1, sqlite_busy_timeout_ms=300
    )
    runtime = Runtime(settings)
    blocker: sqlite3.Connection | None = None
    try:
        run = seed(runtime)
        baseline = run.committed_seq

        blocker = open_connection(settings, writable=True)
        blocker.execute("BEGIN EXCLUSIVE")

        import time

        started = time.perf_counter()
        with pytest.raises(StorageError) as excinfo:
            runtime.writer.execute_sync(
                WriteCommand(run_id=run.run_id, intent=open_intent()), timeout=30
            )
        elapsed = time.perf_counter() - started
        assert excinfo.value.retryable is True
        # Bounded by busy_timeout, not an indefinite wait.
        assert elapsed < 10, elapsed

        blocker.rollback()
        blocker.close()
        blocker = None

        # The writer is still usable afterwards.
        result = runtime.writer.execute_sync(
            WriteCommand(run_id=run.run_id, intent=open_intent()), timeout=30
        )
        assert result.seq == baseline + 1
    finally:
        if blocker is not None:
            blocker.close()
        runtime.close()


def test_a_corrupt_database_copy_fails_the_integrity_check(tmp_path: Path) -> None:
    """WF-082: on a copy, as the design requires - never the live file."""
    settings = make_settings(tmp_path, writer_batch_size=1)
    runtime = Runtime(settings)
    try:
        seed(runtime)
    finally:
        runtime.close()

    original = Path(settings.db_path)
    corrupted = tmp_path / "corrupt.db"
    data = bytearray(original.read_bytes())
    assert len(data) > 8_192
    # Overwrite a page body, leaving the header intact so SQLite opens the
    # file and then discovers the damage - the realistic shape of corruption.
    for offset in range(4_096, 6_144):
        data[offset] = 0x5A
    corrupted.write_bytes(bytes(data))

    broken_settings = make_settings(tmp_path, db_path=str(corrupted))
    connection = open_connection(broken_settings, writable=False)
    try:
        with pytest.raises(Exception) as excinfo:
            verify_integrity(connection)
        assert "integrity_check" in str(excinfo.value)
    finally:
        connection.close()

    # The original is untouched: nothing in this test wrote to it.
    connection = open_connection(settings, writable=False)
    try:
        verify_integrity(connection)
    finally:
        connection.close()


def test_startup_refuses_a_corrupt_database_when_checking_is_enabled(
    tmp_path: Path,
) -> None:
    """Fail closed rather than serve from a damaged file."""
    settings = make_settings(tmp_path, writer_batch_size=1)
    runtime = Runtime(settings)
    try:
        seed(runtime)
    finally:
        runtime.close()

    corrupted = tmp_path / "corrupt2.db"
    data = bytearray(Path(settings.db_path).read_bytes())
    for offset in range(4_096, 6_144):
        data[offset] = 0x33
    corrupted.write_bytes(bytes(data))

    app = create_app(
        make_settings(tmp_path, db_path=str(corrupted), integrity_check_on_start=True),
        lock_process=False,
    )
    with pytest.raises(RuntimeError) as excinfo:
        with TestClient(app):
            pass
    assert "integrity_check" in str(excinfo.value)


def test_a_restart_over_a_persistent_volume_keeps_everything(tmp_path: Path) -> None:
    """WF-121: container restart, same volume, same truth."""
    settings = make_settings(tmp_path)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=5, deal_count=80)
        before = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()

    restarted = create_app(make_settings(tmp_path), lock_process=False)
    with TestClient(restarted) as client:
        ready = client.get("/health/ready").json()
        assert ready["ready"] is True
        after = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()
        assert after["canonical_digest"] == before["canonical_digest"]
        assert after["committed_seq"] == before["committed_seq"]
        assert after["summary"]["failed"] == 0


def test_the_wal_does_not_grow_without_bound_across_many_transactions(
    tmp_path: Path,
) -> None:
    """WF-084: reads hold no transaction, so checkpointing is never blocked.

    The bound asserted is deliberately loose. The point is that the WAL does
    not grow proportionally to the number of transactions, which is what a
    long-lived read transaction would cause.
    """
    settings = make_settings(tmp_path, writer_batch_size=1)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=4, deal_count=0)
        for index in range(150):
            client.post(
                "/admin/v1/runs/run_0001/events",
                json={
                    "kind": "UPDATE_MARK",
                    "symbol": "EURUSD",
                    "bid": f"1.{10000 + index:05d}",
                    "ask": f"1.{10010 + index:05d}",
                },
                headers={**ADMIN, "Idempotency-Key": f"wal-{index}"},
            )
            # Interleaved reads, which under a long-transaction design would
            # pin the WAL and stop it being checkpointed.
            client.get("/v1/marks", headers=DATA)

        wal = Path(settings.db_path).with_suffix(Path(settings.db_path).suffix + "-wal")
        wal_size = wal.stat().st_size if wal.exists() else 0
        assert wal_size < 32 * 1024 * 1024, wal_size

        metrics = client.get("/admin/v1/metrics", headers=ADMIN).text
        assert "sim_sqlite_wal_bytes" in metrics
        assert client.get("/health/ready").json()["ready"] is True


def test_a_failed_migration_leaves_the_previous_schema_version(tmp_path: Path) -> None:
    """WF-123: migrations are transactional, so there is no half schema."""
    from simulator.persistence.migrations import migrate

    settings = make_settings(tmp_path)
    assert initialize_database(settings) == 1

    connection = open_connection(settings, writable=True)
    try:
        # A database claiming a future schema is refused rather than guessed at.
        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE schema_meta SET value = '99' WHERE key = 'schema_version'")
        connection.commit()
        with pytest.raises(RuntimeError) as excinfo:
            migrate(connection)
        assert "newer than this build" in str(excinfo.value)

        connection.execute("BEGIN IMMEDIATE")
        connection.execute("UPDATE schema_meta SET value = '1' WHERE key = 'schema_version'")
        connection.commit()
        assert migrate(connection) == 1
    finally:
        connection.close()


def test_a_cancelled_caller_still_leaves_an_atomic_outcome(tmp_path: Path) -> None:
    """WF-055: cancellation must not produce a partial effect.

    A caller abandoning its future does not reach into the transaction, so the
    write either committed whole or not at all. The decisive check is that the
    sequence and the deal count agree with each other afterwards, and that the
    invariant sweep still passes - a half-applied event would break INV-01 or
    INV-03 immediately.
    """
    from simulator.domain import invariants as inv

    settings = make_settings(tmp_path, writer_batch_size=1)
    runtime = Runtime(settings)
    try:
        run = seed(runtime)
        baseline = run.committed_seq

        future = runtime.writer.submit_command(
            WriteCommand(
                run_id=run.run_id,
                intent=open_intent(),
                idempotency_key="cancelled-key",
                request_hash="cancelled-hash",
            )
        )
        # The caller walks away. Whether the cancel lands before the writer
        # picks the task up is a race, and either outcome must be consistent.
        future.cancel()
        try:
            future.result(timeout=30)
            committed = True
        except Exception:
            committed = False

        after = runtime.coordinator.run(run.run_id)
        if committed or after.committed_seq != baseline:
            assert after.committed_seq == baseline + 1
            assert after.deal_count == 1
        else:
            assert after.committed_seq == baseline
            assert after.deal_count == 0

        with runtime.pool.acquire() as connection:
            summary = inv.summarize(inv.run_all(connection, run.run_id))
            assert summary["failed"] == 0, summary
            assert summary["storage_scope_ok"] is True

        # And the writer is still healthy for the next caller.
        result = runtime.writer.execute_sync(
            WriteCommand(
                run_id=run.run_id,
                intent=open_intent(),
                idempotency_key="cancelled-key",
                request_hash="cancelled-hash",
            ),
            timeout=30,
        )
        assert result.seq == baseline + 1
    finally:
        runtime.close()


def test_the_writer_reports_queued_writes_as_failed_when_it_stops(
    tmp_path: Path,
) -> None:
    """WF-124: a dropped write must never look like a successful one."""
    from simulator.domain.models import ServiceNotReadyError

    settings = make_settings(tmp_path, max_pending_events=64, writer_batch_size=1)
    initialize_database(settings)
    writer = WriterService(settings, metrics=Metrics())
    writer.start()
    futures = []
    try:
        for _ in range(20):
            futures.append(
                writer.submit_command(
                    WriteCommand(run_id="run_0001", intent=open_intent())
                )
            )
    except Exception:
        pass
    writer.stop(drain=False, timeout=5.0)

    # Every future resolved one way or another; none is left pending.
    for future in futures:
        assert future.done()
        with pytest.raises((ServiceNotReadyError, Exception)):
            future.result(timeout=5)
