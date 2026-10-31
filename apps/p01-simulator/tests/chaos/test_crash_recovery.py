"""Crash boundaries: WF-076, WF-077, WF-078, IT-03.

The question these tests answer is *which side of COMMIT* the process died on.
``sleep(0.5)`` followed by a kill cannot answer it - that only proves the
process died somewhere. So the writer carries named failpoints, a child
process is configured to fire one, and it terminates via ``os._exit`` with no
unwinding, no atexit handlers and no SQLite cleanup. That is the only honest
in-process stand-in for SIGKILL (section 10.4).

The child runs with ``writer_batch_size=1`` so that "the failpoint at sequence
N" means exactly one event, not whichever batch happened to contain it.

``kill -9`` is not a power cut, and nothing here claims power-loss durability.
That needs hardware and is recorded as NOT TESTED (WF-087).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from simulator.persistence.failpoints import KILL_EXIT_CODE
from tests.conftest import ADMIN_TOKEN, DATA_TOKEN

pytestmark = [pytest.mark.chaos, pytest.mark.slow]

SRC = Path(__file__).resolve().parents[2] / "src"

WORKER = '''
"""Child process used by the crash-boundary tests. Written at test time."""
import asyncio, json, os, sys
sys.path.insert(0, SRC_PATH)

from datetime import UTC, datetime
from decimal import Decimal

from simulator.settings import Settings
from simulator.domain import invariants as inv
from simulator.domain.models import OpenPosition, Side, SimulatorError
from simulator.generation.coordinator import ScenarioCoordinator
from simulator.generation.generator import ScenarioSpec
from simulator.observability.metrics import NullMetrics
from simulator.persistence import event_store as es
from simulator.persistence.connection import (
    ProcessLock, ReadConnectionPool, initialize_database, open_connection,
)
from simulator.persistence.failpoints import controller_from_settings
from simulator.persistence.writer import WriteCommand, WriterService

DB, PHASE = sys.argv[1], sys.argv[2]
FAILPOINT = sys.argv[3] if len(sys.argv) > 3 else ""
AT_SEQ = int(sys.argv[4]) if len(sys.argv) > 4 else 0

settings = Settings(
    db_path=DB,
    data_token="t-data", admin_token="t-admin", cursor_signing_key="t-key",
    failpoint=FAILPOINT, failpoint_at_seq=AT_SEQ, failpoint_mode="kill",
    # One command per transaction, so "the failpoint at seq N" is exact.
    writer_batch_size=1,
    enforce_single_process_lock=False,
    metrics_enabled=False,
)

OPEN_INTENT = OpenPosition(
    account_id="A-000001", symbol="EURUSD", side=Side.BUY,
    volume_lots=Decimal("0.50"), price=Decimal("1.10000"),
    commission=Decimal("-2.50"), swap=Decimal("0.00"),
)


def report(payload):
    print("RESULT " + json.dumps(payload), flush=True)


async def main():
    initialize_database(settings)
    if PHASE == "report":
        conn = open_connection(settings, writable=False)
        try:
            run_id = es.load_active_run_id(conn)
            run = es.load_run_summary(conn, run_id)
            results = inv.run_all(conn, run_id, scope=inv.FULL_SCOPE)
            digest, counts = inv.canonical_state_digest(conn, run_id)
            report({
                "committed_seq": run.committed_seq,
                "deal_count": run.deal_count,
                "status": run.status.value,
                "counts": counts,
                "digest": digest,
                "summary": inv.summarize(results),
                "consistency": es.check_event_log_consistency(conn, run_id),
            })
        finally:
            conn.close()
        return

    lock = ProcessLock(settings.lock_path)
    lock.acquire()
    pool = ReadConnectionPool(settings)
    writer = WriterService(
        settings, metrics=NullMetrics(),
        failpoints=controller_from_settings(FAILPOINT, AT_SEQ, "kill"),
    )
    writer.start()
    coordinator = ScenarioCoordinator(settings, writer, pool, metrics=NullMetrics())
    coordinator.startup()
    try:
        if PHASE == "seed":
            spec = ScenarioSpec(
                scenario_name="crash", seed=42,
                start_virtual_time=datetime(2026, 10, 30, tzinfo=UTC),
                account_count=1, deal_count=0, max_open_positions=10,
                symbols=("EURUSD",), initial_balance_usd=Decimal("10000.00"),
            )
            accepted = await coordinator.create_run(
                spec, idempotency_key=None, request_hash="seed"
            )
            while True:
                job = coordinator.job(accepted["job_id"])
                if job and job.status.is_terminal:
                    break
                await asyncio.sleep(0.02)
            run = coordinator.run(accepted["run_id"])
            report({"committed_seq": run.committed_seq, "job": job.status.value})
        elif PHASE in {"crash", "retry"}:
            run = coordinator.active_run()
            try:
                result = await writer.execute(
                    WriteCommand(
                        run_id=run.run_id, intent=OPEN_INTENT,
                        idempotency_key="crash-key", request_hash="crash-hash",
                    )
                )
                report({
                    "seq": result.seq, "deal_id": result.deal_id,
                    "replayed": result.replayed, "event_id": result.event_id,
                })
            except SimulatorError as error:
                report({"error_code": error.code, "message": error.message})
        else:
            raise SystemExit(f"unknown phase {PHASE}")
    finally:
        coordinator.shutdown()
        writer.stop(drain=True, timeout=10.0)
        pool.close()
        lock.release()


asyncio.run(main())
'''


@pytest.fixture
def worker(tmp_path: Path) -> Path:
    script = tmp_path / "crash_worker.py"
    script.write_text(
        f"SRC_PATH = {str(SRC)!r}\n" + WORKER, encoding="utf-8"
    )
    return script


def run_phase(
    worker: Path, database: Path, phase: str, failpoint: str = "", at_seq: int = 0
) -> tuple[int, dict]:
    """Run one child phase and return (exit code, parsed RESULT or {})."""
    completed = subprocess.run(
        [sys.executable, "-I", str(worker), str(database), phase, failpoint, str(at_seq)],
        capture_output=True,
        text=True,
        timeout=180,
    )
    payload: dict = {}
    for line in completed.stdout.splitlines():
        if line.startswith("RESULT "):
            payload = json.loads(line[len("RESULT ") :])
    return completed.returncode, payload


@pytest.fixture
def seeded(worker: Path, tmp_path: Path) -> tuple[Path, int]:
    database = tmp_path / "crash.db"
    code, payload = run_phase(worker, database, "seed")
    assert code == 0, payload
    # One account plus one initial quote.
    assert payload["committed_seq"] == 2, payload
    assert payload["job"] == "SUCCEEDED"
    return database, payload["committed_seq"]


def test_a_crash_before_commit_leaves_no_trace(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """WF-076: no event, no deal, and the watermark has not moved."""
    database, baseline = seeded
    target = baseline + 1

    code, _ = run_phase(worker, database, "crash", "just_before_commit", target)
    assert code == KILL_EXIT_CODE, f"the child did not die at the failpoint (exit {code})"

    code, report = run_phase(worker, database, "report")
    assert code == 0
    assert report["committed_seq"] == baseline
    assert report["deal_count"] == 0
    assert report["counts"]["events"] == baseline
    assert report["counts"]["deals"] == 0
    assert report["consistency"] is None
    assert report["summary"]["failed"] == 0
    assert report["summary"]["storage_scope_ok"] is True


def test_a_crash_after_commit_keeps_the_event_exactly_once(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """WF-077: the event survives, and a retry with the same key adds nothing.

    This is the lost-response case: the write committed, the caller never
    learned the outcome, and the retry must be served from the idempotency
    record rather than applied again.
    """
    database, baseline = seeded
    target = baseline + 1

    code, _ = run_phase(worker, database, "crash", "just_after_commit", target)
    assert code == KILL_EXIT_CODE, f"the child did not die at the failpoint (exit {code})"

    code, report = run_phase(worker, database, "report")
    assert code == 0
    assert report["committed_seq"] == target
    assert report["deal_count"] == 1
    assert report["counts"]["deals"] == 1
    assert report["consistency"] is None
    assert report["summary"]["failed"] == 0

    # The retry is a replay, not a second effect.
    code, retried = run_phase(worker, database, "retry")
    assert code == 0, retried
    assert retried["replayed"] is True
    assert retried["seq"] == target
    assert retried["event_id"] == f"E-{target:012d}"

    code, after = run_phase(worker, database, "report")
    assert after["committed_seq"] == target
    assert after["deal_count"] == 1
    assert after["digest"] == report["digest"]
    assert after["summary"]["failed"] == 0


def test_the_write_succeeds_normally_without_a_failpoint(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """Control case. Without it, a failpoint that never fired would look like
    a pass in the tests above (WF-119)."""
    database, baseline = seeded
    code, result = run_phase(worker, database, "crash")
    assert code == 0, result
    assert result["replayed"] is False
    assert result["seq"] == baseline + 1
    assert result["deal_id"] == "D-00000001"

    code, report = run_phase(worker, database, "report")
    assert report["committed_seq"] == baseline + 1
    assert report["deal_count"] == 1
    assert report["summary"]["failed"] == 0


def test_repeated_kill_and_restart_cycles_never_lose_committed_data(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """WF-078: ten crash/restart cycles, watermark monotonic, truth stable."""
    database, baseline = seeded

    code, applied = run_phase(worker, database, "crash")
    assert code == 0, applied
    code, reference = run_phase(worker, database, "report")
    assert code == 0
    watermark = reference["committed_seq"]
    digest = reference["digest"]

    for cycle in range(10):
        # A crash attempt at a sequence that will never be reached, because the
        # idempotent retry is served from storage without a new commit.
        code, _ = run_phase(worker, database, "retry", "just_before_commit", watermark + 50)
        assert code == 0, f"cycle {cycle} unexpectedly died"
        code, report = run_phase(worker, database, "report")
        assert code == 0
        assert report["committed_seq"] == watermark, cycle
        assert report["digest"] == digest, cycle
        assert report["consistency"] is None, cycle
        assert report["summary"]["failed"] == 0, cycle


def test_recovery_leaves_the_run_usable_not_merely_intact(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """After a crash the next write must continue the sequence cleanly."""
    database, baseline = seeded
    code, _ = run_phase(worker, database, "crash", "just_before_commit", baseline + 1)
    assert code == KILL_EXIT_CODE

    # The same write, now without a failpoint, takes the sequence it should.
    code, applied = run_phase(worker, database, "crash")
    assert code == 0, applied
    assert applied["seq"] == baseline + 1
    assert applied["replayed"] is False

    code, report = run_phase(worker, database, "report")
    assert report["committed_seq"] == baseline + 1
    assert report["summary"]["failed"] == 0
    assert report["summary"]["storage_scope_ok"] is True


def test_the_http_service_reports_ready_after_an_unclean_stop(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """A restart over a WAL left by a killed process must come up clean."""
    from fastapi.testclient import TestClient

    from simulator.main import create_app
    from simulator.settings import Settings

    database, baseline = seeded
    code, _ = run_phase(worker, database, "crash", "just_after_commit", baseline + 1)
    assert code == KILL_EXIT_CODE

    settings = Settings(
        db_path=str(database),
        data_token=DATA_TOKEN,
        admin_token=ADMIN_TOKEN,
        cursor_signing_key="t-key",
        enforce_single_process_lock=False,
    )
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        ready = client.get("/health/ready").json()
        assert ready["ready"] is True, ready
        assert ready["recovery_required"] is False
        assert ready["committed_seq"] == baseline + 1

        report = client.get(
            "/admin/v1/diagnostics/invariants?scope=full",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
        ).json()
        assert report["summary"]["failed"] == 0
        deals = client.get(
            "/v1/deals?limit=10", headers={"Authorization": f"Bearer {DATA_TOKEN}"}
        ).json()
        assert len(deals["items"]) == 1


def test_a_projection_event_log_mismatch_is_flagged_not_repaired(
    worker: Path, seeded: tuple[Path, int]
) -> None:
    """WF-083, section 4.3: readiness goes false and a human decides what to rebuild.

    Silently rebuilding financial projections on startup is precisely the
    behaviour the design forbids, so the test asserts the service refuses to
    call itself ready rather than that it fixed anything.
    """
    from fastapi.testclient import TestClient

    from simulator.main import create_app
    from simulator.persistence.connection import open_connection
    from simulator.settings import Settings

    database, baseline = seeded
    code, _ = run_phase(worker, database, "crash")
    assert code == 0

    settings = Settings(
        db_path=str(database),
        data_token=DATA_TOKEN,
        admin_token=ADMIN_TOKEN,
        cursor_signing_key="t-key",
        enforce_single_process_lock=False,
    )
    # Tamper behind the service's back: delete an event the watermark claims.
    connection = open_connection(settings, writable=True)
    try:
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            "DELETE FROM event_log WHERE seq = (SELECT MAX(seq) FROM event_log)"
        )
        connection.commit()
    finally:
        connection.close()

    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        ready = client.get("/health/ready")
        body = ready.json()
        assert body["ready"] is False
        assert body["recovery_required"] is True
        assert "recovery_required" in body["reasons"]
        # Liveness still answers: the process is fine, the data is not.
        assert client.get("/health/live").status_code == 200
