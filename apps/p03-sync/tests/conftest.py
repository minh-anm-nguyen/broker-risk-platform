"""Shared fixtures, and the what-if evidence recorder.

Integration, crash and e2e suites need a real PostgreSQL. When
``SYNC_TEST_DATABASE_URL`` is unset they are skipped, and the cases they carry
are reported ``BLOCKED_ENVIRONMENT`` rather than passing. The design is
explicit that a mock cannot stand in for a transaction test, so a green run on
a machine without a database must not look like a green gate (design 15.1,
WF3-247, WF3-242).

The recorder at the bottom writes ``artifacts/what_if_results.json`` in the
format of design 15.6. It maps the ``whatif`` marker's case ids onto outcomes,
so the release gate reads evidence rather than a claim.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys
from collections import defaultdict
from typing import Any, AsyncIterator, Iterator

import pytest
import pytest_asyncio
from sqlalchemy import text

from trading_sync.application.killpoints import Barriers
from trading_sync.application.source_epoch_guard import SourceEpochGuard
from trading_sync.application.sync_deals import DealSyncService
from trading_sync.application.sync_snapshot import SnapshotSyncService
from trading_sync.config import Settings, load_settings
from trading_sync.domain.snapshots import SourceKind
from trading_sync.infrastructure.db import create_engine, session_factory
from trading_sync.infrastructure.metrics import SyncMetrics
from trading_sync.infrastructure.repository import PostgresSink

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
ARTIFACTS = PROJECT_ROOT / "artifacts"

#: Tables emptied between tests, children before parents.
_TRUNCATE_ORDER = (
    "sync_audit",
    "quarantine_items",
    "sync_page_receipts",
    "account_staging",
    "position_staging",
    "accounts_current",
    "positions_current",
    "deals",
    "sync_checkpoints",
    "sync_runs",
    "source_epochs",
    "sync_sources",
)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--whatif-report",
        action="store",
        default=None,
        help="write the what-if evidence file to this path",
    )


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ.get("SYNC_TEST_DATABASE_URL", "").strip()
    if not url:
        pytest.skip(
            "SYNC_TEST_DATABASE_URL is not set: database-backed cases are "
            "BLOCKED_ENVIRONMENT, not passing",
            allow_module_level=False,
        )
    return url


@pytest.fixture(scope="session")
def migrated_database(database_url: str) -> str:
    """Apply Alembic migrations once per session.

    The suite runs against a migrated schema rather than one built from
    metadata, so the migration itself is under test. A unique constraint that
    exists in the models but not in the migration is a real failure mode
    (WF3-245).
    """
    env = dict(os.environ)
    env["SYNC_DATABASE_URL"] = database_url
    env["PYTHONPATH"] = os.pathsep.join(
        [str(PROJECT_ROOT / "src"), env.get("PYTHONPATH", "")]
    )
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.fail(f"alembic upgrade failed:\n{result.stdout}\n{result.stderr}")
    return database_url


@pytest.fixture
def settings(migrated_database: str) -> Settings:
    return load_settings(
        database_url=migrated_database,
        enable_killpoints=True,
        source={"source_id": "sim_local", "kind": SourceKind.SIMULATOR, "page_size": 500},
        lease={"ttl_seconds": 30, "heartbeat_seconds": 10},
        postgres={"pool_size": 5, "max_overflow": 2},
    )


@pytest_asyncio.fixture
async def engine(settings: Settings) -> AsyncIterator[Any]:
    created = create_engine(settings)
    try:
        yield created
    finally:
        await created.dispose()


@pytest_asyncio.fixture
async def sessions(engine: Any) -> Any:
    return session_factory(engine)


@pytest.fixture
def metrics() -> SyncMetrics:
    return SyncMetrics()


@pytest.fixture
def barriers() -> Iterator[Barriers]:
    armed = Barriers(enabled=True)
    yield armed
    armed.clear()


@pytest.fixture
def sink(sessions: Any, settings: Settings, metrics: SyncMetrics, barriers: Barriers) -> PostgresSink:
    return PostgresSink(sessions, settings, metrics=metrics, barriers=barriers)


@pytest.fixture
def source() -> Any:
    from tests.fakes.fake_source import FakeSimulatorSource

    return FakeSimulatorSource(source_id="sim_local", run_id="run_0001", generation=0)


@pytest.fixture
def guard(sink: PostgresSink, settings: Settings) -> SourceEpochGuard:
    return SourceEpochGuard(
        sink,
        source_id=settings.source.source_id,
        expected_kind=settings.source.kind,
    )


@pytest.fixture
def deal_service(
    sink: PostgresSink,
    settings: Settings,
    guard: SourceEpochGuard,
    metrics: SyncMetrics,
    barriers: Barriers,
) -> DealSyncService:
    return DealSyncService(sink, settings, guard, metrics=metrics, barriers=barriers)


@pytest.fixture
def snapshot_service(
    sink: PostgresSink,
    settings: Settings,
    guard: SourceEpochGuard,
    metrics: SyncMetrics,
    barriers: Barriers,
) -> SnapshotSyncService:
    return SnapshotSyncService(sink, settings, guard, metrics=metrics, barriers=barriers)


@pytest_asyncio.fixture
async def epoch(sink: PostgresSink, source: Any) -> Any:
    """An enrolled, approved epoch matching the fake source's identity."""
    identity = await source.read_meta(source.source_id)
    handle = await sink.register_epoch(identity)
    return await sink.approve_epoch(handle.epoch_id, reason="TEST_SETUP")


# ---------------------------------------------------------------------------
# What-if evidence recorder
# ---------------------------------------------------------------------------

_RESULTS: dict[str, dict[str, Any]] = {}
_CASE_TESTS: dict[str, list[str]] = defaultdict(list)

#: Only PASS means the behaviour was demonstrated. The others are each a
#: distinct kind of "we do not know", kept distinct on purpose (design 15.6).
_PRECEDENCE = {
    "FAIL": 4,
    "NOT_CONCLUSIVE": 3,
    "BLOCKED_ENVIRONMENT": 2,
    "NOT_TESTED": 1,
    "PASS": 0,
}


def _record(case_id: str, status: str, nodeid: str) -> None:
    existing = _RESULTS.get(case_id)
    _CASE_TESTS[case_id].append(nodeid)
    if existing is None or _PRECEDENCE[status] > _PRECEDENCE[existing["status"]]:
        _RESULTS[case_id] = {"case_id": case_id, "status": status}


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Any:
    outcome = yield
    report = outcome.get_result()
    if report.when != "call" and not (report.when == "setup" and report.skipped):
        return
    marker = item.get_closest_marker("whatif")
    if marker is None:
        return
    if report.passed:
        status = "PASS"
    elif report.failed:
        status = "FAIL"
    elif report.skipped:
        reason = str(getattr(report, "longrepr", ""))
        status = (
            "BLOCKED_ENVIRONMENT"
            if "SYNC_TEST_DATABASE_URL" in reason
            else "NOT_TESTED"
        )
    else:  # pragma: no cover - defensive
        status = "NOT_CONCLUSIVE"
    for case_id in marker.args:
        _record(str(case_id), status, item.nodeid)


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    target = session.config.getoption("--whatif-report")
    if target is None:
        ARTIFACTS.mkdir(parents=True, exist_ok=True)
        path = ARTIFACTS / "what_if_results.json"
    else:
        path = pathlib.Path(target)
        path.parent.mkdir(parents=True, exist_ok=True)
    cases = []
    for case_id in sorted(_RESULTS):
        entry = dict(_RESULTS[case_id])
        entry["tests"] = sorted(set(_CASE_TESTS[case_id]))
        cases.append(entry)
    counts: dict[str, int] = defaultdict(int)
    for entry in cases:
        counts[entry["status"]] += 1
    payload = {
        "schema": "trading-data-sync/what-if-results/v1",
        "environment": {
            "python": sys.version.split()[0],
            "database_configured": bool(os.environ.get("SYNC_TEST_DATABASE_URL")),
        },
        "totals": {
            "cases_executed": len(cases),
            "design_case_count": 252,
            **dict(counts),
        },
        "cases": cases,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
