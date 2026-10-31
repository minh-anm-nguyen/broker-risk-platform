"""Shared fixtures.

The important choice here: the source under audit is the **real Project 01
simulator**, booted in-process against a temporary database, not a mock. Its
pagination, cursor signing, snapshot semantics, Decimal-as-string wire format
and fault injection are the contract Project 04 is written against, so testing
against a hand-rolled stand-in would mostly prove that two of my own
assumptions agree (section 10.1 on oracle independence).

Project 03 is represented by ``P03SyncHarness``, which owns the P03 tables and
implements the documented ports. It is clearly a harness, and results obtained
with it are evidence about Project 04 only.

``ManualClock`` is the default clock everywhere so expiry and staleness are
established by the test rather than waited for.
"""

from __future__ import annotations

import sys
import time
import uuid
from collections.abc import Iterator, Mapping, Sequence
from pathlib import Path
from typing import Any

import pytest

# The simulator lives in a sibling component; it is a dependency of these
# tests, not of the service, so the path is added here rather than in pyproject.
#
# Located by search rather than by a fixed number of ``.parent`` hops: when the
# count is wrong the failure is 93 collection errors saying "No module named
# 'simulator'", which points at the import and not at the path arithmetic that
# actually broke.
def _locate_simulator_src() -> Path:
    here = Path(__file__).resolve()
    for candidate in here.parents:
        for relative in (
            Path("p01-simulator") / "src",              # sibling inside apps/
            Path("apps") / "p01-simulator" / "src",     # from the repository root
        ):
            found = candidate / relative
            if (found / "simulator").is_dir():
                return found
    raise RuntimeError(
        "could not locate the Project 01 simulator source tree. These tests "
        "boot the real simulator in-process rather than mocking it, so they "
        "cannot run without it."
    )


_SIMULATOR_SRC = _locate_simulator_src()
if str(_SIMULATOR_SRC) not in sys.path:
    sys.path.insert(0, str(_SIMULATOR_SRC))

from reconciliation.clock import ManualClock  # noqa: E402
from reconciliation.config import Settings, load_settings  # noqa: E402
from reconciliation.db.engine import CaptureStrategy, Database  # noqa: E402
from reconciliation.db.repository import ReconRepository  # noqa: E402
from reconciliation.ports.source import P01HttpSource  # noqa: E402
from reconciliation.scheduler import ReconciliationScheduler  # noqa: E402
from recon_harness.p03_sync import P03SyncHarness  # noqa: E402

DATA_TOKEN = "dtok-" + "a" * 24
ADMIN_TOKEN = "atok-" + "b" * 24
CURSOR_KEY = "ckey-" + "c" * 24


# ---------------------------------------------------------------------------
# Project 01 simulator, in-process
# ---------------------------------------------------------------------------


@pytest.fixture
def p01_ttl_seconds() -> int:
    """Snapshot TTL for the simulator under test.

    A test module overrides this (by redefining the fixture) when it needs a
    genuine ``410 SNAPSHOT_EXPIRED`` from P01 rather than a simulated one.
    """
    return 900


@pytest.fixture
def p01_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, p01_ttl_seconds: int) -> Path:
    db_path = tmp_path / "simulator.db"
    monkeypatch.setenv("SIM_ENV", "test")
    monkeypatch.setenv("SIM_DB_PATH", str(db_path))
    monkeypatch.setenv("SIM_DATA_TOKEN", DATA_TOKEN)
    monkeypatch.setenv("SIM_ADMIN_TOKEN", ADMIN_TOKEN)
    monkeypatch.setenv("SIM_CURSOR_SIGNING_KEY", CURSOR_KEY)
    # A short snapshot TTL is what makes genuine 410 SNAPSHOT_EXPIRED
    # reachable in a test without waiting 15 minutes.
    # The simulator's field is ``snapshot_ttl_sec``; the env name must match it
    # exactly or pydantic-settings ignores the override silently.
    monkeypatch.setenv("SIM_SNAPSHOT_TTL_SEC", str(p01_ttl_seconds))
    # The fixture database is disposable and recreated per test, so paying for
    # fsync on every insert buys nothing here. Durability behaviour is measured
    # deliberately in the chaos suite, which sets this back to FULL.
    monkeypatch.setenv("SIM_SQLITE_SYNCHRONOUS", "OFF")
    return db_path


@pytest.fixture
def p01_client(p01_env: Path) -> Iterator[Any]:
    """A live P01 client. The context manager runs the app lifespan."""
    import warnings

    from simulator.main import create_app

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from starlette.testclient import TestClient

    app = create_app()
    with TestClient(app, headers={"Authorization": f"Bearer {DATA_TOKEN}"}) as client:
        yield client


class P01Admin:
    """Thin wrapper over P01's admin plane, for building fixtures."""

    def __init__(self, client: Any) -> None:
        self._client = client
        self._headers = {"Authorization": f"Bearer {ADMIN_TOKEN}"}

    def _post(self, path: str, payload: Mapping[str, Any] | None = None, **extra: str) -> Any:
        headers = dict(self._headers)
        headers["Idempotency-Key"] = extra.pop("idempotency_key", str(uuid.uuid4()))
        headers.update(extra)
        return self._client.post(path, json=dict(payload or {}), headers=headers)

    def create_run(
        self,
        *,
        accounts: int = 5,
        deals: int = 40,
        max_open_positions: int = 5,
        seed: int = 42,
        scenario: str = "recon-fixture",
        start_virtual_time: str = "2026-01-01T00:00:00Z",
    ) -> dict[str, Any]:
        response = self._post(
            "/admin/v1/runs",
            {
                "scenario_name": scenario,
                "seed": seed,
                "start_virtual_time": start_virtual_time,
                "account_count": accounts,
                "deal_count": deals,
                "max_open_positions": max_open_positions,
            },
        )
        assert response.status_code == 202, response.text
        body = response.json()
        self.await_job(body["job_id"])
        return body

    def reset_run(self, run_id: str, *, expected_generation: int | None = None) -> dict[str, Any]:
        """Start a new generation: the source-reset fixture (F-07)."""
        payload: dict[str, Any] = {}
        if expected_generation is not None:
            payload["expected_generation"] = expected_generation
        response = self._post(f"/admin/v1/runs/{run_id}/reset", payload)
        assert response.status_code == 202, response.text
        body = response.json()
        if body.get("job_id"):
            self.await_job(body["job_id"])
        return body

    def advance(self, run_id: str, *, events: int = 0, clock_steps: int = 0) -> None:
        response = self._post(
            f"/admin/v1/runs/{run_id}/advance", {"events": events, "clock_steps": clock_steps}
        )
        assert response.status_code in (200, 202), response.text
        body = response.json()
        if isinstance(body, dict) and body.get("job_id"):
            self.await_job(body["job_id"])

    def await_job(self, job_id: str, *, timeout_s: float = 30.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            response = self._client.get(f"/admin/v1/jobs/{job_id}", headers=self._headers)
            assert response.status_code == 200, response.text
            body = response.json()
            if body["status"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                assert body["status"] == "SUCCEEDED", body
                return body
            time.sleep(0.02)
        raise AssertionError(f"job {job_id} did not finish within {timeout_s}s")

    def meta(self) -> dict[str, Any]:
        response = self._client.get(
            "/v1/meta", headers={"Authorization": f"Bearer {DATA_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        return response.json()

    # ---- fault injection ------------------------------------------------

    def add_fault(
        self,
        *,
        mode: str,
        parameters: Mapping[str, Any] | None = None,
        path: str | None = None,
        trigger: str = "always",
        n: int = 0,
        max_activations: int = 1,
        rule_id: str | None = None,
    ) -> str:
        payload: dict[str, Any] = {
            "action": {"mode": mode, "parameters": dict(parameters or {})},
            "trigger": {"kind": trigger, "n": n, "max_activations": max_activations},
        }
        if path is not None:
            payload["scope"] = {"path": path}
        if rule_id is not None:
            payload["rule_id"] = rule_id
        response = self._post("/admin/v1/faults", payload)
        assert response.status_code == 201, response.text
        return response.json().get("rule_id", rule_id or "")

    def fault_activations(self, rule_id: str) -> int:
        """Read P01's own activation counter.

        This is what makes a fault-injection test honest: the assertion is
        against the simulator's count, not against the hope that the fault
        fired (section 10.6).
        """
        response = self._client.get("/admin/v1/faults", headers=self._headers)
        assert response.status_code == 200, response.text
        body = response.json()
        rules = body.get("items", body) if isinstance(body, dict) else body
        for rule in rules:
            if rule.get("rule_id") == rule_id:
                return int(rule.get("activations", 0))
        return 0

    def clear_faults(self) -> None:
        response = self._client.delete("/admin/v1/faults", headers=self._headers)
        assert response.status_code in (200, 204), response.text


@pytest.fixture
def p01_admin(p01_client: Any) -> P01Admin:
    return P01Admin(p01_client)


@pytest.fixture
def source(p01_client: Any) -> P01HttpSource:
    """The source port, driving the real simulator."""
    return P01HttpSource(
        client=p01_client,
        source_id="sim_local",
        max_page_size=500,
        max_retries=2,
        retry_base_delay_s=0.0,
        sleep=lambda _seconds: None,
    )


# ---------------------------------------------------------------------------
# Project 04 service
# ---------------------------------------------------------------------------


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return load_settings(
        env="test",
        db_path=tmp_path / "recon.db",
        artifacts_dir=tmp_path / "artifacts",
        source_id="sim_local",
        worker_id="recon-worker-A",
        approval_required=True,
        allow_auto_dispatch=False,
        allow_deal_backfill=False,
        max_concurrent_runs=4,
        source_max_page_size=100,
    )


@pytest.fixture
def db(settings: Settings) -> Database:
    database = Database(
        settings.db_path,
        busy_timeout_ms=settings.sqlite_busy_timeout_ms,
        synchronous="OFF",  # tests are disposable; FULL is measured separately
    )
    database.create_schema()
    return database


@pytest.fixture
def repository(db: Database, settings: Settings, clock: ManualClock) -> ReconRepository:
    return ReconRepository(db, settings, clock)


@pytest.fixture
def harness(db: Database, source: P01HttpSource, clock: ManualClock) -> P03SyncHarness:
    return P03SyncHarness(db=db, source=source, clock=clock, source_id="sim_local", page_size=100)


@pytest.fixture
def scheduler(
    db: Database,
    repository: ReconRepository,
    source: P01HttpSource,
    harness: P03SyncHarness,
    settings: Settings,
    clock: ManualClock,
) -> ReconciliationScheduler:
    return ReconciliationScheduler(
        db=db,
        repository=repository,
        source=source,
        read_port=harness,
        repair_port=harness,
        settings=settings,
        clock=clock,
    )


def make_scheduler(
    *,
    db: Database,
    repository: ReconRepository,
    source: Any,
    harness: Any,
    settings: Settings,
    clock: ManualClock,
    capture_strategy: CaptureStrategy = CaptureStrategy.IMMEDIATE,
) -> ReconciliationScheduler:
    """Build a scheduler with a non-default capture strategy or fake source."""
    return ReconciliationScheduler(
        db=db,
        repository=repository,
        source=source,
        read_port=harness,
        repair_port=harness,
        settings=settings,
        clock=clock,
        capture_strategy=capture_strategy,
    )


# ---------------------------------------------------------------------------
# Composite fixture: a fully synced, clean world
# ---------------------------------------------------------------------------


class SyncedWorld:
    """A source with data and a P03 target fully caught up with it.

    This is fixture ``F-01-CLEAN`` in miniature: every audit of it must return
    ``MATCHED`` with zero findings, and every corruption test starts by
    breaking exactly one thing about it.
    """

    def __init__(
        self,
        *,
        epoch_id: str,
        run_id: str,
        generation: int,
        snapshot_seq: int,
        watermark: int,
        account_scan_id: str,
        position_scan_id: str,
        harness: P03SyncHarness,
        admin: P01Admin,
    ) -> None:
        self.epoch_id = epoch_id
        self.run_id = run_id
        self.generation = generation
        self.snapshot_seq = snapshot_seq
        self.watermark = watermark
        self.account_scan_id = account_scan_id
        self.position_scan_id = position_scan_id
        self.harness = harness
        self.admin = admin


@pytest.fixture
def synced_world(p01_admin: P01Admin, harness: P03SyncHarness) -> SyncedWorld:
    run = p01_admin.create_run(accounts=6, deals=45, max_open_positions=6)
    meta = p01_admin.meta()
    epoch_id = harness.bootstrap_epoch()
    watermark = harness.sync_deals(epoch_id)
    account_scan = harness.publish_snapshot(epoch_id, "accounts")
    position_scan = harness.publish_snapshot(epoch_id, "positions")
    return SyncedWorld(
        epoch_id=epoch_id,
        run_id=run["run_id"],
        generation=run["generation"],
        snapshot_seq=int(meta["snapshot_seq"]),
        watermark=watermark,
        account_scan_id=account_scan,
        position_scan_id=position_scan,
        harness=harness,
        admin=p01_admin,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def deal_rows(admin: P01Admin, client: Any, *, limit: int = 500) -> list[dict[str, Any]]:
    """Read every deal straight from P01, as an independent oracle."""
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"limit": limit}
        if cursor is None:
            params["after_seq"] = 0
        else:
            params = {"cursor": cursor}
        response = client.get(
            "/v1/deals", params=params, headers={"Authorization": f"Bearer {DATA_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        body = response.json()
        rows.extend(body["items"])
        cursor = body.get("next_cursor")
        if cursor is None:
            return rows


def entity_rows(client: Any, entity: str, *, limit: int = 500) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        params: dict[str, Any] = {"limit": limit} if cursor is None else {"cursor": cursor}
        response = client.get(
            f"/v1/{entity}", params=params, headers={"Authorization": f"Bearer {DATA_TOKEN}"}
        )
        assert response.status_code == 200, response.text
        body = response.json()
        rows.extend(body["items"])
        cursor = body.get("next_cursor")
        if cursor is None:
            return rows


def assert_no_p03_mutation_by_p04(db: Database) -> None:
    """INV04-07/INV04-18 as a reusable assertion."""
    import sqlite3

    from reconciliation.db.engine import Role

    conn = db.connect(Role.RECON)
    try:
        for statement in (
            "UPDATE deals SET commission = '0.00'",
            "DELETE FROM deals",
            "UPDATE accounts_current SET balance = '0.00'",
            "DELETE FROM positions_current",
            "UPDATE sync_checkpoints SET watermark_seq = 999999",
        ):
            with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                conn.execute(statement)
    finally:
        conn.close()
