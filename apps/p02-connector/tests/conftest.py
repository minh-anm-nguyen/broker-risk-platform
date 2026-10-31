"""Shared fixtures.

Three things are deliberately *not* fixtures:

* A module-level connector. Each test gets its own, because the whole point of
  the epoch model is that session state is per-instance and a shared one
  would let one test's quarantine leak into the next.
* A shared metrics registry. Each connector owns one, so counter assertions
  are not affected by test ordering.
* Real sleeps. Time-sensitive tests use
  :class:`~mt5_connector.deadline.ManualClock` and advance it, so a loaded CI
  machine changes the wall time a test takes but not whether it passes.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from mt5_connector.client import Connector
from mt5_connector.config import Settings
from mt5_connector.drivers.base import DriverIdentity, DriverResponse
from mt5_connector.drivers.fake_blocking import FakeBlockingDriver
from mt5_connector.telemetry import SECRETS, Metrics

from .fakesim import ADMIN_TOKEN, DATA_TOKEN, DatasetSpec, FakeSimulatorServer, build_dataset
from .fakesim.app import FakeSimulator

#: A value planted in the environment so a test can assert it never reaches a
#: log line, an error envelope or an evidence file (INV-15, WF2-B-06).
SECRET_CANARY = "SECRET_CANARY_123_do_not_log_me"


@pytest.fixture(autouse=True)
def _clean_secret_registry() -> Iterator[None]:
    """Keep the process-wide redaction registry from leaking between tests."""
    SECRETS.clear()
    yield
    SECRETS.clear()


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


def make_settings(**overrides: Any) -> Settings:
    """Settings with fast, test-sized budgets."""
    base: dict[str, Any] = {
        "source_id": "broker-sim-01",
        "base_url": "http://127.0.0.1:9",
        "source_kind": "fake",
        "driver": "fake_blocking",
        "max_queue_pending": 8,
        "default_page_size": 10,
        "max_page_size": 100,
        "page_total_deadline_ms": 1_000,
        "queue_wait_cap_ms": 400,
        "upstream_read_timeout_ms": 500,
        "connect_timeout_ms": 200,
        "max_attempts_per_read": 3,
        "retry_base_backoff_ms": 5,
        "retry_max_backoff_ms": 20,
        "retry_after_cap_ms": 50,
        "minimum_attempt_budget_ms": 10,
        "hung_call_hard_limit_ms": 300,
        "drain_timeout_ms": 500,
        "heartbeat_interval_ms": 10,
    }
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def settings() -> Settings:
    return make_settings()


# ---------------------------------------------------------------------------
# Fake driver helpers
# ---------------------------------------------------------------------------


def fake_meta(*, run_id: str = "run_fake", generation: int = 1, max_page_size: int = 100) -> dict:
    """A capability document in the Project 01 shape."""
    return {
        "run_id": run_id,
        "generation": generation,
        "snapshot_seq": 100,
        "capabilities": {
            "api_version": "v1",
            "default_page_size": 10,
            "max_page_size": max_page_size,
            "snapshot_ttl_seconds": 300,
            "after_seq_is_exclusive": True,
            "cursor_is_opaque_and_signed": True,
            "simulator_specific_fields": [
                "run_id",
                "generation",
                "snapshot_seq",
                "source_seq",
            ],
            "wire_compatible_with_mt5_manager_api": False,
        },
    }


def page_body(
    items: list[dict] | None = None,
    *,
    run_id: str = "run_fake",
    generation: int = 1,
    snapshot_seq: int = 100,
    next_cursor: str | None = None,
) -> dict:
    return {
        "run_id": run_id,
        "generation": generation,
        "snapshot_seq": snapshot_seq,
        "next_cursor": next_cursor,
        "items": items or [],
    }


def ok(payload: dict, *, status: int = 200, byte_count: int = 512) -> DriverResponse:
    return DriverResponse(
        status=status,
        payload=payload,
        byte_count=byte_count,
        content_type="application/json",
    )


def deal_row(index: int, *, source_seq: int | None = None) -> dict:
    return {
        "deal_id": f"deal-{index:06d}",
        "source_seq": source_seq if source_seq is not None else index * 2,
        "account_id": "acct-0001",
        "position_id": "pos-00001",
        "symbol": "EURUSD",
        "entry": "IN",
        "side": "BUY",
        "volume_lots": "0.10",
        "price": "1.08500",
        "realized_pnl_gross": "0.00",
        "commission": "-0.70",
        "swap": "0.00",
        "executed_at": "2026-10-01T00:00:00.000Z",
    }


def account_row(index: int) -> dict:
    return {
        "account_id": f"acct-{index:04d}",
        "currency": "USD",
        "balance": "10000.00",
        "credit": "0.00",
    }


def make_fake_driver(
    *,
    run_id: str = "run_fake",
    generation: int = 1,
    responder: Any = None,
    max_page_size: int = 100,
) -> FakeBlockingDriver:
    meta = fake_meta(run_id=run_id, generation=generation, max_page_size=max_page_size)
    identity = DriverIdentity(
        source_kind="fake",
        contract_version="v1",
        run_id=run_id,
        generation=generation,
        raw_meta=meta,
    )

    def default_responder(operation: str, params: dict) -> DriverResponse:
        if operation == "fetch_meta":
            return ok(meta)
        return ok(page_body(run_id=run_id, generation=generation))

    return FakeBlockingDriver(
        identity=identity,
        default_response=ok(page_body(run_id=run_id, generation=generation)),
        response_factory=responder or default_responder,
    )


@pytest.fixture
def fake_driver() -> FakeBlockingDriver:
    return make_fake_driver()


async def make_connector(
    driver: FakeBlockingDriver,
    *,
    settings: Settings | None = None,
    clock: Any = None,
    connect: bool = True,
) -> Connector:
    resolved = settings or make_settings()
    connector = Connector(
        resolved,
        driver_factory=lambda: driver,
        clock=clock,
        metrics=Metrics(source_id=resolved.source_id),
    )
    if connect:
        await connector.connect()
    return connector


@pytest.fixture
async def connector(fake_driver: FakeBlockingDriver) -> AsyncIterator[Connector]:
    instance = await make_connector(fake_driver)
    try:
        yield instance
    finally:
        await instance.close()


# ---------------------------------------------------------------------------
# The contract double, over a real socket
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def dataset_spec() -> DatasetSpec:
    return DatasetSpec(seed=42, accounts=10, deals=50, positions=8)


@pytest.fixture
def sim_server(dataset_spec: DatasetSpec) -> Iterator[FakeSimulatorServer]:
    state = FakeSimulator(build_dataset(dataset_spec))
    server = FakeSimulatorServer.start(state)
    try:
        yield server
    finally:
        server.stop()


@pytest.fixture
def sim_settings(sim_server: FakeSimulatorServer) -> Settings:
    return make_settings(
        base_url=sim_server.base_url,
        source_kind="simulator",
        driver="simulator_sync_http",
        page_total_deadline_ms=3_000,
        queue_wait_cap_ms=500,
        upstream_read_timeout_ms=1_500,
    )


@pytest.fixture
def data_token_env(monkeypatch: pytest.MonkeyPatch) -> str:
    monkeypatch.setenv("SIMULATOR_DATA_TOKEN", DATA_TOKEN)
    return DATA_TOKEN


@pytest.fixture
def admin_token() -> str:
    return ADMIN_TOKEN


@pytest.fixture
async def sim_connector(
    sim_settings: Settings, data_token_env: str
) -> AsyncIterator[Connector]:
    from mt5_connector.client import build_driver_factory

    connector = Connector(
        sim_settings,
        driver_factory=build_driver_factory(sim_settings, token=data_token_env),
        metrics=Metrics(source_id=sim_settings.source_id),
    )
    try:
        yield connector
    finally:
        await connector.close()


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


async def wait_for(predicate: Any, *, timeout_s: float = 2.0, interval_s: float = 0.005) -> bool:
    """Poll a predicate on the event loop. Returns whether it became true."""
    import time as _time

    deadline = _time.monotonic() + timeout_s
    while _time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval_s)
    return predicate()
