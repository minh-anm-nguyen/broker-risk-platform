"""Shared fixtures.

Every fixture builds a fully independent application against a temporary
database. There are no module-level singletons to reset, so a test cannot
inherit state from the one before it - which is the only way a suite that
asserts on sequence numbers and digests stays trustworthy.

``enforce_single_process_lock`` is off in tests because many applications run
inside one pytest process by design. The lock itself is tested directly in
``tests/contract/test_security.py``.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

# The package lives under src/ and the suite runs without an install step.
_SRC = Path(__file__).resolve().parents[1] / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from fastapi.testclient import TestClient  # noqa: E402

from simulator.generation.coordinator import ScenarioCoordinator  # noqa: E402
from simulator.main import create_app  # noqa: E402
from simulator.observability.metrics import Metrics  # noqa: E402
from simulator.persistence.connection import (  # noqa: E402
    ReadConnectionPool,
    initialize_database,
    open_connection,
)
from simulator.persistence.writer import WriterService  # noqa: E402
from simulator.settings import Settings  # noqa: E402

DATA_TOKEN = "test-data-token"
ADMIN_TOKEN = "test-admin-token"

#: A scenario small enough to generate in well under a second but large enough
#: to exercise paging, partial closes and multiple symbols.
BASELINE_SCENARIO: dict[str, Any] = {
    "scenario_name": "test-baseline",
    "seed": 42,
    "start_virtual_time": "2026-10-30T00:00:00Z",
    "account_count": 10,
    "deal_count": 120,
    "max_open_positions": 20,
    "symbols": ["EURUSD", "GBPUSD", "AUDUSD"],
    "initial_balance_usd": "10000.00",
}


def make_settings(tmp_path: Path, **overrides: Any) -> Settings:
    base: dict[str, Any] = {
        "db_path": str(tmp_path / "sim.db"),
        "data_token": DATA_TOKEN,
        "admin_token": ADMIN_TOKEN,
        "cursor_signing_key": "test-cursor-signing-key",
        "enforce_single_process_lock": False,
        "writer_batch_size": 50,
        "metrics_enabled": True,
        # A tiny snapshot TTL would make paging tests flaky; an explicit value
        # keeps the TTL assertions deliberate instead of accidental.
        "snapshot_ttl_sec": 900,
    }
    base.update(overrides)
    return Settings(**base)


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path)


@pytest.fixture
def client(settings: Settings) -> Iterator[TestClient]:
    app = create_app(settings, lock_process=False)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture
def data_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {DATA_TOKEN}"}


@pytest.fixture
def admin_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {ADMIN_TOKEN}"}


def wait_for_job(
    client: TestClient,
    admin_headers: dict[str, str],
    status_url: str,
    *,
    timeout: float = 60.0,
) -> dict[str, Any]:
    """Poll a job to a terminal state.

    Polling a status endpoint, not sleeping a fixed interval: a fixed sleep
    either makes the suite slow or makes it flaky, and usually both (WF-118).
    """
    deadline = time.monotonic() + timeout
    payload: dict[str, Any] = {}
    while time.monotonic() < deadline:
        response = client.get(status_url, headers=admin_headers)
        assert response.status_code == 200, response.text
        payload = response.json()
        if payload["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            return payload
        time.sleep(0.01)
    raise AssertionError(f"job did not finish within {timeout}s: {payload}")


def create_run(
    client: TestClient,
    admin_headers: dict[str, str],
    *,
    idempotency_key: str = "test-create-run",
    wait: bool = True,
    **overrides: Any,
) -> dict[str, Any]:
    body = {**BASELINE_SCENARIO, **overrides}
    response = client.post(
        "/admin/v1/runs",
        json=body,
        headers={**admin_headers, "Idempotency-Key": idempotency_key},
    )
    assert response.status_code == 202, response.text
    accepted = response.json()
    if wait and accepted.get("status_url"):
        job = wait_for_job(client, admin_headers, accepted["status_url"])
        assert job["status"] == "SUCCEEDED", job
        accepted["job"] = job
    return accepted


@pytest.fixture
def seeded_run(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> dict[str, Any]:
    """A completed baseline run, with its meta snapshot already read."""
    accepted = create_run(client, admin_headers)
    meta = client.get("/v1/meta", headers=data_headers)
    assert meta.status_code == 200, meta.text
    return {**accepted, "meta": meta.json()}


@pytest.fixture
def db_connection(settings: Settings):
    """Read-only connection for oracle queries, independent of the service."""
    connection = open_connection(settings, writable=False)
    try:
        yield connection
    finally:
        connection.close()


class Runtime:
    """Writer plus coordinator without the HTTP layer.

    Used by tests that are about storage or generation semantics, where going
    through HTTP would only add an authentication path that is tested
    elsewhere.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.schema_version = initialize_database(settings)
        self.metrics = Metrics()
        self.pool = ReadConnectionPool(settings)
        self.writer = WriterService(settings, metrics=self.metrics)
        self.writer.start()
        self.coordinator = ScenarioCoordinator(
            settings, self.writer, self.pool, metrics=self.metrics
        )
        self.coordinator.startup()

    def close(self) -> None:
        self.coordinator.shutdown()
        self.writer.stop(drain=True, timeout=20.0)
        self.pool.close()


@pytest.fixture
def runtime(settings: Settings) -> Iterator[Runtime]:
    instance = Runtime(settings)
    try:
        yield instance
    finally:
        instance.close()


def decimal_of(value: str) -> Decimal:
    return Decimal(value)


def collect_all_pages(
    client: TestClient,
    headers: dict[str, str],
    url: str,
    *,
    max_pages: int = 1_000,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Walk a cursor to exhaustion, returning (items, raw page envelopes).

    Follow-up requests send *only* the cursor and drop the original query
    string. Re-sending identical filters alongside a cursor is also legal, but
    this exercises the path a connector actually takes: the filters travel
    inside the signed token, so the cursor alone must be sufficient.
    """
    base = url.split("?")[0]
    items: list[dict[str, Any]] = []
    pages: list[dict[str, Any]] = []
    next_url: str | None = url
    for _ in range(max_pages):
        if next_url is None:
            return items, pages
        response = client.get(next_url, headers=headers)
        assert response.status_code == 200, response.text
        page = response.json()
        pages.append(page)
        items.extend(page["items"])
        cursor = page["next_cursor"]
        next_url = f"{base}?cursor={cursor}" if cursor else None
    raise AssertionError("pagination did not terminate")
