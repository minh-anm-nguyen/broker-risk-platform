"""Concurrency and backpressure: WF-025, WF-048, WF-052, WF-056, WF-090,
WF-092, WF-093, INV-16 (partly).

The claim under test is not "it is fast". It is that under concurrency the
service either answers correctly or refuses in a named, bounded way - and that
it never returns a half-committed read, never deadlocks, and never grows
without limit.
"""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.main import create_app
from tests.conftest import (
    ADMIN_TOKEN,
    DATA_TOKEN,
    create_run,
    make_settings,
    wait_for_job,
)

pytestmark = pytest.mark.integration

ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
DATA = {"Authorization": f"Bearer {DATA_TOKEN}"}


def test_concurrent_reads_during_generation_never_see_half_a_commit(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """WF-048: 100 concurrent reads while the writer runs.

    Every page must be internally consistent with its own ``snapshot_seq``:
    no deal beyond it, and the envelope identity never mixed.
    """
    accepted = create_run(client, admin_headers, account_count=15, deal_count=1_200, wait=False)

    problems: list[str] = []

    def read(index: int) -> None:
        route = ["/v1/deals?limit=50", "/v1/positions?limit=50", "/v1/accounts?limit=20", "/v1/meta"][
            index % 4
        ]
        response = client.get(route, headers=data_headers)
        if response.status_code != 200:
            problems.append(f"{route} -> {response.status_code} {response.text[:120]}")
            return
        body = response.json()
        if "items" not in body:
            return
        snapshot = body["snapshot_seq"]
        for item in body["items"]:
            if "source_seq" in item and item["source_seq"] > snapshot:
                problems.append(f"row beyond snapshot: {item['source_seq']} > {snapshot}")
        if body["run_id"] != "run_0001":
            problems.append(f"unexpected run identity {body['run_id']}")

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(read, range(100)))

    job = wait_for_job(client, admin_headers, accepted["status_url"], timeout=180)
    assert job["status"] == "SUCCEEDED", job
    assert problems == [], problems[:5]

    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0


def test_liveness_answers_while_the_data_plane_is_saturated(tmp_path) -> None:
    """WF-056: liveness has its own budget and touches no database."""
    settings = make_settings(
        tmp_path,
        max_data_requests_in_flight=2,
        admission_timeout_ms=50,
        rate_limit_enabled=False,
    )
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=10, deal_count=600, wait=False)

        live_statuses: list[int] = []
        stop_at = time.monotonic() + 1.5

        def hammer(_: int) -> None:
            while time.monotonic() < stop_at:
                client.get("/v1/deals?limit=200", headers=DATA)

        def probe(_: int) -> None:
            while time.monotonic() < stop_at:
                live_statuses.append(client.get("/health/live").status_code)
                time.sleep(0.01)

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(hammer, index) for index in range(8)]
            futures.append(pool.submit(probe, 0))
            for future in futures:
                future.result()

        assert live_statuses, "the probe never ran"
        assert set(live_statuses) == {200}, set(live_statuses)


def test_admission_control_refuses_rather_than_queues_without_limit(tmp_path) -> None:
    """WF-092: a bounded refusal, with a name, instead of unbounded waiting."""
    settings = make_settings(
        tmp_path,
        max_data_requests_in_flight=1,
        admission_timeout_ms=10,
        rate_limit_enabled=False,
    )
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=10, deal_count=800)

        # A deliberate delay holds the single slot so others must be refused.
        client.post(
            "/admin/v1/faults",
            json={
                "rule_id": "f-hold",
                "scope": {"path": "/v1/deals"},
                "trigger": {"kind": "always", "max_activations": 100, "ttl_seconds": 60},
                "action": {"mode": "delay_before_response", "parameters": {"delay_ms": 300}},
            },
            headers=ADMIN,
        )

        def request(_: int) -> int:
            return client.get("/v1/deals?limit=10", headers=DATA).status_code

        with ThreadPoolExecutor(max_workers=8) as pool:
            statuses = list(pool.map(request, range(8)))

        assert 503 in statuses, statuses
        refused = client.get("/v1/deals?limit=10", headers=DATA)
        # Confirm the refusal is the named, retryable one.
        if refused.status_code == 503:
            error = refused.json()["error"]
            assert error["code"] == "SERVICE_NOT_READY"
            assert error["retryable"] is True
            assert error["details"]["cause"] == "admission_timeout"
            assert error["details"]["max_in_flight"] == 1

        client.delete("/admin/v1/faults", headers=ADMIN)
        # Once the pressure is gone the service recovers without intervention.
        assert client.get("/v1/deals?limit=10", headers=DATA).status_code == 200

        metrics = client.get("/admin/v1/metrics", headers=ADMIN).text
        assert "sim_requests_rejected_total" in metrics


def test_the_writer_queue_is_bounded_and_rejects_cleanly(tmp_path) -> None:
    """WF-090: the queue says no instead of growing until the kernel decides."""
    from simulator.domain.models import WriterOverloadedError
    from simulator.observability.metrics import Metrics
    from simulator.persistence.connection import initialize_database
    from simulator.persistence.writer import WriteCommand, WriterService
    from simulator.domain.models import UpdateMark
    from decimal import Decimal

    settings = make_settings(tmp_path, max_pending_events=16, writer_batch_size=1)
    initialize_database(settings)
    writer = WriterService(settings, metrics=Metrics())
    # Deliberately not started: nothing drains the queue, so the bound is
    # reached deterministically rather than by racing a live writer.
    writer._running = True  # noqa: SLF001 - exercising the bound directly
    try:
        accepted = 0
        refused = None
        for _ in range(200):
            try:
                writer.submit_command(
                    WriteCommand(
                        run_id="run_0001",
                        intent=UpdateMark("EURUSD", Decimal("1.1"), Decimal("1.2")),
                    )
                )
                accepted += 1
            except WriterOverloadedError as error:
                refused = error
                break
        assert accepted == 16, accepted
        assert refused is not None
        assert refused.code == "WRITER_OVERLOADED"
        assert refused.retryable is True
        assert refused.details["queue_capacity"] == 16
        assert writer.queue_depth == 16
    finally:
        writer._running = False  # noqa: SLF001
        writer._fail_remaining(  # noqa: SLF001
            WriterOverloadedError("test teardown")
        )


def test_a_slow_writer_does_not_block_reads_or_health(tmp_path) -> None:
    """WF-093: SQLite work happens off the event loop."""
    settings = make_settings(tmp_path, rate_limit_enabled=False)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        accepted = create_run(client, ADMIN, account_count=20, deal_count=2_000, wait=False)

        latencies: list[float] = []
        for _ in range(25):
            started = time.perf_counter()
            response = client.get("/health/live")
            latencies.append(time.perf_counter() - started)
            assert response.status_code == 200
            started = time.perf_counter()
            assert client.get("/v1/meta", headers=DATA).status_code == 200
            latencies.append(time.perf_counter() - started)

        wait_for_job(client, ADMIN, accepted["status_url"], timeout=180)
        # Generous bound: the point is that the loop is not blocked for
        # seconds at a time, not a latency SLO (that is the benchmark's job).
        assert max(latencies) < 2.0, max(latencies)


def test_concurrent_closes_of_one_position_cannot_over_close(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-025: one succeeds, the rest are refused; the volume never goes negative."""
    create_run(client, admin_headers, account_count=2, deal_count=0)
    client.post(
        "/admin/v1/runs/run_0001/events",
        json={
            "kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
            "side": "BUY", "volume_lots": "1.00", "price": "1.10000",
        },
        headers={**admin_headers, "Idempotency-Key": "open-one"},
    )

    def close(index: int):
        return client.post(
            "/admin/v1/runs/run_0001/events",
            json={
                "kind": "CLOSE_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
                "volume_lots": "1.00", "price": "1.10000",
            },
            headers={**admin_headers, "Idempotency-Key": f"close-{index}"},
        )

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(close, range(6)))

    created = [item for item in responses if item.status_code == 201]
    assert len(created) == 1, [item.status_code for item in responses]
    assert all(item.status_code == 404 for item in responses if item.status_code != 201)

    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0
    assert report["record_counts"]["deals"] == 2  # the open and exactly one close


def test_a_client_that_disconnects_releases_its_slot(tmp_path) -> None:
    """WF-052: an abandoned request must not leak an admission permit."""
    settings = make_settings(
        tmp_path, max_data_requests_in_flight=4, admission_timeout_ms=200,
        rate_limit_enabled=False,
    )
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=5, deal_count=200)
        client.post(
            "/admin/v1/faults",
            json={
                "rule_id": "f-slow",
                "scope": {"path": "/v1/deals"},
                "trigger": {"kind": "always", "max_activations": 100, "ttl_seconds": 60},
                "action": {"mode": "delay_before_response", "parameters": {"delay_ms": 150}},
            },
            headers=ADMIN,
        )

        def abandon(_: int) -> None:
            try:
                client.get("/v1/deals?limit=5", headers=DATA, timeout=0.02)
            except Exception:
                pass  # the abandonment is the point

        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(abandon, range(16)))

        client.delete("/admin/v1/faults", headers=ADMIN)
        # If permits leaked, every subsequent request would be refused forever.
        for _ in range(10):
            assert client.get("/v1/deals?limit=5", headers=DATA).status_code == 200
        assert client.get("/health/ready").json()["requests_in_flight"] <= 1


def test_many_distinct_scans_stay_within_the_snapshot_budget(tmp_path) -> None:
    """WF-043 under concurrency: bounded, and never silently wrong."""
    settings = make_settings(
        tmp_path, max_active_snapshots=5, rate_limit_enabled=False
    )
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=20, deal_count=600)

        def scan(index: int) -> tuple[int, bool]:
            response = client.get(
                f"/v1/deals?account_id=A-{index + 1:06d}&limit=1", headers=DATA
            )
            return response.status_code, response.status_code == 200

        with ThreadPoolExecutor(max_workers=10) as pool:
            results = list(pool.map(scan, range(20)))

        statuses = {status for status, _ in results}
        assert statuses <= {200, 429}, statuses
        ready = client.get("/health/ready").json()
        assert ready["active_snapshots"] <= 5
        assert ready["ready"] is True

        report = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()
        assert report["summary"]["failed"] == 0
