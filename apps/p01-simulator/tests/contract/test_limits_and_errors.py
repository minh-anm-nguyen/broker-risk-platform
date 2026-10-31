"""Bounded resources and the error envelope.

Every knob here has a finite ceiling, and crossing it produces a *named* error
rather than a slow death. The distinction the tests insist on: an over-limit
request is refused, never silently clamped. Clamping ``limit=100000000`` to 500
would let a client believe it had received everything (WF-040, WF-091).
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.api.errors import CODE_TO_STATUS, ERROR_CODES
from simulator.main import create_app
from tests.conftest import ADMIN_TOKEN, DATA_TOKEN, create_run, make_settings

pytestmark = pytest.mark.contract


def test_error_envelope_has_the_documented_shape(client: TestClient) -> None:
    """Section 5.3."""
    response = client.get("/v1/deals", headers={"Authorization": "Bearer wrong"})
    body = response.json()
    assert set(body) == {"error"}
    error = body["error"]
    assert set(error) == {"code", "message", "retryable", "request_id", "run_id", "details"}
    assert error["code"] in ERROR_CODES
    assert isinstance(error["retryable"], bool)
    assert error["request_id"] == response.headers["x-request-id"]


def test_every_mapped_code_has_a_sane_status() -> None:
    """The code-to-status table is an API surface, so it is asserted directly."""
    assert CODE_TO_STATUS["INVALID_CURSOR"] == 400
    assert CODE_TO_STATUS["UNAUTHORIZED"] == 401
    assert CODE_TO_STATUS["FORBIDDEN"] == 403
    assert CODE_TO_STATUS["NOT_FOUND"] == 404
    assert CODE_TO_STATUS["IDEMPOTENCY_CONFLICT"] == 409
    # 410 means "start a new sync", which is what makes it different from 400.
    assert CODE_TO_STATUS["SNAPSHOT_EXPIRED"] == 410
    assert CODE_TO_STATUS["RUN_REPLACED"] == 410
    assert CODE_TO_STATUS["PAYLOAD_TOO_LARGE"] == 413
    assert CODE_TO_STATUS["VALIDATION_FAILED"] == 422
    assert CODE_TO_STATUS["UNSUPPORTED_OPERATION"] == 422
    assert CODE_TO_STATUS["RATE_LIMITED"] == 429
    assert CODE_TO_STATUS["SERVICE_NOT_READY"] == 503
    assert CODE_TO_STATUS["WRITER_OVERLOADED"] == 503
    assert CODE_TO_STATUS["STORAGE_ERROR"] == 503
    assert CODE_TO_STATUS["SIMULATED_UPSTREAM_TIMEOUT"] == 504


def test_retryable_errors_carry_a_retry_after_header(
    client: TestClient, data_headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    """RFC 9110: give the client a number instead of making it guess."""
    create_run(client, admin_headers)
    client.post(
        "/admin/v1/faults",
        json={
            "rule_id": "f-429",
            "scope": {"path": "/v1/deals"},
            "trigger": {"kind": "always", "max_activations": 5},
            "action": {"mode": "http_error", "parameters": {"status": 429, "retry_after_seconds": 7}},
        },
        headers=admin_headers,
    )
    response = client.get("/v1/deals?limit=1", headers=data_headers)
    assert response.status_code == 429
    assert response.headers["retry-after"] == "7"
    assert response.json()["error"]["retryable"] is True


def test_internal_errors_never_leak_a_traceback(
    client: TestClient, data_headers: dict[str, str], admin_headers: dict[str, str]
) -> None:
    create_run(client, admin_headers)
    client.post(
        "/admin/v1/faults",
        json={
            "rule_id": "f-500",
            "scope": {"path": "/v1/deals"},
            "trigger": {"kind": "always", "max_activations": 2},
            "action": {"mode": "http_error", "parameters": {"status": 500}},
        },
        headers=admin_headers,
    )
    response = client.get("/v1/deals?limit=1", headers=data_headers)
    assert response.status_code == 500
    text = response.text
    for leak in ("Traceback", "File \"", "simulator/", "sqlite3", "SELECT"):
        assert leak not in text


# ---------------------------------------------------------------------------
# Page size
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("route", ["/v1/accounts", "/v1/positions", "/v1/deals", "/v1/marks"])
@pytest.mark.parametrize("limit", [1, 500])
def test_valid_page_sizes_are_accepted(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any],
    route: str, limit: int,
) -> None:
    """WF-040."""
    response = client.get(f"{route}?limit={limit}", headers=data_headers)
    assert response.status_code == 200
    assert len(response.json()["items"]) <= limit


@pytest.mark.parametrize("limit", [0, -1, 501, 100_000_000, 2**63])
def test_out_of_range_page_sizes_are_refused_not_clamped(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any], limit: int
) -> None:
    """WF-040, WF-091: clamping would be a silent lie about completeness."""
    response = client.get(f"/v1/deals?limit={limit}", headers=data_headers)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_FAILED"


def test_the_default_page_size_is_applied_when_omitted(
    client: TestClient, data_headers: dict[str, str], settings, seeded_run: dict[str, Any]
) -> None:
    body = client.get("/v1/deals", headers=data_headers).json()
    assert len(body["items"]) == min(settings.default_page_size, 120)


def test_a_non_numeric_limit_is_refused(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    assert client.get("/v1/deals?limit=lots", headers=data_headers).status_code == 422


# ---------------------------------------------------------------------------
# Snapshot budget
# ---------------------------------------------------------------------------


def test_exceeding_the_snapshot_budget_refuses_new_scans_only(tmp_path) -> None:
    """WF-043: never break a scan already in progress to admit a new one."""
    settings = make_settings(tmp_path, max_active_snapshots=2)
    app = create_app(settings, lock_process=False)
    data = {"Authorization": f"Bearer {DATA_TOKEN}"}
    admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    with TestClient(app) as client:
        create_run(client, admin)
        held = []
        for account in ("A-000001", "A-000002"):
            page = client.get(f"/v1/deals?account_id={account}&limit=1", headers=data)
            assert page.status_code == 200
            if page.json()["next_cursor"]:
                held.append(page.json()["next_cursor"])

        refused = None
        for account in ("A-000003", "A-000004", "A-000005"):
            response = client.get(f"/v1/deals?account_id={account}&limit=1", headers=data)
            if response.status_code == 429:
                refused = response
                break
        assert refused is not None, "the snapshot budget was never reached"
        assert refused.json()["error"]["code"] == "SNAPSHOT_CAPACITY_REACHED"
        assert refused.headers["retry-after"]

        # Scans already in flight keep working.
        for cursor in held:
            assert client.get(f"/v1/deals?cursor={cursor}", headers=data).status_code == 200


def test_a_single_page_response_consumes_no_snapshot(tmp_path) -> None:
    """Minting a lease for a finished scan would burn the budget for nothing."""
    settings = make_settings(tmp_path, max_active_snapshots=1)
    app = create_app(settings, lock_process=False)
    data = {"Authorization": f"Bearer {DATA_TOKEN}"}
    admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    with TestClient(app) as client:
        create_run(client, admin)
        for _ in range(20):
            response = client.get("/v1/deals?limit=500", headers=data)
            assert response.status_code == 200
            assert response.json()["next_cursor"] is None
        assert client.get("/health/ready").json()["active_snapshots"] == 0


# ---------------------------------------------------------------------------
# Rate limiting and admission
# ---------------------------------------------------------------------------


def test_the_rate_limit_rejects_rather_than_queues(tmp_path) -> None:
    """WF-074: a 429 is the cheap answer; queueing would hide the overload.

    No run is created here: ``/v1/meta`` answers without one, and polling a
    generation job would spend the admin principal's own budget and make the
    test about the wrong thing.
    """
    settings = make_settings(tmp_path, rate_limit_rps=1.0, rate_limit_burst=3)
    app = create_app(settings, lock_process=False)
    data = {"Authorization": f"Bearer {DATA_TOKEN}"}
    with TestClient(app) as client:
        statuses = [client.get("/v1/meta", headers=data).status_code for _ in range(12)]
        assert 429 in statuses, statuses
        limited = next(
            response
            for response in (client.get("/v1/meta", headers=data) for _ in range(5))
            if response.status_code == 429
        )
        body = limited.json()["error"]
        assert body["code"] == "RATE_LIMITED"
        assert body["retryable"] is True
        assert limited.headers["retry-after"]
        # Liveness is never rate limited: a probe must still answer (WF-056).
        assert client.get("/health/live").status_code == 200


def test_rate_limits_are_per_principal(tmp_path) -> None:
    """One noisy consumer must not exhaust the operator's own budget."""
    settings = make_settings(tmp_path, rate_limit_rps=1.0, rate_limit_burst=4)
    app = create_app(settings, lock_process=False)
    data = {"Authorization": f"Bearer {DATA_TOKEN}"}
    admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    with TestClient(app) as client:
        # The run row exists as soon as the 202 returns, so there is no need to
        # poll the job and spend the admin budget.
        create_run(client, admin, wait=False)
        for _ in range(8):
            client.get("/v1/meta", headers=data)
        assert client.get("/v1/meta", headers=data).status_code == 429
        # The admin principal has its own bucket and still has tokens left.
        assert client.get("/admin/v1/runs/current", headers=admin).status_code == 200


def test_rate_limiting_can_be_disabled_for_benchmarks(tmp_path) -> None:
    """Counting rate-limited traffic as throughput would invalidate a result."""
    settings = make_settings(tmp_path, rate_limit_enabled=False)
    app = create_app(settings, lock_process=False)
    data = {"Authorization": f"Bearer {DATA_TOKEN}"}
    admin = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
    with TestClient(app) as client:
        create_run(client, admin)
        statuses = {client.get("/v1/meta", headers=data).status_code for _ in range(60)}
        assert statuses == {200}


# ---------------------------------------------------------------------------
# Readiness reflects real limits
# ---------------------------------------------------------------------------


def test_readiness_reports_the_bounds_it_is_enforcing(
    client: TestClient, settings, seeded_run: dict[str, Any]
) -> None:
    body = client.get("/health/ready").json()
    assert body["ready"] is True
    assert body["reasons"] == []
    assert body["writer_queue_capacity"] == settings.max_pending_events
    assert body["schema_version"] == 1
    assert body["writer_running"] is True
    assert body["recovery_required"] is False
    assert body["faults_armed"] is True


def test_readiness_is_true_with_no_run_but_names_no_false_reasons(
    client: TestClient,
) -> None:
    """Readiness means "can serve consistent reads of whatever exists"."""
    body = client.get("/health/ready").json()
    assert body["ready"] is True
    assert body["run_id"] is None
    assert body["committed_seq"] == 0


def test_liveness_does_not_touch_the_database(client: TestClient) -> None:
    """WF-056: it must answer when every data slot and connection is busy."""
    body = client.get("/health/live").json()
    assert body == {"status": "alive", "version": "1.0.0"}


def test_unknown_routes_use_the_error_envelope(client: TestClient) -> None:
    response = client.get("/v1/nonexistent")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "NOT_FOUND"


def test_wrong_method_uses_the_error_envelope(
    client: TestClient, data_headers: dict[str, str]
) -> None:
    response = client.post("/v1/deals", headers=data_headers)
    assert response.status_code == 405
    assert "error" in response.json()
