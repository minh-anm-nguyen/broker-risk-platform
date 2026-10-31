"""Control-plane HTTP contract: runs, jobs, injected events, faults.

The recurring theme is that creating state requires an ``Idempotency-Key``.
These are exactly the calls whose response can be lost, and a retry without a
key would create a second run or a second event.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.conftest import BASELINE_SCENARIO, create_run, wait_for_job

pytestmark = pytest.mark.contract

IDEMPOTENT_ROUTES = [
    ("POST", "/admin/v1/runs", BASELINE_SCENARIO),
    ("POST", "/admin/v1/runs/run_0001/reset", {}),
    ("POST", "/admin/v1/runs/run_0001/advance", {"events": 1}),
    ("POST", "/admin/v1/runs/run_0001/events", {"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.1", "ask": "1.2"}),
]


@pytest.mark.parametrize(("method", "route", "body"), IDEMPOTENT_ROUTES)
def test_state_creating_routes_require_an_idempotency_key(
    client: TestClient, admin_headers: dict[str, str], method: str, route: str, body: dict
) -> None:
    """WF-053: without a key, a lost response means a duplicated effect."""
    response = client.request(method, route, json=body, headers=admin_headers)
    assert response.status_code == 422
    error = response.json()["error"]
    assert error["code"] == "VALIDATION_FAILED"
    assert error["details"]["header"] == "Idempotency-Key"


def test_run_creation_is_accepted_asynchronously(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """Section 5.2: 100k deals are not generated inside one HTTP request."""
    response = client.post(
        "/admin/v1/runs",
        json=BASELINE_SCENARIO,
        headers={**admin_headers, "Idempotency-Key": "create-1"},
    )
    assert response.status_code == 202
    body = response.json()
    assert body["run_id"] == "run_0001"
    assert body["generation"] == 1
    assert body["status"] == "PENDING"
    assert body["status_url"] == f"/admin/v1/jobs/{body['job_id']}"
    assert body["replayed"] is False


def test_job_reports_progress_in_a_named_unit(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """``processed_count`` is meaningless without saying what it counts."""
    accepted = create_run(client, admin_headers, wait=False)
    job = wait_for_job(client, admin_headers, accepted["status_url"])
    assert job["status"] == "SUCCEEDED"
    assert job["kind"] == "generate"
    assert job["unit"] == "deals"
    assert job["processed_count"] == job["target_count"] == 120
    assert job["error_code"] is None
    assert job["started_at"] and job["finished_at"]


def test_current_run_echoes_the_spec_for_reproducibility(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """A reproducibility claim is only checkable if the inputs are visible."""
    create_run(client, admin_headers)
    body = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert body["seed"] == BASELINE_SCENARIO["seed"]
    assert body["spec"]["scenario_name"] == BASELINE_SCENARIO["scenario_name"]
    assert body["spec"]["symbols"] == BASELINE_SCENARIO["symbols"]
    assert body["generator_version"]
    assert body["accounting_policy_version"]
    assert body["symbol_catalog_version"]
    # Nothing secret travels in it.
    serialized = str(body).lower()
    assert "token" not in serialized
    assert "signing" not in serialized


def test_current_run_is_404_before_any_run_exists(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    response = client.get("/admin/v1/runs/current", headers=admin_headers)
    assert response.status_code == 404
    assert response.json()["error"]["details"]["hint"] == "POST /admin/v1/runs"


def test_a_second_run_without_reset_is_a_conflict(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """One active run is the model, so the API says so instead of guessing."""
    create_run(client, admin_headers)
    response = client.post(
        "/admin/v1/runs",
        json=BASELINE_SCENARIO,
        headers={**admin_headers, "Idempotency-Key": "create-2"},
    )
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "RUN_CONFLICT"
    assert error["details"]["active_run_id"] == "run_0001"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("account_count", -1),
        ("deal_count", -5),
        ("seed", -1),
        ("symbols", []),
        ("symbols", ["EURUSD", "EURUSD"]),
        ("symbols", ["NOPE"]),
        ("initial_balance_usd", "-10.00"),
        ("initial_balance_usd", 1000.5),
        ("initial_balance_usd", "NaN"),
        ("start_virtual_time", "not-a-timestamp"),
        ("generator_version", "99.0"),
        ("account_count", "ten"),
        ("deal_count", None),
    ],
)
def test_invalid_scenarios_are_refused_without_creating_a_job(
    client: TestClient, admin_headers: dict[str, str], field: str, value: Any
) -> None:
    """WF-006, WF-007: reject the spec, do not half-start a run."""
    response = client.post(
        "/admin/v1/runs",
        json={**BASELINE_SCENARIO, field: value},
        headers={**admin_headers, "Idempotency-Key": f"bad-{field}-{value}"},
    )
    assert response.status_code == 422, (field, value, response.text)
    assert client.get("/admin/v1/runs/current", headers=admin_headers).status_code == 404


def test_unknown_scenario_fields_are_refused(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """Ignoring an unexpected field lets a caller think it took effect."""
    response = client.post(
        "/admin/v1/runs",
        json={**BASELINE_SCENARIO, "hedging_mode": True},
        headers={**admin_headers, "Idempotency-Key": "extra-field"},
    )
    assert response.status_code == 422


def test_deal_count_without_accounts_is_refused(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    response = client.post(
        "/admin/v1/runs",
        json={**BASELINE_SCENARIO, "account_count": 0, "deal_count": 10},
        headers={**admin_headers, "Idempotency-Key": "no-accounts"},
    )
    assert response.status_code == 422


def test_fx_conversion_symbols_are_refused_at_scenario_level(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-029: better to refuse than to generate confidently wrong P&L."""
    response = client.post(
        "/admin/v1/runs",
        json={**BASELINE_SCENARIO, "symbols": ["USDJPY"]},
        headers={**admin_headers, "Idempotency-Key": "fx"},
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "UNSUPPORTED_OPERATION"


# ---------------------------------------------------------------------------
# Injected events
# ---------------------------------------------------------------------------


def inject(
    client: TestClient, admin_headers: dict[str, str], key: str, **payload: Any
):
    return client.post(
        "/admin/v1/runs/run_0001/events",
        json=payload,
        headers={**admin_headers, "Idempotency-Key": key},
    )


def test_injecting_a_structured_event_returns_its_coordinates(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    before = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    response = inject(
        client,
        admin_headers,
        "inject-1",
        kind="UPDATE_MARK",
        symbol="EURUSD",
        bid="1.10500",
        ask="1.10510",
    )
    assert response.status_code == 201
    body = response.json()
    assert body["seq"] == before["committed_seq"] + 1
    assert body["event_type"] == "MARK_PRICE_UPDATED"
    assert body["event_id"] == f"E-{body['seq']:012d}"
    assert body["deal_id"] is None
    assert body["replayed"] is False


def test_injected_open_and_close_produce_exact_money(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """The section 3.4 vector, driven through the HTTP surface end to end."""
    create_run(client, admin_headers, account_count=0, deal_count=0)
    inject(client, admin_headers, "a", kind="CREATE_ACCOUNT", account_id="A-000001", balance="10000.00")
    inject(client, admin_headers, "m", kind="UPDATE_MARK", symbol="EURUSD", bid="1.10000", ask="1.10010")
    inject(
        client, admin_headers, "o", kind="OPEN_POSITION", account_id="A-000001", symbol="EURUSD",
        side="BUY", volume_lots="0.50", price="1.10000", commission="-2.50", swap="0.00",
    )
    account = client.get("/v1/accounts/A-000001", headers=data_headers).json()
    assert account["balance"] == "9997.50"

    inject(client, admin_headers, "m2", kind="UPDATE_MARK", symbol="EURUSD", bid="1.10200", ask="1.10210")
    account = client.get("/v1/accounts/A-000001", headers=data_headers).json()
    assert account["unrealized_pnl"] == "100.00"
    assert account["equity"] == "10097.50"

    response = inject(
        client, admin_headers, "c", kind="CLOSE_POSITION", account_id="A-000001", symbol="EURUSD",
        volume_lots="0.20", price="1.10300", commission="-1.00", swap="-0.50",
    )
    assert response.json()["event_type"] == "POSITION_PARTIALLY_CLOSED"
    deals = client.get("/v1/deals?limit=10", headers=data_headers).json()["items"]
    closing = [item for item in deals if item["entry"] == "OUT"][0]
    assert closing["realized_pnl_gross"] == "60.00"
    assert closing["side"] == "SELL"  # closing a long is a sell
    assert client.get("/v1/accounts/A-000001", headers=data_headers).json()["balance"] == "10056.00"


@pytest.mark.parametrize(
    ("payload", "expected_status"),
    [
        ({"kind": "OPEN_POSITION", "symbol": "EURUSD", "side": "BUY", "volume_lots": "1", "price": "1.1"}, 422),
        ({"kind": "ADJUST_ACCOUNT", "account_id": "A-000001"}, 422),
        ({"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.1"}, 422),
        ({"kind": "NOT_A_KIND", "account_id": "A-000001"}, 422),
        ({"kind": "CLOSE_POSITION", "account_id": "A-999999", "symbol": "EURUSD", "volume_lots": "1", "price": "1.1"}, 404),
    ],
)
def test_incomplete_or_impossible_events_are_refused(
    client: TestClient,
    admin_headers: dict[str, str],
    seeded_run: dict[str, Any],
    payload: dict,
    expected_status: int,
) -> None:
    response = inject(client, admin_headers, f"bad-{payload['kind']}-{len(payload)}", **payload)
    assert response.status_code == expected_status, response.text


def test_a_rejected_event_does_not_advance_the_sequence(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-015/WF-016: 'seq không nhảy' on a domain rejection."""
    before = client.get("/admin/v1/runs/current", headers=admin_headers).json()["committed_seq"]
    response = inject(
        client, admin_headers, "reject-1", kind="CLOSE_POSITION", account_id="A-000001",
        symbol="EURUSD", volume_lots="99999", price="1.10000",
    )
    assert response.status_code in {404, 422}
    after = client.get("/admin/v1/runs/current", headers=admin_headers).json()["committed_seq"]
    assert after == before


# ---------------------------------------------------------------------------
# Advance and stop
# ---------------------------------------------------------------------------


def test_advance_moves_the_virtual_clock_without_events(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    before = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    response = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"clock_steps": 100},
        headers={**admin_headers, "Idempotency-Key": "advance-clock"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["job_id"] is None  # no events requested, so no job
    after = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert after["virtual_time"] > before["virtual_time"]
    assert after["committed_seq"] == before["committed_seq"]


def test_advance_generates_more_events_on_the_same_seeded_stream(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    before = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    response = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": 50},
        headers={**admin_headers, "Idempotency-Key": "advance-events"},
    )
    assert response.status_code == 200
    job = wait_for_job(client, admin_headers, response.json()["status_url"])
    assert job["status"] == "SUCCEEDED"
    assert job["kind"] == "advance"
    assert job["unit"] == "events"
    assert job["processed_count"] == 50
    after = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert after["committed_seq"] == before["committed_seq"] + 50


def test_advance_beyond_the_configured_maximum_is_refused(
    client: TestClient, admin_headers: dict[str, str], settings, seeded_run: dict[str, Any]
) -> None:
    response = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": settings.max_advance_events + 1},
        headers={**admin_headers, "Idempotency-Key": "advance-too-big"},
    )
    assert response.status_code == 422


def test_stopping_a_run_freezes_it(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """INV-14: a terminal run accepts no further events."""
    response = client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    assert response.status_code == 200
    assert response.json()["status"] in {"STOPPED", "STOPPING"}
    frozen = client.get("/admin/v1/runs/current", headers=admin_headers).json()["committed_seq"]

    rejected = inject(
        client, admin_headers, "after-stop", kind="UPDATE_MARK", symbol="EURUSD",
        bid="1.10000", ask="1.10010",
    )
    assert rejected.status_code == 409
    assert rejected.json()["error"]["code"] == "RUN_CONFLICT"
    assert client.get("/admin/v1/runs/current", headers=admin_headers).json()["committed_seq"] == frozen


def test_stopping_twice_is_a_no_op_success(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """A teardown path should not have to care whether it already ran."""
    client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    again = client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    assert again.status_code == 200
    assert again.json()["drained"] is True


def test_stopping_an_unknown_run_is_404(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    assert client.post("/admin/v1/runs/run_9999/stop", headers=admin_headers).status_code == 404


def test_unknown_job_is_404(client: TestClient, admin_headers: dict[str, str]) -> None:
    assert client.get("/admin/v1/jobs/job-nope", headers=admin_headers).status_code == 404


# ---------------------------------------------------------------------------
# Fault rule configuration
# ---------------------------------------------------------------------------


def declare_fault(client: TestClient, admin_headers: dict[str, str], **payload: Any):
    return client.post("/admin/v1/faults", json=payload, headers=admin_headers)


def test_declaring_and_listing_a_fault(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    response = declare_fault(
        client,
        admin_headers,
        rule_id="f-1",
        scope={"method": "GET", "path": "/v1/deals"},
        trigger={"kind": "nth_matching_request", "n": 2, "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503, "retry_after_seconds": 2}},
    )
    assert response.status_code == 201
    rule = response.json()
    assert rule["rule_id"] == "f-1"
    assert rule["run_id"] == "run_0001"
    assert rule["match_count"] == 0
    assert rule["activations"] == 0
    assert rule["expires_at"] is not None

    listing = client.get("/admin/v1/faults", headers=admin_headers).json()
    assert listing["armed"] is True
    assert [item["rule_id"] for item in listing["items"]] == ["f-1"]


def test_duplicate_rule_id_is_a_conflict(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    payload = {
        "rule_id": "f-dup",
        "trigger": {"kind": "always"},
        "action": {"mode": "http_error", "parameters": {"status": 503}},
    }
    assert declare_fault(client, admin_headers, **payload).status_code == 201
    assert declare_fault(client, admin_headers, **payload).status_code == 409


@pytest.mark.parametrize(
    "payload",
    [
        # Health and the control plane are never faultable.
        {"scope": {"path": "/health/live"}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        {"scope": {"path": "/health"}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        {"scope": {"path": "/admin/v1/runs"}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        # A path has to be a path.
        {"scope": {"path": "v1/deals"}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        # Only codes the error contract defines.
        {"action": {"mode": "http_error", "parameters": {"status": 418}}},
        {"action": {"mode": "http_error", "parameters": {"status": 200}}},
        # Unknown mode, and a transport layer this API cannot honestly provide.
        {"action": {"mode": "reset_connection", "parameters": {}}},
        {"layer": "transport", "action": {"mode": "http_error", "parameters": {"status": 503}}},
        # Mandatory bounds.
        {"trigger": {"kind": "nth_matching_request", "n": 0}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        {"trigger": {"kind": "probability", "probability": "1.5"}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        {"trigger": {"kind": "always", "ttl_seconds": 0}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        {"trigger": {"kind": "always", "max_activations": 0}, "action": {"mode": "http_error", "parameters": {"status": 503}}},
        # A delay beyond the configured ceiling would wedge the service.
        {"action": {"mode": "delay_before_response", "parameters": {"delay_ms": 10_000_000}}},
        # Field-editing modes need to say which field.
        {"action": {"mode": "omit_field", "parameters": {}}},
        {"action": {"mode": "replace_field_value", "parameters": {"field": "deal_id"}}},
    ],
)
def test_unsafe_or_incomplete_fault_rules_are_refused(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any], payload: dict
) -> None:
    """WF-106: a mistyped rule must not be able to take the service out."""
    response = declare_fault(client, admin_headers, **payload)
    assert response.status_code == 422, response.text


def test_deleting_a_fault_is_idempotent(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    declare_fault(
        client, admin_headers, rule_id="f-del", trigger={"kind": "always"},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    first = client.delete("/admin/v1/faults/f-del", headers=admin_headers).json()
    second = client.delete("/admin/v1/faults/f-del", headers=admin_headers).json()
    assert first["removed"] is True
    assert second["removed"] is False  # absent, but still a success


def test_the_master_switch_disarms_every_rule_at_once(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    """Section 6.3: an operator needs one call to stop all injection."""
    declare_fault(
        client, admin_headers, rule_id="f-arm", scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 100},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 503

    client.post("/admin/v1/faults/arm", json={"armed": False}, headers=admin_headers)
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 200

    client.post("/admin/v1/faults/arm", json={"armed": True}, headers=admin_headers)
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 503


def test_metrics_are_exposed_on_the_control_plane_only(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    response = client.get("/admin/v1/metrics", headers=admin_headers)
    assert response.status_code == 200
    assert "sim_http_requests_total" in response.text
    assert "sim_last_committed_seq" in response.text
    assert client.get("/admin/v1/metrics", headers=data_headers).status_code == 403


def test_metric_labels_never_contain_entity_ids(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    """Section 11.1: one time series per account would kill the backend."""
    client.get("/v1/accounts/A-000001", headers=data_headers)
    client.get("/v1/deals?account_id=A-000002", headers=data_headers)
    body = client.get("/admin/v1/metrics", headers=admin_headers).text
    assert "A-000001" not in body
    assert "A-000002" not in body
    # The route template is used, not the resolved path.
    assert "/v1/accounts/{account_id}" in body
