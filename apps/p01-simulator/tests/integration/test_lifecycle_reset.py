"""Run lifecycle and generation isolation: WF-008, WF-038, WF-049, WF-050,
WF-059, WF-085, WF-105, INV-14, INV-15.

A reset is the operation most likely to go subtly wrong, because it has to
move three things at once - the new run row, the old run's status and the
active pointer - and a reader must never observe a generation that is half
swapped.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.conftest import BASELINE_SCENARIO, create_run, wait_for_job

pytestmark = pytest.mark.integration


def reset(
    client: TestClient,
    admin_headers: dict[str, str],
    *,
    key: str,
    generation: int | None = 1,
    spec: dict[str, Any] | None = None,
    run_id: str = "run_0001",
):
    body: dict[str, Any] = {}
    if generation is not None:
        body["expected_generation"] = generation
    if spec is not None:
        body["spec"] = spec
    return client.post(
        f"/admin/v1/runs/{run_id}/reset",
        json=body,
        headers={**admin_headers, "Idempotency-Key": key},
    )


def test_reset_starts_a_new_generation_and_retires_the_old_one(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    response = reset(client, admin_headers, key="r1")
    assert response.status_code == 202
    body = response.json()
    assert body["run_id"] == "run_0002"
    assert body["generation"] == 2
    assert body["previous_run_id"] == "run_0001"
    assert body["previous_generation"] == 1
    wait_for_job(client, admin_headers, body["status_url"])

    old = client.get("/admin/v1/runs/run_0001", headers=admin_headers).json()
    assert old["status"] == "REPLACED"
    assert old["replaced_at"] is not None

    current = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert current["run_id"] == "run_0002"
    assert current["status"] == "RUNNING"
    assert client.get("/v1/meta", headers=data_headers).json()["run_id"] == "run_0002"


def test_the_generation_guard_blocks_a_stale_operator(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-105: resetting a run you are no longer looking at."""
    response = reset(client, admin_headers, key="stale", generation=99)
    assert response.status_code == 409
    error = response.json()["error"]
    assert error["code"] == "RUN_CONFLICT"
    assert error["details"]["expected_generation"] == 99
    assert error["details"]["active_generation"] == 1
    # Nothing moved.
    assert client.get("/admin/v1/runs/current", headers=admin_headers).json()["run_id"] == "run_0001"


def test_reset_without_a_guard_is_allowed_but_explicit(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """Omitting the guard is a choice, not an accident of the schema."""
    response = reset(client, admin_headers, key="noguard", generation=None)
    assert response.status_code == 202


def test_a_reset_reuses_the_previous_spec_when_none_is_given(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    response = reset(client, admin_headers, key="same-spec")
    wait_for_job(client, admin_headers, response.json()["status_url"])
    current = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert current["spec"]["seed"] == BASELINE_SCENARIO["seed"]
    assert current["spec"]["deal_count"] == BASELINE_SCENARIO["deal_count"]
    assert current["spec_hash"] == seeded_run["meta"]["spec_hash"]


def test_a_reset_may_supply_a_new_spec(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    response = reset(
        client, admin_headers, key="new-spec",
        spec={**BASELINE_SCENARIO, "seed": 7, "deal_count": 30},
    )
    wait_for_job(client, admin_headers, response.json()["status_url"])
    current = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert current["seed"] == 7
    assert current["deal_count"] == 30
    assert current["spec_hash"] != seeded_run["meta"]["spec_hash"]


def test_old_cursors_are_rejected_after_a_reset(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    """WF-038: 410 RUN_REPLACED, so the consumer starts a new sync."""
    page = client.get("/v1/deals?limit=5", headers=data_headers).json()
    cursor = page["next_cursor"]
    assert cursor

    wait_for_job(client, admin_headers, reset(client, admin_headers, key="r2").json()["status_url"])

    response = client.get(f"/v1/deals?cursor={cursor}", headers=data_headers)
    assert response.status_code == 410
    assert response.json()["error"]["code"] == "RUN_REPLACED"
    details = response.json()["error"]["details"]
    assert details["cursor_run_id"] == "run_0001"
    assert details["active_run_id"] == "run_0002"
    # A new scan against the new generation works immediately.
    fresh = client.get("/v1/deals?limit=5", headers=data_headers).json()
    assert fresh["run_id"] == "run_0002"


def test_the_replaced_run_keeps_its_own_data_intact(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-085, INV-15: a reset retires a run, it does not delete one."""
    before = client.get(
        "/admin/v1/diagnostics/invariants?run_id=run_0001&scope=full", headers=admin_headers
    ).json()

    wait_for_job(client, admin_headers, reset(client, admin_headers, key="r3").json()["status_url"])

    after = client.get(
        "/admin/v1/diagnostics/invariants?run_id=run_0001&scope=full", headers=admin_headers
    ).json()
    assert after["canonical_digest"] == before["canonical_digest"]
    assert after["committed_seq"] == before["committed_seq"]
    assert after["summary"]["failed"] == 0


def test_generations_do_not_share_identifiers_or_data(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    """INV-15: the same ids exist in both runs and must never be mixed."""
    first_deals = {
        item["deal_id"] for item in client.get("/v1/deals?limit=500", headers=data_headers).json()["items"]
    }
    wait_for_job(client, admin_headers, reset(client, admin_headers, key="r4").json()["status_url"])
    second = client.get("/v1/deals?limit=500", headers=data_headers).json()

    assert second["run_id"] == "run_0002"
    # Identifiers restart per run, which is exactly why every response carries
    # run_id and generation.
    assert {item["deal_id"] for item in second["items"]} & first_deals
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    results = {item["invariant_id"]: item["status"] for item in report["results"]}
    assert results["INV-15"] == "PASS"
    assert results["INV-03"] == "PASS"  # no cross-run references


def test_concurrent_resets_produce_exactly_one_new_generation(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-008, WF-049: two operators racing must not both win."""

    def attempt(index: int):
        return reset(client, admin_headers, key=f"race-{index}")

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(attempt, range(6)))

    accepted = [item for item in responses if item.status_code == 202]
    rejected = [item for item in responses if item.status_code != 202]
    assert len(accepted) == 1, [item.status_code for item in responses]
    # The losers get a clear conflict or a job-limit refusal, never a success.
    assert all(item.status_code in {409, 429} for item in rejected)

    wait_for_job(client, admin_headers, accepted[0].json()["status_url"], timeout=120)
    current = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert current["generation"] == 2
    assert client.get("/admin/v1/runs/run_0003", headers=admin_headers).status_code == 404


def test_a_concurrent_create_and_reset_do_not_both_apply(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-049: start versus reset, serialized by the coordinator lock."""

    def create():
        return client.post(
            "/admin/v1/runs",
            json=BASELINE_SCENARIO,
            headers={**admin_headers, "Idempotency-Key": "race-create"},
        )

    def do_reset():
        return reset(client, admin_headers, key="race-reset")

    with ThreadPoolExecutor(max_workers=2) as pool:
        created = pool.submit(create)
        resetted = pool.submit(do_reset)
        outcomes = [created.result(), resetted.result()]

    successes = [item for item in outcomes if item.status_code == 202]
    assert len(successes) == 1, [item.status_code for item in outcomes]
    current = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    assert current["generation"] in {1, 2}
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0


def test_stopping_while_generation_is_in_flight_reports_an_accurate_count(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-050: drain or cancel, but report what actually committed."""
    accepted = create_run(client, admin_headers, deal_count=2_000, wait=False)
    stop = client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    assert stop.status_code == 200

    job = wait_for_job(client, admin_headers, accepted["status_url"], timeout=120)
    assert job["status"] in {"CANCELLED", "SUCCEEDED"}

    run = client.get("/admin/v1/runs/run_0001", headers=admin_headers).json()
    assert run["status"] in {"STOPPED", "STOPPING"}

    # The reported progress matches what the database actually holds, which is
    # the part that would be easy to get wrong.
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0
    assert report["record_counts"]["deals"] == run["deal_count"]
    assert report["committed_seq"] == run["committed_seq"]


def test_a_stopped_run_can_still_be_read(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    seeded_run: dict[str, Any],
) -> None:
    """Stopping ends generation, not availability."""
    client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    page = client.get("/v1/deals?limit=10", headers=data_headers)
    assert page.status_code == 200
    assert len(page.json()["items"]) == 10
    assert client.get("/v1/meta", headers=data_headers).json()["status"] in {"STOPPED", "STOPPING"}


def test_a_stopped_run_can_be_replaced(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    response = reset(client, admin_headers, key="after-stop")
    assert response.status_code == 202
    wait_for_job(client, admin_headers, response.json()["status_url"])
    assert client.get("/admin/v1/runs/current", headers=admin_headers).json()["generation"] == 2


def test_creating_a_run_after_stopping_is_allowed(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """A terminal run no longer blocks a fresh create."""
    client.post("/admin/v1/runs/run_0001/stop", headers=admin_headers)
    response = client.post(
        "/admin/v1/runs",
        json=BASELINE_SCENARIO,
        headers={**admin_headers, "Idempotency-Key": "fresh-after-stop"},
    )
    assert response.status_code == 202
    assert response.json()["generation"] == 2


def test_a_cancelled_job_leaves_consistent_data(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-011, WF-012: events 1..N-1 remain sound, with no half event N."""
    accepted = create_run(client, admin_headers, deal_count=3_000, wait=False)
    cancelled = client.post(
        f"/admin/v1/jobs/{accepted['job_id']}/cancel", headers=admin_headers
    )
    assert cancelled.status_code == 200
    job = wait_for_job(client, admin_headers, accepted["status_url"], timeout=120)
    assert job["status"] in {"CANCELLED", "SUCCEEDED"}

    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0
    assert report["summary"]["storage_scope_ok"] is True
    # INV-01: the sequence is still contiguous with no gap where the cancel hit.
    results = {item["invariant_id"]: item for item in report["results"]}
    assert results["INV-01"]["status"] == "PASS"
    observed = results["INV-01"]["observed"]
    assert observed["event_count"] == observed["committed_seq"]


def test_virtual_clock_advance_races_do_not_corrupt_the_clock(
    client: TestClient, admin_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-059: clock advance and reset are serialized by the same lock."""

    def advance(index: int):
        return client.post(
            "/admin/v1/runs/run_0001/advance",
            json={"clock_steps": 10},
            headers={**admin_headers, "Idempotency-Key": f"clock-{index}"},
        )

    with ThreadPoolExecutor(max_workers=5) as pool:
        responses = list(pool.map(advance, range(5)))
    assert all(item.status_code == 200 for item in responses)

    run = client.get("/admin/v1/runs/current", headers=admin_headers).json()
    # Virtual time only ever moves forward.
    assert run["virtual_time"] > seeded_run["meta"]["virtual_time"]
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0
