"""Idempotency: WF-003, WF-004, WF-051, WF-053, WF-054, INV-13.

The scenario this guards against is mundane and common: the write committed,
the response was lost, the client retried. Without an idempotency record in
the *same transaction* as the effect, that retry books a second deal.

The decisive assertion in each test is not the response code - it is that the
committed sequence and the deal count did not move on the retry.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.conftest import BASELINE_SCENARIO, create_run, wait_for_job

pytestmark = pytest.mark.integration


def committed_seq(client: TestClient, admin_headers: dict[str, str]) -> int:
    return client.get("/admin/v1/runs/current", headers=admin_headers).json()["committed_seq"]


def deal_count(client: TestClient, admin_headers: dict[str, str]) -> int:
    return client.get("/admin/v1/runs/current", headers=admin_headers).json()["deal_count"]


def inject(client: TestClient, admin_headers: dict[str, str], key: str, **payload: Any):
    return client.post(
        "/admin/v1/runs/run_0001/events",
        json=payload,
        headers={**admin_headers, "Idempotency-Key": key},
    )


OPEN_EVENT = {
    "kind": "OPEN_POSITION",
    "account_id": "A-000001",
    "symbol": "EURUSD",
    "side": "BUY",
    "volume_lots": "0.50",
    "price": "1.10000",
    "commission": "-2.50",
    "swap": "0.00",
}


@pytest.fixture
def blank_run(client: TestClient, admin_headers: dict[str, str]) -> dict[str, Any]:
    """Accounts and quotes but no trading, so every netting key is free.

    Injecting into a generated run would hit ``POSITION_ALREADY_OPEN`` for
    whichever (account, symbol) the generator happened to leave open - a
    correct rejection, but one that has nothing to do with idempotency.
    """
    accepted = create_run(client, admin_headers, account_count=6, deal_count=0)
    return accepted


def test_replaying_a_run_creation_returns_the_original_job(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-003: one run, one job, however many times the request arrives."""
    first = client.post(
        "/admin/v1/runs",
        json=BASELINE_SCENARIO,
        headers={**admin_headers, "Idempotency-Key": "run-key"},
    ).json()
    wait_for_job(client, admin_headers, first["status_url"])

    for _ in range(3):
        replay = client.post(
            "/admin/v1/runs",
            json=BASELINE_SCENARIO,
            headers={**admin_headers, "Idempotency-Key": "run-key"},
        )
        assert replay.status_code == 202
        body = replay.json()
        assert body["job_id"] == first["job_id"]
        assert body["run_id"] == first["run_id"]
        assert body["replayed"] is True

    # And exactly one run exists.
    assert client.get("/admin/v1/runs/run_0002", headers=admin_headers).status_code == 404


def test_the_same_key_with_a_different_body_is_a_conflict(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """WF-004: returning the old result would be a lie; applying the new one
    would destroy the key's meaning."""
    client.post(
        "/admin/v1/runs",
        json=BASELINE_SCENARIO,
        headers={**admin_headers, "Idempotency-Key": "conflict-key"},
    )
    response = client.post(
        "/admin/v1/runs",
        json={**BASELINE_SCENARIO, "seed": 999},
        headers={**admin_headers, "Idempotency-Key": "conflict-key"},
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


def test_replaying_an_injected_event_adds_nothing(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    """WF-053 and INV-13: the lost-response case, which is the whole point."""
    before_seq = committed_seq(client, admin_headers)
    before_deals = deal_count(client, admin_headers)

    first = inject(client, admin_headers, "evt-1", **{**OPEN_EVENT, "account_id": "A-000002"})
    assert first.status_code == 201
    original = first.json()
    assert original["replayed"] is False
    assert committed_seq(client, admin_headers) == before_seq + 1
    assert deal_count(client, admin_headers) == before_deals + 1

    for _ in range(5):
        replay = inject(
            client, admin_headers, "evt-1", **{**OPEN_EVENT, "account_id": "A-000002"}
        )
        assert replay.status_code == 201
        body = replay.json()
        assert body["seq"] == original["seq"]
        assert body["event_id"] == original["event_id"]
        assert body["deal_id"] == original["deal_id"]
        assert body["replayed"] is True

    # The decisive check: nothing moved.
    assert committed_seq(client, admin_headers) == before_seq + 1
    assert deal_count(client, admin_headers) == before_deals + 1


def test_a_replay_is_served_even_after_the_state_has_moved_on(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    """A retry must not be re-validated against newer state.

    Re-applying the original intent now would fail (the position is already
    open), so a service that re-validated instead of replaying would return an
    error for a write that actually succeeded.
    """
    first = inject(client, admin_headers, "evt-2", **{**OPEN_EVENT, "account_id": "A-000003"})
    assert first.status_code == 201

    inject(
        client, admin_headers, "evt-3",
        kind="UPDATE_MARK", symbol="EURUSD", bid="1.20000", ask="1.20010",
    )
    inject(
        client, admin_headers, "evt-4",
        kind="ADJUST_ACCOUNT", account_id="A-000003", amount="-5.00", reason="test",
    )

    replay = inject(client, admin_headers, "evt-2", **{**OPEN_EVENT, "account_id": "A-000003"})
    assert replay.status_code == 201
    assert replay.json()["replayed"] is True
    assert replay.json()["seq"] == first.json()["seq"]


def test_a_rejected_event_leaves_its_key_free(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    """WF-054: a write that never committed must not burn the key.

    Otherwise a client that fixed its request and retried with the same key
    would be told it had already succeeded.
    """
    rejected = inject(
        client, admin_headers, "retry-key",
        kind="OPEN_POSITION", account_id="A-999999", symbol="EURUSD",
        side="BUY", volume_lots="0.50", price="1.10000",
    )
    assert rejected.status_code == 404

    corrected = inject(
        client, admin_headers, "retry-key", **{**OPEN_EVENT, "account_id": "A-000004"}
    )
    assert corrected.status_code == 201
    assert corrected.json()["replayed"] is False


def test_concurrent_requests_with_one_key_commit_one_effect(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    """WF-051: exactly one committed effect, the rest replayed or conflicted."""
    before_seq = committed_seq(client, admin_headers)
    payload = {**OPEN_EVENT, "account_id": "A-000005"}

    def attempt(_: int):
        return inject(client, admin_headers, "concurrent-key", **payload)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(attempt, range(8)))

    created = [item for item in responses if item.status_code == 201]
    assert len(created) == 8, [item.status_code for item in responses]
    sequences = {item.json()["seq"] for item in created}
    assert len(sequences) == 1, "concurrent retries resolved to different events"
    assert sum(1 for item in created if item.json()["replayed"] is False) == 1

    assert committed_seq(client, admin_headers) == before_seq + 1


def test_one_key_reused_for_a_different_operation_is_a_conflict(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    """Keys share one namespace per run, and the route is in the digest.

    So reusing a key across two different operations is a 409 rather than
    being mistaken for a replay. That is the safer of the two possible
    designs: the alternative - scoping keys per route - would let a buggy
    client reuse one key everywhere and never find out.
    """
    event = inject(
        client, admin_headers, "shared-key",
        kind="UPDATE_MARK", symbol="EURUSD", bid="1.10000", ask="1.10010",
    )
    assert event.status_code == 201

    advance = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"clock_steps": 1},
        headers={**admin_headers, "Idempotency-Key": "shared-key"},
    )
    assert advance.status_code == 409
    assert advance.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"

    # A distinct key works, and the original effect is untouched.
    accepted = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"clock_steps": 1},
        headers={**admin_headers, "Idempotency-Key": "its-own-key"},
    )
    assert accepted.status_code == 200
    assert inject(
        client, admin_headers, "shared-key",
        kind="UPDATE_MARK", symbol="EURUSD", bid="1.10000", ask="1.10010",
    ).json()["replayed"] is True


def test_an_advance_replay_does_not_advance_twice(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    headers = {**admin_headers, "Idempotency-Key": "advance-once"}
    first = client.post(
        "/admin/v1/runs/run_0001/advance", json={"events": 20}, headers=headers
    ).json()
    wait_for_job(client, admin_headers, first["status_url"])
    after_first = committed_seq(client, admin_headers)

    replay = client.post(
        "/admin/v1/runs/run_0001/advance", json={"events": 20}, headers=headers
    ).json()
    assert replay["replayed"] is True
    assert replay["job_id"] == first["job_id"]
    assert committed_seq(client, admin_headers) == after_first


@pytest.mark.parametrize("key", ["", "   ", "k" * 200])
def test_unusable_idempotency_keys_are_refused(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any], key: str
) -> None:
    response = client.post(
        "/admin/v1/runs/run_0001/events",
        json={"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.1", "ask": "1.2"},
        headers={**admin_headers, "Idempotency-Key": key},
    )
    assert response.status_code == 422


def test_inv13_holds_after_a_burst_of_replays(
    client: TestClient, admin_headers: dict[str, str], blank_run: dict[str, Any]
) -> None:
    """One key, one committed effect - verified by the storage sweep."""
    for index in range(10):
        for _ in range(3):
            inject(
                client, admin_headers, f"burst-{index}",
                kind="UPDATE_MARK", symbol="EURUSD",
                bid=f"1.1{index:04d}", ask=f"1.1{index + 1:04d}",
            )
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    results = {item["invariant_id"]: item for item in report["results"]}
    assert results["INV-13"]["status"] == "PASS", results["INV-13"]
    assert results["INV-01"]["status"] == "PASS"
    assert report["summary"]["failed"] == 0
