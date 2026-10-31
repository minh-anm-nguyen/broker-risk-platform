"""Metamorphic properties over the running service (section 10.3).

A metamorphic test does not need to know the right answer. It states a
relationship between two runs that must hold whatever the answer is, which is
exactly what is checkable about a system whose output is 100k generated deals.

The relations exercised here:

* replaying the same input stream yields the same content digest;
* page size does not change the set of records returned;
* turning a response fault on and then off leaves stored truth unchanged;
* a rejected operation leaves the sequence and the digest unchanged;
* retrying with one idempotency key does not change the event count;
* total closed volume never exceeds total opened volume;
* a balance replayed from the deal ledger matches the projection, regardless
  of the order pages were read in;
* a reset leaves the archived run's digest untouched.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.main import create_app
from tests.conftest import (
    ADMIN_TOKEN,
    BASELINE_SCENARIO,
    DATA_TOKEN,
    collect_all_pages,
    create_run,
    make_settings,
    wait_for_job,
)

pytestmark = pytest.mark.property

ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
DATA = {"Authorization": f"Bearer {DATA_TOKEN}"}


def digest(client: TestClient) -> str:
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
    ).json()
    assert report["summary"]["failed"] == 0, report["summary"]
    return report["canonical_digest"]


def test_replaying_an_input_stream_reproduces_the_digest(tmp_path: Path) -> None:
    """The same injected events in the same order produce the same data."""
    events = [
        {"kind": "CREATE_ACCOUNT", "account_id": "A-000001", "balance": "10000.00"},
        {"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10000", "ask": "1.10010"},
        {
            "kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
            "side": "BUY", "volume_lots": "0.50", "price": "1.10000",
            "commission": "-2.50", "swap": "0.00",
        },
        {"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10200", "ask": "1.10210"},
        {
            "kind": "CLOSE_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
            "volume_lots": "0.20", "price": "1.10300",
            "commission": "-1.00", "swap": "-0.50",
        },
        {
            "kind": "ADJUST_ACCOUNT", "account_id": "A-000001",
            "amount": "-25.00", "reason": "metamorphic replay",
        },
    ]

    def run(directory: Path) -> str:
        app = create_app(make_settings(directory), lock_process=False)
        with TestClient(app) as client:
            create_run(client, ADMIN, account_count=0, deal_count=0)
            for index, event in enumerate(events):
                response = client.post(
                    "/admin/v1/runs/run_0001/events",
                    json=event,
                    headers={**ADMIN, "Idempotency-Key": f"e{index}"},
                )
                assert response.status_code == 201, response.text
            return digest(client)

    assert run(tmp_path / "first") == run(tmp_path / "second")


@pytest.mark.parametrize("page_size", [1, 7, 25, 100, 500])
def test_page_size_does_not_change_the_result_set(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    page_size: int,
) -> None:
    """Section 10.3: page 100 then page 1 must compose to the same list."""
    create_run(client, admin_headers, account_count=8, deal_count=200)
    reference, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")
    paged, _ = collect_all_pages(client, data_headers, f"/v1/deals?limit={page_size}")

    assert [item["deal_id"] for item in paged] == [item["deal_id"] for item in reference]
    assert paged == reference  # identical payloads, not merely the same ids


def test_turning_a_fault_on_and_off_leaves_truth_unchanged(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """INV-12 as a relation: the digest before equals the digest after."""
    create_run(client, admin_headers, account_count=6, deal_count=120)
    before = digest(client)

    client.post(
        "/admin/v1/faults",
        json={
            "rule_id": "f-meta",
            "scope": {"path": "/v1/deals"},
            "trigger": {"kind": "always", "max_activations": 20, "ttl_seconds": 60},
            "action": {"mode": "drop_one_item", "parameters": {"selection": "last"}},
        },
        headers=admin_headers,
    )
    for _ in range(10):
        client.get("/v1/deals?limit=10", headers=data_headers)
    client.delete("/admin/v1/faults", headers=admin_headers)

    assert digest(client) == before


def test_a_rejected_operation_leaves_the_digest_unchanged(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """Every rejection path, one after another, must be a no-op."""
    create_run(client, admin_headers, account_count=4, deal_count=0)
    client.post(
        "/admin/v1/runs/run_0001/events",
        json={"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10000", "ask": "1.10010"},
        headers={**ADMIN, "Idempotency-Key": "seed-mark"},
    )
    before = digest(client)
    committed = client.get("/admin/v1/runs/current", headers=admin_headers).json()[
        "committed_seq"
    ]

    rejections = [
        {"kind": "OPEN_POSITION", "account_id": "A-999999", "symbol": "EURUSD", "side": "BUY", "volume_lots": "1.00", "price": "1.10000"},
        {"kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "EURUSD", "side": "BUY", "volume_lots": "0", "price": "1.10000"},
        {"kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "EURUSD", "side": "BUY", "volume_lots": "0.001", "price": "1.10000"},
        {"kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "USDJPY", "side": "BUY", "volume_lots": "1.00", "price": "150.00000"},
        {"kind": "CLOSE_POSITION", "account_id": "A-000001", "symbol": "EURUSD", "volume_lots": "1.00", "price": "1.10000"},
        {"kind": "ADJUST_ACCOUNT", "account_id": "A-000001", "amount": "10.00", "reason": ""},
        {"kind": "CREATE_ACCOUNT", "account_id": "A-000001", "balance": "1.00"},
        {"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10200", "ask": "1.10100"},
    ]
    for index, event in enumerate(rejections):
        response = client.post(
            "/admin/v1/runs/run_0001/events",
            json=event,
            headers={**ADMIN, "Idempotency-Key": f"rej-{index}"},
        )
        assert response.status_code >= 400, event

    assert digest(client) == before
    assert (
        client.get("/admin/v1/runs/current", headers=admin_headers).json()["committed_seq"]
        == committed
    )


def test_retrying_with_one_key_never_changes_the_event_count(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """INV-13 as a relation across many retries."""
    create_run(client, admin_headers, account_count=3, deal_count=0)
    event = {"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10000", "ask": "1.10010"}

    first = client.post(
        "/admin/v1/runs/run_0001/events",
        json=event,
        headers={**ADMIN, "Idempotency-Key": "stable"},
    )
    assert first.status_code == 201
    after_first = digest(client)

    for _ in range(12):
        client.post(
            "/admin/v1/runs/run_0001/events",
            json=event,
            headers={**ADMIN, "Idempotency-Key": "stable"},
        )
    assert digest(client) == after_first


def test_closed_volume_never_exceeds_opened_volume(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """Section 10.3, stated over the published API rather than the database."""
    create_run(client, admin_headers, account_count=10, deal_count=400)
    deals, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")

    opened: dict[str, Decimal] = {}
    closed: dict[str, Decimal] = {}
    for deal in deals:
        target = opened if deal["entry"] == "IN" else closed
        position = deal["position_id"]
        target[position] = target.get(position, Decimal(0)) + Decimal(deal["volume_lots"])

    assert opened
    for position, total_closed in closed.items():
        assert total_closed <= opened[position], position

    positions, _ = collect_all_pages(client, data_headers, "/v1/positions?limit=500")
    for position in positions:
        remaining = Decimal(position["volume_lots"])
        expected = opened.get(position["position_id"], Decimal(0)) - closed.get(
            position["position_id"], Decimal(0)
        )
        assert remaining == expected, position["position_id"]


def test_balances_replay_from_the_api_regardless_of_read_order(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """A balance must not depend on how a consumer paginated.

    The same deals are collected once in ascending page order and once by
    per-account scans, and both must reconstruct the published balance.
    """
    create_run(client, admin_headers, account_count=6, deal_count=240)
    accounts, _ = collect_all_pages(client, data_headers, "/v1/accounts?limit=500")
    initial = Decimal(BASELINE_SCENARIO["initial_balance_usd"])

    forward, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")
    by_account: dict[str, list[dict[str, Any]]] = {}
    for account in accounts:
        items, _ = collect_all_pages(
            client, data_headers, f"/v1/deals?account_id={account['account_id']}&limit=37"
        )
        by_account[account["account_id"]] = items

    def replay(deals: list[dict[str, Any]]) -> dict[str, Decimal]:
        balances: dict[str, Decimal] = {}
        for deal in deals:
            delta = (
                Decimal(deal["realized_pnl_gross"])
                + Decimal(deal["commission"])
                + Decimal(deal["swap"])
            )
            account_id = deal["account_id"]
            balances[account_id] = balances.get(account_id, initial) + delta
        return balances

    from_forward = replay(forward)
    from_scans = replay([deal for items in by_account.values() for deal in items])

    for account in accounts:
        account_id = account["account_id"]
        published = Decimal(account["balance"])
        assert from_forward.get(account_id, initial) == published, account_id
        assert from_scans.get(account_id, initial) == published, account_id


def test_resetting_does_not_disturb_the_archived_run(
    client: TestClient, admin_headers: dict[str, str]
) -> None:
    """Section 10.3: a new generation leaves the old digest untouched."""
    create_run(client, admin_headers, account_count=5, deal_count=100)
    archived = client.get(
        "/admin/v1/diagnostics/invariants?run_id=run_0001&scope=full", headers=admin_headers
    ).json()["canonical_digest"]

    reset = client.post(
        "/admin/v1/runs/run_0001/reset",
        json={"expected_generation": 1},
        headers={**ADMIN, "Idempotency-Key": "meta-reset"},
    )
    wait_for_job(client, admin_headers, reset.json()["status_url"])

    after = client.get(
        "/admin/v1/diagnostics/invariants?run_id=run_0001&scope=full", headers=admin_headers
    ).json()["canonical_digest"]
    assert after == archived


def test_advancing_is_additive_and_never_rewrites_history(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """Deals already published must be byte-identical after more are generated."""
    create_run(client, admin_headers, account_count=6, deal_count=150)
    before, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")

    advance = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": 120},
        headers={**ADMIN, "Idempotency-Key": "meta-advance"},
    )
    wait_for_job(client, admin_headers, advance.json()["status_url"])

    after, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=500")
    assert len(after) > len(before)
    assert after[: len(before)] == before


def test_as_of_reads_are_stable_as_the_run_grows(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """A historical read must not change because newer data arrived."""
    create_run(client, admin_headers, account_count=5, deal_count=120)
    pinned = client.get("/v1/meta", headers=data_headers).json()["snapshot_seq"]
    snapshots = {
        account_id: client.get(
            f"/v1/accounts/{account_id}?as_of_seq={pinned}", headers=data_headers
        ).json()
        for account_id in ("A-000001", "A-000002", "A-000003")
    }

    advance = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"events": 200},
        headers={**ADMIN, "Idempotency-Key": "meta-asof"},
    )
    wait_for_job(client, admin_headers, advance.json()["status_url"])

    for account_id, original in snapshots.items():
        again = client.get(
            f"/v1/accounts/{account_id}?as_of_seq={pinned}", headers=data_headers
        ).json()
        assert again["balance"] == original["balance"], account_id
        assert again["as_of_seq"] == pinned
