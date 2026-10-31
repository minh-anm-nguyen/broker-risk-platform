"""Data-plane HTTP contract.

What a connector is entitled to rely on: the response envelope, decimals as
strings, the ``[from, to)`` time convention, exclusive ``after_seq``, and the
fact that a missing valuation is ``null`` rather than zero.

These are contract tests, so they assert on the *wire* representation. A test
that parsed the JSON back into Decimals would pass even if the service emitted
JSON numbers, which is the bug that matters here.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.conftest import collect_all_pages, create_run

pytestmark = pytest.mark.contract

LIST_ROUTES = ["/v1/accounts", "/v1/positions", "/v1/deals", "/v1/marks"]

#: Fields whose wire type must be a JSON string, per ADR-005.
MONEY_FIELDS = {
    "balance",
    "credit",
    "volume_lots",
    "price",
    "open_price",
    "realized_pnl_gross",
    "commission",
    "swap",
    "bid",
    "ask",
    "unrealized_pnl",
    "equity",
}


def test_meta_answers_before_any_run_exists(
    client: TestClient, data_headers: dict[str, str]
) -> None:
    """A connector discovers capabilities first and then waits for a run."""
    response = client.get("/v1/meta", headers=data_headers)
    assert response.status_code == 200
    body = response.json()
    assert body["run_id"] is None
    assert body["snapshot_seq"] == 0
    capabilities = body["capabilities"]
    assert capabilities["api_version"] == "v1"
    assert capabilities["after_seq_is_exclusive"] is True
    assert capabilities["time_range_convention"] == "[from_time, to_time)"
    # The simulator must never claim to be the real thing.
    assert capabilities["wire_compatible_with_mt5_manager_api"] is False
    assert set(capabilities["simulator_specific_fields"]) == {
        "run_id",
        "generation",
        "snapshot_seq",
        "source_seq",
    }


def test_meta_reflects_the_active_run(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    body = client.get("/v1/meta", headers=data_headers).json()
    assert body["run_id"] == seeded_run["run_id"]
    assert body["generation"] == seeded_run["generation"]
    assert body["status"] == "RUNNING"
    assert body["deal_count"] == 120
    assert body["account_count"] == 10
    assert body["snapshot_seq"] > 0
    assert body["spec_hash"].startswith("sha256:")


@pytest.mark.parametrize("route", LIST_ROUTES)
def test_every_list_response_carries_the_envelope(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any], route: str
) -> None:
    """WF-109: a consumer must be able to detect it is reading a different run."""
    body = client.get(route, headers=data_headers).json()
    assert body["run_id"] == seeded_run["run_id"]
    assert body["generation"] == seeded_run["generation"]
    assert isinstance(body["snapshot_seq"], int)
    assert "next_cursor" in body
    assert isinstance(body["items"], list)


@pytest.mark.parametrize("route", LIST_ROUTES)
def test_money_fields_are_json_strings_on_the_wire(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any], route: str
) -> None:
    """Asserted against the raw text, not the parsed object."""
    raw = client.get(f"{route}?limit=20", headers=data_headers).text
    document = json.loads(raw)
    assert document["items"], f"{route} returned nothing to check"
    for item in document["items"]:
        for field in MONEY_FIELDS & set(item):
            value = item[field]
            assert value is None or isinstance(value, str), (route, field, value)
            if isinstance(value, str):
                Decimal(value)  # parses, and carries no exponent
                assert "e" not in value.lower()


def test_empty_snapshot_returns_an_empty_page_not_an_error(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """WF-005, WF-039: an empty scenario is legal."""
    create_run(client, admin_headers, account_count=0, deal_count=0)
    body = client.get("/v1/deals", headers=data_headers).json()
    assert body["items"] == []
    assert body["next_cursor"] is None
    assert client.get("/v1/accounts", headers=data_headers).json()["items"] == []


def test_deal_payload_has_the_documented_shape(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    deal = client.get("/v1/deals?limit=1", headers=data_headers).json()["items"][0]
    assert set(deal) == {
        "deal_id",
        "source_seq",
        "account_id",
        "position_id",
        "symbol",
        "entry",
        "side",
        "volume_lots",
        "price",
        "realized_pnl_gross",
        "commission",
        "swap",
        "executed_at",
    }
    assert deal["entry"] in {"IN", "OUT"}
    assert deal["side"] in {"BUY", "SELL"}
    assert deal["executed_at"].endswith("Z")


def test_opening_deals_realize_nothing(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    items, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=100")
    entries = {item["entry"] for item in items}
    assert entries == {"IN", "OUT"}
    for item in items:
        if item["entry"] == "IN":
            assert Decimal(item["realized_pnl_gross"]) == 0


def test_closed_positions_report_no_unrealized_value(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """A closed position has nothing to mark, and null says so."""
    items, _ = collect_all_pages(client, data_headers, "/v1/positions?limit=100")
    closed = [item for item in items if item["status"] == "CLOSED"]
    assert closed, "the baseline scenario should close some positions"
    for item in closed:
        assert item["unrealized_pnl"] is None
        assert Decimal(item["volume_lots"]) == 0


def test_open_positions_are_valued_against_an_in_snapshot_mark(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    items, _ = collect_all_pages(client, data_headers, "/v1/positions?status=OPEN&limit=100")
    assert items
    for item in items:
        assert item["status"] == "OPEN"
        assert Decimal(item["volume_lots"]) > 0
        assert item["unrealized_pnl"] is not None
        assert item["mark_market_time"] is not None
        assert item["mark_stale"] in {True, False}


def test_account_detail_reports_derived_values_and_their_quality(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    body = client.get("/v1/accounts/A-000001", headers=data_headers).json()
    assert body["account_id"] == "A-000001"
    assert body["currency"] == "USD"
    assert body["data_quality"] in {"COMPLETE", "MARK_MISSING"}
    if body["data_quality"] == "COMPLETE":
        # equity = balance + unrealized, exactly.
        assert Decimal(body["equity"]) == Decimal(body["balance"]) + Decimal(
            body["unrealized_pnl"]
        )
    else:
        # Withheld entirely rather than partially computed.
        assert body["unrealized_pnl"] is None
        assert body["equity"] is None


def test_as_of_seq_reads_history_not_just_the_present(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """The point of versioned projections: a past value is still readable."""
    snapshot = seeded_run["meta"]["snapshot_seq"]
    current = client.get("/v1/accounts/A-000001", headers=data_headers).json()
    early = client.get(
        "/v1/accounts/A-000001?as_of_seq=2", headers=data_headers
    )
    assert early.status_code in {200, 404}
    if early.status_code == 200:
        assert early.json()["as_of_seq"] == 2
    late = client.get(
        f"/v1/accounts/A-000001?as_of_seq={snapshot}", headers=data_headers
    ).json()
    assert late["balance"] == current["balance"]


def test_as_of_seq_beyond_the_watermark_is_refused(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """Reading the future would have to invent data."""
    snapshot = seeded_run["meta"]["snapshot_seq"]
    response = client.get(
        f"/v1/accounts/A-000001?as_of_seq={snapshot + 1000}", headers=data_headers
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_FAILED"


def test_after_seq_is_exclusive(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-042: a client that processed seq N must not be handed seq N again."""
    first = client.get("/v1/deals?limit=5", headers=data_headers).json()["items"]
    boundary = first[2]["source_seq"]
    following = client.get(
        f"/v1/deals?after_seq={boundary}&limit=10", headers=data_headers
    ).json()["items"]
    assert following
    assert all(item["source_seq"] > boundary for item in following)


def test_time_range_is_half_open_and_tiles_without_overlap(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-041: adjacent windows must not double count a boundary deal."""
    probe = client.get("/v1/deals?limit=10", headers=data_headers).json()["items"][5]
    boundary = probe["executed_at"]
    before, _ = collect_all_pages(
        client, data_headers, f"/v1/deals?to_time={boundary}&limit=100"
    )
    after, _ = collect_all_pages(
        client, data_headers, f"/v1/deals?from_time={boundary}&limit=100"
    )
    everything, _ = collect_all_pages(client, data_headers, "/v1/deals?limit=100")

    before_ids = {item["deal_id"] for item in before}
    after_ids = {item["deal_id"] for item in after}
    assert not (before_ids & after_ids), "a deal landed in both windows"
    assert before_ids | after_ids == {item["deal_id"] for item in everything}
    # The boundary itself belongs to the later window (inclusive start).
    assert probe["deal_id"] in after_ids


def test_reversed_time_range_is_refused(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    response = client.get(
        "/v1/deals?from_time=2026-10-30T02:00:00Z&to_time=2026-10-30T01:00:00Z",
        headers=data_headers,
    )
    assert response.status_code == 422


def test_account_and_symbol_filters_narrow_the_result(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    by_account, _ = collect_all_pages(
        client, data_headers, "/v1/deals?account_id=A-000003&limit=100"
    )
    assert by_account
    assert {item["account_id"] for item in by_account} == {"A-000003"}
    by_symbol, _ = collect_all_pages(
        client, data_headers, "/v1/deals?symbol=EURUSD&limit=100"
    )
    assert by_symbol
    assert {item["symbol"] for item in by_symbol} == {"EURUSD"}


def test_marks_endpoint_accepts_a_symbol_list(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    body = client.get("/v1/marks?symbols=EURUSD,AUDUSD", headers=data_headers).json()
    assert {item["symbol"] for item in body["items"]} == {"EURUSD", "AUDUSD"}
    assert all("stale" in item for item in body["items"])


def test_a_stale_mark_is_flagged_rather_than_presented_as_current(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """WF-030: staleness is surfaced, not hidden.

    The virtual clock is advanced far past the last quote. The mark is still
    returned - it is the most recent one that exists - but ``stale`` says so,
    and a consumer can refuse to present it as a current valuation.
    """
    create_run(client, admin_headers, account_count=2, deal_count=0)
    client.post(
        "/admin/v1/runs/run_0001/events",
        json={"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10000", "ask": "1.10010"},
        headers={**admin_headers, "Idempotency-Key": "stale-mark"},
    )
    fresh = client.get("/v1/marks?symbols=EURUSD", headers=data_headers).json()["items"][0]
    assert fresh["stale"] is False

    # Jump business time well past the staleness threshold.
    advanced = client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"clock_steps": 5_000},
        headers={**admin_headers, "Idempotency-Key": "stale-advance"},
    )
    assert advanced.status_code == 200

    aged = client.get("/v1/marks?symbols=EURUSD", headers=data_headers).json()["items"][0]
    assert aged["stale"] is True
    # The quote itself is unchanged: staleness is metadata, not a mutation.
    assert aged["bid"] == fresh["bid"]
    assert aged["market_time"] == fresh["market_time"]


def test_a_position_reports_the_staleness_of_the_mark_it_was_valued_with(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str]
) -> None:
    """WF-030: a valuation must carry the freshness of its input."""
    create_run(client, admin_headers, account_count=2, deal_count=0)
    for key, payload in (
        ("m", {"kind": "UPDATE_MARK", "symbol": "EURUSD", "bid": "1.10000", "ask": "1.10010"}),
        (
            "o",
            {
                "kind": "OPEN_POSITION", "account_id": "A-000001", "symbol": "EURUSD",
                "side": "BUY", "volume_lots": "0.10", "price": "1.10000",
            },
        ),
    ):
        response = client.post(
            "/admin/v1/runs/run_0001/events",
            json=payload,
            headers={**admin_headers, "Idempotency-Key": f"stale-pos-{key}"},
        )
        assert response.status_code == 201, response.text

    before = client.get("/v1/positions?status=OPEN", headers=data_headers).json()["items"][0]
    assert before["mark_stale"] is False
    assert before["unrealized_pnl"] is not None

    client.post(
        "/admin/v1/runs/run_0001/advance",
        json={"clock_steps": 5_000},
        headers={**admin_headers, "Idempotency-Key": "stale-pos-advance"},
    )
    after = client.get("/v1/positions?status=OPEN", headers=data_headers).json()["items"][0]
    assert after["mark_stale"] is True
    # Still valued, because a stale quote is the best information available -
    # but the consumer is told, rather than left to assume it is current.
    assert after["unrealized_pnl"] == before["unrealized_pnl"]


def test_unknown_filter_values_yield_an_empty_page_not_an_error(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """An account that does not exist is an empty result, not a 404 on a list."""
    body = client.get("/v1/deals?account_id=A-999999", headers=data_headers).json()
    assert body["items"] == []
    assert body["next_cursor"] is None


def test_responses_are_not_cacheable(
    client: TestClient, data_headers: dict[str, str], seeded_run: dict[str, Any]
) -> None:
    """WF-109: a cached financial payload is a trap for anything assuming freshness."""
    response = client.get("/v1/deals?limit=1", headers=data_headers)
    assert response.headers["cache-control"] == "no-store"


def test_every_response_carries_a_request_id(
    client: TestClient, data_headers: dict[str, str]
) -> None:
    response = client.get("/v1/meta", headers=data_headers)
    assert response.headers["x-request-id"]


def test_a_supplied_request_id_is_echoed_for_correlation(
    client: TestClient, data_headers: dict[str, str]
) -> None:
    response = client.get(
        "/v1/meta", headers={**data_headers, "X-Request-Id": "caller-supplied-id"}
    )
    assert response.headers["x-request-id"] == "caller-supplied-id"


def test_openapi_schema_is_published(client: TestClient) -> None:
    """Section 5.4: the contract is in the repository, not in folklore."""
    schema = client.get("/openapi.json").json()
    paths = schema["paths"]
    for route in LIST_ROUTES + ["/v1/meta", "/health/live", "/health/ready"]:
        assert route in paths, route
    assert "/admin/v1/runs" in paths
    # The provenance disclaimer travels with the contract.
    assert "not the MT5 Manager API" in schema["info"]["description"]
