"""Deliberate faults: INV-12 and WF-060..WF-075.

Two things are being tested, and they are easy to conflate:

1. **The fault fires exactly as configured.** A chaos result is worthless if
   the fault never actually happened, so every test asserts the activation
   count as well as the observable effect (WF-112, WF-119).
2. **The canonical store is untouched.** Response-layer faults mutate a copy
   of the outgoing payload. If one could change stored truth, the simulator
   would stop being usable as an oracle for the system under test (INV-12).

What is *not* tested here: whether a downstream connector survives these
faults. That is Project 02's question, and claiming it here would be exactly
the conflation section 8 warns against.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.conftest import create_run

pytestmark = pytest.mark.integration


def declare(client: TestClient, admin_headers: dict[str, str], **payload: Any) -> dict[str, Any]:
    response = client.post("/admin/v1/faults", json=payload, headers=admin_headers)
    assert response.status_code == 201, response.text
    return response.json()


def audit(client: TestClient, admin_headers: dict[str, str], rule_id: str) -> dict[str, Any]:
    listing = client.get("/admin/v1/faults", headers=admin_headers).json()
    for item in listing["items"]:
        if item["rule_id"] == rule_id:
            return item
    raise AssertionError(f"rule {rule_id} is no longer listed")


def truth_digest(client: TestClient, admin_headers: dict[str, str]) -> str:
    report = client.get(
        "/admin/v1/diagnostics/invariants?scope=full", headers=admin_headers
    ).json()
    assert report["summary"]["failed"] == 0, report["summary"]
    return report["canonical_digest"]


@pytest.fixture
def faulted_run(client: TestClient, admin_headers: dict[str, str]) -> str:
    create_run(client, admin_headers, account_count=8, deal_count=150)
    return truth_digest(client, admin_headers)


# ---------------------------------------------------------------------------
# Transport-shaped faults
# ---------------------------------------------------------------------------


def test_an_nth_request_fault_hits_exactly_that_request(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-060: request #3 fails and 1, 2, 4, 5 do not."""
    declare(
        client, admin_headers, rule_id="f-500",
        scope={"method": "GET", "path": "/v1/deals"},
        trigger={"kind": "nth_matching_request", "n": 3, "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 500}},
    )
    statuses = [client.get("/v1/deals?limit=1", headers=data_headers).status_code for _ in range(5)]
    assert statuses == [200, 200, 500, 200, 200]

    rule = audit(client, admin_headers, "f-500")
    assert rule["activations"] == 1
    # Matches are counted while the rule is live. It exhausted on request 3,
    # so requests 4 and 5 were skipped before matching rather than counted
    # against a rule that could never fire again.
    assert rule["match_count"] == 3
    assert rule["exhausted"] is True
    assert truth_digest(client, admin_headers) == faulted_run


def test_a_scoped_fault_leaves_other_routes_alone(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    declare(
        client, admin_headers, rule_id="f-deals-only",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 10, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 503
    assert client.get("/v1/positions?limit=1", headers=data_headers).status_code == 200
    assert client.get("/v1/accounts?limit=1", headers=data_headers).status_code == 200
    assert client.get("/v1/meta", headers=data_headers).status_code == 200


def test_a_fault_can_be_scoped_to_one_account(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    declare(
        client, admin_headers, rule_id="f-one-account",
        scope={"path": "/v1/deals", "account_id": "A-000002"},
        trigger={"kind": "always", "max_activations": 10, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    assert client.get("/v1/deals?account_id=A-000002", headers=data_headers).status_code == 503
    assert client.get("/v1/deals?account_id=A-000003", headers=data_headers).status_code == 200


def test_a_retry_after_header_is_emitted_for_backoff(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-061."""
    declare(
        client, admin_headers, rule_id="f-503",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503, "retry_after_seconds": 5}},
    )
    response = client.get("/v1/deals?limit=1", headers=data_headers)
    assert response.status_code == 503
    assert response.headers["retry-after"] == "5"
    assert response.headers["x-fault-injected"] == "1"
    assert response.json()["error"]["details"]["injected"] is True


def test_an_injected_delay_is_bounded_and_does_not_block_liveness(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-062, WF-093: the delay is awaited, not slept, so the loop stays free."""
    import time

    declare(
        client, admin_headers, rule_id="f-delay",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "delay_before_response", "parameters": {"delay_ms": 400}},
    )
    started = time.perf_counter()
    response = client.get("/v1/deals?limit=1", headers=data_headers)
    elapsed = time.perf_counter() - started
    assert response.status_code == 200
    assert 0.35 < elapsed < 5.0, elapsed
    assert audit(client, admin_headers, "f-delay")["activations"] == 1
    # An unfaulted request afterwards is fast again.
    started = time.perf_counter()
    client.get("/v1/deals?limit=1", headers=data_headers)
    assert time.perf_counter() - started < 0.35


# ---------------------------------------------------------------------------
# Payload faults - the canonical store must be untouched
# ---------------------------------------------------------------------------


def test_faults_do_not_change_truth(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """INV-12: every body-mutating mode, then the digest must be unchanged."""
    modes = [
        ("drop_one_item", {"selection": "last"}),
        ("duplicate_one_item", {"selection": "first"}),
        ("shuffle_items", {}),
        ("omit_field", {"field": "deal_id", "selection": "first"}),
        ("replace_field_value", {"field": "volume_lots", "value": "NaN", "selection": "first"}),
        ("malformed_json", {}),
        ("wrong_content_type", {}),
        ("truncate_response", {"keep_bytes": 40}),
        ("stale_snapshot", {"seq_delta": 5}),
    ]
    for index, (mode, parameters) in enumerate(modes):
        rule_id = f"f-{mode}"
        declare(
            client, admin_headers, rule_id=rule_id,
            scope={"path": "/v1/deals"},
            trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
            action={"mode": mode, "parameters": parameters},
        )
        response = client.get("/v1/deals?limit=5", headers=data_headers)
        assert response.headers.get("x-fault-injected") == "1", mode
        assert audit(client, admin_headers, rule_id)["activations"] == 1, mode
        client.delete(f"/admin/v1/faults/{rule_id}", headers=admin_headers)

    assert truth_digest(client, admin_headers) == faulted_run

    # With faults gone, the payload is well formed again.
    clean = client.get("/v1/deals?limit=5", headers=data_headers)
    assert clean.status_code == 200
    assert "x-fault-injected" not in clean.headers
    assert len(clean.json()["items"]) == 5


def test_dropping_an_item_is_invisible_in_the_status_code(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-064: a 200 with a missing deal is exactly the nasty case."""
    baseline = client.get("/v1/deals?limit=10", headers=data_headers).json()
    declare(
        client, admin_headers, rule_id="f-drop",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "drop_one_item", "parameters": {"selection": "last"}},
    )
    faulted = client.get("/v1/deals?limit=10", headers=data_headers)
    assert faulted.status_code == 200
    assert len(faulted.json()["items"]) == 9
    assert faulted.json()["items"] == baseline["items"][:9]
    assert truth_digest(client, admin_headers) == faulted_run


def test_duplicating_an_item_produces_a_repeated_deal_id(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-063: downstream deduplication has something real to catch."""
    declare(
        client, admin_headers, rule_id="f-dup",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "duplicate_one_item", "parameters": {"selection": "first"}},
    )
    items = client.get("/v1/deals?limit=5", headers=data_headers).json()["items"]
    identifiers = [item["deal_id"] for item in items]
    assert len(identifiers) == 6
    assert len(set(identifiers)) == 5
    assert truth_digest(client, admin_headers) == faulted_run


def test_shuffling_changes_order_but_not_membership(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-065."""
    baseline = client.get("/v1/deals?limit=8", headers=data_headers).json()["items"]
    declare(
        client, admin_headers, rule_id="f-shuffle",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "shuffle_items", "parameters": {}},
    )
    shuffled = client.get("/v1/deals?limit=8", headers=data_headers).json()["items"]
    assert shuffled != baseline
    assert {item["deal_id"] for item in shuffled} == {item["deal_id"] for item in baseline}


def test_a_missing_required_field_reaches_the_client(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-066."""
    declare(
        client, admin_headers, rule_id="f-omit",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "omit_field", "parameters": {"field": "deal_id", "selection": "first"}},
    )
    items = client.get("/v1/deals?limit=3", headers=data_headers).json()["items"]
    assert "deal_id" not in items[0]
    assert "deal_id" in items[1]


def test_a_poisoned_decimal_string_reaches_the_client(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-067: the value a downstream parser must refuse."""
    declare(
        client, admin_headers, rule_id="f-nan",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={
            "mode": "replace_field_value",
            "parameters": {"field": "price", "value": "1e9999", "selection": "first"},
        },
    )
    items = client.get("/v1/deals?limit=3", headers=data_headers).json()["items"]
    assert items[0]["price"] == "1e9999"
    # Our own validation would refuse it, which is what downstream must do too.
    from simulator.domain import accounting as acc
    from simulator.domain.models import ValidationError

    with pytest.raises(ValidationError):
        acc.validate_money(acc.parse_decimal(items[0]["price"], field="price"), field="price")
    assert truth_digest(client, admin_headers) == faulted_run


def test_malformed_json_is_not_parseable(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-068: deliberately outside the error contract (section 5.3)."""
    declare(
        client, admin_headers, rule_id="f-malformed",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "malformed_json", "parameters": {}},
    )
    response = client.get("/v1/deals?limit=3", headers=data_headers)
    assert response.status_code == 200
    with pytest.raises(json.JSONDecodeError):
        response.json()


def test_a_wrong_content_type_is_emitted(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-073."""
    declare(
        client, admin_headers, rule_id="f-ct",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "wrong_content_type", "parameters": {}},
    )
    response = client.get("/v1/deals?limit=3", headers=data_headers)
    assert response.headers["content-type"].startswith("text/plain")


def test_a_truncated_response_is_incomplete(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-069: observable in-process; wire-level framing needs the proxy."""
    declare(
        client, admin_headers, rule_id="f-trunc",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "truncate_response", "parameters": {"keep_bytes": 30}},
    )
    response = client.get("/v1/deals?limit=5", headers=data_headers)
    assert len(response.content) == 30
    with pytest.raises(json.JSONDecodeError):
        response.json()


def test_a_stale_snapshot_is_detectable_from_the_envelope(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-072: the client can see the sequence went backwards."""
    honest = client.get("/v1/deals?limit=3", headers=data_headers).json()["snapshot_seq"]
    declare(
        client, admin_headers, rule_id="f-stale",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "stale_snapshot", "parameters": {"seq_delta": 25}},
    )
    stale = client.get("/v1/deals?limit=3", headers=data_headers).json()
    assert stale["snapshot_seq"] == honest - 25
    assert stale["run_id"]  # run identity is still present and honest


# ---------------------------------------------------------------------------
# Fault engine safety
# ---------------------------------------------------------------------------


def test_health_is_never_faulted_even_with_a_matching_rule(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """Section 6.3: liveness must stay answerable during a chaos run."""
    # Declaring a health-scoped rule is refused at configuration time...
    refused = client.post(
        "/admin/v1/faults",
        json={
            "scope": {"path": "/health/live"},
            "trigger": {"kind": "always"},
            "action": {"mode": "http_error", "parameters": {"status": 503}},
        },
        headers=admin_headers,
    )
    assert refused.status_code == 422

    # ...and a broad rule still cannot reach health or the control plane.
    declare(
        client, admin_headers, rule_id="f-broad",
        scope={"path": "/"},
        trigger={"kind": "always", "max_activations": 50, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    assert client.get("/health/live").status_code == 200
    assert client.get("/health/ready").status_code == 200
    assert client.get("/admin/v1/faults", headers=admin_headers).status_code == 200
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 503


def test_only_one_body_mutation_applies_per_request(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """Section 6.3: composing two would produce a payload no test described.

    The lower-priority rule still records the match, so nth-request counting
    stays meaningful.
    """
    declare(
        client, admin_headers, rule_id="f-first", priority=10,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "drop_one_item", "parameters": {"selection": "last"}},
    )
    declare(
        client, admin_headers, rule_id="f-second", priority=20,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "duplicate_one_item", "parameters": {"selection": "first"}},
    )
    items = client.get("/v1/deals?limit=10", headers=data_headers).json()["items"]
    assert len(items) == 9  # only the drop applied
    assert audit(client, admin_headers, "f-first")["activations"] == 1
    second = audit(client, admin_headers, "f-second")
    assert second["activations"] == 0
    assert second["match_count"] == 1


def test_priority_decides_which_rule_fires_not_insertion_order(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    declare(
        client, admin_headers, rule_id="f-late-but-urgent", priority=1,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 504}},
    )
    declare(
        client, admin_headers, rule_id="f-early-but-not", priority=500,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 500}},
    )
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 504


def test_a_delay_and_a_terminal_fault_compose(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """One delay plus one terminal action is the single allowed combination."""
    import time

    declare(
        client, admin_headers, rule_id="f-slow", priority=10,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "delay_before_response", "parameters": {"delay_ms": 250}},
    )
    declare(
        client, admin_headers, rule_id="f-then-fail", priority=20,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    started = time.perf_counter()
    response = client.get("/v1/deals?limit=1", headers=data_headers)
    elapsed = time.perf_counter() - started
    assert response.status_code == 503
    assert elapsed > 0.2
    assert audit(client, admin_headers, "f-slow")["activations"] == 1
    assert audit(client, admin_headers, "f-then-fail")["activations"] == 1


def test_an_exhausted_rule_stops_firing(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-106: max_activations is a hard stop, not a suggestion."""
    declare(
        client, admin_headers, rule_id="f-twice",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 2, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    statuses = [client.get("/v1/deals?limit=1", headers=data_headers).status_code for _ in range(6)]
    assert statuses == [503, 503, 200, 200, 200, 200]


def test_a_probabilistic_fault_is_reproducible_and_bounded(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """Section 6.3: 'random' activation must still be replayable."""
    declare(
        client, admin_headers, rule_id="f-prob",
        scope={"path": "/v1/deals"},
        trigger={
            "kind": "probability", "probability": "0.5",
            "max_activations": 1000, "ttl_seconds": 60,
        },
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    statuses = [client.get("/v1/deals?limit=1", headers=data_headers).status_code for _ in range(60)]
    failures = statuses.count(503)
    assert 15 < failures < 45, statuses
    rule = audit(client, admin_headers, "f-prob")
    assert rule["match_count"] == 60
    assert rule["activations"] == failures


def test_clearing_faults_restores_clean_behaviour(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    for index in range(3):
        declare(
            client, admin_headers, rule_id=f"f-bulk-{index}",
            scope={"path": "/v1/deals"},
            trigger={"kind": "always", "max_activations": 100, "ttl_seconds": 60},
            action={"mode": "http_error", "parameters": {"status": 503}},
        )
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 503
    removed = client.delete("/admin/v1/faults", headers=admin_headers).json()["removed"]
    assert removed == 3
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 200
    assert truth_digest(client, admin_headers) == faulted_run


def test_faults_are_released_when_the_run_is_replaced(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """A rule configured for a replaced generation must not fire against the new one."""
    declare(
        client, admin_headers, rule_id="f-old-gen",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 100, "ttl_seconds": 600},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 503

    client.post(
        "/admin/v1/runs/run_0001/reset",
        json={"expected_generation": 1},
        headers={**admin_headers, "Idempotency-Key": "reset-faults"},
    )
    listing = client.get("/admin/v1/faults", headers=admin_headers).json()
    assert listing["items"] == []
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 200


def test_fault_activations_are_counted_in_metrics(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-112: metrics are the audit trail, separate from the baseline error rate."""
    declare(
        client, admin_headers, rule_id="f-metric",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 3, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    for _ in range(3):
        client.get("/v1/deals?limit=1", headers=data_headers)
    metrics = client.get("/admin/v1/metrics", headers=admin_headers).text
    assert 'sim_fault_activations_total{mode="http_error",rule_id="f-metric"} 3.0' in metrics
    assert 'sim_fault_matches_total{rule_id="f-metric"}' in metrics


def test_an_injected_error_is_distinguishable_from_a_real_one(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-113: a baseline 5xx must not be mistaken for an injected one.

    If the two were indistinguishable, every chaos run would contaminate the
    error-rate baseline and a genuine regression could hide inside it. An
    injected response carries ``x-fault-injected`` and ``details.injected``,
    and only it increments ``sim_fault_activations_total``.
    """
    declare(
        client, admin_headers, rule_id="f-injected-503",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    injected = client.get("/v1/deals?limit=1", headers=data_headers)
    assert injected.status_code == 503
    assert injected.headers["x-fault-injected"] == "1"
    assert injected.json()["error"]["details"]["injected"] is True

    # A real rejection, produced by the service itself rather than a rule.
    genuine = client.get("/v1/deals?limit=9999", headers=data_headers)
    assert genuine.status_code == 422
    assert "x-fault-injected" not in genuine.headers
    assert genuine.json()["error"]["details"].get("injected") is None

    metrics = client.get("/admin/v1/metrics", headers=admin_headers).text
    # The injected failure is counted as a fault activation...
    assert 'sim_fault_activations_total{mode="http_error",rule_id="f-injected-503"} 1.0' in metrics
    # ...and separately in the plain HTTP status counter, where an operator can
    # subtract it from the baseline.
    assert 'sim_http_requests_total{plane="data",route="/v1/deals",status="503"}' in metrics
    assert 'sim_http_requests_total{plane="data",route="/v1/deals",status="422"}' in metrics


def test_the_rule_set_is_snapshotted_at_the_start_of_a_request(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-057: a rule change must not alter a decision already taken.

    Asserted against the engine rather than by racing two HTTP requests. The
    obvious version - hold a request open with a long delay, declare a rule
    from another thread, assert the in-flight request was unaffected - cannot
    be made deterministic: ``TestClient`` serializes through one portal, so
    whether the declaration lands before or after the request reads the rule
    set is a coin flip. A test that passes on a race is worse than no test
    (WF-118), so the mechanism is tested directly: ``snapshot()`` hands out an
    immutable view, and a later mutation cannot reach a view already taken.
    """
    engine = client.app.state.context.faults

    declare(
        client, admin_headers, rule_id="f-already-there", priority=10,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "drop_one_item", "parameters": {"selection": "last"}},
    )
    taken = engine.snapshot()
    assert isinstance(taken, tuple)  # immutable by construction
    assert [rule.rule_id for rule in taken] == ["f-already-there"]

    declare(
        client, admin_headers, rule_id="f-added-later", priority=20,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 503}},
    )
    # The view taken earlier is unchanged, which is what makes a decision
    # already computed from it stable for the rest of that request.
    assert [rule.rule_id for rule in taken] == ["f-already-there"]
    assert [rule.rule_id for rule in engine.snapshot()] == [
        "f-already-there",
        "f-added-later",
    ]

    # Removal is equally invisible to a view already handed out.
    engine.remove("f-added-later")
    assert [rule.rule_id for rule in taken] == ["f-already-there"]
    assert [rule.rule_id for rule in engine.snapshot()] == ["f-already-there"]

    # Ordering is by (priority, rule_id), not insertion order, so which rule
    # fires is decided by the declaration rather than by dict iteration.
    declare(
        client, admin_headers, rule_id="f-urgent", priority=1,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "http_error", "parameters": {"status": 504}},
    )
    assert [rule.rule_id for rule in engine.snapshot()] == [
        "f-urgent",
        "f-already-there",
    ]

    # And a change between requests does take effect, so the snapshot is
    # per-request rather than frozen for the process.
    assert client.get("/v1/deals?limit=5", headers=data_headers).status_code == 504
    engine.remove("f-urgent")
    assert len(client.get("/v1/deals?limit=5", headers=data_headers).json()["items"]) == 4


def test_many_rules_are_matched_without_unbounded_work(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    settings, faulted_run: str,
) -> None:
    """WF-099: the rule limit is enforced, and lookup stays usable at it.

    The assertion is the bound and the ordering, not a latency figure: rules
    are evaluated in ``(priority, rule_id)`` order, so the one that fires is
    decided by the declaration rather than by dict iteration.
    """
    for index in range(settings.max_fault_rules - 1):
        declare(
            client, admin_headers, rule_id=f"f-bulk-{index:04d}", priority=500 + index,
            # Scoped to a path no request uses, so these only cost matching.
            scope={"path": "/v1/marks", "symbol": f"NOPE{index}"},
            trigger={"kind": "always", "max_activations": 1, "ttl_seconds": 600},
            action={"mode": "http_error", "parameters": {"status": 503}},
        )
    listing = client.get("/admin/v1/faults", headers=admin_headers).json()
    assert len(listing["items"]) == settings.max_fault_rules - 1

    # One more fits; the one after that is refused rather than accepted silently.
    declare(
        client, admin_headers, rule_id="f-last", priority=1,
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 2, "ttl_seconds": 600},
        action={"mode": "http_error", "parameters": {"status": 504}},
    )
    refused = client.post(
        "/admin/v1/faults",
        json={
            "rule_id": "f-over-limit",
            "trigger": {"kind": "always"},
            "action": {"mode": "http_error", "parameters": {"status": 503}},
        },
        headers=admin_headers,
    )
    assert refused.status_code == 422
    assert refused.json()["error"]["details"]["max_fault_rules"] == settings.max_fault_rules

    # With 100 rules loaded, the lowest-priority-number rule still decides.
    assert client.get("/v1/deals?limit=1", headers=data_headers).status_code == 504
    # Requests the bulk rules do not scope to are untouched.
    assert client.get("/v1/marks", headers=data_headers).status_code == 200


def test_a_fault_is_not_counted_when_it_did_not_apply(
    client: TestClient, admin_headers: dict[str, str], data_headers: dict[str, str],
    faulted_run: str,
) -> None:
    """WF-119: an activation count that includes faults that never ran is a lie.

    Here a body mutation is scoped to a request that will fail validation, so
    the handler never produces a payload to mutate.
    """
    declare(
        client, admin_headers, rule_id="f-never-applies",
        scope={"path": "/v1/deals"},
        trigger={"kind": "always", "max_activations": 5, "ttl_seconds": 60},
        action={"mode": "drop_one_item", "parameters": {"selection": "last"}},
    )
    refused = client.get("/v1/deals?limit=9999", headers=data_headers)
    assert refused.status_code == 422
    assert "x-fault-injected" not in refused.headers
    rule = audit(client, admin_headers, "f-never-applies")
    assert rule["match_count"] == 1
    assert rule["activations"] == 0
