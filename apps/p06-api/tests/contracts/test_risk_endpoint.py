"""IT06-01 and friends: a successful report is judged by its content, not its status.

A 200 proves nothing on its own. Each test here checks the money strings, the
cut vector, the versions, the digests, the header policy and the engine call
count. A fast 200 with a missing cut is a failure.
"""

from __future__ import annotations

import pytest
from conftest import ACCOUNT_A, ACCOUNT_B, SOURCE_ID

pytestmark = pytest.mark.contract


async def test_ready_report_carries_money_strings_cut_and_digests(
    client, context, auth, risk_url, no_float_money, evidence
):
    """WF06-A-01, WF06-F-01, WF06-K-01: the full READY contract."""
    before = context.views.world.checksum()

    response = await client.get(risk_url, params={"source_id": SOURCE_ID}, headers=auth)

    assert response.status_code == 200, response.text
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["cache-control"] == "private, no-store"
    assert response.headers["pragma"] == "no-cache"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.headers["vary"] == "Authorization"
    assert response.headers["x-request-id"].startswith("req-")

    no_float_money(response.text)
    body = response.json()

    assert body["status"] == "READY"
    assert body["currency"] == "USD"
    assert body["scope"] == {
        "source_id": SOURCE_ID,
        "epoch_id": "E1",
        "account_ids": [ACCOUNT_A],
    }

    # The cut vector must be complete; a report without it is unverifiable.
    cuts = body["cuts"]
    assert cuts["account_scan_id"] == "scan-A"
    assert cuts["position_scan_id"] == "scan-P"
    assert cuts["deal_watermark_seq"] == 1050
    assert cuts["quote_snapshot_id"] == "quote-S1030"
    assert cuts["recon_revision"] == "recon-R10"
    assert cuts["consistency"] == "COMPATIBLE"

    versions = body["versions"]
    assert versions == {
        "risk_policy": "risk-policy-v1",
        "symbol_catalog": "sim-usd-fx-v1",
        "api_schema": "risk-response-v1",
        "result_contract": "risk-result-v1",
    }

    exposure = body["components"]["exposure"]
    assert exposure["status"] == "READY"
    assert exposure["by_symbol"]["EURUSD"] == {
        "unit_currency": "EUR",
        "long_units": "50000",
        "short_units": "0",
        "gross_units": "50000",
        "net_units": "50000",
    }

    realized = body["components"]["realized"]
    assert realized["gross_usd"] == "60.00"
    assert realized["commission_usd"] == "-3.50"
    assert realized["swap_usd"] == "-0.50"
    assert realized["net_excluding_fee_usd"] == "56.00"
    assert realized["fee_coverage"] == "NOT_AVAILABLE"
    # INV06-06: the unqualified name must not appear at all.
    assert "net_usd" not in realized

    unrealized = body["components"]["unrealized"]
    assert unrealized["status"] == "READY"
    assert unrealized["total_usd"] == "25.00"
    assert unrealized["quote_snapshot_id"] == "quote-S1030"

    assert body["composite_total"] == "81.00"
    assert body["input_digest"].startswith("sha256:")
    assert body["output_digest"].startswith("sha256:")
    assert body["audit_id"]

    assert context.engine.stats.calls == 1
    assert context.views.world.checksum() == before, "P06 must never mutate upstream state"

    receipts = context.audit.all_records()
    assert [row.decision for row in receipts] == ["ALLOW"]
    assert receipts[0].output_digest == body["output_digest"]
    # The audit row proves scope by hash, without carrying the account number.
    assert ACCOUNT_A not in receipts[0].principal_scope_hash

    evidence.record(
        scenario_ids=("WF06-A-01", "WF06-F-01", "WF06-K-01", "FX-READY-001", "FX-REALIZED-002"),
        test_id="IT06-01",
        expected_http=200,
        observed_http=response.status_code,
        assertions=(
            "money_fields_are_strings",
            "cut_vector_complete",
            "composite_total_81.00",
            "engine_called_once",
            "upstream_checksum_unchanged",
            "audit_receipt_committed",
        ),
        engine_calls=context.engine.stats.calls,
        source_unchanged=True,
        audit_decisions=tuple(row.decision for row in receipts),
    )


async def test_component_selection_limits_the_payload(client, context, auth, risk_url):
    """Only the requested components are computed and returned."""
    response = await client.get(
        risk_url, params={"source_id": SOURCE_ID, "components": "EXPOSURE"}, headers=auth
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body["components"]) == {"exposure"}
    # A single non-money component cannot produce a cross-component total.
    assert body["composite_total"] is None


async def test_component_order_does_not_change_the_result(client, auth, risk_url):
    """Metamorphic: the component list is canonicalised before use."""
    forward = await client.get(
        risk_url,
        params={"source_id": SOURCE_ID, "components": "EXPOSURE,REALIZED,UNREALIZED"},
        headers=auth,
    )
    reversed_order = await client.get(
        risk_url,
        params={"source_id": SOURCE_ID, "components": "UNREALIZED,REALIZED,EXPOSURE"},
        headers=auth,
    )
    assert forward.status_code == reversed_order.status_code == 200
    assert forward.json()["output_digest"] == reversed_order.json()["output_digest"]
    assert forward.json()["input_digest"] == reversed_order.json()["input_digest"]


async def test_repeated_identical_requests_are_digest_stable(client, auth, risk_url):
    """Idempotent read semantics: same cut in, same canonical payload out."""
    first = await client.get(risk_url, params={"source_id": SOURCE_ID}, headers=auth)
    second = await client.get(risk_url, params={"source_id": SOURCE_ID}, headers=auth)
    assert first.status_code == second.status_code == 200
    a, b = first.json(), second.json()
    assert a["output_digest"] == b["output_digest"]
    assert a["components"] == b["components"]
    # Request-scoped identifiers are allowed to differ, and must.
    assert a["request_id"] != b["request_id"]
    assert a["audit_id"] != b["audit_id"]


async def test_account_id_keeps_its_exact_string_form(client, context, auth):
    """WF06-A-08: 00042 is not 42, and the shorter form is not in scope."""
    padded = await client.get(
        f"/api/v1/accounts/{ACCOUNT_A}/risk", params={"source_id": SOURCE_ID}, headers=auth
    )
    assert padded.status_code == 200
    assert padded.json()["scope"]["account_ids"] == ["00042"]

    unpadded = await client.get(
        "/api/v1/accounts/42/risk", params={"source_id": SOURCE_ID}, headers=auth
    )
    assert unpadded.status_code == 404
    assert unpadded.json()["code"] == "RESOURCE_NOT_FOUND"


async def test_other_tenants_account_is_masked_and_costs_no_compute(client, context, auth):
    """WF06-C-01: a BOLA attempt is a masked 404 with zero engine work."""
    response = await client.get(
        f"/api/v1/accounts/{ACCOUNT_B}/risk", params={"source_id": SOURCE_ID}, headers=auth
    )
    assert response.status_code == 404
    assert response.headers["content-type"].startswith("application/problem+json")
    body = response.json()
    assert body["code"] == "RESOURCE_NOT_FOUND"
    assert ACCOUNT_B not in response.text
    assert context.engine.stats.calls == 0
    assert context.views.counters.get("positions", 0) == 0
