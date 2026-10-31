"""Domain AD -- the Project 06 boundary: API semantics and consumer misuse.

Covers WF5-349..360. These tests encode the contract that stops a correct
engine from being misrepresented by the layer in front of it.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest

from risk.contract import RiskRequest, parse_components
from risk.domain.facts import AuthorizedScope
from risk.errors import ContractError, ReasonCode
from risk.status import Component, ComponentStatus, GlobalStatus
from risk_tools.fixtures import ACCOUNT_A, SOURCE_A, T0, TENANT_A, scenario

pytestmark = [pytest.mark.contract, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))


@pytest.mark.wf5("WF5-349", "WF5-025")
async def test_strict_composite_over_independent_cuts_is_blocked(
    service_factory, request_factory, evidence
):
    """Without a freeze proof the composite is refused -- no plausible total."""
    spec = scenario("R05-cut-mismatch")
    service, _ = service_factory(spec)

    result = await service.compute(request_factory(spec, strict_composite=True), SCOPE)

    assert result.composite_total is None
    assert "INCOMPATIBLE_CUT" in result.reason_codes
    assert result.cuts["consistency"] == "INDEPENDENT"
    assert result.status is not GlobalStatus.READY
    evidence(
        expected_status="BLOCKED",
        observed_status=str(result.status),
        expected_reason_codes=("INCOMPATIBLE_CUT",),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
        output_digest=result.output_digest,
    )


@pytest.mark.wf5("WF5-350")
async def test_partial_opt_in_returns_degraded_with_a_null_composite(
    service_factory, request_factory, evidence
):
    """With strict_composite off the caller gets the usable components, labelled."""
    spec = scenario("R05-cut-mismatch")
    service, _ = service_factory(spec)

    result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    assert result.status is GlobalStatus.DEGRADED
    assert result.composite_total is None
    assert result.reason_codes  # DEGRADED always explains itself
    # The components that *are* usable still carry their values.
    usable = [c for c, p in result.components.items() if p.status.has_values]
    assert usable
    evidence(
        expected_status="DEGRADED",
        observed_status=str(result.status),
        observed_reason_codes=result.reason_codes,
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-351")
async def test_money_serialises_as_decimal_strings(service_factory, request_factory, evidence):
    """Every monetary field crosses the boundary as a string, never a JSON number."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    payload = result.as_payload()
    encoded = json.dumps(payload)
    decoded = json.loads(encoded)

    realized = decoded["components"]["REALIZED"]
    for key in ("gross", "commission", "swap", "net_excluding_fee"):
        assert isinstance(realized[key], str), f"{key} must be a string on the wire"
    assert realized["net_excluding_fee"] == "56.00"

    unrealized = decoded["components"]["UNREALIZED"]
    assert isinstance(unrealized["total"], str)
    # A float round-trip is exactly what this prevents.
    assert "56.00" == realized["net_excluding_fee"] != 56.0
    evidence(
        expected_values={"net_excluding_fee": "56.00", "type": "str"},
        observed_values={
            "net_excluding_fee": realized["net_excluding_fee"],
            "type": type(realized["net_excluding_fee"]).__name__,
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-352")
async def test_blocked_component_carries_no_value_keys(
    service_factory, request_factory, evidence
):
    """A blocked component has a status and reasons and no numbers at all.

    Not ``null`` -- absent. A null unrealized is one ``|| 0`` away from being
    rendered as a flat line at zero, which is the exact misreading the design
    forbids (section 9.1).
    """
    spec = scenario("R05-empty-incomplete")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.EXPOSURE, Component.UNREALIZED})),
        SCOPE,
    )

    for component in (Component.EXPOSURE, Component.UNREALIZED):
        payload = result.component(component).as_payload()
        assert payload["status"] in ("BLOCKED", "UNSUPPORTED")
        assert payload["reason_codes"]
        for forbidden in ("total", "gross", "net_excluding_fee", "by_symbol", "by_position"):
            assert forbidden not in payload, f"{component} leaked a {forbidden} key"
        assert None not in payload.values()
    evidence(
        expected_status="BLOCKED",
        observed_status=str(result.component(Component.EXPOSURE).status),
        fixture_id=spec.name,
        notes=("absent keys, not null values",),
    )


@pytest.mark.wf5("WF5-353")
async def test_every_non_ready_outcome_carries_a_reason(
    service_factory, request_factory, evidence
):
    """A status without a reason code is itself a contract violation."""
    for fixture_id in ("R05-empty-incomplete", "R05-recon-conflict", "R05-cut-mismatch"):
        spec = scenario(fixture_id)
        service, _ = service_factory(spec)
        result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)

        assert result.reason_codes, f"{fixture_id} returned no reason"
        for component, payload in result.components.items():
            if payload.status is not ComponentStatus.READY:
                assert payload.reason_codes, f"{fixture_id}/{component} has no reason"
    evidence(
        oracle="OPERATIONAL",
        observed_values={"fixtures_checked": "3"},
    )


@pytest.mark.wf5("WF5-354")
async def test_different_quote_snapshots_give_different_digests(
    service_factory, request_factory, evidence
):
    """Two requests differing only in the mark cannot share a cached answer."""
    from risk.adapters.quotes import FixtureQuoteProvider
    from risk_tools.fixtures import QuoteSpec, quote_snapshot_for

    spec = scenario("R05-buy-mark")
    first = quote_snapshot_for(spec)

    moved = scenario("R05-buy-mark")
    moved.quote_snapshot_id = "quote-S1040"
    moved.quotes = (QuoteSpec(bid="1.10500", ask="1.10520", quote_id="q-EURUSD-2"),)
    moved.quote_bound_position_scan_id = "scan-P"
    second = quote_snapshot_for(moved)

    provider = FixtureQuoteProvider(
        {first.snapshot_id: first, second.snapshot_id: second},
        default_snapshot_id=first.snapshot_id,
    )
    service, _ = service_factory(spec, quote_provider=provider)

    result_a = await service.compute(
        request_factory(spec, components=frozenset({Component.UNREALIZED}),
                        quote_snapshot_id=first.snapshot_id),
        SCOPE,
    )
    result_b = await service.compute(
        request_factory(spec, components=frozenset({Component.UNREALIZED}),
                        quote_snapshot_id=second.snapshot_id),
        SCOPE,
    )

    assert result_a.cuts["quote_snapshot_id"] != result_b.cuts["quote_snapshot_id"]
    assert result_a.input_digest != result_b.input_digest
    assert result_a.output_digest != result_b.output_digest
    assert result_a.component(Component.UNREALIZED).values is not None
    evidence(
        observed_values={
            "snapshot_a": str(result_a.cuts["quote_snapshot_id"]),
            "snapshot_b": str(result_b.cuts["quote_snapshot_id"]),
        },
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-355")
def test_the_pure_core_is_synchronous_and_io_free(evidence):
    """The calculators are plain functions, so Project 06 can run them off-loop.

    Asserted structurally: if a calculator were a coroutine, the only way to
    call it from a FastAPI handler would be to await it on the event loop, and
    a one-million-row Decimal aggregation would stall every other request.
    """
    import inspect

    from risk.domain.exposure import compute_exposure
    from risk.domain.realized import compute_realized
    from risk.domain.unrealized import compute_unrealized

    for function in (compute_exposure, compute_realized, compute_unrealized):
        assert not inspect.iscoroutinefunction(function), f"{function.__name__} is async"

    # And the domain package imports nothing that performs I/O.
    import risk.domain.exposure as exposure_module
    import risk.domain.realized as realized_module
    import risk.domain.unrealized as unrealized_module

    for module in (exposure_module, realized_module, unrealized_module):
        source = inspect.getsource(module)
        for forbidden in ("import sqlite3", "import httpx", "import requests", "import psycopg"):
            assert forbidden not in source, f"{module.__name__} performs I/O"
    evidence(oracle="OPERATIONAL", notes=("CPU-bound work stays callable from a worker",))


@pytest.mark.wf5("WF5-356")
async def test_account_identifiers_do_not_appear_in_the_envelope(
    service_factory, request_factory, evidence
):
    """The response identifies the scope by hash, so a URL or log cannot leak it."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    encoded = json.dumps(result.as_payload())
    assert ACCOUNT_A not in encoded
    assert result.scope["scope_hash"]
    assert result.scope["account_count"] == 1
    # The log record is held to the same rule.
    log_record = json.dumps(service.last_log_record)
    assert ACCOUNT_A not in log_record
    assert TENANT_A not in log_record
    evidence(
        oracle="OPERATIONAL",
        observed_values={"account_in_payload": "False", "account_in_log": "False"},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-357")
@pytest.mark.p1
async def test_repeated_identical_request_resolves_to_one_audit_receipt(
    service_factory, request_factory, audit_sink, evidence
):
    """A client retry produces the same calculation id and one stored receipt."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    request = request_factory(spec)

    first = await service.compute(request, SCOPE)
    second = await service.compute(request, SCOPE)

    assert first.calculation_id == second.calculation_id
    assert first.output_digest == second.output_digest
    assert len(audit_sink) == 1
    assert audit_sink.writes == 1
    evidence(
        expected_values={"receipts": "1"},
        observed_values={"receipts": str(len(audit_sink)),
                         "calculation_id": first.calculation_id},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-358")
@pytest.mark.p1
async def test_totals_derive_from_the_full_authorized_scope(
    service_factory, request_factory, evidence
):
    """The manifest records how many accounts the figures cover."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    assert result.lineage["row_counts"]["accounts"] == 1
    assert result.scope["account_count"] == 1
    # Row counts travel with the result, so a paginating consumer cannot sum a
    # page and present it as the whole.
    assert set(result.lineage["row_counts"]) >= {"accounts", "positions", "deals"}
    evidence(
        observed_values={"row_counts": json.dumps(result.lineage["row_counts"])},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-360")
@pytest.mark.p2
async def test_envelope_contains_no_spreadsheet_formula_surface(
    service_factory, request_factory, evidence
):
    """No value in the envelope begins with a character a spreadsheet executes."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    def walk(node):
        if isinstance(node, dict):
            for key, value in node.items():
                yield from walk(key)
                yield from walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                yield from walk(value)
        elif isinstance(node, str):
            yield node

    dangerous = ("=", "+", "-", "@", "\t", "\r")
    offenders = [
        text
        for text in walk(result.as_payload())
        if text.startswith(dangerous) and not _is_plain_number(text)
    ]
    assert offenders == [], offenders
    evidence(
        oracle="OPERATIONAL",
        observed_values={"offenders": str(len(offenders))},
        notes=("negative amounts are plain numbers; an export layer still quotes them",),
    )


def _is_plain_number(text: str) -> bool:
    """A leading '-' on a decimal amount is a sign, not a formula."""
    try:
        from decimal import Decimal

        Decimal(text)
        return True
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Request validation
# --------------------------------------------------------------------------- #


def test_request_rejects_unknown_component():
    with pytest.raises(ContractError) as exc:
        parse_components(["EXPOSURE", "VAR_99"])
    assert exc.value.reason is ReasonCode.CONTRACT_MISSING_FIELD


def test_request_rejects_inverted_window():
    with pytest.raises(ContractError) as exc:
        RiskRequest(
            epoch_id="e",
            components=frozenset({Component.REALIZED}),
            period_start=T0,
            period_end=T0 - timedelta(days=1),
        )
    assert exc.value.reason is ReasonCode.INVALID_TIME_WINDOW


def test_realized_requires_an_explicit_window():
    """An unbounded realized window is not what a caller asking for a month means."""
    with pytest.raises(ContractError) as exc:
        RiskRequest(epoch_id="e", components=frozenset({Component.REALIZED}))
    assert exc.value.reason is ReasonCode.INVALID_TIME_WINDOW


def test_request_rejects_naive_timestamps():
    from datetime import datetime

    with pytest.raises(ContractError) as exc:
        RiskRequest(
            epoch_id="e",
            components=frozenset({Component.EXPOSURE}),
            valuation_as_of=datetime(2026, 10, 15, 12, 0),
        )
    assert exc.value.reason is ReasonCode.NAIVE_TIMESTAMP_REJECTED


def test_request_from_payload_parses_the_documented_shape():
    """The example request body of design section 7.1 round-trips."""
    payload = {
        "source_id": "sim-broker-a",
        "epoch_id": "3cecd819-fd61-420b-8e35-019c70203b4e",
        "account_ids": ["00042"],
        "components": ["EXPOSURE", "REALIZED", "UNREALIZED"],
        "period_start_utc": "2026-10-01T00:00:00Z",
        "period_end_utc": "2026-11-01T00:00:00Z",
        "strict_composite": False,
        "risk_policy_version": "risk-policy-v1",
        "symbol_catalog_version": "sim-usd-fx-v1",
    }
    request = RiskRequest.from_payload(payload)

    assert request.components == frozenset(Component)
    assert request.account_ids == ("00042",)
    assert request.strict_composite is False
    assert request.period_start is not None and request.period_start.tzinfo is not None
    # source_id in the body is ignored by the request object: authorization is
    # the only thing that decides which source is read.
    assert not hasattr(request, "source_id")
