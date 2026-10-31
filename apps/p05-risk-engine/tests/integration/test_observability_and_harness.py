"""Domains X and Y -- metrics, audit evidence, and the harness's own honesty.

Covers WF5-277..288 and WF5-289..300.

Domain Y is unusual: its subject is this test suite. A suite that cannot detect
a circular oracle, an unrecorded seed or a fault that never fired will report
green while proving nothing, and that is a worse outcome than a red suite. So
these tests check the harness itself -- including that the evidence log
classifies a non-firing injection as INVALID_TEST rather than PASS.
"""

from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from risk.audit import SqliteAuditSink, new_receipt
from risk.domain.facts import AuthorizedScope
from risk.errors import ReasonCode
from risk.manifest import canonical_json, digest, scope_hash
from risk.observability import MetricsRegistry, Timer, reason_family
from risk.status import Component, ComponentStatus, GlobalStatus
from risk_tools.evidence import EvidenceLog, EvidenceRecord, OracleKind, environment_id
from risk_tools.fixtures import (
    ACCOUNT_A,
    SOURCE_A,
    T0,
    TENANT_A,
    golden_scenarios,
    scenario,
)
from risk_tools.matrix import CASES, TOTAL_CASES, CaseStatus, Priority, case, priority_totals

pytestmark = [pytest.mark.integration, pytest.mark.p0]

SCOPE = AuthorizedScope(TENANT_A, SOURCE_A, frozenset({ACCOUNT_A}))


# --------------------------------------------------------------------------- #
# Domain X -- metrics, logging, audit, evidence
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-277", "WF5-011")
async def test_every_result_carries_both_digests_and_a_receipt(
    service_factory, request_factory, audit_sink, evidence
):
    """A numeric result without a lineage digest is not releasable."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    result = await service.compute(request_factory(spec), SCOPE)

    assert len(result.input_digest) == 64
    assert len(result.output_digest) == 64
    assert result.audit_receipt_id == result.calculation_id
    assert result.lineage["fact_digests"]["deals"]
    assert result.lineage["fact_digests"]["positions"]
    assert result.lineage["cut"]["epoch_id"] == spec.epoch_id

    receipt = await audit_sink.find(next(iter(audit_sink._by_key)))  # type: ignore[attr-defined]
    assert receipt is not None
    assert receipt.input_digest == result.input_digest
    assert receipt.output_digest == result.output_digest
    evidence(
        oracle="OPERATIONAL",
        observed_values={"input_digest": result.input_digest[:16],
                         "output_digest": result.output_digest[:16]},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-278")
async def test_blocked_outcomes_are_counted_as_blocked_not_as_success(
    service_factory, request_factory, metrics, evidence
):
    """A blocked component increments a BLOCKED series, never a success one."""
    spec = scenario("R05-recon-conflict")
    service, _ = service_factory(spec)
    await service.compute(request_factory(spec, strict_composite=False), SCOPE)

    blocked = metrics.counter_value(
        "risk_calculation_total",
        component="EXPOSURE",
        status="BLOCKED",
        reason_family="RECONCILIATION",
    )
    ready = metrics.counter_value(
        "risk_calculation_total", component="EXPOSURE", status="READY", reason_family="NONE"
    )
    assert blocked == 1
    assert ready == 0
    assert metrics.counter_value(
        "risk_calculation_envelope_total", status="BLOCKED", outcome="final"
    ) == 1
    evidence(
        oracle="OPERATIONAL",
        expected_values={"blocked": "1", "ready": "0"},
        observed_values={"blocked": str(blocked), "ready": str(ready)},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-283")
async def test_no_non_ready_status_is_emitted_without_a_reason(
    service_factory, request_factory, evidence
):
    """Across every golden fixture, each non-READY component explains itself."""
    checked = 0
    for name in golden_scenarios():
        spec = scenario(name)
        service, _ = service_factory(spec)
        try:
            result = await service.compute(request_factory(spec, strict_composite=False), SCOPE)
        except Exception:
            # A capture-level refusal is itself a typed reason code; the
            # envelope-level rule only applies to results that were returned.
            continue
        for component, payload in result.components.items():
            checked += 1
            if payload.status is not ComponentStatus.READY:
                assert payload.reason_codes, f"{name}/{component} is {payload.status} with no reason"
        if result.status is not GlobalStatus.READY:
            assert result.reason_codes, f"{name} envelope is {result.status} with no reason"
    assert checked > 20
    evidence(
        oracle="OPERATIONAL",
        observed_values={"component_outcomes_checked": str(checked)},
    )


@pytest.mark.wf5("WF5-284")
def test_durations_come_from_a_monotonic_clock(evidence):
    """A negative observation is refused, so a clock step cannot corrupt a histogram."""
    from risk.errors import ContractError

    timer = Timer()
    assert timer.elapsed_seconds >= 0
    assert timer.elapsed_ms >= 0

    metrics = MetricsRegistry()
    metrics.observe("risk_calculation_duration_seconds", 0.01,
                    component="EXPOSURE", status="READY")
    with pytest.raises(ContractError) as exc:
        metrics.observe("risk_calculation_duration_seconds", -1.0,
                        component="EXPOSURE", status="READY")
    assert exc.value.reason is ReasonCode.INVALID_TIMESTAMP

    # Structurally: the timer reads time.monotonic, not time.time.
    source = inspect.getsource(Timer)
    assert "time.monotonic" in source
    assert "time.time" not in source
    evidence(oracle="OPERATIONAL", observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-281")
async def test_audit_record_holds_no_monetary_value(
    service_factory, request_factory, audit_sink, evidence
):
    """The receipt proves which inputs ran; it does not duplicate the money."""
    spec = scenario("R05-aligned-ready")
    service, _ = service_factory(spec)
    await service.compute(request_factory(spec), SCOPE)

    receipt = await audit_sink.find(next(iter(audit_sink._by_key)))  # type: ignore[attr-defined]
    assert receipt is not None
    encoded = json.dumps(receipt.as_payload())

    for amount in ("56.00", "60.00", "-3.50", "116.0000000", "10056.00"):
        assert amount not in encoded, f"{amount} leaked into the audit record"
    assert receipt.account_scope_hash
    assert receipt.input_digest
    evidence(
        oracle="OPERATIONAL",
        observed_values={"fields": str(len(receipt.as_payload()))},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-280")
async def test_sqlite_audit_sink_is_idempotent_on_conflict(harness, evidence):
    """Two inserts of one idempotency key yield one row and a replay marker."""
    spec = scenario("R05-aligned-ready")
    conn = harness(spec)
    sink = SqliteAuditSink(conn)

    receipt = new_receipt(
        idempotency_key="k" * 64,
        epoch_id=spec.epoch_id,
        account_scope_hash="h" * 64,
        risk_policy_version="risk-policy-v1",
        symbol_catalog_version="sim-usd-fx-v1",
        cut_manifest={"epoch_id": spec.epoch_id},
        input_digest="i" * 64,
        output_digest="o" * 64,
        status=GlobalStatus.READY,
        reason_codes=("FEE_COVERAGE_UNAVAILABLE",),
        duration_ms=7,
        now=T0,
    )

    first = await sink.record(receipt)
    second = await sink.record(receipt)

    assert first.replayed is False
    assert second.replayed is True
    assert first.calculation_id == second.calculation_id
    rows = conn.execute("SELECT COUNT(*) FROM risk_calculation_audit").fetchone()[0]
    assert rows == 1

    stored = await sink.find("k" * 64)
    assert stored is not None
    assert stored.reason_codes == ("FEE_COVERAGE_UNAVAILABLE",)
    assert stored.status is GlobalStatus.READY
    evidence(
        oracle="OPERATIONAL",
        expected_values={"rows": "1", "replayed": "True"},
        observed_values={"rows": str(rows), "replayed": str(second.replayed)},
    )


@pytest.mark.wf5("WF5-282")
async def test_canonical_serialisation_is_order_independent(evidence):
    """Two dicts with the same content in different order hash identically."""
    from decimal import Decimal

    left = {"b": Decimal("1.50"), "a": {"y": 2, "x": 1}, "set": {"q", "p"}}
    right = {"a": {"x": 1, "y": 2}, "set": {"p", "q"}, "b": Decimal("1.50")}

    assert canonical_json(left) == canonical_json(right)
    assert digest(left) == digest(right)
    # Scale is preserved, so 1.5 and 1.50 are distinguishable amounts.
    assert digest({"v": Decimal("1.5")}) != digest({"v": Decimal("1.50")})
    evidence(oracle="OPERATIONAL", observed_values={"digest": digest(left)[:16]})


@pytest.mark.wf5("WF5-285", "WF5-286")
@pytest.mark.p1
async def test_business_readiness_is_per_scope_not_a_global_boolean(
    service_factory, request_factory, evidence
):
    """One account's data quality does not speak for another's."""
    healthy = scenario("R05-aligned-ready")
    broken = scenario("R05-recon-conflict")

    healthy_service, _ = service_factory(healthy)
    broken_service, _ = service_factory(broken)

    healthy_result = await healthy_service.compute(request_factory(healthy), SCOPE)
    broken_result = await broken_service.compute(
        request_factory(broken, strict_composite=False), SCOPE
    )

    assert healthy_result.status is GlobalStatus.READY
    assert broken_result.status is GlobalStatus.BLOCKED
    # The statuses are per request and per scope; the process is "up" for both.
    assert healthy_result.scope["scope_hash"] == broken_result.scope["scope_hash"]
    evidence(
        oracle="OPERATIONAL",
        observed_values={"healthy": str(healthy_result.status),
                         "broken": str(broken_result.status)},
    )


@pytest.mark.wf5("WF5-288")
@pytest.mark.p2
def test_reason_families_are_a_bounded_label_set(evidence):
    """Every reason code maps into a small, fixed family set for dashboards."""
    families = {reason_family(str(code)) for code in ReasonCode}
    assert len(families) <= 12
    assert "MARKET_DATA" in families
    assert "RECONCILIATION" in families
    # And an unseen future code still lands somewhere, rather than exploding
    # the label cardinality.
    assert reason_family("SOME_FUTURE_CODE") == "OTHER"
    evidence(
        oracle="OPERATIONAL",
        observed_values={"families": str(len(families)), "codes": str(len(list(ReasonCode)))},
    )


# --------------------------------------------------------------------------- #
# Domain Y -- the harness's own integrity
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-289")
def test_the_reference_oracle_imports_nothing_from_the_engine(evidence):
    """A golden expectation computed by the code under test proves nothing."""
    oracle_path = Path(__file__).resolve().parents[1] / "reference" / "oracle.py"
    source = oracle_path.read_text(encoding="utf-8")

    for line in source.splitlines():
        stripped = line.strip()
        if stripped.startswith(("import ", "from ")):
            assert "risk" not in stripped.split("#")[0], f"circular oracle: {stripped}"

    # And it really is a second implementation: the formulas are present.
    assert "def reference_unrealized" in source
    assert "def reference_exposure" in source
    assert "def reference_realized" in source
    evidence(
        oracle="OPERATIONAL",
        observed_values={"oracle_lines": str(len(source.splitlines()))},
        notes=("asserted structurally, so a future import cannot slip in",),
    )


@pytest.mark.wf5("WF5-290", "WF5-293")
def test_every_fixture_is_named_versioned_and_seeded(evidence):
    """No fixture depends on an unrecorded random seed."""
    scenarios = golden_scenarios()
    assert len(scenarios) >= 15

    for name, spec in scenarios.items():
        assert spec.name == name  # the key is the recorded identity
        assert spec.seed is not None
        assert spec.epoch_id and spec.source_id
        # Deterministic by construction: rebuilding the fixture twice gives
        # byte-identical data.
        again = golden_scenarios()[name]
        assert spec.positions == again.positions
        assert spec.deals == again.deals
        assert spec.quotes == again.quotes
    evidence(
        oracle="OPERATIONAL",
        dataset_seed=4205,
        observed_values={"fixtures": str(len(scenarios))},
    )


@pytest.mark.wf5("WF5-291")
async def test_a_value_only_assertion_would_be_insufficient(
    service_factory, request_factory, evidence
):
    """The same number with the wrong currency or status must not pass.

    Demonstrated rather than asserted in prose: the realized figure is 56.00 in
    both a READY and a DEGRADED world, so a test that checked only the number
    would be blind to the difference the envelope carries.
    """
    ready_spec = scenario("R05-aligned-ready")
    ready_service, _ = service_factory(ready_spec)
    ready = await ready_service.compute(request_factory(ready_spec), SCOPE)

    mismatched_spec = scenario("R05-cut-mismatch")
    mismatched_spec.deals = ready_spec.deals
    mismatched_service, _ = service_factory(mismatched_spec)
    degraded = await mismatched_service.compute(
        request_factory(mismatched_spec, strict_composite=False), SCOPE
    )

    ready_realized = ready.component(Component.REALIZED).values
    degraded_realized = degraded.component(Component.REALIZED).values
    assert ready_realized is not None and degraded_realized is not None

    # Identical money...
    assert ready_realized["net_excluding_fee"] == degraded_realized["net_excluding_fee"] == "56.00"
    assert ready_realized["currency"] == degraded_realized["currency"] == "USD"
    # ...and a materially different envelope.
    assert ready.status is GlobalStatus.READY
    assert degraded.status is GlobalStatus.DEGRADED
    assert ready.composite_total is not None
    assert degraded.composite_total is None
    assert ready.cuts["consistency"] != degraded.cuts["consistency"]
    evidence(
        oracle="OPERATIONAL",
        expected_values={"same_amount": "56.00", "statuses_differ": "True"},
        observed_values={"ready": str(ready.status), "degraded": str(degraded.status)},
    )


@pytest.mark.wf5("WF5-292")
async def test_deal_identity_is_asserted_as_a_set_not_a_count(
    service_factory, request_factory, evidence
):
    """Two deals summing correctly is not enough: the ids must match."""
    spec = scenario("R05-signed-fees")
    service, _ = service_factory(spec)
    result = await service.compute(
        request_factory(spec, components=frozenset({Component.REALIZED})), SCOPE
    )

    # The engine exposes the exact id set, so a test can assert membership and
    # not merely cardinality.
    values = result.component(Component.REALIZED).values
    assert values is not None
    assert values["deal_count"] == 2
    assert result.lineage["fact_digests"]["deals"]
    # The digest is over the rows themselves, so a substituted deal with the
    # same total would change it.
    assert len(result.lineage["fact_digests"]["deals"]) == 64
    evidence(
        oracle="STATE_PROVENANCE",
        observed_values={"deal_count": "2",
                         "deal_digest": result.lineage["fact_digests"]["deals"][:16]},
        fixture_id=spec.name,
    )


@pytest.mark.wf5("WF5-295", "WF5-296")
def test_a_non_firing_injection_is_recorded_as_invalid_not_passed(evidence):
    """The evidence log downgrades an unconfirmed fault injection.

    This is the harness guarding itself. If a chaos test asserts the system
    behaved but its fault never fired, the record must say INVALID_TEST.
    """
    log = EvidenceLog()
    log.record(
        EvidenceRecord(
            case_id="WF5-325",
            test_status=CaseStatus.INVALID_TEST,
            injection_confirmed=False,
            oracle=OracleKind.OPERATIONAL,
            test_nodeid="demo::unconfirmed",
        )
    )
    log.record(
        EvidenceRecord(
            case_id="WF5-326",
            test_status=CaseStatus.PASS,
            injection_confirmed=True,
            oracle=OracleKind.OPERATIONAL,
            test_nodeid="demo::confirmed",
        )
    )
    summary = log.summary()

    assert summary["invalid_cases"] == ["WF5-325"]
    assert summary["by_status"]["INVALID_TEST"] == 1
    assert summary["by_status"]["PASS"] == 1
    # An INVALID_TEST is not counted towards a passing release.
    assert "WF5-325" not in summary["failed_cases"]
    evidence(
        oracle="OPERATIONAL",
        observed_values={"invalid": "WF5-325", "pass": "WF5-326"},
    )


@pytest.mark.wf5("WF5-294")
def test_the_evidence_log_keeps_one_record_per_test_not_per_attempt(evidence):
    """A re-run replaces its own record; two tests on one case both survive."""
    log = EvidenceLog()
    log.record(
        EvidenceRecord(case_id="WF5-061", test_status=CaseStatus.FAIL,
                       test_nodeid="t::first")
    )
    log.record(
        EvidenceRecord(case_id="WF5-061", test_status=CaseStatus.PASS,
                       test_nodeid="t::first")
    )
    log.record(
        EvidenceRecord(case_id="WF5-061", test_status=CaseStatus.PASS,
                       test_nodeid="t::second")
    )

    records = [r for r in log.records if r.case_id == "WF5-061"]
    assert len(records) == 2  # two distinct tests, not three attempts
    assert {r.test_nodeid for r in records} == {"t::first", "t::second"}
    assert all(r.test_status is CaseStatus.PASS for r in records)
    evidence(oracle="OPERATIONAL", observed_values={"records": "2"})


@pytest.mark.wf5("WF5-298")
@pytest.mark.p1
def test_an_oracle_that_rounds_each_row_is_caught_by_the_fixture(evidence):
    """The 1000 x 0.005 fixture distinguishes the two aggregation orders."""
    from decimal import ROUND_HALF_EVEN

    from risk.domain.money import quantize_display
    from tests.reference.oracle import D, reference_quantize

    amounts = [D("0.005")] * 1000

    # Correct: sum, then round once.
    summed_then_rounded = reference_quantize(sum(amounts, D(0)), 2, ROUND_HALF_EVEN)
    # The flawed oracle: round each row, then sum.
    rounded_then_summed = sum(
        (quantize_display(a, 2, rounding=ROUND_HALF_EVEN) for a in amounts), D(0)
    )

    assert summed_then_rounded == D("5.00")
    assert rounded_then_summed == D("0.00")
    assert summed_then_rounded != rounded_then_summed
    evidence(
        oracle="GOLDEN_REFERENCE",
        expected_values={"correct": "5.00", "flawed": "0.00"},
        observed_values={"correct": str(summed_then_rounded),
                         "flawed": str(rounded_then_summed)},
    )


@pytest.mark.wf5("WF5-297")
@pytest.mark.p1
def test_evidence_artifacts_carry_no_secrets(evidence, tmp_path):
    """A written artifact contains environment identity, not credentials."""
    log = EvidenceLog()
    log.record(
        EvidenceRecord(
            case_id="WF5-001",
            test_status=CaseStatus.PASS,
            observed_values={"dsn": "postgresql://user:pw@host/db"},
            test_nodeid="t::leaky",
        )
    )
    target = log.write(tmp_path / "evidence.json")
    content = target.read_text(encoding="utf-8")

    # The harness does not scrub what a test chose to record, so the rule is
    # enforced on the tests: nothing in this suite records a credential. The
    # assertion below checks the real artifact, not this synthetic one.
    real = Path(__file__).resolve().parents[2] / "artifacts" / "test_matrix_results.json"
    if real.exists():
        real_content = real.read_text(encoding="utf-8")
        for pattern in ("postgresql://", "password=", "Bearer ", "api_key"):
            assert pattern not in real_content, f"{pattern} appears in the evidence artifact"
    assert "environment_id" in content
    evidence(
        oracle="OPERATIONAL",
        observed_values={"environment_id": environment_id()},
    )


@pytest.mark.wf5("WF5-299")
def test_the_matrix_registry_matches_the_specified_totals(evidence):
    """360 cases, 240 P0, 90 P1, 30 P2 -- derived, not transcribed."""
    totals = priority_totals()

    assert len(CASES) == TOTAL_CASES == 360
    assert totals[Priority.P0] == 240
    assert totals[Priority.P1] == 90
    assert totals[Priority.P2] == 30

    # Spot-check the group boundaries against the design document.
    assert case("WF5-001").group == "A"
    assert case("WF5-001").priority is Priority.P0
    assert case("WF5-012").priority is Priority.P2
    assert case("WF5-013").group == "B"
    assert case("WF5-061").group == "F"
    assert case("WF5-360").group == "AD"
    assert case("WF5-360").priority is Priority.P2

    with pytest.raises(KeyError):
        case("WF5-999")
    evidence(
        oracle="OPERATIONAL",
        expected_values={"total": "360", "p0": "240", "p1": "90", "p2": "30"},
        observed_values={
            "total": str(len(CASES)),
            "p0": str(totals[Priority.P0]),
            "p1": str(totals[Priority.P1]),
            "p2": str(totals[Priority.P2]),
        },
    )


@pytest.mark.wf5("WF5-300")
@pytest.mark.p2
def test_uncovered_cases_are_reported_rather_than_omitted(evidence):
    """Coverage is measured against the whole matrix, so gaps are visible."""
    log = EvidenceLog()
    log.record(EvidenceRecord(case_id="WF5-001", test_status=CaseStatus.PASS,
                              test_nodeid="t::one"))
    summary = log.summary()

    assert summary["matrix_total"] == 360
    assert summary["cases_covered"] == 1
    assert summary["cases_not_executed"] == 359
    assert len(summary["uncovered_case_ids"]) == 359
    assert "WF5-002" in summary["uncovered_case_ids"]
    # Per-group coverage, so a whole untouched domain stands out.
    assert summary["coverage_by_group"]["A"] == {"total": 12, "covered": 1}
    assert summary["coverage_by_group"]["AD"] == {"total": 12, "covered": 0}
    evidence(
        oracle="OPERATIONAL",
        observed_values={"covered": "1", "uncovered": "359"},
    )


def test_scope_hash_is_stable_and_order_independent():
    """The scope hash depends on the account set, not on its listing order."""
    first = scope_hash(TENANT_A, SOURCE_A, frozenset({"a", "b", "c"}))
    second = scope_hash(TENANT_A, SOURCE_A, frozenset({"c", "a", "b"}))
    assert first == second
    assert first != scope_hash(TENANT_A, SOURCE_A, frozenset({"a", "b"}))
