"""The output contract guard and the status-to-HTTP decision.

WF06-E and WF06-P. Every case builds a deliberately broken result and checks
the guard refuses it with ``502`` and no amounts, plus the strict/partial
decision table from design doc section 2.2.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from risk_api.app.report_quality import (
    ExpectedResultShape,
    decide_outcome,
    reason_family,
    validate_engine_result,
)
from risk_api.contracts.problem_details import ApiProblem
from risk_engine import (
    ExposureComponent,
    RealizedComponent,
    RiskCut,
    RiskResult,
    SymbolExposure,
    UnrealizedComponent,
)

pytestmark = pytest.mark.unit

CUT = RiskCut(
    source_id="sim-broker-a",
    epoch_id="E1",
    account_scan_id="scan-A",
    position_scan_id="scan-P",
    deal_watermark_seq=1050,
    quote_snapshot_id="quote-S1030",
    consistency="COMPATIBLE",
)

EXPECTED = ExpectedResultShape(
    source_id="sim-broker-a",
    epoch_id="E1",
    accounts=("00042",),
    components=("EXPOSURE", "REALIZED", "UNREALIZED"),
    currency="USD",
    risk_policy_version="risk-policy-v1",
    symbol_catalog_version="sim-usd-fx-v1",
    position_scan_id="scan-P",
    account_scan_id="scan-A",
    deal_watermark_seq=1050,
    quote_snapshot_id="quote-S1030",
)

DIGEST = "sha256:" + "a" * 64


def good_result(**overrides) -> RiskResult:
    base = {
        "calculation_id": "calc-1",
        "status": "READY",
        "reason_codes": ("FEE_COVERAGE_UNAVAILABLE",),
        "currency": "USD",
        "risk_policy_version": "risk-policy-v1",
        "symbol_catalog_version": "sim-usd-fx-v1",
        "scope": {"source_id": "sim-broker-a", "epoch_id": "E1", "account_ids": ["00042"]},
        "cuts": CUT,
        "exposure": ExposureComponent(
            status="READY",
            by_symbol={
                "EURUSD": SymbolExposure(
                    unit_currency="EUR",
                    long_units=Decimal("50000"),
                    short_units=Decimal("0"),
                    gross_units=Decimal("50000"),
                    net_units=Decimal("50000"),
                )
            },
            positions_counted=1,
        ),
        "realized": RealizedComponent(
            status="READY",
            currency="USD",
            gross=Decimal("60.00"),
            commission=Decimal("-3.50"),
            swap=Decimal("-0.50"),
            net_excluding_fee=Decimal("56.00"),
            fee_coverage="NOT_AVAILABLE",
            deals_counted=1,
        ),
        "unrealized": UnrealizedComponent(
            status="READY",
            currency="USD",
            total=Decimal("25.00"),
            by_symbol={"EURUSD": Decimal("25.00")},
            quote_snapshot_id="quote-S1030",
        ),
        "composite_total": Decimal("81.00"),
        "input_digest": DIGEST,
        "output_digest": DIGEST,
        "strict_composite": True,
    }
    base.update(overrides)
    return RiskResult(**base)  # type: ignore[arg-type]


class TestAcceptsAWellFormedResult:
    def test_the_golden_result_passes(self):
        validate_engine_result(good_result(), EXPECTED)

    def test_a_subset_of_components_passes_when_requested(self):
        expected = replace(EXPECTED, components=("EXPOSURE",))
        result = good_result(
            realized=None, unrealized=None, composite_total=None, reason_codes=()
        )
        validate_engine_result(result, expected)


class TestRefusesBrokenResults:
    def _assert_refused(self, result: RiskResult, expected=EXPECTED) -> ApiProblem:
        with pytest.raises(ApiProblem) as caught:
            validate_engine_result(result, expected)
        assert caught.value.code == "INVALID_RISK_RESULT"
        assert caught.value.status == 502
        return caught.value

    def test_a_wrong_currency_is_refused(self):
        """WF06-E-02: a currency the request did not ask for is not negotiable."""
        self._assert_refused(good_result(currency="XXX"))

    def test_a_missing_input_digest_is_refused(self):
        self._assert_refused(good_result(input_digest=""))

    def test_a_malformed_digest_is_refused(self):
        self._assert_refused(good_result(output_digest="not-a-digest"))

    def test_a_dropped_position_scan_is_refused(self):
        self._assert_refused(good_result(cuts=replace(CUT, position_scan_id=None)))

    def test_a_cut_for_another_epoch_is_refused(self):
        self._assert_refused(good_result(cuts=replace(CUT, epoch_id="E2")))

    def test_a_scope_wider_than_the_authorized_set_is_refused(self):
        """INV06-01: a result naming an unauthorized account must never be sent."""
        self._assert_refused(
            good_result(
                scope={
                    "source_id": "sim-broker-a",
                    "epoch_id": "E1",
                    "account_ids": ["00042", "00043"],
                }
            )
        )

    def test_a_scope_for_another_source_is_refused(self):
        self._assert_refused(
            good_result(
                scope={"source_id": "other-broker", "epoch_id": "E1", "account_ids": ["00042"]}
            )
        )

    def test_a_non_finite_amount_is_refused(self):
        realized = replace(good_result().realized, net_excluding_fee=Decimal("NaN"))
        self._assert_refused(good_result(realized=realized))

    def test_a_float_amount_is_refused(self):
        realized = replace(good_result().realized, net_excluding_fee=56.0)  # type: ignore[arg-type]
        self._assert_refused(good_result(realized=realized))

    def test_realized_components_that_do_not_add_up_are_refused(self):
        """A net that is not gross plus signed fees is silently wrong money."""
        realized = replace(good_result().realized, net_excluding_fee=Decimal("63.50"))
        problem = self._assert_refused(good_result(realized=realized))
        assert "net does not equal gross" in problem.detail

    def test_missing_fee_coverage_is_refused(self):
        realized = replace(good_result().realized, fee_coverage=None)
        self._assert_refused(good_result(realized=realized))

    def test_a_ready_realized_component_without_amounts_is_refused(self):
        realized = replace(good_result().realized, gross=None)
        self._assert_refused(good_result(realized=realized))

    def test_exposure_sides_that_do_not_reconcile_are_refused(self):
        broken = ExposureComponent(
            status="READY",
            by_symbol={
                "EURUSD": SymbolExposure(
                    unit_currency="EUR",
                    long_units=Decimal("50000"),
                    short_units=Decimal("0"),
                    gross_units=Decimal("99999"),
                    net_units=Decimal("50000"),
                )
            },
        )
        self._assert_refused(good_result(exposure=broken))

    def test_an_exposure_bucket_without_a_unit_currency_is_refused(self):
        broken = ExposureComponent(
            status="READY",
            by_symbol={
                "EURUSD": SymbolExposure(
                    unit_currency="",
                    long_units=Decimal("0"),
                    short_units=Decimal("0"),
                    gross_units=Decimal("0"),
                    net_units=Decimal("0"),
                )
            },
        )
        self._assert_refused(good_result(exposure=broken))

    def test_unrealized_without_a_quote_snapshot_is_refused(self):
        unrealized = replace(good_result().unrealized, quote_snapshot_id=None)
        self._assert_refused(good_result(unrealized=unrealized))

    def test_unrealized_valued_against_another_snapshot_is_refused(self):
        unrealized = replace(good_result().unrealized, quote_snapshot_id="quote-S9999")
        self._assert_refused(good_result(unrealized=unrealized))

    def test_a_component_the_request_did_not_ask_for_is_refused(self):
        expected = replace(EXPECTED, components=("EXPOSURE",))
        self._assert_refused(good_result(), expected)

    def test_a_requested_component_that_is_absent_is_refused(self):
        self._assert_refused(good_result(unrealized=None))

    def test_a_composite_total_on_a_non_ready_result_is_refused(self):
        self._assert_refused(good_result(status="DEGRADED"))

    def test_a_composite_total_without_a_compatible_cut_is_refused(self):
        self._assert_refused(good_result(cuts=replace(CUT, consistency="INDEPENDENT")))

    def test_an_unknown_status_is_refused(self):
        self._assert_refused(good_result(status="PROBABLY_FINE"))

    def test_a_policy_version_mismatch_is_refused(self):
        self._assert_refused(good_result(risk_policy_version="risk-policy-v2"))

    def test_a_catalog_version_mismatch_is_refused(self):
        self._assert_refused(good_result(symbol_catalog_version="other-catalog"))

    def test_no_amounts_appear_in_the_refusal_detail(self):
        realized = replace(good_result().realized, net_excluding_fee=Decimal("99999.99"))
        problem = self._assert_refused(good_result(realized=realized))
        assert "99999" not in problem.detail


class TestOutcomeDecision:
    def test_ready_emits_a_200(self):
        outcome = decide_outcome(good_result(), allow_partial=False)
        assert (outcome.kind, outcome.http_status, outcome.partial) == ("emit", 200, False)

    def test_degraded_with_opt_in_emits_a_200_partial(self):
        result = good_result(status="DEGRADED", composite_total=None)
        outcome = decide_outcome(result, allow_partial=True)
        assert (outcome.http_status, outcome.partial) == (200, True)

    def test_degraded_without_opt_in_is_a_409(self):
        """IT06-03: a strict caller never receives an incomplete report as success."""
        result = good_result(status="DEGRADED", composite_total=None)
        with pytest.raises(ApiProblem) as caught:
            decide_outcome(result, allow_partial=False)
        assert caught.value.code == "COMPOSITE_UNPROVEN"
        assert caught.value.status == 409

    def test_a_degraded_result_carrying_a_total_is_a_contract_violation(self):
        result = good_result(status="DEGRADED")
        with pytest.raises(ApiProblem) as caught:
            decide_outcome(result, allow_partial=True)
        assert caught.value.code == "INVALID_RISK_RESULT"

    @pytest.mark.parametrize(
        ("reasons", "expected_code"),
        [
            (("RECON_BLOCKING",), "DATA_NOT_READY"),
            (("P03_UNPUBLISHED",), "DATA_NOT_READY"),
            (("QUOTE_CUT_UNPROVEN",), "COMPOSITE_UNPROVEN"),
            (("EPOCH_CHANGED",), "EPOCH_CHANGED"),
            (("DEAL_WATERMARK_UNPROVEN",), "DATA_NOT_READY"),
        ],
    )
    def test_blocked_maps_through_its_reason(self, reasons, expected_code):
        result = good_result(status="BLOCKED", reason_codes=reasons, composite_total=None)
        with pytest.raises(ApiProblem) as caught:
            decide_outcome(result, allow_partial=True)
        assert caught.value.code == expected_code
        assert caught.value.reason_codes == reasons

    def test_unsupported_is_a_422(self):
        result = good_result(
            status="UNSUPPORTED", reason_codes=("UNSUPPORTED_CALC_MODE",), composite_total=None
        )
        with pytest.raises(ApiProblem) as caught:
            decide_outcome(result, allow_partial=True)
        assert caught.value.code == "UNSUPPORTED_CALC_MODE"
        assert caught.value.status == 422

    def test_inconclusive_is_a_409(self):
        result = good_result(status="INCONCLUSIVE", reason_codes=(), composite_total=None)
        with pytest.raises(ApiProblem) as caught:
            decide_outcome(result, allow_partial=True)
        assert caught.value.code == "DATA_NOT_READY"


@pytest.mark.parametrize(
    ("reasons", "family"),
    [
        ((), "none"),
        (("QUOTE_CUT_UNPROVEN",), "quote"),
        (("STALE_MARK",), "quote"),
        (("DEAL_WATERMARK_UNPROVEN",), "projection"),
        (("RECON_BLOCKING",), "reconciliation"),
        (("FEE_COVERAGE_UNAVAILABLE",), "coverage"),
        (("INCOMPATIBLE_CUT",), "cut"),
        (("UNSUPPORTED_CALC_MODE",), "profile"),
        (("SOMETHING_NEW",), "other"),
    ],
)
def test_reason_family_stays_low_cardinality(reasons, family):
    assert reason_family(reasons) == family
