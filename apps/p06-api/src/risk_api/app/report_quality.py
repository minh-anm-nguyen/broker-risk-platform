"""The output contract guard and the status-to-HTTP decision.

This is where "HTTP success is not the correctness oracle" is enforced. Two
separate jobs:

``validate_engine_result``
    Refuse to serialize a result that fails its own contract. A missing cut,
    a wrong currency, an absent digest or a non-finite amount produces
    ``502 INVALID_RISK_RESULT`` and no money in the body. It is better to
    serve nothing than to serve an unverifiable number.

``decide_outcome``
    Map the engine's quality status onto HTTP. ``DEGRADED`` only becomes a
    ``200`` when the caller explicitly opted in, and then only with
    ``composite_total`` absent. A strict caller gets a typed ``409``.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal, Sequence

from risk_engine import RiskResult

from ..contracts.problem_details import ApiProblem, public_code_for_reasons

__all__ = [
    "ExpectedResultShape",
    "ResultOutcome",
    "validate_engine_result",
    "decide_outcome",
    "reason_family",
]

_ALLOWED_STATUSES = frozenset({"READY", "DEGRADED", "BLOCKED", "UNSUPPORTED", "INCONCLUSIVE"})
_DIGEST_PREFIX = "sha256:"


@dataclass(frozen=True, slots=True)
class ExpectedResultShape:
    """What the service asked for, to compare against what came back."""

    source_id: str
    epoch_id: str
    accounts: tuple[str, ...]
    components: tuple[str, ...]
    currency: str
    risk_policy_version: str
    symbol_catalog_version: str
    position_scan_id: str | None
    account_scan_id: str | None
    deal_watermark_seq: int | None
    quote_snapshot_id: str | None


@dataclass(frozen=True, slots=True)
class ResultOutcome:
    """The decision: emit a body, or raise a problem."""

    kind: Literal["emit", "refuse"]
    http_status: int
    partial: bool


def _invalid(detail: str) -> ApiProblem:
    """A contract violation by the upstream engine. No amounts are disclosed."""
    return ApiProblem(
        "INVALID_RISK_RESULT",
        f"the upstream risk result failed its contract: {detail}",
        reason_codes=("INVALID_RISK_RESULT",),
    )


def _check_amount(value: object, *, field: str) -> None:
    if value is None:
        return
    if isinstance(value, bool) or isinstance(value, float):
        raise _invalid(f"{field} is not a Decimal")
    if not isinstance(value, Decimal):
        raise _invalid(f"{field} is not a Decimal")
    if not value.is_finite():
        raise _invalid(f"{field} is not finite")


def validate_engine_result(result: RiskResult, expected: ExpectedResultShape) -> None:
    """Verify the engine honoured the contract this request was built on."""
    if result.status not in _ALLOWED_STATUSES:
        raise _invalid("unknown result status")

    if result.currency != expected.currency:
        raise _invalid("reporting currency does not match the requested profile")
    if result.risk_policy_version != expected.risk_policy_version:
        raise _invalid("risk policy version does not match the pinned version")
    if result.symbol_catalog_version != expected.symbol_catalog_version:
        raise _invalid("symbol catalog version does not match the pinned version")

    scope_accounts = tuple(result.scope.get("account_ids", ()))  # type: ignore[arg-type]
    if tuple(sorted(scope_accounts)) != tuple(sorted(expected.accounts)):
        # A result scoped to accounts we did not authorize must never be sent.
        raise _invalid("result scope does not match the authorized account set")
    if result.scope.get("source_id") != expected.source_id:
        raise _invalid("result scope names a different source")
    if result.scope.get("epoch_id") != expected.epoch_id:
        raise _invalid("result scope names a different epoch")

    cuts = result.cuts
    if cuts.source_id != expected.source_id or cuts.epoch_id != expected.epoch_id:
        raise _invalid("result cut does not match the captured source epoch")

    for digest, name in ((result.input_digest, "input_digest"), (result.output_digest, "output_digest")):
        if not isinstance(digest, str) or not digest.startswith(_DIGEST_PREFIX) or len(digest) < 24:
            raise _invalid(f"{name} is missing or malformed")

    _validate_components(result, expected)

    if result.composite_total is not None:
        _check_amount(result.composite_total, field="composite_total")
        if result.status != "READY":
            raise _invalid("a composite total was returned for a non-READY result")
        if cuts.consistency != "COMPATIBLE":
            raise _invalid("a composite total was returned without a compatible cut")


def _validate_components(result: RiskResult, expected: ExpectedResultShape) -> None:
    requested = frozenset(expected.components)
    present = {
        "EXPOSURE": result.exposure is not None,
        "REALIZED": result.realized is not None,
        "UNREALIZED": result.unrealized is not None,
    }
    for name, is_present in present.items():
        if (name in requested) != is_present:
            raise _invalid(f"component set does not match the request ({name})")

    if result.exposure is not None and result.exposure.status == "READY":
        if expected.position_scan_id is not None and result.cuts.position_scan_id != expected.position_scan_id:
            raise _invalid("exposure was proven against a different position scan")
        for symbol, bucket in result.exposure.by_symbol.items():
            if not symbol:
                raise _invalid("exposure carries an empty symbol key")
            if not bucket.unit_currency:
                raise _invalid("exposure bucket has no unit currency")
            for field, value in (
                ("long_units", bucket.long_units),
                ("short_units", bucket.short_units),
                ("gross_units", bucket.gross_units),
                ("net_units", bucket.net_units),
            ):
                _check_amount(value, field=f"exposure.{symbol}.{field}")
            if bucket.gross_units != bucket.long_units + bucket.short_units:
                raise _invalid("exposure gross units are not the sum of the sides")
            if bucket.net_units != bucket.long_units - bucket.short_units:
                raise _invalid("exposure net units are not the difference of the sides")

    realized = result.realized
    if realized is not None and realized.status == "READY":
        if realized.currency != expected.currency:
            raise _invalid("realized component reports a different currency")
        if realized.fee_coverage not in ("COVERED", "NOT_AVAILABLE"):
            raise _invalid("realized component has no fee coverage declaration")
        for field, value in (
            ("gross", realized.gross),
            ("commission", realized.commission),
            ("swap", realized.swap),
            ("net_excluding_fee", realized.net_excluding_fee),
        ):
            _check_amount(value, field=f"realized.{field}")
        if None in (realized.gross, realized.commission, realized.swap, realized.net_excluding_fee):
            raise _invalid("a READY realized component is missing an amount")
        expected_net = realized.gross + realized.commission + realized.swap  # type: ignore[operator]
        if realized.net_excluding_fee != expected_net:
            # The components must actually add up; signed fees are not abs().
            raise _invalid("realized net does not equal gross plus signed fees")
        if expected.deal_watermark_seq is not None and result.cuts.deal_watermark_seq is None:
            raise _invalid("a READY realized component carries no deal watermark")

    unrealized = result.unrealized
    if unrealized is not None and unrealized.status == "READY":
        if unrealized.currency != expected.currency:
            raise _invalid("unrealized component reports a different currency")
        _check_amount(unrealized.total, field="unrealized.total")
        if unrealized.total is None:
            raise _invalid("a READY unrealized component has no total")
        if unrealized.quote_snapshot_id is None or result.cuts.quote_snapshot_id is None:
            raise _invalid("a READY unrealized component carries no quote snapshot")
        if expected.quote_snapshot_id is not None and (
            unrealized.quote_snapshot_id != expected.quote_snapshot_id
        ):
            raise _invalid("unrealized was valued against a different quote snapshot")
        for symbol, amount in unrealized.by_symbol.items():
            _check_amount(amount, field=f"unrealized.{symbol}")


def decide_outcome(result: RiskResult, *, allow_partial: bool) -> ResultOutcome:
    """Map a validated result status onto an HTTP outcome."""
    if result.status == "READY":
        return ResultOutcome(kind="emit", http_status=200, partial=False)

    if result.status == "DEGRADED":
        if not allow_partial:
            # Strict callers never receive an incomplete report as a success.
            raise ApiProblem(
                "COMPOSITE_UNPROVEN",
                "required financial components cannot be proven at a compatible data cut",
                reason_codes=result.reason_codes,
            )
        if result.composite_total is not None:
            raise _invalid("a degraded result must not carry a composite total")
        return ResultOutcome(kind="emit", http_status=200, partial=True)

    if result.status == "UNSUPPORTED":
        raise ApiProblem(
            "UNSUPPORTED_CALC_MODE",
            "the requested calculation profile is not supported for this data",
            reason_codes=result.reason_codes,
        )

    if result.status == "INCONCLUSIVE":
        raise ApiProblem(
            "DATA_NOT_READY",
            "the calculation could not reach a conclusion for this cut",
            reason_codes=result.reason_codes,
        )

    # BLOCKED: pick the most specific public code the reasons justify.
    raise ApiProblem(
        public_code_for_reasons(result.reason_codes),
        "the report cannot be produced for the current data cut",
        reason_codes=result.reason_codes,
    )


def reason_family(reasons: Sequence[str]) -> str:
    """A low-cardinality family label for metrics."""
    if not reasons:
        return "none"
    first = sorted(reasons)[0]
    if first.startswith("QUOTE") or first == "STALE_MARK":
        return "quote"
    if first.startswith("DEAL") or first == "P03_UNPUBLISHED":
        return "projection"
    if first.startswith("RECON"):
        return "reconciliation"
    if first.startswith("FEE") or first == "MONEY_FIELD_MISSING":
        return "coverage"
    if first in ("INCOMPATIBLE_CUT", "EPOCH_CHANGED", "POLICY_VERSION_CHANGED"):
        return "cut"
    if first in ("UNSUPPORTED_CALC_MODE", "FX_RATE_MISSING"):
        return "profile"
    return "other"
