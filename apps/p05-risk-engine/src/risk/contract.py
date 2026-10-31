"""The library-level contract Project 06 will wrap in HTTP.

This module is the whole public surface. Project 06 adds authentication, HTTP
status mapping and serialization; it adds no business rules, and it is not
permitted to reach past this contract into the domain.

Response shape decisions that exist to stop a consumer from going wrong:

* Money is a decimal **string**. A JSON number would be parsed into a float by
  every client in existence, and ``0.1 + 0.2`` would reappear on the far side
  of a wire that carried exact values (WF5-351).
* A component that is BLOCKED or UNSUPPORTED has **no** value keys at all --
  not ``null``, not ``0``. A missing key is hard to render as a zero by
  accident; a ``null`` is one ``|| 0`` away from it (WF5-352).
* ``composite_total`` is ``None`` unless the cuts were provably aligned. There
  is no second, laxer total for callers who would rather have a number.
* There is no field named ``net`` or ``net_all_in``. The only net is
  ``net_excluding_fee``, named after what it excludes (WF5-359).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Sequence

from risk.domain.exposure import ExposureResult
from risk.domain.money import Money
from risk.domain.realized import RealizedResult
from risk.domain.unrealized import UnrealizedResult
from risk.errors import ContractError, ReasonCode
from risk.status import Component, ComponentStatus, ExactnessClass, GlobalStatus

__all__ = [
    "RiskRequest",
    "ComponentPayload",
    "RiskResult",
    "parse_components",
]


def parse_components(raw: Sequence[str] | None) -> frozenset[Component]:
    """Validate the requested component list."""
    if raw is None:
        return frozenset(Component)
    components: set[Component] = set()
    for name in raw:
        try:
            components.add(Component(name))
        except ValueError:
            raise ContractError(
                ReasonCode.CONTRACT_MISSING_FIELD, "unknown component requested", component=str(name)
            ) from None
    if not components:
        raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "at least one component is required")
    return frozenset(components)


@dataclass(frozen=True)
class RiskRequest:
    """What a trusted caller asks for.

    ``account_ids`` is a *filter*, not an authorization. The authorization
    arrives separately, as an :class:`~risk.domain.facts.AuthorizedScope`, and
    the filter can only narrow it. That separation is what makes a tampered
    request body harmless (section 7.1, WF5-195).
    """

    epoch_id: str
    components: frozenset[Component]
    account_ids: tuple[str, ...] | None = None
    period_start: datetime | None = None
    period_end: datetime | None = None
    valuation_as_of: datetime | None = None
    quote_snapshot_id: str | None = None
    strict_composite: bool = True
    risk_policy_version: str = "risk-policy-v1"
    symbol_catalog_version: str = "sim-usd-fx-v1"
    include_quote_notional: bool = False
    historical: bool = False

    def __post_init__(self) -> None:
        if not self.epoch_id:
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "epoch_id is required")
        if not self.components:
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "components is required")
        if self.period_start is not None and self.period_end is not None:
            if self.period_start >= self.period_end:
                raise ContractError(
                    ReasonCode.INVALID_TIME_WINDOW, "period_start must be strictly before period_end"
                )
        for name in ("period_start", "period_end", "valuation_as_of"):
            value = getattr(self, name)
            if value is not None and value.tzinfo is None:
                raise ContractError(
                    ReasonCode.NAIVE_TIMESTAMP_REJECTED, "request timestamps need an offset", field=name
                )
        if Component.REALIZED in self.components and (
            self.period_start is None or self.period_end is None
        ):
            # An unbounded realized window would silently mean "everything the
            # watermark covers", which is not what a caller asking for a month
            # expects to get.
            raise ContractError(
                ReasonCode.INVALID_TIME_WINDOW, "realized requires an explicit period window"
            )

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> RiskRequest:
        """Build a request from an untrusted mapping (the P06 boundary)."""
        from risk.domain.facts import parse_utc

        def optional_time(key: str) -> datetime | None:
            value = payload.get(key)
            return None if value is None else parse_utc(value, field_name=key)

        account_ids = payload.get("account_ids")
        if account_ids is not None:
            if not isinstance(account_ids, (list, tuple)):
                raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "account_ids must be a list")
            account_ids = tuple(str(a) for a in account_ids)

        return cls(
            epoch_id=str(payload.get("epoch_id", "")),
            components=parse_components(payload.get("components")),
            account_ids=account_ids,
            period_start=optional_time("period_start_utc"),
            period_end=optional_time("period_end_utc"),
            valuation_as_of=optional_time("valuation_as_of_utc"),
            quote_snapshot_id=(
                None if payload.get("quote_snapshot_id") is None else str(payload["quote_snapshot_id"])
            ),
            strict_composite=bool(payload.get("strict_composite", True)),
            risk_policy_version=str(payload.get("risk_policy_version", "risk-policy-v1")),
            symbol_catalog_version=str(payload.get("symbol_catalog_version", "sim-usd-fx-v1")),
            include_quote_notional=bool(payload.get("include_quote_notional", False)),
            historical=bool(payload.get("historical", False)),
        )


@dataclass(frozen=True)
class ComponentPayload:
    """One component's status, reasons and -- only if usable -- its values."""

    status: ComponentStatus
    reason_codes: tuple[str, ...] = ()
    values: dict[str, object] | None = None

    def as_payload(self) -> dict[str, object]:
        out: dict[str, object] = {"status": str(self.status)}
        if self.reason_codes:
            out["reason_codes"] = list(self.reason_codes)
        if self.values is not None and self.status.has_values:
            out.update(self.values)
        return out


@dataclass(frozen=True)
class RiskResult:
    """The envelope returned to the caller."""

    calculation_id: str
    status: GlobalStatus
    reason_codes: tuple[str, ...]
    currency: str | None
    exactness: ExactnessClass
    scope: dict[str, object]
    cuts: dict[str, object]
    components: dict[Component, ComponentPayload]
    composite_total: dict[str, object] | None
    input_digest: str
    output_digest: str
    risk_policy_version: str
    symbol_catalog_version: str
    duration_ms: int = 0
    audit_receipt_id: str | None = None
    lineage: dict[str, object] = field(default_factory=dict)

    def component(self, component: Component) -> ComponentPayload:
        return self.components[component]

    def as_payload(self) -> dict[str, object]:
        return {
            "calculation_id": self.calculation_id,
            "status": str(self.status),
            "reason_codes": list(self.reason_codes),
            "currency": self.currency,
            "exactness": str(self.exactness),
            "scope": self.scope,
            "cuts": self.cuts,
            "components": {
                str(name): payload.as_payload() for name, payload in sorted(self.components.items())
            },
            "composite_total": self.composite_total,
            "input_digest": self.input_digest,
            "output_digest": self.output_digest,
            "risk_policy_version": self.risk_policy_version,
            "symbol_catalog_version": self.symbol_catalog_version,
            "duration_ms": self.duration_ms,
            "audit_receipt_id": self.audit_receipt_id,
        }


def exposure_payload(result: ExposureResult) -> dict[str, object]:
    return result.as_payload()


def realized_payload(result: RealizedResult, *, rounding: str) -> dict[str, object]:
    """Render realized figures, quantized exactly once.

    The exact values are kept alongside the display values. A consumer that
    needs to re-add the components must use the exact ones; the display values
    exist for presentation and are each rounded from the full-precision sum,
    never from an already-rounded part (INV05-15).
    """
    payload = result.as_payload()
    payload["display"] = {
        "gross": str(result.gross.display(rounding)),
        "commission": str(result.commission.display(rounding)),
        "swap": str(result.swap.display(rounding)),
        "net_excluding_fee": str(result.net_excluding_fee.display(rounding)),
    }
    return payload


def unrealized_payload(result: UnrealizedResult, *, rounding: str) -> dict[str, object]:
    payload = result.as_payload()
    payload["display"] = {"total": str(result.total.display(rounding))}
    return payload


def composite_payload(
    realized: RealizedResult | None,
    unrealized: UnrealizedResult | None,
    *,
    rounding: str,
) -> dict[str, object] | None:
    """Add realized and unrealized, only when both are present and aligned.

    The caller of this function has already established that the cuts are
    provably compatible. The addition itself is still guarded: the two
    :class:`Money` values must agree on currency or the sum raises rather than
    producing a mixed-currency scalar (INV05-08).
    """
    if realized is None or unrealized is None:
        return None
    total: Money = Money(
        realized.net_excluding_fee.amount + unrealized.total.amount,
        realized.currency,
        realized.net_excluding_fee.semantic,
    )
    if realized.currency != unrealized.currency:
        raise ContractError(
            ReasonCode.CURRENCY_MISMATCH,
            "composite total would mix currencies",
            realized=realized.currency,
            unrealized=unrealized.currency,
        )
    return {
        "currency": realized.currency,
        "realized_net_excluding_fee": realized.net_excluding_fee.canonical(),
        "unrealized_gross": unrealized.total.canonical(),
        "total_excluding_fee": total.canonical(),
        "display_total_excluding_fee": str(total.display(rounding)),
        "fee_coverage": str(realized.fee_coverage),
    }

