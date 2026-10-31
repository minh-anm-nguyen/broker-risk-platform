"""Realized P&L: aggregate the amounts Project 03 preserved from the source.

The one thing this module does not do is recompute profit from prices. The
source already booked an authoritative amount per deal, and
``(close - open) * lots`` disagrees with it the moment partial closes, reversals,
rounding conventions or conversion legs are involved. The price formula appears
only in the test oracle, applied to a fixture where it is known to hold
(section 5.3).

What the module does do is keep the three signed components apart until the
last moment, and label what the "net" figure excludes. ``gross + commission +
swap`` is a real number with a real meaning -- it is simply not an all-in net
while Project 03 carries no ``DEAL_FEE`` column, and saying so is the difference
between an honest report and a plausible one (WF5-139).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from risk.domain.classification import EligibilityResult
from risk.domain.money import Money, MoneySemantic
from risk.errors import ContractError, ReasonCode
from risk.status import FeeCoverage

__all__ = [
    "RealizedResult",
    "NonTradingSummary",
    "compute_realized",
]


@dataclass(frozen=True, slots=True)
class NonTradingSummary:
    """Cash movements that are real but are not trading results.

    Reported next to the trading figures, never inside them. A deposit of
    +1 000 shows up here so that an operator can see it, and does not inflate
    the P&L line by a cent (WF5-041, WF5-118).
    """

    category_totals: dict[str, Money]
    deal_count: int

    def as_payload(self) -> dict[str, object]:
        return {
            "deal_count": self.deal_count,
            "category_totals": {
                category: {"amount": money.canonical(), "currency": money.currency}
                for category, money in sorted(self.category_totals.items())
            },
        }


@dataclass(frozen=True)
class RealizedResult:
    """Signed realized components plus a precise coverage disclosure."""

    currency: str
    gross: Money
    commission: Money
    swap: Money
    net_excluding_fee: Money
    deal_count: int
    deal_ids: frozenset[str]
    fee_coverage: FeeCoverage
    non_trading: NonTradingSummary
    excluded_out_of_window: int = 0
    excluded_beyond_watermark: int = 0

    def as_payload(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "currency": self.currency,
            "gross": self.gross.canonical(),
            "commission": self.commission.canonical(),
            "swap": self.swap.canonical(),
            "net_excluding_fee": self.net_excluding_fee.canonical(),
            "deal_count": self.deal_count,
            "fee_coverage": str(self.fee_coverage),
            "excluded_out_of_window": self.excluded_out_of_window,
            "excluded_beyond_watermark": self.excluded_beyond_watermark,
            "non_trading": self.non_trading.as_payload(),
        }
        # Deliberately absent: any key named ``net`` or ``net_all_in``. A
        # consumer must not be able to reach a total that implies fee coverage
        # this build cannot prove (WF5-359).
        return payload


def compute_realized(
    eligibility: EligibilityResult,
    *,
    currency: str,
    fee_fields_available: bool = False,
) -> RealizedResult:
    """Sum the eligible deal amounts, preserving sign and separation.

    Args:
        eligibility: Already filtered by scope, window, watermark and class.
        currency: The reporting currency every deal must already be in.
        fee_fields_available: Whether the upstream schema carries fee columns.
            ``False`` -- the current Project 03 shape -- forces
            ``fee_coverage = NOT_AVAILABLE``.

    Raises:
        ContractError: if an unclassified deal reached the aggregator, or if the
            deals disagree with ``currency``.

    Note:
        Addition happens on :class:`~risk.domain.money.Money`, so a deal in a
        different currency raises rather than contributing a wrong magnitude.
        No value is rounded here; rounding is applied once, at the reporting
        boundary, after the sum (WF5-087, WF5-137).
    """
    if eligibility.has_unclassified:
        # Reaching the aggregator with unmapped rows would mean the caller
        # decided to ignore them. Only the gate may make that decision, and it
        # may only make it by blocking (WF5-010).
        first_reason = eligibility.unclassified[0][1]
        raise ContractError(
            first_reason,
            "unclassified deals must be resolved before aggregation",
            unclassified_count=len(eligibility.unclassified),
        )

    gross = Money.zero(currency, MoneySemantic.REALIZED_GROSS)
    commission = Money.zero(currency, MoneySemantic.COMMISSION)
    swap = Money.zero(currency, MoneySemantic.SWAP)

    for deal in eligibility.trading:
        if deal.currency != currency:
            raise ContractError(
                ReasonCode.CURRENCY_MISMATCH,
                "deal currency differs from the reporting currency",
                deal_currency=deal.currency,
                reporting_currency=currency,
            )
        gross = gross + deal.gross_profit
        commission = commission + deal.commission
        swap = swap + deal.swap

    # The one place the three components are combined. The result is relabelled
    # explicitly so no caller can mistake it for a gross or a fee total.
    net_amount: Decimal = gross.amount + commission.amount + swap.amount
    net = Money(net_amount, currency, MoneySemantic.NET_EXCLUDING_FEE)

    category_totals: dict[str, Money] = {}
    from risk.domain.classification import SourceCapability, classify_deal

    for deal in eligibility.non_trading:
        if deal.currency != currency:
            raise ContractError(
                ReasonCode.CURRENCY_MISMATCH,
                "non-trading deal currency differs from the reporting currency",
                deal_currency=deal.currency,
            )
        category = str(
            classify_deal(deal, capability=SourceCapability.FULL_DEAL_TAXONOMY_V1).category
        )
        bucket = category_totals.get(category)
        # Non-trading rows carry their amount in ``gross_profit``; relabel to a
        # neutral semantic so it can never be added to a trading gross.
        contribution = deal.gross_profit.reinterpret(MoneySemantic.FEE)
        category_totals[category] = contribution if bucket is None else bucket + contribution

    coverage = FeeCoverage.COMPLETE if fee_fields_available else FeeCoverage.NOT_AVAILABLE

    return RealizedResult(
        currency=currency,
        gross=gross,
        commission=commission,
        swap=swap,
        net_excluding_fee=net,
        deal_count=len(eligibility.trading),
        deal_ids=eligibility.trading_deal_ids,
        fee_coverage=coverage,
        non_trading=NonTradingSummary(
            category_totals=category_totals, deal_count=len(eligibility.non_trading)
        ),
        excluded_out_of_window=eligibility.excluded_out_of_window,
        excluded_beyond_watermark=eligibility.excluded_beyond_watermark,
    )
