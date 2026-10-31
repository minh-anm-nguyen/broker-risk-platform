"""Deal classification: which rows are trading P&L and which are something else.

A deal table is not a profit ledger. MetaTrader records cash deposits, broker
credits, daily commission postings, corrections and cancellations in the same
place as trade closes, and each of those carries a monetary amount. Summing the
amount column therefore produces a number that looks like P&L, is auditable,
and is wrong (WF5-041, WF5-042).

So classification is a whitelist with an explicit unknown. Three outcomes:

``TRADING``       counts towards realized trading P&L.
``NON_TRADING``   a real cash movement, reported separately, never as P&L.
``UNCLASSIFIED``  no approved mapping -- blocks the affected scope.

The third outcome is the one that earns its keep. A broker that introduces a new
deal type must not have it silently folded into either of the first two.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum, unique
from typing import Final, Iterable, Mapping

from risk.domain.facts import DealFact
from risk.errors import ContractError, ReasonCode

__all__ = [
    "DealClass",
    "DealCategory",
    "SourceCapability",
    "classify_deal",
    "EligibilityResult",
    "select_eligible_deals",
    "TRADING_ENTRY_KINDS",
    "NON_TRADING_DEAL_TYPES",
    "UNSUPPORTED_DEAL_TYPES",
]


@unique
class DealClass(StrEnum):
    TRADING = "TRADING"
    NON_TRADING = "NON_TRADING"
    UNCLASSIFIED = "UNCLASSIFIED"


@unique
class DealCategory(StrEnum):
    """Reporting bucket for a classified deal."""

    TRADE_RESULT = "TRADE_RESULT"
    BALANCE_OPERATION = "BALANCE_OPERATION"
    CREDIT_OPERATION = "CREDIT_OPERATION"
    FEE_POSTING = "FEE_POSTING"
    CORRECTION = "CORRECTION"
    UNKNOWN = "UNKNOWN"


@unique
class SourceCapability(StrEnum):
    """What the upstream source has *proved* about its deal stream.

    ``SIMULATOR_TRADING_ONLY_V1`` is the only capability under which a deal row
    carrying no ``deal_type`` may be treated as a trade: the simulator fixture
    contract guarantees it emits nothing else. A real Manager SDK feed makes no
    such promise, so the same missing field becomes ``UNCLASSIFIED`` there
    (WF5-012).
    """

    SIMULATOR_TRADING_ONLY_V1 = "SIMULATOR_TRADING_ONLY_V1"
    FULL_DEAL_TAXONOMY_V1 = "FULL_DEAL_TAXONOMY_V1"
    UNKNOWN_SOURCE = "UNKNOWN_SOURCE"


#: Entry kinds that describe a position lifecycle event and may carry trade P&L.
TRADING_ENTRY_KINDS: Final[frozenset[str]] = frozenset({"IN", "OUT", "INOUT", "OUT_BY"})

#: Deal types that move cash without being a trade result.
NON_TRADING_DEAL_TYPES: Final[Mapping[str, DealCategory]] = {
    "DEAL_TYPE_BALANCE": DealCategory.BALANCE_OPERATION,
    "DEAL_TYPE_CREDIT": DealCategory.CREDIT_OPERATION,
    "DEAL_TYPE_CHARGE": DealCategory.FEE_POSTING,
    "DEAL_TYPE_BONUS": DealCategory.BALANCE_OPERATION,
    "DEAL_TYPE_COMMISSION": DealCategory.FEE_POSTING,
    "DEAL_TYPE_COMMISSION_DAILY": DealCategory.FEE_POSTING,
    "DEAL_TYPE_COMMISSION_MONTHLY": DealCategory.FEE_POSTING,
    "DEAL_TYPE_INTEREST": DealCategory.FEE_POSTING,
}

#: Types whose semantics this build refuses to guess at. A correction rewrites
#: a previously reported figure and a cancellation withdraws one; both need an
#: approved adjustment-accounting policy before any number may move
#: (WF5-044, WF5-045).
UNSUPPORTED_DEAL_TYPES: Final[frozenset[str]] = frozenset(
    {
        "DEAL_TYPE_CORRECTION",
        "DEAL_TYPE_BUY_CANCELED",
        "DEAL_TYPE_SELL_CANCELED",
        "DEAL_TYPE_BALANCE_CANCELED",
    }
)

#: Trade deal types, present when the source supplies a full taxonomy.
TRADING_DEAL_TYPES: Final[frozenset[str]] = frozenset({"DEAL_TYPE_BUY", "DEAL_TYPE_SELL"})


@dataclass(frozen=True, slots=True)
class Classification:
    deal_class: DealClass
    category: DealCategory
    reason: ReasonCode | None = None


def classify_deal(deal: DealFact, *, capability: SourceCapability) -> Classification:
    """Decide what a deal row is, under a declared source capability."""
    if deal.entry_kind not in TRADING_ENTRY_KINDS and deal.deal_type is None:
        # An unrecognised entry kind with no type to fall back on: quarantine
        # it rather than inventing a bucket (WF5-010).
        return Classification(
            DealClass.UNCLASSIFIED, DealCategory.UNKNOWN, ReasonCode.ENUM_VERSION_UNMAPPED
        )

    if deal.deal_type is None:
        if capability is SourceCapability.SIMULATOR_TRADING_ONLY_V1:
            return Classification(DealClass.TRADING, DealCategory.TRADE_RESULT)
        return Classification(
            DealClass.UNCLASSIFIED, DealCategory.UNKNOWN, ReasonCode.DEAL_TYPE_UNCLASSIFIED
        )

    if deal.deal_type in UNSUPPORTED_DEAL_TYPES:
        return Classification(
            DealClass.UNCLASSIFIED, DealCategory.CORRECTION, ReasonCode.DEAL_CORRECTION_UNSUPPORTED
        )
    if deal.deal_type in NON_TRADING_DEAL_TYPES:
        return Classification(DealClass.NON_TRADING, NON_TRADING_DEAL_TYPES[deal.deal_type])
    if deal.deal_type in TRADING_DEAL_TYPES:
        if deal.entry_kind not in TRADING_ENTRY_KINDS:
            return Classification(
                DealClass.UNCLASSIFIED, DealCategory.UNKNOWN, ReasonCode.ENUM_VERSION_UNMAPPED
            )
        return Classification(DealClass.TRADING, DealCategory.TRADE_RESULT)

    return Classification(
        DealClass.UNCLASSIFIED, DealCategory.UNKNOWN, ReasonCode.DEAL_TYPE_UNCLASSIFIED
    )


@dataclass(frozen=True)
class EligibilityResult:
    """Outcome of applying scope, window, watermark and classification filters."""

    trading: tuple[DealFact, ...]
    non_trading: tuple[DealFact, ...]
    excluded_out_of_window: int
    excluded_beyond_watermark: int
    unclassified: tuple[tuple[str, ReasonCode], ...]

    @property
    def has_unclassified(self) -> bool:
        return bool(self.unclassified)

    @property
    def trading_deal_ids(self) -> frozenset[str]:
        return frozenset(d.deal_id for d in self.trading)


def select_eligible_deals(
    deals: Iterable[DealFact],
    *,
    capability: SourceCapability,
    period_start: datetime | None,
    period_end: datetime | None,
    deal_watermark_seq: int | None,
) -> EligibilityResult:
    """Partition a deal stream into what may be aggregated and what may not.

    The window is half-open ``[start, end)``. That choice is not cosmetic:
    adjacent daily reports built on closed intervals double-count every deal
    that lands exactly on midnight (WF5-157, WF5-158).

    Duplicate ``deal_id`` values are a hard error rather than a silent
    de-duplication. A reader that delivers the same row twice is broken, and
    quietly compensating for it hides the breakage until the day the duplicate
    carries a *different* amount (WF5-114).
    """
    if period_start is not None and period_end is not None and period_start >= period_end:
        raise ContractError(ReasonCode.INVALID_TIME_WINDOW, "period_start must precede period_end")

    trading: list[DealFact] = []
    non_trading: list[DealFact] = []
    unclassified: list[tuple[str, ReasonCode]] = []
    out_of_window = 0
    beyond_watermark = 0
    seen: dict[str, DealFact] = {}

    for deal in deals:
        previous = seen.get(deal.deal_id)
        if previous is not None:
            raise ContractError(
                ReasonCode.DATA_INTEGRITY_CONFLICT,
                "duplicate deal_id in one captured input set",
                deal_id=deal.deal_id,
            )
        seen[deal.deal_id] = deal

        if deal_watermark_seq is not None and deal.source_seq > deal_watermark_seq:
            # Beyond the captured watermark the set is not provably complete,
            # so those rows belong to the next report, not this one (WF5-218).
            beyond_watermark += 1
            continue
        if period_start is not None and deal.executed_at < period_start:
            out_of_window += 1
            continue
        if period_end is not None and deal.executed_at >= period_end:
            out_of_window += 1
            continue

        classification = classify_deal(deal, capability=capability)
        if classification.deal_class is DealClass.TRADING:
            trading.append(deal)
        elif classification.deal_class is DealClass.NON_TRADING:
            non_trading.append(deal)
        else:
            assert classification.reason is not None
            unclassified.append((deal.deal_id, classification.reason))

    # Deterministic order so that the output digest does not depend on the
    # order the reader happened to deliver rows in (INV05-10).
    key = lambda d: (d.source_seq, d.deal_id)  # noqa: E731
    return EligibilityResult(
        trading=tuple(sorted(trading, key=key)),
        non_trading=tuple(sorted(non_trading, key=key)),
        excluded_out_of_window=out_of_window,
        excluded_beyond_watermark=beyond_watermark,
        unclassified=tuple(sorted(unclassified)),
    )
