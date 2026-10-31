"""The P05 accounting boundary: what P08 is permitted to add up. Section 7.

P08 owns no financial semantics. It may group, count and sum amounts that P05
has already declared comparable, and it must refuse everything else. The two
refusals that matter most:

* **Net P&L without full fee coverage.** ``gross + commission + swap`` looks
  like a net figure and is not one when other fees exist. Section 7.1 requires
  it to be labelled by coverage or withheld, so :class:`FeeCoverage` decides
  whether a net is emitted at all (INV08-15).
* **FX conversion.** Converting EUR into USD needs a versioned rate set with
  an as-of time and a stated conversion side. None of that exists in P0, so
  conversion raises instead of guessing (FR08-12, P08-M).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import StrEnum
from typing import Mapping

from ..errors import AnalyticsError, Reason
from ..money import checked_sum, parse_currency


class PolicyError(AnalyticsError):
    """A metric was requested that the approved policy does not cover."""

    status_code = 422


class FeeCoverage(StrEnum):
    """How much of the true cost base the fee fields actually represent.

    ``COMMISSION_AND_SWAP_ONLY`` is the honest MVP answer: those two fields
    are present and trustworthy, and anything else that a broker charges is
    not modelled. ``UNKNOWN`` exists so that a missing declaration is loud
    rather than being quietly treated as "complete".
    """

    COMMISSION_AND_SWAP_ONLY = "COMMISSION_AND_SWAP_ONLY"
    ALL_FEES_INCLUDED = "ALL_FEES_INCLUDED"
    GROSS_ONLY = "GROSS_ONLY"
    UNKNOWN = "UNKNOWN"

    @property
    def permits_full_net(self) -> bool:
        """Only a complete cost base may be reported as an economic net."""
        return self is FeeCoverage.ALL_FEES_INCLUDED

    @property
    def permits_partial_net(self) -> bool:
        """A components-covered subtotal, which must be labelled as such."""
        return self in (FeeCoverage.ALL_FEES_INCLUDED, FeeCoverage.COMMISSION_AND_SWAP_ONLY)


@dataclass(frozen=True, slots=True)
class SymbolSpec:
    """Minimum instrument metadata P08 needs to aggregate by symbol."""

    symbol: str
    quote_currency: str
    lot_unit: str
    enabled: bool = True


@dataclass(frozen=True, slots=True)
class SymbolCatalog:
    """A versioned instrument catalogue supplied by P05. Section 2.1.

    P08 never derives a symbol's currency from its name. ``EURUSD`` does not
    imply the Deal settles in USD, and that inference is precisely the
    "assume USD" shortcut section 8.4 forbids.
    """

    version: str
    symbols: Mapping[str, SymbolSpec]

    def require(self, symbol: str) -> SymbolSpec:
        spec = self.symbols.get(symbol)
        if spec is None:
            raise PolicyError(
                Reason.SYMBOL_UNKNOWN,
                f"symbol {symbol!r} is not in catalog {self.version}",
                symbol=symbol,
                catalog_version=self.version,
            )
        if not spec.enabled:
            raise PolicyError(
                Reason.SYMBOL_UNKNOWN,
                f"symbol {symbol!r} is disabled in catalog {self.version}",
                symbol=symbol,
            )
        return spec

    def lot_unit(self, symbol: str) -> str:
        return self.require(symbol).lot_unit


@dataclass(frozen=True, slots=True)
class AccountingPolicy:
    """One immutable, versioned accounting policy. Pinned per build.

    ``version`` is part of the build identity, so changing any field here
    forces a new ``build_id`` instead of mutating a published report
    (ADR08-05).
    """

    version: str
    fee_coverage: FeeCoverage
    supported_currencies: frozenset[str]
    symbol_catalog: SymbolCatalog
    #: ``True`` only when a rate set with an as-of time has been agreed. P0
    #: leaves this ``False``, which makes conversion impossible by construction.
    fx_conversion_enabled: bool = False
    fx_rate_set_id: str | None = None
    #: Which reporting currencies may be requested as an output unit.
    report_currencies: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        for currency in self.supported_currencies:
            parse_currency(currency, "supported_currencies")
        if self.fx_conversion_enabled and not self.fx_rate_set_id:
            raise PolicyError(
                Reason.POLICY_VERSION_UNKNOWN,
                "fx_conversion_enabled requires an fx_rate_set_id",
            )

    def require_currency(self, currency: str) -> str:
        parsed = parse_currency(currency)
        if parsed not in self.supported_currencies:
            raise PolicyError(
                Reason.CURRENCY_UNSUPPORTED,
                f"currency {parsed} is outside policy {self.version}",
                currency=parsed,
                policy_version=self.version,
            )
        return parsed

    def validate_declared_coverage(self, declared: str) -> FeeCoverage:
        """Check an event's declared coverage against the pinned policy.

        A Deal claiming broader coverage than the policy grants is a contract
        break, not an upgrade: honouring it would let one event widen the
        meaning of every total it lands in.
        """
        try:
            coverage = FeeCoverage(declared)
        except ValueError as exc:
            raise PolicyError(
                Reason.FEE_COVERAGE_UNKNOWN,
                f"unrecognised fee_coverage {declared!r}",
                declared=declared,
            ) from exc
        if coverage is FeeCoverage.UNKNOWN:
            raise PolicyError(
                Reason.FEE_COVERAGE_UNKNOWN,
                "event declares UNKNOWN fee coverage; monetary reporting is blocked",
            )
        if coverage is FeeCoverage.ALL_FEES_INCLUDED and not self.fee_coverage.permits_full_net:
            raise PolicyError(
                Reason.FEE_COVERAGE_UNKNOWN,
                "event claims full fee coverage beyond the approved policy",
                policy_coverage=str(self.fee_coverage),
                declared=declared,
            )
        return coverage

    def net_realized(
        self, gross: Decimal, commission: Decimal, swap: Decimal
    ) -> tuple[Decimal | None, str]:
        """Return ``(net, label)`` under this policy's fee coverage.

        ``None`` means "no net may be published". The label travels with the
        number into the API response so a consumer cannot mistake a partial
        subtotal for an economic net (section 7.1).
        """
        if self.fee_coverage is FeeCoverage.UNKNOWN:
            return None, "NET_UNAVAILABLE_COVERAGE_UNKNOWN"
        if self.fee_coverage is FeeCoverage.GROSS_ONLY:
            return None, "NET_UNAVAILABLE_FEES_NOT_MODELLED"
        total = checked_sum((gross, commission, swap), field="net_realized")
        if self.fee_coverage.permits_full_net:
            return total, "NET_FULL"
        return total, "NET_COMPONENTS_COVERED"

    def convert(self, amount: Decimal, from_currency: str, to_currency: str) -> Decimal:
        """FX conversion, which P0 refuses by design."""
        if from_currency == to_currency:
            return amount
        if not self.fx_conversion_enabled:
            raise PolicyError(
                Reason.FX_CONVERSION_UNSUPPORTED,
                "FX conversion requires a versioned rate set; policy has none",
                from_currency=from_currency,
                to_currency=to_currency,
                policy_version=self.version,
            )
        # P1 (FR08-12): a rate set id alone is not a rate. Until the lookup,
        # as-of selection and conversion side are agreed with P05, enabling
        # the flag must still not produce a number.
        raise PolicyError(
            Reason.FX_CONVERSION_UNSUPPORTED,
            "rate-set conversion is not implemented; see FR08-12",
            fx_rate_set_id=self.fx_rate_set_id,
        )


#: The single approved P0 policy: USD and EUR kept apart, no conversion, fees
#: limited to commission and swap. Section 0.2.
def default_p0_policy() -> AccountingPolicy:
    catalog = SymbolCatalog(
        version="sym-catalog-v1",
        symbols={
            "EURUSD": SymbolSpec("EURUSD", "USD", "LOT_100K_BASE"),
            "GBPUSD": SymbolSpec("GBPUSD", "USD", "LOT_100K_BASE"),
            "USDJPY": SymbolSpec("USDJPY", "JPY", "LOT_100K_BASE"),
            "XAUUSD": SymbolSpec("XAUUSD", "USD", "LOT_100_OZ"),
        },
    )
    return AccountingPolicy(
        version="usd-fx-synthetic-v1",
        fee_coverage=FeeCoverage.COMMISSION_AND_SWAP_ONLY,
        supported_currencies=frozenset({"USD", "EUR"}),
        symbol_catalog=catalog,
        fx_conversion_enabled=False,
        report_currencies=frozenset({"USD", "EUR"}),
    )
