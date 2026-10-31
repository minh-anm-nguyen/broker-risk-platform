"""FX conversion: a versioned rate set with an explicitly bounded path search.

This is the P1 surface. The P0 profile never converts anything -- it refuses
non-USD profit outright -- and this module exists so that the refusal has a
principled replacement rather than a ``rate or 1`` fallback.

Four properties are enforced, each corresponding to a way currency conversion
goes wrong in practice:

*Approved paths only.* The hop graph is built from the rate set, searched
deterministically and capped at ``max_hops``. "Best rate wins" is not a policy;
it is an invitation to pick whichever stale quote flatters the number
(WF5-102, WF5-106).

*Side awareness.* Settling a profit and settling a loss are opposite trades, so
they take opposite sides of the rate. Applying the bid to both directions
silently pockets the spread twice (WF5-105).

*Time coherence.* A path whose legs were quoted minutes apart is not a rate at
an instant. Strict valuation requires the legs to agree within a tolerance
(WF5-104).

*One provider per pair.* Two providers disagreeing beyond a threshold is an
unresolved question, not an average (WF5-103).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, localcontext

from risk.domain.money import CALC_PRECISION, Money, validate_currency
from risk.errors import ContractError, QualityGateError, ReasonCode

__all__ = [
    "FxQuote",
    "FxRateSet",
    "ConversionLeg",
    "ConversionResult",
    "DEFAULT_MAX_FX_HOPS",
]

#: Section 8.1 caps the P1 conversion graph at two hops.
DEFAULT_MAX_FX_HOPS = 2


@dataclass(frozen=True, slots=True)
class FxQuote:
    """A two-sided rate for ``base -> quote`` from one provider."""

    base_currency: str
    quote_currency: str
    bid: Decimal
    ask: Decimal
    market_time: datetime
    provider: str
    rate_id: str

    def __post_init__(self) -> None:
        validate_currency(self.base_currency)
        validate_currency(self.quote_currency)
        if self.base_currency == self.quote_currency:
            raise ContractError(ReasonCode.FX_RATE_INVALID, "an FX pair needs two distinct currencies")
        for name in ("bid", "ask"):
            value = getattr(self, name)
            if not isinstance(value, Decimal) or not value.is_finite():
                raise ContractError(ReasonCode.FX_RATE_INVALID, "rate must be a finite Decimal", field=name)
            if value <= 0:
                # A zero or negative rate would silently zero out or flip every
                # converted amount (WF5-099).
                raise ContractError(ReasonCode.FX_RATE_INVALID, "rate must be positive", field=name)
        if self.bid > self.ask:
            raise ContractError(ReasonCode.FX_SPREAD_INVALID, "FX bid above ask")
        if self.market_time.tzinfo is None:
            raise ContractError(ReasonCode.NAIVE_TIMESTAMP_REJECTED, "rate market_time must be tz-aware")

    @property
    def pair(self) -> tuple[str, str]:
        return (self.base_currency, self.quote_currency)

    def rate_for(self, *, selling_base: bool) -> Decimal:
        """Rate to apply when selling (bid) or buying (ask) the base currency."""
        return self.bid if selling_base else self.ask

    def inverted(self) -> FxQuote:
        """The reciprocal pair.

        Inversion swaps the sides as well as the numbers: the bid of the
        inverse is ``1/ask``, not ``1/bid``. Getting that backwards is the
        classic inverse-rate bug and it always favours the house (WF5-101).
        """
        with localcontext() as ctx:
            ctx.prec = CALC_PRECISION
            return FxQuote(
                base_currency=self.quote_currency,
                quote_currency=self.base_currency,
                bid=Decimal(1) / self.ask,
                ask=Decimal(1) / self.bid,
                market_time=self.market_time,
                provider=self.provider,
                rate_id=f"{self.rate_id}:inv",
            )


@dataclass(frozen=True, slots=True)
class ConversionLeg:
    """One hop of an executed conversion, recorded for the audit trail."""

    base_currency: str
    quote_currency: str
    rate: Decimal
    rate_id: str
    provider: str
    inverted: bool

    def as_payload(self) -> dict[str, object]:
        return {
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "rate": f"{self.rate:f}",
            "rate_id": self.rate_id,
            "provider": self.provider,
            "inverted": self.inverted,
        }


@dataclass(frozen=True)
class ConversionResult:
    converted: Money
    legs: tuple[ConversionLeg, ...]
    rate_set_id: str

    def as_payload(self) -> dict[str, object]:
        return {
            "amount": self.converted.canonical(),
            "currency": self.converted.currency,
            "rate_set_id": self.rate_set_id,
            "legs": [leg.as_payload() for leg in self.legs],
        }


@dataclass(frozen=True)
class FxRateSet:
    """An immutable, versioned collection of rates with one provider per pair."""

    rate_set_id: str
    quotes: tuple[FxQuote, ...]
    max_hops: int = DEFAULT_MAX_FX_HOPS
    max_leg_time_spread_seconds: int = 60

    def __post_init__(self) -> None:
        if not self.rate_set_id:
            raise ContractError(ReasonCode.CONTRACT_MISSING_FIELD, "rate_set_id is required")
        if self.max_hops < 1:
            raise ContractError(ReasonCode.FX_PATH_TOO_LONG, "max_hops must be at least 1")
        seen: dict[tuple[str, str], FxQuote] = {}
        for quote in self.quotes:
            existing = seen.get(quote.pair)
            if existing is not None and existing.provider != quote.provider:
                # Two providers for one pair: a policy decision nobody made.
                raise ContractError(
                    ReasonCode.FX_PROVIDER_CONFLICT,
                    "two providers supply the same pair in one rate set",
                    pair=f"{quote.base_currency}{quote.quote_currency}",
                )
            if existing is not None:
                raise ContractError(
                    ReasonCode.DATA_INTEGRITY_CONFLICT,
                    "duplicate pair in one rate set",
                    pair=f"{quote.base_currency}{quote.quote_currency}",
                )
            seen[quote.pair] = quote
        object.__setattr__(self, "_by_pair", seen)

    def _edges(self) -> dict[str, list[tuple[str, FxQuote, bool]]]:
        """Adjacency list including the inverse of every declared pair."""
        by_pair: dict[tuple[str, str], FxQuote] = getattr(self, "_by_pair")
        graph: dict[str, list[tuple[str, FxQuote, bool]]] = {}
        for (base, quote_ccy), quote in by_pair.items():
            graph.setdefault(base, []).append((quote_ccy, quote, False))
            graph.setdefault(quote_ccy, []).append((base, quote, True))
        # Deterministic neighbour order: two hosts must choose the same path.
        for neighbours in graph.values():
            neighbours.sort(key=lambda item: (item[0], item[1].rate_id, item[2]))
        return graph

    def find_path(self, source: str, target: str) -> tuple[tuple[str, FxQuote, bool], ...]:
        """Breadth-first search for the shortest approved path.

        Raises:
            QualityGateError: ``FX_RATE_MISSING`` when no path exists within
                ``max_hops``; ``FX_PATH_TOO_LONG`` when a path exists but is
                longer than the policy allows.
        """
        validate_currency(source)
        validate_currency(target)
        if source == target:
            return ()

        graph = self._edges()
        # BFS, so the first path found is the shortest; visited-set membership
        # is what makes a declared cycle unreachable rather than fatal
        # (a cycle in the data is fine, traversing it is not).
        frontier: list[tuple[str, tuple[tuple[str, FxQuote, bool], ...]]] = [(source, ())]
        visited = {source}
        depth = 0
        while frontier and depth < self.max_hops:
            depth += 1
            next_frontier: list[tuple[str, tuple[tuple[str, FxQuote, bool], ...]]] = []
            for currency, path in frontier:
                for neighbour, quote, inverted in graph.get(currency, ()):
                    if neighbour in visited:
                        continue
                    extended = path + ((neighbour, quote, inverted),)
                    if neighbour == target:
                        return extended
                    visited.add(neighbour)
                    next_frontier.append((neighbour, extended))
            frontier = next_frontier

        # Distinguish "no route" from "route too long" so the operator knows
        # whether to add a rate or to raise the hop budget.
        reachable = self._reachable_ignoring_hops(source)
        if target in reachable:
            raise QualityGateError(
                ReasonCode.FX_PATH_TOO_LONG,
                "conversion path exceeds the approved hop limit",
                source=source,
                target=target,
                max_hops=self.max_hops,
            )
        raise QualityGateError(
            ReasonCode.FX_RATE_MISSING,
            "no approved conversion path between these currencies",
            source=source,
            target=target,
            rate_set_id=self.rate_set_id,
        )

    def _reachable_ignoring_hops(self, source: str) -> set[str]:
        graph = self._edges()
        seen = {source}
        stack = [source]
        while stack:
            current = stack.pop()
            for neighbour, _quote, _inv in graph.get(current, ()):
                if neighbour not in seen:
                    seen.add(neighbour)
                    stack.append(neighbour)
        return seen

    def _check_time_coherence(self, quotes: list[FxQuote], *, strict: bool) -> None:
        if not strict or len(quotes) < 2:
            return
        times = [q.market_time for q in quotes]
        spread = max(times) - min(times)
        if spread > timedelta(seconds=self.max_leg_time_spread_seconds):
            raise QualityGateError(
                ReasonCode.FX_RATE_SET_INCOHERENT,
                "conversion legs were quoted too far apart for strict valuation",
                spread_seconds=int(spread.total_seconds()),
                limit_seconds=self.max_leg_time_spread_seconds,
            )

    def convert(
        self,
        money: Money,
        target_currency: str,
        *,
        strict_time_coherence: bool = True,
    ) -> ConversionResult:
        """Convert an amount along an approved path, side-aware.

        The side is chosen from the amount's own sign. A positive balance is
        realised by *selling* the source currency (bid); a negative balance has
        to be covered by *buying* it (ask).
        """
        validate_currency(target_currency)
        if money.currency == target_currency:
            return ConversionResult(converted=money, legs=(), rate_set_id=self.rate_set_id)

        path = self.find_path(money.currency, target_currency)
        self._check_time_coherence([quote for _ccy, quote, _inv in path], strict=strict_time_coherence)

        selling_base = money.amount >= 0
        current = money
        legs: list[ConversionLeg] = []
        for neighbour, quote, inverted in path:
            effective = quote.inverted() if inverted else quote
            rate = effective.rate_for(selling_base=selling_base)
            current = current.converted(rate, neighbour)
            legs.append(
                ConversionLeg(
                    base_currency=effective.base_currency,
                    quote_currency=effective.quote_currency,
                    rate=rate,
                    rate_id=effective.rate_id,
                    provider=effective.provider,
                    inverted=inverted,
                )
            )
        return ConversionResult(converted=current, legs=tuple(legs), rate_set_id=self.rate_set_id)
