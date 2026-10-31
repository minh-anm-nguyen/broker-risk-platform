"""Quote providers: pinned, immutable, versioned market marks.

Project 03 does not persist quotes, so this adapter is the boundary where
market data enters the engine. Everything about it is built around one
requirement: a calculation must be able to say *which* prices it used, and be
able to prove it would get the same ones again.

Hence:

* A snapshot is immutable once handed out. A provider that re-issues the same
  ``quote_id`` with a different price is quarantined, not accepted as an update
  (WF5-067, WF5-186).
* A snapshot may declare the position scan it was captured against. That
  binding is the only thing that lets unrealized P&L claim to describe the same
  instant as the position set (WF5-029).
* Freshness is evaluated against the valuation instant, not against "now".
  Reusing the wall clock would make a historical report's quotes look stale and
  a live report's look fresh after a clock jump (WF5-182).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Mapping, Protocol, Sequence

from risk.domain.facts import QuoteFact, QuoteSnapshot, parse_utc
from risk.domain.money import parse_decimal
from risk.errors import ContractError, QualityGateError, ReasonCode

__all__ = [
    "QuoteProvider",
    "FixtureQuoteProvider",
    "ImmutableQuoteStore",
    "build_snapshot",
]


class QuoteProvider(Protocol):
    """Supplies one immutable snapshot per calculation."""

    async def snapshot(
        self, *, symbols: Sequence[str], as_of: datetime, snapshot_id: str | None = None
    ) -> QuoteSnapshot: ...


def build_snapshot(
    snapshot_id: str,
    rows: Sequence[Mapping[str, object]],
    *,
    captured_at: datetime,
    source_id: str = "",
    bound_position_scan_id: str | None = None,
) -> QuoteSnapshot:
    """Validate raw quote rows into a pinned snapshot.

    Each row needs ``symbol``, ``bid``, ``ask``, ``market_time`` and
    ``quote_id``. Prices must be decimal strings: a float here is the same
    precision loss as anywhere else, and a mark is multiplied by 100 000 units
    before anyone sees it.
    """
    quotes: dict[str, QuoteFact] = {}
    for row in rows:
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise ContractError(ReasonCode.QUOTE_SYMBOL_MISMATCH, "quote row lacks a symbol")
        if symbol in quotes:
            raise ContractError(
                ReasonCode.QUOTE_IMMUTABILITY_CONFLICT,
                "two quotes for one symbol in a single snapshot",
                symbol=symbol,
            )
        quote_id = row.get("quote_id")
        if not isinstance(quote_id, str) or not quote_id:
            # Without an id there is nothing to pin, and "the price at the time"
            # stops being a reproducible statement (WF5-008).
            raise ContractError(
                ReasonCode.QUOTE_CUT_UNPROVEN, "quote row lacks a version id", symbol=symbol
            )
        quotes[symbol] = QuoteFact(
            symbol=symbol,
            bid=parse_decimal(row.get("bid"), field="bid"),
            ask=parse_decimal(row.get("ask"), field="ask"),
            market_time=parse_utc(row.get("market_time"), field_name="market_time"),
            quote_id=quote_id,
            snapshot_id=snapshot_id,
            feed_status=str(row.get("feed_status", "ACTIVE")),
        )
    return QuoteSnapshot(
        snapshot_id=snapshot_id,
        quotes=quotes,
        captured_at=captured_at,
        source_id=source_id,
        bound_position_scan_id=bound_position_scan_id,
    )


class ImmutableQuoteStore:
    """Append-only quote store that rejects contradicting re-publication.

    This is the mechanism behind the immutability claim. Storing a tick is
    cheap; proving that the tick a report cited has not been rewritten
    underneath it requires refusing the rewrite at the moment it is attempted.
    """

    def __init__(self) -> None:
        self._by_quote_id: dict[str, QuoteFact] = {}
        self._snapshots: dict[str, QuoteSnapshot] = {}

    def put(self, quote: QuoteFact) -> None:
        existing = self._by_quote_id.get(quote.quote_id)
        if existing is not None:
            same = (
                existing.symbol == quote.symbol
                and existing.bid == quote.bid
                and existing.ask == quote.ask
                and existing.market_time == quote.market_time
            )
            if not same:
                raise ContractError(
                    ReasonCode.QUOTE_IMMUTABILITY_CONFLICT,
                    "a quote id was re-published with different content",
                    quote_id=quote.quote_id,
                )
            return
        self._by_quote_id[quote.quote_id] = quote

    def publish_snapshot(self, snapshot: QuoteSnapshot) -> None:
        existing = self._snapshots.get(snapshot.snapshot_id)
        if existing is not None:
            for symbol, quote in snapshot.quotes.items():
                previous = existing.quotes.get(symbol)
                if previous is not None and (previous.bid, previous.ask) != (quote.bid, quote.ask):
                    raise ContractError(
                        ReasonCode.QUOTE_IMMUTABILITY_CONFLICT,
                        "a snapshot id was re-published with different prices",
                        snapshot_id=snapshot.snapshot_id,
                        symbol=symbol,
                    )
            return
        for quote in snapshot.quotes.values():
            self.put(quote)
        self._snapshots[snapshot.snapshot_id] = snapshot

    def get_snapshot(self, snapshot_id: str) -> QuoteSnapshot:
        try:
            return self._snapshots[snapshot_id]
        except KeyError:
            raise QualityGateError(
                ReasonCode.SNAPSHOT_EXPIRED,
                "the requested quote snapshot is no longer available",
                snapshot_id=snapshot_id,
            ) from None

    def __len__(self) -> int:
        return len(self._snapshots)


@dataclass
class FixtureQuoteProvider:
    """Deterministic provider over a pre-built set of snapshots.

    The P0 profile uses this rather than a live feed, and the design document
    is explicit that it should: a reproducible mark is a precondition for a
    reproducible result, and a live feed cannot give one.
    """

    snapshots: dict[str, QuoteSnapshot]
    default_snapshot_id: str | None = None
    #: Simulates a provider outage or a 429 for the fault-injection tests.
    fail_with: ReasonCode | None = None

    async def snapshot(
        self, *, symbols: Sequence[str], as_of: datetime, snapshot_id: str | None = None
    ) -> QuoteSnapshot:
        if self.fail_with is not None:
            raise QualityGateError(self.fail_with, "quote provider is unavailable")

        key = snapshot_id or self.default_snapshot_id
        if key is None:
            raise QualityGateError(
                ReasonCode.QUOTE_CUT_UNPROVEN, "no quote snapshot id was requested or configured"
            )
        try:
            snapshot = self.snapshots[key]
        except KeyError:
            raise QualityGateError(
                ReasonCode.SNAPSHOT_EXPIRED, "unknown quote snapshot", snapshot_id=key
            ) from None

        missing = sorted(set(symbols) - set(snapshot.quotes))
        if missing:
            # Reported as a provider-level failure so the gate can decide
            # whether the affected component or the whole request blocks.
            raise QualityGateError(
                ReasonCode.QUOTE_MISSING,
                "snapshot does not price every requested symbol",
                missing_count=len(missing),
                snapshot_id=key,
            )
        return snapshot

