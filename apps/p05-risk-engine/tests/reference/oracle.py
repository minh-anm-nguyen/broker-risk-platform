"""An independent reference implementation of the P0 arithmetic.

This module imports **nothing** from ``risk``. That is the whole point: an
expected value produced by the code under test proves only that the code agrees
with itself (WF5-289). Here the formulas are re-derived from the design
document's section 5, in the most literal way possible, using only the standard
library.

It is intentionally the *naive* implementation -- plain dicts, plain Decimals,
no typed money, no validation. The engine is the careful one; this is the second
opinion. Where the two disagree, the disagreement is the finding.

A deliberate exception to the "naive" rule: :func:`reference_realized_from_prices`
reconstructs realized P&L from prices, which the engine must never do. It exists
to cross-check the *fixture* -- to confirm that the simulator's booked amount
and the price arithmetic agree on a controlled dataset -- and nothing else.
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal, localcontext
from typing import Iterable, Mapping, Sequence

__all__ = [
    "D",
    "reference_units",
    "reference_unrealized",
    "reference_exposure",
    "reference_realized",
    "reference_realized_from_prices",
    "reference_mid",
    "reference_quote_notional",
    "reference_in_window",
    "reference_quantize",
    "PRECISION",
]

#: Matches the engine's working precision. Chosen independently from the same
#: documented policy value, not imported from it.
PRECISION = 50


def D(text: str | int) -> Decimal:
    """Build a Decimal the only safe way: from a string or an exact int."""
    return Decimal(text)


def reference_units(lots: str, contract_size: str) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = PRECISION
        return D(lots) * D(contract_size)


def reference_unrealized(
    *,
    side: str,
    lots: str,
    open_price: str,
    bid: str,
    ask: str,
    contract_size: str,
) -> Decimal:
    """Section 5.4, transcribed literally.

    BUY closes at the bid, SELL closes at the ask. Written out as two separate
    branches rather than a sign trick, so that a reader can check each against
    the document without unpicking any cleverness.
    """
    units = reference_units(lots, contract_size)
    with localcontext() as ctx:
        ctx.prec = PRECISION
        if side == "BUY":
            return (D(bid) - D(open_price)) * units
        if side == "SELL":
            return (D(open_price) - D(ask)) * units
    raise ValueError(f"unsupported side {side!r}")


def reference_exposure(
    positions: Sequence[Mapping[str, str]], contract_sizes: Mapping[str, str]
) -> dict[str, dict[str, Decimal]]:
    """Section 5.2, transcribed literally.

    Long and short are accumulated as positive magnitudes in separate buckets;
    gross adds them and net subtracts them. Only ``OPEN`` and ``PRESENT`` rows
    participate.
    """
    out: dict[str, dict[str, Decimal]] = {}
    with localcontext() as ctx:
        ctx.prec = PRECISION
        for position in positions:
            if position.get("presence", "PRESENT") != "PRESENT":
                continue
            if position.get("source_status", "OPEN") != "OPEN":
                continue
            symbol = position["symbol"]
            units = D(position["lots"]) * D(contract_sizes[symbol])
            bucket = out.setdefault(symbol, {"long": D(0), "short": D(0)})
            if position["side"] == "BUY":
                bucket["long"] += units
            elif position["side"] == "SELL":
                bucket["short"] += units
            else:
                raise ValueError(f"unsupported side {position['side']!r}")
        for bucket in out.values():
            bucket["gross"] = bucket["long"] + bucket["short"]
            bucket["net"] = bucket["long"] - bucket["short"]
    return out


def reference_realized(deals: Iterable[Mapping[str, str]]) -> dict[str, Decimal]:
    """Section 5.3, transcribed literally.

    Three independent signed sums, combined once at the end. ``abs()`` appears
    nowhere: a positive commission is a rebate and keeps its sign.
    """
    gross = D(0)
    commission = D(0)
    swap = D(0)
    with localcontext() as ctx:
        ctx.prec = PRECISION
        for deal in deals:
            gross += D(deal["gross"])
            commission += D(deal["commission"])
            swap += D(deal["swap"])
        net = gross + commission + swap
    return {
        "gross": gross,
        "commission": commission,
        "swap": swap,
        "net_excluding_fee": net,
    }


def reference_realized_from_prices(
    closes: Iterable[Mapping[str, str]], contract_size: str
) -> Decimal:
    """Reconstruct realized P&L from close prices -- fixture cross-check only.

    The engine must never do this: on a real broker feed the booked amount and
    the price arithmetic diverge through partial closes, conversion legs and
    rounding conventions, and the booked amount is the authoritative one
    (section 5.3). On the synthetic fixtures the two agree, and checking that
    they agree is what validates the fixture itself.
    """
    total = D(0)
    with localcontext() as ctx:
        ctx.prec = PRECISION
        for row in closes:
            units = D(row["lots"]) * D(contract_size)
            if row["side"] == "BUY":
                total += (D(row["close_price"]) - D(row["open_price"])) * units
            else:
                total += (D(row["open_price"]) - D(row["close_price"])) * units
    return total


def reference_mid(bid: str, ask: str) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = PRECISION
        return (D(bid) + D(ask)) / D(2)


def reference_quote_notional(net_units: Decimal, mid: Decimal) -> Decimal:
    with localcontext() as ctx:
        ctx.prec = PRECISION
        return net_units * mid


def reference_in_window(
    executed_at: datetime, start: datetime | None, end: datetime | None
) -> bool:
    """Half-open window ``[start, end)``, written out explicitly."""
    if start is not None and executed_at < start:
        return False
    if end is not None and executed_at >= end:
        return False
    return True


def reference_quantize(value: Decimal, scale: int, rounding: str) -> Decimal:
    """Round once, at the end, under a named mode."""
    with localcontext() as ctx:
        ctx.prec = PRECISION
        ctx.rounding = rounding
        return value.quantize(Decimal(1).scaleb(-scale))
