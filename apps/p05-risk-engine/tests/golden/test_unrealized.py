"""Domains F, K and P -- quote quality, valuation correctness, staleness.

Covers WF5-061..072, WF5-121..132 and WF5-181..192.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import Decimal

import pytest

from risk.adapters.quotes import build_snapshot
from risk.domain.facts import PositionFact, QuoteFact, QuoteSnapshot
from risk.domain.unrealized import compute_unrealized, quote_age_seconds, unrealized_for_position
from risk.errors import ContractError, QualityGateError, ReasonCode, UnsupportedError
from risk.status import Presence, Side
from risk_tools.fixtures import ACCOUNT_A, EPOCH_A, T0
from tests.reference.oracle import D, reference_unrealized

pytestmark = [pytest.mark.golden, pytest.mark.p0]

CONTRACT = "100000"
LIVE_THRESHOLD = D("5")


def position(
    position_id: str = "p-1",
    *,
    symbol: str = "EURUSD",
    side: str = "BUY",
    lots: str = "0.50",
    open_price: str = "1.10000",
    source_status: str = "OPEN",
    presence: Presence = Presence.PRESENT,
) -> PositionFact:
    return PositionFact(
        position_id=position_id,
        account_id=ACCOUNT_A,
        symbol=symbol,
        side=Side(side),
        lots=D(lots),
        open_price=D(open_price),
        source_status=source_status,
        presence=presence,
        epoch_id=EPOCH_A,
        published_scan_id="scan-P",
    )


def snapshot(
    *rows: dict[str, str],
    snapshot_id: str = "quote-S1",
    market_time=None,
) -> QuoteSnapshot:
    when = market_time or T0
    return build_snapshot(
        snapshot_id,
        [
            {
                "symbol": row.get("symbol", "EURUSD"),
                "bid": row["bid"],
                "ask": row["ask"],
                "market_time": when.isoformat(),
                "quote_id": row.get("quote_id", f"q-{row.get('symbol', 'EURUSD')}-1"),
                "feed_status": row.get("feed_status", "ACTIVE"),
            }
            for row in rows
        ],
        captured_at=when,
    )


def value(positions, quotes, catalog, *, as_of=None, threshold=LIVE_THRESHOLD):
    return compute_unrealized(
        positions,
        catalog,
        quotes,
        valuation_as_of=as_of or T0,
        max_quote_age_seconds=threshold,
        reporting_currency="USD",
    )


# --------------------------------------------------------------------------- #
# Domain K -- the four sign cases
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-061", "WF5-121")
def test_buy_profit_uses_bid_only(catalog, evidence):
    """BUY 0.5 from 1.10000 to bid 1.10200 is +100.00, computed off the bid."""
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"})
    result = value((position(),), quotes, catalog)

    expected = reference_unrealized(
        side="BUY", lots="0.50", open_price="1.10000",
        bid="1.10200", ask="1.10220", contract_size=CONTRACT,
    )
    assert result.total.amount == expected == D("100.0000000")
    assert result.by_position[0].close_price == D("1.10200")
    # If the ask had been used the answer would be 110.00, so the assertion
    # below is what actually pins the side rule.
    assert result.total.amount != D("110.00")
    evidence(
        expected_values={"unrealized": "100.00", "close_price": "1.10200"},
        observed_values={
            "unrealized": result.total.canonical(),
            "close_price": str(result.by_position[0].close_price),
        },
    )


@pytest.mark.wf5("WF5-062", "WF5-122")
def test_sell_profit_uses_ask_only(catalog, evidence):
    """SELL 0.5 from 1.10000 with ask 1.09900 is +50.00, computed off the ask."""
    quotes = snapshot({"bid": "1.09880", "ask": "1.09900"})
    result = value((position(side="SELL"),), quotes, catalog)

    expected = reference_unrealized(
        side="SELL", lots="0.50", open_price="1.10000",
        bid="1.09880", ask="1.09900", contract_size=CONTRACT,
    )
    assert result.total.amount == expected == D("50.0000000")
    assert result.by_position[0].close_price == D("1.09900")
    # The bid would have given 60.00: a free 10 USD per half-lot.
    assert result.total.amount != D("60.00")
    evidence(
        expected_values={"unrealized": "50.00"},
        observed_values={"unrealized": result.total.canonical()},
    )


@pytest.mark.wf5("WF5-123")
def test_buy_loss_sign_is_negative(catalog, evidence):
    """A bid below the open price is a loss, reported as a negative number."""
    quotes = snapshot({"bid": "1.09800", "ask": "1.09820"})
    result = value((position(),), quotes, catalog)
    assert result.total.amount == D("-100.0000000")
    evidence(observed_values={"unrealized": result.total.canonical()})


@pytest.mark.wf5("WF5-124")
def test_sell_loss_sign_is_negative(catalog, evidence):
    quotes = snapshot({"bid": "1.10180", "ask": "1.10200"})
    result = value((position(side="SELL"),), quotes, catalog)
    assert result.total.amount == D("-100.0000000")
    evidence(observed_values={"unrealized": result.total.canonical()})


@pytest.mark.wf5("WF5-128")
def test_hedged_pair_valued_per_position_not_netted_first(catalog, evidence):
    """Each leg pays its own side of the spread; netting first would hide that."""
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"})
    positions = (
        position("p-long", side="BUY", lots="1.00"),
        position("p-short", side="SELL", lots="1.00"),
    )
    result = value(positions, quotes, catalog)

    per_position = {v.position_id: v.unrealized.amount for v in result.by_position}
    assert per_position["p-long"] == D("200.0000000")
    assert per_position["p-short"] == D("-220.0000000")
    # The total is negative by exactly the spread on both legs: 2 lots of
    # 0.00020 over 100 000 units. Netting to zero volume first would have
    # produced 0.00 and lost the spread cost entirely.
    assert result.total.amount == D("-20.0000000")
    evidence(
        expected_values={"total": "-20.00", "long": "200.00", "short": "-220.00"},
        observed_values={
            "total": result.total.canonical(),
            "long": str(per_position["p-long"]),
            "short": str(per_position["p-short"]),
        },
    )


@pytest.mark.wf5("WF5-125")
def test_non_open_position_excluded_from_valuation(catalog, evidence):
    """A CLOSED or ABSENT row contributes no P&L."""
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"})
    positions = (
        position("p-open"),
        position("p-closed", source_status="CLOSED"),
        position("p-absent", presence=Presence.ABSENT),
    )
    result = value(positions, quotes, catalog)

    assert len(result.by_position) == 1
    assert result.by_position[0].position_id == "p-open"
    assert result.total.amount == D("100.0000000")
    evidence(observed_values={"valued_positions": str(len(result.by_position))})


@pytest.mark.wf5("WF5-126")
def test_zero_open_price_rejected(evidence):
    """An open price of 0 is corrupt; valuing it would report the full notional."""
    with pytest.raises(ContractError) as exc:
        position(open_price="0")
    assert exc.value.reason is ReasonCode.INVALID_POSITION_PRICE
    evidence(
        expected_reason_codes=("INVALID_POSITION_PRICE",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-127")
def test_missing_quote_for_one_symbol_blocks(catalog, evidence):
    """An un-priced symbol blocks rather than yielding a partial total."""
    quotes = snapshot({"symbol": "EURUSD", "bid": "1.10200", "ask": "1.10220"})
    positions = (position("p-eur"), position("p-gbp", symbol="GBPUSD", open_price="1.25000"))

    with pytest.raises(QualityGateError) as exc:
        value(positions, quotes, catalog)
    assert exc.value.reason is ReasonCode.QUOTE_MISSING
    evidence(
        expected_reason_codes=("QUOTE_MISSING",),
        observed_reason_codes=(str(exc.value.reason),),
        notes=("a total missing one symbol is not a total",),
    )


@pytest.mark.wf5("WF5-129")
@pytest.mark.p1
def test_all_positions_share_one_pinned_snapshot(catalog, evidence):
    """Every valuation in one call cites the same snapshot and quote id."""
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"})
    positions = tuple(position(f"p-{i}", lots="0.10") for i in range(25))
    result = value(positions, quotes, catalog)

    snapshot_ids = {result.quote_snapshot_id}
    quote_ids = {v.quote_id for v in result.by_position}
    assert snapshot_ids == {"quote-S1"}
    assert quote_ids == {"q-EURUSD-1"}
    evidence(observed_values={"distinct_quote_ids": str(len(quote_ids))})


@pytest.mark.wf5("WF5-131")
@pytest.mark.p1
def test_non_usd_profit_currency_blocks_without_fx(catalog, evidence):
    """A JPY-profit instrument cannot be reported in USD without an approved rate."""
    quotes = snapshot(
        {"symbol": "USDJPY", "bid": "150.000", "ask": "150.010", "quote_id": "q-USDJPY-1"}
    )
    positions = (position("p-jpy", symbol="USDJPY", open_price="149.000"),)

    with pytest.raises(UnsupportedError) as exc:
        value(positions, quotes, catalog)
    # USDJPY is in the catalog and its prices are valid; what is missing is a
    # verified path from a JPY profit to a USD report.
    assert exc.value.reason is ReasonCode.UNSUPPORTED_CALC_MODE
    evidence(
        expected_reason_codes=("UNSUPPORTED_CALC_MODE",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-132")
@pytest.mark.p2
def test_no_accrual_field_is_mixed_into_unrealized_gross(catalog, evidence):
    """The unrealized figure is gross valuation only, with no swap folded in."""
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"})
    result = value((position(),), quotes, catalog)
    payload = result.as_payload()

    assert result.total.amount == D("100.0000000")
    for forbidden in ("accrued_swap", "unrealized_including_accruals", "net"):
        assert forbidden not in payload
    evidence(notes=("accruals would need their own named metric and their own proof",))


# --------------------------------------------------------------------------- #
# Domain F -- quote validity
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-063")
def test_inverted_spread_rejected(evidence):
    """bid above ask is not a market; it is refused at construction."""
    with pytest.raises(ContractError) as exc:
        QuoteFact(
            symbol="EURUSD",
            bid=D("1.10300"),
            ask=D("1.10200"),
            market_time=T0,
            quote_id="q-bad",
            snapshot_id="quote-S1",
        )
    assert exc.value.reason is ReasonCode.INVALID_QUOTE
    evidence(
        expected_reason_codes=("INVALID_QUOTE",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-064")
@pytest.mark.parametrize("bid,ask", [("0", "1.1"), ("1.1", "-1"), ("-1.1", "-1.0"), ("0", "0")])
def test_non_positive_prices_rejected(bid, ask, evidence):
    """A zero or negative price is rejected before it can be multiplied."""
    with pytest.raises(ContractError) as exc:
        QuoteFact(
            symbol="EURUSD",
            bid=D(bid),
            ask=D(ask),
            market_time=T0,
            quote_id="q-bad",
            snapshot_id="quote-S1",
        )
    assert exc.value.reason is ReasonCode.INVALID_QUOTE
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-065", "WF5-181")
def test_stale_quote_still_produces_a_number_marked_stale(catalog, evidence):
    """A 20-second-old mark against a 5-second policy is STALE, never zero."""
    old = snapshot({"bid": "1.10200", "ask": "1.10220"}, market_time=T0 - timedelta(seconds=20))
    result = value((position(),), old, catalog)

    assert result.is_stale
    assert result.stale_symbols == frozenset({"EURUSD"})
    # The crucial part: the valuation is present and correct, and labelled.
    assert result.total.amount == D("100.0000000")
    assert result.max_quote_age_seconds == D("20.000000")
    evidence(
        expected_status="STALE",
        observed_status="STALE",
        expected_values={"unrealized": "100.00", "age_s": "20.000000"},
        observed_values={
            "unrealized": result.total.canonical(),
            "age_s": str(result.max_quote_age_seconds),
        },
    )


@pytest.mark.wf5("WF5-065")
def test_quote_inside_threshold_is_fresh(catalog, evidence):
    """Four seconds is inside a five-second policy, so nothing is marked."""
    recent = snapshot({"bid": "1.10200", "ask": "1.10220"}, market_time=T0 - timedelta(seconds=4))
    result = value((position(),), recent, catalog)
    assert not result.is_stale
    evidence(observed_values={"age_s": str(result.max_quote_age_seconds)})


@pytest.mark.wf5("WF5-066", "WF5-164")
def test_quote_after_valuation_instant_rejected(catalog, evidence):
    """A mark dated after the as-of instant is look-ahead and is refused."""
    future = snapshot({"bid": "1.10200", "ask": "1.10220"}, market_time=T0 + timedelta(seconds=30))
    with pytest.raises(QualityGateError) as exc:
        value((position(),), future, catalog)
    assert exc.value.reason is ReasonCode.QUOTE_LOOKAHEAD

    age = quote_age_seconds(future.quotes["EURUSD"], as_of=T0)
    assert age < 0  # the negative age is the signal, not clamped away
    evidence(
        expected_reason_codes=("QUOTE_LOOKAHEAD",),
        observed_reason_codes=(str(exc.value.reason),),
        observed_values={"age_s": str(age)},
    )


@pytest.mark.wf5("WF5-067", "WF5-186")
def test_same_quote_id_with_different_price_quarantined(evidence):
    """A re-published quote id carrying a new price is a conflict, not an update."""
    from risk.adapters.quotes import ImmutableQuoteStore

    store = ImmutableQuoteStore()
    first = QuoteFact(
        symbol="EURUSD", bid=D("1.10200"), ask=D("1.10220"),
        market_time=T0, quote_id="q-1", snapshot_id="s-1",
    )
    store.put(first)
    store.put(first)  # an identical re-send is harmless

    mutated = QuoteFact(
        symbol="EURUSD", bid=D("1.10900"), ask=D("1.10920"),
        market_time=T0, quote_id="q-1", snapshot_id="s-1",
    )
    with pytest.raises(ContractError) as exc:
        store.put(mutated)
    assert exc.value.reason is ReasonCode.QUOTE_IMMUTABILITY_CONFLICT
    evidence(
        expected_reason_codes=("QUOTE_IMMUTABILITY_CONFLICT",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-068", "WF5-076")
def test_quote_keyed_under_the_wrong_symbol_rejected(evidence):
    """A snapshot cannot file a EURUSD quote under the GBPUSD key."""
    eur = QuoteFact(
        symbol="EURUSD", bid=D("1.10200"), ask=D("1.10220"),
        market_time=T0, quote_id="q-1", snapshot_id="s-1",
    )
    with pytest.raises(ContractError) as exc:
        QuoteSnapshot(snapshot_id="s-1", quotes={"GBPUSD": eur}, captured_at=T0)
    assert exc.value.reason is ReasonCode.QUOTE_SYMBOL_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-069", "WF5-189")
@pytest.mark.p1
def test_spread_widening_never_improves_a_liquidation_value(catalog, evidence):
    """Hold the mid, widen the spread tenfold: both sides get worse, never better."""
    narrow = snapshot({"bid": "1.10200", "ask": "1.10220"}, snapshot_id="s-narrow")
    wide = snapshot({"bid": "1.10110", "ask": "1.10310"}, snapshot_id="s-wide")

    # Same mid of 1.10210 in both snapshots.
    assert narrow.quotes["EURUSD"].mid == wide.quotes["EURUSD"].mid

    for side in ("BUY", "SELL"):
        held = (position(side=side, lots="1.00"),)
        before = value(held, narrow, catalog).total.amount
        after = value(held, wide, catalog).total.amount
        # Monotonic: a wider spread can only reduce what a position is worth on
        # liquidation. A mid-based valuation would have reported no change.
        assert after <= before, f"{side} improved on a wider spread"

    evidence(
        oracle="PROPERTY_METAMORPHIC",
        notes=("mid held constant; liquidation value non-increasing on both sides",),
    )


@pytest.mark.wf5("WF5-070", "WF5-190")
@pytest.mark.p1
def test_halted_feed_marks_stale_even_when_timestamp_is_fresh(catalog, evidence):
    """A reachable but halted feed is stale; HTTP success is not market liveness."""
    halted = snapshot({"bid": "1.10200", "ask": "1.10220", "feed_status": "HALTED"})
    result = value((position(),), halted, catalog)

    assert result.max_quote_age_seconds == D("0.000000")  # perfectly fresh timestamp
    assert result.is_stale  # and still not usable as live
    evidence(
        expected_status="STALE",
        observed_status="STALE",
        observed_values={"feed_status": "HALTED", "age_s": "0.000000"},
    )


@pytest.mark.wf5("WF5-071")
@pytest.mark.p1
@pytest.mark.parametrize(
    "bad_time", ["2026-10-15T12:00:00+99:00", "2026-10-15 12:00:00", "not-a-time", "2026-13-45T00:00:00Z"]
)
def test_malformed_or_naive_timestamps_rejected(bad_time, evidence):
    """A malformed offset or a naive timestamp is refused, never interpreted."""
    with pytest.raises(ContractError) as exc:
        build_snapshot(
            "s-1",
            [{"symbol": "EURUSD", "bid": "1.1", "ask": "1.2",
              "market_time": bad_time, "quote_id": "q-1"}],
            captured_at=T0,
        )
    assert exc.value.reason in (
        ReasonCode.INVALID_TIMESTAMP,
        ReasonCode.NAIVE_TIMESTAMP_REJECTED,
    )
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-072", "WF5-187")
@pytest.mark.p2
def test_pinned_snapshot_wins_over_a_late_arriving_tick(catalog, evidence):
    """Valuation follows the pinned snapshot, not whatever arrived last."""
    pinned = snapshot({"bid": "1.10200", "ask": "1.10220"}, snapshot_id="s-pinned")
    later = snapshot({"bid": "1.10900", "ask": "1.10920"}, snapshot_id="s-later")

    result = value((position(),), pinned, catalog)
    assert result.quote_snapshot_id == "s-pinned"
    assert result.total.amount == D("100.0000000")
    # The newer snapshot exists and is simply not what this calculation cited.
    assert value((position(),), later, catalog).total.amount == D("450.0000000")
    evidence(observed_values={"snapshot_used": result.quote_snapshot_id})


@pytest.mark.wf5("WF5-184")
def test_empty_quote_body_is_not_a_zero_price(catalog, evidence):
    """An empty provider response blocks; it does not become a 0.00 mark."""
    from risk.adapters.quotes import FixtureQuoteProvider
    import asyncio

    empty = build_snapshot("s-empty", [], captured_at=T0)
    provider = FixtureQuoteProvider({"s-empty": empty}, default_snapshot_id="s-empty")

    with pytest.raises(QualityGateError) as exc:
        asyncio.run(provider.snapshot(symbols=["EURUSD"], as_of=T0, snapshot_id="s-empty"))
    assert exc.value.reason is ReasonCode.QUOTE_MISSING
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-185")
def test_provider_outage_surfaces_as_a_reason_code(evidence):
    """A 429 or an outage is reported as a reason, not retried forever."""
    from risk.adapters.quotes import FixtureQuoteProvider
    import asyncio

    provider = FixtureQuoteProvider({}, fail_with=ReasonCode.DB_UNAVAILABLE)
    with pytest.raises(QualityGateError) as exc:
        asyncio.run(provider.snapshot(symbols=["EURUSD"], as_of=T0))
    assert exc.value.reason is ReasonCode.DB_UNAVAILABLE
    evidence(injection_confirmed=True, observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-182", "WF5-183")
def test_quote_age_is_measured_against_the_valuation_instant(catalog, evidence):
    """Age comes from market_time versus as-of, not from the wall clock."""
    quote_time = T0 - timedelta(seconds=10)
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"}, market_time=quote_time)

    # A backwards clock jump cannot make an old quote look fresh, because the
    # comparison uses the explicit as-of instant.
    assert quote_age_seconds(quotes.quotes["EURUSD"], as_of=T0) == D("10.000000")
    assert quote_age_seconds(
        quotes.quotes["EURUSD"], as_of=T0 + timedelta(hours=1)
    ) == D("3610.000000")

    stale = value((position(),), quotes, catalog, as_of=T0)
    assert stale.is_stale
    evidence(observed_values={"age_s": "10.000000"})


@pytest.mark.wf5("WF5-188")
def test_ten_percent_price_gap_computes_without_overflow(catalog, evidence):
    """A violent gap produces a large, exact number -- not NaN and not an overflow."""
    gapped = snapshot({"bid": "1.21000", "ask": "1.21020"})
    result = value((position(lots="1.00"),), gapped, catalog)

    assert result.total.amount == D("11000.0000000")
    assert result.total.amount.is_finite()
    evidence(observed_values={"unrealized": result.total.canonical()})


@pytest.mark.wf5("WF5-257")
def test_one_pip_bid_move_is_ten_usd_per_lot(catalog, evidence):
    """+0.0001 on the bid moves a 1-lot BUY by exactly +10 USD."""
    base = snapshot({"bid": "1.10200", "ask": "1.10220"}, snapshot_id="s-a")
    bumped = snapshot({"bid": "1.10210", "ask": "1.10230"}, snapshot_id="s-b")

    held = (position(lots="1.00"),)
    delta = value(held, bumped, catalog).total.amount - value(held, base, catalog).total.amount
    assert delta == D("10.0000000")
    evidence(
        oracle="PROPERTY_METAMORPHIC",
        expected_values={"delta": "10.00"},
        observed_values={"delta": str(delta)},
    )


@pytest.mark.wf5("WF5-258")
def test_one_pip_ask_move_costs_a_short_ten_usd_per_lot(catalog, evidence):
    """+0.0001 on the ask moves a 1-lot SELL by exactly -10 USD."""
    base = snapshot({"bid": "1.10180", "ask": "1.10200"}, snapshot_id="s-a")
    bumped = snapshot({"bid": "1.10190", "ask": "1.10210"}, snapshot_id="s-b")

    held = (position(side="SELL", lots="1.00"),)
    delta = value(held, bumped, catalog).total.amount - value(held, base, catalog).total.amount
    assert delta == D("-10.0000000")
    evidence(oracle="PROPERTY_METAMORPHIC", observed_values={"delta": str(delta)})


@pytest.mark.wf5("WF5-253")
def test_symmetric_prices_give_symmetric_signs(catalog, evidence):
    """Equal distances either side of the open price mirror in sign."""
    up = snapshot({"bid": "1.10100", "ask": "1.10100"}, snapshot_id="s-up")
    down = snapshot({"bid": "1.09900", "ask": "1.09900"}, snapshot_id="s-down")

    buy_up = value((position(side="BUY", lots="1.00"),), up, catalog).total.amount
    buy_down = value((position(side="BUY", lots="1.00"),), down, catalog).total.amount
    sell_up = value((position(side="SELL", lots="1.00"),), up, catalog).total.amount
    sell_down = value((position(side="SELL", lots="1.00"),), down, catalog).total.amount

    assert buy_up == -buy_down == D("100.0000000")
    assert sell_down == -sell_up == D("100.0000000")
    evidence(oracle="PROPERTY_METAMORPHIC",
             observed_values={"buy_up": str(buy_up), "sell_down": str(sell_down)})


def test_unrealized_for_position_rejects_mismatched_quote(catalog):
    """The per-position helper will not price one symbol with another's quote."""
    spec = catalog.get("EURUSD")
    gbp_quote = QuoteFact(
        symbol="GBPUSD", bid=D("1.25000"), ask=D("1.25020"),
        market_time=T0, quote_id="q-gbp", snapshot_id="s-1",
    )
    with pytest.raises(ContractError) as exc:
        unrealized_for_position(position(), spec, gbp_quote)
    assert exc.value.reason is ReasonCode.QUOTE_SYMBOL_MISMATCH


def test_empty_book_values_to_zero(catalog):
    """No open positions is a legitimate zero for this component."""
    quotes = snapshot({"bid": "1.10200", "ask": "1.10220"})
    result = value((), quotes, catalog)
    assert result.total.amount == Decimal(0)
    assert result.by_position == ()
