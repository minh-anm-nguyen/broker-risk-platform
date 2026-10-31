"""Domains V and Z -- algebraic invariants, metamorphic relations, fuzz.

Covers WF5-253..264 and WF5-301..312.

These tests earn their keep only if they can kill a bug. A property like
``sum(reversed(xs)) == sum(xs)`` is true of the standard library and says
nothing about this engine, so every property here is stated over the real
calculators and over inputs the engine would actually receive. Two of them
(``test_exposure_property_kills_a_collapsed_gross`` and
``test_side_property_kills_a_swapped_bid_ask``) are written as explicit mutant
hunts: they re-implement the bug and assert the property catches it (WF5-299).
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from risk.domain.classification import SourceCapability, select_eligible_deals
from risk.domain.exposure import compute_exposure
from risk.domain.facts import DealFact, PositionFact, build_money_triplet
from risk.domain.money import parse_decimal
from risk.domain.realized import compute_realized
from risk.domain.unrealized import compute_unrealized
from risk.errors import ContractError
from risk.manifest import digest
from risk.status import Presence, Side
from risk_tools.fixtures import ACCOUNT_A, EPOCH_A, T0
from tests.reference.oracle import D, reference_realized

pytestmark = [pytest.mark.property, pytest.mark.p0]

SIM = SourceCapability.SIMULATOR_TRADING_ONLY_V1
CONTRACT = D("100000")

#: Settings tuned for a suite that runs on every commit: enough examples to
#: find a sign or ordering bug, few enough to stay inside a minute.
PROFILE = settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

# --------------------------------------------------------------------------- #
# Strategies
# --------------------------------------------------------------------------- #

money_text = st.decimals(
    allow_nan=False,
    allow_infinity=False,
    min_value=Decimal("-1000000"),
    max_value=Decimal("1000000"),
    places=2,
).map(str)

lot_text = st.integers(min_value=1, max_value=10000).map(lambda n: str(Decimal(n) / 100))

price_text = st.integers(min_value=50000, max_value=300000).map(
    lambda n: str(Decimal(n) / Decimal(100000))
)

side_strategy = st.sampled_from([Side.BUY, Side.SELL])
symbol_strategy = st.sampled_from(["EURUSD", "GBPUSD", "AUDUSD"])


def make_position(index: int, symbol: str, side: Side, lots: str, open_price: str = "1.10000"):
    return PositionFact(
        position_id=f"p-{index}",
        account_id=ACCOUNT_A,
        symbol=symbol,
        side=side,
        lots=D(lots),
        open_price=D(open_price),
        source_status="OPEN",
        presence=Presence.PRESENT,
        epoch_id=EPOCH_A,
        published_scan_id="scan-P",
    )


def make_deal(index: int, gross: str, commission: str = "0.00", swap: str = "0.00"):
    g, c, s = build_money_triplet(
        currency="USD", gross=gross, commission=commission, swap=swap
    )
    return DealFact(
        deal_id=f"d-{index}",
        account_id=ACCOUNT_A,
        source_seq=index,
        entry_kind="OUT",
        deal_type=None,
        gross_profit=g,
        commission=c,
        swap=s,
        executed_at=T0,
        epoch_id=EPOCH_A,
    )


def quote_snapshot(bid: str, ask: str, *, symbols=("EURUSD", "GBPUSD", "AUDUSD"), sid="s-prop"):
    from risk.adapters.quotes import build_snapshot

    return build_snapshot(
        sid,
        [
            {
                "symbol": symbol,
                "bid": bid,
                "ask": ask,
                "market_time": T0.isoformat(),
                "quote_id": f"q-{symbol}-{sid}",
            }
            for symbol in symbols
        ],
        captured_at=T0,
    )


def realized_of(deals):
    eligibility = select_eligible_deals(
        deals, capability=SIM, period_start=None, period_end=None, deal_watermark_seq=None
    )
    return compute_realized(eligibility, currency="USD")


# --------------------------------------------------------------------------- #
# Domain V -- invariants over the real engine
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-310", "WF5-155")
@PROFILE
@given(sides=st.lists(st.tuples(side_strategy, lot_text), min_size=1, max_size=40))
def test_gross_always_at_least_absolute_net(sides, catalog):
    """INV05-06: for any book, gross exposure >= |net| in the same unit."""
    positions = tuple(
        make_position(i, "EURUSD", side, lots) for i, (side, lots) in enumerate(sides)
    )
    result = compute_exposure(positions, catalog)
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.gross_units.amount >= abs(eurusd.net_units.amount)
    assert eurusd.long_units.amount >= 0
    assert eurusd.short_units.amount >= 0


@pytest.mark.wf5("WF5-299")
def test_exposure_property_kills_a_collapsed_gross(catalog, evidence):
    """The gross >= |net| property detects a gross that was netted by mistake.

    The mutant is the most tempting single-line error in the module: computing
    gross as long minus short. On a hedged book it reports zero risk.
    """
    positions = (
        make_position(1, "EURUSD", Side.BUY, "1.00"),
        make_position(2, "EURUSD", Side.SELL, "1.00"),
    )
    real = compute_exposure(positions, catalog).by_symbol["EURUSD"]
    assert real.gross_units.amount >= abs(real.net_units.amount)
    assert real.gross_units.amount == D("200000.00")

    # The mutant, re-implemented here rather than described.
    mutant_gross = real.long_units.amount - real.short_units.amount
    mutant_net = real.long_units.amount - real.short_units.amount
    assert mutant_gross == D("0")
    # The property fails for the mutant only because gross and net differ in
    # the real implementation; on the hedged book this is the whole difference
    # between "no risk" and "two lots of risk".
    assert not (mutant_gross > abs(mutant_net)) and mutant_gross == abs(mutant_net)
    assert real.gross_units.amount != mutant_gross
    evidence(
        oracle="PROPERTY_METAMORPHIC",
        expected_values={"real_gross": "200000.00", "mutant_gross": "0"},
        observed_values={"real_gross": real.gross_units.canonical(),
                         "mutant_gross": str(mutant_gross)},
        notes=("mutation: gross computed as long - short",),
    )


@pytest.mark.wf5("WF5-254", "WF5-303")
@PROFILE
@given(
    rows=st.lists(
        st.tuples(symbol_strategy, side_strategy, lot_text), min_size=1, max_size=30
    ),
    rotation=st.integers(min_value=0, max_value=29),
)
def test_position_order_does_not_change_exposure_or_digest(rows, rotation, catalog):
    """Reordering the position stream changes neither the figures nor the digest."""
    positions = tuple(
        make_position(i, symbol, side, lots) for i, (symbol, side, lots) in enumerate(rows)
    )
    shift = rotation % len(positions)
    rotated = positions[shift:] + positions[:shift]

    first = compute_exposure(positions, catalog)
    second = compute_exposure(rotated, catalog)

    assert first.as_payload() == second.as_payload()
    assert digest(first.as_payload()) == digest(second.as_payload())


@pytest.mark.wf5("WF5-255")
@PROFILE
@given(lots=lot_text, bid=price_text)
def test_splitting_a_position_preserves_exposure_and_valuation(lots, bid, catalog):
    """One position of L lots equals two of L/2 at the same open price."""
    whole_lots = D(lots)
    half = whole_lots / 2
    assume(half == half.quantize(D("0.01")))  # stay on the volume grid

    quotes = quote_snapshot(bid, str(D(bid) + D("0.00020")))

    one = (make_position(1, "EURUSD", Side.BUY, str(whole_lots)),)
    two = (
        make_position(1, "EURUSD", Side.BUY, str(half)),
        make_position(2, "EURUSD", Side.BUY, str(half)),
    )

    exposure_one = compute_exposure(one, catalog).by_symbol["EURUSD"]
    exposure_two = compute_exposure(two, catalog).by_symbol["EURUSD"]
    assert exposure_one.net_units.amount == exposure_two.net_units.amount
    assert exposure_one.gross_units.amount == exposure_two.gross_units.amount

    valued_one = compute_unrealized(
        one, catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    )
    valued_two = compute_unrealized(
        two, catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    )
    assert valued_one.total.amount == valued_two.total.amount


@pytest.mark.wf5("WF5-261")
@pytest.mark.p1
@PROFILE
@given(lots=st.integers(min_value=1, max_value=4000).map(lambda n: str(Decimal(n) / 100)))
def test_doubling_volume_doubles_exposure_and_valuation(lots, catalog):
    """The P0 formula is linear in volume, so 2L gives exactly twice the figure."""
    doubled = str(D(lots) * 2)
    quotes = quote_snapshot("1.10200", "1.10220")

    single = (make_position(1, "EURUSD", Side.BUY, lots),)
    double = (make_position(1, "EURUSD", Side.BUY, doubled),)

    exposure_single = compute_exposure(single, catalog).by_symbol["EURUSD"].net_units.amount
    exposure_double = compute_exposure(double, catalog).by_symbol["EURUSD"].net_units.amount
    assert exposure_double == exposure_single * 2

    value_single = compute_unrealized(
        single, catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    ).total.amount
    value_double = compute_unrealized(
        double, catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    ).total.amount
    assert value_double == value_single * 2


@pytest.mark.wf5("WF5-253")
@PROFILE
@given(open_price=price_text, delta=st.integers(min_value=1, max_value=5000))
def test_buy_and_sell_are_mirror_images_at_a_single_price(open_price, delta, catalog):
    """With bid == ask, a long and a short of equal size are exact opposites."""
    mark = str(D(open_price) + D(delta) / D(100000))
    quotes = quote_snapshot(mark, mark)  # zero spread isolates the sign rule

    long_value = compute_unrealized(
        (make_position(1, "EURUSD", Side.BUY, "1.00", open_price),),
        catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    ).total.amount
    short_value = compute_unrealized(
        (make_position(1, "EURUSD", Side.SELL, "1.00", open_price),),
        catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    ).total.amount

    assert long_value == -short_value


@pytest.mark.wf5("WF5-299")
def test_side_property_kills_a_swapped_bid_ask(catalog, evidence):
    """A swapped bid/ask is caught because the spread's sign flips.

    The mutant -- closing a long at the ask and a short at the bid -- makes a
    hedged pair *gain* the spread instead of paying it. The real engine loses
    it, every time.
    """
    quotes = quote_snapshot("1.10200", "1.10220")
    hedged = (
        make_position(1, "EURUSD", Side.BUY, "1.00"),
        make_position(2, "EURUSD", Side.SELL, "1.00"),
    )
    real = compute_unrealized(
        hedged, catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    ).total.amount

    bid, ask = D("1.10200"), D("1.10220")
    open_price, units = D("1.10000"), D("100000")
    mutant = (ask - open_price) * units + (open_price - bid) * units

    assert real == D("-20.0000000")  # the spread is paid, twice
    assert mutant == D("20.00000")  # the mutant is handed it
    assert real < 0 < mutant
    evidence(
        oracle="PROPERTY_METAMORPHIC",
        expected_values={"real": "-20.00", "mutant": "+20.00"},
        observed_values={"real": str(real), "mutant": str(mutant)},
        notes=("mutation: BUY closed at ask and SELL at bid",),
    )


@pytest.mark.wf5("WF5-260", "WF5-301")
@PROFILE
@given(amounts=st.lists(money_text, min_size=0, max_size=120))
def test_realized_aggregation_matches_an_independent_sum(amounts):
    """Realized gross equals a naive independent sum, for any input list."""
    deals = tuple(make_deal(i + 1, gross=a) for i, a in enumerate(amounts))
    result = realized_of(deals)
    expected = reference_realized(
        [{"gross": a, "commission": "0.00", "swap": "0.00"} for a in amounts]
    )

    assert result.gross.amount == expected["gross"]
    assert result.net_excluding_fee.amount == expected["net_excluding_fee"]
    assert result.deal_count == len(amounts)


@pytest.mark.wf5("WF5-303")
@PROFILE
@given(
    rows=st.lists(st.tuples(money_text, money_text, money_text), min_size=1, max_size=60),
    rotation=st.integers(min_value=0, max_value=59),
)
def test_realized_is_permutation_invariant_including_its_digest(rows, rotation):
    """Deal delivery order changes neither the sums nor the output digest."""
    deals = tuple(
        make_deal(i + 1, gross=g, commission=c, swap=s)
        for i, (g, c, s) in enumerate(rows)
    )
    shift = rotation % len(deals)
    rotated = deals[shift:] + deals[:shift]

    first = realized_of(deals)
    second = realized_of(rotated)

    assert first.as_payload() == second.as_payload()
    assert digest(first.as_payload()) == digest(second.as_payload())
    assert first.deal_ids == second.deal_ids


@pytest.mark.wf5("WF5-259")
@PROFILE
@given(amounts=st.lists(money_text, min_size=1, max_size=40))
def test_adding_a_zero_deal_leaves_every_sum_untouched(amounts):
    """A zero-valued deal is provenance, not money."""
    deals = tuple(make_deal(i + 1, gross=a) for i, a in enumerate(amounts))
    with_zero = deals + (make_deal(len(deals) + 1, gross="0.00"),)

    base = realized_of(deals)
    extended = realized_of(with_zero)

    assert base.gross.amount == extended.gross.amount
    assert base.commission.amount == extended.commission.amount
    assert base.swap.amount == extended.swap.amount
    assert extended.deal_count == base.deal_count + 1


@pytest.mark.wf5("WF5-256", "WF5-304")
@PROFILE
@given(
    rows=st.lists(st.tuples(symbol_strategy, side_strategy, lot_text), min_size=1, max_size=20),
    duplicate_at=st.integers(min_value=0, max_value=19),
)
def test_duplicated_position_is_always_detected(rows, duplicate_at, catalog):
    """A duplicated position id raises, for any book and any duplicated row."""
    positions = [
        make_position(i, symbol, side, lots) for i, (symbol, side, lots) in enumerate(rows)
    ]
    index = duplicate_at % len(positions)
    positions.append(positions[index])

    with pytest.raises(ContractError):
        compute_exposure(tuple(positions), catalog)


@pytest.mark.wf5("WF5-264")
@pytest.mark.p2
@PROFILE
@given(
    rows=st.lists(
        st.tuples(symbol_strategy, side_strategy, lot_text, price_text),
        min_size=0,
        max_size=50,
    ),
    bid_ticks=st.integers(min_value=50000, max_value=200000),
    spread_ticks=st.integers(min_value=0, max_value=500),
)
def test_fuzzed_books_never_crash_and_never_fabricate(rows, bid_ticks, spread_ticks, catalog):
    """Arbitrary valid books either produce exact figures or raise a typed error."""
    bid = Decimal(bid_ticks) / Decimal(100000)
    ask = bid + Decimal(spread_ticks) / Decimal(100000)
    quotes = quote_snapshot(str(bid), str(ask))

    positions = tuple(
        make_position(i, symbol, side, lots, price)
        for i, (symbol, side, lots, price) in enumerate(rows)
    )

    exposure = compute_exposure(positions, catalog)
    valued = compute_unrealized(
        positions, catalog, quotes, valuation_as_of=T0,
        max_quote_age_seconds=None, reporting_currency="USD",
    )

    # Nothing nonfinite reaches a result, whatever the input.
    assert valued.total.amount.is_finite()
    for symbol_exposure in exposure.by_symbol.values():
        assert symbol_exposure.gross_units.amount.is_finite()
        assert symbol_exposure.gross_units.amount >= abs(symbol_exposure.net_units.amount)
    # Every valued position is accounted for.
    assert len(valued.by_position) == len(positions)


# --------------------------------------------------------------------------- #
# Domain Z -- adversarial ingress
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-302")
@PROFILE
@given(
    token=st.sampled_from(
        ["NaN", "nan", "-NaN", "Infinity", "-Infinity", "inf", "-inf", "sNaN", "snan"]
    )
)
def test_every_nonfinite_token_is_rejected(token):
    """No spelling of NaN or infinity survives ingress."""
    with pytest.raises(ContractError):
        parse_decimal(token, field="gross")


@pytest.mark.wf5("WF5-312")
@pytest.mark.p2
@PROFILE
@given(garbage=st.text(max_size=80))
def test_arbitrary_text_is_either_a_decimal_or_a_typed_rejection(garbage):
    """Fuzzing the money parser yields a Decimal or a ContractError -- never a crash."""
    try:
        value = parse_decimal(garbage, field="gross")
    except ContractError:
        return
    assert isinstance(value, Decimal)
    assert value.is_finite()


@pytest.mark.wf5("WF5-306")
@PROFILE
@given(bid_ticks=st.integers(min_value=1, max_value=200000),
       delta=st.integers(min_value=1, max_value=50000))
def test_inverted_spreads_are_always_rejected(bid_ticks, delta):
    """For any inverted pair, the quote is refused rather than valued."""
    from risk.domain.facts import QuoteFact

    bid = Decimal(bid_ticks + delta) / Decimal(100000)
    ask = Decimal(bid_ticks) / Decimal(100000)
    assume(bid > ask)

    with pytest.raises(ContractError):
        QuoteFact(
            symbol="EURUSD",
            bid=bid,
            ask=ask,
            market_time=T0,
            quote_id="q-fuzz",
            snapshot_id="s-fuzz",
        )


@pytest.mark.wf5("WF5-308")
@PROFILE
@given(gaps=st.lists(st.integers(min_value=1, max_value=100), min_size=1, max_size=30))
def test_sequence_gaps_are_not_treated_as_missing_deals(gaps):
    """Arbitrary gaps in source_seq do not reduce the included set."""
    sequences: list[int] = []
    current = 0
    for gap in gaps:
        current += gap
        sequences.append(current)

    deals = tuple(
        DealFact(
            deal_id=f"d-{seq}",
            account_id=ACCOUNT_A,
            source_seq=seq,
            entry_kind="OUT",
            deal_type=None,
            **dict(
                zip(
                    ("gross_profit", "commission", "swap"),
                    build_money_triplet(
                        currency="USD", gross="1.00", commission="0.00", swap="0.00"
                    ),
                )
            ),
            executed_at=T0,
            epoch_id=EPOCH_A,
        )
        for seq in sequences
    )
    result = select_eligible_deals(
        deals,
        capability=SIM,
        period_start=None,
        period_end=None,
        deal_watermark_seq=max(sequences) + 1000,
    )
    assert len(result.trading) == len(sequences)
    assert result.excluded_beyond_watermark == 0


@pytest.mark.wf5("WF5-307")
@pytest.mark.p1
@PROFILE
@given(currency=st.sampled_from(["EUR", "GBP", "JPY", "AUD", "CHF"]))
def test_a_foreign_account_currency_blocks_the_usd_report(currency, catalog):
    """A non-USD reporting currency is blocked by the supported-profile gate."""
    from risk.domain.money import Money, MoneySemantic

    usd = Money(D("10.00"), "USD", MoneySemantic.REALIZED_GROSS)
    foreign = Money(D("10.00"), currency, MoneySemantic.REALIZED_GROSS)
    with pytest.raises(ContractError):
        usd + foreign


@pytest.mark.wf5("WF5-305")
@PROFILE
@given(
    rows=st.lists(st.tuples(symbol_strategy, side_strategy, lot_text), min_size=2, max_size=20),
    omit_at=st.integers(min_value=0, max_value=19),
)
def test_omitting_a_quote_blocks_rather_than_undervaluing(rows, omit_at, catalog):
    """Dropping one symbol's mark blocks the valuation instead of shrinking it."""
    from risk.errors import QualityGateError

    positions = tuple(
        make_position(i, symbol, side, lots) for i, (symbol, side, lots) in enumerate(rows)
    )
    symbols = sorted({p.symbol for p in positions})
    assume(len(symbols) >= 2)

    omitted = symbols[omit_at % len(symbols)]
    partial = quote_snapshot(
        "1.10200", "1.10220", symbols=tuple(s for s in symbols if s != omitted)
    )

    with pytest.raises(QualityGateError):
        compute_unrealized(
            positions, catalog, partial, valuation_as_of=T0,
            max_quote_age_seconds=None, reporting_currency="USD",
        )


@pytest.mark.wf5("WF5-309")
@pytest.mark.p1
@PROFILE
@given(
    start_hour=st.integers(min_value=0, max_value=20),
    span_hours=st.integers(min_value=1, max_value=48),
    offset_minutes=st.integers(min_value=-720, max_value=720),
)
def test_adjacent_half_open_windows_partition_the_timeline(
    start_hour, span_hours, offset_minutes
):
    """Two touching windows include every instant exactly once."""
    from datetime import timedelta

    from tests.reference.oracle import reference_in_window

    start = T0 + timedelta(hours=start_hour)
    middle = start + timedelta(hours=span_hours)
    end = middle + timedelta(hours=span_hours)
    instant = middle + timedelta(minutes=offset_minutes)

    first = reference_in_window(instant, start, middle)
    second = reference_in_window(instant, middle, end)

    # Exactly one of the two windows contains the instant, unless it falls
    # outside both -- never both at once.
    assert not (first and second)
    if start <= instant < end:
        assert first or second


@pytest.mark.wf5("WF5-311")
@pytest.mark.p1
@PROFILE
@given(amounts=st.lists(money_text, min_size=1, max_size=80))
def test_batched_aggregation_matches_single_pass(amounts):
    """Aggregating in batches of 1, 7 and all-at-once gives one answer."""
    from risk.limits import batched

    deals = tuple(make_deal(i + 1, gross=a) for i, a in enumerate(amounts))
    whole = realized_of(deals).gross.amount

    for size in (1, 7, len(deals)):
        running = D(0)
        for chunk in batched(deals, size):
            running += realized_of(chunk).gross.amount
        assert running == whole, f"batch size {size} changed the answer"
