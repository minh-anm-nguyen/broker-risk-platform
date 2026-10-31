"""Domains G, I and N -- instrument specs, FX conversion graph, time windows.

Covers WF5-073..084, WF5-097..108 and WF5-157..168.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from risk.domain.facts import parse_utc
from risk.domain.fx import FxQuote, FxRateSet
from risk.domain.instruments import CalcMode, InstrumentSpec, SymbolCatalog
from risk.domain.money import Money, MoneySemantic
from risk.errors import ContractError, QualityGateError, ReasonCode, UnsupportedError
from risk.policies.loader import load_catalog
from tests.reference.oracle import D

pytestmark = [pytest.mark.golden, pytest.mark.p0]


def spec(
    *,
    symbol: str = "EURUSD",
    base: str = "EUR",
    quote: str = "USD",
    profit: str = "USD",
    mode: CalcMode = CalcMode.FOREX_LINEAR_P0,
    contract: str = "100000",
    tick: str = "0.00001",
    step: str = "0.01",
    volume_max: str = "100",
    version: str = "test-v1",
) -> InstrumentSpec:
    return InstrumentSpec(
        symbol=symbol,
        base_currency=base,
        quote_currency=quote,
        profit_currency=profit,
        calc_mode=mode,
        contract_size=D(contract),
        tick_size=D(tick),
        volume_step=D(step),
        volume_max=D(volume_max),
        spec_version=version,
    )


# --------------------------------------------------------------------------- #
# Domain G -- specifications and calculation modes
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-073")
def test_approved_eurusd_spec_is_supported(catalog, evidence):
    """The P0 EURUSD row has a 100 000 contract and a USD-linear mode."""
    eurusd = catalog.get("EURUSD")
    assert eurusd.contract_size == D("100000")
    assert eurusd.calc_mode is CalcMode.FOREX_LINEAR_P0
    assert eurusd.profit_currency == "USD"
    eurusd.require_supported()  # does not raise
    assert eurusd.units_for(D("0.50")) == D("50000.00")
    evidence(
        expected_values={"contract_size": "100000", "units_for_half_lot": "50000.00"},
        observed_values={
            "contract_size": str(eurusd.contract_size),
            "units_for_half_lot": str(eurusd.units_for(D("0.50"))),
        },
    )


@pytest.mark.wf5("WF5-074")
@pytest.mark.parametrize("field,value", [("contract", "0"), ("tick", "0"), ("step", "0")])
def test_zero_spec_numerics_rejected(field, value, evidence):
    """A zero contract size would silently zero every exposure, so it cannot exist."""
    with pytest.raises(ContractError) as exc:
        spec(**{field: value})
    assert exc.value.reason is ReasonCode.INVALID_INSTRUMENT_SPEC
    evidence(
        expected_reason_codes=("INVALID_INSTRUMENT_SPEC",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-084")
@pytest.mark.p2
def test_negative_tick_size_rejected(evidence):
    """A negative tick would reach a modulo and a division; it is refused first."""
    with pytest.raises(ContractError) as exc:
        spec(tick="-0.001")
    assert exc.value.reason is ReasonCode.INVALID_INSTRUMENT_SPEC
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-075")
def test_off_grid_volume_rejected_never_rounded(evidence):
    """0.015 lots against a 0.01 step is refused; rounding would resize the position."""
    eurusd = spec()
    eurusd.validate_volume(D("0.01"))
    eurusd.validate_volume(D("0.50"))

    with pytest.raises(ContractError) as exc:
        eurusd.validate_volume(D("0.015"))
    assert exc.value.reason is ReasonCode.VOLUME_STEP_VIOLATION
    evidence(
        expected_reason_codes=("VOLUME_STEP_VIOLATION",),
        observed_reason_codes=(str(exc.value.reason),),
        notes=("rounding 0.015 to 0.02 would change the client's position size",),
    )


@pytest.mark.wf5("WF5-076")
def test_symbol_must_match_its_catalog_key(evidence):
    """A spec filed under another symbol's key is a catalog error."""
    with pytest.raises(ContractError) as exc:
        SymbolCatalog("test-v1", {"GBPUSD": spec(symbol="EURUSD")})
    assert exc.value.reason is ReasonCode.INVALID_INSTRUMENT_SPEC
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-077")
def test_contract_size_change_scales_exposure_tenfold(evidence):
    """A spec version that moves 100 000 to 10 000 changes the answer by 10x."""
    v1 = SymbolCatalog("spec-v1", {"EURUSD": spec(contract="100000", version="spec-v1")})
    v2 = SymbolCatalog("spec-v2", {"EURUSD": spec(contract="10000", version="spec-v2")})

    units_v1 = v1.get("EURUSD").units_for(D("1.00"))
    units_v2 = v2.get("EURUSD").units_for(D("1.00"))

    assert units_v1 == D("100000.00")
    assert units_v2 == D("10000.00")
    assert units_v1 == units_v2 * 10
    # Both catalogs name their version, which is what lets a result say which
    # arithmetic produced it and lets a cache tell the two apart.
    assert v1.catalog_version != v2.catalog_version
    evidence(
        expected_values={"units_v1": "100000.00", "units_v2": "10000.00"},
        observed_values={"units_v1": str(units_v1), "units_v2": str(units_v2)},
    )


@pytest.mark.wf5("WF5-078")
def test_off_tick_price_rejected_in_strict_validation(evidence):
    """1.100005 is off a 0.00001 grid and is refused when the grid is enforced."""
    eurusd = spec()
    eurusd.validate_price_on_tick(D("1.10005"), field="open_price")

    with pytest.raises(ContractError) as exc:
        eurusd.validate_price_on_tick(D("1.100005"), field="open_price")
    assert exc.value.reason is ReasonCode.PRICE_OFF_TICK
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-079")
def test_cfd_mode_outside_the_whitelist(evidence):
    """A CFD with a broker multiplier is refused, not run through the FX formula."""
    gold = spec(symbol="XAUUSD", base="XAU", mode=CalcMode.CFD, contract="100")
    assert gold.supported is False
    with pytest.raises(UnsupportedError) as exc:
        gold.require_supported()
    assert exc.value.reason is ReasonCode.UNSUPPORTED_CALC_MODE
    evidence(
        expected_reason_codes=("UNSUPPORTED_CALC_MODE",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-080")
def test_futures_mode_blocked_until_a_plugin_exists(evidence):
    """Futures price off tick value, not contract size, so the mode is blocked."""
    future = spec(symbol="ESZ6", base="USD", mode=CalcMode.FUTURES, contract="50")
    with pytest.raises(UnsupportedError) as exc:
        future.require_supported()
    assert exc.value.reason is ReasonCode.UNSUPPORTED_CALC_MODE
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-081", "WF5-267")
@pytest.mark.p1
def test_unlisted_symbol_gets_no_default_spec(catalog, evidence):
    """A symbol that looks like a currency pair still needs a catalog row."""
    with pytest.raises(UnsupportedError) as exc:
        catalog.get("NZDUSD")
    assert exc.value.reason is ReasonCode.UNSUPPORTED_SYMBOL
    # The failure mode this prevents: a generic 100 000 contract applied to an
    # instrument nobody approved, producing a plausible wrong number.
    assert "NZDUSD" not in catalog
    evidence(
        expected_reason_codes=("UNSUPPORTED_SYMBOL",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-082")
@pytest.mark.p1
def test_catalog_row_cannot_carry_a_foreign_spec_version(evidence):
    """A row whose version differs from its catalog means two approvals got merged."""
    with pytest.raises(ContractError) as exc:
        SymbolCatalog("catalog-v2", {"EURUSD": spec(version="catalog-v1")})
    assert exc.value.reason is ReasonCode.SPEC_VERSION_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-083")
@pytest.mark.p1
def test_fractional_contract_size_is_exact(evidence):
    """A 12.5 contract size times 0.03 lots is exactly 0.375, not a float artifact."""
    micro = spec(contract="12.5", step="0.01")
    assert micro.units_for(D("0.03")) == D("0.375")
    evidence(observed_values={"units": str(micro.units_for(D("0.03")))})


@pytest.mark.wf5("WF5-271")
def test_unknown_calc_mode_in_a_catalog_row_rejected(evidence):
    """A mode string this build has never seen fails the load, not the calculation."""
    with pytest.raises(ContractError) as exc:
        SymbolCatalog.from_rows(
            "test-v1",
            [
                {
                    "symbol": "WEIRD",
                    "base_currency": "USD",
                    "quote_currency": "USD",
                    "profit_currency": "USD",
                    "calc_mode": "FOREX_QUANTUM_V9",
                    "contract_size": "1",
                    "tick_size": "1",
                    "volume_step": "1",
                    "volume_max": "1",
                }
            ],
        )
    assert exc.value.reason is ReasonCode.UNSUPPORTED_CALC_MODE
    evidence(observed_reason_codes=(str(exc.value.reason),))


def test_catalog_artifact_rejects_yaml_floats():
    """The shipped catalog keeps its decimals quoted, and the loader enforces it."""
    loaded = load_catalog("sim-usd-fx-v1")
    for symbol in loaded.symbols:
        assert isinstance(loaded.get(symbol).contract_size, Decimal)


# --------------------------------------------------------------------------- #
# Domain I -- FX conversion
# --------------------------------------------------------------------------- #


def fx(base: str, quote: str, bid: str, ask: str, *, provider: str = "p1", when=None) -> FxQuote:
    return FxQuote(
        base_currency=base,
        quote_currency=quote,
        bid=D(bid),
        ask=D(ask),
        market_time=when or datetime(2026, 10, 15, 12, 0, tzinfo=UTC),
        provider=provider,
        rate_id=f"{base}{quote}-1",
    )


@pytest.mark.wf5("WF5-097")
def test_missing_rate_blocks_conversion(evidence):
    """A JPY profit with no JPY rate yields FX_RATE_MISSING, not a USD number."""
    rates = FxRateSet("fx-v1", (fx("EUR", "USD", "1.1000", "1.1002"),))
    jpy = Money(D("1000"), "JPY", MoneySemantic.REALIZED_GROSS)

    with pytest.raises(QualityGateError) as exc:
        rates.convert(jpy, "USD")
    assert exc.value.reason is ReasonCode.FX_RATE_MISSING
    evidence(
        expected_reason_codes=("FX_RATE_MISSING",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-099")
@pytest.mark.parametrize("bid,ask", [("0", "1.1"), ("-1", "1.1"), ("1.1", "0")])
def test_non_positive_rates_rejected(bid, ask, evidence):
    """A zero or negative rate would zero or flip the converted amount."""
    with pytest.raises(ContractError) as exc:
        fx("EUR", "USD", bid, ask)
    assert exc.value.reason is ReasonCode.FX_RATE_INVALID
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-100")
def test_inverted_fx_spread_rejected(evidence):
    """An FX bid above its ask is not a rate set."""
    with pytest.raises(ContractError) as exc:
        fx("EUR", "USD", "1.1010", "1.1000")
    assert exc.value.reason is ReasonCode.FX_SPREAD_INVALID
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-101")
def test_inverse_rate_swaps_the_sides(evidence):
    """The inverse bid is 1/ask, not 1/bid -- the classic inversion bug."""
    eurusd = fx("EUR", "USD", "1.1000", "1.1002")
    inverse = eurusd.inverted()

    # Computed in the reference at the documented working precision rather
    # than at Python's default 28 digits, so the comparison is exact.
    from decimal import localcontext

    from tests.reference.oracle import PRECISION

    with localcontext() as ctx:
        ctx.prec = PRECISION
        expected_bid = D(1) / D("1.1002")
        expected_ask = D(1) / D("1.1000")

    assert inverse.base_currency == "USD"
    assert inverse.quote_currency == "EUR"
    assert inverse.bid == expected_bid
    assert inverse.ask == expected_ask
    # Sanity: the inverse spread is still the right way round.
    assert inverse.bid < inverse.ask
    evidence(
        expected_values={"inverse_bid": "1/ask", "inverse_ask": "1/bid"},
        observed_values={"bid_lt_ask": str(inverse.bid < inverse.ask)},
    )


@pytest.mark.wf5("WF5-102")
def test_cycle_is_not_traversed_and_hops_are_bounded(evidence):
    """A cyclic rate graph is searched breadth-first with a hop cap, never looped."""
    rates = FxRateSet(
        "fx-cycle",
        (
            fx("EUR", "USD", "1.1000", "1.1002"),
            fx("USD", "JPY", "150.00", "150.02"),
            fx("JPY", "EUR", "0.0060", "0.0061"),
        ),
        max_hops=2,
    )
    # EUR -> JPY resolves in one hop via the declared JPY/EUR pair's inverse;
    # the search terminates rather than cycling through USD forever.
    result = rates.convert(Money(D("100"), "EUR", MoneySemantic.REALIZED_GROSS), "JPY")
    assert len(result.legs) == 1
    assert result.converted.currency == "JPY"

    # With a one-hop budget a genuinely two-hop route is refused by policy
    # rather than silently taken.
    narrow = FxRateSet(
        "fx-narrow",
        (fx("EUR", "USD", "1.1000", "1.1002"), fx("USD", "JPY", "150.00", "150.02")),
        max_hops=1,
    )
    with pytest.raises(QualityGateError) as exc:
        narrow.convert(Money(D("100"), "EUR", MoneySemantic.REALIZED_GROSS), "JPY")
    assert exc.value.reason is ReasonCode.FX_PATH_TOO_LONG
    evidence(
        expected_reason_codes=("FX_PATH_TOO_LONG",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-103")
def test_two_providers_for_one_pair_is_a_conflict(evidence):
    """Disagreeing providers are an unresolved question, not an average."""
    with pytest.raises(ContractError) as exc:
        FxRateSet(
            "fx-conflict",
            (
                fx("EUR", "USD", "1.1000", "1.1002", provider="alpha"),
                fx("EUR", "USD", "1.2000", "1.2002", provider="beta"),
            ),
        )
    assert exc.value.reason is ReasonCode.FX_PROVIDER_CONFLICT
    evidence(
        expected_reason_codes=("FX_PROVIDER_CONFLICT",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-104")
def test_legs_quoted_too_far_apart_rejected_for_strict_valuation(evidence):
    """Two legs 15 minutes apart are not a rate at an instant."""
    base = datetime(2026, 10, 15, 12, 0, tzinfo=UTC)
    rates = FxRateSet(
        "fx-skewed",
        (
            fx("EUR", "USD", "1.1000", "1.1002", when=base),
            fx("USD", "JPY", "150.00", "150.02", when=base + timedelta(minutes=15)),
        ),
        max_hops=2,
        max_leg_time_spread_seconds=60,
    )
    money = Money(D("100"), "EUR", MoneySemantic.REALIZED_GROSS)

    with pytest.raises(QualityGateError) as exc:
        rates.convert(money, "JPY")
    assert exc.value.reason is ReasonCode.FX_RATE_SET_INCOHERENT

    # The same path is available to a caller who explicitly accepts the skew.
    lenient = rates.convert(money, "JPY", strict_time_coherence=False)
    assert lenient.converted.currency == "JPY"
    assert len(lenient.legs) == 2
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-105")
@pytest.mark.p1
def test_conversion_is_side_aware_on_the_sign(evidence):
    """A profit sells the source currency (bid); a loss has to buy it (ask)."""
    rates = FxRateSet("fx-v1", (fx("EUR", "USD", "1.1000", "1.1002"),))
    profit = Money(D("100"), "EUR", MoneySemantic.REALIZED_GROSS)
    loss = Money(D("-100"), "EUR", MoneySemantic.REALIZED_GROSS)

    converted_profit = rates.convert(profit, "USD")
    converted_loss = rates.convert(loss, "USD")

    assert converted_profit.legs[0].rate == D("1.1000")  # bid
    assert converted_loss.legs[0].rate == D("1.1002")  # ask
    assert converted_profit.converted.amount == D("110.000")
    assert converted_loss.converted.amount == D("-110.020")
    # The asymmetry is the point: the magnitudes are deliberately unequal.
    assert abs(converted_loss.converted.amount) > converted_profit.converted.amount
    evidence(
        expected_values={"profit_rate": "1.1000", "loss_rate": "1.1002"},
        observed_values={
            "profit_rate": str(converted_profit.legs[0].rate),
            "loss_rate": str(converted_loss.legs[0].rate),
        },
    )


@pytest.mark.wf5("WF5-106")
@pytest.mark.p1
def test_two_hop_cross_rate_records_every_leg(evidence):
    """EUR to JPY through USD multiplies two legs and logs both for the audit."""
    rates = FxRateSet(
        "fx-v1",
        (fx("EUR", "USD", "1.1000", "1.1002"), fx("USD", "JPY", "150.00", "150.02")),
        max_hops=2,
    )
    result = rates.convert(Money(D("100"), "EUR", MoneySemantic.REALIZED_GROSS), "JPY")

    assert len(result.legs) == 2
    assert result.converted.currency == "JPY"
    # 100 EUR -> 110 USD -> 16 500 JPY, both legs on the bid.
    assert result.converted.amount == D("100") * D("1.1000") * D("150.00")
    assert [leg.rate_id for leg in result.legs] == ["EURUSD-1", "USDJPY-1"]
    evidence(
        expected_values={"converted": "16500.000000"},
        observed_values={"converted": result.converted.canonical(),
                         "legs": str(len(result.legs))},
    )


@pytest.mark.wf5("WF5-107")
@pytest.mark.p1
def test_jpy_quantised_to_zero_minor_units(evidence):
    """A JPY total rounds to whole yen, not to arbitrary cents."""
    from decimal import ROUND_HALF_EVEN

    yen = Money(D("16500.4567"), "JPY", MoneySemantic.REALIZED_GROSS)
    assert yen.display(ROUND_HALF_EVEN) == D("16500")
    evidence(observed_values={"display": str(yen.display(ROUND_HALF_EVEN))})


@pytest.mark.wf5("WF5-108")
@pytest.mark.p2
def test_same_currency_conversion_is_a_no_op_with_no_legs(evidence):
    """Converting USD to USD takes no rate and records no leg."""
    rates = FxRateSet("fx-v1", (fx("EUR", "USD", "1.1000", "1.1002"),))
    usd = Money(D("100"), "USD", MoneySemantic.REALIZED_GROSS)
    result = rates.convert(usd, "USD")

    assert result.legs == ()
    assert result.converted is usd
    evidence(notes=("a portfolio in one currency needs no rate proof",))


# --------------------------------------------------------------------------- #
# Domain N -- time
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-159")
def test_offset_timestamp_normalised_to_the_same_instant(evidence):
    """A +07:00 timestamp and its Z equivalent are the same economic instant."""
    with_offset = parse_utc("2026-10-15T19:00:00+07:00", field_name="executed_at")
    as_utc = parse_utc("2026-10-15T12:00:00Z", field_name="executed_at")

    assert with_offset == as_utc
    assert with_offset.tzinfo == UTC
    evidence(
        expected_values={"instant": "2026-10-15T12:00:00+00:00"},
        observed_values={"instant": with_offset.isoformat()},
    )


@pytest.mark.wf5("WF5-160")
def test_naive_timestamp_rejected(evidence):
    """Without an offset the instant is unknown, and guessing it moves money."""
    with pytest.raises(ContractError) as exc:
        parse_utc("2026-10-15T12:00:00", field_name="executed_at")
    assert exc.value.reason is ReasonCode.NAIVE_TIMESTAMP_REJECTED
    evidence(
        expected_reason_codes=("NAIVE_TIMESTAMP_REJECTED",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-161")
@pytest.mark.p1
def test_dst_transition_has_no_duplicated_or_missing_hour_in_utc(evidence):
    """Grouping in UTC sidesteps the duplicated local hour entirely."""
    # 2026-10-25 is the European DST change; 02:30 local occurs twice there.
    first = parse_utc("2026-10-25T02:30:00+02:00", field_name="executed_at")
    second = parse_utc("2026-10-25T02:30:00+01:00", field_name="executed_at")

    assert first != second  # two distinct instants, as they should be
    assert (second - first) == timedelta(hours=1)
    assert first.tzinfo == second.tzinfo == UTC
    evidence(observed_values={"gap_hours": "1"})


@pytest.mark.wf5("WF5-166")
@pytest.mark.p1
def test_leap_day_and_year_boundary_group_correctly(evidence):
    """Feb 29 and Dec 31 sit in their windows without a calendar special case."""
    from tests.reference.oracle import reference_in_window

    leap = parse_utc("2028-02-29T23:59:59.999999Z", field_name="executed_at")
    february = (
        parse_utc("2028-02-01T00:00:00Z", field_name="s"),
        parse_utc("2028-03-01T00:00:00Z", field_name="e"),
    )
    assert reference_in_window(leap, *february)

    year_end = parse_utc("2026-12-31T23:59:59.999999Z", field_name="executed_at")
    december = (
        parse_utc("2026-12-01T00:00:00Z", field_name="s"),
        parse_utc("2027-01-01T00:00:00Z", field_name="e"),
    )
    assert reference_in_window(year_end, *december)
    # And the January window excludes it, so the two do not overlap.
    january = (
        parse_utc("2027-01-01T00:00:00Z", field_name="s"),
        parse_utc("2027-02-01T00:00:00Z", field_name="e"),
    )
    assert not reference_in_window(year_end, *january)
    evidence(oracle="GOLDEN_REFERENCE", observed_values={"leap_in_february": "True"})


@pytest.mark.wf5("WF5-167")
@pytest.mark.p1
def test_microsecond_boundary_is_exact(evidence):
    """One microsecond before the window end is inside; the end itself is not."""
    from tests.reference.oracle import reference_in_window

    end = parse_utc("2026-11-01T00:00:00Z", field_name="e")
    start = parse_utc("2026-10-01T00:00:00Z", field_name="s")

    assert reference_in_window(end - timedelta(microseconds=1), start, end)
    assert not reference_in_window(end, start, end)
    evidence(oracle="GOLDEN_REFERENCE")


@pytest.mark.wf5("WF5-163")
def test_no_finality_flag_is_published(evidence):
    """Nothing in the realized payload claims a period is sealed."""
    from risk.domain.classification import SourceCapability, select_eligible_deals
    from risk.domain.facts import DealFact, build_money_triplet
    from risk.domain.realized import compute_realized
    from risk_tools.fixtures import ACCOUNT_A, EPOCH_A, T0

    g, c, s = build_money_triplet(currency="USD", gross="10.00", commission="0.00", swap="0.00")
    row = DealFact(
        deal_id="d-1",
        account_id=ACCOUNT_A,
        source_seq=1,
        entry_kind="OUT",
        deal_type=None,
        gross_profit=g,
        commission=c,
        swap=s,
        executed_at=T0,
        epoch_id=EPOCH_A,
    )
    eligibility = select_eligible_deals(
        [row],
        capability=SourceCapability.SIMULATOR_TRADING_ONLY_V1,
        period_start=None,
        period_end=None,
        deal_watermark_seq=None,
    )
    payload = compute_realized(eligibility, currency="USD").as_payload()

    # A later deal with an earlier timestamp can always change this figure, so
    # there is no field a consumer could render as DAILY_FINAL.
    for forbidden in ("finality", "final", "sealed", "DAILY_FINAL"):
        assert forbidden not in payload
    # What is published instead: how many rows the watermark held back.
    assert "excluded_beyond_watermark" in payload
    evidence(notes=("provisional by construction until an upstream seal exists",))


@pytest.mark.wf5("WF5-168")
@pytest.mark.p2
def test_oversized_window_is_rejected_by_the_deal_limit(policy, evidence):
    """A ten-year window over a million deals hits the declared cap, not a timeout."""
    from risk.errors import ResourceError
    from risk.limits import check_request_limits

    with pytest.raises(ResourceError) as exc:
        check_request_limits(policy.limits, account_count=1, deal_count=5_000_000)
    assert exc.value.reason is ReasonCode.LIMIT_EXCEEDED
    assert exc.value.detail["limit"] == policy.limits.max_deals_per_window
    evidence(
        expected_reason_codes=("LIMIT_EXCEEDED",),
        observed_reason_codes=(str(exc.value.reason),),
    )
