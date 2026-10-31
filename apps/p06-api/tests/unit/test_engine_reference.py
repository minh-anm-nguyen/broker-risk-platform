"""The Project 05 reference engine, checked against independent arithmetic.

The oracle here is hand-computed, not a second call to the same code: 0.5 lots
of a 100,000-unit contract is 50,000 units; 60.00 gross with -3.50 commission
and -0.50 swap nets 56.00; a 5-pip move on 50,000 units is 25.00.

The blocked cases matter just as much: an unproven quote cut, an incomplete
deal watermark or an unsupported profile must never turn into a number.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from risk_engine import (
    AccountFact,
    CoverageDeclaration,
    DealFact,
    InstrumentSpec,
    PositionFact,
    QuoteFact,
    RiskCut,
    RiskEngine,
    RiskEngineError,
    RiskInputBundle,
    RiskRequest,
    format_money,
    format_units,
)

pytestmark = pytest.mark.unit

ALL = ("EXPOSURE", "REALIZED", "UNREALIZED")
WINDOW = ("2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z")


def eurusd(**overrides) -> InstrumentSpec:
    base = {
        "symbol": "EURUSD",
        "base_currency": "EUR",
        "quote_currency": "USD",
        "profit_currency": "USD",
        "calc_mode": "FOREX_LINEAR",
        "contract_size": Decimal("100000"),
        "tick_size": Decimal("0.00001"),
        "volume_step": Decimal("0.01"),
        "spec_version": "sim-usd-fx-v1",
    }
    base.update(overrides)
    return InstrumentSpec(**base)  # type: ignore[arg-type]


def position(position_id="P-1", account="00042", **overrides) -> PositionFact:
    base = {
        "position_id": position_id,
        "account_id": account,
        "symbol": "EURUSD",
        "side": "BUY",
        "lots": Decimal("0.5"),
        "open_price": Decimal("1.1000"),
        "source_status": "OPEN",
        "presence": "PRESENT",
        "published_scan_id": "scan-P",
    }
    base.update(overrides)
    return PositionFact(**base)  # type: ignore[arg-type]


def deal(deal_id="D-1", account="00042", **overrides) -> DealFact:
    base = {
        "deal_id": deal_id,
        "account_id": account,
        "source_seq": 1050,
        "entry_kind": "OUT",
        "gross_profit": Decimal("60.00"),
        "commission": Decimal("-3.50"),
        "swap": Decimal("-0.50"),
        "executed_at_utc": "2026-10-15T10:00:00Z",
    }
    base.update(overrides)
    return DealFact(**base)  # type: ignore[arg-type]


def bundle(
    *,
    positions=(),
    deals=(),
    quotes=None,
    instruments=None,
    coverage=None,
    consistency="COMPATIBLE",
    quote_snapshot_id="quote-S1030",
) -> RiskInputBundle:
    if quotes is None:
        quotes = (
            QuoteFact(
                symbol="EURUSD",
                bid=Decimal("1.1005"),
                ask=Decimal("1.1007"),
                market_time_utc="2026-10-30T00:00:00Z",
                quote_id="q-1",
                snapshot_id="quote-S1030",
            ),
        )
    return RiskInputBundle(
        cut=RiskCut(
            source_id="sim-broker-a",
            epoch_id="E1",
            account_scan_id="scan-A",
            position_scan_id="scan-P",
            deal_watermark_seq=1050,
            quote_snapshot_id=quote_snapshot_id,
            consistency=consistency,
        ),
        accounts=(AccountFact("00042", "USD", "PRESENT", "scan-A"),),
        positions=tuple(positions),
        deals=tuple(deals),
        quotes=tuple(quotes),
        instruments=tuple(instruments if instruments is not None else (eurusd(),)),
        coverage=coverage
        or CoverageDeclaration(
            deals_complete=True,
            fee_coverage="NOT_AVAILABLE",
            quote_cut_proven=True,
            accounts_published=True,
            positions_published=True,
        ),
        symbol_catalog_version="sim-usd-fx-v1",
    )


def request(**overrides) -> RiskRequest:
    base = {
        "source_id": "sim-broker-a",
        "epoch_id": "E1",
        "account_ids": ("00042",),
        "components": ALL,
        "risk_policy_version": "risk-policy-v1",
        "symbol_catalog_version": "sim-usd-fx-v1",
        "period_start_utc": WINDOW[0],
        "period_end_utc": WINDOW[1],
        "strict_composite": True,
    }
    base.update(overrides)
    return RiskRequest(**base)  # type: ignore[arg-type]


@pytest.fixture
def engine() -> RiskEngine:
    return RiskEngine()


class TestGoldenFixtures:
    def test_fx_ready_001_exposure(self, engine):
        result = engine.compute(request(components=("EXPOSURE",)), bundle(positions=(position(),)))
        assert result.status == "READY"
        bucket = result.exposure.by_symbol["EURUSD"]
        assert bucket.unit_currency == "EUR"
        assert format_units(bucket.long_units) == "50000"
        assert format_units(bucket.short_units) == "0"
        assert format_units(bucket.gross_units) == "50000"
        assert format_units(bucket.net_units) == "50000"

    def test_fx_realized_002_signed_fees(self, engine):
        result = engine.compute(request(components=("REALIZED",)), bundle(deals=(deal(),)))
        realized = result.realized
        assert result.status == "READY"
        assert format_money(realized.gross, "USD") == "60.00"
        assert format_money(realized.commission, "USD") == "-3.50"
        assert format_money(realized.swap, "USD") == "-0.50"
        assert format_money(realized.net_excluding_fee, "USD") == "56.00"
        assert realized.fee_coverage == "NOT_AVAILABLE"
        assert "FEE_COVERAGE_UNAVAILABLE" in result.reason_codes

    def test_unrealized_uses_bid_for_a_long(self, engine):
        """(1.1005 - 1.1000) * 50,000 = 25.00, marked at the bid."""
        result = engine.compute(request(components=("UNREALIZED",)), bundle(positions=(position(),)))
        assert format_money(result.unrealized.total, "USD") == "25.00"

    def test_unrealized_uses_ask_for_a_short(self, engine):
        """(1.1010 - 1.1007) * 20,000 = 6.00, marked at the ask."""
        short = position(side="SELL", lots=Decimal("0.2"), open_price=Decimal("1.1010"))
        result = engine.compute(request(components=("UNREALIZED",)), bundle(positions=(short,)))
        assert format_money(result.unrealized.total, "USD") == "6.00"

    def test_composite_is_the_sum_of_the_money_components(self, engine):
        result = engine.compute(request(), bundle(positions=(position(),), deals=(deal(),)))
        assert result.status == "READY"
        assert format_money(result.composite_total, "USD") == "81.00"

    def test_fx_empty_007_proven_zero_is_not_unknown(self, engine):
        """A proven empty day is an explicit zero, distinct from missing data."""
        result = engine.compute(request(components=("REALIZED",)), bundle(deals=()))
        assert result.realized.status == "READY"
        assert format_money(result.realized.net_excluding_fee, "USD") == "0.00"
        assert result.realized.deals_counted == 0


class TestDeterminism:
    def test_the_same_inputs_produce_the_same_digests(self, engine):
        args = (request(), bundle(positions=(position(),), deals=(deal(),)))
        first = engine.compute(*args)
        second = engine.compute(*args)
        assert first.input_digest == second.input_digest
        assert first.output_digest == second.output_digest
        assert first.calculation_id != second.calculation_id

    def test_position_order_does_not_change_the_digest(self, engine):
        a = position("P-1")
        b = position("P-2", lots=Decimal("0.3"))
        forward = engine.compute(request(), bundle(positions=(a, b), deals=(deal(),)))
        backward = engine.compute(request(), bundle(positions=(b, a), deals=(deal(),)))
        assert forward.input_digest == backward.input_digest
        assert forward.output_digest == backward.output_digest

    def test_a_changed_amount_changes_the_digest(self, engine):
        base = engine.compute(request(), bundle(positions=(position(),), deals=(deal(),)))
        nudged = engine.compute(
            request(),
            bundle(positions=(position(),), deals=(deal(gross_profit=Decimal("60.01")),)),
        )
        assert base.output_digest != nudged.output_digest


class TestWindowSemantics:
    def test_the_interval_is_half_open(self, engine):
        """IT06-10: a deal exactly at the boundary is counted once, at the start."""
        at_start = deal("D-start", executed_at_utc="2026-10-01T00:00:00Z")
        at_end = deal("D-end", executed_at_utc="2026-11-01T00:00:00Z")
        result = engine.compute(
            request(components=("REALIZED",)), bundle(deals=(at_start, at_end))
        )
        assert result.realized.deals_counted == 1

    def test_deals_outside_the_window_are_excluded(self, engine):
        outside = deal("D-old", executed_at_utc="2026-09-30T23:59:59Z")
        result = engine.compute(request(components=("REALIZED",)), bundle(deals=(outside,)))
        assert result.realized.deals_counted == 0

    def test_an_empty_interval_is_refused(self, engine):
        with pytest.raises(RiskEngineError):
            engine.compute(
                request(
                    components=("REALIZED",),
                    period_start_utc="2026-10-01T00:00:00Z",
                    period_end_utc="2026-10-01T00:00:00Z",
                ),
                bundle(deals=(deal(),)),
            )

    def test_a_missing_period_is_refused(self, engine):
        with pytest.raises(RiskEngineError):
            engine.compute(
                request(components=("REALIZED",), period_start_utc=None, period_end_utc=None),
                bundle(deals=(deal(),)),
            )

    def test_a_non_utc_instant_is_refused(self, engine):
        with pytest.raises(RiskEngineError):
            engine.compute(
                request(components=("REALIZED",), period_start_utc="2026-10-01T00:00:00+02:00"),
                bundle(deals=(deal(),)),
            )


class TestBlockers:
    def test_an_unproven_quote_cut_blocks_unrealized(self, engine):
        """FX-PARTIAL-003: no quote proof means no valuation, not a zero."""
        coverage = CoverageDeclaration(
            deals_complete=True,
            fee_coverage="NOT_AVAILABLE",
            quote_cut_proven=False,
            accounts_published=True,
            positions_published=True,
        )
        result = engine.compute(
            request(strict_composite=False),
            bundle(positions=(position(),), deals=(deal(),), coverage=coverage),
        )
        assert result.unrealized.status == "BLOCKED"
        assert result.unrealized.reason_code == "QUOTE_CUT_UNPROVEN"
        assert result.unrealized.total is None
        assert result.status == "DEGRADED"
        assert result.composite_total is None

    def test_strict_turns_the_same_inputs_into_a_blocked_result(self, engine):
        coverage = CoverageDeclaration(
            deals_complete=True,
            fee_coverage="NOT_AVAILABLE",
            quote_cut_proven=False,
            accounts_published=True,
            positions_published=True,
        )
        result = engine.compute(
            request(strict_composite=True),
            bundle(positions=(position(),), deals=(deal(),), coverage=coverage),
        )
        assert result.status == "BLOCKED"
        assert result.composite_total is None

    def test_an_incomplete_deal_watermark_blocks_realized(self, engine):
        coverage = CoverageDeclaration(
            deals_complete=False,
            fee_coverage="NOT_AVAILABLE",
            quote_cut_proven=True,
            accounts_published=True,
            positions_published=True,
        )
        result = engine.compute(
            request(components=("REALIZED",), strict_composite=False),
            bundle(deals=(deal(),), coverage=coverage),
        )
        assert result.realized.status == "BLOCKED"
        assert result.realized.reason_code == "DEAL_WATERMARK_UNPROVEN"
        assert result.realized.net_excluding_fee is None

    def test_an_unpublished_position_scan_blocks_exposure(self, engine):
        coverage = CoverageDeclaration(
            deals_complete=True,
            fee_coverage="NOT_AVAILABLE",
            quote_cut_proven=True,
            accounts_published=True,
            positions_published=False,
        )
        result = engine.compute(
            request(components=("EXPOSURE",), strict_composite=False),
            bundle(positions=(position(),), coverage=coverage),
        )
        assert result.exposure.status == "BLOCKED"
        assert result.exposure.reason_code == "P03_UNPUBLISHED"

    def test_a_stale_quote_blocks_unrealized(self, engine):
        coverage = CoverageDeclaration(
            deals_complete=True,
            fee_coverage="NOT_AVAILABLE",
            quote_cut_proven=True,
            accounts_published=True,
            positions_published=True,
            quote_max_age_seconds=30,
            quote_age_seconds=90,
        )
        result = engine.compute(
            request(components=("UNREALIZED",), strict_composite=False),
            bundle(positions=(position(),), coverage=coverage),
        )
        assert result.unrealized.reason_code == "STALE_MARK"

    def test_a_quote_from_another_snapshot_does_not_count(self, engine):
        other = QuoteFact(
            symbol="EURUSD",
            bid=Decimal("1.1005"),
            ask=Decimal("1.1007"),
            market_time_utc="2026-10-30T00:00:00Z",
            quote_id="q-2",
            snapshot_id="quote-S9999",
        )
        result = engine.compute(
            request(components=("UNREALIZED",), strict_composite=False),
            bundle(positions=(position(),), quotes=(other,)),
        )
        assert result.unrealized.reason_code == "QUOTE_CUT_UNPROVEN"

    def test_a_non_usd_profit_currency_is_refused_not_converted(self, engine):
        """P0 has no FX path, so it says so instead of inventing a rate."""
        jpy = eurusd(symbol="EURUSD", profit_currency="JPY")
        result = engine.compute(
            request(components=("UNREALIZED",), strict_composite=False),
            bundle(positions=(position(),), instruments=(jpy,)),
        )
        assert result.unrealized.reason_code == "FX_RATE_MISSING"

    def test_an_unsupported_calc_mode_is_refused(self, engine):
        exotic = eurusd(calc_mode="CFD_LEVERAGED")
        result = engine.compute(
            request(components=("EXPOSURE",), strict_composite=False),
            bundle(positions=(position(),), instruments=(exotic,)),
        )
        assert result.status == "UNSUPPORTED"
        assert result.exposure.reason_code == "UNSUPPORTED_CALC_MODE"

    def test_a_missing_instrument_spec_is_refused(self, engine):
        result = engine.compute(
            request(components=("EXPOSURE",), strict_composite=False),
            bundle(positions=(position(),), instruments=()),
        )
        assert result.exposure.reason_code == "UNSUPPORTED_CALC_MODE"

    def test_a_crossed_quote_is_a_data_integrity_conflict(self, engine):
        crossed = QuoteFact(
            symbol="EURUSD",
            bid=Decimal("1.2000"),
            ask=Decimal("1.1000"),
            market_time_utc="2026-10-30T00:00:00Z",
            quote_id="q-x",
            snapshot_id="quote-S1030",
        )
        with pytest.raises(RiskEngineError) as caught:
            engine.compute(
                request(components=("UNREALIZED",)),
                bundle(positions=(position(),), quotes=(crossed,)),
            )
        assert "DATA_INTEGRITY_CONFLICT" in caught.value.reason_codes


class TestCutCompatibility:
    def test_an_independent_cut_forbids_a_composite_total(self, engine):
        """INV06-09: two proven components on different cuts cannot be summed."""
        result = engine.compute(
            request(strict_composite=False),
            bundle(positions=(position(),), deals=(deal(),), consistency="INDEPENDENT"),
        )
        assert result.exposure.status == "READY"
        assert result.realized.status == "READY"
        assert result.unrealized.status == "READY"
        assert result.status == "DEGRADED"
        assert result.composite_total is None
        assert "INCOMPATIBLE_CUT" in result.reason_codes

    def test_strict_refuses_an_independent_cut_entirely(self, engine):
        result = engine.compute(
            request(strict_composite=True),
            bundle(positions=(position(),), deals=(deal(),), consistency="INDEPENDENT"),
        )
        assert result.status == "BLOCKED"
        assert "INCOMPATIBLE_CUT" in result.reason_codes

    def test_a_cut_for_another_epoch_is_refused(self, engine):
        wrong = bundle(positions=(position(),))
        wrong = replace(wrong, cut=replace(wrong.cut, epoch_id="E2"))
        with pytest.raises(RiskEngineError) as caught:
            engine.compute(request(), wrong)
        assert caught.value.code == "INCOMPATIBLE_CUT"

    def test_a_single_money_component_needs_no_cross_cut_proof(self, engine):
        result = engine.compute(
            request(components=("REALIZED",)),
            bundle(deals=(deal(),), consistency="INDEPENDENT"),
        )
        assert result.status == "READY"
        assert result.composite_total is None


class TestScopeAndVersions:
    def test_positions_outside_the_account_filter_are_ignored(self, engine):
        other = position("P-9", account="00099", lots=Decimal("9"))
        result = engine.compute(
            request(components=("EXPOSURE",)), bundle(positions=(position(), other))
        )
        assert format_units(result.exposure.by_symbol["EURUSD"].long_units) == "50000"

    def test_an_empty_account_filter_is_refused(self, engine):
        """An empty selection is never a wildcard over the whole broker."""
        with pytest.raises(RiskEngineError) as caught:
            engine.compute(request(account_ids=()), bundle(positions=(position(),)))
        assert "UNAUTHORIZED_SCOPE" in caught.value.reason_codes

    def test_an_uninstalled_policy_version_is_refused(self, engine):
        with pytest.raises(RiskEngineError) as caught:
            engine.compute(request(risk_policy_version="risk-policy-v99"), bundle())
        assert "POLICY_VERSION_CHANGED" in caught.value.reason_codes

    def test_a_catalog_mismatch_between_request_and_bundle_is_refused(self, engine):
        with pytest.raises(RiskEngineError) as caught:
            engine.compute(request(symbol_catalog_version="other-catalog"), bundle())
        assert "POLICY_VERSION_CHANGED" in caught.value.reason_codes

    def test_no_components_is_refused(self, engine):
        with pytest.raises(RiskEngineError):
            engine.compute(request(components=()), bundle())

    def test_an_unknown_component_is_refused(self, engine):
        with pytest.raises(RiskEngineError):
            engine.compute(request(components=("BOGUS",)), bundle())


class TestPresenceSemantics:
    def test_an_absent_position_is_not_treated_as_closed(self, engine):
        """Absence from a scan means unknown, not flat."""
        absent = position("P-absent", presence="ABSENT", lots=Decimal("5"))
        result = engine.compute(
            request(components=("EXPOSURE",)), bundle(positions=(position(), absent))
        )
        assert format_units(result.exposure.by_symbol["EURUSD"].long_units) == "50000"
        assert result.exposure.positions_counted == 1

    def test_a_non_open_source_status_is_excluded(self, engine):
        closed = position("P-closed", source_status="CLOSED", lots=Decimal("5"))
        result = engine.compute(
            request(components=("EXPOSURE",)), bundle(positions=(position(), closed))
        )
        assert result.exposure.positions_counted == 1

    def test_long_and_short_net_against_each_other(self, engine):
        long_pos = position("P-long", lots=Decimal("0.5"))
        short_pos = position("P-short", side="SELL", lots=Decimal("0.2"))
        result = engine.compute(
            request(components=("EXPOSURE",)), bundle(positions=(long_pos, short_pos))
        )
        bucket = result.exposure.by_symbol["EURUSD"]
        assert format_units(bucket.long_units) == "50000"
        assert format_units(bucket.short_units) == "20000"
        assert format_units(bucket.gross_units) == "70000"
        assert format_units(bucket.net_units) == "30000"
