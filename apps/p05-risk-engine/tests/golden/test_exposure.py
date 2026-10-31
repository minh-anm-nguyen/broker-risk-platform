"""Domains E and M -- position state, netting/hedging, exposure units and grouping.

Covers WF5-049 through WF5-060 and WF5-145 through WF5-156.
"""

from __future__ import annotations

import pytest

from risk.domain.exposure import compute_exposure
from risk.domain.facts import PositionFact
from risk.domain.instruments import CalcMode, InstrumentSpec, SymbolCatalog
from risk.errors import ContractError, ReasonCode, UnsupportedError
from risk.status import Presence, Side
from risk_tools.fixtures import ACCOUNT_A, EPOCH_A
from tests.reference.oracle import D, reference_exposure, reference_mid, reference_quote_notional

pytestmark = [pytest.mark.golden, pytest.mark.p0]

CONTRACT = "100000"


def position(
    position_id: str,
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


@pytest.mark.wf5("WF5-049")
def test_buy_position_signed_units_positive(catalog, evidence):
    """BUY 0.50 EURUSD is +50 000 EUR of net exposure."""
    result = compute_exposure((position("p-1"),), catalog)
    eurusd = result.by_symbol["EURUSD"]

    expected = reference_exposure(
        [{"symbol": "EURUSD", "side": "BUY", "lots": "0.50"}], {"EURUSD": CONTRACT}
    )["EURUSD"]

    assert eurusd.net_units.amount == expected["net"] == D("50000.00")
    assert eurusd.unit_currency == "EUR"
    # The unit label is part of the answer, not decoration: the same number in
    # GBP would be a different amount of risk.
    assert eurusd.long_units.unit == "EUR"
    evidence(
        expected_values={"net_units": "50000.00", "unit": "EUR"},
        observed_values={"net_units": eurusd.net_units.canonical(), "unit": eurusd.unit_currency},
    )


@pytest.mark.wf5("WF5-050")
def test_sell_position_signed_units_negative(catalog, evidence):
    """SELL 0.50 EURUSD is -50 000 EUR net, held as +50 000 in the short bucket."""
    result = compute_exposure((position("p-1", side="SELL"),), catalog)
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.net_units.amount == D("-50000.00")
    assert eurusd.short_units.amount == D("50000.00")
    assert eurusd.long_units.amount == D("0")
    assert eurusd.gross_units.amount == D("50000.00")
    evidence(observed_values={"net_units": eurusd.net_units.canonical()})


@pytest.mark.wf5("WF5-051", "WF5-147")
def test_hedged_book_keeps_gross(catalog, evidence):
    """A long and a short of equal size net to zero and still carry 100 000 gross."""
    positions = (position("p-long", side="BUY"), position("p-short", side="SELL"))
    result = compute_exposure(positions, catalog)
    eurusd = result.by_symbol["EURUSD"]

    expected = reference_exposure(
        [
            {"symbol": "EURUSD", "side": "BUY", "lots": "0.50"},
            {"symbol": "EURUSD", "side": "SELL", "lots": "0.50"},
        ],
        {"EURUSD": CONTRACT},
    )["EURUSD"]

    assert eurusd.net_units.amount == expected["net"] == D("0")
    assert eurusd.gross_units.amount == expected["gross"] == D("100000.00")
    # The whole point of the case: gross did not collapse with net.
    assert eurusd.gross_units.amount > abs(eurusd.net_units.amount)
    evidence(
        expected_values={"net": "0", "gross": "100000.00"},
        observed_values={
            "net": eurusd.net_units.canonical(),
            "gross": eurusd.gross_units.canonical(),
        },
    )


@pytest.mark.wf5("WF5-052")
def test_absent_position_is_not_treated_as_closed(catalog, evidence):
    """An ABSENT row contributes nothing and is not reported as a closure."""
    result = compute_exposure(
        (position("p-1"), position("p-gone", presence=Presence.ABSENT)), catalog
    )
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.position_count == 1
    assert eurusd.net_units.amount == D("50000.00")
    assert result.skipped_closed_positions == 1
    # Absence is recorded as "not included", never as "closed": the engine has
    # no close event and must not invent one.
    assert "p-gone" not in result.included_position_ids
    evidence(observed_values={"included": str(len(result.included_position_ids))})


@pytest.mark.wf5("WF5-053")
def test_unknown_position_status_blocks_rather_than_skips(catalog, evidence):
    """A status outside the approved mapping raises; skipping it would understate risk."""
    with pytest.raises(ContractError) as exc:
        compute_exposure((position("p-1", source_status="UNKNOWN"),), catalog)
    assert exc.value.reason is ReasonCode.UNSUPPORTED_POSITION_STATUS
    evidence(
        expected_reason_codes=("UNSUPPORTED_POSITION_STATUS",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-054")
def test_partial_close_uses_published_residual_once(catalog, evidence):
    """A position partially closed to 0.30 lots is counted as 0.30, once."""
    result = compute_exposure((position("p-1", lots="0.30"),), catalog)
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.net_units.amount == D("30000.00")
    assert eurusd.position_count == 1
    evidence(expected_values={"net_units": "30000.00"},
             observed_values={"net_units": eurusd.net_units.canonical()})


@pytest.mark.wf5("WF5-055")
def test_netting_reversal_uses_final_side_and_volume(catalog, evidence):
    """After a reversal the projection holds one row; the deals are not replayed."""
    # Source reversed SELL 0.20 into BUY 0.10; the projection's final state is
    # the single BUY row. Replaying the deal history here would double-count.
    result = compute_exposure((position("p-1", side="BUY", lots="0.10"),), catalog)
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.net_units.amount == D("10000.00")
    assert eurusd.short_units.amount == D("0")
    evidence(observed_values={"net_units": eurusd.net_units.canonical()})


@pytest.mark.wf5("WF5-056")
def test_zero_lot_open_position_rejected(evidence):
    """An OPEN position with zero volume is corrupt input, not a zero-risk position."""
    with pytest.raises(ContractError) as exc:
        position("p-1", lots="0")
    assert exc.value.reason is ReasonCode.INVALID_POSITION_VOLUME
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-151", "WF5-262")
def test_negative_lots_rejected_not_reinterpreted(evidence):
    """A negative volume is refused rather than read as a SELL."""
    with pytest.raises(ContractError) as exc:
        position("p-1", lots="-0.20")
    assert exc.value.reason is ReasonCode.INVALID_POSITION_VOLUME
    evidence(
        expected_reason_codes=("INVALID_POSITION_VOLUME",),
        observed_reason_codes=(str(exc.value.reason),),
        notes=("direction belongs to side, never to the sign of the volume",),
    )


@pytest.mark.wf5("WF5-057", "WF5-256")
@pytest.mark.p1
def test_duplicate_position_id_in_one_scan_rejected(catalog, evidence):
    """Two rows with one id would double the book, so the scan is rejected."""
    with pytest.raises(ContractError) as exc:
        compute_exposure((position("p-1"), position("p-1")), catalog)
    assert exc.value.reason is ReasonCode.DATA_INTEGRITY_CONFLICT
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-145")
def test_two_longs_same_symbol_are_additive(catalog, evidence):
    """Exposure is linear within a symbol."""
    result = compute_exposure(
        (position("p-1", lots="0.50"), position("p-2", lots="0.25")), catalog
    )
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.long_units.amount == D("75000.00")
    assert eurusd.net_units.amount == D("75000.00")
    assert eurusd.position_count == 2
    evidence(observed_values={"long_units": eurusd.long_units.canonical()})


@pytest.mark.wf5("WF5-146")
def test_two_lots_long_one_short_exact_quantities(catalog, evidence):
    """BUY 2 and SELL 1: long 200k, short 100k, net 100k, gross 300k."""
    positions = (
        position("p-long", side="BUY", lots="2.00"),
        position("p-short", side="SELL", lots="1.00"),
    )
    result = compute_exposure(positions, catalog)
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.long_units.amount == D("200000.00")
    assert eurusd.short_units.amount == D("100000.00")
    assert eurusd.net_units.amount == D("100000.00")
    assert eurusd.gross_units.amount == D("300000.00")
    evidence(
        expected_values={"long": "200000.00", "short": "100000.00",
                         "net": "100000.00", "gross": "300000.00"},
        observed_values={
            "long": eurusd.long_units.canonical(),
            "short": eurusd.short_units.canonical(),
            "net": eurusd.net_units.canonical(),
            "gross": eurusd.gross_units.canonical(),
        },
    )


@pytest.mark.wf5("WF5-148")
def test_different_symbols_keep_separate_unit_buckets(catalog, evidence):
    """EURUSD and GBPUSD are reported in EUR and GBP, never combined."""
    positions = (
        position("p-eur", symbol="EURUSD", lots="1.00"),
        position("p-gbp", symbol="GBPUSD", lots="1.00", open_price="1.25000"),
    )
    result = compute_exposure(positions, catalog)

    assert result.by_symbol["EURUSD"].unit_currency == "EUR"
    assert result.by_symbol["GBPUSD"].unit_currency == "GBP"
    # There is no combined key at all. A consumer cannot accidentally read a
    # "total units" that would be meaningless.
    assert set(result.by_symbol) == {"EURUSD", "GBPUSD"}
    assert "TOTAL" not in result.as_payload()["by_symbol"]
    evidence(observed_values={"symbols": ",".join(sorted(result.by_symbol))})


@pytest.mark.wf5("WF5-149", "WF5-150")
def test_quote_notional_uses_labelled_mid(catalog, golden, evidence):
    """Notional at MID = 1.1021 on 100 000 units, labelled as MID."""
    from risk_tools.fixtures import quote_snapshot_for

    spec = golden("R05-buy-mark")
    snapshot = quote_snapshot_for(spec)  # bid 1.10200 / ask 1.10220

    result = compute_exposure(
        (position("p-1", lots="1.00"),),
        catalog,
        quotes=snapshot,
        include_quote_notional=True,
    )
    eurusd = result.by_symbol["EURUSD"]
    expected_mid = reference_mid("1.10200", "1.10220")
    expected_notional = reference_quote_notional(D("100000.00"), expected_mid)

    assert eurusd.quote_notional is not None
    assert eurusd.quote_notional.amount == expected_notional == D("110210.000")
    assert eurusd.quote_notional.currency == "USD"
    # The basis is published next to the number, so nobody reads an
    # informational mid-notional as a liquidation value.
    assert eurusd.valuation_basis == "MID"
    evidence(
        expected_values={"notional": "110210.000", "basis": "MID"},
        observed_values={
            "notional": eurusd.quote_notional.canonical(),
            "basis": str(eurusd.valuation_basis),
        },
    )


@pytest.mark.wf5("WF5-152")
def test_volume_above_catalog_maximum_rejected(catalog, evidence):
    """A 500-lot position against a 100-lot maximum is an anomaly, not exposure."""
    with pytest.raises(ContractError) as exc:
        compute_exposure((position("p-1", lots="500.00"),), catalog)
    assert exc.value.reason is ReasonCode.VOLUME_LIMIT_EXCEEDED
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-154")
@pytest.mark.p1
def test_exposure_grouping_cannot_merge_two_epochs(catalog, evidence):
    """Facts from two epochs cannot reach one bundle, so they cannot merge."""
    from risk.domain.facts import (
        AuthorizedScope,
        CoverageReport,
        RiskCut,
        RiskInputBundle,
    )
    from risk_tools.fixtures import EPOCH_B, SOURCE_A, TENANT_A

    other_epoch = PositionFact(
        position_id="p-other",
        account_id=ACCOUNT_A,
        symbol="EURUSD",
        side=Side.BUY,
        lots=D("1.00"),
        open_price=D("1.10000"),
        source_status="OPEN",
        presence=Presence.PRESENT,
        epoch_id=EPOCH_B,
        published_scan_id="scan-P2",
    )
    with pytest.raises(ContractError) as exc:
        RiskInputBundle(
            cut=RiskCut(source_id=SOURCE_A, epoch_id=EPOCH_A),
            scope=AuthorizedScope(
                tenant_id=TENANT_A, source_id=SOURCE_A, account_ids=frozenset({ACCOUNT_A})
            ),
            accounts=(),
            positions=(position("p-1"), other_epoch),
            deals=(),
            catalog=catalog,
            quotes=None,
            recon=None,
            coverage=CoverageReport(),
        )
    assert exc.value.reason is ReasonCode.EPOCH_IDENTITY_CONFLICT
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-153")
@pytest.mark.p1
def test_same_base_currency_different_specs_not_merged(evidence):
    """Two symbols sharing a base currency stay separate keys with their own specs."""
    spec_a = InstrumentSpec(
        symbol="EURUSD",
        base_currency="EUR",
        quote_currency="USD",
        profit_currency="USD",
        calc_mode=CalcMode.FOREX_LINEAR_P0,
        contract_size=D("100000"),
        tick_size=D("0.00001"),
        volume_step=D("0.01"),
        volume_max=D("100"),
        spec_version="test-v1",
    )
    spec_b = InstrumentSpec(
        symbol="EURUSD.mini",
        base_currency="EUR",
        quote_currency="USD",
        profit_currency="USD",
        calc_mode=CalcMode.FOREX_LINEAR_P0,
        contract_size=D("10000"),  # a tenth of the standard contract
        tick_size=D("0.00001"),
        volume_step=D("0.01"),
        volume_max=D("100"),
        spec_version="test-v1",
    )
    catalog = SymbolCatalog("test-v1", {"EURUSD": spec_a, "EURUSD.mini": spec_b})

    result = compute_exposure(
        (
            position("p-std", symbol="EURUSD", lots="1.00"),
            position("p-mini", symbol="EURUSD.mini", lots="1.00"),
        ),
        catalog,
    )
    assert result.by_symbol["EURUSD"].net_units.amount == D("100000.00")
    assert result.by_symbol["EURUSD.mini"].net_units.amount == D("10000.00")
    # Both are EUR units, and they are still not added: the contract sizes make
    # them different instruments.
    assert set(result.by_symbol) == {"EURUSD", "EURUSD.mini"}
    evidence(observed_values={"keys": ",".join(sorted(result.by_symbol))})


@pytest.mark.wf5("WF5-155")
@pytest.mark.p1
def test_large_book_near_zero_net_preserves_gross(catalog, evidence):
    """A book that nets to nothing still reports the size that is at risk."""
    positions = tuple(
        position(f"p-{i}", side="BUY" if i % 2 == 0 else "SELL", lots="1.00")
        for i in range(100)
    )
    result = compute_exposure(positions, catalog)
    eurusd = result.by_symbol["EURUSD"]

    assert eurusd.net_units.amount == D("0")
    assert eurusd.gross_units.amount == D("10000000.00")
    assert eurusd.gross_units.amount >= abs(eurusd.net_units.amount)
    evidence(
        observed_values={
            "net": eurusd.net_units.canonical(),
            "gross": eurusd.gross_units.canonical(),
        }
    )


@pytest.mark.wf5("WF5-156")
@pytest.mark.p2
def test_unsupported_mode_gives_units_but_never_money(catalog, golden, evidence):
    """Gold lots yield a diagnostic unit count and an explicit refusal to value them."""
    from risk_tools.fixtures import quote_snapshot_for

    gold = position("p-gold", symbol="XAUUSD", lots="1.00", open_price="2400.00")

    # Units come from the contract size alone, so they are meaningful and are
    # flagged as diagnostic-only.
    units_only = compute_exposure((gold,), catalog)
    xau = units_only.by_symbol["XAUUSD"]
    assert xau.net_units.amount == D("100.00")
    assert xau.unit_currency == "XAU"
    assert xau.calc_mode_supported is False
    assert xau.quote_notional is None

    # A money notional for the same position is refused outright: there is no
    # verified formula, so there is no number.
    spec = golden("R05-unsupported-symbol")
    with pytest.raises(UnsupportedError) as exc:
        compute_exposure(
            (gold,),
            catalog,
            quotes=quote_snapshot_for(spec),
            include_quote_notional=True,
        )
    assert exc.value.reason is ReasonCode.UNSUPPORTED_CALC_MODE
    evidence(
        expected_reason_codes=("UNSUPPORTED_CALC_MODE",),
        observed_reason_codes=(str(exc.value.reason),),
        observed_values={"net_units": xau.net_units.canonical(), "notional": "absent"},
    )


def test_empty_position_set_yields_empty_exposure(catalog):
    """No positions is a valid, empty answer -- the completeness proof is the gate's job."""
    result = compute_exposure((), catalog)
    assert result.by_symbol == {}
    assert result.included_position_ids == frozenset()
