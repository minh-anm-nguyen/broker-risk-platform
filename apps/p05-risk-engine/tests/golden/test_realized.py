"""Domains D, J and L -- deal classification, realized aggregation, signed fees.

Covers WF5-037..048, WF5-109..120 and WF5-133..144.
"""

from __future__ import annotations

from datetime import timedelta
from decimal import ROUND_HALF_EVEN

import pytest

from risk.domain.classification import (
    DealCategory,
    DealClass,
    SourceCapability,
    classify_deal,
    select_eligible_deals,
)
from risk.domain.facts import DealFact, build_money_triplet
from risk.domain.money import Money, MoneySemantic
from risk.domain.realized import compute_realized
from risk.errors import ContractError, ReasonCode
from risk.status import FeeCoverage
from risk_tools.fixtures import ACCOUNT_A, EPOCH_A, T0
from tests.reference.oracle import D, reference_realized, reference_realized_from_prices

pytestmark = [pytest.mark.golden, pytest.mark.p0]

SIM = SourceCapability.SIMULATOR_TRADING_ONLY_V1
FULL = SourceCapability.FULL_DEAL_TAXONOMY_V1


def deal(
    deal_id: str,
    *,
    seq: int = 1,
    entry_kind: str = "OUT",
    deal_type: str | None = None,
    gross: str = "0.00",
    commission: str = "0.00",
    swap: str = "0.00",
    currency: str = "USD",
    executed_at=None,
) -> DealFact:
    g, c, s = build_money_triplet(
        currency=currency, gross=gross, commission=commission, swap=swap
    )
    return DealFact(
        deal_id=deal_id,
        account_id=ACCOUNT_A,
        source_seq=seq,
        entry_kind=entry_kind,
        deal_type=deal_type,
        gross_profit=g,
        commission=c,
        swap=s,
        executed_at=executed_at or T0,
        epoch_id=EPOCH_A,
    )


def realized(deals, *, capability=SIM, watermark=None, start=None, end=None, fees=False):
    eligibility = select_eligible_deals(
        deals,
        capability=capability,
        period_start=start,
        period_end=end,
        deal_watermark_seq=watermark,
    )
    return compute_realized(eligibility, currency="USD", fee_fields_available=fees)


# --------------------------------------------------------------------------- #
# Domain D -- classification
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-037")
def test_opening_deal_has_zero_gross_and_real_commission(evidence):
    """An opening deal books no profit; its -2.00 commission is still an expense."""
    result = realized([deal("d-open", entry_kind="IN", gross="0.00", commission="-2.00")])

    assert result.gross.amount == D("0.00")
    assert result.commission.amount == D("-2.00")
    assert result.net_excluding_fee.amount == D("-2.00")
    evidence(
        expected_values={"gross": "0.00", "commission": "-2.00"},
        observed_values={"gross": result.gross.canonical(),
                         "commission": result.commission.canonical()},
    )


@pytest.mark.wf5("WF5-038")
def test_closing_loss_is_summed_exactly(evidence):
    """A -20 close stays -20; the sign is not absorbed anywhere."""
    result = realized([deal("d-close", gross="-20.00")])
    assert result.gross.amount == D("-20.00")
    assert result.net_excluding_fee.amount == D("-20.00")
    evidence(observed_values={"gross": result.gross.canonical()})


@pytest.mark.wf5("WF5-039", "WF5-116")
def test_inout_reversal_counted_once_from_source_amount(evidence):
    """An INOUT reversal contributes its booked amount once, with no synthetic close."""
    deals = [
        deal("d-inout", entry_kind="INOUT", gross="40.00", commission="-1.00"),
        deal("d-later", seq=2, entry_kind="OUT", gross="5.00"),
    ]
    result = realized(deals)

    expected = reference_realized(
        [
            {"gross": "40.00", "commission": "-1.00", "swap": "0.00"},
            {"gross": "5.00", "commission": "0.00", "swap": "0.00"},
        ]
    )
    assert result.gross.amount == expected["gross"] == D("45.00")
    assert result.deal_count == 2
    # Exactly the two source deal ids, no reconstructed third event.
    assert result.deal_ids == frozenset({"d-inout", "d-later"})
    evidence(
        expected_values={"gross": "45.00", "deal_count": "2"},
        observed_values={"gross": result.gross.canonical(), "deal_count": str(result.deal_count)},
    )


@pytest.mark.wf5("WF5-040")
def test_out_by_close_uses_source_semantics(evidence):
    """OUT_BY is in the approved entry-kind set and uses the booked amount."""
    classification = classify_deal(deal("d-outby", entry_kind="OUT_BY"), capability=SIM)
    assert classification.deal_class is DealClass.TRADING

    result = realized([deal("d-outby", entry_kind="OUT_BY", gross="12.50")])
    assert result.gross.amount == D("12.50")
    evidence(observed_values={"class": str(classification.deal_class)})


@pytest.mark.wf5("WF5-041")
def test_balance_deposit_excluded_from_trading_pnl(evidence):
    """A +1000 deposit appears as a non-trading category, not as profit."""
    deals = [
        deal("d-dep", entry_kind="IN", deal_type="DEAL_TYPE_BALANCE", gross="1000.00"),
        deal("d-trade", seq=2, entry_kind="OUT", deal_type="DEAL_TYPE_SELL", gross="30.00"),
    ]
    result = realized(deals, capability=FULL)

    assert result.gross.amount == D("30.00")
    assert result.deal_count == 1
    assert result.non_trading.deal_count == 1
    assert result.non_trading.category_totals[
        str(DealCategory.BALANCE_OPERATION)
    ].amount == D("1000.00")
    # The deposit is visible but structurally unable to reach the trading line.
    assert "d-dep" not in result.deal_ids
    evidence(
        expected_values={"trading_gross": "30.00", "non_trading": "1000.00"},
        observed_values={
            "trading_gross": result.gross.canonical(),
            "non_trading": result.non_trading.category_totals[
                str(DealCategory.BALANCE_OPERATION)
            ].canonical(),
        },
    )


@pytest.mark.wf5("WF5-042")
def test_credit_operation_not_reported_as_trade_profit(evidence):
    """A +500 broker credit is a credit operation, not a win."""
    result = realized(
        [deal("d-credit", entry_kind="IN", deal_type="DEAL_TYPE_CREDIT", gross="500.00")],
        capability=FULL,
    )
    assert result.gross.amount == D("0")
    assert result.non_trading.category_totals[
        str(DealCategory.CREDIT_OPERATION)
    ].amount == D("500.00")
    evidence(observed_values={"trading_gross": result.gross.canonical()})


@pytest.mark.wf5("WF5-043", "WF5-140")
def test_daily_commission_posting_is_a_separate_category(evidence):
    """A -3 daily commission posting is a fee category, not a trade commission."""
    deals = [
        deal("d-fee", entry_kind="IN", deal_type="DEAL_TYPE_COMMISSION_DAILY", gross="-3.00"),
        deal("d-trade", seq=2, entry_kind="OUT", deal_type="DEAL_TYPE_SELL",
             gross="10.00", commission="-1.00"),
    ]
    result = realized(deals, capability=FULL)

    # The trade's own commission is unaffected; the daily posting is not added
    # to it, so the same cash flow cannot be counted twice.
    assert result.commission.amount == D("-1.00")
    assert result.non_trading.category_totals[str(DealCategory.FEE_POSTING)].amount == D("-3.00")
    assert result.fee_coverage is FeeCoverage.NOT_AVAILABLE
    evidence(
        observed_values={
            "trade_commission": result.commission.canonical(),
            "fee_posting": result.non_trading.category_totals[
                str(DealCategory.FEE_POSTING)
            ].canonical(),
            "fee_coverage": str(result.fee_coverage),
        }
    )


@pytest.mark.wf5("WF5-044")
def test_correction_deal_blocks_rather_than_adjusting(evidence):
    """A CORRECTION has no approved mapping, so the aggregation refuses to run."""
    deals = [deal("d-corr", deal_type="DEAL_TYPE_CORRECTION", gross="-5.00")]
    eligibility = select_eligible_deals(
        deals, capability=FULL, period_start=None, period_end=None, deal_watermark_seq=None
    )
    assert eligibility.has_unclassified

    with pytest.raises(ContractError) as exc:
        compute_realized(eligibility, currency="USD")
    assert exc.value.reason is ReasonCode.DEAL_CORRECTION_UNSUPPORTED
    evidence(
        expected_reason_codes=("DEAL_CORRECTION_UNSUPPORTED",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-045")
@pytest.mark.p1
def test_canceled_deal_type_rejected(evidence):
    """BUY_CANCELED withdraws a previously reported figure; P0 refuses it."""
    classification = classify_deal(
        deal("d-cancel", deal_type="DEAL_TYPE_BUY_CANCELED", gross="7.00"), capability=FULL
    )
    assert classification.deal_class is DealClass.UNCLASSIFIED
    assert classification.reason is ReasonCode.DEAL_CORRECTION_UNSUPPORTED
    evidence(observed_reason_codes=(str(classification.reason),))


@pytest.mark.wf5("WF5-046", "WF5-010")
@pytest.mark.p1
def test_unknown_entry_kind_quarantined(evidence):
    """A new PARTIAL_OUT enum is quarantined until somebody maps it."""
    classification = classify_deal(
        deal("d-new", entry_kind="PARTIAL_OUT", gross="9.00"), capability=SIM
    )
    assert classification.deal_class is DealClass.UNCLASSIFIED
    assert classification.reason is ReasonCode.ENUM_VERSION_UNMAPPED
    evidence(observed_reason_codes=(str(classification.reason),))


@pytest.mark.wf5("WF5-012")
@pytest.mark.p2
def test_untyped_deal_from_unknown_source_is_unclassified(evidence):
    """The same untyped row is a trade on the simulator and unclassified elsewhere."""
    row = deal("d-untyped", gross="10.00")

    assert classify_deal(row, capability=SIM).deal_class is DealClass.TRADING
    # A real Manager SDK feed makes no trading-only promise, so the absence of
    # a deal type stops being evidence of a trade.
    unknown = classify_deal(row, capability=SourceCapability.UNKNOWN_SOURCE)
    assert unknown.deal_class is DealClass.UNCLASSIFIED
    assert unknown.reason is ReasonCode.DEAL_TYPE_UNCLASSIFIED
    evidence(observed_reason_codes=(str(unknown.reason),))


@pytest.mark.wf5("WF5-047", "WF5-114", "WF5-263")
@pytest.mark.p1
def test_duplicate_deal_id_rejected_not_deduplicated(evidence):
    """An identical duplicate is a reader bug and is surfaced as one."""
    row = deal("d-dup", gross="10.00")
    with pytest.raises(ContractError) as exc:
        select_eligible_deals(
            [row, row], capability=SIM, period_start=None, period_end=None,
            deal_watermark_seq=None
        )
    assert exc.value.reason is ReasonCode.DATA_INTEGRITY_CONFLICT
    evidence(
        observed_reason_codes=(str(exc.value.reason),),
        notes=("silent de-duplication would hide the day the duplicate differs",),
    )


# --------------------------------------------------------------------------- #
# Domain J -- aggregation and windows
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-109", "WF5-005")
def test_single_deal_net_excluding_fee(evidence):
    """+60 gross, -1 commission, -0.5 swap gives 58.50."""
    result = realized([deal("d-1", gross="60.00", commission="-1.00", swap="-0.50")])
    expected = reference_realized(
        [{"gross": "60.00", "commission": "-1.00", "swap": "-0.50"}]
    )

    assert result.net_excluding_fee.amount == expected["net_excluding_fee"] == D("58.50")
    assert result.net_excluding_fee.semantic is MoneySemantic.NET_EXCLUDING_FEE
    evidence(
        expected_values={"net_excluding_fee": "58.50"},
        observed_values={"net_excluding_fee": result.net_excluding_fee.canonical()},
    )


@pytest.mark.wf5("WF5-110")
def test_two_deals_net_to_minus_five(evidence):
    """+10 and -15 is -5, exactly."""
    result = realized([deal("d-1", gross="10.00"), deal("d-2", seq=2, gross="-15.00")])
    assert result.gross.amount == D("-5.00")
    evidence(observed_values={"gross": result.gross.canonical()})


@pytest.mark.wf5("WF5-111", "WF5-157", "WF5-158")
def test_half_open_window_includes_start_excludes_end(evidence):
    """A deal at the window start counts; one at the end does not."""
    start = T0
    end = T0 + timedelta(days=1)
    deals = [
        deal("d-at-start", seq=1, gross="10.00", executed_at=start),
        deal("d-inside", seq=2, gross="20.00", executed_at=start + timedelta(hours=5)),
        deal("d-at-end", seq=3, gross="40.00", executed_at=end),
        deal("d-before", seq=4, gross="80.00", executed_at=start - timedelta(seconds=1)),
    ]
    result = realized(deals, start=start, end=end)

    assert result.deal_ids == frozenset({"d-at-start", "d-inside"})
    assert result.gross.amount == D("30.00")
    assert result.excluded_out_of_window == 2
    # Two adjacent half-open windows partition the timeline exactly once, which
    # is what keeps consecutive daily reports from double-counting midnight.
    next_window = realized(deals, start=end, end=end + timedelta(days=1))
    assert next_window.deal_ids == frozenset({"d-at-end"})
    evidence(
        expected_values={"window_1": "30.00", "window_2": "40.00"},
        observed_values={
            "window_1": result.gross.canonical(),
            "window_2": next_window.gross.canonical(),
        },
    )


@pytest.mark.wf5("WF5-112", "WF5-135")
def test_positive_commission_rebate_keeps_its_sign(evidence):
    """A +1 rebate increases the net; abs() would turn a credit into a charge."""
    result = realized([deal("d-rebate", gross="0.00", commission="1.00")])
    assert result.commission.amount == D("1.00")
    assert result.net_excluding_fee.amount == D("1.00")
    evidence(
        expected_values={"commission": "1.00", "net": "1.00"},
        observed_values={"commission": result.commission.canonical(),
                         "net": result.net_excluding_fee.canonical()},
    )


@pytest.mark.wf5("WF5-136")
def test_positive_swap_credit_keeps_its_sign(evidence):
    result = realized([deal("d-swap", swap="0.75")])
    assert result.swap.amount == D("0.75")
    assert result.net_excluding_fee.amount == D("0.75")
    evidence(observed_values={"swap": result.swap.canonical()})


@pytest.mark.wf5("WF5-113", "WF5-005")
def test_missing_money_field_blocks_without_a_zero(evidence):
    """A NULL swap raises MONEY_FIELD_MISSING; it never becomes 0."""
    with pytest.raises(ContractError) as exc:
        build_money_triplet(currency="USD", gross="10.00", commission="-1.00", swap=None)
    assert exc.value.reason is ReasonCode.MONEY_FIELD_MISSING
    evidence(
        expected_reason_codes=("MONEY_FIELD_MISSING",),
        observed_reason_codes=(str(exc.value.reason),),
    )


@pytest.mark.wf5("WF5-115")
def test_partial_close_uses_booked_amount_not_original_volume(evidence):
    """The source booked +40 on a partial close; that is the figure reported.

    The oracle here deliberately recomputes from prices to show the two agree
    on a controlled fixture -- which is what makes the fixture trustworthy. The
    engine still uses the booked amount, because on real data the two diverge.
    """
    booked = realized([deal("d-partial", gross="40.00")])
    from_prices = reference_realized_from_prices(
        [{"side": "BUY", "lots": "0.20", "open_price": "1.10000", "close_price": "1.10200"}],
        contract_size="100000",
    )

    assert booked.gross.amount == D("40.00")
    assert from_prices == D("40.00000")  # same value, independently derived
    assert booked.gross.amount == from_prices
    evidence(
        oracle="GOLDEN_REFERENCE",
        expected_values={"from_prices": "40.00000"},
        observed_values={"booked": booked.gross.canonical()},
    )


@pytest.mark.wf5("WF5-118")
@pytest.mark.p1
def test_daily_window_excludes_deposit(evidence):
    """A deposit inside the reporting window is still not trading realized."""
    start = T0 - timedelta(hours=6)
    end = T0 + timedelta(hours=6)
    deals = [
        deal("d-dep", entry_kind="IN", deal_type="DEAL_TYPE_BALANCE", gross="1000.00"),
        deal("d-trade", seq=2, deal_type="DEAL_TYPE_SELL", gross="15.00"),
    ]
    result = realized(deals, capability=FULL, start=start, end=end)
    assert result.gross.amount == D("15.00")
    assert result.non_trading.deal_count == 1
    evidence(observed_values={"gross": result.gross.canonical()})


@pytest.mark.wf5("WF5-119")
@pytest.mark.p1
def test_mixed_currency_deals_block_the_sum(evidence):
    """A EUR deal in a USD report raises rather than contributing a magnitude."""
    deals = [deal("d-usd", gross="10.00"), deal("d-eur", seq=2, gross="10.00", currency="EUR")]
    with pytest.raises(ContractError) as exc:
        realized(deals)
    assert exc.value.reason is ReasonCode.CURRENCY_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-162", "WF5-218")
def test_deal_beyond_watermark_excluded_until_watermark_advances(evidence):
    """A late arrival with an in-window timestamp waits for the watermark."""
    deals = [
        deal("d-early", seq=1000, gross="60.00"),
        deal("d-late", seq=2000, gross="25.00"),
    ]
    at_1050 = realized(deals, watermark=1050)
    at_2050 = realized(deals, watermark=2050)

    assert at_1050.gross.amount == D("60.00")
    assert at_1050.excluded_beyond_watermark == 1
    # Once the watermark advances the same deal is included -- the earlier
    # report was provisional, not wrong.
    assert at_2050.gross.amount == D("85.00")
    assert at_2050.excluded_beyond_watermark == 0
    evidence(
        expected_values={"at_w1050": "60.00", "at_w2050": "85.00"},
        observed_values={
            "at_w1050": at_1050.gross.canonical(),
            "at_w2050": at_2050.gross.canonical(),
        },
    )


# --------------------------------------------------------------------------- #
# Domain L -- fees and coverage
# --------------------------------------------------------------------------- #


@pytest.mark.wf5("WF5-133", "WF5-134")
def test_commission_and_swap_reduce_the_net(evidence):
    """-2.50 commission and -0.50 swap each move the net down by their amount."""
    base = realized([deal("d-1", gross="100.00")])
    with_fees = realized([deal("d-1", gross="100.00", commission="-2.50", swap="-0.50")])

    assert base.net_excluding_fee.amount == D("100.00")
    assert with_fees.net_excluding_fee.amount == D("97.00")
    assert base.net_excluding_fee.amount - with_fees.net_excluding_fee.amount == D("3.00")
    evidence(observed_values={"net_with_fees": with_fees.net_excluding_fee.canonical()})


@pytest.mark.wf5("WF5-137")
def test_two_half_cent_commissions_sum_exactly(evidence):
    """-0.005 twice is -0.010, and only then rounded."""
    result = realized(
        [
            deal("d-1", commission="-0.005"),
            deal("d-2", seq=2, commission="-0.005"),
        ]
    )
    assert result.commission.amount == D("-0.010")
    assert result.commission.display(ROUND_HALF_EVEN) == D("-0.01")
    evidence(
        expected_values={"exact": "-0.010", "display": "-0.01"},
        observed_values={
            "exact": result.commission.canonical(),
            "display": str(result.commission.display(ROUND_HALF_EVEN)),
        },
    )


@pytest.mark.wf5("WF5-139", "WF5-359")
def test_no_all_in_net_field_exists(evidence):
    """The payload has net_excluding_fee and nothing a consumer could read as all-in."""
    result = realized([deal("d-1", gross="60.00", commission="-3.50", swap="-0.50")])
    payload = result.as_payload()

    assert payload["net_excluding_fee"] == "56.00"
    assert payload["fee_coverage"] == "NOT_AVAILABLE"
    # The absence is the assertion: no key named net, net_all_in or total.
    for forbidden in ("net", "net_all_in", "total", "all_in_net"):
        assert forbidden not in payload
    evidence(
        expected_values={"net_excluding_fee": "56.00", "fee_coverage": "NOT_AVAILABLE"},
        observed_values={
            "net_excluding_fee": str(payload["net_excluding_fee"]),
            "fee_coverage": str(payload["fee_coverage"]),
        },
    )


@pytest.mark.wf5("WF5-138")
def test_fee_coverage_flips_to_complete_only_when_declared(evidence):
    """Coverage is COMPLETE only when the schema actually carries the fee fields."""
    without = realized([deal("d-1", gross="10.00")], fees=False)
    with_fees = realized([deal("d-1", gross="10.00")], fees=True)

    assert without.fee_coverage is FeeCoverage.NOT_AVAILABLE
    assert with_fees.fee_coverage is FeeCoverage.COMPLETE
    evidence(
        observed_values={
            "declared_false": str(without.fee_coverage),
            "declared_true": str(with_fees.fee_coverage),
        }
    )


@pytest.mark.wf5("WF5-141")
@pytest.mark.p1
def test_posted_swap_counted_once(evidence):
    """Only the posted swap on the deal is counted; no position accrual is added."""
    result = realized([deal("d-close", gross="10.00", swap="-0.50")])
    assert result.swap.amount == D("-0.50")
    # Project 03 carries no position-level swap, so there is nothing to double
    # count. The assertion records that absence deliberately.
    assert "accrued_swap" not in result.as_payload()
    evidence(notes=("no position-level swap exists upstream in the P0 profile",))


@pytest.mark.wf5("WF5-143")
@pytest.mark.p1
def test_triple_swap_rollover_not_modelled(evidence):
    """The engine reports the swap the source booked, and models no accrual rule."""
    result = realized([deal("d-wed", swap="-1.50")])
    assert result.swap.amount == D("-1.50")
    assert result.swap.semantic is MoneySemantic.SWAP
    evidence(notes=("broker accrual policy is upstream; fabricating one is out of scope",))


@pytest.mark.wf5("WF5-144")
@pytest.mark.p2
def test_fee_currency_differing_from_profit_currency_rejected(evidence):
    """One deal cannot book EUR profit and a USD commission without a rate."""
    g = Money(D("10.00"), "EUR", MoneySemantic.REALIZED_GROSS)
    c = Money(D("-1.00"), "USD", MoneySemantic.COMMISSION)
    s = Money(D("0.00"), "EUR", MoneySemantic.SWAP)
    with pytest.raises(ContractError) as exc:
        DealFact(
            deal_id="d-mixed",
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
    assert exc.value.reason is ReasonCode.CURRENCY_MISMATCH
    evidence(observed_reason_codes=(str(exc.value.reason),))


@pytest.mark.wf5("WF5-259")
def test_adding_an_all_zero_deal_changes_provenance_not_amounts(evidence):
    """A zero deal leaves the sums alone and still shows up in the deal set."""
    before = realized([deal("d-1", gross="10.00")])
    after = realized([deal("d-1", gross="10.00"), deal("d-zero", seq=2)])

    assert before.gross.amount == after.gross.amount == D("10.00")
    assert after.deal_count == before.deal_count + 1
    assert "d-zero" in after.deal_ids
    evidence(
        observed_values={"count_before": str(before.deal_count),
                         "count_after": str(after.deal_count)},
    )


@pytest.mark.wf5("WF5-260")
def test_splitting_a_deal_preserves_the_total(evidence):
    """One deal of 10 and two of 7 and 3 aggregate identically."""
    single = realized([deal("d-whole", gross="10.00")])
    split = realized([deal("d-a", gross="7.00"), deal("d-b", seq=2, gross="3.00")])
    assert single.gross.amount == split.gross.amount == D("10.00")
    evidence(observed_values={"single": single.gross.canonical(),
                              "split": split.gross.canonical()})


def test_deal_order_does_not_affect_result():
    """The aggregator sorts, so reader order cannot change the output."""
    deals = [deal(f"d-{i}", seq=i, gross=f"{i}.50") for i in range(1, 20)]
    forward = realized(deals)
    backward = realized(list(reversed(deals)))
    assert forward.gross.amount == backward.gross.amount
    assert forward.deal_ids == backward.deal_ids
