"""DTO construction as validation.

Covers WF2-L-03, WF2-L-04, WF2-L-06, WF2-L-07, WF2-L-09, WF2-L-11, WF2-A-09,
WF2-R-07 and the DTO half of INV-10.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from mt5_connector.dto import build_account, build_deal, build_position
from mt5_connector.scalars import ScalarError

pytestmark = pytest.mark.unit


def account(**overrides) -> dict:
    base = {
        "account_id": "acct-0001",
        "currency": "USD",
        "balance": "10000.00",
        "credit": "250.00",
    }
    base.update(overrides)
    return base


def position(**overrides) -> dict:
    base = {
        "position_id": "pos-00001",
        "account_id": "acct-0001",
        "symbol": "EURUSD",
        "side": "BUY",
        "volume_lots": "0.50",
        "open_price": "1.08500",
        "status": "OPEN",
    }
    base.update(overrides)
    return base


def deal(**overrides) -> dict:
    base = {
        "deal_id": "deal-000001",
        "source_seq": 126,
        "account_id": "acct-0001",
        "position_id": "pos-00001",
        "symbol": "EURUSD",
        "entry": "OUT",
        "side": "SELL",
        "volume_lots": "0.20",
        "price": "1.08600",
        "realized_pnl_gross": "20.00",
        "commission": "-0.70",
        "swap": "-0.15",
        "executed_at": "2026-10-30T05:32:01.123Z",
    }
    base.update(overrides)
    return base


class TestAccount:
    def test_valid_account_preserves_scale(self) -> None:
        built = build_account(account())
        assert built.balance == Decimal("10000.00")
        assert built.balance.as_tuple().exponent == -2

    def test_currency_is_preserved_never_converted(self) -> None:
        """WF2-L-09: do not assume USD and do not convert."""
        built = build_account(account(currency="JPY", balance="1000000"))
        assert built.currency == "JPY"
        assert built.balance == Decimal("1000000")

    @pytest.mark.parametrize("currency", ["usd", "US", "USDX", "", "U$D", "123"])
    def test_malformed_currency_is_refused(self, currency: str) -> None:
        with pytest.raises(ScalarError):
            build_account(account(currency=currency))

    def test_missing_field_is_an_error_not_a_default(self) -> None:
        payload = account()
        del payload["credit"]
        with pytest.raises(ScalarError) as excinfo:
            build_account(payload)
        assert excinfo.value.field == "credit"

    def test_blank_account_id_is_refused(self) -> None:
        """WF2-L-07."""
        with pytest.raises(ScalarError):
            build_account(account(account_id=""))

    def test_float_balance_is_refused(self) -> None:
        with pytest.raises(ScalarError):
            build_account(account(balance=10000.0))

    def test_non_object_payload_is_refused(self) -> None:
        for payload in ([], "acct", 7, None):
            with pytest.raises(ScalarError):
                build_account(payload)


class TestPosition:
    def test_valid_open_position(self) -> None:
        built = build_position(position())
        assert built.status == "OPEN"
        assert built.volume_lots == Decimal("0.50")
        assert built.unrealized_pnl is None

    def test_negative_volume_is_refused(self) -> None:
        """WF2-L-04: side carries direction; volume is not a signed quantity."""
        with pytest.raises(ScalarError) as excinfo:
            build_position(position(volume_lots="-0.50"))
        assert excinfo.value.field == "volume_lots"

    def test_open_with_zero_volume_is_refused(self) -> None:
        """WF2-L-11: an open position holding nothing is a contradiction."""
        with pytest.raises(ScalarError) as excinfo:
            build_position(position(volume_lots="0.00"))
        assert "OPEN" in excinfo.value.reason

    def test_closed_with_zero_volume_is_accepted(self) -> None:
        built = build_position(position(status="CLOSED", volume_lots="0.00"))
        assert built.volume_lots == Decimal("0.00")

    def test_non_positive_open_price_is_refused(self) -> None:
        for price in ("0.00", "-1.08500"):
            with pytest.raises(ScalarError):
                build_position(position(open_price=price))

    def test_unknown_side_is_refused(self) -> None:
        with pytest.raises(ScalarError):
            build_position(position(side="LONG"))

    def test_missing_mark_stays_none_and_is_not_zero(self) -> None:
        # None means "not known at this snapshot". Reporting 0.00 would be a
        # confidently wrong number.
        built = build_position(position())
        assert built.unrealized_pnl is None
        assert built.mark_market_time is None
        assert built.mark_stale is None

    def test_mark_fields_are_decoded_when_present(self) -> None:
        built = build_position(
            position(
                unrealized_pnl="-12.34",
                mark_market_time="2026-10-30T05:00:00Z",
                mark_stale=True,
            )
        )
        assert built.unrealized_pnl == Decimal("-12.34")
        assert built.mark_market_time == datetime(2026, 10, 30, 5, 0, 0, tzinfo=UTC)
        assert built.mark_stale is True

    def test_non_boolean_mark_stale_is_refused(self) -> None:
        with pytest.raises(ScalarError):
            build_position(position(mark_stale="yes"))


class TestDeal:
    def test_valid_deal_preserves_signs_exactly(self) -> None:
        """WF2-L-03: a flipped fee sign turns every cost into revenue."""
        built = build_deal(deal(), expect_source_seq=True)
        assert built.commission == Decimal("-0.70")
        assert built.swap == Decimal("-0.15")
        assert built.realized_pnl_gross == Decimal("20.00")
        assert built.source_seq == 126

    def test_executed_at_is_the_source_instant_not_receive_time(self) -> None:
        built = build_deal(deal(), expect_source_seq=True)
        assert built.executed_at == datetime(2026, 10, 30, 5, 32, 1, 123_000, tzinfo=UTC)

    def test_float_price_is_refused(self) -> None:
        """WF2-L-01 at the DTO boundary."""
        with pytest.raises(ScalarError) as excinfo:
            build_deal(deal(price=1.086), expect_source_seq=True)
        assert excinfo.value.field == "price"

    def test_nan_commission_is_refused(self) -> None:
        """WF2-L-02."""
        with pytest.raises(ScalarError) as excinfo:
            build_deal(deal(commission="NaN"), expect_source_seq=True)
        assert excinfo.value.field == "commission"

    def test_non_positive_volume_or_price_is_refused(self) -> None:
        for field, value in (("volume_lots", "0.00"), ("price", "0.00")):
            with pytest.raises(ScalarError):
                build_deal(deal(**{field: value}), expect_source_seq=True)

    def test_declared_source_seq_must_be_present(self) -> None:
        """WF2-A-09: a declared field going missing is a contract break."""
        payload = deal()
        del payload["source_seq"]
        with pytest.raises(ScalarError) as excinfo:
            build_deal(payload, expect_source_seq=True)
        assert excinfo.value.field == "source_seq"

    def test_undeclared_source_seq_is_refused_rather_than_adopted(self) -> None:
        """WF2-R-07: do not start relying on a capability nobody negotiated."""
        with pytest.raises(ScalarError) as excinfo:
            build_deal(deal(), expect_source_seq=False)
        assert "did not declare" in excinfo.value.reason

    def test_a_source_without_sequences_yields_none_not_a_fabricated_value(self) -> None:
        payload = deal()
        del payload["source_seq"]
        built = build_deal(payload, expect_source_seq=False)
        assert built.source_seq is None

    def test_source_seq_must_be_a_positive_integer(self) -> None:
        for bad in (0, -1, "126", 126.0, True):
            with pytest.raises(ScalarError):
                build_deal(deal(source_seq=bad), expect_source_seq=True)

    def test_unknown_entry_is_refused(self) -> None:
        with pytest.raises(ScalarError):
            build_deal(deal(entry="INOUT"), expect_source_seq=True)

    def test_dedup_key_is_the_deal_id_alone(self) -> None:
        """WF2-K-09: the same id with different money must collide, not split."""
        first = build_deal(deal(), expect_source_seq=True)
        second = build_deal(deal(commission="-9.99"), expect_source_seq=True)
        assert first.dedup_key == second.dedup_key == "deal-000001"

    def test_dto_is_immutable(self) -> None:
        built = build_deal(deal(), expect_source_seq=True)
        with pytest.raises(Exception):
            built.commission = Decimal("0")  # type: ignore[misc]
