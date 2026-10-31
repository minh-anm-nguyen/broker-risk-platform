"""Record construction: every declared field required, nothing imputed."""

from __future__ import annotations

from decimal import Decimal

import pytest

from trading_sync.domain.entities import AccountRecord, DealRecord, PositionRecord
from trading_sync.domain.snapshots import Side
from trading_sync.errors import InvalidMoneyError, SchemaError

pytestmark = pytest.mark.unit


def deal_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "deal_id": "D1",
        "source_seq": 10,
        "account_id": "A1",
        "position_id": "P1",
        "symbol": "EURUSD",
        "entry_kind": "IN",
        "side": "BUY",
        "volume_lots": "0.10",
        "price": "1.10000",
        "realized_pnl_gross": "0.00",
        "commission": "-2.50",
        "swap": "0.00",
        "executed_at": "2026-01-01T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


def account_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "account_id": "A1",
        "currency": "USD",
        "balance": "1000.00",
        "credit": "0.00",
    }
    payload.update(overrides)
    return payload


def position_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "position_id": "P1",
        "account_id": "A1",
        "symbol": "EURUSD",
        "side": "SELL",
        "volume_lots": "0.10",
        "open_price": "1.10000",
        "source_status": "OPEN",
    }
    payload.update(overrides)
    return payload


@pytest.mark.whatif("WF3-085")
def test_deal_preserves_every_money_field_exactly() -> None:
    record = DealRecord.from_payload(deal_payload(commission="-0.01", swap="-1.25"))
    assert record.commission == Decimal("-0.01")
    assert record.swap == Decimal("-1.25")
    assert record.side is Side.BUY


@pytest.mark.whatif("WF3-092")
@pytest.mark.parametrize("missing", ["commission", "swap", "realized_pnl_gross", "price", "executed_at", "deal_id"])
def test_missing_declared_field_fails_rather_than_defaulting(missing: str) -> None:
    """An absent commission is a validation failure, not a zero.

    Defaulting it would invent a number and make the record look complete,
    which is worse than failing because nothing downstream can tell.
    """
    payload = deal_payload()
    del payload[missing]
    with pytest.raises(SchemaError) as caught:
        DealRecord.from_payload(payload)
    assert caught.value.safe_details["field"] == missing


@pytest.mark.whatif("WF3-092")
def test_explicit_null_money_is_not_treated_as_zero() -> None:
    with pytest.raises(InvalidMoneyError):
        DealRecord.from_payload(deal_payload(commission=None))


@pytest.mark.whatif("WF3-009", "WF3-176")
def test_unknown_fields_are_ignored_and_do_not_change_the_hash() -> None:
    """A backward-compatible upstream addition must not look like a rewrite.

    The hash covers the declared field set for the pinned mapper version, so an
    added field leaves existing rows hashing as before (WF3-009).
    """
    plain = DealRecord.from_payload(deal_payload())
    extended = DealRecord.from_payload(deal_payload(broker_reference="XYZ-1"))
    assert plain.payload_hash == extended.payload_hash


@pytest.mark.whatif("WF3-080")
@pytest.mark.parametrize("side", ["LONG", "buy", "", "B", None, 1])
def test_unknown_side_is_rejected_with_no_fallback(side: object) -> None:
    """A wrong default here inverts the sign of a position's exposure."""
    with pytest.raises(SchemaError):
        PositionRecord.from_payload(position_payload(side=side))


@pytest.mark.whatif("WF3-077")
def test_deal_may_reference_no_position() -> None:
    """A historical deal whose position has left the current set is still valid.

    Rejecting it would discard real history to satisfy a constraint the source
    never promised (ADR-014).
    """
    assert DealRecord.from_payload(deal_payload(position_id=None)).position_id is None


@pytest.mark.whatif("WF3-069")
@pytest.mark.parametrize("currency", ["US", "usd", "USDD", "", "U$D", 1])
def test_invalid_currency_codes_are_rejected(currency: object) -> None:
    with pytest.raises(SchemaError):
        AccountRecord.from_payload(account_payload(currency=currency))


@pytest.mark.whatif("WF3-187")
def test_currency_allowlist_blocks_a_unit_swap() -> None:
    """A payload that swaps USD for JPY while keeping the digits restates a
    balance by two orders of magnitude. Format validation alone cannot see it,
    so a configured source checks against an allowlist and refuses the unknown
    code rather than converting (WF3-187).
    """
    allowed = ("USD", "EUR")
    assert AccountRecord.from_payload(
        account_payload(currency="USD"), allowed_currencies=allowed
    ).currency == "USD"
    with pytest.raises(SchemaError) as caught:
        AccountRecord.from_payload(
            account_payload(currency="JPY"), allowed_currencies=allowed
        )
    assert caught.value.safe_details["currency"] == "JPY"


@pytest.mark.whatif("WF3-083")
@pytest.mark.parametrize("volume", ["-0.10", "0"])
def test_non_positive_position_volume_is_rejected(volume: str) -> None:
    with pytest.raises(InvalidMoneyError):
        PositionRecord.from_payload(position_payload(volume_lots=volume))


@pytest.mark.whatif("WF3-074")
def test_source_status_is_preserved_verbatim() -> None:
    """``CLOSED`` only ever comes from the source saying so (INV-08)."""
    record = PositionRecord.from_payload(position_payload(source_status="CLOSED"))
    assert record.source_status == "CLOSED"


@pytest.mark.whatif("WF3-029")
@pytest.mark.parametrize("seq", [0, -1, "10", 10.0, True, None])
def test_source_seq_must_be_a_positive_integer(seq: object) -> None:
    with pytest.raises(SchemaError):
        DealRecord.from_payload(deal_payload(source_seq=seq))


@pytest.mark.whatif("WF3-035")
def test_old_timestamp_with_new_sequence_is_accepted() -> None:
    """Event order and business time are allowed to disagree.

    A correction posted now for last week's fill has a new sequence and an old
    timestamp; incremental sync follows the sequence (fixture G05).
    """
    record = DealRecord.from_payload(
        deal_payload(source_seq=105, executed_at="2020-01-01T00:00:00+00:00")
    )
    assert record.source_seq == 105
    assert record.executed_at.year == 2020
