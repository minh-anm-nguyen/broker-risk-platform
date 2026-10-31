"""Canonical encoding and the money policy.

The property under test in :func:`test_equivalent_decimal_spellings_hash_the_same`
is the one design section 14.1 names: equivalent allowed decimal normalisation
must produce one hash. Its mirror, :func:`test_excess_precision_is_rejected_not_rounded`,
is what keeps that from being a licence to round.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from outbox.canonical import (
    MAX_DEPTH,
    canonical_json_bytes,
    decode_wire_bytes,
    payload_hash,
    sha256_hex,
    validate_canonical_tree,
)
from outbox.errors import MoneyPolicyError, ValidationError
from outbox.money import (
    CurrencyStatus,
    MoneyCurrency,
    add_money,
    normalize_money,
    normalize_price,
    normalize_volume,
    parse_decimal,
)

pytestmark = pytest.mark.unit


class TestCanonicalEncoding:
    def test_key_order_does_not_change_the_bytes(self) -> None:
        first = canonical_json_bytes({"b": 1, "a": 2, "c": {"z": 1, "y": 2}})
        second = canonical_json_bytes({"c": {"y": 2, "z": 1}, "a": 2, "b": 1})
        assert first == second == b'{"a":2,"b":1,"c":{"y":2,"z":1}}'

    def test_no_incidental_whitespace(self) -> None:
        assert b" " not in canonical_json_bytes({"a": [1, 2], "b": "x"})

    def test_non_ascii_is_kept_as_utf8_not_escaped(self) -> None:
        # Escaping would still be valid JSON but a different byte string, and
        # the hash is over bytes.
        assert canonical_json_bytes({"symbol": "EURUSD€"}) == '{"symbol":"EURUSD€"}'.encode()

    @pytest.mark.parametrize("value", [1.5, float("nan"), float("inf"), -0.0])
    def test_floats_are_refused(self, value: float) -> None:
        with pytest.raises(ValidationError, match="float"):
            canonical_json_bytes({"amount": value})

    def test_decimal_is_refused_with_a_pointer_to_the_fix(self) -> None:
        with pytest.raises(ValidationError, match="outbox.money"):
            canonical_json_bytes({"amount": Decimal("1.00")})

    def test_non_string_keys_are_refused(self) -> None:
        with pytest.raises(ValidationError, match="non-string object key"):
            canonical_json_bytes({1: "x"})

    def test_oversized_integers_are_refused(self) -> None:
        with pytest.raises(ValidationError, match="signed 64 bits"):
            canonical_json_bytes({"n": 2**63})

    def test_depth_limit_names_the_path(self) -> None:
        nested: dict[str, object] = {"leaf": 1}
        for _ in range(MAX_DEPTH + 2):
            nested = {"a": nested}
        with pytest.raises(ValidationError, match=r"nested deeper than"):
            validate_canonical_tree(nested)

    def test_size_limit_applies_to_the_encoding_not_the_object(self) -> None:
        with pytest.raises(ValidationError, match="limit is 64"):
            canonical_json_bytes({"blob": "x" * 500}, max_bytes=64)

    def test_payload_hash_ignores_the_size_limit(self) -> None:
        # A payload is hashed before it is wrapped; the wire limit is applied
        # once, to the envelope.
        assert len(payload_hash({"blob": "x" * 500_000})) == 64

    def test_sha256_is_lowercase_hex(self) -> None:
        digest = sha256_hex(b"abc")
        assert digest == digest.lower() and len(digest) == 64


class TestWireDecoding:
    def test_round_trip(self) -> None:
        payload = {"a": 1, "b": "two", "c": [1, 2, 3], "d": None, "e": True}
        assert decode_wire_bytes(canonical_json_bytes(payload)) == payload

    def test_a_json_float_is_refused_on_the_way_in(self) -> None:
        # A consumer must not accept what a producer may not emit.
        with pytest.raises(ValidationError, match="decimal strings only"):
            decode_wire_bytes(b'{"amount":1.5}')

    @pytest.mark.parametrize("raw", [b'{"a":NaN}', b'{"a":Infinity}', b'{"a":-Infinity}'])
    def test_json_constants_are_refused(self, raw: bytes) -> None:
        with pytest.raises(ValidationError):
            decode_wire_bytes(raw)

    def test_invalid_utf8_is_refused(self) -> None:
        with pytest.raises(ValidationError, match="UTF-8"):
            decode_wire_bytes(b'{"a":"\xff\xfe"}')

    def test_a_bare_array_is_not_an_envelope(self) -> None:
        with pytest.raises(ValidationError, match="expected object"):
            decode_wire_bytes(b"[1,2]")


class TestMoneyNormalisation:
    @pytest.mark.parametrize(
        ("raw", "expected"),
        [("0.1", "0.10"), ("0.10", "0.10"), ("12.3", "12.30"), ("-0.5", "-0.50"), ("0", "0.00")],
    )
    def test_equivalent_decimal_spellings_hash_the_same(self, raw: str, expected: str) -> None:
        assert normalize_money(raw, field="commission") == expected
        assert payload_hash({"commission": normalize_money(raw, field="commission")}) == payload_hash(
            {"commission": expected}
        )

    @pytest.mark.parametrize("raw", ["0.105", "1.2345", "-0.001"])
    def test_excess_precision_is_rejected_not_rounded(self, raw: str) -> None:
        with pytest.raises(MoneyPolicyError, match="rounding a financial value is not permitted"):
            normalize_money(raw, field="commission")

    def test_negative_zero_collapses_to_one_representation(self) -> None:
        # Otherwise a zero amount would have two spellings and two hashes.
        assert normalize_money("-0.00", field="swap") == "0.00"

    @pytest.mark.parametrize("raw", [1.5, 1, True, None, Decimal("1.00")])
    def test_only_strings_are_accepted(self, raw: object) -> None:
        with pytest.raises(MoneyPolicyError, match="expected a decimal string"):
            normalize_money(raw, field="commission")

    @pytest.mark.parametrize("raw", ["1e5", "+1.00", " 1.00", "1_000.00", "NaN", "Infinity", "01.0"])
    def test_exotic_decimal_spellings_are_refused(self, raw: str) -> None:
        with pytest.raises(MoneyPolicyError, match="not a plain decimal string"):
            normalize_money(raw, field="commission")

    def test_price_keeps_five_decimals(self) -> None:
        assert normalize_price("1.105", field="price") == "1.10500"

    def test_volume_must_be_positive(self) -> None:
        assert normalize_volume("0.1", field="volume_lots") == "0.10"
        with pytest.raises(MoneyPolicyError, match="must be greater than zero"):
            normalize_volume("0.00", field="volume_lots")

    def test_absurd_precision_is_out_of_range(self) -> None:
        with pytest.raises(MoneyPolicyError, match="significant digits exceeds"):
            parse_decimal("1" * 25, field="realized_pnl_gross")

    def test_addition_is_exact(self) -> None:
        total = "0.00"
        for _ in range(3):
            total = add_money(total, "0.10")
        # 0.1 three times in float is 0.30000000000000004.
        assert total == "0.30"

    def test_large_sums_stay_exact(self) -> None:
        assert add_money("1234567890.12", "0.88") == "1234567891.00"


class TestCurrencyPolicy:
    def test_proven_requires_a_code(self) -> None:
        currency = MoneyCurrency.proven("USD")
        assert currency.aggregatable
        assert currency.as_payload_fields() == {
            "money_currency": "USD",
            "money_currency_status": "PROVEN",
        }

    def test_unproven_carries_no_code_and_may_not_be_aggregated(self) -> None:
        currency = MoneyCurrency.unproven()
        assert not currency.aggregatable
        assert currency.as_payload_fields() == {
            "money_currency": None,
            "money_currency_status": "UNPROVEN",
        }

    @pytest.mark.parametrize("code", ["usd", "US", "USDD", "", "US1", None, 1])
    def test_a_malformed_code_cannot_be_proven(self, code: object) -> None:
        with pytest.raises(MoneyPolicyError):
            MoneyCurrency.proven(code)  # type: ignore[arg-type]

    def test_proven_without_a_code_is_refused_on_the_way_in(self) -> None:
        with pytest.raises(MoneyPolicyError, match="PROVEN requires a money_currency"):
            MoneyCurrency.from_payload({"money_currency_status": "PROVEN", "money_currency": None})

    def test_unproven_with_a_code_is_refused_on_the_way_in(self) -> None:
        # Either half could be the lie, so neither is believed.
        with pytest.raises(MoneyPolicyError, match="UNPROVEN must not carry"):
            MoneyCurrency.from_payload({"money_currency_status": "UNPROVEN", "money_currency": "USD"})

    def test_unknown_status_is_refused(self) -> None:
        with pytest.raises(MoneyPolicyError, match="not a known status"):
            MoneyCurrency.from_payload({"money_currency_status": "PROBABLY_USD"})

    def test_round_trip(self) -> None:
        for currency in (MoneyCurrency.proven("EUR"), MoneyCurrency.unproven()):
            assert MoneyCurrency.from_payload(currency.as_payload_fields()) == currency
        assert MoneyCurrency.unproven().status is CurrencyStatus.UNPROVEN
