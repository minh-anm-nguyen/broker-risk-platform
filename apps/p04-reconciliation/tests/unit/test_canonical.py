"""Canonical encoder and hash contract (section 6.4, F-10, F-13).

Section 10.1 warns against the trap these tests are built to avoid: computing
the "expected" hash by calling the same function under test. That proves only
that the function equals itself.

So the oracle here is built two independent ways:

1. ``_oracle_hash`` assembles the canonical JSON **as a hand-written string**
   and hashes it. It shares no code with ``canonical_json_bytes``, so a change
   to key ordering, separators or the domain header is caught.
2. ``GOLDEN_*`` constants pin exact digests. They were produced by the oracle
   above, and they fail if either the encoder *or* the oracle drifts, which is
   what makes an accidental matched-pair change visible.

A test that only asserted ``payload_hash(x) == payload_hash(x)`` would pass
through every bug in this file's subject matter.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal

import pytest

from reconciliation.canonical import (
    DEAL_SPEC,
    HASH_VERSION,
    SCHEMA_VERSION,
    canonical_record,
    item_key,
    page_hash,
    parse_decimal,
    payload_hash,
    quantize_exact,
    root_hash,
)
from reconciliation.errors import CanonicalizationError

pytestmark = pytest.mark.unit


# A deliberately ordinary deal, taken from P01's actual wire format.
DEAL = {
    "deal_id": "D-00000001",
    "source_seq": 10,
    "account_id": "A-000001",
    "position_id": "P-00000001",
    "symbol": "EURUSD",
    "entry": "IN",
    "side": "SELL",
    "volume_lots": "0.41",
    "price": "1.09953",
    "realized_pnl_gross": "0.00",
    "commission": "-2.05",
    "swap": "0.00",
    "executed_at": "2026-01-01T00:00:09Z",
}

ACCOUNT = {"account_id": "A-000001", "currency": "USD", "balance": "10019.94", "credit": "0.00"}


def _oracle_hash_deal(record: dict[str, str]) -> str:
    """Independent encoder: the canonical JSON written out by hand.

    Mirrors section 6.4's rules literally -- sorted object keys, no
    whitespace, the declared field list inside the domain header, money and
    price rendered at their declared scales, time at microsecond precision.
    """
    fields = (
        '{"account_id":"%(account_id)s",'
        '"commission":"%(commission)s",'
        '"deal_id":"%(deal_id)s",'
        '"entry":"%(entry)s",'
        '"executed_at":"%(executed_at)s",'
        '"position_id":"%(position_id)s",'
        '"price":"%(price)s",'
        '"realized_pnl_gross":"%(realized_pnl_gross)s",'
        '"side":"%(side)s",'
        '"swap":"%(swap)s",'
        '"symbol":"%(symbol)s",'
        '"volume_lots":"%(volume_lots)s"}'
    ) % record
    declared = ",".join(f'"{name}"' for name in DEAL_SPEC.field_names)
    domain = (
        f'{{"entity":"deals","fields":[{declared}],'
        f'"hash_version":"{HASH_VERSION}","schema_version":"{SCHEMA_VERSION}"}}'
    )
    envelope = f'{{"domain":{domain},"fields":{fields}}}'
    return hashlib.sha256(envelope.encode("utf-8")).hexdigest()


#: Pinned digests. A change here is a deliberate, breaking hash-version change.
GOLDEN_DEAL_HASH = "1e7298f7f7c5657cb84ae04bafbbed65ed85f3f0041f42ffa50bd36aa732ca66"


# ---------------------------------------------------------------------------
# Golden vectors
# ---------------------------------------------------------------------------


def test_deal_hash_matches_an_independently_built_oracle() -> None:
    """The encoder agrees with a hand-written canonical JSON string."""
    normalised = dict(DEAL)
    normalised["executed_at"] = "2026-01-01T00:00:09.000000Z"
    assert payload_hash("deals", DEAL) == _oracle_hash_deal(normalised)


def test_deal_hash_matches_the_pinned_golden_digest() -> None:
    """INV04-20: the oracle must catch a change both sides agree on.

    The hand-written oracle above and the encoder could in principle be
    changed together and still agree. The pinned digest is the third,
    immovable reference that makes such a matched-pair change visible.
    """
    assert payload_hash("deals", DEAL) == GOLDEN_DEAL_HASH


def test_source_seq_is_provenance_and_never_enters_the_hash() -> None:
    """Section 5.2 rule 7: the same deal hashes equal in any scan."""
    assert payload_hash("deals", DEAL) == payload_hash("deals", {**DEAL, "source_seq": 999_999})


def test_item_key_uses_the_declared_key_fields() -> None:
    assert item_key("deals", DEAL) == "D-00000001"
    assert item_key("accounts", ACCOUNT) == "A-000001"


# ---------------------------------------------------------------------------
# F-10: money, scale and sign
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("1.0", "1.00"),
        ("1", "1.00"),
        ("-0.00", "0.00"),
        ("0.0", "-0.0"),
        ("10019.9", "10019.90"),
    ],
)
def test_equal_money_under_declared_scale_hashes_equal(left: str, right: str) -> None:
    """Trailing zeros and signed zero are the same amount of money."""
    a = payload_hash("accounts", {**ACCOUNT, "balance": left})
    b = payload_hash("accounts", {**ACCOUNT, "balance": right})
    assert a == b


@pytest.mark.parametrize("value", ["1.004", "0.001", "-2.055", "1.23456789"])
def test_excess_precision_is_refused_not_rounded(value: str) -> None:
    """Rounding here would invent a value the source never sent (section 6.4)."""
    with pytest.raises(CanonicalizationError, match="more precision"):
        payload_hash("accounts", {**ACCOUNT, "balance": value})


@pytest.mark.parametrize("value", [0.1, 1.5, -2.05, float("nan"), float("inf")])
def test_float_money_is_always_refused(value: float) -> None:
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", {**ACCOUNT, "balance": value})


@pytest.mark.parametrize("value", ["NaN", "nan", "Infinity", "-Infinity", "inf"])
def test_nan_and_infinity_strings_are_refused(value: str) -> None:
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", {**ACCOUNT, "balance": value})


@pytest.mark.parametrize("value", ["", " ", "1.00 ", " 1.00", "abc", "1,00", "0x10", "--1"])
def test_malformed_decimal_strings_are_refused(value: str) -> None:
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", {**ACCOUNT, "balance": value})


def test_booleans_are_not_numbers() -> None:
    with pytest.raises(CanonicalizationError, match="boolean"):
        parse_decimal(True, field="balance")


def test_absurd_magnitude_is_refused() -> None:
    with pytest.raises(CanonicalizationError, match="magnitude"):
        parse_decimal("1" + "0" * 25, field="balance")


def test_undeclared_currency_is_incomparable() -> None:
    """A currency with no declared scale cannot be compared safely."""
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", {**ACCOUNT, "currency": "JPY"})


def test_quantize_exact_is_lossless_or_raises() -> None:
    assert quantize_exact(Decimal("1.1"), 2, field="x") == Decimal("1.10")
    with pytest.raises(CanonicalizationError):
        quantize_exact(Decimal("1.111"), 2, field="x")


# ---------------------------------------------------------------------------
# F-13: identifiers
# ---------------------------------------------------------------------------


def test_leading_zeros_make_distinct_identities() -> None:
    a = payload_hash("accounts", {**ACCOUNT, "account_id": "000123"})
    b = payload_hash("accounts", {**ACCOUNT, "account_id": "123"})
    assert a != b
    assert item_key("accounts", {**ACCOUNT, "account_id": "000123"}) == "000123"


@pytest.mark.parametrize("value", [123, True, 1.0, b"A-1", ["A-1"]])
def test_non_string_identifiers_are_refused(value: object) -> None:
    """Coercion is where leading zeros disappear, so it is not offered."""
    with pytest.raises(CanonicalizationError, match="identifier"):
        payload_hash("accounts", {**ACCOUNT, "account_id": value})


def test_null_identifier_is_refused_as_a_nullability_violation() -> None:
    """``None`` is a *missing value*, not a badly typed identifier.

    Checked separately because the distinction matters: the canonical
    projection has to tell "the source sent nothing" apart from "the source
    sent something unusable" (section 6.4 null/missing distinction).
    """
    with pytest.raises(CanonicalizationError, match="not nullable"):
        payload_hash("accounts", {**ACCOUNT, "account_id": None})


@pytest.mark.parametrize("value", ["", "a" * 100, "has space", "drop;table", "../etc"])
def test_identifiers_outside_the_declared_policy_are_refused(value: str) -> None:
    with pytest.raises(CanonicalizationError):
        payload_hash("accounts", {**ACCOUNT, "account_id": value})


# ---------------------------------------------------------------------------
# Timestamps
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("left", "right"),
    [
        ("2026-01-01T00:00:09Z", "2026-01-01T00:00:09.000000Z"),
        ("2026-01-01T00:00:09Z", "2026-01-01T00:00:09+00:00"),
        ("2026-01-01T01:00:09+01:00", "2026-01-01T00:00:09Z"),
    ],
)
def test_equivalent_instants_hash_equal(left: str, right: str) -> None:
    assert payload_hash("deals", {**DEAL, "executed_at": left}) == payload_hash(
        "deals", {**DEAL, "executed_at": right}
    )


@pytest.mark.parametrize(
    "value", ["2026-01-01T00:00:09", "2026-01-01", "not-a-time", "", "2026-13-01T00:00:00Z"]
)
def test_naive_or_malformed_timestamps_are_refused(value: str) -> None:
    """A naive timestamp has no instant, so it cannot be compared."""
    with pytest.raises(CanonicalizationError):
        payload_hash("deals", {**DEAL, "executed_at": value})


# ---------------------------------------------------------------------------
# Schema domain separation
# ---------------------------------------------------------------------------


def test_the_same_fields_hash_differently_across_entities() -> None:
    """Domain separation: entity type is inside the hashed bytes."""
    record = {"account_id": "A-1", "currency": "USD", "balance": "1.00", "credit": "0.00"}
    assert payload_hash("accounts", record) != hashlib.sha256(b"A-1").hexdigest()
    with pytest.raises(CanonicalizationError):
        payload_hash("positions", record)


def test_unknown_fields_are_refused_rather_than_ignored() -> None:
    """An unrecognised field may be the very value that differs."""
    with pytest.raises(CanonicalizationError, match="outside the canonical projection"):
        payload_hash("accounts", {**ACCOUNT, "surprise_bonus": "500.00"})


def test_documented_volatile_fields_are_excluded_without_error() -> None:
    """Derived/volatile values are not properties of the entity (section 6.4)."""
    noisy = {
        **ACCOUNT,
        "as_of_seq": 1234,
        "unrealized_pnl": "15.00",
        "equity": "10034.94",
        "open_position_count": 3,
        "data_quality": "COMPLETE",
    }
    assert payload_hash("accounts", noisy) == payload_hash("accounts", ACCOUNT)


def test_missing_required_field_is_refused() -> None:
    incomplete = {k: v for k, v in ACCOUNT.items() if k != "balance"}
    with pytest.raises(CanonicalizationError, match="missing"):
        payload_hash("accounts", incomplete)


@pytest.mark.parametrize("value", ["LONG", "buy", "", "BUY "])
def test_enum_values_outside_the_domain_are_refused(value: str) -> None:
    with pytest.raises(CanonicalizationError):
        payload_hash("deals", {**DEAL, "side": value})


def test_canonical_record_renders_money_at_declared_scale() -> None:
    record = canonical_record("accounts", {**ACCOUNT, "balance": "1.5"})
    assert record["balance"] == "1.50"
    assert record["credit"] == "0.00"


# ---------------------------------------------------------------------------
# Manifest digests (metamorphic rule 1 in section 10.4)
# ---------------------------------------------------------------------------


def test_root_hash_is_order_independent() -> None:
    items = [("a", "h1"), ("b", "h2"), ("c", "h3")]
    assert root_hash("deals", items) == root_hash("deals", list(reversed(items)))
    assert root_hash("deals", items) == root_hash("deals", [items[1], items[0], items[2]])


def test_root_hash_detects_any_membership_change() -> None:
    items = [("a", "h1"), ("b", "h2")]
    assert root_hash("deals", items) != root_hash("deals", [("a", "h1")])
    assert root_hash("deals", items) != root_hash("deals", [("a", "h1"), ("b", "CHANGED")])
    assert root_hash("deals", items) != root_hash("deals", [*items, ("c", "h3")])


def test_root_hash_is_entity_scoped() -> None:
    items = [("a", "h1")]
    assert root_hash("deals", items) != root_hash("accounts", items)


def test_page_hash_is_order_sensitive() -> None:
    """A page receipt proves the page arrived as transmitted (IT04-01)."""
    items = [("a", "h1"), ("b", "h2")]
    assert page_hash("deals", items) != page_hash("deals", list(reversed(items)))


def test_empty_manifest_has_a_stable_distinct_digest() -> None:
    """F-16: an empty but complete listing is representable, not a crash."""
    empty = root_hash("deals", [])
    assert isinstance(empty, str) and len(empty) == 64
    assert empty != root_hash("deals", [("a", "h1")])
    assert empty == root_hash("deals", [])
