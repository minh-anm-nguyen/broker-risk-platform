"""Canonical hashing: the definition of "the same record".

This hash decides whether a replayed deal is a harmless duplicate (INV-04) or
a money-integrity incident (INV-05). Both failure directions are costly: too
strict and every trailing-zero change raises a false conflict until operators
stop reading the alerts; too loose and a changed commission passes as a
duplicate.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from trading_sync.domain.canonical import (
    CANONICAL_HASH_VERSION,
    canonical_decimal_str,
    canonical_payload,
    page_hash,
    payload_hash,
)

pytestmark = pytest.mark.unit


@pytest.mark.whatif("WF3-095")
@pytest.mark.parametrize(
    ("left", "right"),
    [("1.0", "1.00"), ("1", "1.000000"), ("-0.50", "-0.5"), ("0", "0.00"), ("0", "-0")],
)
def test_numerically_equal_decimals_hash_identically(left: str, right: str) -> None:
    """Formatting is not content.

    A source that starts sending ``1.00`` where it used to send ``1.0`` has not
    changed any value, and must not look like it rewrote history.
    """
    assert payload_hash("deal", {"v": Decimal(left)}) == payload_hash(
        "deal", {"v": Decimal(right)}
    )


@pytest.mark.whatif("WF3-028")
@pytest.mark.parametrize(("left", "right"), [("-2.50", "-25.00"), ("1.00001", "1.00002")])
def test_different_amounts_hash_differently(left: str, right: str) -> None:
    """The case the whole conflict mechanism exists to catch."""
    assert payload_hash("deal", {"v": Decimal(left)}) != payload_hash(
        "deal", {"v": Decimal(right)}
    )


@pytest.mark.whatif("WF3-095")
@pytest.mark.parametrize(
    ("value", "expected"),
    [("1.00", "1"), ("10", "10"), ("1E+2", "100"), ("-0.0", "0"), ("0.010", "0.01")],
)
def test_decimal_canonical_form(value: str, expected: str) -> None:
    """``normalize()`` alone yields ``1E+1`` for ten; plain notation is restored."""
    assert canonical_decimal_str(Decimal(value)) == expected


@pytest.mark.whatif("WF3-173")
def test_hash_is_version_tagged() -> None:
    """A rule change must change every digest, visibly.

    Without the tag, redeploying a different canonicalisation would make stored
    rows start reading as conflicts with no indication why (WF3-173).
    """
    rendered = canonical_payload("deal", {"v": Decimal("1")})
    assert rendered.startswith(f"{CANONICAL_HASH_VERSION}|deal|")


def test_key_order_does_not_affect_the_hash() -> None:
    assert payload_hash("deal", {"a": 1, "b": 2}) == payload_hash("deal", {"b": 2, "a": 1})


def test_entity_is_part_of_the_hash() -> None:
    """Identical fields under different entities are different records."""
    assert payload_hash("deal", {"id": "1"}) != payload_hash("account", {"id": "1"})


@pytest.mark.whatif("WF3-090")
def test_equal_instants_in_different_zones_hash_identically() -> None:
    utc = datetime(2026, 3, 1, 2, 30, tzinfo=timezone.utc)
    offset = datetime(2026, 3, 1, 2, 30, tzinfo=timezone.utc).astimezone()
    assert payload_hash("deal", {"t": utc}) == payload_hash("deal", {"t": offset})


def test_naive_datetime_cannot_be_canonicalized() -> None:
    """Hashing a naive datetime would hash an ambiguous value."""
    with pytest.raises(ValueError):
        payload_hash("deal", {"t": datetime(2026, 3, 1, 2, 30)})


@pytest.mark.whatif("WF3-086")
def test_binary_float_cannot_be_canonicalized() -> None:
    """A float reaching this layer means money escaped validation upstream."""
    with pytest.raises(ValueError):
        payload_hash("deal", {"v": 0.1})


@pytest.mark.whatif("WF3-038", "WF3-059")
def test_page_hash_covers_provenance_not_only_items() -> None:
    """The same rows under a different snapshot are a different page.

    This is what lets a receipt replay detect that the source changed its
    answer rather than simply resent it (WF3-059).
    """
    base = dict(
        entity="deals", run_id="r1", generation=0, page_index=0, item_hashes=("a", "b")
    )
    assert page_hash(snapshot_seq=10, **base) != page_hash(snapshot_seq=11, **base)
    assert page_hash(snapshot_seq=10, **base) == page_hash(snapshot_seq=10, **base)


def test_page_hash_is_order_sensitive() -> None:
    """Deal page ordering is part of the contract being verified, so it counts."""
    base = dict(entity="deals", run_id="r1", generation=0, snapshot_seq=10, page_index=0)
    assert page_hash(item_hashes=("a", "b"), **base) != page_hash(
        item_hashes=("b", "a"), **base
    )
