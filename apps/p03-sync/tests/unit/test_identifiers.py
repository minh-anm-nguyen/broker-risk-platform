"""Identifier policy: one source key must map to exactly one database key."""

from __future__ import annotations

import pytest

from trading_sync.domain.identifiers import MAX_IDENTIFIER_LENGTH, normalize_identifier
from trading_sync.errors import InvalidIdentifierError

pytestmark = pytest.mark.unit


@pytest.mark.whatif("WF3-034")
def test_leading_zeros_are_preserved_as_distinct_ids() -> None:
    """``'0001'`` and ``'1'`` are two accounts, and must stay two.

    An integer round-trip anywhere in the pipeline merges them, which silently
    attributes one account's deals to another (AS-03).
    """
    assert normalize_identifier("0001", field="account_id") == "0001"
    assert normalize_identifier("1", field="account_id") == "1"
    assert normalize_identifier("0001", field="account_id") != normalize_identifier(
        "1", field="account_id"
    )


@pytest.mark.whatif("WF3-033")
@pytest.mark.parametrize("raw", ["", "   ", "\t", " "])
def test_empty_and_whitespace_ids_are_rejected(raw: str) -> None:
    with pytest.raises(InvalidIdentifierError):
        normalize_identifier(raw, field="deal_id")


@pytest.mark.whatif("WF3-033")
@pytest.mark.parametrize("raw", [" 7", "7 ", " 7 "])
def test_padded_ids_are_rejected_rather_than_trimmed(raw: str) -> None:
    """Trimming would let ``' 7'`` collide with ``'7'``."""
    with pytest.raises(InvalidIdentifierError):
        normalize_identifier(raw, field="deal_id")


@pytest.mark.whatif("WF3-100", "WF3-181")
@pytest.mark.parametrize(
    "raw",
    ["O'Brien", 'quote"id', "semi;colon", "drop--table", "id/with/slash", "deal_id=1 OR 1=1"],
)
def test_sql_metacharacters_are_allowed_and_preserved(raw: str) -> None:
    """Nothing is escaped because nothing is interpolated.

    Ids reach SQL only as bound parameters, so rejecting these characters would
    discard legitimate source data without buying any safety. The injection
    defence is parameterisation, asserted end-to-end in the integration suite.
    """
    assert normalize_identifier(raw, field="deal_id") == raw


@pytest.mark.whatif("WF3-153")
def test_oversized_ids_are_rejected_before_allocation() -> None:
    with pytest.raises(InvalidIdentifierError) as caught:
        normalize_identifier("x" * (MAX_IDENTIFIER_LENGTH + 1), field="account_id")
    assert caught.value.safe_details["max_length"] == MAX_IDENTIFIER_LENGTH


@pytest.mark.whatif("WF3-192")
def test_non_nfc_identifiers_are_rejected_rather_than_folded() -> None:
    """Two spellings of one glyph must not become one key by our choice.

    ``'e'`` plus a combining acute is a different byte sequence from the
    precomposed character. Normalising would merge them; rejecting the
    decomposed form keeps the mapping explicit and lossless.
    """
    decomposed = "cafe" + "\u0301"
    with pytest.raises(InvalidIdentifierError):
        normalize_identifier(decomposed, field="account_id")
    assert normalize_identifier("caf\u00e9", field="account_id") == "caf\u00e9"


@pytest.mark.whatif("WF3-192")
@pytest.mark.parametrize("raw", ["a\x00b", "a\nb", "a\x7fb"])
def test_control_characters_are_rejected(raw: str) -> None:
    with pytest.raises(InvalidIdentifierError):
        normalize_identifier(raw, field="deal_id")


@pytest.mark.whatif("WF3-033")
@pytest.mark.parametrize("raw", [1, 1.5, None, True, b"1"])
def test_non_string_ids_are_rejected(raw: object) -> None:
    with pytest.raises(InvalidIdentifierError):
        normalize_identifier(raw, field="deal_id")
