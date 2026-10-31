"""Source identifier normalization.

Account, position and deal ids stay **strings** for their whole life in this
service. The source's formatting is part of the identity: ``'0001'`` and
``'1'`` are two different accounts as far as we are concerned, and an integer
round-trip would merge them (AS-03, WF3-034).

The policy is restrictive on purpose. Every rule below exists to stop two
distinct source keys from becoming one database key, or one source key from
becoming two:

* surrounding whitespace is rejected rather than stripped, so ``' 7'`` cannot
  collide with ``'7'``;
* a value must already be Unicode NFC, so two spellings of one glyph are not
  silently folded together (WF3-192);
* length is capped before the value reaches the database or a log buffer
  (WF3-153).

Quotes, semicolons and other SQL metacharacters are deliberately *allowed*:
the sink only ever passes ids as bound parameters, so there is nothing to
escape, and rejecting them would discard legitimate source data (WF3-100,
WF3-181).
"""

from __future__ import annotations

import unicodedata

from trading_sync.errors import InvalidIdentifierError

__all__ = ["FILTER_UNFILTERED", "MAX_IDENTIFIER_LENGTH", "normalize_identifier"]

#: Only a scan carrying this scope may drive the global deal watermark or mark
#: rows absent. Any narrower scope gets its own checkpoint row (INV-18).
FILTER_UNFILTERED = "UNFILTERED"

#: Generous next to real MT5 logins, small enough that an oversized id is
#: refused before it is allocated, stored or logged.
MAX_IDENTIFIER_LENGTH = 64


def normalize_identifier(
    raw: object,
    *,
    field: str,
    max_length: int = MAX_IDENTIFIER_LENGTH,
) -> str:
    """Return ``raw`` unchanged as a ``str``, or raise.

    "Normalize" here means *validate and pass through*, not transform: the only
    value accepted is one that already satisfies every rule.
    """
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise InvalidIdentifierError(
            "identifier must be a JSON string",
            field=field,
            actual_type=type(raw).__name__,
        )
    if raw == "":
        raise InvalidIdentifierError("identifier is empty", field=field)
    if raw.strip() == "":
        # Whitespace-only ids are a source bug that would otherwise become a
        # real primary-key value (WF3-033).
        raise InvalidIdentifierError("identifier is whitespace only", field=field)
    if raw != raw.strip():
        raise InvalidIdentifierError(
            "identifier has leading or trailing whitespace",
            field=field,
            length=len(raw),
        )
    if len(raw) > max_length:
        raise InvalidIdentifierError(
            "identifier exceeds length cap",
            field=field,
            length=len(raw),
            max_length=max_length,
        )
    for char in raw:
        code = ord(char)
        if code < 0x20 or code == 0x7F:
            raise InvalidIdentifierError(
                "identifier contains a control character",
                field=field,
                code_point=code,
            )
    if unicodedata.normalize("NFC", raw) != raw:
        raise InvalidIdentifierError(
            "identifier is not Unicode NFC normalized", field=field
        )
    return raw
