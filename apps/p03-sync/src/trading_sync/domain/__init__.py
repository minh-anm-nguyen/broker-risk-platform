"""Pure domain rules: identifiers, money, time, canonical hashing, page validation.

Nothing here performs I/O, reads a clock or touches the database. That is
deliberate: these are the rules deciding whether a source response is
admissible at all, and they must be testable without a server so the money and
identity vectors can be exercised exhaustively (design 15.1, "Pure unit" row).

The guiding rule throughout is **reject, do not repair**. Every function raises
rather than coercing, because each coercion this layer could perform --
rounding a thirteenth decimal place, reading a naive timestamp as server-local,
turning an unknown side into ``BUY`` -- would silently change a number or a
fact somebody is accountable for.
"""

from __future__ import annotations

from trading_sync.domain.canonical import (
    CANONICAL_HASH_VERSION,
    canonical_decimal_str,
    canonical_payload,
    page_hash,
    payload_hash,
)
from trading_sync.domain.entities import (
    MAX_SYMBOL_LENGTH,
    AccountRecord,
    DealRecord,
    PositionRecord,
)
from trading_sync.domain.identifiers import (
    FILTER_UNFILTERED,
    MAX_IDENTIFIER_LENGTH,
    normalize_identifier,
)
from trading_sync.domain.money import (
    MAX_INTEGER_DIGITS,
    MONEY_PRECISION,
    MONEY_SCALE,
    parse_money,
    parse_positive_money,
)
from trading_sync.domain.snapshots import (
    Entity,
    EpochStatus,
    Presence,
    RawPage,
    ScanStatus,
    Side,
    SourceIdentity,
    SourceKind,
    ValidatedPage,
    parse_side,
)
from trading_sync.domain.timestamps import MAX_YEAR, MIN_YEAR, parse_utc_timestamp
from trading_sync.domain.validation import (
    DEFAULT_MAX_PAGE_ITEMS,
    DEFAULT_MAX_PAGES,
    ScanValidationState,
    validate_page,
)

__all__ = [
    "CANONICAL_HASH_VERSION",
    "DEFAULT_MAX_PAGES",
    "DEFAULT_MAX_PAGE_ITEMS",
    "FILTER_UNFILTERED",
    "MAX_IDENTIFIER_LENGTH",
    "MAX_INTEGER_DIGITS",
    "MAX_SYMBOL_LENGTH",
    "MAX_YEAR",
    "MIN_YEAR",
    "MONEY_PRECISION",
    "MONEY_SCALE",
    "AccountRecord",
    "DealRecord",
    "Entity",
    "EpochStatus",
    "PositionRecord",
    "Presence",
    "RawPage",
    "ScanStatus",
    "ScanValidationState",
    "Side",
    "SourceIdentity",
    "SourceKind",
    "ValidatedPage",
    "canonical_decimal_str",
    "canonical_payload",
    "normalize_identifier",
    "page_hash",
    "parse_money",
    "parse_positive_money",
    "parse_side",
    "parse_utc_timestamp",
    "payload_hash",
    "validate_page",
]
