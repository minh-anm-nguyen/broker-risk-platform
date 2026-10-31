"""Timestamp parsing: tz-aware in, UTC out, instant preserved.

A timestamp without an offset is ambiguous, and both ways of resolving the
ambiguity -- assume UTC, or assume the server's zone -- are wrong often enough
to be dangerous. A naive timestamp is therefore rejected rather than
interpreted (AS-08, WF3-091).

An offset timestamp is converted to UTC. Conversion changes the
*representation* only: ``2026-03-01T09:30:00+07:00`` and
``2026-03-01T02:30:00Z`` are one instant and must hash identically, which is
what makes a fixture spanning a DST boundary reproducible (WF3-090, WF3-096,
INV-13).
"""

from __future__ import annotations

from datetime import datetime, timezone

from trading_sync.errors import InvalidTimestampError

__all__ = ["MAX_YEAR", "MIN_YEAR", "parse_utc_timestamp"]

#: A bounded plausible window. The upper bound stops a year-9999 value from
#: reaching a date index or a retention calculation (WF3-094).
MIN_YEAR = 1970
MAX_YEAR = 2100


def parse_utc_timestamp(raw: object, *, field: str) -> datetime:
    """Parse an ISO-8601 timestamp that carries an explicit UTC offset."""
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise InvalidTimestampError(
            "timestamp must be a JSON string",
            field=field,
            actual_type=type(raw).__name__,
        )
    if raw == "" or raw != raw.strip():
        raise InvalidTimestampError("timestamp is empty or padded", field=field)
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        raise InvalidTimestampError(
            "timestamp is not ISO-8601", field=field, length=len(raw)
        ) from None
    if parsed.tzinfo is None or parsed.tzinfo.utcoffset(parsed) is None:
        raise InvalidTimestampError(
            "timestamp has no UTC offset; refusing to assume one", field=field
        )
    instant = parsed.astimezone(timezone.utc)
    if not MIN_YEAR <= instant.year <= MAX_YEAR:
        raise InvalidTimestampError(
            "timestamp is outside the accepted window",
            field=field,
            year=instant.year,
            min_year=MIN_YEAR,
            max_year=MAX_YEAR,
        )
    return instant
