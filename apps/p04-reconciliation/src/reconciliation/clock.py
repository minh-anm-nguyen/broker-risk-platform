"""Time source abstraction.

Design rule: evidence age, plan expiry, approval expiry and lease expiry are
all *decisions* in this service (section 8.1), so the tests that cover them
must be able to move time deliberately rather than sleep and hope. Production
code always takes a ``Clock``; only ``SystemClock`` reads the wall clock.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol


def to_iso(moment: datetime) -> str:
    """Render a UTC instant in the fixed form used in every stored column."""
    if moment.tzinfo is None:
        raise ValueError("refusing to store a naive timestamp")
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"


def from_iso(text: str) -> datetime:
    candidate = text[:-1] + "+00:00" if text.endswith("Z") else text
    moment = datetime.fromisoformat(candidate)
    if moment.tzinfo is None:
        raise ValueError(f"stored timestamp is not timezone-aware: {text!r}")
    return moment.astimezone(timezone.utc)


class Clock(Protocol):
    def now(self) -> datetime: ...


class SystemClock:
    """Wall clock, always UTC-aware."""

    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class ManualClock:
    """Test clock that only moves when a test moves it.

    Used by every expiry test so "the approval is 6 minutes old" is a fact the
    test establishes, not a race it waits for.
    """

    def __init__(self, start: datetime | None = None) -> None:
        self._now = start or datetime(2026, 1, 1, tzinfo=timezone.utc)

    def now(self) -> datetime:
        return self._now

    def advance(self, seconds: float) -> datetime:
        self._now = self._now + timedelta(seconds=seconds)
        return self._now

    def set(self, moment: datetime) -> None:
        if moment.tzinfo is None:
            raise ValueError("ManualClock requires an aware datetime")
        self._now = moment.astimezone(timezone.utc)
