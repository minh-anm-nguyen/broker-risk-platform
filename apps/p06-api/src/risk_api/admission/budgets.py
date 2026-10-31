"""Deadlines and sub-budgets.

Design doc section 7.2 splits one interactive deadline into capture, compute
and final-check sub-budgets. A ``Deadline`` is passed down the call chain so
every stage knows how much time it actually has left, rather than each stage
assuming the full budget.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable

from ..contracts.problem_details import ApiProblem

__all__ = ["Deadline", "RequestBudget", "Clock", "monotonic_clock"]

Clock = Callable[[], float]


def monotonic_clock() -> float:
    return time.monotonic()


@dataclass(frozen=True, slots=True)
class Deadline:
    """An absolute budget expressed on a monotonic clock."""

    expires_at: float
    clock: Clock = monotonic_clock

    @classmethod
    def after(cls, seconds: float, *, clock: Clock = monotonic_clock) -> "Deadline":
        return cls(expires_at=clock() + seconds, clock=clock)

    def remaining(self) -> float:
        return self.expires_at - self.clock()

    def expired(self) -> bool:
        return self.remaining() <= 0

    def check(self, *, stage: str) -> None:
        """Raise the documented timeout problem if the budget is gone."""
        if self.expired():
            raise ApiProblem(
                "DEADLINE_EXCEEDED",
                f"the request exceeded its server deadline during {stage}",
                reason_codes=("DEADLINE_EXCEEDED",),
            )

    def slice(self, seconds: float) -> float:
        """A sub-budget that can never outlive the overall deadline."""
        return max(0.0, min(seconds, self.remaining()))


@dataclass(frozen=True, slots=True)
class RequestBudget:
    """Named sub-budgets for one request class."""

    total_seconds: float
    capture_seconds: float
    compute_seconds: float
    final_check_seconds: float

    def __post_init__(self) -> None:
        parts = self.capture_seconds + self.compute_seconds + self.final_check_seconds
        if parts > self.total_seconds:
            raise ValueError("sub-budgets must fit inside the total deadline")

    def deadline(self, *, clock: Clock = monotonic_clock) -> Deadline:
        return Deadline.after(self.total_seconds, clock=clock)
