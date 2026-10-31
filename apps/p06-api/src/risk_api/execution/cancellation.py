"""Late-result fencing.

``asyncio.wait_for`` abandons the *await*; it does not stop a thread or process
that is already burning CPU. So a timed-out computation can still finish and
try to hand back a result. The fence here makes that harmless: every submission
gets a generation token, and a result whose token has been retired is counted
and dropped instead of being returned to whoever is asking now.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from typing import Generic, TypeVar

__all__ = ["GenerationFence", "FenceToken", "LateResultDiscarded"]

T = TypeVar("T")


class LateResultDiscarded(RuntimeError):
    """A result arrived after its request was abandoned."""

    def __init__(self, token: "FenceToken") -> None:
        super().__init__(f"late result for retired generation {token.generation}")
        self.token = token


@dataclass(frozen=True, slots=True)
class FenceToken:
    """Identifies one submission attempt."""

    generation: int
    request_id: str


@dataclass(slots=True)
class GenerationFence(Generic[T]):
    """Tracks which submissions are still wanted."""

    _counter: itertools.count = field(default_factory=lambda: itertools.count(1))
    _live: set[int] = field(default_factory=set)
    discarded: int = 0
    retired: int = 0

    def issue(self, request_id: str) -> FenceToken:
        generation = next(self._counter)
        self._live.add(generation)
        return FenceToken(generation=generation, request_id=request_id)

    def retire(self, token: FenceToken) -> None:
        """Stop wanting this submission's result."""
        if token.generation in self._live:
            self._live.discard(token.generation)
            self.retired += 1

    def is_live(self, token: FenceToken) -> bool:
        return token.generation in self._live

    def accept(self, token: FenceToken, value: T) -> T:
        """Return the value only if its submission is still wanted."""
        if token.generation not in self._live:
            self.discarded += 1
            raise LateResultDiscarded(token)
        self._live.discard(token.generation)
        return value

    @property
    def live_count(self) -> int:
        return len(self._live)
