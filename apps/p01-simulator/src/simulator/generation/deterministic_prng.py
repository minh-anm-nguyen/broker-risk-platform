"""Counter-based deterministic pseudo-randomness.

A single seeded ``random.Random`` is not enough here, and the reason is worth
stating plainly: its output depends on the *order and number* of calls made
against it. Add a draw, reorder two draws, or let a second thread share the
object, and every dataset generated afterwards changes while the seed stays
the same. The bug then looks like "the simulator is non-deterministic", which
is the one property this component exists to provide.

So no mutable generator state exists. Every value is derived independently
from its own coordinates:

    SHA-256(version | seed | spec_digest | purpose | index | nonce)

A value therefore depends only on *what it is*, never on what was drawn
before it. Adding a new kind of draw with a new ``purpose`` cannot disturb any
existing one, which makes the generator safe to extend without invalidating
golden fixtures (section 4.4).

Modulo bias: ranges are reduced from a 256-bit draw, so the bias is bounded by
``span / 2**256``. For any span this generator will ever see that is far below
anything observable and far below the precision of the data it produces.
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from decimal import Decimal
from typing import TypeVar

from .. import GENERATOR_VERSION
from ..domain.accounting import calc_context

T = TypeVar("T")

_BYTE_ORDER = "big"
_DRAW_BYTES = 32


class DeterministicRandom:
    """Stateless derivation of reproducible values from fixed coordinates."""

    __slots__ = ("_prefix",)

    def __init__(self, seed: int, spec_digest: str, *, version: str = GENERATOR_VERSION) -> None:
        if seed < 0:
            raise ValueError("seed must not be negative")
        # The version is part of the key material: changing a generation
        # algorithm is a breaking change, and the digest should say so.
        self._prefix = f"{version}|{seed}|{spec_digest}|".encode()

    def _draw(self, purpose: str, index: int, nonce: int) -> int:
        digest = hashlib.sha256(
            self._prefix + f"{purpose}|{index}|{nonce}".encode()
        ).digest()
        return int.from_bytes(digest[:_DRAW_BYTES], _BYTE_ORDER)

    def uniform_int(self, purpose: str, index: int, *, low: int, high: int, nonce: int = 0) -> int:
        """Uniform integer in the inclusive range ``[low, high]``."""
        if high < low:
            raise ValueError(f"empty range [{low}, {high}] for purpose {purpose!r}")
        span = high - low + 1
        return low + (self._draw(purpose, index, nonce) % span)

    def choice(self, purpose: str, index: int, options: Sequence[T], nonce: int = 0) -> T:
        if not options:
            raise ValueError(f"no options to choose from for purpose {purpose!r}")
        return options[self._draw(purpose, index, nonce) % len(options)]

    def weighted_choice(
        self, purpose: str, index: int, weighted: Sequence[tuple[T, int]], nonce: int = 0
    ) -> T:
        """Pick from ``(value, weight)`` pairs.

        The caller's order is significant and must be stable: reordering the
        pairs changes which value a given draw selects.
        """
        total = sum(weight for _, weight in weighted)
        if total <= 0:
            raise ValueError(f"weights must sum to a positive value for purpose {purpose!r}")
        point = self._draw(purpose, index, nonce) % total
        cumulative = 0
        for value, weight in weighted:
            cumulative += weight
            if point < cumulative:
                return value
        return weighted[-1][0]  # pragma: no cover - unreachable while total matches

    def uniform_decimal(
        self,
        purpose: str,
        index: int,
        *,
        low: Decimal,
        high: Decimal,
        step: Decimal,
        nonce: int = 0,
    ) -> Decimal:
        """Uniform value on the grid ``low, low+step, ..., <= high``.

        Snapping to a grid rather than returning an arbitrary fraction is what
        keeps generated volumes and prices valid against the symbol's lot step
        and digit count without a rounding pass that could round them off it.
        """
        if step <= 0:
            raise ValueError("step must be positive")
        if high < low:
            raise ValueError(f"empty range for purpose {purpose!r}")
        with calc_context():
            steps = int((high - low) / step)
            offset = self.uniform_int(purpose, index, low=0, high=steps, nonce=nonce)
            return low + step * offset

    def probability_hit(
        self, purpose: str, index: int, probability: Decimal, nonce: int = 0
    ) -> bool:
        """Deterministic Bernoulli trial.

        Used by probabilistic fault rules so that "random" activation is still
        reproducible from ``(seed, rule_id, matching_request_index)`` and a
        failing chaos run can be replayed exactly (section 6.3).
        """
        if probability <= 0:
            return False
        if probability >= 1:
            return True
        # Compare on a 1e9 grid: finer than any probability a rule should use,
        # and exact in integer arithmetic.
        scale = 1_000_000_000
        with calc_context():
            threshold = int(probability * scale)
        return (self._draw(purpose, index, nonce) % scale) < threshold
