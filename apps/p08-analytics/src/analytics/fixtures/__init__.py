"""Deterministic golden fixtures G08-01 .. G08-20 (section 9.3).

These live in the package rather than under ``tests/`` because the benchmark
and CLI tools need them too, and because a fixture that can only be built by
the test suite tends to drift from the one the design describes.

The generator is seeded and uses no wall clock, so a fixture rendered today
and a fixture rendered in CI next month are byte-identical. That is what makes
"rebuilding identical pinned inputs yields an identical digest" (INV08-22) a
testable claim rather than an aspiration.
"""

from .golden import (
    GOLDEN_FIXTURES,
    DealSpec,
    GoldenFixture,
    deal_envelope,
    fixture,
    fixture_ids,
    generate_deals,
)

__all__ = [
    "GOLDEN_FIXTURES",
    "DealSpec",
    "GoldenFixture",
    "deal_envelope",
    "fixture",
    "fixture_ids",
    "generate_deals",
]
