"""Counter-based deterministic randomness.

The property that matters is *not* statistical quality - it is that a value
depends only on its own coordinates. A seeded ``random.Random`` fails that:
inserting one extra draw shifts every value after it, and the dataset changes
while the seed stays the same.

The tests below pin that property down explicitly, because it is the one a
careless refactor breaks.
"""

from __future__ import annotations

from collections import Counter
from decimal import Decimal

import pytest

from simulator.generation.deterministic_prng import DeterministicRandom

pytestmark = pytest.mark.unit

SEED = 42
DIGEST = "sha256:scenario-digest"


@pytest.fixture
def random() -> DeterministicRandom:
    return DeterministicRandom(SEED, DIGEST)


def test_same_coordinates_always_give_the_same_value(random: DeterministicRandom) -> None:
    for index in range(50):
        first = random.uniform_int("volume", index, low=1, high=1000)
        second = random.uniform_int("volume", index, low=1, high=1000)
        assert first == second


def test_two_instances_with_the_same_seed_agree() -> None:
    left = DeterministicRandom(SEED, DIGEST)
    right = DeterministicRandom(SEED, DIGEST)
    assert [left.uniform_int("x", i, low=0, high=99) for i in range(100)] == [
        right.uniform_int("x", i, low=0, high=99) for i in range(100)
    ]


def test_call_order_does_not_affect_values(random: DeterministicRandom) -> None:
    """The defining difference from a stateful RNG.

    Drawing index 7 after index 0, or after a thousand unrelated draws, must
    give the same answer.
    """
    expected = random.uniform_int("action", 7, low=0, high=999)
    for index in range(1000):
        random.uniform_int("noise", index, low=0, high=999)
    assert random.uniform_int("action", 7, low=0, high=999) == expected


def test_purposes_are_independent(random: DeterministicRandom) -> None:
    """Adding a new kind of draw must not disturb existing ones."""
    volumes = [random.uniform_int("volume", i, low=0, high=10_000) for i in range(200)]
    sides = [random.uniform_int("side", i, low=0, high=10_000) for i in range(200)]
    assert volumes != sides
    # Re-deriving the original purpose is unaffected by the new one existing.
    assert [random.uniform_int("volume", i, low=0, high=10_000) for i in range(200)] == volumes


def test_a_different_seed_gives_a_different_stream() -> None:
    """WF-002: a new seed must produce different data, invariants still holding."""
    left = DeterministicRandom(1, DIGEST)
    right = DeterministicRandom(2, DIGEST)
    assert [left.uniform_int("x", i, low=0, high=10_000) for i in range(100)] != [
        right.uniform_int("x", i, low=0, high=10_000) for i in range(100)
    ]


def test_a_different_spec_digest_gives_a_different_stream() -> None:
    """Two scenarios with the same seed are still different scenarios."""
    left = DeterministicRandom(SEED, "sha256:spec-a")
    right = DeterministicRandom(SEED, "sha256:spec-b")
    assert [left.uniform_int("x", i, low=0, high=10_000) for i in range(100)] != [
        right.uniform_int("x", i, low=0, high=10_000) for i in range(100)
    ]


def test_generator_version_is_part_of_the_key() -> None:
    """Changing an algorithm is a breaking change and the digest must say so."""
    left = DeterministicRandom(SEED, DIGEST, version="1.0")
    right = DeterministicRandom(SEED, DIGEST, version="2.0")
    assert [left.uniform_int("x", i, low=0, high=10_000) for i in range(100)] != [
        right.uniform_int("x", i, low=0, high=10_000) for i in range(100)
    ]


def test_uniform_int_respects_an_inclusive_range(random: DeterministicRandom) -> None:
    values = [random.uniform_int("x", i, low=5, high=7) for i in range(600)]
    assert set(values) == {5, 6, 7}


def test_uniform_int_handles_a_single_value_range(random: DeterministicRandom) -> None:
    assert random.uniform_int("x", 0, low=3, high=3) == 3


def test_empty_range_is_an_error(random: DeterministicRandom) -> None:
    with pytest.raises(ValueError):
        random.uniform_int("x", 0, low=5, high=4)


def test_distribution_is_not_obviously_skewed(random: DeterministicRandom) -> None:
    """A sanity bound, not a statistical test.

    With 6000 draws over 6 buckets the expected count is 1000. A derivation bug
    that collapsed the output would show up here immediately; a subtle bias
    would not, and is not what this guards against.
    """
    counts = Counter(random.uniform_int("dice", i, low=1, high=6) for i in range(6_000))
    assert set(counts) == {1, 2, 3, 4, 5, 6}
    assert all(800 < count < 1_200 for count in counts.values()), counts


def test_weighted_choice_follows_its_weights(random: DeterministicRandom) -> None:
    weighted = [("tick", 25), ("open", 35), ("close", 30), ("increase", 10)]
    counts = Counter(random.weighted_choice("action", i, weighted) for i in range(10_000))
    assert set(counts) == {"tick", "open", "close", "increase"}
    # Within a few percent of the declared weights.
    assert 2_200 < counts["tick"] < 2_800
    assert 3_200 < counts["open"] < 3_800
    assert 800 < counts["increase"] < 1_200


def test_weighted_choice_order_is_significant(random: DeterministicRandom) -> None:
    """Documented contract: reordering the pairs changes the dataset."""
    forward = [random.weighted_choice("a", i, [("x", 1), ("y", 1)]) for i in range(50)]
    reversed_pairs = [random.weighted_choice("a", i, [("y", 1), ("x", 1)]) for i in range(50)]
    assert forward != reversed_pairs


def test_zero_weights_are_an_error(random: DeterministicRandom) -> None:
    with pytest.raises(ValueError):
        random.weighted_choice("a", 0, [("x", 0), ("y", 0)])


def test_choice_requires_options(random: DeterministicRandom) -> None:
    with pytest.raises(ValueError):
        random.choice("a", 0, [])


def test_uniform_decimal_stays_on_the_grid(random: DeterministicRandom) -> None:
    """Snapping to the grid is what keeps generated volumes valid.

    An arbitrary fraction would need a rounding pass that could round it off
    the lot step again.
    """
    step = Decimal("0.01")
    values = [
        random.uniform_decimal("volume", i, low=Decimal("0.01"), high=Decimal("1.00"), step=step)
        for i in range(500)
    ]
    for value in values:
        assert Decimal("0.01") <= value <= Decimal("1.00")
        assert (value / step) == (value / step).to_integral_value()
    assert len(set(values)) > 50  # actually varying, not a constant


def test_uniform_decimal_handles_a_negative_range(random: DeterministicRandom) -> None:
    values = [
        random.uniform_decimal(
            "swap", i, low=Decimal("-1.00"), high=Decimal("0.20"), step=Decimal("0.10")
        )
        for i in range(300)
    ]
    assert min(values) >= Decimal("-1.00")
    assert max(values) <= Decimal("0.20")
    # The range reaches above zero, which is what exercises a positive swap.
    assert any(value > 0 for value in values)


def test_uniform_decimal_rejects_a_non_positive_step(random: DeterministicRandom) -> None:
    with pytest.raises(ValueError):
        random.uniform_decimal("x", 0, low=Decimal(0), high=Decimal(1), step=Decimal(0))


@pytest.mark.parametrize(
    ("probability", "expected"),
    [(Decimal("0"), False), (Decimal("1"), True), (Decimal("-0.5"), False), (Decimal("2"), True)],
)
def test_probability_edges_short_circuit(
    random: DeterministicRandom, probability: Decimal, expected: bool
) -> None:
    assert all(
        random.probability_hit("fault", i, probability) is expected for i in range(20)
    )


def test_probability_is_reproducible_and_roughly_calibrated(random: DeterministicRandom) -> None:
    """Section 6.3: a probabilistic fault must still be replayable exactly."""
    first = [random.probability_hit("fault", i, Decimal("0.25")) for i in range(4_000)]
    second = [random.probability_hit("fault", i, Decimal("0.25")) for i in range(4_000)]
    assert first == second
    hits = sum(first)
    assert 850 < hits < 1_150, hits


def test_negative_seed_is_rejected() -> None:
    with pytest.raises(ValueError):
        DeterministicRandom(-1, DIGEST)
