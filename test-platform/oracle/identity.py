"""Which records exist, compared as sets.

Counting is the check that fails to fail. If the source has 26 deals and the
database has 26 deals, a count comparison passes even when they are not the
same 26 - one lost on the way in and one invented on the way out cancel out
perfectly. P09 section 2.3 says so in as many words, which is why nothing in
this module returns a number as its verdict.

The verdict names the records. An operator reading `missing={'D-00000017'}` can
go and look at that deal; an operator reading `expected 26, got 26` learns
nothing at all.
"""

from __future__ import annotations

from dataclasses import dataclass, field

__all__ = ["IdentityVerdict", "compare_identity_sets", "SAMPLE_LIMIT"]

#: How many offending ids a verdict carries. A diff of ten thousand ids is no
#: more useful than a diff of twenty and it makes an evidence bundle unreadable.
#: The counts stay exact regardless; only the listing is capped.
SAMPLE_LIMIT = 20


@dataclass(frozen=True, slots=True)
class IdentityVerdict:
    """The outcome of one set comparison."""

    label: str
    source_total: int
    target_total: int
    matched: int
    missing_total: int
    unexpected_total: int
    missing_sample: tuple[str, ...] = field(default_factory=tuple)
    unexpected_sample: tuple[str, ...] = field(default_factory=tuple)

    @property
    def ok(self) -> bool:
        """True only when the two sets are identical.

        Not "close enough" and not "the counts agree": one record on either
        side is a failure, because a platform that can lose one deal can lose
        any deal.
        """
        return self.missing_total == 0 and self.unexpected_total == 0

    def describe(self) -> str:
        if self.ok:
            return (
                f"{self.label}: OK - {self.matched} records, identical sets "
                f"(source {self.source_total}, target {self.target_total})"
            )
        parts = [f"{self.label}: MISMATCH"]
        if self.missing_total:
            sample = ", ".join(self.missing_sample)
            more = "" if self.missing_total <= len(self.missing_sample) else ", ..."
            parts.append(
                f"{self.missing_total} in the source but not the target [{sample}{more}]"
            )
        if self.unexpected_total:
            sample = ", ".join(self.unexpected_sample)
            more = "" if self.unexpected_total <= len(self.unexpected_sample) else ", ..."
            parts.append(
                f"{self.unexpected_total} in the target but not the source [{sample}{more}]"
            )
        # Printed last, deliberately: it is the number that would have looked
        # perfectly fine on its own.
        parts.append(f"counts were source={self.source_total} target={self.target_total}")
        return "; ".join(parts)


def compare_identity_sets(
    label: str, source_ids: set[str], target_ids: set[str]
) -> IdentityVerdict:
    """Compare two identity sets exactly.

    ``source_ids`` is what the source says exists; ``target_ids`` is what was
    persisted or served. Both are sets of strings, because an id is an identity
    and not a number: ``"00042"`` and ``42`` are different account ids and
    coercing either way has already lost information.
    """
    missing = sorted(source_ids - target_ids)
    unexpected = sorted(target_ids - source_ids)
    return IdentityVerdict(
        label=label,
        source_total=len(source_ids),
        target_total=len(target_ids),
        matched=len(source_ids & target_ids),
        missing_total=len(missing),
        unexpected_total=len(unexpected),
        missing_sample=tuple(missing[:SAMPLE_LIMIT]),
        unexpected_sample=tuple(unexpected[:SAMPLE_LIMIT]),
    )
