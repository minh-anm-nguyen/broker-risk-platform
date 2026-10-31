"""Status vocabulary: component status, global status, cut consistency, coverage.

Design section 4.2/4.3 deliberately refuses a single boolean "is the number
good?". Four orthogonal questions are kept apart:

``calculable``        were the arithmetic inputs complete and supported?
``fresh``             is the market data young enough for the requested policy?
``cross_entity``      do the entity cuts provably belong to one logical instant?
``reconciled``        does P04 evidence cover this scope without a blocking finding?

A caller that wants one word gets :class:`GlobalStatus`, but it is derived from
the component statuses rather than replacing them.
"""

from __future__ import annotations

from enum import StrEnum, unique

__all__ = [
    "ComponentStatus",
    "GlobalStatus",
    "Consistency",
    "ExactnessClass",
    "FeeCoverage",
    "Component",
    "Presence",
    "Side",
    "derive_global_status",
]


@unique
class ComponentStatus(StrEnum):
    """Outcome of a single component (exposure / realized / unrealized)."""

    READY = "READY"
    STALE = "STALE"
    BLOCKED = "BLOCKED"
    UNSUPPORTED = "UNSUPPORTED"

    @property
    def has_values(self) -> bool:
        """Whether the component may carry monetary values.

        ``STALE`` keeps its numbers but brands them as not live; ``BLOCKED`` and
        ``UNSUPPORTED`` must carry no amounts at all, so that no consumer can
        render a fabricated zero (section 9.1).
        """
        return self in (ComponentStatus.READY, ComponentStatus.STALE)


@unique
class GlobalStatus(StrEnum):
    """Envelope status returned to the caller."""

    READY = "READY"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"
    UNSUPPORTED = "UNSUPPORTED"
    REJECTED = "REJECTED"


@unique
class Consistency(StrEnum):
    """How the entity cuts in a :class:`~risk.domain.facts.RiskCut` relate."""

    #: An upstream freeze proof binds every entity cut to one logical instant.
    COMPATIBLE = "COMPATIBLE"
    #: Each cut is individually valid, but nothing binds them together.
    INDEPENDENT = "INDEPENDENT"
    #: Cut identity itself is missing or unverifiable.
    UNPROVEN = "UNPROVEN"


@unique
class ExactnessClass(StrEnum):
    """The three result classes of design section 4.1."""

    COMPONENT_EXACT = "COMPONENT_EXACT"
    COMPOSITE_ALIGNED = "COMPOSITE_ALIGNED"
    PROVISIONAL = "PROVISIONAL"


@unique
class FeeCoverage(StrEnum):
    """Which charge categories the realized figures provably include."""

    #: Commission and swap only; no DEAL_FEE / non-trading charge coverage.
    NOT_AVAILABLE = "NOT_AVAILABLE"
    #: A fee field was present but its currency or posting policy is unmapped.
    UNKNOWN = "UNKNOWN"
    #: Every charge category of the approved policy is covered.
    COMPLETE = "COMPLETE"


@unique
class Component(StrEnum):
    EXPOSURE = "EXPOSURE"
    REALIZED = "REALIZED"
    UNREALIZED = "UNREALIZED"


@unique
class Presence(StrEnum):
    """P03 projection presence marker."""

    PRESENT = "PRESENT"
    ABSENT = "ABSENT"


@unique
class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"

    @property
    def opposite(self) -> "Side":
        return Side.SELL if self is Side.BUY else Side.BUY


def derive_global_status(
    component_statuses: dict[Component, ComponentStatus],
    *,
    strict_composite: bool,
) -> GlobalStatus:
    """Collapse per-component statuses into one envelope status.

    The asymmetry is intentional. Under ``strict_composite`` a caller has asked
    for a portfolio-level answer, so a single unusable component poisons the
    whole envelope. Without it the caller has opted in to partial data and gets
    ``DEGRADED`` -- which is *not* success, merely an honest partial.
    """
    if not component_statuses:
        return GlobalStatus.BLOCKED

    values = list(component_statuses.values())
    if all(s is ComponentStatus.READY for s in values):
        return GlobalStatus.READY

    if strict_composite:
        # Propagate the most specific cause rather than flattening to BLOCKED.
        if any(s is ComponentStatus.BLOCKED for s in values):
            return GlobalStatus.BLOCKED
        if any(s is ComponentStatus.UNSUPPORTED for s in values):
            return GlobalStatus.UNSUPPORTED
        return GlobalStatus.BLOCKED  # STALE under strict composite is not READY

    if any(s.has_values for s in values):
        return GlobalStatus.DEGRADED
    if all(s is ComponentStatus.UNSUPPORTED for s in values):
        return GlobalStatus.UNSUPPORTED
    return GlobalStatus.BLOCKED
