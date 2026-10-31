"""The WF5 case registry: 360 scenarios across 30 failure domains.

The matrix in the design document is a specification, and a specification has a
failure mode of its own: a table of cases nobody executed, reported as if it
were a table of results. This module exists to make that impossible to do by
accident.

Every case starts at ``NOT_EXECUTED``. A test claims a case by marking itself
with the case id; the evidence recorder writes the observed outcome; the report
tool prints coverage *against the full 360*, so an uncovered domain shows up as
a gap rather than as silence (WF5-290, section 13.7).

The group/priority structure is derived, not transcribed: each of the 30 domains
holds 12 consecutive cases, of which the first eight are P0, the next three P1
and the last P2. That is the structure the document specifies, and deriving it
means the registry cannot drift out of step with the totals (240 / 90 / 30).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum, unique
from typing import Final

__all__ = [
    "Priority",
    "CaseStatus",
    "MatrixCase",
    "DOMAINS",
    "CASES",
    "case",
    "cases_for_domain",
    "priority_totals",
    "TOTAL_CASES",
]

TOTAL_CASES: Final[int] = 360
CASES_PER_DOMAIN: Final[int] = 12


@unique
class Priority(StrEnum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"


@unique
class CaseStatus(StrEnum):
    """Legal values for a case's observed outcome (section 13.7).

    ``INVALID_TEST`` is the important one. A test whose fault injector never
    fired, or whose oracle reuses the engine it is checking, has not passed --
    it has failed to be a test, and counting it as a pass is worse than not
    running it (WF5-295, WF5-289).
    """

    NOT_EXECUTED = "NOT_EXECUTED"
    PASS = "PASS"
    FAIL = "FAIL"
    INVALID_TEST = "INVALID_TEST"
    SKIPPED_WITH_APPROVED_EXCEPTION = "SKIPPED_WITH_APPROVED_EXCEPTION"


#: The 30 failure domains, in the document's order. The index is what maps a
#: case number onto a domain, so this tuple's order is load-bearing.
DOMAINS: Final[tuple[tuple[str, str], ...]] = (
    ("A", "Upstream contract, schema handshake and provenance"),
    ("B", "Epoch, source reset, identity fencing and scope"),
    ("C", "Cross-entity cuts, MVCC and snapshot completeness"),
    ("D", "Deal types, eligibility and financial classification"),
    ("E", "Position state, direction, netting, hedging and lifecycle"),
    ("F", "Quote quality, bid/ask, time and market anomalies"),
    ("G", "Symbol specifications, units and calculation modes"),
    ("H", "Decimal precision, rounding, overflow and canonical money"),
    ("I", "FX conversion, currency precision and conversion graph"),
    ("J", "Realized P&L aggregation and accounting windows"),
    ("K", "Unrealized P&L and valuation correctness"),
    ("L", "Commission, swap, deal fees and sign conventions"),
    ("M", "Exposure units, gross/net, grouping and concentration"),
    ("N", "Time ranges, historical reports and late arrivals"),
    ("O", "Corrections, backfills, reconciliation and repair races"),
    ("P", "Staleness, quote reliability and volatile markets"),
    ("Q", "Security, tenant isolation, authorization and privacy"),
    ("R", "Concurrency, caching, races and request dedup"),
    ("S", "Database MVCC, read anomalies and consistency"),
    ("T", "PostgreSQL availability, errors and resource exhaustion"),
    ("U", "CPU, RAM, backpressure, cancellation and runtime"),
    ("V", "Arithmetic algorithm, invariants and metamorphic bugs"),
    ("W", "Versioning, rollout, schema migrations and rollback"),
    ("X", "Metrics, logging, audit, health semantics and evidence"),
    ("Y", "Test harness, goldens, mutation and false confidence"),
    ("Z", "Property-based, fuzz, adversarial and differential tests"),
    ("AA", "Throughput, load, stress, soak and scaling"),
    ("AB", "Lifecycle, crash recovery, backups and disaster runbooks"),
    ("AC", "Compound failure campaigns and correlated what-if"),
    ("AD", "P06 handoff, API semantics, consumer misuse and presentation"),
)


@dataclass(frozen=True, slots=True)
class MatrixCase:
    case_id: str
    number: int
    group: str
    domain: str
    priority: Priority

    @property
    def index_in_group(self) -> int:
        return (self.number - 1) % CASES_PER_DOMAIN


def _priority_for(index_in_group: int) -> Priority:
    if index_in_group < 8:
        return Priority.P0
    if index_in_group < 11:
        return Priority.P1
    return Priority.P2


def _build_registry() -> dict[str, MatrixCase]:
    registry: dict[str, MatrixCase] = {}
    for number in range(1, TOTAL_CASES + 1):
        group_index = (number - 1) // CASES_PER_DOMAIN
        group, domain = DOMAINS[group_index]
        case_id = f"WF5-{number:03d}"
        registry[case_id] = MatrixCase(
            case_id=case_id,
            number=number,
            group=group,
            domain=domain,
            priority=_priority_for((number - 1) % CASES_PER_DOMAIN),
        )
    return registry


CASES: Final[dict[str, MatrixCase]] = _build_registry()


def case(case_id: str) -> MatrixCase:
    """Look up a case, rejecting an id that is not in the matrix.

    A test that marks itself ``WF5-999`` is claiming coverage of something the
    specification does not contain, which is a typo at best.
    """
    try:
        return CASES[case_id]
    except KeyError:
        raise KeyError(f"{case_id!r} is not one of the {TOTAL_CASES} matrix cases") from None


def cases_for_domain(group: str) -> tuple[MatrixCase, ...]:
    return tuple(c for c in CASES.values() if c.group == group)


def priority_totals() -> dict[Priority, int]:
    totals = {Priority.P0: 0, Priority.P1: 0, Priority.P2: 0}
    for item in CASES.values():
        totals[item.priority] += 1
    return totals
