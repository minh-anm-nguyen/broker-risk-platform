"""Money, recomputed independently and compared by value.

This module does the arithmetic itself. It does not import the risk engine, the
sync service's money helpers or anything else from the platform, because an
oracle that shares a rounding rule with the code it checks cannot detect a
wrong rounding rule.

Two decisions that look small and are not:

**Comparison is by value, not by text.** ``"0.00"``, ``"0E-12"`` and ``"0"``
are one amount written three ways, and PostgreSQL returns ``Decimal('0E-12')``
for a column a JSON payload rendered as ``"0.00"``. A string comparison here
would report a mismatch on every zero in the database - a false failure, which
costs more trust than a missed one, because people learn to ignore an oracle
that cries wolf.

**A float is refused, not converted.** If a value arrives as a float it has
already been through binary rounding and the exact amount is gone; accepting it
would mean checking the corruption against itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Iterable

__all__ = [
    "MoneyOracleError",
    "MoneyVerdict",
    "compare_money",
    "parse_amount",
    "sum_amounts",
]


class MoneyOracleError(ValueError):
    """An amount could not be read exactly, so no comparison is possible."""


def parse_amount(value: object, *, field: str) -> Decimal:
    """Read one amount exactly, or refuse.

    Accepts ``Decimal``, ``int`` and ``str``. Refuses ``float`` and ``bool``:
    the first has already lost the exact value, and the second is an ``int``
    that means something else entirely.
    """
    if isinstance(value, bool) or isinstance(value, float):
        raise MoneyOracleError(
            f"{field}: refusing a {type(value).__name__}; an exact amount cannot "
            "be recovered from one"
        )
    if isinstance(value, Decimal):
        parsed = value
    elif isinstance(value, int):
        parsed = Decimal(value)
    elif isinstance(value, str):
        try:
            parsed = Decimal(value)
        except InvalidOperation as exc:
            raise MoneyOracleError(f"{field}: {value!r} is not a decimal") from exc
    else:
        raise MoneyOracleError(f"{field}: cannot read {type(value).__name__} as money")
    if not parsed.is_finite():
        raise MoneyOracleError(f"{field}: {value!r} is not finite")
    return parsed


def sum_amounts(values: Iterable[object], *, field: str) -> Decimal:
    """Add a column of amounts with exact arithmetic."""
    total = Decimal(0)
    for index, value in enumerate(values):
        total += parse_amount(value, field=f"{field}[{index}]")
    return total


@dataclass(frozen=True, slots=True)
class MoneyVerdict:
    """The outcome of one monetary comparison."""

    label: str
    expected: Decimal
    actual: Decimal
    records: int

    @property
    def ok(self) -> bool:
        """Exact equality by value. No tolerance.

        A tolerance would be the wrong instrument: these are sums of exact
        decimals, not measurements, so any difference at all is a defect
        rather than noise.
        """
        return self.expected == self.actual

    @property
    def difference(self) -> Decimal:
        return self.actual - self.expected

    def describe(self) -> str:
        if self.ok:
            return (
                f"{self.label}: OK - {format(self.actual, 'f')} over "
                f"{self.records} records, recomputed independently"
            )
        return (
            f"{self.label}: WRONG MONEY - expected {format(self.expected, 'f')}, "
            f"got {format(self.actual, 'f')}, difference "
            f"{format(self.difference, 'f')} over {self.records} records"
        )


def compare_money(
    label: str,
    source_values: Iterable[object],
    target_values: Iterable[object],
) -> MoneyVerdict:
    """Sum both sides independently and compare by value.

    The source total is the expectation, because the source is the only thing
    in this comparison that is not under test.
    """
    source_list = list(source_values)
    target_list = list(target_values)
    return MoneyVerdict(
        label=label,
        expected=sum_amounts(source_list, field=f"{label}.source"),
        actual=sum_amounts(target_list, field=f"{label}.target"),
        records=len(target_list),
    )
