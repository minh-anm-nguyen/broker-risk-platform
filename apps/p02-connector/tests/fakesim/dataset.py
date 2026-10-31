"""A deterministic synthetic dataset with the shape Project 01 publishes.

Deterministic because every oracle in the suite compares against it: the same
seed must produce the same accounts, the same deal ids and the same
``source_seq`` values on every run and on every machine, or a failure cannot
be told apart from a reseed (INV-17).

The one property that is easy to miss and important to model: **``source_seq``
has gaps**. It counts domain *events*, and most events are not deals - a mark
update, an account adjustment and a position open all consume a sequence
number without producing a deal. A generator that emitted 1, 2, 3... would
make a connector that alarms on gaps look correct, and that connector would
then alarm continuously against the real source (WF2-K-01, INV-19, F04).
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

__all__ = ["Dataset", "DatasetSpec", "build_dataset", "SYMBOLS"]

SYMBOLS: tuple[str, ...] = ("EURUSD", "GBPUSD", "XAUUSD")

_BASE_PRICE: dict[str, Decimal] = {
    "EURUSD": Decimal("1.08500"),
    "GBPUSD": Decimal("1.27000"),
    "XAUUSD": Decimal("2015.00"),
}
_PRICE_STEP: dict[str, Decimal] = {
    "EURUSD": Decimal("0.00001"),
    "GBPUSD": Decimal("0.00001"),
    "XAUUSD": Decimal("0.01"),
}
_START = datetime(2026, 10, 1, 0, 0, 0, tzinfo=UTC)


@dataclass(frozen=True, slots=True)
class DatasetSpec:
    seed: int = 42
    accounts: int = 10
    deals: int = 50
    positions: int = 8
    run_id: str = "run_0001"
    generation: int = 1
    scenario_name: str = "baseline"


@dataclass(slots=True)
class Dataset:
    """Rows in the exact wire form the data plane returns."""

    spec: DatasetSpec
    accounts: list[dict[str, Any]] = field(default_factory=list)
    positions: list[dict[str, Any]] = field(default_factory=list)
    deals: list[dict[str, Any]] = field(default_factory=list)
    committed_seq: int = 0
    #: ``source_seq`` values consumed by events that produced no deal. Kept so
    #: a test can assert the gaps it relies on actually exist.
    non_deal_sequences: list[int] = field(default_factory=list)

    @property
    def run_id(self) -> str:
        return self.spec.run_id

    @property
    def generation(self) -> int:
        return self.spec.generation

    def deal_ids(self) -> list[str]:
        return [row["deal_id"] for row in self.deals]

    def deals_after(self, after_seq: int | None) -> list[dict[str, Any]]:
        """The exclusive ``after_seq`` filter, as the contract defines it."""
        if after_seq is None:
            return list(self.deals)
        return [row for row in self.deals if row["source_seq"] > after_seq]

    def with_generation(self, *, run_id: str | None = None, generation: int) -> "Dataset":
        """A new generation of this dataset, as a reset would produce."""
        spec = DatasetSpec(
            seed=self.spec.seed + 1,
            accounts=self.spec.accounts,
            deals=self.spec.deals,
            positions=self.spec.positions,
            run_id=run_id or self.spec.run_id,
            generation=generation,
            scenario_name=self.spec.scenario_name,
        )
        return build_dataset(spec)


def _money(value: Decimal, places: int = 2) -> str:
    quantum = Decimal(1).scaleb(-places)
    return str(value.quantize(quantum))


def build_dataset(spec: DatasetSpec) -> Dataset:
    rng = random.Random(spec.seed)
    data = Dataset(spec=spec)
    sequence = 0

    for index in range(1, spec.accounts + 1):
        # Account creation consumes a sequence number and produces no deal.
        sequence += 1
        data.non_deal_sequences.append(sequence)
        balance = Decimal("10000.00") + Decimal(rng.randrange(0, 500_000)) / Decimal(100)
        credit = Decimal("0.00") if index % 4 else Decimal("250.00")
        data.accounts.append(
            {
                "account_id": f"acct-{index:04d}",
                "currency": "USD",
                "balance": _money(balance),
                "credit": _money(credit),
            }
        )

    for index in range(1, spec.positions + 1):
        sequence += 1
        data.non_deal_sequences.append(sequence)
        symbol = SYMBOLS[index % len(SYMBOLS)]
        account = data.accounts[index % max(1, len(data.accounts))]["account_id"]
        step = _PRICE_STEP[symbol]
        price = _BASE_PRICE[symbol] + step * rng.randrange(-500, 500)
        volume = Decimal(rng.randrange(1, 40)) / Decimal(100)
        # Every third position is CLOSED, so the OPEN-with-zero-volume
        # invariant has non-trivial data to hold against.
        status = "CLOSED" if index % 3 == 0 else "OPEN"
        data.positions.append(
            {
                "position_id": f"pos-{index:05d}",
                "account_id": account,
                "symbol": symbol,
                "side": "BUY" if index % 2 else "SELL",
                "volume_lots": _money(volume if status == "OPEN" else Decimal("0.00")),
                "open_price": _money(price, 5 if symbol != "XAUUSD" else 2),
                "status": status,
                "unrealized_pnl": _money(
                    Decimal(rng.randrange(-5_000, 5_000)) / Decimal(100)
                )
                if status == "OPEN"
                else None,
                "mark_market_time": "2026-10-01T12:00:00.000Z",
                "mark_stale": False,
            }
        )
    # A CLOSED position legitimately has zero volume; an OPEN one must not.
    for row in data.positions:
        if row["status"] == "OPEN" and Decimal(row["volume_lots"]) == 0:
            row["volume_lots"] = "0.10"

    for index in range(1, spec.deals + 1):
        # One or two non-deal events before each deal: this is what puts the
        # gaps in ``source_seq``.
        gap = 1 if index % 3 else 2
        for _ in range(gap):
            sequence += 1
            data.non_deal_sequences.append(sequence)
        sequence += 1
        symbol = SYMBOLS[index % len(SYMBOLS)]
        account = data.accounts[index % max(1, len(data.accounts))]["account_id"]
        position = data.positions[index % max(1, len(data.positions))]["position_id"]
        step = _PRICE_STEP[symbol]
        price = _BASE_PRICE[symbol] + step * rng.randrange(-800, 800)
        volume = Decimal(rng.randrange(1, 50)) / Decimal(100)
        entry = "IN" if index % 2 else "OUT"
        realized = (
            Decimal("0.00")
            if entry == "IN"
            else Decimal(rng.randrange(-20_000, 20_000)) / Decimal(100)
        )
        data.deals.append(
            {
                "deal_id": f"deal-{index:06d}",
                "source_seq": sequence,
                "account_id": account,
                "position_id": position,
                "symbol": symbol,
                "entry": entry,
                "side": "BUY" if index % 2 else "SELL",
                "volume_lots": _money(volume),
                "price": _money(price, 5 if symbol != "XAUUSD" else 2),
                "realized_pnl_gross": _money(realized),
                # Commission is negative: a fee is money leaving the account.
                # A connector that "corrected" the sign would turn every cost
                # into revenue (WF2-L-03).
                "commission": _money(-(Decimal(rng.randrange(10, 400)) / Decimal(100))),
                "swap": _money(Decimal(rng.randrange(-120, 60)) / Decimal(100)),
                "executed_at": (_START + timedelta(seconds=index * 17)).strftime(
                    "%Y-%m-%dT%H:%M:%S.000Z"
                ),
            }
        )

    # Deals are served in ascending (source_seq, deal_id) order, which is what
    # a keyset cursor scan depends on.
    data.deals.sort(key=lambda row: (row["source_seq"], row["deal_id"]))
    data.accounts.sort(key=lambda row: row["account_id"])
    data.positions.sort(key=lambda row: row["position_id"])
    data.committed_seq = sequence
    return data
