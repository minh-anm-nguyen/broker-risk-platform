"""The golden fixture catalogue from design section 14.3.

These are builders, not data files. A Deal fact has to hash to a stable value,
and the hash depends on normalisation rules that live in code -- so a JSON
fixture on disk would record a hash that silently becomes wrong the day a scale
or a field changes, and the test would then be asserting against a stale
expectation rather than against the contract.

What each fixture *is* is a named, reproducible situation, reused by the
contract tests, the chaos campaigns and the CLI demo so that all three are
talking about the same thing. G07-04 is the one to read if you only read one:
source sequences 5, 9 and 20 with nothing in between, because non-Deal source
events consume sequence numbers and the gaps are **not** missing Deals (design
section 4.2).
"""

from __future__ import annotations

from dataclasses import dataclass, replace

from .money import MoneyCurrency
from .writer.outbox_writer import DealFact

#: A fixed epoch so fixtures are byte-reproducible across runs.
EPOCH_A = "00000000-0000-4000-8000-000000000001"
EPOCH_B = "00000000-0000-4000-8000-000000000002"

#: A fixed instant, matching the design's worked example.
T0 = "2026-10-30T05:59:59.000000Z"


def deal(
    deal_id: str,
    *,
    account_id: str = "A-42",
    source_seq: int = 1013,
    realized: str = "12.30",
    commission: str = "-0.50",
    swap: str = "-0.10",
    currency: MoneyCurrency | None = None,
    executed_at: str = T0,
    symbol: str = "EURUSD",
    side: str = "BUY",
    entry_kind: str = "OUT",
    volume_lots: str = "0.10",
    price: str = "1.10500",
    position_id: str | None = "P-9",
) -> DealFact:
    """One Deal fact. Defaults match the envelope example in section 5.3."""
    return DealFact(
        deal_id=deal_id,
        account_id=account_id,
        symbol=symbol,
        side=side,
        entry_kind=entry_kind,
        volume_lots=volume_lots,
        price=price,
        realized_pnl_gross=realized,
        commission=commission,
        swap=swap,
        currency=currency or MoneyCurrency.proven("USD"),
        executed_at=executed_at,
        source_seq=source_seq,
        position_id=position_id,
    )


@dataclass(frozen=True, slots=True)
class Fixture:
    """A named fixture with the deals it contains and what it is for."""

    fixture_id: str
    purpose: str
    deals: tuple[DealFact, ...] = ()


#: G07-01: one valid USD Deal. Exactly one logical effect everywhere.
G07_01 = Fixture("G07-01", "one valid USD simulated Deal", (deal("D-1001"),))

#: G07-02: the same Deal twice, identical content. No second event.
G07_02 = Fixture(
    "G07-02",
    "same Deal delivered to P03 twice with an identical hash",
    (deal("D-1001"), deal("D-1001")),
)

#: G07-03: the same Deal id with a changed commission. A source conflict, never
#: a correction to publish.
G07_03 = Fixture(
    "G07-03",
    "same Deal id with a changed commission",
    (deal("D-1001"), replace(deal("D-1001"), commission="-0.75")),
)

#: G07-04: source sequences 5, 9, 20. The gaps are other source events, not
#: missing Deals -- do not synthesise 6..8.
G07_04 = Fixture(
    "G07-04",
    "non-contiguous source sequences 5, 9, 20",
    (
        deal("D-2001", source_seq=5),
        deal("D-2002", source_seq=9),
        deal("D-2003", source_seq=20),
    ),
)

#: G07-05: a new source sequence carrying an old execution time. A late-arriving
#: historical Deal, which is legitimate and must still be published once.
G07_05 = Fixture(
    "G07-05",
    "new source_seq with an old executed_at",
    (
        deal("D-3001", source_seq=100, executed_at="2026-10-29T10:00:00.000000Z"),
        deal("D-3002", source_seq=101, executed_at="2026-10-30T10:00:00.000000Z"),
    ),
)

#: G07-14: exact money strings and signs. Commission and swap stay negative.
G07_14 = Fixture(
    "G07-14",
    "commission -0.50, swap -0.10, exact strings and signs preserved",
    (deal("D-1401", realized="1234567890.12", commission="-0.50", swap="-0.10"),),
)

#: G07-17: one account id used by two different brokers. Isolation must hold in
#: the partition key and in the projection.
G07_17 = Fixture(
    "G07-17", "the same account id across two brokers", (deal("D-1701", account_id="A-42"),)
)


def unproven_currency_deal(deal_id: str = "D-9001", **overrides: object) -> DealFact:
    """A Deal whose calculation currency P05 has not established.

    The amounts are still exact and still hashed; what is missing is the unit.
    A consumer may count it and must not add it to a currency total, and P08
    must not aggregate it as USD just because the simulator uses USD.
    """
    return deal(deal_id, currency=MoneyCurrency.unproven(), **overrides)  # type: ignore[arg-type]


def account_burst(count: int, *, account_id: str = "A-42", first_seq: int = 1) -> tuple[DealFact, ...]:
    """``count`` Deals on one account, for head-lock and ordering tests (B07-04)."""
    return tuple(
        deal(f"D-{first_seq + index:06d}", account_id=account_id, source_seq=first_seq + index)
        for index in range(count)
    )


def spread_load(count: int, *, accounts: int = 10, first_seq: int = 1) -> tuple[DealFact, ...]:
    """``count`` Deals spread over ``accounts`` accounts, for throughput runs."""
    return tuple(
        deal(
            f"D-{first_seq + index:06d}",
            account_id=f"A-{index % accounts:03d}",
            source_seq=first_seq + index,
        )
        for index in range(count)
    )


def skewed_load(count: int, *, accounts: int = 10, hot_share: float = 0.8) -> tuple[DealFact, ...]:
    """A hot-account distribution, which is what a real broker looks like.

    Design section 6.3 point 7 warns that one aggregate head becomes a hot spot
    and that the throughput benchmark must include a skewed distribution. An
    even spread would measure the easy case and report it as the number.
    """
    if accounts < 2:
        raise ValueError("a skewed distribution needs at least two accounts")
    hot = int(count * hot_share)
    # The cold accounts deliberately exclude the hot one: if ``A-000`` also
    # received round-robin traffic, ``hot_share`` would not describe the share
    # of events on the hot key and a fairness assertion could not be written.
    return tuple(
        deal(
            f"D-{index:06d}",
            account_id="A-000" if index < hot else f"A-{1 + (index % (accounts - 1)):03d}",
            source_seq=index + 1,
        )
        for index in range(count)
    )


ALL_FIXTURES: tuple[Fixture, ...] = (
    G07_01,
    G07_02,
    G07_03,
    G07_04,
    G07_05,
    G07_14,
    G07_17,
)
