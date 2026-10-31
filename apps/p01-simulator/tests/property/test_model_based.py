"""Model-based testing: the engine against an independent reference model.

The reference model below is written from the rules in SYSTEM_DESIGN section
3.3/3.4, not from the engine. That is the whole point - an oracle derived from
the implementation proves only that the implementation agrees with itself.

Two properties are checked after every single operation, which is what makes a
failure easy to localize:

* the engine and the model agree on the resulting state, and
* a rejected operation leaves the engine exactly as it was.

The stateful machine then drives long random sequences through both.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, assume, given, settings as hypothesis_settings
from hypothesis import strategies as strategies_module
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)

from simulator.domain import accounting as acc
from simulator.domain import engine as eng
from simulator.domain.models import (
    ClosePosition,
    CreateAccount,
    IncreasePosition,
    OpenPosition,
    PositionStatus,
    Side,
    SimulatorError,
    UpdateMark,
    symbol_spec,
)
from tests.unit.test_engine import MemoryView

pytestmark = pytest.mark.property

T0 = datetime(2026, 10, 30, 0, 0, 0, tzinfo=UTC)
SYMBOLS = ("EURUSD", "GBPUSD", "AUDUSD")
CONTRACT_SIZE = Decimal("100000")

st = strategies_module

volumes = st.integers(min_value=1, max_value=100).map(lambda cents: Decimal(cents) / 100)
prices = st.integers(min_value=50_000, max_value=200_000).map(
    lambda ticks: (Decimal(ticks) / Decimal(100_000)).quantize(Decimal("0.00001"))
)
fees = st.integers(min_value=-500, max_value=200).map(lambda cents: Decimal(cents) / 100)
sides = st.sampled_from([Side.BUY, Side.SELL])
symbols = st.sampled_from(SYMBOLS)


# ---------------------------------------------------------------------------
# Reference model, written from the spec
# ---------------------------------------------------------------------------


@dataclass
class ModelPosition:
    side: Side
    volume: Decimal
    open_price: Decimal


@dataclass
class ReferenceBrokerModel:
    """Minimal, independent restatement of the domain rules."""

    balances: dict[str, Decimal] = field(default_factory=dict)
    positions: dict[tuple[str, str], ModelPosition] = field(default_factory=dict)
    quotes: dict[str, tuple[Decimal, Decimal]] = field(default_factory=dict)
    realized: Decimal = Decimal("0.00")

    def create_account(self, account_id: str, balance: Decimal) -> None:
        self.balances[account_id] = balance

    def update_mark(self, symbol: str, bid: Decimal, ask: Decimal) -> None:
        self.quotes[symbol] = (bid, ask)

    def open(
        self, account: str, symbol: str, side: Side, volume: Decimal,
        price: Decimal, commission: Decimal, swap: Decimal,
    ) -> None:
        self.positions[(account, symbol)] = ModelPosition(side, volume, price)
        self._book(account, Decimal("0.00"), commission, swap)

    def increase(
        self, account: str, symbol: str, volume: Decimal, price: Decimal,
        commission: Decimal, swap: Decimal,
    ) -> None:
        position = self.positions[(account, symbol)]
        total = position.volume + volume
        blended = (position.open_price * position.volume + price * volume) / total
        position.open_price = blended.quantize(Decimal("0.00001"), rounding=acc.ROUNDING)
        position.volume = total
        self._book(account, Decimal("0.00"), commission, swap)

    def close(
        self, account: str, symbol: str, volume: Decimal, price: Decimal,
        commission: Decimal, swap: Decimal,
    ) -> Decimal:
        position = self.positions[(account, symbol)]
        difference = (
            price - position.open_price
            if position.side is Side.BUY
            else position.open_price - price
        )
        gross = (difference * volume * CONTRACT_SIZE).quantize(
            Decimal("0.01"), rounding=acc.ROUNDING
        )
        position.volume -= volume
        if position.volume == 0:
            del self.positions[(account, symbol)]
        self._book(account, gross, commission, swap)
        self.realized += gross
        return gross

    def _book(
        self, account: str, gross: Decimal, commission: Decimal, swap: Decimal
    ) -> None:
        delta = (gross + commission + swap).quantize(Decimal("0.01"), rounding=acc.ROUNDING)
        self.balances[account] = (self.balances[account] + delta).quantize(
            Decimal("0.01"), rounding=acc.ROUNDING
        )

    def unrealized(self, account: str, symbol: str) -> Decimal | None:
        position = self.positions.get((account, symbol))
        if position is None:
            return None
        quote = self.quotes.get(symbol)
        if quote is None:
            return None
        bid, ask = quote
        close_price = bid if position.side is Side.BUY else ask
        difference = (
            close_price - position.open_price
            if position.side is Side.BUY
            else position.open_price - close_price
        )
        return (difference * position.volume * CONTRACT_SIZE).quantize(
            Decimal("0.01"), rounding=acc.ROUNDING
        )


# ---------------------------------------------------------------------------
# Single-operation agreement
# ---------------------------------------------------------------------------


def build_pair() -> tuple[MemoryView, ReferenceBrokerModel]:
    view = MemoryView()
    model = ReferenceBrokerModel()
    view.apply(CreateAccount("A-000001", "USD", Decimal("100000.00"), Decimal("0.00")))
    model.create_account("A-000001", Decimal("100000.00"))
    return view, model


@given(
    side=sides,
    symbol=symbols,
    open_volume=volumes,
    open_price=prices,
    close_price=prices,
    commission=fees,
    swap=fees,
)
@hypothesis_settings(max_examples=250, deadline=None)
def test_open_then_full_close_agrees_with_the_model(
    side: Side,
    symbol: str,
    open_volume: Decimal,
    open_price: Decimal,
    close_price: Decimal,
    commission: Decimal,
    swap: Decimal,
) -> None:
    view, model = build_pair()

    view.apply(
        OpenPosition("A-000001", symbol, side, open_volume, open_price, commission, swap)
    )
    model.open("A-000001", symbol, side, open_volume, open_price, commission, swap)
    assert view.accounts["A-000001"].balance == model.balances["A-000001"]

    applied = view.apply(
        ClosePosition("A-000001", symbol, open_volume, close_price, commission, swap)
    )
    expected_gross = model.close(
        "A-000001", symbol, open_volume, close_price, commission, swap
    )
    assert applied.deal.realized_pnl_gross == expected_gross
    assert view.accounts["A-000001"].balance == model.balances["A-000001"]
    assert view.positions[("A-000001", symbol)].status is PositionStatus.CLOSED


@given(
    side=sides,
    first_volume=volumes,
    first_price=prices,
    second_volume=volumes,
    second_price=prices,
)
@hypothesis_settings(max_examples=250, deadline=None)
def test_increase_blends_the_open_price_like_the_model(
    side: Side,
    first_volume: Decimal,
    first_price: Decimal,
    second_volume: Decimal,
    second_price: Decimal,
) -> None:
    assume(first_volume + second_volume <= Decimal("100"))
    view, model = build_pair()
    zero = Decimal("0.00")

    view.apply(OpenPosition("A-000001", "EURUSD", side, first_volume, first_price, zero, zero))
    model.open("A-000001", "EURUSD", side, first_volume, first_price, zero, zero)

    view.apply(
        IncreasePosition("A-000001", "EURUSD", side, second_volume, second_price, zero, zero)
    )
    model.increase("A-000001", "EURUSD", second_volume, second_price, zero, zero)

    position = view.positions[("A-000001", "EURUSD")]
    expected = model.positions[("A-000001", "EURUSD")]
    assert position.volume_lots == expected.volume
    assert position.open_price == expected.open_price


@given(
    side=sides,
    volume=volumes,
    open_price=prices,
    bid=prices,
)
@hypothesis_settings(max_examples=200, deadline=None)
def test_unrealized_agrees_with_the_model(
    side: Side, volume: Decimal, open_price: Decimal, bid: Decimal
) -> None:
    view, model = build_pair()
    spec = symbol_spec("EURUSD")
    ask = acc.quantize_price(bid + Decimal("0.00010"), spec)
    zero = Decimal("0.00")

    view.apply(UpdateMark("EURUSD", bid, ask))
    model.update_mark("EURUSD", bid, ask)
    view.apply(OpenPosition("A-000001", "EURUSD", side, volume, open_price, zero, zero))
    model.open("A-000001", "EURUSD", side, volume, open_price, zero, zero)

    position = view.positions[("A-000001", "EURUSD")]
    assert eng.unrealized_for(position, view.marks["EURUSD"]) == model.unrealized(
        "A-000001", "EURUSD"
    )


@given(
    volume=volumes,
    over=st.integers(min_value=1, max_value=50).map(lambda cents: Decimal(cents) / 100),
    price=prices,
)
@hypothesis_settings(max_examples=200, deadline=None)
def test_over_closing_is_always_rejected_and_changes_nothing(
    volume: Decimal, over: Decimal, price: Decimal
) -> None:
    """WF-016 as a property rather than a single example."""
    view, _ = build_pair()
    zero = Decimal("0.00")
    view.apply(OpenPosition("A-000001", "EURUSD", Side.BUY, volume, price, zero, zero))
    before = view.fingerprint()
    with pytest.raises(SimulatorError):
        view.apply(ClosePosition("A-000001", "EURUSD", volume + over, price, zero, zero))
    assert view.fingerprint() == before


# ---------------------------------------------------------------------------
# Stateful machine
# ---------------------------------------------------------------------------


class BrokerStateMachine(RuleBasedStateMachine):
    """Drives long random sequences through the engine and the model together.

    Every rule asserts agreement immediately, and the invariants re-derive the
    balance from the deal ledger after every step, so a divergence is caught
    at the operation that caused it rather than many steps later.
    """

    def __init__(self) -> None:
        super().__init__()
        self.view = MemoryView()
        self.model = ReferenceBrokerModel()
        self.accounts: list[str] = []
        self.rejections = 0
        self.applied = 0

    @initialize()
    def setup(self) -> None:
        for ordinal in range(1, 4):
            account_id = f"A-{ordinal:06d}"
            self.view.apply(
                CreateAccount(account_id, "USD", Decimal("100000.00"), Decimal("0.00"))
            )
            self.model.create_account(account_id, Decimal("100000.00"))
            self.accounts.append(account_id)
        for symbol in SYMBOLS:
            spec = symbol_spec(symbol)
            bid = acc.quantize_price(Decimal("1.10000"), spec)
            ask = acc.quantize_price(bid + Decimal("0.00010"), spec)
            self.view.apply(UpdateMark(symbol, bid, ask))
            self.model.update_mark(symbol, bid, ask)

    def _attempt(self, intent) -> bool:
        """Apply to the engine; on rejection assert nothing moved."""
        before = self.view.fingerprint()
        try:
            self.view.apply(intent)
        except SimulatorError:
            self.rejections += 1
            assert self.view.fingerprint() == before
            return False
        self.applied += 1
        return True

    @rule(account=st.integers(0, 2), symbol=symbols, side=sides, volume=volumes, price=prices, commission=fees)
    def open_position(
        self, account: int, symbol: str, side: Side, volume: Decimal,
        price: Decimal, commission: Decimal,
    ) -> None:
        account_id = self.accounts[account]
        key = (account_id, symbol)
        already_open = key in self.model.positions
        if self._attempt(
            OpenPosition(account_id, symbol, side, volume, price, commission, Decimal("0.00"))
        ):
            assert not already_open
            self.model.open(
                account_id, symbol, side, volume, price, commission, Decimal("0.00")
            )
        else:
            # The only legitimate reason to refuse a well-formed open.
            assert already_open

    @rule(account=st.integers(0, 2), symbol=symbols, volume=volumes, price=prices, commission=fees, swap=fees)
    def close_position(
        self, account: int, symbol: str, volume: Decimal, price: Decimal,
        commission: Decimal, swap: Decimal,
    ) -> None:
        account_id = self.accounts[account]
        position = self.model.positions.get((account_id, symbol))
        closable = position is not None and volume <= position.volume
        if self._attempt(
            ClosePosition(account_id, symbol, volume, price, commission, swap)
        ):
            assert closable
            self.model.close(account_id, symbol, volume, price, commission, swap)
        else:
            assert not closable

    @rule(account=st.integers(0, 2), symbol=symbols, volume=volumes, price=prices)
    def increase_position(
        self, account: int, symbol: str, volume: Decimal, price: Decimal
    ) -> None:
        account_id = self.accounts[account]
        position = self.model.positions.get((account_id, symbol))
        spec = symbol_spec(symbol)
        can_increase = position is not None and position.volume + volume <= spec.max_lot
        side = position.side if position is not None else Side.BUY
        if self._attempt(
            IncreasePosition(
                account_id, symbol, side, volume, price, Decimal("0.00"), Decimal("0.00")
            )
        ):
            assert can_increase
            self.model.increase(
                account_id, symbol, volume, price, Decimal("0.00"), Decimal("0.00")
            )
        else:
            assert not can_increase

    @rule(symbol=symbols, bid=prices)
    def tick(self, symbol: str, bid: Decimal) -> None:
        spec = symbol_spec(symbol)
        ask = acc.quantize_price(bid + Decimal("0.00010"), spec)
        if self._attempt(UpdateMark(symbol, bid, ask)):
            self.model.update_mark(symbol, bid, ask)

    @invariant()
    def balances_agree(self) -> None:
        for account_id in self.accounts:
            assert (
                self.view.accounts[account_id].balance == self.model.balances[account_id]
            ), account_id

    @invariant()
    def balance_recomputes_from_the_ledger(self) -> None:
        """INV-07 continuously, not just at the end."""
        replayed = {account_id: Decimal("100000.00") for account_id in self.accounts}
        for deal in self.view.deals:
            replayed[deal.account_id] = acc.quantize_money(
                replayed[deal.account_id]
                + acc.balance_delta(deal.realized_pnl_gross, deal.commission, deal.swap)
            )
        for account_id in self.accounts:
            assert replayed[account_id] == self.view.accounts[account_id].balance

    @invariant()
    def volumes_never_go_negative(self) -> None:
        """INV-05/INV-06."""
        for position in self.view.positions.values():
            assert position.volume_lots >= 0
            if position.status is PositionStatus.OPEN:
                assert position.volume_lots > 0
            else:
                assert position.volume_lots == 0

    @invariant()
    def closed_volume_never_exceeds_opened(self) -> None:
        opened: dict[str, Decimal] = {}
        closed: dict[str, Decimal] = {}
        for deal in self.view.deals:
            target = opened if deal.entry.value == "IN" else closed
            target[deal.position_id] = (
                target.get(deal.position_id, Decimal(0)) + deal.volume_lots
            )
        for position_id, total_closed in closed.items():
            assert total_closed <= opened[position_id]

    @invariant()
    def one_open_position_per_netting_key(self) -> None:
        open_keys = [
            key
            for key, position in self.view.positions.items()
            if position.status is PositionStatus.OPEN
        ]
        assert len(open_keys) == len(set(open_keys))

    @invariant()
    @precondition(lambda self: self.view.deals)
    def every_deal_is_internally_consistent(self) -> None:
        for deal in self.view.deals:
            assert deal.volume_lots > 0
            assert deal.price > 0
            acc.ensure_finite(deal.realized_pnl_gross, field="gross")
            acc.ensure_finite(deal.commission, field="commission")
            if deal.entry.value == "IN":
                assert deal.realized_pnl_gross == 0


TestBrokerStateMachine = BrokerStateMachine.TestCase
TestBrokerStateMachine.settings = hypothesis_settings(
    max_examples=40,
    stateful_step_count=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
