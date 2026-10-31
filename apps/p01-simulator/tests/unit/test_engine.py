"""The event state machine, against an in-memory reference view.

The reference view here is the second implementation of the ``StateView`` port
(the writer is the first). Having one lets the engine be tested without SQLite
in the way, and it is the same view the property tests drive.

The assertion that recurs throughout: **a rejected intent changes nothing** -
not the state, not the identifier counters, not the sequence. Several what-if
cases (WF-013, WF-015, WF-016, WF-018) are really all the same requirement.
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from simulator.domain import accounting as acc
from simulator.domain import engine as eng
from simulator.domain.models import (
    AdjustAccount,
    ClosePosition,
    ConflictError,
    CreateAccount,
    DealEntry,
    EventType,
    IncreasePosition,
    NotFoundError,
    OpenPosition,
    PositionStatus,
    Side,
    SimulatorError,
    UnsupportedOperationError,
    UpdateMark,
    ValidationError,
)

pytestmark = pytest.mark.unit

T0 = datetime(2026, 10, 30, 0, 0, 0, tzinfo=UTC)


class MemoryView:
    """In-memory ``StateView`` plus an apply loop, for engine-only testing."""

    def __init__(self) -> None:
        self.accounts: dict[str, object] = {}
        self.positions: dict[tuple[str, str], object] = {}
        self.marks: dict[str, object] = {}
        self.deals: list[object] = []
        self.events: list[tuple[int, EventType]] = []
        self.position_ordinal = 0
        self.deal_ordinal = 0
        self.seq = 0

    # -- StateView ---------------------------------------------------------
    def get_account(self, account_id: str):
        return self.accounts.get(account_id)

    def get_open_position(self, account_id: str, symbol: str):
        position = self.positions.get((account_id, symbol))
        if position is None or position.status is not PositionStatus.OPEN:
            return None
        return position

    def get_mark(self, symbol: str):
        return self.marks.get(symbol)

    def open_position_count(self) -> int:
        return sum(
            1 for item in self.positions.values() if item.status is PositionStatus.OPEN
        )

    def deal_count(self) -> int:
        return len(self.deals)

    def allocate_position_ordinal(self) -> int:
        self.position_ordinal += 1
        return self.position_ordinal

    def allocate_deal_ordinal(self) -> int:
        self.deal_ordinal += 1
        return self.deal_ordinal

    # -- apply -------------------------------------------------------------
    def apply(self, intent, *, limits: eng.EngineLimits | None = None):
        candidate_seq = self.seq + 1
        applied = eng.apply_intent(
            self, intent, seq=candidate_seq, virtual_time=T0, limits=limits
        )
        for mutation in applied.mutations:
            if isinstance(mutation, eng.AccountMutation):
                self.accounts[mutation.account.account_id] = mutation.account
            elif isinstance(mutation, eng.PositionMutation):
                key = (mutation.position.account_id, mutation.position.symbol)
                self.positions[key] = mutation.position
            else:
                self.marks[mutation.mark.symbol] = mutation.mark
        if applied.deal is not None:
            self.deals.append(applied.deal)
        self.seq = candidate_seq
        self.events.append((candidate_seq, applied.event_type))
        return applied

    def fingerprint(self) -> tuple:
        """Everything a rejected intent must leave untouched."""
        return (
            copy.deepcopy(self.accounts),
            copy.deepcopy(self.positions),
            copy.deepcopy(self.marks),
            len(self.deals),
            self.position_ordinal,
            self.deal_ordinal,
            self.seq,
            tuple(self.events),
        )


@pytest.fixture
def view() -> MemoryView:
    state = MemoryView()
    state.apply(CreateAccount("A-000001", "USD", Decimal("10000.00"), Decimal("0.00")))
    state.apply(UpdateMark("EURUSD", Decimal("1.10000"), Decimal("1.10010")))
    return state


def assert_rejected(view: MemoryView, intent, expected: type[SimulatorError]) -> SimulatorError:
    before = view.fingerprint()
    with pytest.raises(expected) as excinfo:
        view.apply(intent)
    assert view.fingerprint() == before, "a rejected intent must change nothing"
    return excinfo.value


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


def test_open_then_partial_then_full_close(view: MemoryView) -> None:
    """WF-017: volumes and status track through the whole lifecycle."""
    opened = view.apply(
        OpenPosition(
            "A-000001", "EURUSD", Side.BUY, Decimal("0.50"), Decimal("1.10000"),
            Decimal("-2.50"), Decimal("0.00"),
        )
    )
    assert opened.event_type is EventType.POSITION_OPENED
    assert opened.deal.entry is DealEntry.IN
    assert opened.deal.side is Side.BUY
    assert acc.format_decimal(opened.deal.realized_pnl_gross) == "0.00"
    assert acc.format_decimal(view.accounts["A-000001"].balance) == "9997.50"

    partial = view.apply(
        ClosePosition(
            "A-000001", "EURUSD", Decimal("0.20"), Decimal("1.10300"),
            Decimal("-1.00"), Decimal("-0.50"),
        )
    )
    assert partial.event_type is EventType.POSITION_PARTIALLY_CLOSED
    # Closing a long is a sell.
    assert partial.deal.entry is DealEntry.OUT
    assert partial.deal.side is Side.SELL
    assert acc.format_decimal(partial.deal.realized_pnl_gross) == "60.00"
    position = view.positions[("A-000001", "EURUSD")]
    assert position.status is PositionStatus.OPEN
    assert acc.format_decimal(position.volume_lots) == "0.30"
    # The remainder keeps its cost basis; a partial exit does not re-average it.
    assert acc.format_decimal(position.open_price) == "1.10000"

    full = view.apply(
        ClosePosition(
            "A-000001", "EURUSD", Decimal("0.30"), Decimal("1.10300"),
            Decimal("0.00"), Decimal("0.00"),
        )
    )
    assert full.event_type is EventType.POSITION_CLOSED
    closed = view.positions[("A-000001", "EURUSD")]
    assert closed.status is PositionStatus.CLOSED
    assert acc.format_decimal(closed.volume_lots) == "0.00"
    assert acc.format_decimal(view.accounts["A-000001"].balance) == "10146.00"


def test_balance_recomputes_from_the_deal_ledger(view: MemoryView) -> None:
    """INV-07: the projection must equal a replay of the deals."""
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("-5.00"), Decimal("0.00"))
    )
    view.apply(
        ClosePosition("A-000001", "EURUSD", Decimal("0.40"), Decimal("1.10500"), Decimal("-2.00"), Decimal("0.30"))
    )
    view.apply(AdjustAccount("A-000001", Decimal("-100.00"), "manual correction"))
    view.apply(
        ClosePosition("A-000001", "EURUSD", Decimal("0.60"), Decimal("1.09500"), Decimal("-3.00"), Decimal("-1.00"))
    )

    replayed = Decimal("10000.00")
    for deal in view.deals:
        replayed = acc.quantize_money(
            replayed + acc.balance_delta(deal.realized_pnl_gross, deal.commission, deal.swap)
        )
    replayed = acc.quantize_money(replayed + Decimal("-100.00"))
    assert replayed == view.accounts["A-000001"].balance


def test_increase_blends_open_price_and_adds_volume(view: MemoryView) -> None:
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    applied = view.apply(
        IncreasePosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.20000"), Decimal("0.00"), Decimal("0.00"))
    )
    assert applied.event_type is EventType.POSITION_INCREASED
    assert acc.format_decimal(applied.deal.realized_pnl_gross) == "0.00"
    position = view.positions[("A-000001", "EURUSD")]
    assert acc.format_decimal(position.open_price) == "1.15000"
    assert acc.format_decimal(position.volume_lots) == "2.00"


def test_price_tick_has_no_financial_side_effects(view: MemoryView) -> None:
    """WF-024: a quote update creates no deal and moves no balance."""
    before_balance = view.accounts["A-000001"].balance
    before_deals = len(view.deals)
    applied = view.apply(UpdateMark("EURUSD", Decimal("1.20000"), Decimal("1.20010")))
    assert applied.event_type is EventType.MARK_PRICE_UPDATED
    assert applied.deal is None
    assert len(view.deals) == before_deals
    assert view.accounts["A-000001"].balance == before_balance


def test_negative_balance_is_reported_not_floored(view: MemoryView) -> None:
    """WF-026: a loss larger than the balance is a fact, not an error."""
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("10.00"), Decimal("1.30000"), Decimal("0.00"), Decimal("0.00"))
    )
    view.apply(
        ClosePosition("A-000001", "EURUSD", Decimal("10.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    balance = view.accounts["A-000001"].balance
    assert balance < 0
    assert acc.format_decimal(balance) == "-190000.00"


def test_unrealized_is_none_without_a_mark() -> None:
    """A missing quote is unknown, not zero."""
    state = MemoryView()
    state.apply(CreateAccount("A-000001", "USD", Decimal("10000.00"), Decimal("0.00")))
    state.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    position = state.positions[("A-000001", "EURUSD")]
    assert eng.unrealized_for(position, None) is None


def test_unrealized_is_none_for_a_closed_position(view: MemoryView) -> None:
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    view.apply(
        ClosePosition("A-000001", "EURUSD", Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    position = view.positions[("A-000001", "EURUSD")]
    assert eng.unrealized_for(position, view.marks["EURUSD"]) is None


def test_reopening_after_a_full_close_creates_a_new_position_id(view: MemoryView) -> None:
    first = view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    view.apply(
        ClosePosition("A-000001", "EURUSD", Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    second = view.apply(
        OpenPosition("A-000001", "EURUSD", Side.SELL, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    assert first.deal.position_id != second.deal.position_id


# ---------------------------------------------------------------------------
# Rejections: state must be untouched
# ---------------------------------------------------------------------------


def test_unknown_account_is_rejected_whole(view: MemoryView) -> None:
    """WF-013: no orphan position and no orphan deal."""
    assert_rejected(
        view,
        OpenPosition("A-999999", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        NotFoundError,
    )


def test_closing_a_nonexistent_position_is_rejected(view: MemoryView) -> None:
    """WF-015: not a silent no-op success."""
    assert_rejected(
        view,
        ClosePosition("A-000001", "EURUSD", Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        NotFoundError,
    )


def test_closing_an_already_closed_position_is_rejected(view: MemoryView) -> None:
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    view.apply(
        ClosePosition("A-000001", "EURUSD", Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    assert_rejected(
        view,
        ClosePosition("A-000001", "EURUSD", Decimal("0.01"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        NotFoundError,
    )


def test_over_closing_changes_nothing(view: MemoryView) -> None:
    """WF-016: remaining volume and balance must be identical afterwards."""
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.50"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    error = assert_rejected(
        view,
        ClosePosition("A-000001", "EURUSD", Decimal("0.60"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        ValidationError,
    )
    assert error.code == "CLOSE_VOLUME_EXCEEDS_POSITION"
    assert error.details["remaining"] == "0.50"


def test_second_open_on_one_netting_key_is_a_conflict(view: MemoryView) -> None:
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.50"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    error = assert_rejected(
        view,
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.10"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        ConflictError,
    )
    assert error.code == "POSITION_ALREADY_OPEN"


def test_reversal_is_refused_rather_than_netted(view: MemoryView) -> None:
    """WF-028: silently netting would change what the position means."""
    view.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00"))
    )
    error = assert_rejected(
        view,
        IncreasePosition("A-000001", "EURUSD", Side.SELL, Decimal("1.00"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        UnsupportedOperationError,
    )
    assert "hint" in error.details


def test_duplicate_account_is_a_conflict(view: MemoryView) -> None:
    error = assert_rejected(
        view, CreateAccount("A-000001", "USD", Decimal("1.00"), Decimal("0.00")), ConflictError
    )
    assert error.code == "ACCOUNT_ALREADY_EXISTS"


def test_non_usd_account_is_refused(view: MemoryView) -> None:
    """WF-031: applying USD rules to a EUR account would be wrong, quietly."""
    assert_rejected(
        view, CreateAccount("A-000002", "EUR", Decimal("1.00"), Decimal("0.00")), UnsupportedOperationError
    )


def test_adjustment_requires_a_reason(view: MemoryView) -> None:
    """An unexplained balance change is indistinguishable from corruption."""
    assert_rejected(view, AdjustAccount("A-000001", Decimal("10.00"), "   "), ValidationError)


def test_negative_initial_balance_is_refused(view: MemoryView) -> None:
    assert_rejected(
        view, CreateAccount("A-000002", "USD", Decimal("-1.00"), Decimal("0.00")), ValidationError
    )


@pytest.mark.parametrize("volume", ["0", "-0.50", "0.001", "1000"])
def test_invalid_open_volumes_are_rejected(view: MemoryView, volume: str) -> None:
    assert_rejected(
        view,
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal(volume), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        ValidationError,
    )


@pytest.mark.parametrize("price", ["0", "-1.10000", "1.100005"])
def test_invalid_open_prices_are_rejected(view: MemoryView, price: str) -> None:
    assert_rejected(
        view,
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.50"), Decimal(price), Decimal("0.00"), Decimal("0.00")),
        ValidationError,
    )


def test_open_position_limit_is_enforced() -> None:
    state = MemoryView()
    state.apply(CreateAccount("A-000001", "USD", Decimal("10000.00"), Decimal("0.00")))
    limits = eng.EngineLimits(max_open_positions=1)
    state.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.10"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        limits=limits,
    )
    before = state.fingerprint()
    with pytest.raises(ValidationError) as excinfo:
        state.apply(
            OpenPosition("A-000001", "GBPUSD", Side.BUY, Decimal("0.10"), Decimal("1.27000"), Decimal("0.00"), Decimal("0.00")),
            limits=limits,
        )
    assert excinfo.value.code == "OPEN_POSITION_LIMIT_REACHED"
    assert state.fingerprint() == before


def test_deal_budget_is_enforced() -> None:
    state = MemoryView()
    state.apply(CreateAccount("A-000001", "USD", Decimal("10000.00"), Decimal("0.00")))
    limits = eng.EngineLimits(max_deals=1)
    state.apply(
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.10"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
        limits=limits,
    )
    with pytest.raises(ValidationError) as excinfo:
        state.apply(
            ClosePosition("A-000001", "EURUSD", Decimal("0.10"), Decimal("1.10000"), Decimal("0.00"), Decimal("0.00")),
            limits=limits,
        )
    assert excinfo.value.code == "DEAL_LIMIT_REACHED"


def test_fx_symbol_mark_is_refused(view: MemoryView) -> None:
    assert_rejected(
        view, UpdateMark("USDJPY", Decimal("150.000"), Decimal("150.010")), UnsupportedOperationError
    )


def test_inverted_quote_is_refused(view: MemoryView) -> None:
    assert_rejected(
        view, UpdateMark("EURUSD", Decimal("1.10200"), Decimal("1.10100")), ValidationError
    )


def test_event_id_is_derived_from_seq_not_random() -> None:
    """A UUID here would make two replays of one seed produce different ids."""
    assert eng.event_id_for(1) == "E-000000000001"
    assert eng.event_id_for(141854) == "E-000000141854"
    assert eng.event_id_for(7) == eng.event_id_for(7)


def test_describe_intent_round_trips_every_kind(view: MemoryView) -> None:
    intents = [
        CreateAccount("A-000002", "USD", Decimal("1.00"), Decimal("0.00")),
        AdjustAccount("A-000001", Decimal("1.00"), "why"),
        UpdateMark("EURUSD", Decimal("1.10000"), Decimal("1.10010")),
        OpenPosition("A-000001", "EURUSD", Side.BUY, Decimal("0.10"), Decimal("1.1"), Decimal("0.00"), Decimal("0.00")),
        IncreasePosition("A-000001", "EURUSD", Side.BUY, Decimal("0.10"), Decimal("1.1"), Decimal("0.00"), Decimal("0.00")),
        ClosePosition("A-000001", "EURUSD", Decimal("0.10"), Decimal("1.1"), Decimal("0.00"), Decimal("0.00")),
    ]
    kinds = {eng.describe_intent(intent)["kind"] for intent in intents}
    assert kinds == {
        "CREATE_ACCOUNT",
        "ADJUST_ACCOUNT",
        "UPDATE_MARK",
        "OPEN_POSITION",
        "INCREASE_POSITION",
        "CLOSE_POSITION",
    }
