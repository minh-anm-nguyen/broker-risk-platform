"""Property-based tests (design section 14.13).

Covers the generated half of WF2-L-01, WF2-L-02, WF2-L-10, WF2-G-05,
WF2-H-09, WF2-H-10, WF2-I-02, WF2-I-10, WF2-M-08 and INV-03, INV-10, INV-11.

Property testing is here for a specific reason: the example-based suite checks
the failures somebody thought of. These check *invariants* over inputs nobody
chose, which is the only kind of test that can surprise its author.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone, UTC
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, assume, given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st

from mt5_connector.connection_fsm import (
    IllegalTransitionError,
    SessionState,
    SessionStateMachine,
)
from mt5_connector.deadline import Deadline, ManualClock
from mt5_connector.dto import build_deal
from mt5_connector.ports import Entity
from mt5_connector.retry import parse_retry_after
from mt5_connector.scalars import ScalarError, format_money, parse_money, parse_timestamp
from mt5_connector.scan import ScanContext
from mt5_connector.validation import cursor_fingerprint

pytestmark = [pytest.mark.property]

PROFILE = hyp_settings(
    max_examples=150,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)

decimal_text = st.from_regex(r"\A-?(0|[1-9][0-9]{0,20})(\.[0-9]{1,10})?\Z", fullmatch=True)
money_text = st.from_regex(r"\A-?(0|[1-9][0-9]{0,6})\.[0-9]{2}\Z", fullmatch=True)
positive_text = st.from_regex(r"\A[1-9][0-9]{0,4}\.[0-9]{2}\Z", fullmatch=True)


class TestMoneyProperties:
    @given(decimal_text)
    @PROFILE
    def test_valid_decimal_text_round_trips(self, text: str) -> None:
        value = parse_money(text, field="amount")
        assert value == Decimal(text)
        # Canonical formatting is idempotent and reparses to the same value,
        # which is what lets a digest over amounts be stable.
        formatted = format_money(value)
        assert parse_money(formatted, field="amount") == value
        assert format_money(parse_money(formatted, field="amount")) == formatted

    @given(st.floats(allow_nan=True, allow_infinity=True))
    @PROFILE
    def test_no_float_is_ever_accepted(self, value: float) -> None:
        """WF2-L-01: there is no float this parser will coerce."""
        with pytest.raises(ScalarError):
            parse_money(value, field="amount")

    @given(st.text(max_size=80))
    @PROFILE
    def test_arbitrary_text_either_parses_exactly_or_is_refused(self, text: str) -> None:
        """INV-10: never a partial, rounded or repaired decode."""
        try:
            value = parse_money(text, field="amount")
        except ScalarError:
            return
        assert value == Decimal(text.strip())
        assert value.is_finite()

    @given(st.integers(min_value=-(10**30), max_value=10**30))
    @PROFILE
    def test_integers_decode_without_loss(self, value: int) -> None:
        assert parse_money(value, field="amount") == Decimal(value)

    @given(st.integers(min_value=35, max_value=200))
    @PROFILE
    def test_digit_count_is_always_bounded(self, digits: int) -> None:
        """WF2-L-10: an arbitrarily long literal is an allocation attack."""
        with pytest.raises(ScalarError):
            parse_money("1" * digits, field="amount")


class TestTimestampProperties:
    @given(
        st.datetimes(
            min_value=datetime(1991, 1, 1), max_value=datetime(2099, 12, 31)
        ),
        st.integers(min_value=-13, max_value=13),
    )
    @PROFILE
    def test_any_offset_normalises_to_the_same_instant(self, naive, offset_hours) -> None:
        """WF2-A-10: normalise the representation, never the instant."""
        aware = naive.replace(
            tzinfo=timezone(timedelta(hours=offset_hours)), microsecond=0
        )
        parsed = parse_timestamp(aware.isoformat(), field="executed_at")
        assert parsed == aware.astimezone(UTC)
        assert parsed.utcoffset() == timedelta(0)

    @given(st.text(max_size=40))
    @PROFILE
    def test_arbitrary_text_is_either_a_utc_instant_or_refused(self, text: str) -> None:
        try:
            parsed = parse_timestamp(text, field="executed_at")
        except ScalarError:
            return
        assert parsed.utcoffset() == timedelta(0)


class TestRetryAfterProperties:
    @given(st.text(max_size=40), st.floats(min_value=0.0, max_value=600.0))
    @PROFILE
    def test_the_result_is_always_inside_the_cap_or_none(self, text, cap) -> None:
        """WF2-H-09, WF2-D-02: never negative, never above the cap."""
        value = parse_retry_after(text, cap_s=cap)
        if value is None:
            return
        assert 0.0 <= value <= cap

    @given(st.integers(min_value=-(10**9), max_value=10**9))
    @PROFILE
    def test_any_integer_header_is_clamped(self, seconds: int) -> None:
        value = parse_retry_after(str(seconds), cap_s=2.0)
        assert value is not None
        assert 0.0 <= value <= 2.0


class TestDeadlineProperties:
    @given(
        st.integers(min_value=0, max_value=600_000),
        st.lists(st.floats(min_value=0.0, max_value=10.0), max_size=20),
    )
    @PROFILE
    def test_remaining_is_non_increasing_and_never_negative(
        self, total_ms, advances
    ) -> None:
        """WF2-H-10: no sequence of advances produces an immortal request."""
        clock = ManualClock()
        deadline = Deadline.after(total_ms, clock=clock)
        previous = deadline.remaining_s
        for step in advances:
            clock.advance(step)
            current = deadline.remaining_s
            assert current >= 0.0
            assert current <= previous + 1e-9
            previous = current

    @given(
        st.integers(min_value=1, max_value=60_000),
        st.integers(min_value=0, max_value=120_000),
    )
    @PROFILE
    def test_a_cap_can_only_move_the_instant_earlier(self, total_ms, cap_ms) -> None:
        clock = ManualClock()
        deadline = Deadline.after(total_ms, clock=clock)
        assert deadline.capped(cap_ms).at <= deadline.at + 1e-9


class TestStateMachineProperties:
    @given(st.lists(st.sampled_from(list(SessionState)), min_size=1, max_size=40))
    @PROFILE
    def test_no_sequence_of_attempts_reaches_an_illegal_state(self, targets) -> None:
        """WF2-G-05: an illegal edge is refused, never applied."""
        fsm = SessionStateMachine()
        for target in targets:
            before = fsm.state
            try:
                fsm.transition(
                    target, reason="generated", at_monotonic=0.0, worker_epoch=1
                )
            except IllegalTransitionError:
                assert fsm.state is before
            else:
                assert fsm.state is target

    @given(st.lists(st.sampled_from(list(SessionState)), min_size=1, max_size=40))
    @PROFILE
    def test_history_records_exactly_the_applied_transitions(self, targets) -> None:
        fsm = SessionStateMachine()
        applied = 0
        for target in targets:
            before = fsm.state
            try:
                record = fsm.transition(
                    target, reason="generated", at_monotonic=0.0, worker_epoch=1
                )
            except IllegalTransitionError:
                continue
            if record is not None:
                applied += 1
                assert record.source is before
                assert record.target is target
        assert len(fsm.history) == applied

    @given(st.lists(st.sampled_from(list(SessionState)), min_size=1, max_size=30))
    @PROFILE
    def test_serving_and_quarantined_never_disagree_with_the_state(self, targets) -> None:
        fsm = SessionStateMachine()
        for target in targets:
            try:
                fsm.transition(
                    target, reason="generated", at_monotonic=0.0, worker_epoch=1
                )
            except IllegalTransitionError:
                pass
            if fsm.serving:
                assert fsm.state in (SessionState.READY, SessionState.DEGRADED)
            if fsm.quarantined:
                assert fsm.state in (SessionState.SUSPECT, SessionState.QUARANTINED)


def make_context(*, max_tracked_keys: int = 1_000) -> ScanContext:
    return ScanContext(
        scan_id="scan-prop",
        entity=Entity.DEALS,
        filter_digest="d",
        requested_after_seq=None,
        max_pages=2_500,
        max_tracked_keys=max_tracked_keys,
    )


class TestScanContextProperties:
    @given(st.lists(st.text(min_size=1, max_size=24), min_size=1, max_size=60))
    @PROFILE
    def test_a_repeated_cursor_is_always_detected(self, cursors) -> None:
        """WF2-I-02: a loop terminates however the cursors are ordered."""
        context = make_context()
        seen: set[str] = set()
        for cursor in cursors:
            digest = cursor_fingerprint(cursor)
            accepted = context.note_cursor(cursor)
            assert accepted is (digest not in seen)
            seen.add(digest)

    @given(st.lists(st.text(min_size=1, max_size=12), min_size=1, max_size=80))
    @PROFILE
    def test_the_first_duplicate_key_is_the_one_reported(self, keys) -> None:
        context = make_context()
        duplicate = context.track_keys(list(keys))
        expected = None
        seen: set[str] = set()
        for key in keys:
            if key in seen:
                expected = key
                break
            seen.add(key)
        assert duplicate == expected

    @given(st.integers(min_value=1, max_value=50))
    @PROFILE
    def test_exceeding_the_key_budget_drops_the_claim_not_the_bound(self, budget) -> None:
        """WF2-I-10, WF2-M-08: stop claiming what you stopped checking."""
        context = make_context(max_tracked_keys=budget)
        context.track_keys([f"k{index}" for index in range(budget * 4)])
        assert context.global_uniqueness_verified is False
        assert len(context.seen_keys) == 0
        assert context.track_keys(["k0", "k0"]) is None
        assert context.summary()["global_uniqueness_verified"] is False

    @given(st.lists(st.text(min_size=1, max_size=8), min_size=1, max_size=200))
    @PROFILE
    def test_tracked_keys_never_exceed_the_budget(self, keys) -> None:
        """INV-03: the duplicate set is a bound, not a hope."""
        context = make_context(max_tracked_keys=16)
        context.track_keys(list(keys))
        assert len(context.seen_keys) <= 17


class TestDealProperties:
    @given(
        source_seq=st.integers(min_value=1, max_value=10**9),
        commission=money_text,
        swap=money_text,
        realized=money_text,
        volume=positive_text,
        price=positive_text,
    )
    @PROFILE
    def test_signs_and_scales_survive_construction(
        self, source_seq, commission, swap, realized, volume, price
    ) -> None:
        """WF2-L-03: no amount is rounded, re-signed or rescaled."""
        built = build_deal(
            {
                "deal_id": "deal-000001",
                "source_seq": source_seq,
                "account_id": "acct-0001",
                "position_id": "pos-00001",
                "symbol": "EURUSD",
                "entry": "IN",
                "side": "BUY",
                "volume_lots": volume,
                "price": price,
                "realized_pnl_gross": realized,
                "commission": commission,
                "swap": swap,
                "executed_at": "2026-10-01T00:00:00.000Z",
            },
            expect_source_seq=True,
        )
        assert built.commission == Decimal(commission)
        assert built.swap == Decimal(swap)
        assert built.realized_pnl_gross == Decimal(realized)
        assert built.commission.as_tuple().exponent == -2
        assert built.source_seq == source_seq

    @given(st.dictionaries(st.text(max_size=12), st.text(max_size=12), max_size=8))
    @PROFILE
    def test_an_arbitrary_object_never_becomes_a_deal(self, payload) -> None:
        """INV-10: a generated payload cannot accidentally validate."""
        assume("deal_id" not in payload)
        with pytest.raises(ScalarError):
            build_deal(payload, expect_source_seq=True)
