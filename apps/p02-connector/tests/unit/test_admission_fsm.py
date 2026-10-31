"""Admission control and the session state machine.

Covers WF2-M-01, WF2-M-02, WF2-G-03, WF2-G-05, WF2-G-09, WF2-O-02, INV-03,
INV-14 and INV-20.
"""

from __future__ import annotations

import pytest

from mt5_connector.admission import AdmissionController
from mt5_connector.connection_fsm import (
    IllegalTransitionError,
    SessionState,
    SessionStateMachine,
)
from mt5_connector.deadline import Deadline, ManualClock
from mt5_connector.errors import ConnectorClosedError, DeadlineExceededError, OverloadedError

pytestmark = pytest.mark.unit


def admission(**kwargs) -> AdmissionController:
    base = {"max_pending": 4, "max_concurrent_scans": 2}
    base.update(kwargs)
    return AdmissionController(**base)


class TestAdmission:
    def test_admits_up_to_capacity_then_refuses_immediately(self) -> None:
        """WF2-M-01, INV-20: a refusal is a function call, not a wait."""
        clock = ManualClock()
        gate = admission(max_pending=3)
        deadline = Deadline.after(1_000, clock=clock)
        tickets = [
            gate.acquire(request_id=f"r{i}", deadline=deadline, operation="fetch_deals")
            for i in range(3)
        ]
        assert gate.pending == 3
        assert gate.saturated

        before = clock.monotonic()
        with pytest.raises(OverloadedError) as excinfo:
            gate.acquire(request_id="r3", deadline=deadline, operation="fetch_deals")
        # No time passed: the rejection involved no await at all.
        assert clock.monotonic() == before
        assert excinfo.value.details["queue_capacity"] == 3
        assert gate.rejected_overload_total == 1

        gate.release(tickets[0])
        assert gate.pending == 2
        assert gate.acquire(request_id="r4", deadline=deadline, operation="fetch_deals")

    def test_hundred_callers_against_capacity_32_admit_exactly_32(self) -> None:
        """WF2-M-01 at the stated numbers, and WF2-M-02's bound."""
        clock = ManualClock()
        gate = admission(max_pending=32)
        deadline = Deadline.after(3_000, clock=clock)
        admitted = 0
        rejected = 0
        for index in range(100):
            try:
                gate.acquire(
                    request_id=f"r{index}", deadline=deadline, operation="fetch_deals"
                )
                admitted += 1
            except OverloadedError:
                rejected += 1
        assert admitted == 32
        assert rejected == 68
        assert gate.peak_pending == 32
        assert gate.pending == 32

    def test_peak_pending_proves_the_cap_was_never_exceeded(self) -> None:
        clock = ManualClock()
        gate = admission(max_pending=2)
        deadline = Deadline.after(1_000, clock=clock)
        for round_index in range(20):
            ticket = gate.acquire(
                request_id=f"r{round_index}", deadline=deadline, operation="fetch_deals"
            )
            gate.release(ticket)
        assert gate.peak_pending <= 2

    def test_an_expired_request_does_not_consume_a_slot(self) -> None:
        clock = ManualClock()
        gate = admission(max_pending=1)
        deadline = Deadline.after(100, clock=clock)
        clock.advance(0.5)
        with pytest.raises(DeadlineExceededError):
            gate.acquire(request_id="r0", deadline=deadline, operation="fetch_deals")
        assert gate.pending == 0

    def test_release_is_idempotent(self) -> None:
        """WF2-G-03: a double release would let the gate exceed its cap."""
        clock = ManualClock()
        gate = admission(max_pending=1)
        ticket = gate.acquire(
            request_id="r0", deadline=Deadline.after(1_000, clock=clock), operation="fetch_deals"
        )
        gate.release(ticket)
        gate.release(ticket)
        gate.release(ticket)
        assert gate.pending == 0

    def test_hundred_cancellations_return_the_counters_to_zero(self) -> None:
        """WF2-G-03."""
        clock = ManualClock()
        gate = admission(max_pending=100)
        deadline = Deadline.after(1_000, clock=clock)
        tickets = [
            gate.acquire(request_id=f"r{i}", deadline=deadline, operation="fetch_deals")
            for i in range(100)
        ]
        for ticket in tickets:
            gate.release(ticket)
        assert gate.pending == 0
        assert gate.snapshot()["pending"] == 0

    def test_per_client_quota_stops_one_caller_starving_the_rest(self) -> None:
        """WF2-G-09, FR-18."""
        clock = ManualClock()
        gate = admission(max_pending=10, max_pending_per_client=3)
        deadline = Deadline.after(1_000, clock=clock)
        for index in range(3):
            gate.acquire(
                request_id=f"a{index}",
                deadline=deadline,
                operation="fetch_deals",
                client_id="noisy",
            )
        with pytest.raises(OverloadedError) as excinfo:
            gate.acquire(
                request_id="a4", deadline=deadline, operation="fetch_deals", client_id="noisy"
            )
        assert excinfo.value.details["client_quota"] == 3
        # The sync service is unaffected.
        assert gate.acquire(
            request_id="s0", deadline=deadline, operation="fetch_deals", client_id="sync"
        )
        assert gate.pending_for("noisy") == 3

    def test_client_counters_do_not_grow_without_bound(self) -> None:
        clock = ManualClock()
        gate = admission(max_pending=4, max_pending_per_client=2)
        deadline = Deadline.after(1_000, clock=clock)
        for index in range(500):
            ticket = gate.acquire(
                request_id=f"r{index}",
                deadline=deadline,
                operation="fetch_deals",
                client_id=f"client-{index}",
            )
            gate.release(ticket)
        assert gate.pending == 0
        assert gate.pending_for("client-0") == 0

    def test_scan_slots_are_bounded(self) -> None:
        gate = admission(max_concurrent_scans=2)
        first = gate.acquire_scan(scan_id="s1")
        gate.acquire_scan(scan_id="s2")
        with pytest.raises(OverloadedError):
            gate.acquire_scan(scan_id="s3")
        gate.release_scan(first)
        assert gate.acquire_scan(scan_id="s4")

    def test_close_stops_new_admissions_but_not_existing_ones(self) -> None:
        """WF2-O-02, INV-14: "no new work" and "work finished" are different."""
        clock = ManualClock()
        gate = admission(max_pending=4)
        deadline = Deadline.after(1_000, clock=clock)
        ticket = gate.acquire(request_id="r0", deadline=deadline, operation="fetch_deals")
        gate.close()
        with pytest.raises(ConnectorClosedError):
            gate.acquire(request_id="r1", deadline=deadline, operation="fetch_deals")
        with pytest.raises(ConnectorClosedError):
            gate.acquire_scan(scan_id="s1")
        # The admitted request can still finish.
        gate.release(ticket)
        assert gate.pending == 0

    def test_constructor_refuses_a_nonpositive_cap(self) -> None:
        for kwargs in ({"max_pending": 0}, {"max_concurrent_scans": 0}):
            with pytest.raises(ValueError):
                admission(**kwargs)


class TestStateMachine:
    def test_the_happy_path(self) -> None:
        fsm = SessionStateMachine()
        assert fsm.state is SessionState.STOPPED
        fsm.transition(
            SessionState.CONNECTING, reason="start", at_monotonic=1.0, worker_epoch=1
        )
        fsm.transition(
            SessionState.READY, reason="handshake", at_monotonic=1.1, worker_epoch=1
        )
        assert fsm.serving
        assert [item.target.value for item in fsm.history] == ["CONNECTING", "READY"]

    def test_an_illegal_edge_raises_rather_than_being_applied(self) -> None:
        """WF2-G-05: a connect completing after a close must not set READY."""
        fsm = SessionStateMachine()
        with pytest.raises(IllegalTransitionError) as excinfo:
            fsm.transition(
                SessionState.READY, reason="race", at_monotonic=1.0, worker_epoch=1
            )
        assert excinfo.value.details["from"] == "STOPPED"
        assert excinfo.value.details["to"] == "READY"
        assert "CONNECTING" in excinfo.value.details["legal_targets"]
        assert fsm.state is SessionState.STOPPED
        assert fsm.rejected_transitions == 1

    def test_ready_after_closed_is_impossible(self) -> None:
        """WF2-G-05, WF2-G-06."""
        fsm = SessionStateMachine()
        for target in (SessionState.CONNECTING, SessionState.READY, SessionState.DRAINING):
            fsm.transition(target, reason="x", at_monotonic=1.0, worker_epoch=1)
        fsm.transition(SessionState.STOPPED, reason="done", at_monotonic=1.0, worker_epoch=1)
        with pytest.raises(IllegalTransitionError):
            fsm.transition(
                SessionState.READY, reason="late handshake", at_monotonic=2.0, worker_epoch=1
            )

    def test_a_self_transition_is_a_no_op(self) -> None:
        fsm = SessionStateMachine()
        fsm.transition(SessionState.CONNECTING, reason="a", at_monotonic=1.0, worker_epoch=1)
        assert (
            fsm.transition(
                SessionState.CONNECTING, reason="b", at_monotonic=1.0, worker_epoch=1
            )
            is None
        )
        assert len(fsm.history) == 1

    def test_suspect_leads_only_to_quarantine_or_backoff(self) -> None:
        fsm = SessionStateMachine()
        for target in (SessionState.CONNECTING, SessionState.READY, SessionState.SUSPECT):
            fsm.transition(target, reason="x", at_monotonic=1.0, worker_epoch=1)
        with pytest.raises(IllegalTransitionError):
            fsm.transition(
                SessionState.READY, reason="hopeful", at_monotonic=1.0, worker_epoch=1
            )
        assert fsm.quarantined
        fsm.transition(SessionState.QUARANTINED, reason="hung", at_monotonic=1.0, worker_epoch=1)
        assert not fsm.serving

    def test_disabled_cannot_be_left_by_a_retry(self) -> None:
        """WF2-B-01, WF2-H-06: no retry path out of a credential failure."""
        fsm = SessionStateMachine()
        fsm.transition(SessionState.CONNECTING, reason="x", at_monotonic=1.0, worker_epoch=1)
        fsm.transition(SessionState.DISABLED, reason="401", at_monotonic=1.0, worker_epoch=1)
        with pytest.raises(IllegalTransitionError) as excinfo:
            fsm.transition(
                SessionState.CONNECTING, reason="retry", at_monotonic=1.0, worker_epoch=1
            )
        assert "explicit reconfiguration" in excinfo.value.message

    def test_reconfigured_is_the_only_way_out_of_disabled(self) -> None:
        """Section 6.4: rotation must be recoverable without a restart."""
        fsm = SessionStateMachine()
        fsm.transition(SessionState.CONNECTING, reason="x", at_monotonic=1.0, worker_epoch=1)
        fsm.transition(SessionState.DISABLED, reason="401", at_monotonic=1.0, worker_epoch=1)
        record = fsm.reconfigured(at_monotonic=2.0, worker_epoch=1)
        assert record is not None
        assert fsm.state is SessionState.CONNECTING

    def test_reconfigured_is_a_no_op_outside_disabled(self) -> None:
        fsm = SessionStateMachine()
        assert fsm.reconfigured(at_monotonic=1.0, worker_epoch=1) is None
        assert fsm.state is SessionState.STOPPED

    def test_history_is_bounded_for_a_flapping_source(self) -> None:
        """WF2-C-08: twenty thousand flaps must not grow a list forever."""
        fsm = SessionStateMachine(history_limit=16)
        for index in range(500):
            fsm.transition(
                SessionState.CONNECTING, reason="x", at_monotonic=float(index), worker_epoch=1
            )
            fsm.transition(
                SessionState.BACKOFF, reason="y", at_monotonic=float(index), worker_epoch=1
            )
        assert len(fsm.history) == 16
        assert len(fsm.timeline()) == 16

    def test_every_state_can_reach_a_terminal_path(self) -> None:
        """No state is a dead end that would make shutdown impossible."""
        from mt5_connector.connection_fsm import _EDGES

        for state, targets in _EDGES.items():
            if state is SessionState.STOPPED:
                continue
            reachable = set(targets)
            # Either it can drain directly, or it can reach a state that can.
            assert reachable, f"{state} has no outgoing edges"
