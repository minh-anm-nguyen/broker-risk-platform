"""The session state machine (design section 6).

The legal edges are data, and every transition goes through
:meth:`SessionStateMachine.transition`, which refuses an edge that is not in
the table. That matters for a specific class of bug: a connect that completes
after a close, or a health probe that succeeds after a quarantine, would
otherwise set ``READY`` on a session that no longer exists. Making the edge
illegal turns that race into a loud error instead of a service reporting
itself healthy while holding a dead session (WF2-G-05, WF2-G-06).

Two states deserve naming because they are easy to conflate:

* ``SUSPECT`` - a call has run past its hard limit. The session *might* still
  be fine; what is certain is that the connector no longer knows.
* ``QUARANTINED`` - the call is still running and cannot be stopped. New work
  is refused. In thread mode this is where the session stays, because there
  is no safe way to kill the thread; only the process worker can leave this
  state by force (section 5.4).

``READY`` means "this connector can serve reads under the contract it
negotiated". It does **not** mean the data is fresh or globally consistent - a
live TCP connection implies neither, and a consumer still owns its own
reconciliation (section 6).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Final

from .errors import ConnectorError

__all__ = ["SessionState", "Transition", "SessionStateMachine", "IllegalTransitionError"]


class SessionState(StrEnum):
    STOPPED = "STOPPED"
    CONNECTING = "CONNECTING"
    READY = "READY"
    DEGRADED = "DEGRADED"
    BACKOFF = "BACKOFF"
    SUSPECT = "SUSPECT"
    QUARANTINED = "QUARANTINED"
    RESYNC_REQUIRED = "RESYNC_REQUIRED"
    DISABLED = "DISABLED"
    DRAINING = "DRAINING"


class IllegalTransitionError(ConnectorError):
    code = "ILLEGAL_STATE_TRANSITION"
    category = "lifecycle"
    safe_action = (
        "this is a connector bug, not a source failure: a transition was attempted "
        "that the state machine does not permit"
    )


#: The edge table, transcribed from the design's state diagram.
#:
#: One edge is **added** to that diagram: ``DISABLED -> CONNECTING``. The
#: diagram makes ``DISABLED`` reachable only from ``CONNECTING`` and exitable
#: only to ``DRAINING``, which would make a credential rotation
#: unrecoverable without a restart - and section 6.4 requires recovery after
#: rotation. The edge is guarded: it is reachable only through
#: :meth:`SessionStateMachine.reconfigured`, never from a retry, so a bad
#: credential still cannot produce a retry loop (WF2-B-09, WF2-H-06).
_EDGES: Final[dict[SessionState, frozenset[SessionState]]] = {
    SessionState.STOPPED: frozenset({SessionState.CONNECTING}),
    SessionState.CONNECTING: frozenset(
        {
            SessionState.READY,
            SessionState.BACKOFF,
            SessionState.DISABLED,
            SessionState.DRAINING,
        }
    ),
    SessionState.BACKOFF: frozenset({SessionState.CONNECTING, SessionState.DRAINING}),
    # ``READY -> DISABLED`` and ``DEGRADED -> DISABLED`` are the second
    # addition to the design's diagram. There, DISABLED is reachable only from
    # CONNECTING, which covers a credential that was wrong at handshake but
    # not one revoked while the session was serving - and that session would
    # then stay READY while every read failed 401, with readiness reporting
    # healthy (WF2-B-09, WF2-B-01).
    SessionState.READY: frozenset(
        {
            SessionState.DEGRADED,
            SessionState.SUSPECT,
            SessionState.RESYNC_REQUIRED,
            SessionState.DISABLED,
            SessionState.DRAINING,
        }
    ),
    SessionState.DEGRADED: frozenset(
        {
            SessionState.READY,
            SessionState.SUSPECT,
            SessionState.RESYNC_REQUIRED,
            SessionState.DISABLED,
            SessionState.DRAINING,
        }
    ),
    SessionState.SUSPECT: frozenset({SessionState.QUARANTINED, SessionState.BACKOFF}),
    SessionState.QUARANTINED: frozenset({SessionState.BACKOFF, SessionState.DRAINING}),
    SessionState.RESYNC_REQUIRED: frozenset({SessionState.READY, SessionState.DRAINING}),
    SessionState.DISABLED: frozenset({SessionState.CONNECTING, SessionState.DRAINING}),
    SessionState.DRAINING: frozenset({SessionState.STOPPED}),
}

#: States in which the connector will admit a read.
SERVING_STATES: Final[frozenset[SessionState]] = frozenset(
    {SessionState.READY, SessionState.DEGRADED}
)

#: States from which a reconnect attempt makes sense without operator action.
RECOVERABLE_STATES: Final[frozenset[SessionState]] = frozenset(
    {SessionState.STOPPED, SessionState.BACKOFF}
)

#: Transitions only :meth:`SessionStateMachine.reconfigured` may perform.
_GUARDED_EDGES: Final[frozenset[tuple[SessionState, SessionState]]] = frozenset(
    {(SessionState.DISABLED, SessionState.CONNECTING)}
)


@dataclass(frozen=True, slots=True)
class Transition:
    """One recorded edge, for the timeline a test asserts on."""

    source: SessionState
    target: SessionState
    reason: str
    at_monotonic: float
    worker_epoch: int


class SessionStateMachine:
    """The state of one source's session, plus its transition history."""

    def __init__(self, *, history_limit: int = 256) -> None:
        self._state = SessionState.STOPPED
        self._history: list[Transition] = []
        self._history_limit = history_limit
        self.rejected_transitions: int = 0

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def history(self) -> tuple[Transition, ...]:
        return tuple(self._history)

    @property
    def serving(self) -> bool:
        return self._state in SERVING_STATES

    @property
    def quarantined(self) -> bool:
        return self._state in (SessionState.SUSPECT, SessionState.QUARANTINED)

    def can(self, target: SessionState) -> bool:
        if target is self._state:
            return True
        if (self._state, target) in _GUARDED_EDGES:
            return False
        return target in _EDGES[self._state]

    def transition(
        self,
        target: SessionState,
        *,
        reason: str,
        at_monotonic: float,
        worker_epoch: int,
        _guarded: bool = False,
    ) -> Transition | None:
        """Move to ``target``, or raise if that edge does not exist.

        A transition to the current state is a no-op and returns ``None``,
        which keeps idempotent signalling - two consecutive resync signals,
        say - from needing a special case at every call site.
        """
        if target is self._state:
            return None
        guarded = (self._state, target) in _GUARDED_EDGES
        if guarded and not _guarded:
            self.rejected_transitions += 1
            raise IllegalTransitionError(
                f"the edge {self._state.value} -> {target.value} is reachable only "
                "through an explicit reconfiguration",
                details={"from": self._state.value, "to": target.value, "reason": reason},
            )
        if target not in _EDGES[self._state]:
            self.rejected_transitions += 1
            raise IllegalTransitionError(
                f"{self._state.value} -> {target.value} is not a legal session transition",
                details={
                    "from": self._state.value,
                    "to": target.value,
                    "reason": reason,
                    "legal_targets": sorted(item.value for item in _EDGES[self._state]),
                },
            )
        record = Transition(
            source=self._state,
            target=target,
            reason=reason,
            at_monotonic=at_monotonic,
            worker_epoch=worker_epoch,
        )
        self._state = target
        self._history.append(record)
        if len(self._history) > self._history_limit:
            # Bounded: a flapping source must not grow this list without
            # limit. The oldest entries go first.
            del self._history[: len(self._history) - self._history_limit]
        return record

    def reconfigured(self, *, at_monotonic: float, worker_epoch: int) -> Transition | None:
        """Leave ``DISABLED`` because the configuration or secret changed."""
        if self._state is not SessionState.DISABLED:
            return None
        return self.transition(
            SessionState.CONNECTING,
            reason="configuration or credential replaced",
            at_monotonic=at_monotonic,
            worker_epoch=worker_epoch,
            _guarded=True,
        )

    def timeline(self) -> list[dict[str, object]]:
        return [
            {
                "from": item.source.value,
                "to": item.target.value,
                "reason": item.reason,
                "at_monotonic": item.at_monotonic,
                "worker_epoch": item.worker_epoch,
            }
            for item in self._history
        ]
