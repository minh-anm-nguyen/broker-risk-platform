"""Test-only fault injection registry.

Design doc section 11.1. Injection is configuration of the *test harness*, not
a public surface: the registry refuses to arm anything unless
``settings.faults_enabled`` is true, and no router reads from it.

Barriers exist so race tests do not rely on sleeps. A test arms a barrier at a
named point, the request blocks there, the test mutates upstream state, then
releases the barrier.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Final

__all__ = ["FaultPoint", "FaultRegistry", "FaultTriggered", "Barrier"]


class FaultPoint:
    """Named injection points. Keep this list and the harness recipes in step."""

    AUTH_JWT_INVALID_SIGNATURE: Final = "auth.jwt_invalid_signature"
    AUTH_POLICY_REVISION_CHANGE: Final = "auth.policy_revision_change"
    DB_POOL_EXHAUSTED: Final = "db.pool_exhausted"
    DB_FAIL_BEFORE_SNAPSHOT: Final = "db.fail_before_snapshot"
    DB_FAIL_AFTER_SNAPSHOT: Final = "db.fail_after_snapshot"
    DB_FAIL_FINAL_CHECK: Final = "db.fail_final_check"
    DB_STATEMENT_TIMEOUT: Final = "db.statement_timeout"
    ENGINE_RETURN_BAD_CURRENCY: Final = "p05.return_bad_currency"
    ENGINE_RETURN_FLOAT: Final = "p05.return_float"
    ENGINE_DROP_INPUT_DIGEST: Final = "p05.drop_input_digest"
    ENGINE_DROP_CUT: Final = "p05.drop_cut"
    ENGINE_RETURN_NAN: Final = "p05.return_nan"
    ENGINE_RAISE: Final = "p05.raise"
    ENGINE_DELAY: Final = "p05.delay_ms"
    ENGINE_HANG: Final = "p05.hang"
    ENGINE_BURN_CPU: Final = "p05.burn_cpu_ms"
    RECON_REVISION_BUMP: Final = "p04.recon_revision_bump"
    SOURCE_ROTATE_GENERATION: Final = "source.rotate_generation"
    AUDIT_FAIL_BEFORE_COMMIT: Final = "audit.fail_before_commit"
    AUDIT_FAIL_AFTER_COMMIT: Final = "audit.fail_after_commit"
    AUDIT_LOSE_ACK: Final = "audit.lose_commit_ack"
    AUDIT_SINK_DOWN: Final = "audit.sink_down"
    WORKER_KILL_AFTER_COMPUTE: Final = "worker.kill_after_compute"
    EDGE_DISCONNECT_AFTER_HEADERS: Final = "edge.disconnect_after_headers"
    RESPONSE_CORRUPT_DECIMAL_TYPE: Final = "response.corrupt_decimal_type"
    CLOCK_FREEZE: Final = "clock.freeze"

    # Barrier points used by the race schedules in design doc section 16.
    BARRIER_AFTER_CUT_CAPTURE: Final = "barrier.after_cut_capture"
    BARRIER_AFTER_COMPUTE: Final = "barrier.after_p05_compute"
    BARRIER_BEFORE_FINAL_CHECK: Final = "barrier.before_final_check"
    BARRIER_BEFORE_AUDIT: Final = "barrier.before_audit"


class FaultTriggered(RuntimeError):
    """Raised by the registry when an armed raise-fault fires."""

    def __init__(self, point: str, payload: Any = None) -> None:
        super().__init__(f"fault fired at {point}")
        self.point = point
        self.payload = payload


@dataclass(slots=True)
class Barrier:
    """A one-shot gate a test can hold and then release."""

    event: asyncio.Event = field(default_factory=asyncio.Event)
    arrivals: int = 0

    def release(self) -> None:
        self.event.set()


@dataclass(slots=True)
class FaultRegistry:
    """Armed faults for one app instance."""

    enabled: bool = False
    armed: dict[str, Any] = field(default_factory=dict)
    fired: list[str] = field(default_factory=list)
    barriers: dict[str, Barrier] = field(default_factory=dict)

    # ------------------------------------------------------------------ arming

    def arm(self, point: str, payload: Any = True) -> None:
        """Arm a fault. Refused unless the instance enabled fault injection."""
        if not self.enabled:
            raise RuntimeError("fault injection is disabled on this instance")
        self.armed[point] = payload

    def arm_once(self, point: str, payload: Any = True) -> None:
        """Arm a fault that disarms itself the first time it fires."""
        self.arm(point, {"once": True, "payload": payload})

    def disarm(self, point: str) -> None:
        self.armed.pop(point, None)

    def clear(self) -> None:
        self.armed.clear()
        self.fired.clear()
        self.barriers.clear()

    def arm_barrier(self, point: str) -> Barrier:
        """Arm a barrier and return its handle so a test can release it."""
        if not self.enabled:
            raise RuntimeError("fault injection is disabled on this instance")
        barrier = Barrier()
        self.barriers[point] = barrier
        return barrier

    # ----------------------------------------------------------------- probing

    def active(self, point: str) -> Any:
        """Return the payload for an armed fault, or ``None``.

        Records the firing so a test can assert the injection actually
        happened rather than inferring it from the outcome.
        """
        if not self.enabled:
            return None
        payload = self.armed.get(point)
        if payload is None:
            return None
        self.fired.append(point)
        if isinstance(payload, dict) and payload.get("once"):
            self.armed.pop(point, None)
            return payload.get("payload", True)
        return payload

    def raise_if_armed(self, point: str) -> None:
        payload = self.active(point)
        if payload is not None:
            raise FaultTriggered(point, payload)

    async def wait_if_barrier(self, point: str, *, timeout: float = 10.0) -> None:
        """Block at a barrier point if one is armed."""
        if not self.enabled:
            return
        barrier = self.barriers.get(point)
        if barrier is None:
            return
        barrier.arrivals += 1
        self.fired.append(point)
        try:
            await asyncio.wait_for(barrier.event.wait(), timeout=timeout)
        except TimeoutError as exc:  # pragma: no cover - a hung test, not a feature
            raise RuntimeError(f"barrier {point} was never released") from exc

    async def delay_if_armed(self, point: str) -> None:
        """Sleep for an armed delay, expressed in milliseconds."""
        payload = self.active(point)
        if payload is None:
            return
        millis = 0.0 if isinstance(payload, bool) else float(payload)
        if millis > 0:
            await asyncio.sleep(millis / 1000.0)

    def has_fired(self, point: str) -> bool:
        return point in self.fired
