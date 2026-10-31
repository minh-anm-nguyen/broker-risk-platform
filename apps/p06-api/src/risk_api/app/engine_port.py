"""Adapter to the Project 05 risk engine.

Project 06 calls the engine; it never recomputes money. This module is the
whole of that boundary:

* it runs the computation in a bounded CPU worker under the remaining budget;
* it counts invocations, so a test can assert an unauthorized request caused
  **zero** engine calls;
* it translates engine exceptions into the documented public codes;
* in test builds it can return deliberately malformed results, which is how
  the ``502 INVALID_RISK_RESULT`` path gets exercised for real.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Protocol

from risk_engine import (
    RiskEngine,
    RiskEngineError,
    RiskInputBundle,
    RiskRequest,
    RiskResult,
)

from ..admission.budgets import Deadline
from ..contracts.problem_details import ApiProblem, public_code_for_reasons
from ..execution.cpu_worker import CpuWorkerPool
from ..faults import FaultPoint, FaultRegistry

__all__ = ["RiskEnginePort", "BoundedRiskEngineAdapter", "EngineCallStats"]


@dataclass(slots=True)
class EngineCallStats:
    calls: int = 0
    failures: int = 0
    blocked: int = 0

    def snapshot(self) -> dict[str, int]:
        return {"calls": self.calls, "failures": self.failures, "blocked": self.blocked}


class RiskEnginePort(Protocol):
    """What the application layer is allowed to ask of the engine."""

    async def compute(
        self,
        request: RiskRequest,
        bundle: RiskInputBundle,
        *,
        deadline: Deadline,
        compute_budget_seconds: float,
        request_id: str,
    ) -> RiskResult: ...

    @property
    def stats(self) -> EngineCallStats: ...


class BoundedRiskEngineAdapter:
    """Run the pure engine off the event loop, under a deadline."""

    def __init__(
        self,
        *,
        engine: RiskEngine,
        workers: CpuWorkerPool,
        faults: FaultRegistry | None = None,
    ) -> None:
        self._engine = engine
        self._workers = workers
        self._faults = faults or FaultRegistry()
        self._stats = EngineCallStats()

    @property
    def stats(self) -> EngineCallStats:
        return self._stats

    async def compute(
        self,
        request: RiskRequest,
        bundle: RiskInputBundle,
        *,
        deadline: Deadline,
        compute_budget_seconds: float,
        request_id: str,
    ) -> RiskResult:
        await self._faults.delay_if_armed(FaultPoint.ENGINE_DELAY)
        if self._faults.active(FaultPoint.ENGINE_RAISE) is not None:
            self._stats.calls += 1
            self._stats.failures += 1
            raise ApiProblem(
                "DB_UNAVAILABLE",
                "the risk engine could not complete the computation",
                reason_codes=("DB_UNAVAILABLE",),
            )

        burn_ms = self._faults.active(FaultPoint.ENGINE_BURN_CPU)
        hang = self._faults.active(FaultPoint.ENGINE_HANG) is not None

        budget = deadline.slice(compute_budget_seconds)
        self._stats.calls += 1

        def _run() -> RiskResult:
            if hang:
                _burn_cpu_seconds(30.0)
            elif burn_ms is not None and not isinstance(burn_ms, bool):
                _burn_cpu_seconds(float(burn_ms) / 1000.0)
            return self._engine.compute(request, bundle)

        try:
            result = await self._workers.run(
                _run, timeout_seconds=budget, request_id=request_id
            )
        except ApiProblem:
            self._stats.failures += 1
            raise
        except RiskEngineError as exc:
            self._stats.blocked += 1
            raise ApiProblem(
                public_code_for_reasons(exc.reason_codes),
                "the risk engine refused the request",
                reason_codes=exc.reason_codes,
            ) from exc

        return self._apply_result_faults(result)

    # ------------------------------------------------------------ test faults

    def _apply_result_faults(self, result: RiskResult) -> RiskResult:
        """Deliberately corrupt a result so the output guard can be proven."""
        if self._faults.active(FaultPoint.ENGINE_RETURN_BAD_CURRENCY) is not None:
            return replace(result, currency="XXX")
        if self._faults.active(FaultPoint.ENGINE_DROP_INPUT_DIGEST) is not None:
            return replace(result, input_digest="")
        if self._faults.active(FaultPoint.ENGINE_DROP_CUT) is not None:
            return replace(result, cuts=replace(result.cuts, position_scan_id=None))
        if self._faults.active(FaultPoint.ENGINE_RETURN_NAN) is not None:
            return _with_realized_amount(result, Decimal("NaN"))
        if self._faults.active(FaultPoint.ENGINE_RETURN_FLOAT) is not None:
            return _with_realized_amount(result, 56.0)  # type: ignore[arg-type]
        return result


def _with_realized_amount(result: RiskResult, value: object) -> RiskResult:
    if result.realized is None:
        return result
    return replace(result, realized=replace(result.realized, net_excluding_fee=value))  # type: ignore[arg-type]


def _burn_cpu_seconds(seconds: float) -> None:
    """Occupy a worker thread with real work, not a sleep.

    A sleep would yield; the point of these faults is a worker that keeps the
    CPU and ignores the fact that its caller stopped waiting.
    """
    import time

    end = time.perf_counter() + seconds
    accumulator = 0
    while time.perf_counter() < end:
        accumulator = (accumulator + 1) % 1_000_003
