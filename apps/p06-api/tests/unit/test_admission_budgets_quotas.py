"""Quotas, deadlines and admission control.

WF06-R and WF06-S. The distinction being protected: exceeding your own quota
is ``429`` and exhausting the service is ``503``. Both must release every
resource they took, including when the client disappears.
"""

from __future__ import annotations

import asyncio

import pytest
from risk_api.admission.budgets import Deadline, RequestBudget
from risk_api.admission.quotas import PrincipalQuotas, TokenBucket
from risk_api.admission.scheduler import AdmissionController
from risk_api.contracts.problem_details import ApiProblem

pytestmark = pytest.mark.unit


class MovableClock:
    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


# ------------------------------------------------------------- token bucket


class TestTokenBucket:
    def test_starts_full_at_the_burst_ceiling(self):
        bucket = TokenBucket(rate_per_second=20, burst=40)
        assert bucket.tokens == 40

    def test_consumes_then_refuses_with_a_retry_hint(self):
        bucket = TokenBucket(rate_per_second=20, burst=40)
        assert bucket.try_consume(40, now=0.0) == (True, 0.0)
        allowed, retry_after = bucket.try_consume(20, now=0.0)
        assert allowed is False
        assert retry_after == pytest.approx(1.0)

    def test_refills_at_the_sustained_rate(self):
        bucket = TokenBucket(rate_per_second=20, burst=40)
        bucket.try_consume(40, now=0.0)
        assert bucket.try_consume(20, now=1.0)[0] is True

    def test_never_refills_past_the_burst_ceiling(self):
        bucket = TokenBucket(rate_per_second=20, burst=40)
        bucket.try_consume(40, now=0.0)
        bucket.try_consume(1, now=1_000.0)
        assert bucket.tokens <= 40

    def test_a_request_heavier_than_the_burst_is_refused_immediately(self):
        """Telling a client to retry something that can never fit is a trap."""
        bucket = TokenBucket(rate_per_second=20, burst=40)
        allowed, retry_after = bucket.try_consume(41, now=0.0)
        assert allowed is False
        assert retry_after == float("inf")

    def test_non_positive_weight_is_a_programming_error(self):
        bucket = TokenBucket(rate_per_second=20, burst=40)
        with pytest.raises(ValueError):
            bucket.try_consume(0, now=0.0)

    def test_burst_below_rate_is_refused_at_construction(self):
        with pytest.raises(ValueError):
            TokenBucket(rate_per_second=20, burst=5)


class TestPrincipalQuotas:
    def test_tenants_have_independent_buckets(self):
        """WF06-R: one noisy tenant must not consume another tenant's quota."""
        clock = MovableClock()
        quotas = PrincipalQuotas(rate_per_second=20, burst=40, clock=clock)
        for _ in range(10):
            assert quotas.check(tenant_id="tenant-a", weight=4).allowed
        assert quotas.check(tenant_id="tenant-a", weight=4).allowed is False
        assert quotas.check(tenant_id="tenant-b", weight=4).allowed is True

    def test_retry_after_is_a_whole_number_of_seconds(self):
        clock = MovableClock()
        quotas = PrincipalQuotas(rate_per_second=20, burst=40, clock=clock)
        quotas.check(tenant_id="t", weight=40)
        decision = quotas.check(tenant_id="t", weight=40)
        assert decision.allowed is False
        assert decision.retry_after_seconds >= 1
        assert isinstance(decision.retry_after_seconds, int)

    def test_quota_recovers_as_the_clock_advances(self):
        clock = MovableClock()
        quotas = PrincipalQuotas(rate_per_second=20, burst=40, clock=clock)
        quotas.check(tenant_id="t", weight=40)
        assert quotas.check(tenant_id="t", weight=20).allowed is False
        clock.advance(1.0)
        assert quotas.check(tenant_id="t", weight=20).allowed is True

    def test_heavier_requests_cost_more_quota(self):
        clock = MovableClock()
        quotas = PrincipalQuotas(rate_per_second=20, burst=40, clock=clock)
        for _ in range(10):
            quotas.check(tenant_id="heavy", weight=4)
        assert quotas.tokens_left("heavy") == pytest.approx(0.0)

        quotas2 = PrincipalQuotas(rate_per_second=20, burst=40, clock=MovableClock())
        for _ in range(10):
            quotas2.check(tenant_id="cheap", weight=1)
        assert quotas2.tokens_left("cheap") == pytest.approx(30.0)


# ----------------------------------------------------------------- deadlines


class TestDeadlines:
    def test_remaining_shrinks_as_the_clock_moves(self):
        clock = MovableClock()
        deadline = Deadline.after(5.0, clock=clock)
        assert deadline.remaining() == pytest.approx(5.0)
        clock.advance(2.0)
        assert deadline.remaining() == pytest.approx(3.0)

    def test_check_raises_the_documented_timeout(self):
        clock = MovableClock()
        deadline = Deadline.after(1.0, clock=clock)
        clock.advance(1.5)
        with pytest.raises(ApiProblem) as caught:
            deadline.check(stage="compute")
        assert caught.value.code == "DEADLINE_EXCEEDED"
        assert caught.value.status == 504
        assert "compute" in caught.value.detail

    def test_a_sub_budget_cannot_outlive_the_deadline(self):
        clock = MovableClock()
        deadline = Deadline.after(1.0, clock=clock)
        assert deadline.slice(3.0) == pytest.approx(1.0)
        clock.advance(0.9)
        assert deadline.slice(3.0) == pytest.approx(0.1, abs=1e-6)

    def test_an_expired_deadline_grants_no_budget(self):
        clock = MovableClock()
        deadline = Deadline.after(0.5, clock=clock)
        clock.advance(1.0)
        assert deadline.slice(3.0) == 0.0

    def test_sub_budgets_must_fit_the_total(self):
        with pytest.raises(ValueError):
            RequestBudget(
                total_seconds=5.0, capture_seconds=3.0, compute_seconds=3.0, final_check_seconds=1.0
            )

    def test_a_coherent_budget_is_accepted(self):
        budget = RequestBudget(
            total_seconds=5.0, capture_seconds=1.0, compute_seconds=3.0, final_check_seconds=0.5
        )
        assert budget.deadline(clock=MovableClock()).remaining() == pytest.approx(5.0)


# ---------------------------------------------------------------- admission


def controller(
    *,
    executing: int = 2,
    queued: int = 2,
    cheap: int = 3,
    rate: float = 100.0,
    burst: float = 200.0,
    clock: MovableClock | None = None,
) -> AdmissionController:
    return AdmissionController(
        quotas=PrincipalQuotas(
            rate_per_second=rate, burst=burst, clock=clock or MovableClock()
        ),
        max_heavy_executing=executing,
        max_heavy_queued=queued,
        max_cheap_concurrent=cheap,
        heavy_weight=4.0,
        cheap_weight=1.0,
        retry_after_seconds=2,
    )


class TestAdmission:
    async def test_a_heavy_request_is_admitted_and_released(self):
        admission = controller()
        deadline = Deadline.after(5.0, clock=MovableClock())
        async with admission.admit(tenant_id="t", workload="heavy", deadline=deadline):
            assert admission.stats.executing_heavy == 1
        assert admission.stats.executing_heavy == 0
        assert admission.stats.admitted_heavy == 1

    async def test_executing_never_exceeds_the_ceiling(self):
        admission = controller(executing=2, queued=8)
        clock = MovableClock()
        release = asyncio.Event()

        async def hold() -> None:
            async with admission.admit(
                tenant_id="t", workload="heavy", deadline=Deadline.after(5.0, clock=clock)
            ):
                await release.wait()

        tasks = [asyncio.create_task(hold()) for _ in range(6)]
        await asyncio.sleep(0.05)
        assert admission.stats.executing_heavy == 2
        assert admission.stats.queued_heavy == 4
        release.set()
        await asyncio.gather(*tasks)
        assert admission.stats.peak_executing_heavy == 2
        assert admission.stats.executing_heavy == 0
        assert admission.stats.queued_heavy == 0

    async def test_a_full_queue_refuses_with_overloaded_and_retry_after(self):
        """WF06-S-01: capacity exhaustion is 503, not a growing backlog."""
        admission = controller(executing=1, queued=1)
        clock = MovableClock()
        release = asyncio.Event()

        async def hold() -> None:
            async with admission.admit(
                tenant_id="t", workload="heavy", deadline=Deadline.after(5.0, clock=clock)
            ):
                await release.wait()

        running = asyncio.create_task(hold())
        waiting = asyncio.create_task(hold())
        await asyncio.sleep(0.05)
        assert admission.stats.executing_heavy == 1
        assert admission.stats.queued_heavy == 1

        with pytest.raises(ApiProblem) as caught:
            async with admission.admit(
                tenant_id="t", workload="heavy", deadline=Deadline.after(5.0, clock=clock)
            ):
                pass
        assert caught.value.code == "OVERLOADED"
        assert caught.value.status == 503
        assert caught.value.headers["Retry-After"] == "2"

        release.set()
        await asyncio.gather(running, waiting)
        assert admission.stats.rejected_capacity == 1

    async def test_quota_exhaustion_is_429_not_503(self):
        admission = controller(rate=4.0, burst=8.0)
        deadline = Deadline.after(5.0, clock=MovableClock())
        async with admission.admit(tenant_id="t", workload="heavy", deadline=deadline):
            pass
        async with admission.admit(tenant_id="t", workload="heavy", deadline=deadline):
            pass
        with pytest.raises(ApiProblem) as caught:
            async with admission.admit(tenant_id="t", workload="heavy", deadline=deadline):
                pass
        assert caught.value.code == "RATE_LIMITED"
        assert caught.value.status == 429
        assert admission.stats.rejected_quota == 1

    async def test_cheap_requests_refuse_rather_than_queue(self):
        admission = controller(cheap=1)
        clock = MovableClock()
        release = asyncio.Event()

        async def hold() -> None:
            async with admission.admit(
                tenant_id="t", workload="cheap", deadline=Deadline.after(5.0, clock=clock)
            ):
                await release.wait()

        running = asyncio.create_task(hold())
        await asyncio.sleep(0.05)
        with pytest.raises(ApiProblem) as caught:
            async with admission.admit(
                tenant_id="t", workload="cheap", deadline=Deadline.after(5.0, clock=clock)
            ):
                pass
        assert caught.value.code == "OVERLOADED"
        release.set()
        await running

    async def test_a_cancelled_waiter_leaves_no_phantom_queue_entry(self):
        """WF06-X: a client that gives up must not hold capacity bookkeeping."""
        admission = controller(executing=1, queued=4)
        clock = MovableClock()
        release = asyncio.Event()

        async def hold() -> None:
            async with admission.admit(
                tenant_id="t", workload="heavy", deadline=Deadline.after(30.0, clock=clock)
            ):
                await release.wait()

        running = asyncio.create_task(hold())
        await asyncio.sleep(0.05)
        waiting = asyncio.create_task(hold())
        await asyncio.sleep(0.05)
        assert admission.stats.queued_heavy == 1

        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        assert admission.stats.queued_heavy == 0

        release.set()
        await running
        assert admission.stats.executing_heavy == 0

    async def test_a_queued_request_gives_up_at_its_deadline(self):
        admission = controller(executing=1, queued=4)
        clock = MovableClock()
        release = asyncio.Event()

        async def hold() -> None:
            async with admission.admit(
                tenant_id="t", workload="heavy", deadline=Deadline.after(30.0, clock=clock)
            ):
                await release.wait()

        running = asyncio.create_task(hold())
        await asyncio.sleep(0.05)
        # A real monotonic deadline here, so the wait genuinely times out.
        with pytest.raises(ApiProblem) as caught:
            async with admission.admit(tenant_id="t", workload="heavy", deadline=Deadline.after(0.05)):
                pass
        assert caught.value.code == "DEADLINE_EXCEEDED"
        assert admission.stats.queued_heavy == 0
        release.set()
        await running

    def test_invalid_ceilings_are_refused(self):
        with pytest.raises(ValueError):
            controller(executing=0)
        with pytest.raises(ValueError):
            AdmissionController(
                quotas=PrincipalQuotas(rate_per_second=1, burst=1, clock=MovableClock()),
                max_heavy_executing=1,
                max_heavy_queued=-1,
                max_cheap_concurrent=1,
                heavy_weight=1.0,
                cheap_weight=1.0,
                retry_after_seconds=1,
            )
