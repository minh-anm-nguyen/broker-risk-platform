"""Load, saturation and event-loop responsiveness.

Design doc section 17. These are **not** benchmarks and they publish no SLO.
They are correctness-under-load tests: at a configured ceiling the service must
reject predictably rather than queue without bound, keep the event loop
responsive, hold resources flat, and never return a wrong or unaudited number.

Every measurement printed here is from this machine and this in-process
harness. Loopback ASGI numbers are not a latency claim about a deployment, and
the 120 ms figure in the provenance note is a CV reference, not a result.
"""

from __future__ import annotations

import asyncio
import gc
import statistics
import time

import pytest
from conftest import ACCOUNT_A, SOURCE_ID, add_position

pytestmark = [pytest.mark.performance, pytest.mark.slow]

RISK = f"/api/v1/accounts/{ACCOUNT_A}/risk"
POSITIONS = "/api/v1/positions"


@pytest.fixture
def settings_kwargs(tmp_path):
    """A quota high enough that capacity, not rate, is what gets tested."""
    return {
        "environment": "test",
        "faults_enabled": True,
        "audit_backend": "memory",
        "audit_path": str(tmp_path / "audit.jsonl"),
        "log_level": "CRITICAL",
        "rate_limit_per_second": 2000.0,
        "rate_limit_burst": 4000.0,
    }


async def drive(client, headers, *, total: int, concurrency: int, path: str, params: dict):
    """Issue ``total`` requests with at most ``concurrency`` in flight."""
    gate = asyncio.Semaphore(concurrency)
    latencies: list[float] = []
    statuses: list[int] = []

    async def one() -> None:
        async with gate:
            started = time.perf_counter()
            response = await client.get(path, params=params, headers=headers)
            latencies.append(time.perf_counter() - started)
            statuses.append(response.status_code)

    await asyncio.gather(*(one() for _ in range(total)))
    return latencies, statuses


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
    return ordered[index]


class TestSaturationBehaviour:
    async def test_a_burst_beyond_capacity_rejects_predictably(self, client, context, auth):
        """LP-05: 60 simultaneous heavy reports against 4 executing + 16 queued.

        The pass condition is the *shape* of the outcome: every response is a
        documented status, nothing is a 500, and the queue never exceeded its
        ceiling.
        """
        latencies, statuses = await drive(
            client, auth, total=60, concurrency=60, path=RISK, params={"source_id": SOURCE_ID}
        )

        assert set(statuses) <= {200, 429, 503, 504}, sorted(set(statuses))
        assert 500 not in statuses
        served = statuses.count(200)
        refused = statuses.count(503) + statuses.count(429)
        assert served + refused + statuses.count(504) == 60
        assert context.admission.stats.peak_queued_heavy <= context.admission.max_heavy_queued
        assert context.admission.stats.peak_executing_heavy <= context.admission.max_heavy_executing
        assert context.admission.stats.executing_heavy == 0
        assert context.views.open_sessions == 0

        print(
            f"\n[burst 60] served={served} refused={refused} "
            f"timeouts={statuses.count(504)} "
            f"p50={percentile(latencies, 0.5) * 1000:.1f}ms "
            f"p95={percentile(latencies, 0.95) * 1000:.1f}ms "
            f"peak_queue={context.admission.stats.peak_queued_heavy}"
        )

    async def test_every_served_response_is_still_correct_under_load(
        self, client, context, auth
    ):
        """Refusing under pressure is fine; being wrong under pressure is not."""
        responses = await asyncio.gather(
            *(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth) for _ in range(40))
        )
        digests = set()
        for response in responses:
            assert response.status_code in (200, 429, 503, 504)
            if response.status_code == 200:
                body = response.json()
                assert body["components"]["realized"]["net_excluding_fee_usd"] == "56.00"
                assert body["composite_total"] == "81.00"
                assert body["audit_id"]
                digests.add(body["output_digest"])
        assert len(digests) <= 1, "concurrent reads disagreed on one cut"

        allowed = [row for row in context.audit.all_records() if row.decision == "ALLOW"]
        served = sum(1 for r in responses if r.status_code == 200)
        assert len(allowed) == served, "a served report had no receipt"

    async def test_capacity_refusals_carry_a_retry_hint(self, client, auth):
        _, statuses = await drive(
            client, auth, total=40, concurrency=40, path=RISK, params={"source_id": SOURCE_ID}
        )
        assert 503 in statuses or 200 in statuses
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        if response.status_code in (429, 503):
            assert "retry-after" in {k.lower() for k in response.headers}


class TestEventLoopResponsiveness:
    async def test_cheap_reads_stay_served_while_heavy_reports_run(
        self, client, context, auth
    ):
        """INV06-15: CPU work is offloaded, so liveness keeps answering."""
        context.faults.arm("p05.burn_cpu_ms", 150)
        heavy = [
            asyncio.create_task(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth))
            for _ in range(8)
        ]
        await asyncio.sleep(0.02)

        probe_latencies: list[float] = []
        for _ in range(10):
            started = time.perf_counter()
            response = await client.get("/health/live")
            probe_latencies.append(time.perf_counter() - started)
            assert response.status_code == 200

        await asyncio.gather(*heavy, return_exceptions=True)
        worst = max(probe_latencies)
        print(f"\n[loop] worst liveness probe under load = {worst * 1000:.1f}ms")
        # Generous: a blocked loop shows seconds here, not tens of milliseconds.
        assert worst < 2.0
        assert context.workers.heartbeat.p99_lag_seconds < 1.0

    async def test_the_worker_pool_never_exceeds_its_ceiling(self, client, context, auth):
        context.faults.arm("p05.burn_cpu_ms", 80)
        await asyncio.gather(
            *(client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth) for _ in range(20)),
            return_exceptions=True,
        )
        assert context.workers.stats.peak_in_flight <= context.workers.max_workers * 2
        await asyncio.sleep(0.3)
        assert context.workers.stats.in_flight == 0


class TestPaginationCost:
    async def test_a_large_scan_pages_in_bounded_work(self, client, context, world, auth):
        """LP-03: page cost must not grow with the offset."""
        for index in range(300):
            add_position(world, f"PL-{index:04d}", ACCOUNT_A, lots="0.01")

        durations: list[float] = []
        cursor = None
        pages = 0
        while pages < 10:
            params = {"source_id": SOURCE_ID, "limit": "50"}
            if cursor:
                params["cursor"] = cursor
            started = time.perf_counter()
            response = await client.get(POSITIONS, params=params, headers=auth)
            durations.append(time.perf_counter() - started)
            assert response.status_code == 200, response.text
            cursor = response.json()["next_cursor"]
            pages += 1
            if not cursor:
                break

        assert pages >= 6, "the fixture did not produce enough pages"
        first_half = statistics.mean(durations[: len(durations) // 2])
        second_half = statistics.mean(durations[len(durations) // 2 :])
        print(
            f"\n[paging] pages={pages} first_half={first_half * 1000:.1f}ms "
            f"second_half={second_half * 1000:.1f}ms"
        )
        # Keyset paging: later pages must not cost dramatically more.
        assert second_half < max(first_half * 6, 0.25)


class TestResourceStability:
    async def test_a_short_soak_leaves_resources_flat(self, client, context, auth):
        """A compressed stand-in for the soak profile: no monotonic growth."""
        gc.collect()
        baseline_objects = len(gc.get_objects())

        for _ in range(6):
            await asyncio.gather(
                *(
                    client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
                    for _ in range(10)
                ),
                return_exceptions=True,
            )

        gc.collect()
        growth = len(gc.get_objects()) - baseline_objects
        print(f"\n[soak] tracked object growth after 60 requests = {growth}")

        assert context.views.open_sessions == 0
        assert context.admission.stats.inflight == 0
        assert context.workers.stats.in_flight == 0
        # A leak per request would be tens of thousands of retained objects.
        assert growth < 50_000

    async def test_repeated_refusals_do_not_accumulate_state(self, client, context):
        for _ in range(60):
            response = await client.get(RISK, params={"source_id": SOURCE_ID})
            assert response.status_code == 401
        assert context.views.open_sessions == 0
        assert context.audit.all_records() == ()
        assert context.engine.stats.calls == 0

    async def test_response_size_stays_within_the_declared_budget(self, client, auth):
        response = await client.get(RISK, params={"source_id": SOURCE_ID}, headers=auth)
        assert response.status_code == 200
        assert len(response.content) < 1024 * 1024
        print(f"\n[size] risk report = {len(response.content)} bytes")
