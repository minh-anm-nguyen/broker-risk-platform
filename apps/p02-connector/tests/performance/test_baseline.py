"""Benchmark profiles B0-B3 and the large-scan oracle (section 11.3).

Covers WF2-M-08, WF2-M-09, INV-08, INV-17 and the measurement discipline of
section 11.4.

These are **measurements with assertions on bounds**, not SLO gates. The
numbers a run produces belong to the machine it ran on, so what is asserted
here is the shape of the result - page counts, id sets, memory that does not
grow with the dataset - while the timings are recorded for the report rather
than compared against a target nobody has measured (section 0.3: the SLO
figures in the design are goals, not results).

Each profile prints one ``BENCH {json}`` line, which
``scripts/run_benchmark.py`` collects into a report alongside the machine and
build facts that make the numbers mean anything.
"""

from __future__ import annotations

import asyncio
import gc
import json
import os
import time

import pytest

from mt5_connector.ports import Entity, ReadPlan, ScanOutcome
from mt5_connector.scan import Scanner

from ..fakesim import DatasetSpec, FakeSimulator, FakeSimulatorServer, build_dataset

pytestmark = [pytest.mark.performance, pytest.mark.slow]

#: Set ``CONNECTOR_BENCH_DEALS`` to run the large profile at its real size.
#: Off by default so the per-commit suite stays fast; the nightly and weekly
#: lanes set it (section 14.16).
LARGE_DEALS = int(os.environ.get("CONNECTOR_BENCH_DEALS", "0"))


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(q * (len(ordered) - 1)))))
    return ordered[index]


def report(name: str, payload: dict) -> None:
    print(f"BENCH {json.dumps({'profile': name, **payload}, default=str)}")


@pytest.fixture
def bench_server(request):
    deals = getattr(request, "param", 500)
    state = FakeSimulator(
        build_dataset(DatasetSpec(seed=42, accounts=50, deals=deals, positions=20))
    )
    server = FakeSimulatorServer.start(state)
    try:
        yield server
    finally:
        server.stop()


async def bench_connector(server):
    from mt5_connector.client import Connector, build_driver_factory
    from mt5_connector.telemetry import Metrics

    from ..conftest import make_settings
    from ..fakesim import DATA_TOKEN

    settings = make_settings(
        base_url=server.base_url,
        source_kind="simulator",
        driver="simulator_sync_http",
        max_queue_pending=32,
        default_page_size=100,
        max_page_size=500,
        page_total_deadline_ms=5_000,
        queue_wait_cap_ms=2_000,
        upstream_read_timeout_ms=3_000,
    )
    connector = Connector(
        settings,
        driver_factory=build_driver_factory(settings, token=DATA_TOKEN),
        metrics=Metrics(source_id=settings.source_id),
    )
    await connector.connect()
    return connector


class TestB0Contract:
    async def test_one_read_of_each_entity_matches_the_source(self, bench_server) -> None:
        """B0: the contract profile. One page per entity, no faults."""
        connector = await bench_connector(bench_server)
        try:
            timings: dict[str, float] = {}
            for entity in Entity:
                started = time.perf_counter()
                page = await connector.read_page(ReadPlan(entity=entity, limit=100))
                timings[entity.value] = (time.perf_counter() - started) * 1000.0
                assert page.provenance.run_id == bench_server.sim.dataset.run_id
                assert page.items
            report("B0", {"page_latency_ms": timings})
        finally:
            await connector.close()


class TestB1Normal:
    async def test_ten_clients_reading_concurrently(self, bench_server) -> None:
        """B1: concurrent readers, no faults, denominators reported separately."""
        connector = await bench_connector(bench_server)
        try:
            latencies: list[float] = []
            errors = 0

            async def client(client_id: str) -> None:
                nonlocal errors
                for _ in range(10):
                    started = time.perf_counter()
                    try:
                        await connector.read_page(
                            ReadPlan(entity=Entity.DEALS, limit=50), client_id=client_id
                        )
                    except Exception:  # noqa: BLE001 - counted, never swallowed
                        errors += 1
                    finally:
                        latencies.append((time.perf_counter() - started) * 1000.0)

            await asyncio.gather(*[client(f"c{index}") for index in range(10)])

            # Section 11.4: admitted, rejected and errored are three different
            # denominators, so all three are reported rather than a percentile
            # computed over successes alone.
            report(
                "B1",
                {
                    "requests": len(latencies),
                    "errors": errors,
                    "admitted": connector.admission.admitted_total,
                    "rejected_overload": connector.admission.rejected_overload_total,
                    "p50_ms": percentile(latencies, 0.50),
                    "p95_ms": percentile(latencies, 0.95),
                    "p99_ms": percentile(latencies, 0.99),
                },
            )
            assert len(latencies) == 100
            assert connector.supervisor.actor.stats.peak_queue_depth <= 32
        finally:
            await connector.close()


class TestB2ConcurrentScans:
    async def test_two_scans_while_the_source_keeps_writing(self, bench_server) -> None:
        """B2, WF2-M-09: each scan internally consistent, none with duplicates."""
        connector = await bench_connector(bench_server)
        try:
            baseline = len(bench_server.sim.dataset.deal_ids())
            scanner = Scanner(connector)

            async def scan() -> list[str]:
                collected: list[str] = []
                async for page in scanner.pages(ReadPlan(entity=Entity.DEALS, limit=50)):
                    collected.extend(item.deal_id for item in page.items)
                return collected

            async def writer() -> None:
                for _ in range(5):
                    await asyncio.sleep(0.02)
                    bench_server.sim.append_deals(5)

            first, second, _ = await asyncio.gather(scan(), scan(), writer())
            # Each scan is pinned to its own snapshot. Whichever snapshot that
            # was, the scan contains no duplicate and no fewer records than
            # existed when it started.
            for collected in (first, second):
                assert len(set(collected)) == len(collected)
                assert len(collected) >= baseline
            report("B2", {"scan_a": len(first), "scan_b": len(second)})
        finally:
            await connector.close()


class TestB3Burst:
    async def test_a_burst_of_a_hundred_callers_is_bounded(self, bench_server) -> None:
        """B3: deterministic overload accounting, no thread per caller."""
        import threading

        connector = await bench_connector(bench_server)
        try:
            threads_before = threading.active_count()
            started = time.perf_counter()
            results = await asyncio.gather(
                *[
                    connector.read_page(ReadPlan(entity=Entity.DEALS, limit=100))
                    for _ in range(100)
                ],
                return_exceptions=True,
            )
            elapsed = time.perf_counter() - started
            failures = [item for item in results if isinstance(item, BaseException)]
            report(
                "B3",
                {
                    "requests": 100,
                    "failures": len(failures),
                    "elapsed_s": elapsed,
                    "peak_pending": connector.admission.peak_pending,
                    "peak_queue_depth": connector.supervisor.actor.stats.peak_queue_depth,
                    "rejected_overload": connector.admission.rejected_overload_total,
                    "threads_delta": threading.active_count() - threads_before,
                },
            )
            assert connector.admission.peak_pending <= 32
            assert connector.supervisor.actor.stats.peak_queue_depth <= 32
            assert threading.active_count() <= threads_before + 2
        finally:
            await connector.close()


class TestLargeScan:
    @pytest.mark.parametrize("bench_server", [LARGE_DEALS or 2_000], indirect=True)
    async def test_a_large_scan_matches_the_oracle_with_bounded_memory(
        self, bench_server
    ) -> None:
        """INV-08, WF2-M-08: the id set matches, and nothing is retained."""
        connector = await bench_connector(bench_server)
        try:
            oracle = bench_server.sim.dataset.deal_ids()
            scanner = Scanner(connector)

            seen = 0
            previous: str | None = None
            gc.collect()
            rss_before = _rss_bytes()
            started = time.perf_counter()
            plan = ReadPlan(entity=Entity.DEALS, limit=500)
            context = scanner.new_context(plan)
            # Streamed, not collected: holding a large scan in a list is what
            # section 7.4 forbids.
            async for page in scanner.pages(plan, context=context):
                for item in page.items:
                    if previous is not None:
                        assert item.deal_id > previous
                    previous = item.deal_id
                    seen += 1
            elapsed = time.perf_counter() - started
            gc.collect()
            rss_after = _rss_bytes()

            assert seen == len(oracle)
            assert previous == oracle[-1]
            report(
                "LARGE",
                {
                    "deals": seen,
                    "elapsed_s": elapsed,
                    "pages_per_s": (seen / 500) / max(elapsed, 1e-6),
                    "rss_delta_mib": (
                        (rss_after - rss_before) / (1024 * 1024) if rss_before else None
                    ),
                },
            )
            # RSS delta is reported, not asserted on. Measured after several
            # hundred other tests it reflects the interpreter's allocator
            # state as much as this scan, and a flaky bound on a real property
            # teaches people to ignore the test.
            #
            # The property itself is asserted structurally instead: whatever
            # the dataset size, the scan retains at most one page of items and
            # a bounded key set, which is what "does not grow with the
            # dataset" actually means (section 7.4).
            assert context.items_seen == len(oracle)
            assert context.pages_seen == (len(oracle) + 499) // 500
            assert len(context.seen_keys) <= context.max_tracked_keys + 1
            assert len(context.cursor_hashes) <= context.cursor_hashes.maxlen
        finally:
            await connector.close()

    async def test_a_page_cap_reports_incomplete_rather_than_complete(
        self, bench_server
    ) -> None:
        """WF2-M-08: a scan that hit its own bound is not a completed scan."""
        from mt5_connector.client import Connector, build_driver_factory
        from mt5_connector.scan import ScanAborted

        from ..conftest import make_settings
        from ..fakesim import DATA_TOKEN

        settings = make_settings(
            base_url=bench_server.base_url,
            source_kind="simulator",
            driver="simulator_sync_http",
            max_pages_per_scan=2,
            page_total_deadline_ms=5_000,
            queue_wait_cap_ms=2_000,
        )
        connector = Connector(
            settings, driver_factory=build_driver_factory(settings, token=DATA_TOKEN)
        )
        try:
            await connector.connect()
            scanner = Scanner(connector)
            result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
            assert result.outcome is not ScanOutcome.COMPLETE
            assert result.error_code == ScanAborted.code
            assert result.checkpoint_candidate is None
        finally:
            await connector.close()


def _rss_bytes() -> int:
    try:
        import psutil
    except ImportError:  # pragma: no cover - psutil is a test extra
        return 0
    return int(psutil.Process().memory_info().rss)
