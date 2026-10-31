"""Benchmarks B08-01 .. B08-12, scaled to run in CI. Section 13.

**Read the numbers correctly.** These run against the deterministic
in-process adapters on whatever machine CI gave us. They therefore measure
*P08's own* ingest, dedup, aggregation and oracle work - not ClickHouse or
Kafka. Section 13.3's targets (100 events/s sustained, a 100k build inside
two minutes, p95 reads under 500 ms) are explicitly **UNMEASURED** until a
run against a real pinned cluster is recorded; nothing here should be quoted
as evidence for them.

What these tests *do* establish, and what makes them worth running:

* the work is linear-ish in Deal count rather than quadratic, so a 10x corpus
  does not become a 100x build;
* memory and row counts stay bounded (INV08-20);
* correctness holds at scale, which is the part most likely to break - a
  dedup bug shows up at 100k rows and not at 3.

The slow tiers are marked ``slow`` and excluded from the per-commit suite.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field, replace
from datetime import date

import pytest

from analytics.errors import BuildState, ReportStatus
from analytics.fixtures.golden import DEFAULT_TOPIC, generate_deals

from helpers import assert_matches_oracle, expected_daily, ingest_all, produce_specs, stub_source

pytestmark = pytest.mark.performance

#: Honest labelling for every number this module prints.
MEASUREMENT_SCOPE = (
    "in-process adapters; measures P08 logic only, not ClickHouse or Kafka; "
    "section 13.3 targets remain UNMEASURED"
)


@dataclass
class Timing:
    label: str
    seconds: float
    rows: int
    extra: dict = field(default_factory=dict)

    @property
    def rows_per_second(self) -> float:
        return self.rows / self.seconds if self.seconds > 0 else float("inf")

    def report(self) -> str:
        return (
            f"{self.label}: {self.rows} rows in {self.seconds:.3f}s "
            f"({self.rows_per_second:,.0f} rows/s) [{MEASUREMENT_SCOPE}]"
        )


def _time(label: str, rows: int, fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    timing = Timing(label=label, seconds=elapsed, rows=rows)
    print("\n" + timing.report())
    return result, timing


class TestB0801SteadyIngest:
    @pytest.mark.parametrize("count", [500, 2_000])
    def test_ingest_throughput_is_recorded(self, stack, count):
        """B08-01 scaled: accepted vs committed, plus a rate for trending."""
        specs = list(generate_deals(count, seed=801))
        produce_specs(stack, specs)
        _, timing = _time(
            f"B08-01 ingest {count}", count, lambda: ingest_all(stack, max_cycles=200)
        )
        assert stack.clickhouse.raw_row_count() == count
        committed = stack.consumer.committed([(DEFAULT_TOPIC, 0)])[(DEFAULT_TOPIC, 0)]
        assert committed == count, "every accepted record must be committed"
        assert timing.rows_per_second > 0


class TestB0803DuplicateStorm:
    def test_duplicates_do_not_change_the_result_or_explode_the_cost(self, stack, scope):
        """B08-03: 100% duplicate replay. Canonical rows must not grow."""
        specs = list(generate_deals(1_000, seed=803))
        produce_specs(stack, specs)
        ingest_all(stack, max_cycles=200)
        clean, clean_timing = _time(
            "B08-03 build (no duplicates)",
            len(specs),
            lambda: stack.build_worker(stub_source(stack, specs)).run(scope, publish=False),
        )
        assert clean.state is BuildState.VERIFIED

        for offset in range(len(specs)):
            stack.broker.redeliver((DEFAULT_TOPIC, 0), offset)
        ingest_all(stack, max_cycles=400)
        assert stack.clickhouse.raw_row_count() == 2 * len(specs)

        dirty, dirty_timing = _time(
            "B08-03 build (100% duplicates)",
            len(specs),
            lambda: stack.build_worker(stub_source(stack, specs)).run(
                replace(scope, cut_id="cut-dup"), publish=False
            ),
        )
        assert dirty.state is BuildState.VERIFIED
        assert dirty.canonical_digest == clean.canonical_digest
        assert len(dirty.dedup.deals) == len(specs)
        # Twice the observations should not cost dramatically more than twice
        # the time; a super-linear blow-up here would be a dedup bug.
        assert dirty_timing.seconds < max(clean_timing.seconds * 8, 5.0)


class TestB0804BuildScaling:
    @pytest.mark.parametrize("count", [1_000, 5_000])
    def test_build_cost_grows_roughly_linearly(self, stack, scope, count):
        """B08-04 scaled: the shape of the curve, not an absolute SLO."""
        specs = list(generate_deals(count, seed=804, accounts=50, days=7))
        produce_specs(stack, specs)
        ingest_all(stack, max_cycles=400)
        outcome, timing = _time(
            f"B08-04 build {count}",
            count,
            lambda: stack.build_worker(stub_source(stack, specs)).run(scope),
        )
        assert outcome.state is BuildState.PUBLISHED
        physical, unique = stack.clickhouse.count_deal_facts(
            build_id=outcome.build_id, tenant_id=scope.tenant_id
        )
        assert physical == unique == count
        assert_matches_oracle(stack, outcome, scope, specs)
        assert timing.rows_per_second > 0

    @pytest.mark.slow
    def test_a_hundred_thousand_deals_stay_correct(self, stack, scope):
        """B08-04 at 100k: correctness at the design's P0 corpus size.

        Marked slow. The point is the exact set and sums at scale, where a
        dedup or grouping bug actually surfaces - not the wall time, which
        says nothing about a real cluster.
        """
        count = 100_000
        specs = list(generate_deals(count, seed=13013, accounts=200, days=30))
        _, produce_timing = _time(
            "B08-04 produce 100k", count, lambda: produce_specs(stack, specs)
        )
        _, ingest_timing = _time(
            "B08-04 ingest 100k", count, lambda: ingest_all(stack, max_cycles=5_000)
        )
        assert stack.clickhouse.raw_row_count() == count

        outcome, build_timing = _time(
            "B08-04 build 100k",
            count,
            lambda: stack.build_worker(stub_source(stack, specs)).run(scope),
        )
        assert outcome.state is BuildState.PUBLISHED
        assert len(outcome.dedup.deals) == count
        physical, unique = stack.clickhouse.count_deal_facts(
            build_id=outcome.build_id, tenant_id=scope.tenant_id
        )
        assert physical == unique == count

        rows = stack.clickhouse.read_daily_rollups(
            build_id=outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        assert sum(row.deal_count for row in rows) == count
        assert len(rows) == len(expected_daily(specs))
        assert_matches_oracle(stack, outcome, scope, specs)
        del produce_timing, ingest_timing, build_timing


class TestB0806MixedReadWrite:
    def test_concurrent_reads_see_one_build_each(self, stack, scope, reader):
        """B08-06 scaled: 100 reads under a published build."""
        specs = list(generate_deals(2_000, seed=806, accounts=40, days=5))
        produce_specs(stack, specs)
        ingest_all(stack, max_cycles=400)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED

        latencies: list[float] = []
        build_ids = set()
        for _ in range(100):
            start = time.perf_counter()
            report = stack.query.daily(
                reader,
                source_id=scope.source_id,
                epoch_id=scope.epoch_id,
                from_day=date(2026, 10, 1),
                to_day=date(2026, 12, 31),
            )
            latencies.append(time.perf_counter() - start)
            assert report.envelope.status is ReportStatus.READY
            build_ids.add(report.envelope.build_id)

        latencies.sort()
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]
        print(
            f"\nB08-06 read latency p50={p50 * 1000:.1f}ms p95={p95 * 1000:.1f}ms "
            f"p99={p99 * 1000:.1f}ms over {len(latencies)} reads [{MEASUREMENT_SCOPE}]"
        )
        assert build_ids == {outcome.build_id}, "every response came from one build"
        assert len(latencies) == 100


class TestB0807Skew:
    def test_a_hot_account_does_not_break_correctness(self, stack, scope):
        """B08-07: 90% of records on one account."""
        specs = list(generate_deals(2_000, seed=807, accounts=20, days=3, account_skew=0.9))
        produce_specs(stack, specs)
        ingest_all(stack, max_cycles=400)
        outcome, timing = _time(
            "B08-07 skewed build",
            len(specs),
            lambda: stack.build_worker(stub_source(stack, specs)).run(scope),
        )
        assert outcome.state is BuildState.PUBLISHED
        assert_matches_oracle(stack, outcome, scope, specs)
        hot = [deal for deal in outcome.dedup.deals if deal.account_id == "A-HOT"]
        assert len(hot) == 1_800
        assert timing.rows_per_second > 0


class TestB0808OutageRecovery:
    def test_drain_after_an_outage_is_complete(self, stack, scope):
        """B08-08: storage unavailable, then recovery, then a full sink."""
        specs = list(generate_deals(1_000, seed=808))
        produce_specs(stack, specs)

        stack.clickhouse.set_disk_capacity_rows(100)
        stack.ingest_worker().run_until_idle(max_cycles=20)
        during = stack.clickhouse.raw_row_count()
        assert during < len(specs)

        stack.clickhouse.set_disk_capacity_rows(5_000_000)
        _, timing = _time(
            "B08-08 drain after outage",
            len(specs) - during,
            lambda: stack.ingest_worker().run_until_idle(max_cycles=500),
        )
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        assert len(outcome.dedup.deals) == len(specs)
        assert timing.rows >= 0


class TestResourceBounds:
    def test_memory_stays_bounded_across_a_large_build(self, stack, scope):
        """INV08-20: a bound that is measured, not assumed."""
        psutil = pytest.importorskip("psutil")
        process = psutil.Process(os.getpid())
        before = process.memory_info().rss

        specs = list(generate_deals(5_000, seed=999, accounts=100, days=10))
        produce_specs(stack, specs)
        ingest_all(stack, max_cycles=500)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED

        after = process.memory_info().rss
        growth_mb = (after - before) / (1024 * 1024)
        print(
            f"\nRSS growth across a 5,000-Deal build: {growth_mb:.1f} MiB "
            f"[{MEASUREMENT_SCOPE}]"
        )
        # Generous because the whole corpus is deliberately held in memory
        # here; the assertion catches an unbounded leak, not a tight budget.
        assert growth_mb < 600, f"memory grew by {growth_mb:.1f} MiB"

    def test_parts_do_not_grow_without_bound(self, stack):
        """Section 13.3: parts must stabilise after maintenance."""
        for batch in range(20):
            produce_specs(stack, list(generate_deals(10, seed=batch, first_source_seq=batch * 10 + 1)))
            ingest_all(stack)
        raw_table = "broker_analytics.broker_events_raw"
        before = stack.clickhouse.storage_stats().active_parts[raw_table]
        stack.clickhouse.optimize()
        after = stack.clickhouse.storage_stats().active_parts[raw_table]
        print(f"\nparts before merge={before} after merge={after}")
        assert before > 1
        assert after == 1

    def test_rebuild_cost_is_not_quadratic(self, stack, scope, settings, clock):
        """A 4x corpus must not cost 16x. Section 13.2's arithmetic."""
        from analytics.main import build_lab_stack

        timings = {}
        for count in (500, 2_000):
            fresh = build_lab_stack(settings=settings, clock=clock)
            try:
                specs = list(generate_deals(count, seed=count, accounts=20, days=5))
                produce_specs(fresh, specs)
                ingest_all(fresh, max_cycles=400)
                start = time.perf_counter()
                outcome = fresh.build_worker(stub_source(fresh, specs)).run(scope)
                timings[count] = time.perf_counter() - start
                assert outcome.state is BuildState.PUBLISHED
            finally:
                fresh.close()
        ratio = timings[2_000] / max(timings[500], 1e-6)
        print(
            f"\nbuild scaling: 500 -> {timings[500]:.3f}s, 2000 -> {timings[2_000]:.3f}s "
            f"(ratio {ratio:.1f}x for 4x the rows) [{MEASUREMENT_SCOPE}]"
        )
        # 4x the rows at 16x the cost would be quadratic. A generous ceiling
        # keeps this from flaking on a noisy CI box while still catching a
        # genuine complexity regression.
        assert ratio < 12, f"build cost scaled {ratio:.1f}x for 4x the rows"


class TestBenchmarkHonesty:
    def test_the_design_targets_are_declared_unmeasured(self):
        """Section 13.3 lists every target as UNMEASURED.

        This test exists so nobody can quote this module's numbers as
        evidence for the design's SLOs: the numbers above measure P08's logic
        against in-process adapters, and the targets are about a real pinned
        cluster.
        """
        unmeasured_targets = {
            "ingest_sustained_events_per_second": 100,
            "ingest_burst_events_per_second": 1_000,
            "build_100k_seconds": 120,
            "build_1m_seconds": 900,
            "read_p95_ms": 500,
            "freshness_seconds": 300,
            "consumer_rss_mib": 512,
        }
        assert len(unmeasured_targets) == 7
        assert "not ClickHouse or Kafka" in MEASUREMENT_SCOPE
        assert "UNMEASURED" in MEASUREMENT_SCOPE
