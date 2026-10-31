"""Scaled-down benchmark suites B07-01 .. B07-10.

These are not the benchmark. Design section 16.1 could not be clearer: ``DEV``
is for correctness and rapid iteration and is **not a durability proof**, and
the throughput and latency targets belong to the ``HA-LIKE`` lab with three
brokers, RF=3 and ``min.insync.replicas=2``. Everything here runs in one
process against an in-process broker model with no network, no replication and
no broker fsync, so the rates it prints are harness rates.

What these *do* assert, and what makes them worth running in CI, is the shape
rather than the magnitude:

* the backlog drains completely, with the identity sets matching afterwards;
* resource use stays inside the declared caps under load and under outage;
* a hot aggregate does not starve the quiet ones, and its own version stream
  stays contiguous;
* throughput does not collapse superlinearly as the event count grows, which is
  how an accidental O(n^2) in the claim query would show up.

Each test records a measurement dictionary in the report so a run leaves the
artefacts section 16.3 asks for -- counts, not just latency.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from outbox.broker.embedded import Fault
from outbox.fixtures import EPOCH_A, account_burst, skewed_load, spread_load
from outbox.oracles import ResourceBudget, assert_all, o7_boundedness, run_core_suite
from outbox.ops.replay import backlog_drain_estimate
from outbox.pipeline import Pipeline

pytestmark = [pytest.mark.performance, pytest.mark.slow]

#: Deliberately small so the suite stays runnable per commit. The real suites
#: use 100/200/1000 events per second over 30 minutes.
BASELINE_EVENTS = 400
BURST_EVENTS = 800

CAVEAT = (
    "in-process broker model: no network, no replication, no broker fsync. "
    "Harness figure, not a Kafka measurement."
)


def _record(name: str, measurements: dict[str, object]) -> None:
    """Attach a measurement artefact to the run.

    Written to ``evidence/`` so a CI job can collect it. A load report with
    only latency in it is insufficient (section 16.3), so each one carries the
    counts as well.
    """
    payload = {"suite": name, "caveat": CAVEAT, **measurements}
    directory = Path("evidence")
    directory.mkdir(exist_ok=True)
    (directory / f"{name}.json").write_text(json.dumps(payload, indent=2, sort_keys=True))
    print(json.dumps(payload, sort_keys=True))


class TestSteadyLoad:
    def test_b07_02_sustained_load_drains_completely(self, pipeline: Pipeline) -> None:
        facts = spread_load(BASELINE_EVENTS, accounts=40)
        write_start = time.perf_counter()
        for index in range(0, len(facts), 50):
            pipeline.sync.insert_deals(EPOCH_A, facts[index : index + 50])
        write_seconds = time.perf_counter() - write_start

        publish_start = time.perf_counter()
        summary = pipeline.worker.run_until_idle(max_cycles=200_000)
        publish_seconds = time.perf_counter() - publish_start

        consume_start = time.perf_counter()
        report = pipeline.run_consumer()
        consume_seconds = time.perf_counter() - consume_start

        stats = pipeline.store.stats(now=pipeline.clock.now())
        _record(
            "B07-02-sustained",
            {
                "events": BASELINE_EVENTS,
                "accounts": 40,
                "write_seconds": round(write_seconds, 3),
                "publish_seconds": round(publish_seconds, 3),
                "consume_seconds": round(consume_seconds, 3),
                "write_events_per_second": round(BASELINE_EVENTS / write_seconds, 1),
                "publish_events_per_second": round(BASELINE_EVENTS / publish_seconds, 1),
                "acked": summary.acked,
                "applied": report.applied,
                "duplicates": report.duplicates,
                "quarantined": report.quarantined,
                "broker_records": pipeline.broker.record_count(),
                "pending_at_end": stats.pending_total,
                "published_at_end": stats.published_total,
                "storage_bytes": stats.storage_bytes,
            },
        )

        assert (stats.pending_total, stats.published_total) == (0, BASELINE_EVENTS)
        assert summary.acked == BASELINE_EVENTS
        assert report.applied == BASELINE_EVENTS
        assert_all(
            run_core_suite(
                pipeline.store,
                pipeline.broker,
                pipeline.consumer_store,
                source_id=pipeline.source_id,
                epoch_id=EPOCH_A,
                consumer_name=pipeline.settings.consumer_name,
                budget=ResourceBudget(max_claimed_rows=pipeline.settings.max_inflight),
            )
        )

    def test_b07_03_a_burst_does_not_grow_the_queue_without_bound(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, spread_load(BURST_EVENTS, accounts=80))
        peak_inflight = 0
        peak_buffer = 0
        peak_claimed = 0
        cycles = 0

        while True:
            report = pipeline.publisher.publish_cycle()
            cycles += 1
            peak_inflight = max(peak_inflight, pipeline.publisher.limits.inflight)
            peak_buffer = max(peak_buffer, pipeline.producer.buffer_used_bytes())
            peak_claimed = max(
                peak_claimed, pipeline.store.stats(now=pipeline.clock.now()).claimed_total
            )
            if report.claimed == 0 and report.callbacks_served == 0:
                break
            assert cycles < 200_000

        stats = pipeline.store.stats(now=pipeline.clock.now())
        _record(
            "B07-03-burst",
            {
                "events": BURST_EVENTS,
                "cycles": cycles,
                "peak_inflight": peak_inflight,
                "peak_buffer_bytes": peak_buffer,
                "peak_claimed_rows": peak_claimed,
                "inflight_cap": pipeline.settings.max_inflight,
                "buffer_cap_bytes": pipeline.settings.producer_buffer_max_bytes,
                "published": stats.published_total,
            },
        )

        assert stats.published_total == BURST_EVENTS
        assert peak_inflight <= pipeline.settings.max_inflight
        assert peak_claimed <= pipeline.settings.max_inflight
        assert peak_buffer <= pipeline.settings.producer_buffer_max_bytes

    def test_throughput_does_not_degrade_superlinearly(self, tmp_path, clock, hooks) -> None:
        """A rough check for an accidental O(n^2) in the claim path.

        The claim query contains a ``NOT EXISTS`` subquery over the same table,
        so it is exactly the kind of statement that can quietly become
        quadratic without the right index. Comparing the per-event cost at two
        sizes is a blunt instrument, but it is sensitive to that specific
        mistake and cheap to run.
        """
        from outbox.pipeline import build_pipeline

        timings: dict[int, float] = {}
        for size in (200, 800):
            built = build_pipeline(
                root=tmp_path / f"scale-{size}",
                clock=clock,
                hooks=hooks,
                claim_batch_size=50,
                max_inflight=64,
                synchronous="NORMAL",
            )
            try:
                built.sync.open_epoch(EPOCH_A)
                built.sync.insert_deals(EPOCH_A, spread_load(size, accounts=size // 4))
                start = time.perf_counter()
                built.worker.run_until_idle(max_cycles=500_000)
                timings[size] = (time.perf_counter() - start) / size
            finally:
                built.close()

        ratio = timings[800] / timings[200]
        _record(
            "scaling",
            {
                "seconds_per_event_at_200": round(timings[200], 6),
                "seconds_per_event_at_800": round(timings[800], 6),
                "per_event_cost_ratio": round(ratio, 2),
            },
        )
        # A 4x size increase that quadruples the per-event cost is quadratic.
        # The threshold is loose because a laptop under load is noisy; it is
        # tight enough to catch a missing index.
        assert ratio < 3.0, f"per-event cost grew {ratio:.1f}x from 200 to 800 events"


class TestOutageAndRecovery:
    def test_b07_05_outage_then_full_recovery_with_a_reported_rate(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, spread_load(BASELINE_EVENTS, accounts=40))
        pipeline.broker.fault = Fault.DOWN
        for _ in range(30):
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(5)

        during = pipeline.store.stats(now=pipeline.clock.now())
        assert during.published_total == 0
        assert pipeline.broker.record_count() == 0

        pipeline.broker.fault = Fault.NONE
        pipeline.clock.advance(3600)
        pipeline.publisher.circuit.record_success()
        start = time.perf_counter()
        summary = pipeline.worker.run_until_idle(max_cycles=500_000)
        drain_seconds = time.perf_counter() - start
        pipeline.run_consumer()

        after = pipeline.store.stats(now=pipeline.clock.now())
        rate = BASELINE_EVENTS / drain_seconds
        _record(
            "B07-05-outage-recovery",
            {
                "events": BASELINE_EVENTS,
                "pending_during_outage": during.pending_total + during.claimed_total,
                "storage_bytes_during_outage": during.storage_bytes,
                "drain_seconds": round(drain_seconds, 3),
                "measured_drain_rate_events_per_second": round(rate, 1),
                # Derived from the measured rate, never promised in advance.
                "projected_100k_drain_seconds": round(
                    backlog_drain_estimate(pending=100_000, publish_rate=rate) or -1, 1
                ),
                "acked": summary.acked,
                "published_at_end": after.published_total,
            },
        )
        assert (after.pending_total, after.published_total) == (0, BASELINE_EVENTS)
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == BASELINE_EVENTS

    def test_b07_06_leader_loss_midstream_preserves_correctness(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, spread_load(200, accounts=20))
        published_before = 0
        for index in range(60):
            # Flip the leader away every so often, mid-drain.
            pipeline.broker.fault = (
                Fault.LEADER_UNAVAILABLE if index % 7 == 0 else Fault.NONE
            )
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(3)
            published_before = pipeline.store.stats(now=pipeline.clock.now()).published_total

        pipeline.broker.fault = Fault.NONE
        pipeline.clock.advance(600)
        pipeline.publisher.circuit.record_success()
        pipeline.worker.run_until_idle(max_cycles=500_000)
        pipeline.run_consumer()

        stats = pipeline.store.stats(now=pipeline.clock.now())
        _record(
            "B07-06-leader-loss",
            {
                "events": 200,
                "published_before_settling": published_before,
                "published_at_end": stats.published_total,
                "broker_physical_records": pipeline.broker.record_count(),
                "retry_attempts": pipeline.relay_metrics.value(
                    "outbox_publish_attempts_total", outcome="RETRY"
                ),
            },
        )
        assert stats.published_total == 200
        assert_all(
            run_core_suite(
                pipeline.store,
                pipeline.broker,
                pipeline.consumer_store,
                source_id=pipeline.source_id,
                epoch_id=EPOCH_A,
                consumer_name=pipeline.settings.consumer_name,
            )
        )


class TestFairnessAndHotKeys:
    def test_b07_04_a_hot_account_keeps_a_contiguous_contended_stream(
        self, pipeline: Pipeline
    ) -> None:
        """One account, many Deals: the head lock is the contention point.

        Design section 6.3 point 7 calls this out as a hot spot. What must hold
        is that the versions come out contiguous and in order despite every
        event serialising on one head row.
        """
        pipeline.sync.insert_deals(EPOCH_A, account_burst(300, account_id="A-hot"))
        start = time.perf_counter()
        pipeline.worker.run_until_idle(max_cycles=500_000)
        elapsed = time.perf_counter() - start
        pipeline.run_consumer()

        versions = sorted(
            record.aggregate_version
            for record in pipeline.store.iter_records()
            if record.aggregate_key == "A-hot"
        )
        _record(
            "B07-04-hot-account",
            {
                "events": 300,
                "publish_seconds": round(elapsed, 3),
                "events_per_second": round(300 / elapsed, 1),
                "max_version": versions[-1],
            },
        )
        assert versions == list(range(1, 301))
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 300
        ledger = pipeline.consumer_store.version_ledger(pipeline.settings.consumer_name)
        assert ledger[0]["last_applied_version"] == 300

    def test_a_skewed_distribution_does_not_starve_the_quiet_accounts(
        self, pipeline: Pipeline
    ) -> None:
        """An even spread measures the easy case; real brokers are skewed."""
        pipeline.sync.insert_deals(EPOCH_A, skewed_load(400, accounts=10, hot_share=0.8))
        pipeline.worker.run_until_idle(max_cycles=500_000)
        pipeline.run_consumer()

        projection = {
            row.account_id: row.deal_count
            for row in pipeline.consumer_store.projection(
                pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
            )
        }
        _record("skew-fairness", {"events": 400, "per_account_counts": projection})
        assert sum(projection.values()) == 400
        # Every quiet account made progress, not just the hot one.
        quiet = {account: count for account, count in projection.items() if account != "A-000"}
        assert all(count > 0 for count in quiet.values()), projection
        assert len(quiet) >= 8


class TestResourceCeilings:
    def test_o7_holds_under_load(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, spread_load(BASELINE_EVENTS, accounts=40))
        pipeline.worker.run_until_idle(max_cycles=500_000)
        result = o7_boundedness(
            pipeline.store,
            budget=ResourceBudget(
                max_claimed_rows=pipeline.settings.max_inflight,
                max_inflight=pipeline.settings.max_inflight,
                max_buffer_bytes=pipeline.settings.producer_buffer_max_bytes,
                max_rss_bytes=1_500_000_000,
            ),
            inflight=pipeline.publisher.limits.inflight,
            buffer_bytes=pipeline.producer.buffer_used_bytes(),
            now=pipeline.clock.now(),
        )
        assert result.ok, result.details
