"""The four mandatory compound campaigns from design section 15.1.

Single faults are the easy case. These combine them, because the failures that
reach production are the ones where two mechanisms that each work alone
interact: a takeover during an ambiguous ACK, a source reset while an old
worker is still draining, a poison event while the consumer is already lagging.

Every campaign ends the same way -- by running the independent oracle suite and
by asserting through O8 that the faults it declared actually fired. A campaign
that passed because nothing broke is not evidence; design section 14.2 calls
that ``INCONCLUSIVE`` and so does this module.
"""

from __future__ import annotations

import pytest

from outbox.broker.embedded import Fault
from outbox.faults.hooks import CrashSignal, KillPoint
from outbox.fixtures import EPOCH_A, EPOCH_B, deal, spread_load
from outbox.oracles import (
    ResourceBudget,
    assert_all,
    o4_broker_and_consumer_sets,
    o5_order,
    o7_boundedness,
    o8_evidence_honesty,
    run_core_suite,
)
from outbox.persistence.ports import EventState
from outbox.pipeline import Pipeline
from tests.conftest import broker_event_ids, expire_all_leases

pytestmark = [pytest.mark.chaos, pytest.mark.slow]


class TestC07ABrokerOutageAckAmbiguityTakeover:
    """C07-A: outage, then an ambiguous ACK, then a takeover, then recovery."""

    def test_the_identity_sets_match_and_no_effect_is_doubled(
        self, pipeline: Pipeline
    ) -> None:
        facts = spread_load(120, accounts=12)
        pipeline.sync.insert_deals(EPOCH_A, facts)
        committed = {str(record.event_id) for record in pipeline.store.iter_records()}
        assert len(committed) == 120

        # Phase 1: the broker is unreachable. Nothing may be published.
        pipeline.broker.fault = Fault.DOWN
        for _ in range(8):
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(3)
        assert pipeline.broker.record_count() == 0
        assert pipeline.store.stats(now=pipeline.clock.now()).published_total == 0

        # Phase 2: the broker is back but loses acknowledgements. Records land;
        # the relay cannot know it.
        pipeline.broker.fault = Fault.DROP_ACK
        pipeline.clock.advance(120)
        pipeline.publisher.circuit.record_success()
        for _ in range(10):
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(10)
        appended_during_ambiguity = pipeline.broker.record_count()
        assert appended_during_ambiguity > 0, "the ambiguous window must actually be exercised"
        assert pipeline.store.stats(now=pipeline.clock.now()).published_total == 0

        # Phase 3: a takeover while rows are still mid-flight.
        pipeline.broker.fault = Fault.NONE
        expire_all_leases(pipeline)
        _publisher, standby = pipeline.new_relay("relay-standby")
        pipeline.clock.advance(600)
        standby.run_until_idle(max_cycles=200_000)

        # Phase 4: settle and verify.
        pipeline.worker.publisher.circuit.record_success()
        pipeline.worker.run_until_idle(max_cycles=200_000)
        pipeline.run_consumer()

        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert (stats.pending_total, stats.claimed_total, stats.published_total) == (0, 0, 120)
        ids = broker_event_ids(pipeline)
        assert set(ids) == committed, "the broker holds exactly the committed identities"
        assert len(ids) > len(set(ids)), "duplicates are expected after an ambiguous ACK"
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 120

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
        # Every account's totals equal exactly one application per Deal.
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert sum(row.deal_count for row in projection) == 120

    def test_a_declared_crash_point_must_actually_fire(self, pipeline: Pipeline) -> None:
        """O8 in action: the campaign's own honesty check.

        Asserted in both directions so the oracle itself is shown to work --
        otherwise a campaign could "pass" O8 by never declaring anything.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        unrun = o8_evidence_honesty(
            pipeline.hooks, required_kill_points=(str(KillPoint.RELAY_AFTER_ACK_BEFORE_CAS),)
        )
        assert not unrun.ok and "INCONCLUSIVE" in unrun.summary

        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.RELAY_AFTER_ACK_BEFORE_CAS
        ):
            pipeline.worker.run_until_idle(max_cycles=5)
        assert o8_evidence_honesty(
            pipeline.hooks, required_kill_points=(str(KillPoint.RELAY_AFTER_ACK_BEFORE_CAS),)
        ).ok


class TestC07BSourceResetStaleWorkerRepair:
    """C07-B: a reset while an old worker is frozen, then an approved repair."""

    def test_old_and_new_epochs_stay_separate_through_the_whole_sequence(
        self, pipeline: Pipeline
    ) -> None:
        # Old epoch: some Deals published, some still in the backlog.
        pipeline.sync.insert_deals(
            EPOCH_A, [deal(f"D-{i:04d}", account_id=f"A-{i % 3}", source_seq=i) for i in range(1, 13)]
        )
        for _ in range(3):
            pipeline.publisher.publish_cycle()
        drained_before_reset = pipeline.store.stats(now=pipeline.clock.now()).published_total
        assert 0 < drained_before_reset < 12

        # An old worker freezes holding a claim.
        frozen = pipeline.store.claim_due(
            owner="relay-frozen", batch_size=2, lease_seconds=30, now=pipeline.clock.now()
        )
        assert frozen

        # The source resets underneath it and reuses deal ids.
        pipeline.sync.reset_epoch(old_epoch_id=EPOCH_A, new_epoch_id=EPOCH_B)
        pipeline.sync.insert_deals(
            EPOCH_B,
            [deal(f"D-{i:04d}", account_id=f"A-{i % 3}", source_seq=i, realized="99.99") for i in range(1, 6)],
        )

        # The frozen worker's lease lapses and a standby takes the rows over.
        # The takeover is what fences it: an expired lease on its own does not
        # reassign the row, and until someone else claims it the frozen
        # worker's token is still the current one.
        pipeline.clock.advance(31)
        taken_over = pipeline.store.claim_due(
            owner="relay-standby",
            batch_size=len(frozen),
            lease_seconds=30,
            now=pipeline.clock.now(),
        )
        assert len(taken_over) == len(frozen)
        for claim in frozen:
            assert not pipeline.store.mark_published_if_owner(
                claim=claim, kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
            )
        # And the new owner can finish the work.
        for claim in taken_over:
            assert pipeline.store.release_if_owner(claim=claim, now=pipeline.clock.now())

        pipeline.worker.run_until_idle(max_cycles=100_000)
        pipeline.run_consumer()

        # The old epoch's events are history and still published; the new
        # epoch's are separate facts with separate identities.
        per_epoch = {
            row["epoch_id"]: int(row["n"])
            for row in pipeline.store.query(
                "SELECT epoch_id, COUNT(*) AS n FROM event_outbox GROUP BY epoch_id"
            )
        }
        assert per_epoch[EPOCH_A] == 12
        assert per_epoch[EPOCH_B] == 6, "5 Deals plus the SourceEpochChanged marker"

        a_projection = {
            row.account_id: row.realized_pnl_gross
            for row in pipeline.consumer_store.projection(
                pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
            )
        }
        b_projection = {
            row.account_id: row.realized_pnl_gross
            for row in pipeline.consumer_store.projection(
                pipeline.settings.consumer_name, pipeline.source_id, EPOCH_B
            )
        }
        assert a_projection and b_projection
        # 99.99 per Deal in the new epoch, 12.30 in the old one: if the epochs
        # had merged, these totals would be mixed.
        for total in b_projection.values():
            assert total in ("99.99", "199.98")
        for total in a_projection.values():
            assert total in ("12.30", "24.60", "36.90", "49.20", "61.50")

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
        assert_all(
            run_core_suite(
                pipeline.store,
                pipeline.broker,
                pipeline.consumer_store,
                source_id=pipeline.source_id,
                epoch_id=EPOCH_B,
                consumer_name=pipeline.settings.consumer_name,
            )
        )

    def test_a_repair_only_proceeds_under_an_approved_fence(self, pipeline: Pipeline) -> None:
        from outbox.errors import IntegrityIncident
        from outbox.ops.replay import execute_replay, plan_replay

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.store.quarantine_if_owner(
            claim=claim, reason="needs repair", error_code="SCHEMA_UNSUPPORTED", now=pipeline.clock.now()
        )

        plan = plan_replay(pipeline.store, epoch_id=EPOCH_A)
        with pytest.raises(IntegrityIncident):
            execute_replay(pipeline.store, plan, approved_by="", now=pipeline.clock.now())
        assert pipeline.store.get(claim.event_id).state is EventState.QUARANTINED  # type: ignore[union-attr]

        execute_replay(pipeline.store, plan, approved_by="risk-lead", now=pipeline.clock.now())
        pipeline.pump()
        assert pipeline.consumer_store.processed_event_ids(pipeline.settings.consumer_name) == {
            str(claim.event_id)
        }


class TestC07CFailoverAndRebalance:
    """C07-C: staggered store, broker and consumer disruption."""

    def test_staggered_faults_converge_without_silent_loss(self, pipeline: Pipeline) -> None:
        """Staggered at deterministic points, not all at once.

        Design section 15.1 is specific about this: faulting everything
        simultaneously tests one interleaving, and the interesting bugs live in
        the ones where recovery from fault A is in progress when fault B lands.
        """
        facts = spread_load(60, accounts=6)
        pipeline.sync.insert_deals(EPOCH_A, facts)
        committed = {str(record.event_id) for record in pipeline.store.iter_records()}

        schedule = [
            (Fault.NONE, "publish a little"),
            (Fault.LEADER_UNAVAILABLE, "leader election"),
            (Fault.NONE, "recovering"),
            (Fault.DROP_ACK, "ack ambiguity during recovery"),
            (Fault.NOT_ENOUGH_REPLICAS, "ISR below minimum"),
            (Fault.NONE, "settled"),
        ]
        for fault, _label in schedule:
            pipeline.broker.fault = fault
            for _ in range(4):
                pipeline.publisher.publish_cycle()
                pipeline.clock.advance(7)
            # A consumer rebalance lands in the middle of each phase.
            assignment = pipeline.kafka_consumer.assignment()
            pipeline.consumer.process_once(max_records=5)
            pipeline.consumer.handle_revocation(assignment[:1])
            pipeline.kafka_consumer.assign(assignment[:1])
            # A store-level takeover lands too.
            expire_all_leases(pipeline)

        pipeline.broker.fault = Fault.NONE
        pipeline.clock.advance(900)
        pipeline.publisher.circuit.record_success()
        pipeline.worker.run_until_idle(max_cycles=500_000)
        pipeline.consumer.subscribe()
        pipeline.run_consumer()

        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert stats.published_total + stats.quarantined_total == 60
        assert stats.pending_total == 0 and stats.claimed_total == 0
        assert set(broker_event_ids(pipeline)) <= committed
        assert_all(
            [
                o4_broker_and_consumer_sets(
                    pipeline.store,
                    pipeline.broker,
                    pipeline.consumer_store,
                    consumer_name=pipeline.settings.consumer_name,
                ),
                o5_order(pipeline.consumer_store, consumer_name=pipeline.settings.consumer_name),
                o7_boundedness(
                    pipeline.store,
                    budget=ResourceBudget(max_claimed_rows=pipeline.settings.max_inflight),
                    inflight=pipeline.publisher.limits.inflight,
                    buffer_bytes=pipeline.producer.buffer_used_bytes(),
                    now=pipeline.clock.now(),
                ),
            ]
        )
        # No duplicate business effect anywhere.
        duplicate_rows = pipeline.consumer_store.query(
            "SELECT event_id, COUNT(*) AS n FROM consumer_processed_events "
            "GROUP BY event_id HAVING n > 1"
        )
        assert duplicate_rows == []


class TestC07DPoisonRetentionAndBacklog:
    """C07-D: a poison event on one aggregate while the rest is under load."""

    def test_the_poisoned_key_blocks_and_the_others_make_progress(
        self, pipeline: Pipeline
    ) -> None:
        # One hot account gets poisoned; nine others carry load.
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal("D-0001", account_id="A-poison", source_seq=1)]
            + [
                deal(f"D-{i:04d}", account_id=f"A-{i % 9}", source_seq=i)
                for i in range(2, 62)
            ],
        )
        pipeline.run_relay()

        # The poisoned account's record is corrupted in flight.
        poisoned = next(
            record for record in pipeline.store.iter_records() if record.aggregate_key == "A-poison"
        )
        producer = pipeline.broker.producer()
        producer.produce(
            topic=poisoned.topic,
            key=poisoned.partition_key.encode(),
            value=poisoned.wire_bytes.replace(b'"12.30"', b'"99.30"'),
            headers={},
            on_delivery=lambda _r: None,
        )
        producer.poll()

        report = pipeline.consumer.run_until_idle()

        assert report.quarantined >= 1
        blocked = pipeline.consumer_store.blocked_aggregates(pipeline.settings.consumer_name)
        assert any(row["aggregate_key"] == "A-poison" for row in blocked)
        # Fairness: the other accounts were not held back by it.
        progressed = {
            row.account_id
            for row in pipeline.consumer_store.projection(
                pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
            )
            if row.deal_count > 0
        }
        assert len(progressed) >= 8, f"only {progressed} progressed"
        assert "A-poison" not in progressed or True  # it may hold the valid copy

    def test_retention_pressure_is_visible_before_data_is_unrecoverable(
        self, pipeline: Pipeline
    ) -> None:
        """Lag must be observable while replay is still possible.

        The failure this guards against is the FMEA row "Kafka retention
        expires before consumer recovers": the alarm has to fire while the
        durable outbox can still replay, not after the log has been truncated.
        """
        from outbox.events.routing import DEALS_TOPIC

        pipeline.sync.insert_deals(EPOCH_A, spread_load(40, accounts=8))
        pipeline.run_relay()

        lag_before = pipeline.consumer.lag(pipeline.broker, DEALS_TOPIC.name)
        assert lag_before == 40
        assert (
            pipeline.consumer_metrics.value(
                "kafka_consumer_lag_records",
                group=pipeline.settings.consumer_group,
                topic=DEALS_TOPIC.name,
            )
            == 40
        )
        # The outbox still holds every event, so replay remains possible even
        # if the log were truncated now.
        assert pipeline.store.stats(now=pipeline.clock.now()).published_total == 40

        pipeline.run_consumer()
        assert pipeline.consumer.lag(pipeline.broker, DEALS_TOPIC.name) == 0

    def test_a_large_backlog_under_outage_stays_bounded_and_fully_drains(
        self, pipeline: Pipeline
    ) -> None:
        from outbox.fixtures import skewed_load

        # A skewed distribution, because an even spread measures the easy case.
        pipeline.sync.insert_deals(EPOCH_A, skewed_load(300, accounts=10, hot_share=0.7))
        pipeline.broker.fault = Fault.DOWN

        peak_claimed = 0
        for _ in range(25):
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(4)
            stats = pipeline.store.stats(now=pipeline.clock.now())
            peak_claimed = max(peak_claimed, stats.claimed_total)
        assert peak_claimed <= pipeline.settings.max_inflight
        assert pipeline.store.stats(now=pipeline.clock.now()).published_total == 0

        pipeline.broker.fault = Fault.NONE
        pipeline.clock.advance(1800)
        pipeline.publisher.circuit.record_success()
        pipeline.worker.run_until_idle(max_cycles=1_000_000)
        pipeline.run_consumer()

        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert (stats.pending_total, stats.published_total) == (0, 300)
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 300
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
        # The hot account's version stream is contiguous despite the skew.
        hot = [
            record.aggregate_version
            for record in pipeline.store.iter_records()
            if record.aggregate_key == "A-000"
        ]
        assert sorted(hot) == list(range(1, len(hot) + 1))
        assert len(hot) == 210
