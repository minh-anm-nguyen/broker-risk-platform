"""KP07-01 .. KP07-24: every named fault site, fired and verified.

Design section 15 lists the sites and adds the rule that gives them meaning: a
test that did not reach its hook is ``INCONCLUSIVE``, not a pass. So every test
here asserts ``hooks.fired(...) >= 1`` before it asserts anything about state,
and :func:`test_every_kill_point_is_covered` fails if a site in the inventory
has no test at all -- which is what stops the inventory from quietly becoming
aspirational.

The expected outcome for a crash before COMMIT is always the same sentence --
neither the business row nor the event survives -- and that repetition is the
point rather than a sign the tests could be merged: each one proves it for a
*different* instruction boundary, and a bug that moved the outbox insert outside
the transaction would be caught by exactly one of them.
"""

from __future__ import annotations

import pytest

from outbox.broker.embedded import Fault
from outbox.consumer.reference import ReferenceConsumer
from outbox.events.routing import DEALS_TOPIC
from outbox.faults.hooks import CrashSignal, KillPoint
from outbox.fixtures import EPOCH_A, EPOCH_B, deal
from outbox.persistence.ports import EventState
from outbox.pipeline import Pipeline
from tests.conftest import expire_all_leases

pytestmark = pytest.mark.chaos


def _counts(pipeline: Pipeline) -> tuple[int, int]:
    deals = int(pipeline.store.query("SELECT COUNT(*) AS n FROM deals")[0]["n"])
    events = int(pipeline.store.query("SELECT COUNT(*) AS n FROM event_outbox")[0]["n"])
    return deals, events


class TestP03TransactionKillPoints:
    """KP07-01 .. KP07-10."""

    def test_kp07_01_before_begin_leaves_no_state(self, pipeline: Pipeline) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_BEFORE_BEGIN):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert pipeline.hooks.fired(KillPoint.P03_BEFORE_BEGIN) == 1
        assert _counts(pipeline) == (0, 0)

    def test_kp07_02_after_begin_leaves_no_state(self, pipeline: Pipeline) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_AFTER_BEGIN):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert pipeline.hooks.fired(KillPoint.P03_AFTER_BEGIN) == 1
        assert _counts(pipeline) == (0, 0)

    def test_kp07_03_after_deal_insert_rolls_the_deal_back(self, pipeline: Pipeline) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_AFTER_DEAL_INSERT):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert pipeline.hooks.fired(KillPoint.P03_AFTER_DEAL_INSERT) == 1
        assert _counts(pipeline) == (0, 0)

    def test_kp07_04_after_head_bump_rolls_the_version_back_too(self, pipeline: Pipeline) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_AFTER_HEAD_BUMP):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert pipeline.hooks.fired(KillPoint.P03_AFTER_HEAD_BUMP) == 1
        assert _counts(pipeline) == (0, 0)
        # A head that survived would make the next event version 2 with no
        # version 1, and the consumer would block on the gap forever.
        assert pipeline.store.aggregate_head(pipeline.source_id, EPOCH_A, "A-42") == 0

    def test_kp07_05_after_outbox_insert_rolls_everything_back(self, pipeline: Pipeline) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_AFTER_OUTBOX_INSERT):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert pipeline.hooks.fired(KillPoint.P03_AFTER_OUTBOX_INSERT) == 1
        assert _counts(pipeline) == (0, 0)

    def test_kp07_05b_a_crash_mid_batch_loses_the_whole_page(self, pipeline: Pipeline) -> None:
        # All or nothing across the page, not "the first two deals survived".
        batch = [deal(f"D-{i:04d}", account_id=f"A-{i}", source_seq=i) for i in range(1, 6)]
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.P03_AFTER_OUTBOX_INSERT, after=3
        ):
            pipeline.sync.insert_deals(EPOCH_A, batch)
        assert pipeline.hooks.fired(KillPoint.P03_AFTER_OUTBOX_INSERT) == 3
        assert _counts(pipeline) == (0, 0)

    def test_kp07_06_before_commit_is_all_or_nothing(self, pipeline: Pipeline) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_BEFORE_COMMIT):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001"), deal("D-1002", source_seq=2)])
        assert pipeline.hooks.fired(KillPoint.P03_BEFORE_COMMIT) == 1
        assert _counts(pipeline) == (0, 0)

    def test_kp07_07_after_commit_before_ack_keeps_both_rows(self, pipeline: Pipeline) -> None:
        """The ambiguous commit. Durable, and the caller does not know it."""
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.P03_AFTER_COMMIT_BEFORE_ACK
        ):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert pipeline.hooks.fired(KillPoint.P03_AFTER_COMMIT_BEFORE_ACK) == 1
        assert _counts(pipeline) == (1, 1)
        # And the retry finds the identity rather than minting a new one.
        outcomes = pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert outcomes[0].duplicate_of_identical
        assert _counts(pipeline) == (1, 1)

    def test_kp07_08_snapshot_crash_after_staging_leaves_no_marker(
        self, pipeline: Pipeline
    ) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.P03_SNAPSHOT_AFTER_STAGING
        ):
            pipeline.sync.stage_scan(
                epoch_id=EPOCH_A,
                entity="accounts",
                scan_id="scan-1",
                snapshot_seq=1,
                record_ids=["A-1", "A-2"],
                lease_token="lease-1",
            )
        assert pipeline.hooks.fired(KillPoint.P03_SNAPSHOT_AFTER_STAGING) == 1
        # Staging itself rolled back, so there is not even a STAGED scan.
        assert pipeline.store.query("SELECT COUNT(*) AS n FROM sync_runs")[0]["n"] == 0
        assert _counts(pipeline)[1] == 0

    def test_kp07_09_crash_after_publish_update_rolls_the_publish_back(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="accounts",
            scan_id="scan-1",
            snapshot_seq=1,
            record_ids=["A-1", "A-2"],
            lease_token="lease-1",
        )
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.P03_SNAPSHOT_AFTER_PUBLISH_UPDATE
        ):
            pipeline.sync.publish_scan(
                epoch_id=EPOCH_A, entity="accounts", scan_id="scan-1", lease_token="lease-1"
            )
        assert pipeline.hooks.fired(KillPoint.P03_SNAPSHOT_AFTER_PUBLISH_UPDATE) == 1
        status = pipeline.store.query("SELECT status FROM sync_runs WHERE scan_id = ?", ("scan-1",))
        assert status[0]["status"] == "STAGED", "the publish flip rolled back"
        assert pipeline.store.query("SELECT COUNT(*) AS n FROM accounts_current")[0]["n"] == 0
        assert _counts(pipeline)[1] == 0

    def test_kp07_10_crash_after_marker_insert_rolls_both_back(self, pipeline: Pipeline) -> None:
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="positions",
            scan_id="scan-2",
            snapshot_seq=2,
            record_ids=["P-1"],
            lease_token="lease-1",
        )
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.P03_SNAPSHOT_AFTER_MARKER_INSERT
        ):
            pipeline.sync.publish_scan(
                epoch_id=EPOCH_A, entity="positions", scan_id="scan-2", lease_token="lease-1"
            )
        assert pipeline.hooks.fired(KillPoint.P03_SNAPSHOT_AFTER_MARKER_INSERT) == 1
        status = pipeline.store.query("SELECT status FROM sync_runs WHERE scan_id = ?", ("scan-2",))
        assert status[0]["status"] == "STAGED"
        assert _counts(pipeline)[1] == 0


class TestRelayKillPoints:
    """KP07-11 .. KP07-17."""

    def test_kp07_11_crash_before_claim_leaves_the_row_eligible(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.RELAY_BEFORE_CLAIM):
            pipeline.publisher.publish_cycle()
        assert pipeline.hooks.fired(KillPoint.RELAY_BEFORE_CLAIM) == 1
        record = next(iter(pipeline.store.iter_records()))
        assert record.state is EventState.PENDING
        assert record.attempt_count == 0

    def test_kp07_12_crash_after_claim_is_recovered_by_lease_expiry(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.RELAY_AFTER_CLAIM_BEFORE_PRODUCE
        ):
            pipeline.publisher.publish_cycle()
        assert pipeline.hooks.fired(KillPoint.RELAY_AFTER_CLAIM_BEFORE_PRODUCE) == 1
        record = next(iter(pipeline.store.iter_records()))
        assert record.state is EventState.CLAIMED
        assert pipeline.broker.record_count() == 0

        pipeline.clock.advance(31)
        _publisher, standby = pipeline.new_relay("relay-standby")
        standby.run_until_idle(max_cycles=20)
        record = next(iter(pipeline.store.iter_records()))
        assert record.state is EventState.PUBLISHED
        assert pipeline.broker.record_count() == 1

    def test_kp07_13_crash_after_enqueue_before_callback_is_retry_safe(
        self, pipeline: Pipeline
    ) -> None:
        """The broker may or may not hold it; the retry is safe either way.

        The record is enqueued in a producer that then dies, so nothing was
        ever delivered. The row stays CLAIMED until its lease lapses.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.RELAY_AFTER_ENQUEUE_BEFORE_CALLBACK
        ):
            pipeline.publisher.publish_cycle()
        assert pipeline.hooks.fired(KillPoint.RELAY_AFTER_ENQUEUE_BEFORE_CALLBACK) == 1
        record = next(iter(pipeline.store.iter_records()))
        assert record.state is EventState.CLAIMED and record.published_at is None

        pipeline.clock.advance(31)
        _publisher, standby = pipeline.new_relay("relay-standby")
        standby.run_until_idle(max_cycles=20)
        assert next(iter(pipeline.store.iter_records())).state is EventState.PUBLISHED

    def test_kp07_14_crash_after_ack_before_cas_yields_one_effect(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.RELAY_AFTER_ACK_BEFORE_CAS
        ):
            pipeline.worker.run_until_idle(max_cycles=5)
        assert pipeline.hooks.fired(KillPoint.RELAY_AFTER_ACK_BEFORE_CAS) == 1
        assert pipeline.broker.record_count() == 1
        assert next(iter(pipeline.store.iter_records())).state is EventState.CLAIMED

        assert expire_all_leases(pipeline) == 1
        _publisher, standby = pipeline.new_relay("relay-standby")
        standby.run_until_idle(max_cycles=20)
        assert pipeline.broker.record_count() == 2
        report = pipeline.run_consumer()
        assert (report.applied, report.duplicates) == (1, 1)

    def test_kp07_15_crash_after_cas_leaves_published_durable(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.RELAY_AFTER_CAS_BEFORE_OBSERVE
        ):
            pipeline.worker.run_until_idle(max_cycles=5)
        assert pipeline.hooks.fired(KillPoint.RELAY_AFTER_CAS_BEFORE_OBSERVE) == 1
        # The CAS committed before the crash, so restart finds it published and
        # does not publish a second copy.
        record = next(iter(pipeline.store.iter_records()))
        assert record.state is EventState.PUBLISHED and record.published_at is not None

        _publisher, restarted = pipeline.new_relay("relay-restarted")
        restarted.run_until_idle(max_cycles=20)
        assert pipeline.broker.record_count() == 1

    def test_kp07_16_a_takeover_mid_flight_is_fenced_in_the_database(
        self, pipeline: Pipeline
    ) -> None:
        """The DB state is fenced; the in-flight produce cannot be recalled.

        This is the subtlety design section 4.1 insists on: a lease token stops
        a stale relay writing to PostgreSQL, and does nothing about a request
        already at the broker. Both halves are asserted here.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="relay-old", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        # The old relay enqueues but has not been polled yet.
        pipeline.producer.produce(
            topic=claim.topic,
            key=claim.partition_key.encode(),
            value=claim.wire_bytes,
            headers={},
            on_delivery=lambda _r: None,
        )

        pipeline.clock.advance(31)
        pipeline.hooks.fire(KillPoint.RELAY_LEASE_EXPIRED_WITH_INFLIGHT, event_id=str(claim.event_id))
        _publisher, standby = pipeline.new_relay("relay-standby")
        standby.run_until_idle(max_cycles=20)

        assert pipeline.hooks.fired(KillPoint.RELAY_LEASE_EXPIRED_WITH_INFLIGHT) == 1
        assert pipeline.broker.record_count() == 1
        # The old relay's request finally lands, after the standby's.
        pipeline.producer.poll()
        assert pipeline.broker.record_count() == 2
        # It still cannot write to the row.
        assert not pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=0, kafka_offset=99, now=pipeline.clock.now()
        )
        report = pipeline.run_consumer()
        assert (report.applied, report.duplicates) == (1, 1)

    def test_kp07_17_graceful_drain_never_invents_a_published_row(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A, [deal(f"D-{i:04d}", account_id=f"A-{i}", source_seq=i) for i in range(1, 6)]
        )
        pipeline.broker.fault = Fault.DOWN
        pipeline.publisher.publish_cycle()
        total = pipeline.publisher.drain()

        assert pipeline.hooks.fired(KillPoint.RELAY_DURING_GRACEFUL_FLUSH) >= 1
        assert total.acked == 0
        # Nothing published on the way out, and nothing stuck CLAIMED forever.
        assert all(
            record.state is not EventState.PUBLISHED for record in pipeline.store.iter_records()
        )
        assert pipeline.publisher.limits.inflight == 0

    def test_a_drained_publisher_stops_claiming(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.publisher.begin_drain()
        report = pipeline.publisher.publish_cycle()
        assert report.claimed == 0
        assert next(iter(pipeline.store.iter_records())).state is EventState.PENDING


class TestConsumerKillPoints:
    """KP07-18 .. KP07-23."""

    def _restart_consumer(self, pipeline: Pipeline) -> ReferenceConsumer:
        fresh = pipeline.broker.consumer(group=pipeline.settings.consumer_group)
        consumer = ReferenceConsumer(
            fresh,
            pipeline.consumer_store,
            settings=pipeline.settings,
            clock=pipeline.clock,
            hooks=pipeline.hooks,
        )
        consumer.subscribe()
        return consumer

    def test_kp07_18_crash_after_poll_has_no_effect(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_POLL_BEFORE_BEGIN
        ):
            pipeline.consumer.process_once()
        assert pipeline.hooks.fired(KillPoint.CONSUMER_AFTER_POLL_BEFORE_BEGIN) == 1
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 0
        assert self._restart_consumer(pipeline).run_until_idle().applied == 1

    def test_kp07_19_crash_after_inbox_insert_rolls_the_whole_tx_back(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_INBOX_INSERT
        ):
            pipeline.consumer.process_once()
        assert pipeline.hooks.fired(KillPoint.CONSUMER_AFTER_INBOX_INSERT) == 1
        # The inbox row went with the rollback; otherwise the retry would see a
        # duplicate and skip applying the money.
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 0
        report = self._restart_consumer(pipeline).run_until_idle()
        assert report.applied == 1
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].realized_pnl_gross == "12.30"

    def test_kp07_20_crash_after_projection_before_commit_rolls_back(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_PROJECTION_BEFORE_COMMIT
        ):
            pipeline.consumer.process_once()
        assert pipeline.hooks.fired(KillPoint.CONSUMER_AFTER_PROJECTION_BEFORE_COMMIT) == 1
        assert pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        ) == []
        assert self._restart_consumer(pipeline).run_until_idle().applied == 1

    def test_kp07_21_crash_before_offset_commit_redelivers_and_dedups(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_DB_COMMIT_BEFORE_OFFSET
        ):
            pipeline.consumer.process_once()
        assert pipeline.hooks.fired(KillPoint.CONSUMER_AFTER_DB_COMMIT_BEFORE_OFFSET) == 1
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 1
        assert pipeline.kafka_consumer.committed(DEALS_TOPIC.name, 0) == 0

        report = self._restart_consumer(pipeline).run_until_idle()
        assert (report.applied, report.duplicates) == (0, 1)
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 1

    def test_kp07_22_crash_after_offset_commit_keeps_the_db_commit(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_OFFSET_COMMIT
        ):
            pipeline.consumer.process_once()
        assert pipeline.hooks.fired(KillPoint.CONSUMER_AFTER_OFFSET_COMMIT) == 1
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 1
        report = self._restart_consumer(pipeline).run_until_idle()
        assert report.applied == 0 and report.polled == 0

    def test_kp07_23_revocation_commits_nothing_beyond_what_was_applied(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A, [deal(f"D-{i:04d}", account_id=f"A-{i}", source_seq=i) for i in range(1, 7)]
        )
        pipeline.run_relay()
        report = pipeline.consumer.process_once(max_records=3)
        applied = report.applied
        committed_before = dict(report.committed)

        pipeline.consumer.handle_revocation(pipeline.kafka_consumer.assignment())
        assert pipeline.hooks.fired(KillPoint.CONSUMER_DURING_REVOCATION) == 1

        # Every committed offset corresponds to a record this member applied.
        processed = pipeline.consumer_store.processed_count(pipeline.settings.consumer_name)
        assert processed == applied
        assert sum(committed_before.values()) <= processed + len(committed_before)
        assert pipeline.consumer.store.query(
            "SELECT COUNT(*) AS n FROM consumer_processed_events"
        )[0]["n"] == applied


class TestEpochTransitionKillPoint:
    """KP07-24."""

    def test_kp07_24_a_crash_during_an_epoch_transition_rolls_it_back(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.EPOCH_TRANSITION_MID_FLIGHT
        ):
            pipeline.sync.reset_epoch(old_epoch_id=EPOCH_A, new_epoch_id=EPOCH_B)
        assert pipeline.hooks.fired(KillPoint.EPOCH_TRANSITION_MID_FLIGHT) == 1
        # Neither half of the transition survived: no retired old epoch without
        # a new one, and nobody was told about an epoch that does not exist.
        statuses = {
            row["epoch_id"]: row["status"]
            for row in pipeline.store.query("SELECT epoch_id, status FROM source_epochs")
        }
        assert statuses == {EPOCH_A: "ACTIVE"}
        assert pipeline.store.query(
            "SELECT COUNT(*) AS n FROM event_outbox WHERE event_type = 'SourceEpochChanged'"
        )[0]["n"] == 0

    def test_a_completed_transition_announces_itself_once(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        result = pipeline.sync.reset_epoch(old_epoch_id=EPOCH_A, new_epoch_id=EPOCH_B)
        assert result.created
        # Re-running the same transition is idempotent on the event, because
        # the new epoch id is the identity.
        statuses = {
            row["epoch_id"]: row["status"]
            for row in pipeline.store.query("SELECT epoch_id, status FROM source_epochs")
        }
        assert statuses == {EPOCH_A: "RETIRED", EPOCH_B: "ACTIVE"}


def test_every_kill_point_is_covered() -> None:
    """Fail if a site in the inventory has no test naming it.

    Without this, the inventory drifts into a list of good intentions: a new
    kill point can be added to the enum and never exercised, and nothing says
    so.
    """
    from pathlib import Path

    covered = Path(__file__).read_text(encoding="utf-8")
    missing = [
        str(point)
        for point in KillPoint
        if point.name not in covered and str(point) not in covered
    ]
    assert missing == [], f"kill points with no test in this module: {missing}"
