"""IT07-01 .. IT07-20, the given/when/then contract tests from design section 14.4.

One test per numbered contract, named after it, so a failure maps straight back
to the clause it violates. Each one ends with an assertion about *durable
state* rather than about a return value: the question these answer is "what is
in the database and the broker after this happened", which is the only question
an incident review can ask.
"""

from __future__ import annotations

import json

import pytest

from outbox.broker.embedded import Fault, OffsetOutOfRange
from outbox.consumer.reference import GAP_BLOCKED
from outbox.errors import EpochBlocked, IntegrityIncident, TenantForbidden
from outbox.events.keys import EventType, build_dedupe_key
from outbox.events.routing import DEALS_TOPIC
from outbox.faults.hooks import CrashSignal, KillPoint
from outbox.fixtures import EPOCH_A, EPOCH_B, deal, unproven_currency_deal
from outbox.money import MoneyCurrency
from outbox.oracles import assert_all, run_core_suite
from outbox.persistence.ports import EventState
from outbox.pipeline import Pipeline, build_pipeline
from tests.conftest import broker_event_ids, expire_all_leases, published_event_ids

pytestmark = pytest.mark.contract


def _deal_count(pipeline: Pipeline, epoch: str = EPOCH_A) -> int:
    return int(
        pipeline.store.query("SELECT COUNT(*) AS n FROM deals WHERE epoch_id = ?", (epoch,))[0]["n"]
    )


def _outbox_count(pipeline: Pipeline, epoch: str = EPOCH_A) -> int:
    return int(
        pipeline.store.query("SELECT COUNT(*) AS n FROM event_outbox WHERE epoch_id = ?", (epoch,))[0][
            "n"
        ]
    )


class TestAtomicity:
    def test_it07_01_business_tx_rollback_leaves_neither_row(self, pipeline: Pipeline) -> None:
        """Given a fault immediately before the outbox INSERT, when the
        transaction aborts, then neither the Deal nor the event exists."""
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_AFTER_HEAD_BUMP):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])

        assert pipeline.hooks.fired(KillPoint.P03_AFTER_HEAD_BUMP) == 1
        assert _deal_count(pipeline) == 0
        assert _outbox_count(pipeline) == 0
        assert pipeline.broker.record_count() == 0
        # The aggregate head must roll back too, or the next event would be
        # version 2 with no version 1 and the consumer would block forever.
        assert pipeline.store.aggregate_head(pipeline.source_id, EPOCH_A, "A-42") == 0

    def test_it07_01b_rollback_after_outbox_insert_also_loses_the_deal(
        self, pipeline: Pipeline
    ) -> None:
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(KillPoint.P03_AFTER_OUTBOX_INSERT):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        assert _deal_count(pipeline) == 0
        assert _outbox_count(pipeline) == 0

    def test_it07_02_atomic_commit_gives_set_equality(self, pipeline: Pipeline) -> None:
        """Given a clean epoch, when a Deal batch completes, then the inserted
        Deal set equals the logical DealRecorded set."""
        batch = [deal(f"D-{index:04d}", account_id=f"A-{index % 3}", source_seq=index) for index in range(1, 13)]
        outcomes = pipeline.sync.insert_deals(EPOCH_A, batch)

        assert all(outcome.inserted for outcome in outcomes)
        assert _deal_count(pipeline) == 12
        assert _outbox_count(pipeline) == 12
        pipeline.pump()
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

    def test_it07_02b_duplicates_do_not_allocate_extra_versions(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001"), deal("D-1002", source_seq=2)])
        head_before = pipeline.store.aggregate_head(pipeline.source_id, EPOCH_A, "A-42")

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001"), deal("D-1002", source_seq=2)])

        assert pipeline.store.aggregate_head(pipeline.source_id, EPOCH_A, "A-42") == head_before == 2
        assert _outbox_count(pipeline) == 2

    def test_it07_03_ambiguous_commit_does_not_create_a_second_event(
        self, pipeline: Pipeline
    ) -> None:
        """Given P03 reaches a durable COMMIT, when the client never sees the
        ACK, then the retry recognises the identity and emits nothing new."""
        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.P03_AFTER_COMMIT_BEFORE_ACK
        ):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])

        # The commit is durable even though the caller saw an error.
        assert _deal_count(pipeline) == 1
        assert _outbox_count(pipeline) == 1
        first = pipeline.store.get_by_dedupe(
            pipeline.source_id, EPOCH_A, build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, "D-1001")
        )
        assert first is not None

        outcomes = pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])

        assert outcomes[0].duplicate_of_identical is True
        assert _outbox_count(pipeline) == 1
        again = pipeline.store.get_by_dedupe(
            pipeline.source_id, EPOCH_A, build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, "D-1001")
        )
        assert again is not None
        assert (again.event_id, again.payload_sha256) == (first.event_id, first.payload_sha256)

    def test_it07_03b_a_changed_amount_under_one_identity_is_an_incident(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", commission="-0.50")])
        with pytest.raises(IntegrityIncident, match="immutable"):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", commission="-0.75")])
        # Neither version wins silently: the stored row is untouched.
        stored = pipeline.store.query(
            "SELECT commission FROM deals WHERE epoch_id = ? AND deal_id = ?", (EPOCH_A, "D-1001")
        )
        assert stored[0]["commission"] == "-0.50"
        assert _outbox_count(pipeline) == 1


class TestSnapshotMarkers:
    def test_it07_04_an_incomplete_scan_produces_no_marker(self, pipeline: Pipeline) -> None:
        """Given a scan that failed on its terminal page, when P03 aborts, then
        no marker exists and the previous current set stays authoritative."""
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="accounts",
            scan_id="scan-partial",
            snapshot_seq=100,
            record_ids=["A-1", "A-2"],
            lease_token="lease-1",
            complete=False,
        )
        with pytest.raises(IntegrityIncident, match="incomplete scan is never published"):
            pipeline.sync.publish_scan(
                epoch_id=EPOCH_A, entity="accounts", scan_id="scan-partial", lease_token="lease-1"
            )

        assert _outbox_count(pipeline) == 0
        assert (
            pipeline.store.query("SELECT COUNT(*) AS n FROM accounts_current")[0]["n"] == 0
        )
        status = pipeline.store.query(
            "SELECT status FROM sync_runs WHERE scan_id = ?", ("scan-partial",)
        )
        assert status[0]["status"] == "STAGED"

    def test_it07_04b_an_aborted_scan_cannot_be_marked(self, pipeline: Pipeline) -> None:
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="accounts",
            scan_id="scan-aborted",
            snapshot_seq=100,
            record_ids=["A-1"],
            lease_token="lease-1",
        )
        pipeline.sync.abort_scan(epoch_id=EPOCH_A, scan_id="scan-aborted")
        with pytest.raises(IntegrityIncident, match="not STAGED"):
            pipeline.sync.publish_scan(
                epoch_id=EPOCH_A, entity="accounts", scan_id="scan-aborted", lease_token="lease-1"
            )
        assert _outbox_count(pipeline) == 0

    def test_it07_05_a_complete_scan_yields_one_notification_only_marker(
        self, pipeline: Pipeline
    ) -> None:
        """Given P03 publishes a full Position scan under a matching lease, when
        the transaction commits, then the marker names that scan and is
        consumable as notification only."""
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="positions",
            scan_id="scan-pos-1",
            snapshot_seq=101,
            record_ids=["P-1", "P-2", "P-3"],
            lease_token="lease-1",
        )
        result = pipeline.sync.publish_scan(
            epoch_id=EPOCH_A, entity="positions", scan_id="scan-pos-1", lease_token="lease-1"
        )

        assert result.created
        assert result.envelope is not None
        payload = result.envelope.payload
        assert payload["notification_only"] is True
        assert payload["scan_id"] == "scan-pos-1"
        assert payload["record_count"] == 3
        # The marker is not the snapshot image: no position list travels.
        assert "positions" not in payload
        assert pipeline.store.query("SELECT COUNT(*) AS n FROM positions_current")[0]["n"] == 3

        pipeline.pump()
        notices = pipeline.consumer_store.snapshot_notices(pipeline.settings.consumer_name)
        assert [(row["entity"], row["scan_id"]) for row in notices] == [("positions", "scan-pos-1")]

    def test_it07_05b_a_fenced_worker_cannot_announce_a_publication(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="accounts",
            scan_id="scan-1",
            snapshot_seq=1,
            record_ids=["A-1"],
            lease_token="lease-held-by-someone-else",
        )
        with pytest.raises(IntegrityIncident, match="fenced"):
            pipeline.sync.publish_scan(
                epoch_id=EPOCH_A, entity="accounts", scan_id="scan-1", lease_token="lease-stale"
            )
        assert _outbox_count(pipeline) == 0

    def test_it07_05c_account_and_position_scans_do_not_form_a_common_cut(
        self, pipeline: Pipeline
    ) -> None:
        # Two independent scans publish two independent markers. Nothing claims
        # they represent one consistent snapshot of the broker.
        for entity, scan_id, records in (
            ("accounts", "scan-acc", ["A-1"]),
            ("positions", "scan-pos", ["P-1", "P-2"]),
        ):
            pipeline.sync.stage_scan(
                epoch_id=EPOCH_A,
                entity=entity,
                scan_id=scan_id,
                snapshot_seq=1,
                record_ids=records,
                lease_token="lease-1",
            )
            pipeline.sync.publish_scan(
                epoch_id=EPOCH_A, entity=entity, scan_id=scan_id, lease_token="lease-1"
            )
        rows = pipeline.store.query(
            "SELECT event_type, aggregate_key FROM event_outbox ORDER BY aggregate_key"
        )
        assert [(row["event_type"], row["aggregate_key"]) for row in rows] == [
            ("AccountSnapshotPublished", "accounts"),
            ("PositionSnapshotPublished", "positions"),
        ]


class TestRelayDelivery:
    def test_it07_06_ack_then_kill_gives_one_business_effect(self, pipeline: Pipeline) -> None:
        """Given one committed event, when the relay is killed after the broker
        ACK but before the DB CAS, then a standby may publish again and the
        consumer records exactly one effect."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])

        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.RELAY_AFTER_ACK_BEFORE_CAS
        ):
            pipeline.worker.run_until_idle(max_cycles=5)

        assert pipeline.hooks.fired(KillPoint.RELAY_AFTER_ACK_BEFORE_CAS) == 1
        # The record is in the broker, but the row is still CLAIMED.
        assert pipeline.broker.record_count() == 1
        assert published_event_ids(pipeline) == set()

        assert expire_all_leases(pipeline) == 1
        _standby_publisher, standby_worker = pipeline.new_relay("relay-standby")
        standby_worker.run_until_idle(max_cycles=20)

        ids = broker_event_ids(pipeline)
        assert len(ids) == 2, "the standby republished, which is legal at-least-once"
        assert len(set(ids)) == 1, "both copies carry the same derived event id"
        assert len(published_event_ids(pipeline)) == 1

        report = pipeline.run_consumer()
        assert report.applied == 1
        assert report.duplicates == 1
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 1
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 1
        assert projection[0].realized_pnl_gross == "12.30"

    def test_it07_07_a_resumed_stale_relay_cannot_overwrite_the_new_owner(
        self, pipeline: Pipeline
    ) -> None:
        """Given relay A holds a claim, when A stalls past its lease and B takes
        over, then A cannot mark PUBLISHED or overwrite B's claim state."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        stale_claims = pipeline.store.claim_due(
            owner="relay-a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )
        assert len(stale_claims) == 1
        stale = stale_claims[0]

        pipeline.clock.advance(31)
        fresh = pipeline.store.claim_due(
            owner="relay-b", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )
        assert len(fresh) == 1 and fresh[0].claim_version > stale.claim_version

        # A wakes up and tries to finish its work.
        assert not pipeline.store.mark_published_if_owner(
            claim=stale, kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
        )
        assert not pipeline.store.reschedule_if_owner(
            claim=stale, delay_seconds=1, error_code="X", now=pipeline.clock.now()
        )
        assert not pipeline.store.quarantine_if_owner(
            claim=stale, reason="r", error_code="X", now=pipeline.clock.now()
        )
        # B is still the owner, with its row untouched.
        record = pipeline.store.get(stale.event_id)
        assert record is not None
        assert record.state is EventState.CLAIMED
        assert record.claim_owner == "relay-b"

        assert pipeline.store.mark_published_if_owner(
            claim=fresh[0], kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
        )

    def test_it07_08_a_late_old_version_does_not_apply_out_of_order(
        self, pipeline: Pipeline
    ) -> None:
        """Given v1 is delayed while v2 arrives first, when the consumer
        processes, then v2 is not applied before v1 and the projection
        eventually reaches the correct state or an explicit blocked state."""
        pipeline.sync.insert_deals(
            EPOCH_A, [deal("D-1001", source_seq=1), deal("D-1002", source_seq=2)]
        )
        records = {
            record.aggregate_version: record for record in pipeline.store.iter_records()
        }
        v1, v2 = records[1], records[2]

        # Append v2 first, as a takeover between two producers can.
        producer = pipeline.broker.producer()
        for record in (v2, v1):
            producer.produce(
                topic=record.topic,
                key=record.partition_key.encode(),
                value=record.wire_bytes,
                headers={"event_id": str(record.event_id)},
                on_delivery=lambda _result: None,
            )
            producer.poll()

        first = pipeline.consumer.process_once()
        assert first.gaps == 1
        assert [outcome for _, outcome in first.outcomes] == [GAP_BLOCKED]
        blocked = pipeline.consumer_store.blocked_aggregates(pipeline.settings.consumer_name)
        assert len(blocked) == 1 and "version gap" in blocked[0]["blocked_reason"]
        # Nothing was applied, and nothing is in the inbox pretending to be.
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 0
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection == []

        # The partition stays halted: the offset was never advanced past v2.
        assert pipeline.consumer.store.query(
            "SELECT COUNT(*) AS n FROM consumer_processed_events"
        )[0]["n"] == 0

    def test_it07_09_broker_outage_preserves_the_durable_set(self, pipeline: Pipeline) -> None:
        """Given committed rows and an unavailable Kafka, when the relay runs,
        then the rows remain, memory stays bounded, and set equality holds once
        the broker returns."""
        batch = [deal(f"D-{index:04d}", account_id=f"A-{index % 7}", source_seq=index) for index in range(1, 61)]
        pipeline.sync.insert_deals(EPOCH_A, batch)

        pipeline.broker.fault = Fault.DOWN
        for _ in range(12):
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(1)

        during = pipeline.store.stats(now=pipeline.clock.now())
        assert during.published_total == 0
        assert during.pending_total + during.claimed_total == 60
        assert pipeline.broker.record_count() == 0
        assert pipeline.publisher.limits.inflight == 0
        assert pipeline.relay_metrics.value("relay_circuit_open_total") >= 1

        pipeline.broker.fault = Fault.NONE
        pipeline.clock.advance(300)
        pipeline.publisher.circuit.record_success()
        pipeline.worker.run_until_idle(max_cycles=20_000)
        pipeline.run_consumer()

        after = pipeline.store.stats(now=pipeline.clock.now())
        assert (after.pending_total, after.published_total) == (0, 60)
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

    def test_it07_10_an_unserved_callback_never_marks_published(self, pipeline: Pipeline) -> None:
        """Given the producer queue accepted a record but no callback has
        reported success, then the event is not PUBLISHED and stays eligible."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claims = pipeline.store.claim_due(
            owner="relay-1", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )
        claim = claims[0]
        pipeline.producer.produce(
            topic=claim.topic,
            key=claim.partition_key.encode(),
            value=claim.wire_bytes,
            headers={},
            on_delivery=lambda _result: None,
        )

        # produce() enqueued. Nothing has been delivered and nothing polled.
        assert pipeline.producer.in_flight() == 1
        assert pipeline.producer.buffer_used_bytes() == len(claim.wire_bytes)
        assert pipeline.broker.record_count() == 0
        record = pipeline.store.get(claim.event_id)
        assert record is not None and record.state is EventState.CLAIMED
        assert record.published_at is None

        pipeline.clock.advance(31)
        reclaimed = pipeline.store.claim_due(
            owner="relay-2", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )
        assert len(reclaimed) == 1


class TestConsumerCrashes:
    def test_it07_11_crash_before_commit_applies_exactly_once_on_retry(
        self, pipeline: Pipeline
    ) -> None:
        """Given an event in Kafka, when the consumer dies after the inbox
        insert but before COMMIT, then the transaction rolls back and the retry
        applies it once."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()

        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_PROJECTION_BEFORE_COMMIT
        ):
            pipeline.consumer.process_once()

        assert pipeline.hooks.fired(KillPoint.CONSUMER_AFTER_PROJECTION_BEFORE_COMMIT) == 1
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 0
        assert pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        ) == []
        # The offset was never committed, so the record is redelivered.
        assert pipeline.kafka_consumer.committed(DEALS_TOPIC.name, 0) == 0

        fresh = pipeline.broker.consumer(group=pipeline.settings.consumer_group)
        from outbox.consumer.reference import ReferenceConsumer

        restarted = ReferenceConsumer(
            fresh,
            pipeline.consumer_store,
            settings=pipeline.settings,
            clock=pipeline.clock,
            hooks=pipeline.hooks,
        )
        restarted.subscribe()
        report = restarted.run_until_idle()
        assert report.applied == 1
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 1 and projection[0].realized_pnl_gross == "12.30"

    def test_it07_12_crash_after_db_commit_does_not_reapply_the_money(
        self, pipeline: Pipeline
    ) -> None:
        """Given the event is applied in the database, when the consumer dies
        before committing the offset, then the replay does not double the
        financial delta."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()

        with pytest.raises(CrashSignal), pipeline.hooks.crash_at(
            KillPoint.CONSUMER_AFTER_DB_COMMIT_BEFORE_OFFSET
        ):
            pipeline.consumer.process_once()

        # The database commit is durable; the offset commit never happened.
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 1
        assert pipeline.kafka_consumer.committed(DEALS_TOPIC.name, 0) == 0

        fresh = pipeline.broker.consumer(group=pipeline.settings.consumer_group)
        from outbox.consumer.reference import ReferenceConsumer

        restarted = ReferenceConsumer(
            fresh,
            pipeline.consumer_store,
            settings=pipeline.settings,
            clock=pipeline.clock,
            hooks=pipeline.hooks,
        )
        restarted.subscribe()
        report = restarted.run_until_idle()

        assert report.duplicates == 1 and report.applied == 0
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 1
        assert projection[0].realized_pnl_gross == "12.30"


class TestFinancialAndTenancy:
    def test_it07_13_an_unproven_currency_is_not_aggregated(self, pipeline: Pipeline) -> None:
        """Given a Deal without a verified calculation currency, when the event
        is built, then the status is explicitly unproven and no consumer may
        aggregate it as a currency amount."""
        pipeline.sync.insert_deals(EPOCH_A, [unproven_currency_deal("D-9001")])
        pipeline.pump()

        record = next(iter(pipeline.store.iter_records()))
        assert record.payload["money_currency_status"] == "UNPROVEN"
        assert record.payload["money_currency"] is None
        # The amounts themselves are still exact and still hashed.
        assert record.payload["realized_pnl_gross"] == "12.30"

        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 1
        assert projection[0].unproven_currency_deal_count == 1
        assert projection[0].money_currency is None
        assert projection[0].realized_pnl_gross == "0.00", "never invented a currency total"

    def test_it07_13b_mixing_two_proven_currencies_stops_the_aggregate(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A,
            [
                deal("D-1001", source_seq=1, currency=MoneyCurrency.proven("USD")),
                deal("D-1002", source_seq=2, currency=MoneyCurrency.proven("EUR")),
            ],
        )
        pipeline.run_relay()
        report = pipeline.consumer.run_until_idle()

        assert report.applied == 1
        assert report.quarantined == 1
        quarantined = pipeline.consumer_store.quarantined(pipeline.settings.consumer_name)
        assert "not money" in quarantined[0]["reason"]
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].money_currency == "USD"
        assert projection[0].deal_count == 1

    def test_it07_14_a_foreign_tenant_event_is_never_applied(self, tmp_path, clock, hooks) -> None:
        """Given a consumer authorised for broker A only, when broker B's event
        appears on the shared topic, then it is rejected and nothing of B's
        reaches A's projection."""
        broker_a = build_pipeline(
            root=tmp_path / "a",
            clock=clock,
            hooks=hooks,
            source_id="sim-broker-01",
            subscribe=False,
            synchronous="NORMAL"
        )
        broker_b = build_pipeline(
            root=tmp_path / "b",
            clock=clock,
            hooks=hooks,
            source_id="sim-broker-02",
            subscribe=False,
            synchronous="NORMAL"
        )
        try:
            broker_a.sync.open_epoch(EPOCH_A)
            broker_b.sync.open_epoch(EPOCH_A)
            # Deliberately the same account id in both tenants (fixture G07-17).
            broker_a.sync.insert_deals(EPOCH_A, [deal("D-1001", account_id="A-42")])
            broker_b.sync.insert_deals(EPOCH_A, [deal("D-1001", account_id="A-42")])

            a_record = next(iter(broker_a.store.iter_records()))
            b_record = next(iter(broker_b.store.iter_records()))
            # Different tenants, same account: different identities and keys.
            assert a_record.event_id != b_record.event_id
            assert a_record.partition_key != b_record.partition_key

            # Push both onto A's broker, as a shared topic would.
            producer = broker_a.broker.producer()
            for record in (a_record, b_record):
                producer.produce(
                    topic=record.topic,
                    key=record.partition_key.encode(),
                    value=record.wire_bytes,
                    headers={"event_id": str(record.event_id)},
                    on_delivery=lambda _r: None,
                )
            producer.poll()

            broker_a.consumer.subscribe()
            report = broker_a.consumer.run_until_idle()

            assert report.quarantined == 1
            quarantined = broker_a.consumer_store.quarantined(broker_a.settings.consumer_name)
            assert quarantined[0]["error_code"] == "TENANT_FORBIDDEN"
            assert broker_a.consumer_store.processed_event_ids(
                broker_a.settings.consumer_name
            ) <= {str(a_record.event_id)}
        finally:
            broker_a.close()
            broker_b.close()

    def test_it07_14b_the_writer_refuses_an_unlisted_source(self, pipeline: Pipeline) -> None:
        with pipeline.store.transaction() as tx:
            with pytest.raises(TenantForbidden, match="allowlist"):
                pipeline.writer.record_deal(
                    tx, source_id="sim-broker-99", epoch_id=EPOCH_A, deal=deal("D-1001")
                )

    def test_it07_14c_an_epoch_belonging_to_no_source_is_refused(self, pipeline: Pipeline) -> None:
        with pipeline.store.transaction() as tx:
            with pytest.raises(TenantForbidden, match="not registered"):
                pipeline.writer.record_deal(
                    tx, source_id=pipeline.source_id, epoch_id=EPOCH_B, deal=deal("D-1001")
                )


class TestPoisonAndRetention:
    def test_it07_15_a_poison_record_blocks_its_aggregate(self, pipeline: Pipeline) -> None:
        """Given an invalid record E1 and a valid E2 for the same aggregate,
        when the consumer sees both, then E1 is visibly quarantined and E2 is
        not applied ahead of it."""
        pipeline.sync.insert_deals(
            EPOCH_A, [deal("D-1001", source_seq=1), deal("D-1002", source_seq=2)]
        )
        records = {record.aggregate_version: record for record in pipeline.store.iter_records()}

        producer = pipeline.broker.producer()
        # E1 corrupted in flight: the payload no longer matches its hash.
        corrupted = records[1].wire_bytes.replace(b'"12.30"', b'"99.30"')
        for payload, record in ((corrupted, records[1]), (records[2].wire_bytes, records[2])):
            producer.produce(
                topic=record.topic,
                key=record.partition_key.encode(),
                value=payload,
                headers={},
                on_delivery=lambda _r: None,
            )
            producer.poll()

        report = pipeline.consumer.run_until_idle()

        assert report.quarantined == 1
        assert report.applied == 0, "E2 must not be applied ahead of the blocked E1"
        quarantined = pipeline.consumer_store.quarantined(pipeline.settings.consumer_name)
        assert quarantined[0]["error_code"] == "INTEGRITY_CONFLICT"
        assert pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        ) == []

    def test_it07_15b_an_unrelated_aggregate_keeps_making_progress(
        self, pipeline: Pipeline
    ) -> None:
        # Fairness: one poisoned account must not stop the others.
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal("D-1001", account_id="A-1", source_seq=1), deal("D-2001", account_id="A-2", source_seq=2)],
        )
        records = {record.aggregate_key: record for record in pipeline.store.iter_records()}
        producer = pipeline.broker.producer()
        payloads = [
            (records["A-1"], records["A-1"].wire_bytes.replace(b'"12.30"', b'"99.30"')),
            (records["A-2"], records["A-2"].wire_bytes),
        ]
        for record, payload in payloads:
            producer.produce(
                topic=record.topic,
                key=record.partition_key.encode(),
                value=payload,
                headers={},
                on_delivery=lambda _r: None,
            )
            producer.poll()

        report = pipeline.consumer.run_until_idle()
        assert report.quarantined == 1
        # A-1 and A-2 hash to different partitions, so A-2 is not behind A-1.
        if records["A-1"].partition_key != records["A-2"].partition_key:
            from outbox.events.routing import partition_for_key

            a1 = partition_for_key(records["A-1"].partition_key, DEALS_TOPIC.partitions)
            a2 = partition_for_key(records["A-2"].partition_key, DEALS_TOPIC.partitions)
            if a1 != a2:
                assert report.applied == 1

    def test_it07_16_a_consumer_past_retention_cannot_skip_the_gap(
        self, pipeline: Pipeline
    ) -> None:
        """Given a consumer inactive beyond Kafka retention, when it returns,
        then it must not jump to the latest offset and report success."""
        pipeline.sync.insert_deals(
            EPOCH_A, [deal(f"D-{index:04d}", account_id=f"A-{index}", source_seq=index) for index in range(1, 7)]
        )
        pipeline.run_relay()
        assert pipeline.broker.record_count() == 6

        # Expire the prefix of every partition that holds anything.
        expired = 0
        for partition in range(DEALS_TOPIC.partitions):
            watermark = pipeline.broker.high_watermark(DEALS_TOPIC.name, partition)
            if watermark:
                expired += pipeline.broker.expire_retention(
                    DEALS_TOPIC.name, partition, keep_from_offset=watermark
                )
        assert expired == 6

        fresh = pipeline.broker.consumer(group="group-returning-late")
        fresh.subscribe([DEALS_TOPIC.name])
        with pytest.raises(OffsetOutOfRange, match="removed by retention"):
            fresh.poll(max_records=10)


class TestResilienceAndEvolution:
    def test_it07_17_coupled_failover_converges_without_loss(self, pipeline: Pipeline) -> None:
        """Given a committed event, when the broker leader and the store both
        wobble while it is in flight, then retries converge with no loss and no
        duplicate business effect."""
        pipeline.sync.insert_deals(
            EPOCH_A, [deal(f"D-{index:04d}", account_id=f"A-{index % 4}", source_seq=index) for index in range(1, 21)]
        )

        faults = [Fault.LEADER_UNAVAILABLE, Fault.DROP_ACK, Fault.NOT_ENOUGH_REPLICAS, Fault.NONE]
        for fault in faults:
            pipeline.broker.fault = fault
            for _ in range(6):
                pipeline.publisher.publish_cycle()
                pipeline.clock.advance(5)

        pipeline.broker.fault = Fault.NONE
        pipeline.clock.advance(600)
        pipeline.publisher.circuit.record_success()
        pipeline.worker.run_until_idle(max_cycles=50_000)
        pipeline.run_consumer()

        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert (stats.pending_total, stats.published_total, stats.quarantined_total) == (0, 20, 0)
        assert pipeline.relay_metrics.value("outbox_ack_ambiguous_total") > 0
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
        # Duplicates in the log are expected; duplicate effects are not.
        assert len(broker_event_ids(pipeline)) >= 20
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 20

    def test_it07_18_an_unsupported_schema_version_is_quarantined_not_guessed(
        self, pipeline: Pipeline
    ) -> None:
        """Given a consumer whose allowlist is v1, when a v2 record arrives,
        then it is quarantined rather than interpreted."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        record = next(iter(pipeline.store.iter_records()))
        raw = json.loads(record.wire_bytes)
        raw["schema_version"] = 2

        producer = pipeline.broker.producer()
        producer.produce(
            topic=record.topic,
            key=record.partition_key.encode(),
            value=json.dumps(raw, sort_keys=True, separators=(",", ":")).encode(),
            headers={},
            on_delivery=lambda _r: None,
        )
        producer.poll()

        report = pipeline.consumer.run_until_idle()
        assert report.quarantined == 1 and report.applied == 0
        quarantined = pipeline.consumer_store.quarantined(pipeline.settings.consumer_name)
        assert quarantined[0]["error_code"] == "SCHEMA_UNSUPPORTED"

    def test_it07_19_a_large_backlog_stays_within_its_resource_budget(
        self, pipeline: Pipeline
    ) -> None:
        """Given a backlog and no broker, when the relay stays live, then
        claims, in-flight records and buffers stay inside their caps."""
        from outbox.fixtures import spread_load

        pipeline.sync.insert_deals(EPOCH_A, spread_load(400, accounts=40))
        pipeline.broker.fault = Fault.DOWN

        peak_claimed = 0
        for _ in range(40):
            pipeline.publisher.publish_cycle()
            pipeline.clock.advance(2)
            stats = pipeline.store.stats(now=pipeline.clock.now())
            peak_claimed = max(peak_claimed, stats.claimed_total)
            assert pipeline.publisher.limits.inflight <= pipeline.settings.max_inflight
            assert pipeline.producer.buffer_used_bytes() <= pipeline.settings.producer_buffer_max_bytes

        # Never 400 rows claimed into memory at once.
        assert peak_claimed <= pipeline.settings.max_inflight
        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert stats.pending_total + stats.claimed_total == 400
        assert stats.published_total == 0

    def test_it07_20_a_source_reset_keeps_the_two_epochs_separate(
        self, pipeline: Pipeline
    ) -> None:
        """Given an epoch with history, when the source resets and P03 adopts a
        new epoch, then the reused Deal id does not merge the two."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", realized="12.30")])
        pipeline.sync.reset_epoch(old_epoch_id=EPOCH_A, new_epoch_id=EPOCH_B)
        # The same deal id, a different fact, in the new epoch.
        pipeline.sync.insert_deals(EPOCH_B, [deal("D-1001", realized="50.00")])
        pipeline.pump()

        by_epoch: dict[str, list[str]] = {}
        for record in pipeline.store.iter_records():
            by_epoch.setdefault(record.epoch_id, []).append(record.dedupe_key)
        assert set(by_epoch) == {EPOCH_A, EPOCH_B}
        # Different dedupe keys and therefore different derived event ids.
        assert len({str(record.event_id) for record in pipeline.store.iter_records()}) == 3

        for epoch, expected in ((EPOCH_A, "12.30"), (EPOCH_B, "50.00")):
            projection = pipeline.consumer_store.projection(
                pipeline.settings.consumer_name, pipeline.source_id, epoch
            )
            assert projection[0].realized_pnl_gross == expected, f"{epoch} totals must not merge"

    def test_it07_20b_a_blocked_epoch_accepts_no_new_events(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.sync.set_epoch_status(EPOCH_A, "BLOCKED")
        with pytest.raises(EpochBlocked, match="new event creation is gated"):
            pipeline.sync.insert_deals(EPOCH_A, [deal("D-1002", source_seq=2)])
        # The already-committed event is history and is left alone.
        assert _outbox_count(pipeline) == 1
