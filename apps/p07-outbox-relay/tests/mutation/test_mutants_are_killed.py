"""Deliberately broken implementations the suite must reject.

Design section 15.2 lists the mutations: "disable outbox INSERT, make PUBLISHED
before callback, remove consumer unique key, commit offset before DB, change
commission sign, remove epoch from dedupe key, skip stale-worker CAS, and
reverse application order. The suite **must fail** each mutated
implementation."

This is the test suite testing itself. Each case builds a mutant -- by patching
one function, or by performing the broken sequence by hand -- and asserts that
an oracle or an invariant catches it. A mutant that survives means the
corresponding real test is decorative: it would pass whether or not the
behaviour it names is implemented.

The failure message of a surviving mutant is therefore phrased as an
instruction about *the suite*, not about the code under test.
"""

from __future__ import annotations

import json
import uuid

import pytest

from outbox.canonical import payload_hash
from outbox.consumer.inbox import ProcessedOutcome
from outbox.errors import IntegrityIncident
from outbox.events.keys import EventType, build_dedupe_key, derive_event_id, hex_identity
from outbox.fixtures import EPOCH_A, EPOCH_B, deal
from outbox.oracles import (
    o1_deal_to_outbox_set_equality,
    o2_no_orphan_outbox_deal,
    o3_scan_marker_equivalence,
    o4_broker_and_consumer_sets,
    o5_order,
    o6_money_identity,
    o8_evidence_honesty,
)
from outbox.persistence.ports import EventState
from outbox.pipeline import Pipeline

pytestmark = pytest.mark.mutation


class TestMutantsInEventCreation:
    def test_a_deal_committed_without_its_outbox_row_is_caught_by_o1(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: the outbox INSERT is skipped.

        Simulated by inserting the Deal directly, which is exactly the state a
        non-transactional writer would leave behind.
        """
        from outbox.clock import to_wire

        fact = deal("D-9001")
        with pipeline.store.transaction() as tx:
            tx.execute(
                "INSERT INTO deals (epoch_id, deal_id, account_id, position_id, symbol, side, "
                "entry_kind, volume_lots, price, realized_pnl_gross, commission, swap, "
                "money_currency, source_seq, executed_at, payload_hash) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    EPOCH_A,
                    fact.deal_id,
                    fact.account_id,
                    fact.position_id,
                    fact.symbol,
                    fact.side,
                    fact.entry_kind,
                    fact.volume_lots,
                    fact.price,
                    fact.realized_pnl_gross,
                    fact.commission,
                    fact.swap,
                    "USD",
                    fact.source_seq,
                    fact.executed_at,
                    fact.business_hash,
                ),
            )
        result = o1_deal_to_outbox_set_equality(pipeline.store, epoch_id=EPOCH_A)
        assert not result.ok, "MUTANT SURVIVED: O1 does not detect a Deal with no event"
        assert "D-9001" in " ".join(result.details)

    def test_an_event_with_no_deal_is_caught_by_o2(self, pipeline: Pipeline) -> None:
        """Mutant: a fabricated DealRecorded. O2 must find the orphan."""
        from outbox.clock import to_wire

        dedupe = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, "D-GHOST")
        payload = {
            "deal_id": "D-GHOST",
            "account_id": "A-42",
            "position_id": None,
            "symbol": "EURUSD",
            "side": "BUY",
            "entry_kind": "OUT",
            "volume_lots": "0.10",
            "price": "1.10500",
            "realized_pnl_gross": "12.30",
            "commission": "-0.50",
            "swap": "-0.10",
            "money_currency": "USD",
            "money_currency_status": "PROVEN",
            "executed_at": "2026-10-30T05:59:59.000000Z",
        }
        with pipeline.store.transaction() as tx:
            tx.execute(
                "INSERT INTO event_outbox (event_id, source_id, epoch_id, dedupe_key, "
                "aggregate_type, aggregate_key, aggregate_version, event_type, schema_version, "
                "topic, partition_key, payload, payload_sha256, wire_bytes, state, created_at, "
                "next_attempt_at) VALUES (?, ?, ?, ?, 'account', 'A-42', 1, 'DealRecorded', 1, "
                "'broker.deals.v1', 'k', ?, ?, ?, 'PENDING', ?, ?)",
                (
                    str(derive_event_id(pipeline.source_id, dedupe)),
                    pipeline.source_id,
                    EPOCH_A,
                    dedupe,
                    json.dumps(payload, sort_keys=True, separators=(",", ":")),
                    payload_hash(payload),
                    b"{}",
                    to_wire(pipeline.clock.now()),
                    to_wire(pipeline.clock.now()),
                ),
            )
        result = o2_no_orphan_outbox_deal(pipeline.store, epoch_id=EPOCH_A)
        assert not result.ok, "MUTANT SURVIVED: O2 does not detect an event with no Deal"
        assert "no Deal row" in " ".join(result.details)

    def test_a_changed_commission_in_the_event_is_caught_by_o2(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: the sign or magnitude of a fee is altered on the way out.

        The outbox row's own hash stays self-consistent, so only a comparison
        against the *source* catches it. That is why O2 compares amounts and not
        just existence.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", commission="-0.50")])
        with pipeline.store.transaction() as tx:
            tx.execute(
                "UPDATE deals SET commission = '0.50' WHERE epoch_id = ? AND deal_id = ?",
                (EPOCH_A, "D-1001"),
            )
        result = o2_no_orphan_outbox_deal(pipeline.store, epoch_id=EPOCH_A)
        assert not result.ok, "MUTANT SURVIVED: O2 does not compare monetary amounts"
        assert "commission" in " ".join(result.details)

    def test_a_marker_for_an_unpublished_scan_is_caught_by_o3(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: the writer emits a marker for a STAGED scan."""
        pipeline.sync.stage_scan(
            epoch_id=EPOCH_A,
            entity="accounts",
            scan_id="scan-1",
            snapshot_seq=1,
            record_ids=["A-1"],
            lease_token="lease-1",
        )
        pipeline.sync.publish_scan(
            epoch_id=EPOCH_A, entity="accounts", scan_id="scan-1", lease_token="lease-1"
        )
        assert o3_scan_marker_equivalence(pipeline.store, epoch_id=EPOCH_A).ok

        # Now walk the scan back to STAGED while its marker remains.
        with pipeline.store.transaction() as tx:
            tx.execute("UPDATE sync_runs SET status = 'STAGED' WHERE scan_id = ?", ("scan-1",))
        result = o3_scan_marker_equivalence(pipeline.store, epoch_id=EPOCH_A)
        assert not result.ok, "MUTANT SURVIVED: O3 does not detect a marker for a staged scan"

    def test_dropping_the_epoch_from_the_dedupe_key_collides_across_a_reset(self) -> None:
        """Mutant: ``dedupe_key = f"{event_type}:{deal_id}"``.

        No database needed: the mutant is a pure function, and the bug is that
        two facts from different epochs collapse onto one key. With the epoch
        present they stay distinct, which is what INV07-10 requires.
        """

        def mutant_key(_event_type: object, _epoch_id: str, identity: str) -> str:
            return f"DealRecorded:{hex_identity(identity)}"

        assert mutant_key(None, EPOCH_A, "D-1001") == mutant_key(None, EPOCH_B, "D-1001"), (
            "the mutant is not actually broken"
        )
        correct_a = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, "D-1001")
        correct_b = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_B, "D-1001")
        assert correct_a != correct_b, (
            "MUTANT SURVIVED: the dedupe key does not separate epochs, so a source reset "
            "would merge two different facts under one identity"
        )
        assert derive_event_id("s", correct_a) != derive_event_id("s", correct_b)

    def test_the_database_itself_rejects_a_duplicate_version(self, pipeline: Pipeline) -> None:
        """Mutant: the writer allocates a version without the head lock.

        The unique index is the backstop, so this asserts the backstop exists
        rather than trusting the writer's ordering.
        """
        import sqlite3

        from outbox.clock import to_wire

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", account_id="A-42")])
        with pytest.raises(sqlite3.IntegrityError):
            with pipeline.store.transaction() as tx:
                tx.execute(
                    "INSERT INTO event_outbox (event_id, source_id, epoch_id, dedupe_key, "
                    "aggregate_type, aggregate_key, aggregate_version, event_type, schema_version, "
                    "topic, partition_key, payload, payload_sha256, wire_bytes, state, created_at, "
                    "next_attempt_at) VALUES (?, ?, ?, ?, 'account', 'A-42', 1, 'DealRecorded', 1, "
                    "'broker.deals.v1', 'k', '{}', ?, ?, 'PENDING', ?, ?)",
                    (
                        str(uuid.uuid4()),
                        pipeline.source_id,
                        EPOCH_A,
                        build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, "D-OTHER"),
                        "a" * 64,
                        b"{}",
                        to_wire(pipeline.clock.now()),
                        to_wire(pipeline.clock.now()),
                    ),
                )


class TestMutantsInTheRelay:
    def test_marking_published_before_the_callback_is_caught_by_o4(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: ``PUBLISHED`` is written after ``produce()`` returns.

        The row then claims the broker holds a record it never received, which
        is the single most dangerous available misreading of the client API.
        O4 catches it by comparing the published set with the broker log.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="relay-mutant", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        # The mutant enqueues and immediately declares success.
        pipeline.producer.produce(
            topic=claim.topic,
            key=claim.partition_key.encode(),
            value=claim.wire_bytes,
            headers={},
            on_delivery=lambda _r: None,
        )
        assert pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
        )
        assert pipeline.broker.record_count() == 0

        result = o4_broker_and_consumer_sets(
            pipeline.store,
            pipeline.broker,
            pipeline.consumer_store,
            consumer_name=pipeline.settings.consumer_name,
        )
        assert not result.ok, (
            "MUTANT SURVIVED: O4 does not detect a PUBLISHED row that the broker never received"
        )
        assert "absent from the broker log" in " ".join(result.details)

    def test_skipping_the_stale_worker_cas_would_let_a_fenced_relay_write(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: the lifecycle UPDATE drops its token/version predicate.

        Expressed as the difference between the real CAS (refuses) and the
        unconditional UPDATE a mutant would issue (succeeds). If the real CAS
        ever started succeeding here, a resumed stale relay could overwrite the
        new owner's state.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        stale = pipeline.store.claim_due(
            owner="relay-old", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.clock.advance(31)
        fresh = pipeline.store.claim_due(
            owner="relay-new", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]

        assert not pipeline.store.mark_published_if_owner(
            claim=stale, kafka_partition=0, kafka_offset=1, now=pipeline.clock.now()
        ), "MUTANT SURVIVED: the CAS does not fence a stale claim"

        # What the mutant would do instead, shown to be destructive.
        with pipeline.store.transaction() as tx:
            tx.execute(
                "UPDATE event_outbox SET state = 'PUBLISHED', published_at = ?, claim_owner = NULL, "
                "claim_token = NULL, lease_until = NULL WHERE outbox_id = ?",
                ("2026-10-30T06:00:00.000000Z", stale.outbox_id),
            )
        record = pipeline.store.get(stale.event_id)
        assert record is not None and record.state is EventState.PUBLISHED
        # And the legitimate owner can now no longer complete its own work.
        assert not pipeline.store.mark_published_if_owner(
            claim=fresh, kafka_partition=0, kafka_offset=2, now=pipeline.clock.now()
        )

    def test_an_unbounded_claim_loop_is_caught_by_the_inflight_cap(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: claim everything into memory, then wait for Kafka."""
        from outbox.fixtures import spread_load
        from outbox.relay.limits import InFlightLimits

        pipeline.sync.insert_deals(EPOCH_A, spread_load(80, accounts=80))
        limits = InFlightLimits(
            max_inflight=pipeline.settings.max_inflight,
            max_buffer_bytes=pipeline.settings.producer_buffer_max_bytes,
            max_event_bytes=pipeline.settings.max_event_wire_bytes,
        )
        admitted = 0
        for index in range(80):
            if limits.admit().value != "OK":
                break
            limits.acquire(aggregate_key=f"A-{index}", size_bytes=1000)
            admitted += 1
        assert admitted < 80, (
            "MUTANT SURVIVED: the admission check does not bound how many rows a cycle claims"
        )
        assert admitted <= pipeline.settings.max_inflight


class TestMutantsInTheConsumer:
    def test_removing_the_inbox_unique_key_would_double_the_money(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: no primary key on ``(consumer_name, event_id)``.

        Shown by applying the same payload twice through the projection
        directly, bypassing the inbox check, and comparing with the source.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", realized="12.30")])
        pipeline.pump()
        before = o6_money_identity(
            pipeline.store,
            pipeline.consumer_store,
            consumer_name=pipeline.settings.consumer_name,
            source_id=pipeline.source_id,
            epoch_id=EPOCH_A,
        )
        assert before.ok

        record = next(iter(pipeline.store.iter_records()))
        with pipeline.consumer_store.transaction() as conn:
            pipeline.consumer_store.apply_deal(
                conn,
                consumer_name=pipeline.settings.consumer_name,
                source_id=pipeline.source_id,
                epoch_id=EPOCH_A,
                event_id=record.event_id,
                payload=record.payload,
                now=pipeline.clock.now(),
            )

        after = o6_money_identity(
            pipeline.store,
            pipeline.consumer_store,
            consumer_name=pipeline.settings.consumer_name,
            source_id=pipeline.source_id,
            epoch_id=EPOCH_A,
        )
        assert not after.ok, (
            "MUTANT SURVIVED: O6 does not detect a doubled financial delta"
        )
        assert "realized_pnl_gross" in " ".join(after.details)

    def test_the_inbox_actually_distinguishes_a_hash_conflict(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: treat any duplicate event_id as a harmless redelivery.

        A duplicate with a *different* hash is not a redelivery, it is two
        records disagreeing about one fact. The store must report the two cases
        differently or the consumer cannot act differently.
        """
        event_id = uuid.uuid4()
        common = {
            "consumer_name": "c",
            "source_id": "s",
            "epoch_id": EPOCH_A,
            "aggregate_key": "A-42",
            "aggregate_version": 1,
            "topic": "broker.deals.v1",
            "partition": 0,
            "offset": 0,
            "now": pipeline.clock.now(),
        }
        with pipeline.consumer_store.transaction() as conn:
            first = pipeline.consumer_store.insert_processed(
                conn, event_id=event_id, payload_sha256="a" * 64, **common
            )
            same = pipeline.consumer_store.insert_processed(
                conn, event_id=event_id, payload_sha256="a" * 64, **common
            )
            different = pipeline.consumer_store.insert_processed(
                conn, event_id=event_id, payload_sha256="b" * 64, **common
            )
        assert first is ProcessedOutcome.NEW
        assert same is ProcessedOutcome.DUPLICATE_SAME_HASH
        assert different is ProcessedOutcome.DUPLICATE_DIFFERENT_HASH, (
            "MUTANT SURVIVED: the inbox cannot tell a redelivery from an integrity conflict"
        )

    def test_committing_the_offset_before_the_database_loses_data(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: commit the Kafka offset first, then apply.

        A crash between the two loses the record permanently, because the
        offset says it was handled. Design section 8.2 marks this a P0 test
        that must fail, so the mutant is performed explicitly and the loss
        demonstrated.
        """
        from outbox.events.routing import DEALS_TOPIC

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        records = pipeline.kafka_consumer.poll(max_records=5)
        assert len(records) == 1
        record = records[0]

        # The mutant's order: offset first.
        pipeline.kafka_consumer.commit({record.topic_partition: record.offset + 1})
        # ...and then the process dies before the database write.

        fresh = pipeline.broker.consumer(group=pipeline.settings.consumer_group)
        fresh.subscribe([DEALS_TOPIC.name])
        assert fresh.poll(max_records=5) == [], (
            "MUTANT SURVIVED: a committed offset should hide the record from a restart"
        )
        # The event is in the broker, the offset says it is done, and nothing
        # was ever applied: silent loss.
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 0
        result = o4_broker_and_consumer_sets(
            pipeline.store,
            pipeline.broker,
            pipeline.consumer_store,
            consumer_name=pipeline.settings.consumer_name,
        )
        assert not result.ok, "MUTANT SURVIVED: O4 does not detect a never-processed event"
        assert "never processed" in " ".join(result.details)

    def test_applying_versions_out_of_order_is_caught_by_o5(self, pipeline: Pipeline) -> None:
        """Mutant: apply whatever arrives, ignoring the version ledger."""
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{i:04d}", account_id="A-42", source_seq=i) for i in range(1, 4)],
        )
        pipeline.pump()
        assert o5_order(pipeline.consumer_store, consumer_name=pipeline.settings.consumer_name).ok

        # The mutant's footprint: a version applied with a hole beneath it.
        with pipeline.consumer_store.transaction() as conn:
            conn.execute(
                "UPDATE consumer_aggregate_versions SET last_applied_version = 5 "
                "WHERE consumer_name = ? AND aggregate_key = 'A-42'",
                (pipeline.settings.consumer_name,),
            )
        result = o5_order(pipeline.consumer_store, consumer_name=pipeline.settings.consumer_name)
        assert not result.ok, "MUTANT SURVIVED: O5 does not detect a non-contiguous version stream"

    def test_the_version_guard_refuses_a_non_sequential_advance(
        self, pipeline: Pipeline
    ) -> None:
        """Mutant: ``SET last_applied_version = ?`` with no guard."""
        with pipeline.consumer_store.transaction() as conn:
            pipeline.consumer_store.version_state(
                conn,
                consumer_name="c",
                source_id="s",
                epoch_id=EPOCH_A,
                aggregate_key="A-42",
            )
            with pytest.raises(IntegrityIncident, match="no longer holds the version"):
                pipeline.consumer_store.advance_version(
                    conn,
                    consumer_name="c",
                    source_id="s",
                    epoch_id=EPOCH_A,
                    aggregate_key="A-42",
                    version=4,
                )


class TestTheHonestyOracleItself:
    def test_a_campaign_that_never_fired_its_fault_cannot_pass(self, pipeline: Pipeline) -> None:
        """Mutant: report PASS without triggering the declared fault.

        Without O8 this is the most dangerous mutant of all, because the
        resulting suite is green and tests the happy path only.
        """
        result = o8_evidence_honesty(
            pipeline.hooks,
            required_kill_points=("KP07-14", "KP07-21"),
        )
        assert not result.ok, "MUTANT SURVIVED: O8 accepts a fault that never fired"
        assert set(result.details) == {"KP07-14", "KP07-21"}
