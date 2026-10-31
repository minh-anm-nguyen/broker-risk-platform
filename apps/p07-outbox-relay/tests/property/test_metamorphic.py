"""Metamorphic properties: change the input in a way that should not matter.

Each test here names a transformation and asserts the output is unchanged. They
are the cheapest way to catch a whole class of bug that example-based tests
miss, because they do not require anyone to have guessed the right example --
only to have named the right equivalence.

The equivalences, and what each one protects:

* decimal spelling -> payload hash (two producers must agree byte for byte)
* delivery count -> business effect (at-least-once must not mean at-least-twice-paid)
* relay restart -> published set (replay changes delivery counts, not state)
* page split -> final projection (batching is an ingestion detail)
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest
from hypothesis import assume, given, settings as hyp_settings
from hypothesis.strategies import integers, lists, sampled_from, text

from outbox.canonical import payload_hash
from outbox.events.envelope import Envelope
from outbox.events.keys import EventType, build_dedupe_key, derive_event_id, hex_identity, unhex_identity
from outbox.events.schema import validate_payload
from outbox.fixtures import EPOCH_A, deal
from outbox.money import MoneyCurrency, normalize_money
from outbox.pipeline import Pipeline

pytestmark = pytest.mark.property

T0 = "2026-10-30T05:59:59.000000Z"


def _payload(**overrides: object) -> dict[str, object]:
    base: dict[str, object] = {
        "deal_id": "D-1001",
        "account_id": "A-42",
        "position_id": "P-9",
        "symbol": "EURUSD",
        "side": "BUY",
        "entry_kind": "OUT",
        "volume_lots": "0.10",
        "price": "1.10500",
        "realized_pnl_gross": "12.30",
        "commission": "-0.50",
        "swap": "-0.10",
        "executed_at": T0,
    }
    base.update(MoneyCurrency.proven("USD").as_payload_fields())
    base.update(overrides)
    return base


class TestHashEquivalences:
    @given(trailing=integers(min_value=0, max_value=6))
    def test_trailing_zeros_do_not_change_the_hash(self, trailing: int) -> None:
        """``"12.3"``, ``"12.30"`` and ``"12.300000"`` are one amount.

        Only spellings within the field's scale are equivalent; beyond it the
        value is rejected rather than rounded, which the unit tests cover.
        """
        spelled = "12.3" + "0" * trailing
        try:
            normalized = normalize_money(spelled, field="realized_pnl_gross")
        except Exception:
            assume(False)
            raise
        assert normalized == "12.30"
        assert payload_hash(validate_payload(EventType.DEAL_RECORDED, 1, _payload(
            realized_pnl_gross=spelled
        ))) == payload_hash(validate_payload(EventType.DEAL_RECORDED, 1, _payload()))

    @given(keys=lists(sampled_from(["a", "b", "c", "d"]), min_size=1, max_size=4, unique=True))
    def test_key_insertion_order_does_not_change_the_hash(self, keys: list[str]) -> None:
        forward = {key: index for index, key in enumerate(keys)}
        backward = {key: forward[key] for key in reversed(keys)}
        assert payload_hash(forward) == payload_hash(backward)

    @given(identity=text(min_size=1, max_size=60))
    def test_identity_encoding_round_trips(self, identity: str) -> None:
        assume("\x00" not in identity)
        assert unhex_identity(hex_identity(identity)) == identity

    @given(identity=text(min_size=1, max_size=40))
    def test_the_event_id_is_a_pure_function_of_the_semantic_key(self, identity: str) -> None:
        assume("\x00" not in identity)
        key = build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, identity)
        assert derive_event_id("sim-broker-01", key) == derive_event_id("sim-broker-01", key)

    @given(first=text(min_size=1, max_size=20), second=text(min_size=1, max_size=20))
    def test_distinct_identities_give_distinct_event_ids(self, first: str, second: str) -> None:
        assume("\x00" not in first and "\x00" not in second)
        assume(first != second)
        keys = [
            build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, value) for value in (first, second)
        ]
        assert derive_event_id("s", keys[0]) != derive_event_id("s", keys[1])

    @given(count=integers(min_value=1, max_value=5))
    def test_wire_serialisation_is_deterministic(self, count: int) -> None:
        envelope = Envelope.build(
            event_type=EventType.DEAL_RECORDED,
            schema_version=1,
            source_id="sim-broker-01",
            epoch_id=EPOCH_A,
            aggregate_id="A-42",
            aggregate_version=count,
            payload=_payload(),
            event_time=T0,
            created_at=T0,
            provenance={"source_seq": count},
            identity="D-1001",
        )
        encodings = {envelope.to_wire_bytes() for _ in range(5)}
        assert len(encodings) == 1


class TestPipelineEquivalences:
    @pytest.mark.parametrize("deliveries", [1, 2, 3, 5])
    def test_delivery_count_does_not_change_the_business_effect(
        self, pipeline: Pipeline, deliveries: int
    ) -> None:
        """At-least-once delivery, exactly-once effect (INV07-08)."""
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        record = next(iter(pipeline.store.iter_records()))

        producer = pipeline.broker.producer()
        for _ in range(deliveries - 1):
            producer.produce(
                topic=record.topic,
                key=record.partition_key.encode(),
                value=record.wire_bytes,
                headers={"event_id": str(record.event_id)},
                on_delivery=lambda _r: None,
            )
            producer.poll()
        assert pipeline.broker.record_count() == deliveries

        pipeline.run_consumer()
        projection = pipeline.consumer_store.projection(
            pipeline.settings.consumer_name, pipeline.source_id, EPOCH_A
        )
        assert projection[0].deal_count == 1
        assert projection[0].realized_pnl_gross == "12.30"
        assert pipeline.consumer_store.processed_count(pipeline.settings.consumer_name) == 1

    def test_restarting_the_relay_does_not_change_the_published_set(
        self, pipeline: Pipeline
    ) -> None:
        """Replay changes delivery counts, not business state (INV07-14)."""
        facts = [deal(f"D-{i:04d}", account_id=f"A-{i % 3}", source_seq=i) for i in range(1, 10)]
        pipeline.sync.insert_deals(EPOCH_A, facts)
        pipeline.run_relay()
        first = {str(record.event_id) for record in pipeline.store.iter_records()}
        physical_before = pipeline.broker.record_count()

        _publisher, restarted = pipeline.new_relay("relay-restarted")
        restarted.run_until_idle(max_cycles=100)

        assert {str(record.event_id) for record in pipeline.store.iter_records()} == first
        assert pipeline.broker.record_count() == physical_before, "nothing left to republish"

    @pytest.mark.parametrize("page_size", [1, 2, 3, 9])
    def test_page_size_does_not_change_the_final_projection(
        self, tmp_path, clock, hooks, page_size: int
    ) -> None:
        """Batching is an ingestion detail, not a financial one.

        Aggregate *versions* may legitimately differ between runs only if the
        order differs; the Deals are inserted in ``(source_seq, deal_id)`` order
        regardless of paging, so both the versions and the totals must match.
        """
        from outbox.pipeline import build_pipeline

        facts = [deal(f"D-{i:04d}", account_id=f"A-{i % 2}", source_seq=i) for i in range(1, 10)]
        built = build_pipeline(
            root=tmp_path / f"page-{page_size}", clock=clock, hooks=hooks, synchronous="NORMAL"
        )
        try:
            built.sync.open_epoch(EPOCH_A)
            for index in range(0, len(facts), page_size):
                built.sync.insert_deals(EPOCH_A, facts[index : index + page_size])
            built.pump()
            projection = {
                row.account_id: (row.deal_count, row.realized_pnl_gross, row.commission)
                for row in built.consumer_store.projection(
                    built.settings.consumer_name, built.source_id, EPOCH_A
                )
            }
            versions = {
                (record.aggregate_key, record.aggregate_version)
                for record in built.store.iter_records()
            }
            assert projection == {
                "A-0": (4, "49.20", "-2.00"),
                "A-1": (5, "61.50", "-2.50"),
            }
            assert versions == {("A-0", v) for v in range(1, 5)} | {
                ("A-1", v) for v in range(1, 6)
            }
        finally:
            built.close()

    def test_an_idle_relay_cycle_changes_nothing(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        snapshot = [
            (str(record.event_id), record.state, record.kafka_offset, record.attempt_count)
            for record in pipeline.store.iter_records()
        ]
        for _ in range(5):
            pipeline.publisher.publish_cycle()
        assert [
            (str(record.event_id), record.state, record.kafka_offset, record.attempt_count)
            for record in pipeline.store.iter_records()
        ] == snapshot

    def test_an_idle_consumer_round_changes_nothing(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.pump()
        before = pipeline.consumer_store.dump_projection_json(pipeline.settings.consumer_name)
        for _ in range(5):
            pipeline.consumer.process_once()
        assert pipeline.consumer_store.dump_projection_json(pipeline.settings.consumer_name) == before

    @hyp_settings(max_examples=10, deadline=None)
    @given(amounts=lists(integers(min_value=-9999, max_value=9999), min_size=1, max_size=8))
    def test_projection_totals_equal_an_independent_decimal_sum(
        self, amounts: list[int]
    ) -> None:
        """O6 as a property: any set of amounts sums exactly.

        Hypothesis generates the amounts in minor units so the inputs are
        always valid at scale 2, and the expected total is computed with
        ``Decimal`` here rather than read from anything the consumer wrote.
        """
        import tempfile

        from outbox.clock import ManualClock
        from outbox.faults.hooks import HookRegistry
        from outbox.pipeline import build_pipeline

        with tempfile.TemporaryDirectory() as root:
            built = build_pipeline(
                root=root, clock=ManualClock(), hooks=HookRegistry(), synchronous="NORMAL"
            )
            try:
                built.sync.open_epoch(EPOCH_A)
                facts = [
                    deal(
                        f"D-{index:04d}",
                        account_id="A-42",
                        source_seq=index,
                        realized=f"{Decimal(amount) / 100:.2f}",
                    )
                    for index, amount in enumerate(amounts, start=1)
                ]
                built.sync.insert_deals(EPOCH_A, facts)
                built.pump()
                expected = sum((Decimal(amount) / 100 for amount in amounts), Decimal("0"))
                projection = built.consumer_store.projection(
                    built.settings.consumer_name, built.source_id, EPOCH_A
                )
                assert Decimal(projection[0].realized_pnl_gross) == expected
                assert projection[0].deal_count == len(amounts)
            finally:
                built.close()


class TestBrokerRecordEquivalences:
    def test_every_broker_record_parses_back_to_its_outbox_row(
        self, pipeline: Pipeline
    ) -> None:
        facts = [deal(f"D-{i:04d}", account_id=f"A-{i % 3}", source_seq=i) for i in range(1, 8)]
        pipeline.sync.insert_deals(EPOCH_A, facts)
        pipeline.run_relay()

        by_id = {str(record.event_id): record for record in pipeline.store.iter_records()}
        for record in pipeline.broker.records():
            envelope = Envelope.parse_wire(record.value, supported_schema_versions=(1,))
            row = by_id[str(envelope.event_id)]
            assert envelope.payload_sha256 == row.payload_sha256
            assert envelope.aggregate_version == row.aggregate_version
            assert record.key.decode() == row.partition_key
            assert json.loads(record.value.decode())["payload"] == row.payload
