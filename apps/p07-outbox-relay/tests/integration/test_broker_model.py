"""The broker model's own semantics.

A fake that is wrong in a convenient direction makes every test above it
meaningless, so the model gets tested too. The three properties that matter
most are the ones a careless fake would get wrong: ``produce`` must only
enqueue, ``DROP_ACK`` must append *and* report failure, and order within a
partition must never be shuffled.
"""

from __future__ import annotations

import pytest

from outbox.broker.embedded import EmbeddedKafka, Fault, OffsetOutOfRange
from outbox.broker.ports import DeliveryResult, DeliveryState
from outbox.errors import BrokerError, ErrorCode, MessageTooLarge, ProducerQueueFull
from outbox.events.routing import DEALS_TOPIC, SNAPSHOTS_TOPIC, partition_for_key

pytestmark = pytest.mark.integration


@pytest.fixture
def broker() -> EmbeddedKafka:
    return EmbeddedKafka()


def _send(producer, broker: EmbeddedKafka, key: str, value: bytes, topic: str | None = None) -> list:
    results: list = []
    producer.produce(
        topic=topic or DEALS_TOPIC.name,
        key=key.encode(),
        value=value,
        headers={},
        on_delivery=results.append,
    )
    producer.poll()
    return results


class TestProduceIsEnqueueOnly:
    def test_produce_does_not_deliver(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        results: list = []
        producer.produce(
            topic=DEALS_TOPIC.name, key=b"k", value=b"v", headers={}, on_delivery=results.append
        )
        # This is the misreading the whole design warns about: produce() has
        # returned and nothing has been delivered or acknowledged.
        assert results == []
        assert broker.record_count() == 0
        assert producer.in_flight() == 1
        assert producer.buffer_used_bytes() == 1

    def test_poll_is_what_serves_the_callback(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        results: list = []
        producer.produce(
            topic=DEALS_TOPIC.name, key=b"k", value=b"v", headers={}, on_delivery=results.append
        )
        assert producer.poll() == 1
        assert len(results) == 1 and results[0].broker_acked
        assert broker.record_count() == 1
        assert producer.in_flight() == 0

    def test_flush_reports_what_is_still_queued(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        for index in range(3):
            producer.produce(
                topic=DEALS_TOPIC.name,
                key=f"k{index}".encode(),
                value=b"v",
                headers={},
                on_delivery=lambda _r: None,
            )
        assert producer.flush() == 0
        assert broker.record_count() == 3


class TestFaults:
    @pytest.mark.parametrize(
        ("fault", "code", "appended"),
        [
            (Fault.DOWN, ErrorCode.BROKER_UNREACHABLE, False),
            (Fault.LEADER_UNAVAILABLE, ErrorCode.BROKER_LEADER_UNAVAILABLE, False),
            (Fault.NOT_ENOUGH_REPLICAS, ErrorCode.BROKER_NOT_ENOUGH_REPLICAS, False),
            (Fault.AUTH_DENIED, ErrorCode.AUTH_DENIED, False),
            (Fault.TIMEOUT, ErrorCode.BROKER_TIMEOUT, False),
        ],
    )
    def test_refusing_faults_append_nothing(
        self, broker: EmbeddedKafka, fault: Fault, code: ErrorCode, appended: bool
    ) -> None:
        broker.fault = fault
        producer = broker.producer()
        results = _send(producer, broker, "k", b"v")
        assert results[0].state is DeliveryState.FAILED
        assert results[0].error_code is code
        assert broker.record_count() == (1 if appended else 0)

    def test_drop_ack_appends_and_still_reports_failure(self, broker: EmbeddedKafka) -> None:
        """The ambiguous window from design section 3.3.

        A fake that only failed *before* appending would make duplicates
        untestable and would quietly validate a relay that treats "no ACK" as
        "not delivered".
        """
        broker.fault = Fault.DROP_ACK
        producer = broker.producer()
        results = _send(producer, broker, "k", b"v")
        assert results[0].state is DeliveryState.AMBIGUOUS
        assert results[0].error_code is ErrorCode.BROKER_ACK_AMBIGUOUS
        assert broker.record_count() == 1, "the broker does hold the record"

    def test_duplicate_append_acks_once(self, broker: EmbeddedKafka) -> None:
        broker.fault = Fault.DUPLICATE_APPEND
        producer = broker.producer()
        results = _send(producer, broker, "k", b"v")
        assert results[0].broker_acked
        assert broker.record_count() == 2

    def test_an_oversized_record_is_refused_at_enqueue(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        with pytest.raises(MessageTooLarge):
            producer.produce(
                topic=DEALS_TOPIC.name,
                key=b"k",
                value=b"x" * (broker.max_message_bytes + 1),
                headers={},
                on_delivery=lambda _r: None,
            )
        assert producer.in_flight() == 0

    def test_a_full_local_queue_raises_before_anything_is_sent(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer(queue_max_bytes=100)
        producer.produce(
            topic=DEALS_TOPIC.name, key=b"k", value=b"x" * 80, headers={}, on_delivery=lambda _r: None
        )
        with pytest.raises(ProducerQueueFull):
            producer.produce(
                topic=DEALS_TOPIC.name,
                key=b"k2",
                value=b"x" * 80,
                headers={},
                on_delivery=lambda _r: None,
            )
        # Nothing was lost: the first record is still queued and deliverable.
        assert producer.in_flight() == 1
        assert producer.poll() == 1

    def test_an_unknown_topic_fails_rather_than_being_created(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        results = _send(producer, broker, "k", b"v", topic="broker.not.a.topic")
        assert results[0].error_code is ErrorCode.BROKER_UNREACHABLE

    def test_producer_acl_blocks_an_unauthorised_topic(self, broker: EmbeddedKafka) -> None:
        broker.producer_acl = {SNAPSHOTS_TOPIC.name}
        producer = broker.producer()
        results = _send(producer, broker, "k", b"v", topic=DEALS_TOPIC.name)
        assert results[0].error_code is ErrorCode.AUTH_DENIED
        assert broker.record_count() == 0

    def test_consumer_acl_blocks_an_unauthorised_subscription(self, broker: EmbeddedKafka) -> None:
        broker.consumer_acl = {SNAPSHOTS_TOPIC.name}
        consumer = broker.consumer(group="g")
        with pytest.raises(BrokerError, match="not authorized"):
            consumer.subscribe([DEALS_TOPIC.name])


class TestOrderAndPartitioning:
    def test_order_within_a_partition_is_preserved(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        for index in range(20):
            _send(producer, broker, "same-key", str(index).encode())
        values = [record.value for record in broker.records(DEALS_TOPIC.name)]
        assert values == [str(index).encode() for index in range(20)]

    def test_one_key_always_lands_on_one_partition(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        for _ in range(10):
            _send(producer, broker, "stable-key", b"v")
        partitions = {record.partition for record in broker.records(DEALS_TOPIC.name)}
        assert len(partitions) == 1
        assert partitions == {partition_for_key("stable-key", DEALS_TOPIC.partitions)}

    def test_offsets_are_dense_per_partition(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        for index in range(15):
            _send(producer, broker, f"key-{index}", b"v")
        for partition in range(DEALS_TOPIC.partitions):
            offsets = [
                record.offset
                for record in broker.records(DEALS_TOPIC.name)
                if record.partition == partition
            ]
            assert offsets == list(range(len(offsets)))


class TestConsumerGroupSemantics:
    def test_a_consumer_starts_from_its_committed_offset(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        for index in range(5):
            _send(producer, broker, "k", str(index).encode())
        consumer = broker.consumer(group="g")
        consumer.subscribe([DEALS_TOPIC.name])
        first = consumer.poll(max_records=2)
        assert len(first) == 2
        consumer.commit({first[-1].topic_partition: first[-1].offset + 1})

        restarted = broker.consumer(group="g")
        restarted.subscribe([DEALS_TOPIC.name])
        resumed = restarted.poll(max_records=10)
        assert [record.offset for record in resumed] == [2, 3, 4]

    def test_a_poll_without_a_commit_is_redelivered(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        _send(producer, broker, "k", b"v")
        consumer = broker.consumer(group="g")
        consumer.subscribe([DEALS_TOPIC.name])
        assert len(consumer.poll(max_records=5)) == 1

        restarted = broker.consumer(group="g")
        restarted.subscribe([DEALS_TOPIC.name])
        assert len(restarted.poll(max_records=5)) == 1

    def test_moving_a_committed_offset_backwards_is_refused(self, broker: EmbeddedKafka) -> None:
        # Replay is an operator action with an audit trail, not something the
        # consumer loop may do on its own.
        broker.set_committed("g", DEALS_TOPIC.name, 0, 10)
        consumer = broker.consumer(group="g")
        consumer.subscribe([DEALS_TOPIC.name])
        with pytest.raises(BrokerError, match="backwards"):
            consumer.commit({(DEALS_TOPIC.name, 0): 5})

    def test_revocation_removes_the_partition_from_the_assignment(
        self, broker: EmbeddedKafka
    ) -> None:
        consumer = broker.consumer(group="g")
        consumer.subscribe([DEALS_TOPIC.name])
        assigned = consumer.assignment()
        consumer.revoke(assigned[:2])
        assert len(consumer.assignment()) == len(assigned) - 2
        consumer.assign(assigned[:2])
        assert set(consumer.assignment()) == set(assigned)

    def test_lag_is_the_distance_from_the_committed_offset(self, broker: EmbeddedKafka) -> None:
        producer = broker.producer()
        for index in range(7):
            _send(producer, broker, f"k{index}", b"v")
        assert broker.consumer_lag("g", DEALS_TOPIC.name) == 7
        consumer = broker.consumer(group="g")
        consumer.subscribe([DEALS_TOPIC.name])
        records = consumer.poll(max_records=7)
        consumer.commit({record.topic_partition: record.offset + 1 for record in records})
        assert broker.consumer_lag("g", DEALS_TOPIC.name) == 0


class TestRetention:
    def test_expiry_truncates_the_prefix_and_raises_on_a_lost_offset(
        self, broker: EmbeddedKafka
    ) -> None:
        producer = broker.producer()
        for index in range(5):
            _send(producer, broker, "same-key", str(index).encode())
        partition = partition_for_key("same-key", DEALS_TOPIC.partitions)
        assert broker.expire_retention(DEALS_TOPIC.name, partition, keep_from_offset=3) == 3
        assert broker.log_start_offset(DEALS_TOPIC.name, partition) == 3
        assert broker.high_watermark(DEALS_TOPIC.name, partition) == 5

        consumer = broker.consumer(group="late")
        consumer.subscribe([DEALS_TOPIC.name])
        # A silent jump to the latest offset would hide the lost history.
        with pytest.raises(OffsetOutOfRange):
            consumer.poll(max_records=10)

    def test_a_consumer_at_or_past_the_log_start_reads_normally(
        self, broker: EmbeddedKafka
    ) -> None:
        producer = broker.producer()
        for index in range(5):
            _send(producer, broker, "same-key", str(index).encode())
        partition = partition_for_key("same-key", DEALS_TOPIC.partitions)
        broker.expire_retention(DEALS_TOPIC.name, partition, keep_from_offset=3)
        broker.set_committed("ok-group", DEALS_TOPIC.name, partition, 3)
        consumer = broker.consumer(group="ok-group")
        consumer.subscribe([DEALS_TOPIC.name])
        assert [record.offset for record in consumer.poll(max_records=10)] == [3, 4]


class TestDeliveryResultInvariants:
    def test_an_ack_without_a_position_is_not_constructible(self) -> None:
        # Writing PUBLISHED without a partition and offset would leave a row
        # that cannot be verified against the log.
        with pytest.raises(ValueError, match="partition and an offset"):
            DeliveryResult(state=DeliveryState.ACKED, topic="t")

    def test_a_failure_must_name_an_error_code(self) -> None:
        with pytest.raises(ValueError, match="error code"):
            DeliveryResult(state=DeliveryState.FAILED, topic="t")
