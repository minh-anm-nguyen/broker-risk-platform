"""The Kafka model's own contract. What-if domains D, E, F, AG.

These test the *adapter*, not P08's logic. They matter because every offset
and rebalance assertion elsewhere is only as trustworthy as the model's
fidelity: if the model let a stale generation commit, the fencing tests above
it would pass for the wrong reason.
"""

from __future__ import annotations

import pytest

from analytics.adapters.kafka_memory import InMemoryBroker, InMemoryConsumer
from analytics.errors import AnalyticsError, FencedError, Reason, RetriableError

pytestmark = pytest.mark.component

TOPIC = "broker.deals.v1"


@pytest.fixture
def broker() -> InMemoryBroker:
    broker = InMemoryBroker()
    broker.create_topic(TOPIC, 3)
    return broker


def consumer(broker: InMemoryBroker, *, group="g", partitions=None) -> InMemoryConsumer:
    return InMemoryConsumer(
        broker,
        group_id=group,
        assignment=partitions if partitions is not None else broker.partitions_of(TOPIC),
    )


class TestProduceAndPoll:
    def test_offsets_are_per_partition_and_dense(self, broker):
        for index in range(5):
            assert broker.produce(TOPIC, f"m{index}".encode(), partition=1) == index
        assert broker.watermarks((TOPIC, 1)) == (0, 5)

    def test_keyed_records_stick_to_one_partition(self, broker):
        """Per-account ordering depends on this. P08-D."""
        for _ in range(10):
            broker.produce(TOPIC, b"x", key="A-42")
        populated = [
            index for index in range(3) if broker.watermarks((TOPIC, index))[1] > 0
        ]
        assert len(populated) == 1

    def test_key_hash_is_stable_across_calls(self, broker):
        """``hash()`` is salted per process and unusable for routing."""
        for _ in range(3):
            broker.produce(TOPIC, b"x", key="A-1")
        located = [index for index in range(3) if broker.watermarks((TOPIC, index))[1] == 3]
        assert len(located) == 1

    def test_poll_advances_the_read_position(self, broker):
        broker.produce(TOPIC, b"a", partition=0)
        broker.produce(TOPIC, b"b", partition=0)
        client = consumer(broker)
        assert len(client.poll(max_records=10).records) == 2
        assert client.poll(max_records=10).records == ()

    def test_poll_respects_max_records(self, broker):
        for index in range(10):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        assert len(consumer(broker).poll(max_records=4).records) == 4

    def test_poll_is_deterministic_in_partition_order(self, broker):
        for partition in (2, 0, 1):
            broker.produce(TOPIC, b"x", partition=partition)
        first = [r.partition for r in consumer(broker).poll(max_records=10).records]
        second = [r.partition for r in consumer(broker, group="g2").poll(max_records=10).records]
        assert first == second == [0, 1, 2]

    def test_rejects_unknown_topic(self, broker):
        with pytest.raises(AnalyticsError) as excinfo:
            broker.produce("broker.unknown.v1", b"x")
        assert excinfo.value.reason is Reason.TOPIC_NOT_ALLOWED

    def test_rejects_out_of_range_partition(self, broker):
        with pytest.raises(AnalyticsError):
            broker.produce(TOPIC, b"x", partition=9)


class TestCommitSemantics:
    def test_commit_stores_the_next_offset(self, broker):
        broker.produce(TOPIC, b"a", partition=0)
        client = consumer(broker)
        batch = client.poll(max_records=10)
        client.commit({(TOPIC, 0): 1}, assignment_epoch=batch.assignment_epoch)
        assert client.committed([(TOPIC, 0)]) == {(TOPIC, 0): 1}

    def test_a_new_consumer_resumes_from_the_committed_offset(self, broker):
        for index in range(5):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        first = consumer(broker)
        batch = first.poll(max_records=10)
        first.commit({(TOPIC, 0): 2}, assignment_epoch=batch.assignment_epoch)
        second = consumer(broker)
        offsets = [r.offset for r in second.poll(max_records=10).records if r.partition == 0]
        assert offsets == [2, 3, 4]

    def test_backwards_commit_is_refused(self, broker):
        """An accidental rewind loses data; the audited path is explicit."""
        broker.produce(TOPIC, b"a", partition=0)
        client = consumer(broker)
        epoch = client.poll(max_records=10).assignment_epoch
        client.commit({(TOPIC, 0): 5}, assignment_epoch=epoch)
        with pytest.raises(AnalyticsError) as excinfo:
            client.commit({(TOPIC, 0): 2}, assignment_epoch=epoch)
        assert excinfo.value.reason is Reason.OFFSET_INVALID

    def test_operator_reset_is_allowed_with_an_actor(self, broker):
        """P08-E-11: a rewind is a signed operator action, not a code path."""
        client = consumer(broker)
        epoch = client.poll(max_records=1).assignment_epoch
        client.commit({(TOPIC, 0): 5}, assignment_epoch=epoch)
        broker.reset_offsets("g", {(TOPIC, 0): 0}, actor="oncall-engineer")
        assert broker.committed("g", (TOPIC, 0)) == 0

    def test_operator_reset_requires_an_actor(self, broker):
        with pytest.raises(AnalyticsError) as excinfo:
            broker.reset_offsets("g", {(TOPIC, 0): 0}, actor="")
        assert excinfo.value.reason is Reason.UNAUTHENTICATED


class TestGenerationFencing:
    def test_stale_generation_cannot_commit(self, broker):
        """INV08-11 / P08-F-03, as a deterministic assertion."""
        broker.produce(TOPIC, b"a", partition=0)
        client = consumer(broker)
        stale_epoch = client.poll(max_records=10).assignment_epoch
        client.rebalance(broker.partitions_of(TOPIC))
        with pytest.raises(FencedError) as excinfo:
            client.commit({(TOPIC, 0): 1}, assignment_epoch=stale_epoch)
        assert excinfo.value.reason is Reason.PARTITION_REVOKED

    def test_another_member_joining_fences_the_first(self, broker):
        """P08-F-07: membership is fenced by the broker, not an app lock."""
        first = consumer(broker)
        epoch = first.poll(max_records=1).assignment_epoch
        consumer(broker)  # second member joins the same group
        with pytest.raises(FencedError):
            first.commit({(TOPIC, 0): 1}, assignment_epoch=epoch)

    def test_cannot_commit_an_unassigned_partition(self, broker):
        client = consumer(broker, partitions=[(TOPIC, 0)])
        with pytest.raises(FencedError) as excinfo:
            client.commit({(TOPIC, 2): 1}, assignment_epoch=client.assignment_epoch)
        assert excinfo.value.reason is Reason.PARTITION_REVOKED

    def test_revoke_all_models_group_eviction(self, broker):
        """P08-F-06: exceeding max.poll.interval loses every partition."""
        client = consumer(broker)
        client.revoke_all()
        assert client.assignment() == frozenset()
        assert client.poll(max_records=10).records == ()

    def test_rebalance_re_seeks_to_the_committed_offset(self, broker):
        for index in range(4):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        client = consumer(broker)
        epoch = client.poll(max_records=10).assignment_epoch
        client.commit({(TOPIC, 0): 1}, assignment_epoch=epoch)
        client.rebalance(broker.partitions_of(TOPIC))
        offsets = [r.offset for r in client.poll(max_records=10).records if r.partition == 0]
        assert offsets == [1, 2, 3]


class TestPausing:
    def test_pausing_one_partition_leaves_the_others(self, broker):
        """P08-F-04: poison stops its own partition only."""
        for partition in range(3):
            broker.produce(TOPIC, b"x", partition=partition)
        client = consumer(broker)
        client.pause((TOPIC, 0), reason="unresolved poison")
        partitions = {r.partition for r in client.poll(max_records=10).records}
        assert partitions == {1, 2}
        assert client.paused() == {(TOPIC, 0): "unresolved poison"}

    def test_resume_restores_delivery(self, broker):
        broker.produce(TOPIC, b"x", partition=0)
        client = consumer(broker)
        client.pause((TOPIC, 0), reason="poison")
        assert client.poll(max_records=10).records == ()
        client.resume((TOPIC, 0))
        assert len(client.poll(max_records=10).records) == 1

    def test_cannot_pause_an_unowned_partition(self, broker):
        client = consumer(broker, partitions=[(TOPIC, 0)])
        with pytest.raises(FencedError):
            client.pause((TOPIC, 1), reason="nope")


class TestRetention:
    def test_truncation_raises_the_low_watermark(self, broker):
        for index in range(10):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        assert broker.truncate_to((TOPIC, 0), 4) == 4
        assert broker.watermarks((TOPIC, 0)) == (4, 10)
        assert [r.offset for r in broker.records((TOPIC, 0))] == [4, 5, 6, 7, 8, 9]

    def test_committed_offset_below_retention_refuses_to_resume(self, broker):
        """INV08-13: the history needed to resume is gone, so fail loudly.

        An ``auto.offset.reset`` to latest here would turn missing money into
        a clean dashboard, which is the entire reason the setting is pinned to
        ``none`` (section 16.2).
        """
        for index in range(10):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        client = consumer(broker, partitions=[(TOPIC, 0)])
        epoch = client.poll(max_records=3).assignment_epoch
        client.commit({(TOPIC, 0): 2}, assignment_epoch=epoch)
        broker.truncate_to((TOPIC, 0), 6)
        with pytest.raises(RetriableError) as excinfo:
            consumer(broker, partitions=[(TOPIC, 0)])
        assert excinfo.value.reason is Reason.OFFSET_OUT_OF_RANGE

    def test_committed_offset_at_the_low_watermark_is_not_a_gap(self, broker):
        for index in range(10):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        client = consumer(broker, partitions=[(TOPIC, 0)])
        epoch = client.poll(max_records=10).assignment_epoch
        client.commit({(TOPIC, 0): 6}, assignment_epoch=epoch)
        broker.truncate_to((TOPIC, 0), 6)
        assert consumer(broker, partitions=[(TOPIC, 0)]).position((TOPIC, 0)) == 6

    def test_cold_start_on_an_untruncated_partition_is_legitimate(self, broker):
        for index in range(3):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        assert consumer(broker, partitions=[(TOPIC, 0)]).position((TOPIC, 0)) == 0

    def test_cold_start_after_truncation_is_a_retention_gap(self, broker):
        """No trusted offset *and* an expired log head means records are gone.

        Starting at the low watermark here would silently skip the expired
        records, which is exactly what pinning ``auto.offset.reset`` to
        ``none`` is meant to prevent (INV08-13, P08-E-08).
        """
        for index in range(10):
            broker.produce(TOPIC, f"m{index}".encode(), partition=0)
        broker.truncate_to((TOPIC, 0), 4)
        with pytest.raises(RetriableError) as excinfo:
            consumer(broker, group="never-consumed", partitions=[(TOPIC, 0)])
        assert excinfo.value.reason is Reason.OFFSET_OUT_OF_RANGE

    def test_auto_offset_reset_cannot_be_anything_but_none(self, broker):
        with pytest.raises(AnalyticsError) as excinfo:
            InMemoryConsumer(
                broker, group_id="g", assignment=[(TOPIC, 0)], auto_offset_reset="latest"
            )
        assert excinfo.value.reason is Reason.FIELD_INVALID

    def test_seek_outside_the_retained_range_is_refused(self, broker):
        broker.produce(TOPIC, b"x", partition=0)
        client = consumer(broker, partitions=[(TOPIC, 0)])
        with pytest.raises(RetriableError) as excinfo:
            client.seek((TOPIC, 0), 99)
        assert excinfo.value.reason is Reason.OFFSET_OUT_OF_RANGE


class TestRedelivery:
    def test_redeliver_appends_an_identical_copy(self, broker):
        """At-least-once is the contract. G08-02, ADR08-09."""
        original = broker.produce(TOPIC, b"payload", partition=0)
        duplicate = broker.redeliver((TOPIC, 0), original)
        assert duplicate != original
        records = broker.records((TOPIC, 0))
        assert records[0].value == records[1].value
        assert records[0].offset != records[1].offset

    def test_redeliver_of_an_expired_offset_is_refused(self, broker):
        broker.produce(TOPIC, b"x", partition=0)
        broker.truncate_to((TOPIC, 0), 1)
        with pytest.raises(AnalyticsError) as excinfo:
            broker.redeliver((TOPIC, 0), 0)
        assert excinfo.value.reason is Reason.OFFSET_OUT_OF_RANGE
