"""The claim query, lease fencing, per-aggregate eligibility and the CAS.

These exercise the SQL directly rather than through the relay, because the
properties being checked are properties of the statement: which rows it is
willing to hand out, and which writes it refuses afterwards. Driving them
through the publisher would also work, but a failure would be one layer removed
from the clause that caused it.
"""

from __future__ import annotations

import threading
import uuid

import pytest

from outbox.fixtures import EPOCH_A, deal
from outbox.persistence.ports import AttemptOutcome, EventState
from outbox.persistence.sqlite_store import SqliteOutboxStore
from outbox.pipeline import Pipeline

pytestmark = pytest.mark.integration


class TestEligibility:
    def test_a_pending_row_is_claimable_once(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        first = pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        second = pipeline.store.claim_due(
            owner="b", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        assert len(first) == 1
        assert second == [], "a leased row is not claimable again before its lease expires"

    def test_only_the_lowest_unpublished_version_of_an_aggregate_is_eligible(
        self, pipeline: Pipeline
    ) -> None:
        """This is the per-aggregate ordering guard from design section 7.2.

        Three versions of one account, and exactly one is offered. Offering all
        three would let them be produced concurrently and arrive out of order,
        which is precisely what the consumer's version check would then have to
        block on.
        """
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id="A-42", source_seq=index) for index in range(1, 4)],
        )
        claims = pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        assert [claim.aggregate_version for claim in claims] == [1]

        assert pipeline.store.mark_published_if_owner(
            claim=claims[0], kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
        )
        next_claims = pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        assert [claim.aggregate_version for claim in next_claims] == [2]

    def test_different_aggregates_are_claimed_together(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id=f"A-{index}", source_seq=index) for index in range(1, 6)],
        )
        claims = pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        assert len(claims) == 5
        assert len({claim.aggregate_key for claim in claims}) == 5

    def test_the_same_account_in_two_epochs_is_two_streams(self, pipeline: Pipeline) -> None:
        from outbox.fixtures import EPOCH_B

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", account_id="A-42")])
        pipeline.sync.open_epoch(EPOCH_B)
        pipeline.sync.insert_deals(EPOCH_B, [deal("D-1001", account_id="A-42")])
        claims = pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        assert len(claims) == 2
        assert len({claim.stream_key for claim in claims}) == 2

    def test_backoff_defers_a_row_until_its_next_attempt_time(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        assert pipeline.store.reschedule_if_owner(
            claim=claim, delay_seconds=60, error_code="BROKER_TIMEOUT", now=pipeline.clock.now()
        )

        pipeline.clock.advance(30)
        assert pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        ) == []
        pipeline.clock.advance(31)
        assert len(
            pipeline.store.claim_due(
                owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
            )
        ) == 1

    def test_a_quarantined_lower_version_deliberately_blocks_later_ones(
        self, pipeline: Pipeline
    ) -> None:
        """Design section 7.2 point 5: this is intended, not a bug.

        A parked version 1 holds back versions 2 and 3 until an operator
        decides. Letting them past would apply later money on top of a gap.
        """
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id="A-42", source_seq=index) for index in range(1, 4)],
        )
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        assert pipeline.store.quarantine_if_owner(
            claim=claim, reason="poison", error_code="SCHEMA_UNSUPPORTED", now=pipeline.clock.now()
        )
        assert pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        ) == []

        # An approved requeue preserves the event_id and unblocks the stream.
        assert pipeline.store.requeue_quarantined(
            event_id=claim.event_id, now=pipeline.clock.now(), approved_by="ops-oncall"
        )
        unblocked = pipeline.store.claim_due(
            owner="a", batch_size=10, lease_seconds=30, now=pipeline.clock.now()
        )
        assert [c.aggregate_version for c in unblocked] == [1]
        assert unblocked[0].event_id == claim.event_id

    def test_requeue_requires_an_approver(self, pipeline: Pipeline) -> None:
        from outbox.errors import IntegrityIncident

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        with pytest.raises(IntegrityIncident, match="approver"):
            pipeline.store.requeue_quarantined(
                event_id=uuid.uuid4(), now=pipeline.clock.now(), approved_by=""
            )

    def test_claims_are_ordered_by_outbox_id(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id=f"A-{index}", source_seq=index) for index in range(1, 11)],
        )
        claims = pipeline.store.claim_due(
            owner="a", batch_size=5, lease_seconds=30, now=pipeline.clock.now()
        )
        assert [claim.outbox_id for claim in claims] == sorted(claim.outbox_id for claim in claims)
        assert len(claims) == 5, "batch_size is respected"


class TestLeaseAndFencing:
    def test_an_expired_lease_is_reclaimable_with_a_higher_claim_version(
        self, pipeline: Pipeline
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        first = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.clock.advance(31)
        second = pipeline.store.claim_due(
            owner="b", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]

        assert second.event_id == first.event_id
        assert second.claim_version == first.claim_version + 1
        assert second.claim_token != first.claim_token
        assert second.attempt_count == first.attempt_count + 1

    def test_the_cas_requires_both_the_token_and_the_version(self, pipeline: Pipeline) -> None:
        import dataclasses

        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]

        wrong_token = dataclasses.replace(claim, claim_token=uuid.uuid4())
        wrong_version = dataclasses.replace(claim, claim_version=claim.claim_version + 5)
        for bad in (wrong_token, wrong_version):
            assert not pipeline.store.mark_published_if_owner(
                claim=bad, kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
            )
        assert pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=0, kafka_offset=0, now=pipeline.clock.now()
        )

    def test_publishing_twice_with_one_claim_is_refused(self, pipeline: Pipeline) -> None:
        # The second CAS fails because the row is no longer CLAIMED, which is
        # what keeps a replayed callback from re-stamping published_at.
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        assert pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=0, kafka_offset=7, now=pipeline.clock.now()
        )
        assert not pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=1, kafka_offset=99, now=pipeline.clock.now()
        )
        record = pipeline.store.get(claim.event_id)
        assert record is not None and (record.kafka_partition, record.kafka_offset) == (0, 7)

    def test_an_expired_lease_with_no_takeover_can_still_complete(
        self, pipeline: Pipeline
    ) -> None:
        """A late write is accepted while the worker is still the owner.

        The CAS predicate is the claim *token and version*, not the lease
        deadline, and that is deliberate. Fencing exists to stop a stale worker
        overwriting a *new* owner's state; if nobody has taken the row over,
        there is no competing state to protect and the token on the row is
        still this worker's.

        Adding ``lease_until > now`` to the predicate would reject this write
        and send the row back for another publish -- turning a delivery that
        already succeeded into a guaranteed duplicate for no safety gain. The
        takeover case is covered by
        :meth:`test_the_cas_requires_both_the_token_and_the_version` and by
        IT07-07.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.clock.advance(120)
        assert pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=0, kafka_offset=3, now=pipeline.clock.now()
        )
        record = pipeline.store.get(claim.event_id)
        assert record is not None and record.state is EventState.PUBLISHED

    def test_release_hands_a_claim_straight_back(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        assert pipeline.store.release_if_owner(claim=claim, now=pipeline.clock.now())
        record = pipeline.store.get(claim.event_id)
        assert record is not None and record.state is EventState.PENDING
        assert record.claim_token is None and record.lease_until is None
        assert len(
            pipeline.store.claim_due(
                owner="b", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
            )
        ) == 1

    def test_two_concurrent_owners_never_hold_the_same_row(self, pipeline: Pipeline) -> None:
        """Two threads, two connections, one row each at most.

        SQLite serialises writers, so this is testing the claim protocol rather
        than ``SKIP LOCKED`` throughput -- which matches the MVP discipline of a
        single active owner. The equivalent concurrency claim for several
        shards has to be measured on PostgreSQL.
        """
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id=f"A-{index}", source_seq=index) for index in range(1, 21)],
        )
        results: list[list[str]] = []
        errors: list[BaseException] = []
        barrier = threading.Barrier(2)

        def claim_in_thread(owner: str) -> None:
            store = SqliteOutboxStore(
                pipeline.settings.sqlite_path, clock=pipeline.clock, synchronous="NORMAL"
            )
            try:
                barrier.wait(timeout=5)
                claims = store.claim_due(
                    owner=owner, batch_size=20, lease_seconds=30, now=pipeline.clock.now()
                )
                results.append([str(claim.event_id) for claim in claims])
            except BaseException as exc:  # noqa: BLE001 - reported to the test
                errors.append(exc)
            finally:
                store.close()

        threads = [
            threading.Thread(target=claim_in_thread, args=(f"relay-{index}",)) for index in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        assert not errors, errors
        first, second = results
        assert not (set(first) & set(second)), "a row was handed to two owners at once"
        assert len(first) + len(second) == 20


class TestAttemptAudit:
    def test_attempts_are_recorded_with_their_claim_version(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.store.record_attempt(
            claim=claim, outcome=AttemptOutcome.RETRY, error_code="BROKER_TIMEOUT", now=pipeline.clock.now()
        )
        pipeline.store.record_attempt(
            claim=claim, outcome=AttemptOutcome.ACK, error_code=None, now=pipeline.clock.now()
        )
        rows = pipeline.store.attempts(claim.event_id)
        assert [row["outcome"] for row in rows] == ["RETRY", "ACK"]
        assert {row["claim_version"] for row in rows} == {claim.claim_version}

    def test_a_requeue_leaves_an_audit_row_naming_the_approver(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        pipeline.store.quarantine_if_owner(
            claim=claim, reason="poison", error_code="SCHEMA_UNSUPPORTED", now=pipeline.clock.now()
        )
        pipeline.store.requeue_quarantined(
            event_id=claim.event_id, now=pipeline.clock.now(), approved_by="ops-oncall"
        )
        rows = pipeline.store.attempts(claim.event_id)
        assert any(
            row["outcome"] == "REQUEUED" and "ops-oncall" in (row["error_code"] or "")
            for row in rows
        )
