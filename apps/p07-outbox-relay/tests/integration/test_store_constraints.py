"""Database constraints: the safety that survives a bug in the application.

Every assertion here is about something SQL refuses, not something Python
checks. That distinction is the point of design section 6.2: application-level
validation protects against the code paths you thought of, and a unique index
protects against the ones you did not.
"""

from __future__ import annotations

import json
import sqlite3

import pytest

from outbox.clock import to_wire
from outbox.errors import IntegrityIncident
from outbox.events.keys import EventType, build_dedupe_key
from outbox.fixtures import EPOCH_A, EPOCH_B, deal
from outbox.pipeline import Pipeline

pytestmark = pytest.mark.integration


def _raw_insert(pipeline: Pipeline, **overrides: object) -> None:
    """Insert an outbox row directly, bypassing the writer.

    Only used to prove the database refuses things. Nothing in production takes
    this path -- which is exactly why the constraints have to be real.
    """
    row: dict[str, object] = {
        "event_id": "11111111-1111-4111-8111-111111111111",
        "source_id": pipeline.source_id,
        "epoch_id": EPOCH_A,
        "dedupe_key": build_dedupe_key(EventType.DEAL_RECORDED, EPOCH_A, "D-9999"),
        "aggregate_type": "account",
        "aggregate_key": "A-42",
        "aggregate_version": 1,
        "event_type": "DealRecorded",
        "schema_version": 1,
        "topic": "broker.deals.v1",
        "partition_key": "k",
        "payload": "{}",
        "payload_sha256": "a" * 64,
        "wire_bytes": b"{}",
        "state": "PENDING",
        "created_at": to_wire(pipeline.clock.now()),
        "next_attempt_at": to_wire(pipeline.clock.now()),
    }
    row.update(overrides)
    columns = ", ".join(row)
    placeholders = ", ".join("?" for _ in row)
    with pipeline.store.transaction() as tx:
        tx.execute(f"INSERT INTO event_outbox ({columns}) VALUES ({placeholders})", tuple(row.values()))


class TestUniqueness:
    def test_the_semantic_key_is_unique_per_source_and_epoch(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        existing = next(iter(pipeline.store.iter_records()))
        with pytest.raises(sqlite3.IntegrityError, match="dedupe_key"):
            _raw_insert(
                pipeline,
                dedupe_key=existing.dedupe_key,
                event_id="22222222-2222-4222-8222-222222222222",
                aggregate_version=99,
            )

    def test_the_aggregate_version_is_unique_per_stream(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", account_id="A-42")])
        with pytest.raises(sqlite3.IntegrityError, match="aggregate_version"):
            _raw_insert(pipeline, aggregate_key="A-42", aggregate_version=1)

    def test_the_event_id_is_globally_unique(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        existing = next(iter(pipeline.store.iter_records()))
        with pytest.raises(sqlite3.IntegrityError, match="event_id"):
            _raw_insert(pipeline, event_id=str(existing.event_id), aggregate_version=50)

    def test_the_same_key_in_a_different_epoch_is_permitted(self, pipeline: Pipeline) -> None:
        # A reused deal id across an epoch reset is legitimate and must not be
        # blocked by the uniqueness that protects within an epoch.
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.sync.open_epoch(EPOCH_B)
        pipeline.sync.insert_deals(EPOCH_B, [deal("D-1001")])
        assert len(list(pipeline.store.iter_records())) == 2


class TestCheckConstraints:
    @pytest.mark.parametrize("version", [0, -1])
    def test_aggregate_version_must_be_positive(self, pipeline: Pipeline, version: int) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _raw_insert(pipeline, aggregate_version=version)

    def test_an_unknown_state_is_refused(self, pipeline: Pipeline) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _raw_insert(pipeline, state="PROBABLY_FINE")

    def test_a_claimed_row_must_carry_owner_token_and_lease(self, pipeline: Pipeline) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _raw_insert(pipeline, state="CLAIMED")

    def test_a_published_row_must_carry_published_at(self, pipeline: Pipeline) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _raw_insert(pipeline, state="PUBLISHED")

    def test_a_quarantined_row_must_carry_a_reason(self, pipeline: Pipeline) -> None:
        # Otherwise a parked event would have no recorded cause, and the
        # operator who finds it has nothing to act on.
        with pytest.raises(sqlite3.IntegrityError):
            _raw_insert(pipeline, state="QUARANTINED")

    def test_an_event_cannot_reference_an_unknown_epoch(self, pipeline: Pipeline) -> None:
        with pytest.raises(sqlite3.IntegrityError):
            _raw_insert(pipeline, epoch_id="99999999-9999-4999-8999-999999999999")


class TestImmutability:
    def test_the_payload_cannot_be_updated_after_insert(self, pipeline: Pipeline) -> None:
        """The relay's database role must not be able to change an amount.

        SQLite has no column grants, so the trigger is the whole mechanism here;
        PostgreSQL gets both. Without it, "the relay cannot alter a Deal
        amount" would be a convention rather than a constraint.
        """
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        record = next(iter(pipeline.store.iter_records()))
        tampered = json.dumps({**record.payload, "realized_pnl_gross": "9999.00"})
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            with pipeline.store.transaction() as tx:
                tx.execute(
                    "UPDATE event_outbox SET payload = ? WHERE event_id = ?",
                    (tampered, str(record.event_id)),
                )

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("payload_sha256", "b" * 64),
            ("aggregate_version", 42),
            ("dedupe_key", "DealRecorded:x:00"),
            ("topic", "broker.elsewhere.v1"),
            ("partition_key", "other"),
            ("event_type", "DealInvented"),
            ("schema_version", 7),
            ("wire_bytes", b"{}"),
            ("created_at", "2000-01-01T00:00:00.000000Z"),
        ],
    )
    def test_identity_and_routing_columns_are_immutable(
        self, pipeline: Pipeline, column: str, value: object
    ) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        record = next(iter(pipeline.store.iter_records()))
        with pytest.raises(sqlite3.IntegrityError, match="immutable"):
            with pipeline.store.transaction() as tx:
                tx.execute(
                    f"UPDATE event_outbox SET {column} = ? WHERE event_id = ?",
                    (value, str(record.event_id)),
                )

    def test_lifecycle_columns_remain_writable(self, pipeline: Pipeline) -> None:
        # The trigger must not be so broad that the relay cannot do its job.
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        claim = pipeline.store.claim_due(
            owner="a", batch_size=1, lease_seconds=30, now=pipeline.clock.now()
        )[0]
        assert pipeline.store.mark_published_if_owner(
            claim=claim, kafka_partition=2, kafka_offset=9, now=pipeline.clock.now()
        )


class TestAggregateHeads:
    def test_the_head_advances_once_per_new_event(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id="A-42", source_seq=index) for index in range(1, 6)],
        )
        assert pipeline.store.aggregate_head(pipeline.source_id, EPOCH_A, "A-42") == 5

    def test_a_version_cannot_be_allocated_without_reserving_the_head(
        self, pipeline: Pipeline
    ) -> None:
        """Reserve, check the key, then allocate -- in that order.

        Allocating without reserving would let two concurrent writers both
        decide the event is new and both take version ``n+1``.
        """
        with pipeline.store.transaction() as tx:
            with pytest.raises(IntegrityIncident, match="was not reserved"):
                tx.bump_aggregate_head(pipeline.source_id, EPOCH_A, "A-42")

    def test_heads_are_scoped_per_source_epoch_and_aggregate(self, pipeline: Pipeline) -> None:
        pipeline.sync.open_epoch(EPOCH_B)
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001", account_id="A-42")])
        pipeline.sync.insert_deals(EPOCH_B, [deal("D-1001", account_id="A-42")])
        assert pipeline.store.aggregate_head(pipeline.source_id, EPOCH_A, "A-42") == 1
        assert pipeline.store.aggregate_head(pipeline.source_id, EPOCH_B, "A-42") == 1


class TestStats:
    def test_counts_and_backlog_age_reflect_the_rows(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(
            EPOCH_A,
            [deal(f"D-{index:04d}", account_id=f"A-{index}", source_seq=index) for index in range(1, 4)],
        )
        pipeline.clock.advance(120)
        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert stats.pending_total == 3
        assert stats.oldest_pending_age_seconds == pytest.approx(120, abs=1)
        assert stats.storage_bytes > 0

    def test_backlog_age_never_goes_negative_when_the_clock_moves_back(
        self, pipeline: Pipeline
    ) -> None:
        # A negative age would make an alert threshold unreachable.
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.clock.advance(-600)
        assert pipeline.store.stats(now=pipeline.clock.now()).oldest_pending_age_seconds == 0.0

    def test_a_published_row_leaves_the_backlog(self, pipeline: Pipeline) -> None:
        pipeline.sync.insert_deals(EPOCH_A, [deal("D-1001")])
        pipeline.run_relay()
        stats = pipeline.store.stats(now=pipeline.clock.now())
        assert (stats.pending_total, stats.published_total) == (0, 1)
        assert stats.oldest_pending_age_seconds == 0.0


@pytest.mark.postgres
class TestPostgresParity:
    """The same protocol against PostgreSQL. Skipped without ``OUTBOX_PG_DSN``.

    Deliberately thin: what is worth asserting across backends is that the
    claim, the fencing CAS and the semantic-key uniqueness behave identically.
    The rest is covered by the SQLite tests, which exercise the same code paths.
    """

    def test_claim_cas_and_uniqueness_behave_the_same(self, pg_dsn: str, clock) -> None:
        from outbox.money import MoneyCurrency
        from outbox.persistence.postgres_store import PostgresOutboxStore
        from outbox.writer.outbox_writer import OutboxWriter

        store = PostgresOutboxStore(pg_dsn, clock=clock)
        try:
            # This test inserts into source_epochs itself, so it needs the P03
            # stand-in schema and asks for it explicitly (contract gap CG-07).
            store.migrate(include_p03_standin=True)
            with store.transaction() as tx:
                tx.execute(
                    "INSERT INTO source_epochs (epoch_id, source_id, status, opened_at) "
                    "VALUES (%s, %s, 'ACTIVE', %s) ON CONFLICT DO NOTHING",
                    (EPOCH_A, "sim-broker-01", clock.now()),
                )
            writer = OutboxWriter(source_allowlist=("sim-broker-01",), clock=clock)
            fact = deal("D-pg-1", currency=MoneyCurrency.proven("USD"))
            with store.transaction() as tx:
                first = writer.record_deal(
                    tx, source_id="sim-broker-01", epoch_id=EPOCH_A, deal=fact
                )
            assert first.created
            with store.transaction() as tx:
                second = writer.record_deal(
                    tx, source_id="sim-broker-01", epoch_id=EPOCH_A, deal=fact
                )
            assert not second.created and second.event_id == first.event_id

            claims = store.claim_due(
                owner="relay-a", batch_size=10, lease_seconds=30, now=clock.now()
            )
            assert any(claim.event_id == first.event_id for claim in claims)
            claim = next(claim for claim in claims if claim.event_id == first.event_id)
            assert store.mark_published_if_owner(
                claim=claim, kafka_partition=0, kafka_offset=1, now=clock.now()
            )
            assert not store.mark_published_if_owner(
                claim=claim, kafka_partition=0, kafka_offset=2, now=clock.now()
            )
        finally:
            store.close()
