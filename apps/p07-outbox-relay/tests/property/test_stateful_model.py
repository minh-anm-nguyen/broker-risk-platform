"""The stateful model from design section 15.2, as a Hypothesis state machine.

The point of a model-based test is that it finds interleavings nobody wrote
down. The enumerated contract tests cover the sequences the design anticipated;
this one generates arbitrary sequences of inserts, claims, lease expiries,
broker faults, consumer polls and epoch resets, and checks the invariants after
*every* step.

The model it checks against is deliberately independent of the implementation:
it tracks which Deals were committed and which event ids the broker has seen,
derived from the broker log and the business tables, and never from the
publisher's own bookkeeping. If the invariant checks consulted
``event_outbox.state`` to decide what to expect, they would agree with a broken
publisher.

Example counts are kept modest because each step touches two real SQLite
databases. That is a real limit on depth, and it is the right trade: a shallow
search over the genuine components beats a deep search over a mock whose
failure modes are the ones we invented.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest
from hypothesis import HealthCheck, settings as hyp_settings
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, precondition, rule
from hypothesis.strategies import booleans, integers, sampled_from

from outbox.broker.embedded import Fault
from outbox.clock import ManualClock
from outbox.errors import OutboxError
from outbox.events.keys import EventType, build_dedupe_key, parse_dedupe_key
from outbox.faults.hooks import HookRegistry
from outbox.fixtures import EPOCH_A, EPOCH_B, deal
from outbox.pipeline import build_pipeline

pytestmark = pytest.mark.property

ACCOUNTS = ("A-1", "A-2", "A-3")


class OutboxStateMachine(RuleBasedStateMachine):
    """Generated actions against the real pipeline, invariants after each one."""

    def __init__(self) -> None:
        super().__init__()
        self._tmp = tempfile.TemporaryDirectory(prefix="outbox-stateful-")
        self.clock = ManualClock()
        self.hooks = HookRegistry()
        self.pipeline = build_pipeline(
            root=Path(self._tmp.name),
            clock=self.clock,
            hooks=self.hooks,
            claim_batch_size=5,
            max_inflight=8,
            lease_seconds=30.0,
            delivery_deadline_seconds=5.0,
            backoff_base_seconds=0.5,
            backoff_max_seconds=5.0,
            circuit_failure_threshold=100,
            synchronous="NORMAL",
        )
        # The model: what the *business* committed, independent of the outbox.
        self.committed_deals: dict[tuple[str, str], str] = {}
        self.next_deal = 1
        self.active_epoch = EPOCH_A

    @initialize()
    def open_first_epoch(self) -> None:
        self.pipeline.sync.open_epoch(EPOCH_A)

    def teardown(self) -> None:
        self.pipeline.close()
        self._tmp.cleanup()

    # --- generated actions --------------------------------------------------
    @rule(account=sampled_from(ACCOUNTS), count=integers(min_value=1, max_value=3))
    def insert_deals(self, account: str, count: int) -> None:
        facts = []
        for _ in range(count):
            deal_id = f"D-{self.next_deal:05d}"
            self.next_deal += 1
            facts.append(deal(deal_id, account_id=account, source_seq=self.next_deal))
        self.pipeline.sync.insert_deals(self.active_epoch, facts)
        for fact in facts:
            self.committed_deals[(self.active_epoch, fact.deal_id)] = fact.business_hash

    @rule(same=booleans())
    def reinsert_a_deal(self, same: bool) -> None:
        """Re-deliver an already-committed Deal, identically or not.

        The identical case must be a no-op; the differing case must raise and
        change nothing. Both are generated so the model explores the retry path
        as often as the happy path.
        """
        if not self.committed_deals:
            return
        (epoch, deal_id) = next(iter(self.committed_deals))
        if epoch != self.active_epoch:
            return
        row = self.pipeline.store.query(
            "SELECT account_id, source_seq, commission FROM deals WHERE epoch_id = ? AND deal_id = ?",
            (epoch, deal_id),
        )[0]
        fact = deal(
            deal_id,
            account_id=row["account_id"],
            source_seq=int(row["source_seq"]),
            commission=row["commission"] if same else "-9.99",
        )
        try:
            self.pipeline.sync.insert_deals(epoch, [fact])
        except OutboxError:
            # A conflicting re-delivery is an integrity incident by design.
            assert not same

    @rule()
    def relay_cycle(self) -> None:
        self.pipeline.publisher.publish_cycle()

    @rule(seconds=integers(min_value=1, max_value=40))
    def advance_clock(self, seconds: int) -> None:
        self.clock.advance(seconds)

    @rule()
    def expire_leases(self) -> None:
        with self.pipeline.store.transaction() as tx:
            tx.execute(
                "UPDATE event_outbox SET lease_until = '2000-01-01T00:00:00.000000Z' "
                "WHERE state = 'CLAIMED'"
            )

    @rule(
        fault=sampled_from(
            [Fault.NONE, Fault.DOWN, Fault.DROP_ACK, Fault.TIMEOUT, Fault.DUPLICATE_APPEND]
        )
    )
    def set_broker_fault(self, fault: Fault) -> None:
        self.pipeline.broker.fault = fault

    @rule()
    def consumer_poll(self) -> None:
        self.pipeline.consumer.process_once(max_records=4)

    @rule()
    def replay_a_delivered_record(self) -> None:
        """Rewind this consumer's position, as a redelivery would."""
        for topic, partition in self.pipeline.kafka_consumer.assignment():
            committed = self.pipeline.kafka_consumer.committed(topic, partition)
            self.pipeline.kafka_consumer.seek(topic, partition, max(0, committed - 1))
        self.pipeline.consumer.process_once(max_records=4)

    @precondition(lambda self: self.active_epoch == EPOCH_A)
    @rule()
    def reset_epoch(self) -> None:
        self.pipeline.sync.reset_epoch(old_epoch_id=EPOCH_A, new_epoch_id=EPOCH_B)
        self.active_epoch = EPOCH_B
        self.next_deal = 1  # the source reuses ids after a reset

    # --- invariants, checked after every action -----------------------------
    @invariant()
    def every_committed_deal_has_exactly_one_logical_event(self) -> None:
        """INV07-01/04 from the business side, not from the outbox's side."""
        for (epoch, deal_id) in self.committed_deals:
            key = build_dedupe_key(EventType.DEAL_RECORDED, epoch, deal_id)
            rows = self.pipeline.store.query(
                "SELECT event_id FROM event_outbox WHERE epoch_id = ? AND dedupe_key = ?",
                (epoch, key),
            )
            assert len(rows) == 1, f"{deal_id} in {epoch} has {len(rows)} events"

    @invariant()
    def no_event_exists_without_its_deal(self) -> None:
        for row in self.pipeline.store.query(
            "SELECT dedupe_key, epoch_id FROM event_outbox WHERE event_type = 'DealRecorded'"
        ):
            parsed = parse_dedupe_key(row["dedupe_key"])
            assert (row["epoch_id"], parsed.identity) in self.committed_deals

    @invariant()
    def published_rows_are_really_in_the_broker(self) -> None:
        """INV07-06: PUBLISHED is a claim about the broker, so check the broker."""
        broker_ids = {
            json.loads(record.value.decode("utf-8"))["event_id"]
            for record in self.pipeline.broker.records()
        }
        for row in self.pipeline.store.query(
            "SELECT event_id FROM event_outbox WHERE state = 'PUBLISHED'"
        ):
            assert str(row["event_id"]) in broker_ids

    @invariant()
    def the_consumer_applies_each_event_at_most_once(self) -> None:
        """INV07-08. Duplicate records are legal; duplicate effects are not."""
        rows = self.pipeline.consumer_store.query(
            "SELECT event_id, COUNT(*) AS n FROM consumer_processed_events GROUP BY event_id"
        )
        assert all(int(row["n"]) == 1 for row in rows)

    @invariant()
    def the_consumer_only_applied_events_the_broker_carried(self) -> None:
        broker_ids = {
            json.loads(record.value.decode("utf-8"))["event_id"]
            for record in self.pipeline.broker.records()
        }
        processed = self.pipeline.consumer_store.processed_event_ids(
            self.pipeline.settings.consumer_name
        )
        assert processed <= broker_ids, "the consumer applied an event nobody published"

    @invariant()
    def applied_versions_are_contiguous_or_blocked(self) -> None:
        """INV07-11/O5: a gap is only acceptable when it is declared."""
        name = self.pipeline.settings.consumer_name
        for row in self.pipeline.consumer_store.version_ledger(name):
            last = int(row["last_applied_version"])
            seen = sorted(
                int(record["aggregate_version"])
                for record in self.pipeline.consumer_store.query(
                    "SELECT aggregate_version FROM consumer_processed_events "
                    "WHERE consumer_name = ? AND source_id = ? AND epoch_id = ? "
                    "AND aggregate_key = ? AND aggregate_version <= ?",
                    (name, row["source_id"], row["epoch_id"], row["aggregate_key"], last),
                )
            )
            assert seen == list(range(1, last + 1)), f"{row['aggregate_key']}: {seen} vs 1..{last}"

    @invariant()
    def epochs_never_share_an_identity(self) -> None:
        """INV07-10: a reused deal id across a reset stays two facts."""
        rows = self.pipeline.store.query(
            "SELECT event_id, COUNT(*) AS n FROM event_outbox GROUP BY event_id"
        )
        assert all(int(row["n"]) == 1 for row in rows)
        per_epoch = self.pipeline.store.query(
            "SELECT epoch_id, dedupe_key, COUNT(*) AS n FROM event_outbox "
            "GROUP BY epoch_id, dedupe_key"
        )
        assert all(int(row["n"]) == 1 for row in per_epoch)

    @invariant()
    def resources_stay_bounded(self) -> None:
        """INV07-13. Correct money with an unbounded queue is not a pass."""
        assert self.pipeline.publisher.limits.inflight <= self.pipeline.settings.max_inflight
        assert (
            self.pipeline.producer.buffer_used_bytes()
            <= self.pipeline.settings.producer_buffer_max_bytes
        )
        stats = self.pipeline.store.stats(now=self.clock.now())
        assert stats.claimed_total <= self.pipeline.settings.max_inflight

    @invariant()
    def money_in_the_projection_matches_the_source(self) -> None:
        """INV07-09/O6, recomputed from the Deal rows with Decimal."""
        from decimal import Decimal

        name = self.pipeline.settings.consumer_name
        for epoch in {epoch for epoch, _ in self.committed_deals} or {self.active_epoch}:
            expected: dict[str, Decimal] = {}
            applied_ids = {
                row["event_id"]
                for row in self.pipeline.consumer_store.query(
                    "SELECT event_id FROM consumer_processed_events WHERE consumer_name = ?", (name,)
                )
            }
            for row in self.pipeline.store.query(
                "SELECT account_id, realized_pnl_gross, event_id FROM deals d "
                "JOIN event_outbox o ON o.epoch_id = d.epoch_id "
                "  AND o.dedupe_key = 'DealRecorded:' || d.epoch_id || ':' "
                "    || lower(hex(CAST(d.deal_id AS BLOB))) "
                "WHERE d.epoch_id = ?",
                (epoch,),
            ):
                if str(row["event_id"]) not in applied_ids:
                    continue
                expected[row["account_id"]] = expected.get(
                    row["account_id"], Decimal("0")
                ) + Decimal(str(row["realized_pnl_gross"]))
            for projection in self.pipeline.consumer_store.projection(
                name, self.pipeline.source_id, epoch
            ):
                assert Decimal(projection.realized_pnl_gross) == expected.get(
                    projection.account_id, Decimal("0")
                ), f"{projection.account_id} in {epoch}"


TestOutboxStateMachine = OutboxStateMachine.TestCase
TestOutboxStateMachine.settings = hyp_settings(
    max_examples=12,
    stateful_step_count=18,
    deadline=None,
    # Each step touches two real databases, so timing-based health checks have
    # nothing useful to say here.
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.data_too_large],
)
