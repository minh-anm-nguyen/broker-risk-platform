"""IT-04: the checkpoint boundary, with a fake Project 03 sink.

Covers WF2-K-02, WF2-K-03, WF2-K-04, WF2-K-05, WF2-K-06, WF2-Q-01, WF2-Q-03,
WF2-Q-07, WF2-Q-10, WF2-R-02, WF2-R-06, INV-09, INV-21, INV-22 and ADR-207.

The sink below is a stand-in for Project 03: a unique key on
``(source_id, deal_id)``, a durable watermark, and a transaction boundary that
can be made to fail at either side of the commit. It is deliberately simple,
because the property under test belongs to the *protocol*, not to the
storage: a checkpoint advances only after every page of a scan is persisted,
and a crash before that leaves the old watermark in place.

What the connector contributes is the negative half: it never advances
anything. There is no method on it that could, which is what makes "the
connector marked data as synced before it was persisted" an error this design
does not permit (ADR-207, WF2-Q-01).
"""

from __future__ import annotations

import pytest

from mt5_connector.errors import SourceContractError
from mt5_connector.ports import Entity, ReadPlan, ScanOutcome
from mt5_connector.scan import Scanner

pytestmark = [pytest.mark.integration]


class FakeSink:
    """A minimal stand-in for the Project 03 transactional sink."""

    def __init__(self, *, source_id: str = "broker-sim-01") -> None:
        self.source_id = source_id
        #: The unique key Project 03 is specified to hold.
        self.rows: dict[tuple[str, str], dict] = {}
        #: The durable watermark, per (entity, filter digest). Scoped by
        #: filter, because a watermark from a scan of account A says nothing
        #: about account B (WF2-K-06, INV-21).
        self.checkpoints: dict[tuple[str, str], int] = {}
        self.commit_count = 0
        self.rollback_count = 0
        self.duplicate_writes = 0
        #: Raise on the Nth page commit, to model a crash mid-scan.
        self.fail_on_commit: int | None = None
        #: Drop the acknowledgement after a successful commit.
        self.drop_ack_on_commit: int | None = None

    async def persist(self, page) -> None:
        self.commit_count += 1
        if self.fail_on_commit == self.commit_count:
            self.rollback_count += 1
            raise RuntimeError(f"sink transaction failed on commit {self.commit_count}")
        for item in page.items:
            key = (self.source_id, item.deal_id)
            if key in self.rows:
                # Idempotent upsert. The counter exists so a test can show the
                # re-read happened and still produced no double-count.
                self.duplicate_writes += 1
            self.rows[key] = {
                "deal_id": item.deal_id,
                "source_seq": item.source_seq,
                "commission": item.commission,
            }
        if self.drop_ack_on_commit == self.commit_count:
            raise ConnectionError("the acknowledgement was lost after the commit landed")

    def commit_checkpoint(self, result) -> None:
        """Only ever called with a COMPLETE scan's candidate."""
        candidate = result.checkpoint_candidate
        assert candidate is not None, "a checkpoint must not advance without a candidate"
        assert result.outcome is ScanOutcome.COMPLETE
        self.checkpoints[(candidate.entity.value, candidate.filter_digest)] = (
            candidate.snapshot_seq
        )

    def watermark(self, entity: str, digest: str) -> int:
        return self.checkpoints.get((entity, digest), 0)

    @property
    def deal_ids(self) -> list[str]:
        return sorted(key[1] for key in self.rows)


async def run_scan(connector, sink: FakeSink, plan: ReadPlan):
    scanner = Scanner(connector)
    result = await scanner.run(plan, sink=sink.persist)
    if result.outcome is ScanOutcome.COMPLETE:
        sink.commit_checkpoint(result)
    return result


class TestHappyPath:
    async def test_a_complete_scan_persists_everything_then_commits_once(
        self, sim_connector, sim_server
    ) -> None:
        """The ordering INV-09 requires: persist, then commit."""
        await sim_connector.connect()
        sink = FakeSink()
        result = await run_scan(
            sim_connector, sink, ReadPlan(entity=Entity.DEALS, limit=7)
        )
        assert result.outcome is ScanOutcome.COMPLETE
        assert sink.deal_ids == sorted(sim_server.sim.dataset.deal_ids())
        assert sink.duplicate_writes == 0
        candidate = result.checkpoint_candidate
        assert sink.watermark("deals", candidate.filter_digest) == candidate.snapshot_seq

    async def test_the_connector_exposes_no_way_to_commit_a_checkpoint(
        self, sim_connector
    ) -> None:
        """ADR-207, WF2-Q-01: enforced by the interface, not by convention."""
        await sim_connector.connect()
        for name in dir(sim_connector):
            assert "commit" not in name.lower()
            assert "checkpoint" not in name.lower() or name.startswith("_")
        from mt5_connector.ports import CheckpointCandidate

        assert not any(
            name.startswith("commit") for name in dir(CheckpointCandidate)
        )


class TestCrashBoundaries:
    async def test_a_crash_before_the_terminal_commit_leaves_the_watermark_alone(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-K-03, INV-09."""
        await sim_connector.connect()
        sink = FakeSink()
        sink.fail_on_commit = 3

        scanner = Scanner(sim_connector)
        with pytest.raises(RuntimeError):
            await scanner.run(
                ReadPlan(entity=Entity.DEALS, limit=5), sink=sink.persist
            )
        assert sink.checkpoints == {}
        assert sink.rollback_count == 1
        # Pages 1 and 2 are in the sink - at-least-once - but nothing claims
        # the scan completed.
        assert 0 < len(sink.rows) < len(sim_server.sim.dataset.deals)

        # The re-run starts from the unchanged watermark and converges.
        sink.fail_on_commit = None
        result = await run_scan(
            sim_connector, sink, ReadPlan(entity=Entity.DEALS, limit=5)
        )
        assert result.outcome is ScanOutcome.COMPLETE
        assert sink.deal_ids == sorted(sim_server.sim.dataset.deal_ids())
        # The overlap was re-read and deduplicated, not double-counted.
        assert sink.duplicate_writes > 0
        assert len(sink.rows) == len(sim_server.sim.dataset.deals)

    async def test_a_lost_acknowledgement_does_not_double_count(
        self, sim_connector, sim_server
    ) -> None:
        """IT-04, WF2-K-02, WF2-R-02: the classic at-least-once case."""
        await sim_connector.connect()
        sink = FakeSink()
        sink.drop_ack_on_commit = 2

        scanner = Scanner(sim_connector)
        with pytest.raises(ConnectionError):
            await scanner.run(
                ReadPlan(entity=Entity.DEALS, limit=5), sink=sink.persist
            )
        assert sink.checkpoints == {}
        committed_before = len(sink.rows)
        assert committed_before > 0

        sink.drop_ack_on_commit = None
        result = await run_scan(
            sim_connector, sink, ReadPlan(entity=Entity.DEALS, limit=5)
        )
        assert result.outcome is ScanOutcome.COMPLETE
        # The unique key absorbed the re-read: one row per deal, no doubles.
        assert len(sink.rows) == len(sim_server.sim.dataset.deals)
        assert sink.deal_ids == sorted(sim_server.sim.dataset.deal_ids())

    async def test_a_source_reset_before_the_commit_blocks_the_watermark(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-R-06, WF2-K-04: no cross-run watermark."""
        await sim_connector.connect()
        sink = FakeSink()
        pages = 0

        async def sink_then_reset(page) -> None:
            nonlocal pages
            await sink.persist(page)
            pages += 1
            if pages == 2:
                sim_server.sim.reset_run()

        scanner = Scanner(sim_connector)
        result = await scanner.run(
            ReadPlan(entity=Entity.DEALS, limit=5), sink=sink_then_reset
        )
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.checkpoint_candidate is None
        assert sink.checkpoints == {}

    async def test_an_incomplete_scan_offers_nothing_to_commit(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-06: a partial scan has no watermark, by type."""
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "http_error",
            path="/v1/deals",
            nth=3,
            max_activations=1,
            status=410,
            code="SNAPSHOT_EXPIRED",
        )
        sink = FakeSink()
        scanner = Scanner(sim_connector)
        result = await scanner.run(
            ReadPlan(entity=Entity.DEALS, limit=5), sink=sink.persist
        )
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.checkpoint_candidate is None
        with pytest.raises(AssertionError):
            sink.commit_checkpoint(result)


class TestIncrementalResume:
    async def test_resuming_from_the_watermark_collects_only_new_deals(
        self, sim_connector, sim_server
    ) -> None:
        """INV-22: the sink converges on the source, with no gap."""
        await sim_connector.connect()
        sink = FakeSink()
        first = await run_scan(
            sim_connector, sink, ReadPlan(entity=Entity.DEALS, limit=10)
        )
        digest = first.checkpoint_candidate.filter_digest
        watermark = sink.watermark("deals", digest)
        assert watermark > 0

        sim_server.sim.append_deals(13)
        second = await run_scan(
            sim_connector,
            sink,
            ReadPlan(entity=Entity.DEALS, limit=10, after_seq=watermark),
        )
        assert second.outcome is ScanOutcome.COMPLETE
        assert second.items_seen == 13
        assert len(sink.rows) == len(sim_server.sim.dataset.deals)
        assert sink.deal_ids == sorted(sim_server.sim.dataset.deal_ids())

    async def test_a_filtered_watermark_cannot_be_used_as_a_global_one(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-K-06, INV-21: the digests differ, so the watermarks cannot mix."""
        await sim_connector.connect()
        sink = FakeSink()
        account = sim_server.sim.dataset.accounts[0]["account_id"]
        filtered = await run_scan(
            sim_connector,
            sink,
            ReadPlan(entity=Entity.DEALS, limit=10, account_id=account),
        )
        unfiltered_plan = ReadPlan(entity=Entity.DEALS, limit=10)
        from mt5_connector.validation import filter_digest

        global_digest = filter_digest(unfiltered_plan.filter_fields())
        assert filtered.checkpoint_candidate.filter_digest != global_digest
        # The global scan starts from zero, not from account A's watermark.
        assert sink.watermark("deals", global_digest) == 0

    async def test_two_independent_consumers_do_not_share_state(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-Q-10: the connector holds no shared checkpoint."""
        await sim_connector.connect()
        first_sink = FakeSink(source_id="consumer-1")
        second_sink = FakeSink(source_id="consumer-2")
        first = await run_scan(
            sim_connector, first_sink, ReadPlan(entity=Entity.DEALS, limit=10)
        )
        second = await run_scan(
            sim_connector, second_sink, ReadPlan(entity=Entity.DEALS, limit=10)
        )
        assert first.outcome is second.outcome is ScanOutcome.COMPLETE
        assert first_sink.deal_ids == second_sink.deal_ids
        assert first_sink.checkpoints != {} and second_sink.checkpoints != {}

    async def test_a_stale_cursor_after_a_restart_fails_safely(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-Q-03: a cursor is upstream state, not connector state."""
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        cursor = page.next_cursor
        assert cursor is not None

        # The connector restarts; the consumer still holds the cursor.
        await sim_connector.close()
        from mt5_connector.client import Connector, build_driver_factory

        fresh = Connector(
            sim_connector.settings,
            driver_factory=build_driver_factory(
                sim_connector.settings, token="data-token-canary-AAAA1111"
            ),
        )
        try:
            await fresh.connect()
            # Still the same run and snapshot, so the cursor is honoured - but
            # it was revalidated against the source, not trusted.
            resumed = await fresh.read_page(
                ReadPlan(entity=Entity.DEALS, limit=5, cursor=cursor)
            )
            assert resumed.provenance.run_id == page.provenance.run_id
            assert resumed.snapshot_seq == page.snapshot_seq
        finally:
            await fresh.close()

    async def test_a_cursor_from_a_tampered_token_is_refused(
        self, sim_connector
    ) -> None:
        """WF2-I-04: a signed cursor is not merely obscure."""
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert page.next_cursor is not None
        tampered = page.next_cursor[:-4] + "AAAA"
        with pytest.raises(SourceContractError) as excinfo:
            await sim_connector.read_page(
                ReadPlan(entity=Entity.DEALS, limit=5, cursor=tampered)
            )
        assert excinfo.value.upstream_status == 400
        assert sim_connector.last_retry_stats.attempts == 1
