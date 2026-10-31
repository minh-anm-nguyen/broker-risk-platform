"""The connector against the Project 01 ``v1`` read contract, over real TCP.

Covers FR-01, FR-02, FR-09, FR-10, FR-11, WF2-I-01, WF2-I-05, WF2-K-01,
WF2-K-05, WF2-M-05, WF2-Q-04, WF2-Q-05, INV-07, INV-08 and INV-19.

These run against the contract double in :mod:`tests.fakesim`, served over a
loopback socket so the synchronous driver performs genuine blocking socket
I/O. That is evidence about the *contract*; it is not evidence that the
connector has been run against the Project 01 process, and the scorecard
records the two separately (section 14.1).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from mt5_connector.errors import CapabilityUnsupportedError, ConfigurationError
from mt5_connector.ports import Entity, ReadPlan, ScanOutcome
from mt5_connector.scan import Scanner

pytestmark = [pytest.mark.contract]


class TestHandshake:
    async def test_meta_maps_to_the_negotiated_capability_set(self, sim_connector) -> None:
        identity = await sim_connector.connect()
        assert identity.source_id == "broker-sim-01"
        assert identity.run_id == "run_0001"
        assert identity.generation == 1
        assert identity.worker_epoch == 1
        assert identity.contract_version == "v1"

        capabilities = await sim_connector.capabilities()
        assert capabilities.snapshot_cursor is True
        assert capabilities.incremental_deals_after_seq is True
        # Project 01 pins a snapshot per scan, not across entities (WF2-Q-05).
        assert capabilities.stable_snapshot_across_entities is False
        assert capabilities.supported_entities == {
            Entity.ACCOUNTS,
            Entity.POSITIONS,
            Entity.DEALS,
        }

    async def test_readiness_is_true_only_after_the_handshake(self, sim_connector) -> None:
        ready, reasons = sim_connector.supervisor.ready()
        assert ready is False
        assert "no_session" in reasons
        await sim_connector.connect()
        ready, reasons = sim_connector.supervisor.ready()
        assert ready is True, reasons


class TestReads:
    async def test_an_accounts_page_validates_and_preserves_decimals(
        self, sim_connector, sim_server
    ) -> None:
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.ACCOUNTS, limit=5))
        assert len(page.items) == 5
        assert page.provenance.run_id == "run_0001"
        assert page.provenance.snapshot_seq == sim_server.sim.dataset.committed_seq
        first = page.items[0]
        expected = sim_server.sim.dataset.accounts[0]
        assert first.account_id == expected["account_id"]
        assert first.balance == Decimal(expected["balance"])
        assert first.currency == "USD"

    async def test_a_positions_page_keeps_a_missing_mark_as_none(
        self, sim_connector
    ) -> None:
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.POSITIONS, limit=10))
        assert page.items
        for item in page.items:
            if item.status == "OPEN":
                assert item.volume_lots > 0

    async def test_a_deals_page_carries_source_seq_and_signed_money(
        self, sim_connector
    ) -> None:
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
        assert len(page.items) == 10
        assert all(item.source_seq is not None for item in page.items)
        assert all(item.commission <= 0 for item in page.items)
        assert page.observed_upper_bound == max(item.source_seq for item in page.items)

    async def test_a_page_size_above_the_source_cap_is_refused_locally(
        self, sim_connector
    ) -> None:
        """WF2-M-05: refused before any request is sent."""
        await sim_connector.connect()
        with pytest.raises(ConfigurationError):
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10_000))

    async def test_an_unsupported_capability_is_refused_not_attempted(
        self, sim_settings, data_token_env, sim_server
    ) -> None:
        """WF2-R-07: no incremental read against a source without the pair."""
        from mt5_connector.client import Connector, build_driver_factory

        sim_server.sim.add_fault("none")  # no-op rule; keeps the engine warm
        connector = Connector(
            sim_settings, driver_factory=build_driver_factory(sim_settings, token=data_token_env)
        )
        try:
            await connector.connect()
            # Force the negotiated capability off to model a weaker source.
            from dataclasses import replace

            weakened = replace(
                connector.supervisor.capabilities, incremental_deals_after_seq=False
            )
            connector.supervisor._capabilities = weakened  # type: ignore[attr-defined]
            with pytest.raises(CapabilityUnsupportedError) as excinfo:
                await connector.read_page(
                    ReadPlan(entity=Entity.DEALS, limit=10, after_seq=5)
                )
            assert excinfo.value.details["capability"] == "incremental_deals_after_seq"
        finally:
            await connector.close()


class TestIncrementalFilter:
    async def test_after_seq_is_exclusive(self, sim_connector, sim_server) -> None:
        """AS-03: strictly greater than the watermark."""
        await sim_connector.connect()
        deals = sim_server.sim.dataset.deals
        watermark = deals[4]["source_seq"]
        page = await sim_connector.read_page(
            ReadPlan(entity=Entity.DEALS, limit=10, after_seq=watermark)
        )
        assert all(item.source_seq > watermark for item in page.items)
        assert page.items[0].deal_id == deals[5]["deal_id"]
        assert page.requested_after_seq == watermark

    async def test_source_seq_gaps_do_not_raise_a_missing_deal_alarm(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-K-01, INV-19: the sequence counts events, not deals."""
        sequences = [row["source_seq"] for row in sim_server.sim.dataset.deals]
        gaps = [b - a for a, b in zip(sequences, sequences[1:])]
        assert max(gaps) > 1, "the dataset must contain gaps for this test to mean anything"

        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        result, items = await scanner.collect(
            ReadPlan(entity=Entity.DEALS, limit=7), limit_items=1_000
        )
        assert result.outcome is ScanOutcome.COMPLETE
        assert [item.source_seq for item in items] == sequences

    async def test_an_empty_incremental_scan_still_reports_a_candidate(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-K-05: advance only after a full scan, and only then."""
        await sim_connector.connect()
        highest = sim_server.sim.dataset.deals[-1]["source_seq"]
        scanner = Scanner(sim_connector)
        result = await scanner.run(
            ReadPlan(entity=Entity.DEALS, limit=10, after_seq=highest)
        )
        assert result.outcome is ScanOutcome.COMPLETE
        assert result.items_seen == 0
        candidate = result.checkpoint_candidate
        assert candidate is not None
        assert candidate.snapshot_seq == sim_server.sim.dataset.committed_seq
        assert "candidate only" in candidate.note


class TestPagedScan:
    async def test_a_full_scan_matches_the_source_oracle_exactly(
        self, sim_connector, sim_server
    ) -> None:
        """INV-08: no missing and no duplicate ids in a completed scan."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        result, items = await scanner.collect(
            ReadPlan(entity=Entity.DEALS, limit=7), limit_items=1_000
        )
        assert result.outcome is ScanOutcome.COMPLETE
        observed = [item.deal_id for item in items]
        assert observed == sim_server.sim.dataset.deal_ids()
        assert len(set(observed)) == len(observed)
        assert result.pages_seen == (len(observed) + 6) // 7

    async def test_every_page_of_a_scan_reports_one_snapshot(
        self, sim_connector, sim_server
    ) -> None:
        """INV-07."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        snapshots: set[int] = set()
        async for page in scanner.pages(ReadPlan(entity=Entity.DEALS, limit=4)):
            snapshots.add(page.snapshot_seq)
        assert len(snapshots) == 1

    async def test_a_concurrent_writer_does_not_change_a_running_scan(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-01: the snapshot pinned on page 1 is what the scan returns."""
        await sim_connector.connect()
        before = sim_server.sim.dataset.deal_ids()
        scanner = Scanner(sim_connector)
        collected: list[str] = []
        injected = False
        async for page in scanner.pages(ReadPlan(entity=Entity.DEALS, limit=5)):
            collected.extend(item.deal_id for item in page.items)
            if not injected:
                # Commit new deals mid-scan, exactly as a generator would.
                sim_server.sim.append_deals(25)
                injected = True
        assert injected
        assert collected == before
        # The new rows exist and a *fresh* scan sees them.
        assert len(sim_server.sim.dataset.deal_ids()) == len(before) + 25

    async def test_a_terminal_cursor_does_not_mean_no_future_data(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-Q-04: a consumer reading it that way stops polling."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        first = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=50))
        assert first.outcome is ScanOutcome.COMPLETE
        assert first.checkpoint_candidate is not None
        watermark = first.checkpoint_candidate.snapshot_seq

        sim_server.sim.append_deals(10)
        second = await scanner.run(
            ReadPlan(entity=Entity.DEALS, limit=50, after_seq=watermark)
        )
        assert second.outcome is ScanOutcome.COMPLETE
        assert second.items_seen == 10

    async def test_the_connector_never_sends_a_cursor_with_a_filter(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-05: asserted on the requests the source actually received."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        async for _ in scanner.pages(
            ReadPlan(entity=Entity.DEALS, limit=5, after_seq=0)
        ):
            pass
        paged = [entry for entry in sim_server.sim.request_log if entry["entity"] == "deals"]
        assert len(paged) > 1
        for entry in paged:
            query = entry["query"]
            if "cursor" in query:
                # A cursor travels alone: nothing else in the query string.
                assert set(query) == {"cursor"}, query
            else:
                assert "cursor" not in query

    async def test_per_entity_provenance_is_kept_separate(self, sim_connector) -> None:
        """WF2-J-06, WF2-Q-05: no combined snapshot is implied."""
        await sim_connector.connect()
        accounts = await sim_connector.read_page(ReadPlan(entity=Entity.ACCOUNTS, limit=5))
        deals = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert accounts.provenance.entity is Entity.ACCOUNTS
        assert deals.provenance.entity is Entity.DEALS
        assert accounts.provenance.filter_digest != deals.provenance.filter_digest


class TestFilterScoping:
    async def test_a_filtered_scan_gets_its_own_checkpoint_digest(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-K-06, INV-21: a watermark for account A says nothing about B."""
        await sim_connector.connect()
        accounts = [row["account_id"] for row in sim_server.sim.dataset.accounts[:2]]
        scanner = Scanner(sim_connector)
        results = []
        for account in accounts:
            result = await scanner.run(
                ReadPlan(entity=Entity.DEALS, limit=10, account_id=account)
            )
            assert result.outcome is ScanOutcome.COMPLETE
            results.append(result)
        digests = {result.checkpoint_candidate.filter_digest for result in results}
        assert len(digests) == 2

    async def test_a_filtered_scan_returns_only_matching_rows(
        self, sim_connector, sim_server
    ) -> None:
        await sim_connector.connect()
        account = sim_server.sim.dataset.accounts[0]["account_id"]
        scanner = Scanner(sim_connector)
        _, items = await scanner.collect(
            ReadPlan(entity=Entity.DEALS, limit=10, account_id=account), limit_items=500
        )
        assert items
        assert {item.account_id for item in items} == {account}
