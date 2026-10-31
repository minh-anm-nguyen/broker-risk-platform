"""P1 and P2 hardening: tenancy, ownership, time semantics, read-only scope.

Covers WF2-A-12, WF2-B-12, WF2-G-10, WF2-G-11, WF2-G-12, WF2-I-11, WF2-J-08,
WF2-J-09, WF2-J-11, WF2-K-10, WF2-L-12, WF2-P-08, WF2-P-10, WF2-Q-08,
WF2-Q-09, WF2-Q-11, WF2-Q-12 and WF2-N-11.

Several of these are *scope* tests rather than behaviour tests - "there is no
write method", "no two sources share a session", "the runbook says what thread
mode cannot do". They look trivial and are not: each one pins a property that
a later change could remove silently, and the design treats exactly those as
the ones worth pinning (ADR-209, ADR-212).
"""

from __future__ import annotations

import asyncio
import inspect
import sys
from pathlib import Path

import pytest

from mt5_connector.config import DATA_PLANE_PATHS
from mt5_connector.errors import SnapshotExpiredError
from mt5_connector.ports import Entity, MT5Client, ReadPlan, ScanOutcome
from mt5_connector.scan import Scanner

from ..conftest import fake_meta, make_connector, make_fake_driver, make_settings, ok, page_body

pytestmark = [pytest.mark.integration]

ROOT = Path(__file__).resolve().parents[2]


class TestReadOnlyScope:
    def test_the_port_exposes_no_write_or_trade_operation(self) -> None:
        """WF2-B-12, ADR-209: least privilege enforced by the interface."""
        methods = {
            name
            for name, _ in inspect.getmembers(MT5Client, predicate=callable)
            if not name.startswith("_")
        }
        assert methods == {"connect", "capabilities", "read_page", "close"}

        from mt5_connector.client import Connector

        forbidden = ("order", "trade", "write", "send", "modify", "delete")
        for name in dir(Connector):
            if name.startswith("_"):
                continue
            assert not any(word in name.lower() for word in forbidden), name

    def test_no_upstream_path_is_a_write_or_control_route(self) -> None:
        """ADR-212: the data plane cannot reach a control plane."""
        for path in DATA_PLANE_PATHS.values():
            assert path.startswith("/v1/")
            assert "admin" not in path
            assert "fault" not in path
            assert "reset" not in path

    def test_the_driver_protocol_declares_only_reads(self) -> None:
        from mt5_connector.drivers.base import BlockingDriverProtocol

        methods = {name for name in dir(BlockingDriverProtocol) if not name.startswith("_")}
        assert methods == {
            "open",
            "close",
            "fetch_meta",
            "fetch_accounts",
            "fetch_positions",
            "fetch_deals",
        }


class TestSessionOwnership:
    async def test_two_sources_never_share_a_session(self) -> None:
        """WF2-G-10, WF2-N-11: per-source ownership, with provenance to prove it."""
        first_driver = make_fake_driver(run_id="run_tenant_a")
        second_driver = make_fake_driver(run_id="run_tenant_b")
        first = await make_connector(
            first_driver, settings=make_settings(source_id="tenant-a")
        )
        second = await make_connector(
            second_driver, settings=make_settings(source_id="tenant-b")
        )
        try:
            assert first.supervisor.actor is not second.supervisor.actor
            assert first_driver.open_count == second_driver.open_count == 1

            page_a = await first.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
            page_b = await second.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
            # Each page names its own source, so a record cannot be attributed
            # to the wrong tenant downstream.
            assert page_a.provenance.source_id == "tenant-a"
            assert page_b.provenance.source_id == "tenant-b"
            assert page_a.provenance.run_id == "run_tenant_a"
            assert page_b.provenance.run_id == "run_tenant_b"
            # And each driver only ever saw its own worker thread.
            assert not (first_driver.threads_seen & second_driver.threads_seen)
        finally:
            await first.close()
            await second.close()

    async def test_no_orphan_tasks_survive_a_close(self) -> None:
        """WF2-G-11: a bounded shutdown leaves no task behind."""
        before = {task for task in asyncio.all_tasks() if not task.done()}
        driver = make_fake_driver()
        connector = await make_connector(driver)
        await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert await connector.close() is True
        await asyncio.sleep(0.05)
        leaked = {
            task
            for task in asyncio.all_tasks()
            if not task.done() and task not in before and task is not asyncio.current_task()
        }
        assert not leaked, [task.get_name() for task in leaked]

    async def test_a_call_from_a_thread_that_does_not_own_the_session_is_refused(
        self,
    ) -> None:
        """WF2-G-12, WF2-E-06: ownership is checked, not assumed."""
        from mt5_connector.config import Settings
        from mt5_connector.drivers.simulator_sync_http import SimulatorSyncHttpDriver
        from mt5_connector.errors import ConfigurationError

        settings = Settings(
            source_id="s1",
            base_url="http://127.0.0.1:9",
            source_kind="simulator",
            driver="simulator_sync_http",
        )
        driver = SimulatorSyncHttpDriver(settings, token="x" * 12)
        # Pretend the session was opened on a different thread.
        driver._owner_thread = -1  # type: ignore[attr-defined]
        driver._client = object()  # type: ignore[attr-defined]
        with pytest.raises(ConfigurationError) as excinfo:
            driver.fetch_deals(cursor=None, limit=5)
        assert excinfo.value.details["reason"] == "thread_ownership"


def deal_row(deal_id: str, *, source_seq: int, executed_at: str) -> dict:
    return {
        "deal_id": deal_id,
        "source_seq": source_seq,
        "account_id": "acct-0001",
        "position_id": "pos-00001",
        "symbol": "EURUSD",
        "entry": "IN",
        "side": "BUY",
        "volume_lots": "0.10",
        "price": "1.08500",
        "realized_pnl_gross": "0.00",
        "commission": "-0.70",
        "swap": "0.00",
        "executed_at": executed_at,
    }


class TestTimeSemantics:
    async def test_ordering_follows_the_sequence_not_the_business_timestamp(
        self,
    ) -> None:
        """WF2-J-09, WF2-K-10: a backdated deal is still captured."""
        rows = [
            # Later in sequence, *earlier* in business time: a correction that
            # a time-filtered scan would lose entirely.
            deal_row("deal-000001", source_seq=10, executed_at="2026-10-05T00:00:00.000Z"),
            deal_row("deal-000002", source_seq=14, executed_at="2026-10-01T00:00:00.000Z"),
        ]

        def responder(operation: str, params: dict):
            if operation == "fetch_meta":
                return ok(fake_meta())
            return ok(page_body(items=rows, snapshot_seq=100))

        driver = make_fake_driver(responder=responder)
        connector = await make_connector(driver)
        try:
            page = await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert [item.deal_id for item in page.items] == [
                "deal-000001",
                "deal-000002",
            ]
            # Both captured, and neither timestamp was rewritten to make the
            # ordering look tidy.
            assert page.items[0].executed_at > page.items[1].executed_at
            assert page.observed_upper_bound == 14
        finally:
            await connector.close()

    async def test_a_mark_timestamp_stays_separate_from_the_snapshot(
        self, sim_connector
    ) -> None:
        """WF2-J-08: a moving mark does not become part of the snapshot."""
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.POSITIONS, limit=10))
        marked = [item for item in page.items if item.mark_market_time is not None]
        assert marked, "the dataset must carry marks for this test to mean anything"
        for item in marked:
            assert item.mark_market_time.tzinfo is not None
            assert item.mark_stale in (True, False)
        assert page.snapshot_seq is not None

    async def test_the_three_notions_of_when_are_never_conflated(
        self, sim_connector
    ) -> None:
        """WF2-Q-12: source sequence, business time and read sequence."""
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        deal = page.items[0]
        assert deal.source_seq is not None
        assert deal.executed_at is not None
        assert page.snapshot_seq is not None
        assert page.provenance.snapshot_seq == page.snapshot_seq


class TestSnapshotLifecycle:
    async def test_a_cursor_expiring_mid_scan_restarts_without_skipping(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-11: restart from the safe watermark, lose nothing."""
        await sim_connector.connect()
        oracle = sim_server.sim.dataset.deal_ids()
        scanner = Scanner(sim_connector)
        pages = 0

        async def sink(page) -> None:
            nonlocal pages
            pages += 1
            if pages == 2:
                # The lease is revoked under the running scan.
                sim_server.sim.snapshot_ttl_s = -1.0

        first = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5), sink=sink)
        assert first.outcome is ScanOutcome.RESYNC_REQUIRED
        assert first.error_code == SnapshotExpiredError.code
        assert first.checkpoint_candidate is None

        # A fresh scan - the durable watermark never moved - converges on the
        # whole set, with nothing skipped over the restart.
        sim_server.sim.snapshot_ttl_s = 300.0
        second, items = await scanner.collect(
            ReadPlan(entity=Entity.DEALS, limit=5), limit_items=1_000
        )
        assert second.outcome is ScanOutcome.COMPLETE
        assert [item.deal_id for item in items] == oracle

    async def test_a_candidate_names_the_run_it_came_from(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-J-11: what lets a commit fence reject a cross-run watermark."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=50))
        assert result.outcome is ScanOutcome.COMPLETE
        candidate = result.checkpoint_candidate
        assert candidate is not None
        assert candidate.run_id == "run_0001"
        assert candidate.generation == 1

        sim_server.sim.reset_run()
        assert sim_server.sim.dataset.generation == 2
        # The candidate still names generation 1, so a consumer comparing it
        # against the live identity at commit time sees the mismatch.
        assert candidate.generation != sim_server.sim.dataset.generation


class TestDownstreamContract:
    async def test_amounts_cross_the_boundary_as_decimals_with_their_currency(
        self, sim_connector
    ) -> None:
        """WF2-Q-09: Project 05 must not receive a coerced float."""
        from decimal import Decimal

        await sim_connector.connect()
        accounts = await sim_connector.read_page(ReadPlan(entity=Entity.ACCOUNTS, limit=5))
        deals = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        for account in accounts.items:
            assert isinstance(account.balance, Decimal)
            assert isinstance(account.credit, Decimal)
            assert account.currency and account.currency.isupper()
        for deal in deals.items:
            for field in (
                "price",
                "volume_lots",
                "commission",
                "swap",
                "realized_pnl_gross",
            ):
                assert isinstance(getattr(deal, field), Decimal)

    async def test_a_page_carries_the_source_key_a_sink_needs(
        self, sim_connector
    ) -> None:
        """WF2-Q-11: the downstream dedup key is derivable from the page."""
        await sim_connector.connect()
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        for deal in page.items:
            assert deal.dedup_key == deal.deal_id
        assert page.provenance.source_id
        assert page.provenance.run_id
        assert page.provenance.entity is Entity.DEALS

    async def test_one_scan_reports_one_provenance(self, sim_connector) -> None:
        """WF2-Q-08: identity is bound per page, so a restart cannot blur it."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        provenances = set()
        async for page in scanner.pages(ReadPlan(entity=Entity.DEALS, limit=10)):
            provenances.add(
                (page.provenance.run_id, page.provenance.generation, page.snapshot_seq)
            )
        assert len(provenances) == 1

    def test_positions_are_never_coalesced_by_this_connector(self) -> None:
        """WF2-L-12: hedging versus netting is the source's semantics, not ours."""
        from mt5_connector import scan
        from mt5_connector.ports import Capabilities

        fields = set(Capabilities.__dataclass_fields__)
        assert not any("merge" in name for name in fields)
        source = inspect.getsource(scan)
        for word in ("coalesce", "merge_positions", "netting"):
            assert word not in source


class TestOperability:
    def test_the_runbook_states_what_thread_mode_cannot_do(self) -> None:
        """WF2-P-10: a drill against a wrong runbook is worse than no drill."""
        runbook = ROOT / "docs" / "incident_runbook.md"
        assert runbook.exists(), "the incident runbook is part of the deliverable"
        text = runbook.read_text(encoding="utf-8").lower()
        assert "thread" in text
        assert "cannot" in text or "not possible" in text
        assert "process" in text
        assert "quarantin" in text

    def test_resync_signalling_is_counted_not_alerted_per_event(self) -> None:
        """WF2-P-08: a thousand alerts lose the one that mattered."""
        from mt5_connector.telemetry import Metrics

        metrics = Metrics(source_id="s")
        for _ in range(1_000):
            metrics.snapshot_restart_total.labels("s", "source_epoch_changed").inc()
        rendered = metrics.render().decode()
        # One series carrying a count, not a thousand distinct events.
        assert rendered.count("connector_snapshot_restart_total{") <= 2
        assert "1000.0" in rendered or "1e+03" in rendered

    def test_the_interpreter_lanes_are_declared_and_ungated(self) -> None:
        """WF2-A-12: the 3.12 baseline and the 3.14 lane share one contract."""
        import tomllib

        pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        assert pyproject["project"]["requires-python"] == ">=3.12"
        assert sys.version_info >= (3, 12)
        # No behaviour here is gated on an interpreter version, so the adapter
        # contract is identical on both lanes.
        from mt5_connector import client, scan, supervisor

        for module in (client, scan, supervisor):
            source = inspect.getsource(module)
            assert "version_info" not in source
            assert "sys.version" not in source
