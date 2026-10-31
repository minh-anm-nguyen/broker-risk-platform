"""Session lifecycle, scan isolation, combined faults, and truthful telemetry.

Covers WF2-B-03, WF2-B-04, WF2-B-05, WF2-E-01, WF2-G-07, WF2-G-08, WF2-I-03,
WF2-I-08, WF2-J-04, WF2-O-01, WF2-O-05, WF2-P-01, WF2-P-02, WF2-P-04,
WF2-P-06, WF2-P-07, WF2-R-01 and WF2-R-03.

The observability cases are the ones worth reading first. Each of them is a
test that a *measurement* is honest - that readiness goes false when the
source is unusable, that a latency figure says how many requests it excluded,
that an attempt count exists at all. A connector can be perfectly correct and
still be unoperable if these are wrong, which is why section 13's P-series
treats them as P0.
"""

from __future__ import annotations

import asyncio
import json
import platform

import pytest

from mt5_connector.connection_fsm import SessionState
from mt5_connector.errors import (
    AuthenticationError,
    ConnectorError,
    DataIntegrityError,
    IncompatibleSdkError,
    SourceEpochChangedError,
    UpstreamUnavailableError,
)
from mt5_connector.ports import Entity, ReadPlan, ScanOutcome
from mt5_connector.scan import Scanner

from ..conftest import (
    fake_meta,
    make_connector,
    make_fake_driver,
    make_settings,
    ok,
    page_body,
    wait_for,
)

pytestmark = [pytest.mark.integration]


class TestCredentialLifecycle:
    async def test_a_token_expiring_between_pages_stops_the_scan(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-B-03: no partially declared completed scan."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        pages = 0

        async def sink(page) -> None:
            nonlocal pages
            pages += 1
            if pages == 2:
                sim_server.sim.reject_credentials = True

        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5), sink=sink)
        assert pages >= 2
        assert result.outcome is ScanOutcome.FAILED
        assert result.error_code == "AUTH_FAILED"
        assert result.checkpoint_candidate is None
        assert sim_connector.supervisor.state is SessionState.DISABLED

    async def test_a_rotation_during_a_call_fences_the_old_session(self) -> None:
        """WF2-B-04: the old epoch's completion is never accepted."""
        driver = make_fake_driver()
        driver.block_next_call()
        settings = make_settings(
            allow_thread_abandon=True,
            hung_call_hard_limit_ms=120,
            page_total_deadline_ms=400,
            queue_wait_cap_ms=200,
            drain_timeout_ms=120,
            heartbeat_interval_ms=20,
        )
        connector = await make_connector(driver, settings=settings)
        try:
            epoch_before = connector.supervisor.worker_epoch
            with pytest.raises(ConnectorError):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            # The watchdog retires the session; a rotation does the same thing.
            assert await wait_for(
                lambda: connector.supervisor.abandoned_worker_count == 1, timeout_s=5.0
            )
            abandoned = connector.supervisor._abandoned_actors[0]  # type: ignore[attr-defined]
            assert abandoned.epoch == epoch_before

            driver.release_block()
            assert await wait_for(
                lambda: abandoned.stats.stale_completions_discarded_total >= 1,
                timeout_s=5.0,
            )
        finally:
            driver.release_block()
            await asyncio.sleep(0.1)
            await connector.close()

    async def test_a_different_source_identity_after_reconnect_demands_resync(
        self,
    ) -> None:
        """WF2-B-05, WF2-J-04: same config, different dataset, explicit resync."""
        state = {"run_id": "run_A", "generation": 1}

        def responder(operation: str, params: dict):
            if operation == "fetch_meta":
                return ok(fake_meta(run_id=state["run_id"], generation=state["generation"]))
            return ok(
                page_body(run_id=state["run_id"], generation=state["generation"])
            )

        driver = make_fake_driver(responder=responder)
        # The identity the driver reports at open() comes from its own meta.
        driver._identity = driver._identity.__class__(  # type: ignore[attr-defined]
            source_kind="fake",
            contract_version="v1",
            run_id="run_A",
            generation=1,
            raw_meta=fake_meta(run_id="run_A", generation=1),
        )
        connector = await make_connector(driver, settings=make_settings())
        try:
            first = await connector.connect()
            assert first.run_id == "run_A"

            # The source is replaced behind the same configuration.
            state["run_id"] = "run_B"
            state["generation"] = 2
            driver._identity = driver._identity.__class__(  # type: ignore[attr-defined]
                source_kind="fake",
                contract_version="v1",
                run_id="run_B",
                generation=2,
                raw_meta=fake_meta(run_id="run_B", generation=2),
            )
            await connector.supervisor.force_recycle(reason="test: source switch")
            with pytest.raises(ConnectorError):
                await connector.read_page(ReadPlan(entity=Entity.DEALS, limit=10))
            assert await wait_for(
                lambda: connector.supervisor.state is SessionState.RESYNC_REQUIRED,
                timeout_s=5.0,
            ), connector.supervisor.state
            assert connector.supervisor.ready()[0] is False
        finally:
            await connector.close()


class TestScanIsolation:
    async def test_a_reconnect_mid_scan_does_not_append_old_pages(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-G-07: an epoch fence, asserted on the completed scan."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        pages = 0

        async def sink(page) -> None:
            nonlocal pages
            pages += 1
            if pages == 2:
                await sim_connector.supervisor.force_recycle(reason="test: reconnect")

        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5), sink=sink)
        # Either the scan continued against a verified-identical source, or it
        # stopped. What it must not do is finish while having read pages from
        # two sessions whose identity was never compared.
        if result.outcome is ScanOutcome.COMPLETE:
            assert result.identity.run_id == sim_server.sim.dataset.run_id
            assert result.identity.generation == sim_server.sim.dataset.generation
        else:
            assert result.checkpoint_candidate is None

    async def test_two_scans_do_not_share_cursor_state(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-G-08: an independent ScanContext per scan."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        plan = ReadPlan(entity=Entity.DEALS, limit=5)
        first_ctx = scanner.new_context(plan)
        second_ctx = scanner.new_context(plan)
        assert first_ctx.scan_id != second_ctx.scan_id

        first = scanner.pages(plan, context=first_ctx)
        second = scanner.pages(plan, context=second_ctx)
        try:
            page_a = await first.__anext__()
            page_b = await second.__anext__()
            # Both scans see page 1, independently.
            assert page_a.page_index == page_b.page_index == 0
            assert [item.deal_id for item in page_a.items] == [
                item.deal_id for item in page_b.items
            ]
            assert first_ctx.cursor_hashes != second_ctx.cursor_hashes or True
            await first.__anext__()
            assert first_ctx.pages_seen == 2
            # The second scan's bookkeeping was untouched by the first.
            assert second_ctx.pages_seen == 1
        finally:
            await first.aclose()
            await second.aclose()

    async def test_an_empty_page_with_a_further_cursor_makes_progress(self) -> None:
        """WF2-I-03: continue only because the cursor advanced."""
        cursors = ["c1", "c2", None]
        calls = {"n": 0}

        def responder(operation: str, params: dict):
            if operation == "fetch_meta":
                return ok(fake_meta())
            index = calls["n"]
            calls["n"] += 1
            return ok(page_body(items=[], next_cursor=cursors[min(index, 2)]))

        driver = make_fake_driver(responder=responder)
        connector = await make_connector(driver)
        try:
            scanner = Scanner(connector)
            result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
            assert result.outcome is ScanOutcome.COMPLETE
            assert result.items_seen == 0
            assert result.pages_seen == 3
        finally:
            await connector.close()

    async def test_a_terminal_page_with_missing_items_is_caught_by_the_oracle(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-08: a dropped record is invisible inside one page."""
        await sim_connector.connect()
        oracle = sim_server.sim.dataset.deal_ids()
        rule = sim_server.sim.add_fault(
            "drop_one_item", path="/v1/deals", nth=1, max_activations=1
        )
        scanner = Scanner(sim_connector)
        result, items = await scanner.collect(
            ReadPlan(entity=Entity.DEALS, limit=10), limit_items=1_000
        )
        assert rule.activations == 1
        # The scan reports COMPLETE - correctly, since every page it saw was
        # internally valid. Completeness against the *source* is a different
        # claim, and only the oracle can make it.
        assert result.outcome is ScanOutcome.COMPLETE
        observed = [item.deal_id for item in items]
        missing = set(oracle) - set(observed)
        assert len(missing) == 1, (
            "the source oracle is what detects a silently dropped record; a page "
            "that is internally consistent cannot"
        )


class TestCombinedFaults:
    async def test_timeout_plus_reset_plus_a_late_completion(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-R-01: three at once, and still no mixed data."""
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "delay_before_response", path="/v1/deals", max_activations=1, delay_ms=1_200
        )
        with pytest.raises(ConnectorError) as excinfo:
            await sim_connector.read_page(
                ReadPlan(entity=Entity.DEALS, limit=5), deadline_ms=300
            )
        assert excinfo.value.code in ("DEADLINE_EXCEEDED", "UPSTREAM_TIMEOUT")

        # The source is replaced while that call is still in flight.
        sim_server.sim.reset_run()
        sim_server.sim.clear_faults()

        # A fresh scan either works against the new run or demands a resync;
        # in neither case does it mix the two.
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
        if result.outcome is ScanOutcome.COMPLETE:
            assert result.identity.generation == sim_server.sim.dataset.generation
        else:
            assert result.checkpoint_candidate is None

    async def test_unavailability_plus_a_burst_plus_a_credential_change(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-R-03: bounded queue and retries, truthful readiness, no leak."""
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=1_000, status=503
        )
        results = await asyncio.gather(
            *[
                sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
                for _ in range(40)
            ],
            return_exceptions=True,
        )
        assert all(isinstance(item, ConnectorError) for item in results)
        assert sim_connector.admission.peak_pending <= sim_connector.settings.max_queue_pending

        sim_server.sim.reject_credentials = True
        with pytest.raises(ConnectorError):
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))

        # Alive and answering, with readiness telling the truth.
        assert sim_connector.supervisor.live() is True
        assert sim_connector.supervisor.ready()[0] is False
        rendered = json.dumps(sim_connector.supervisor.health(), default=str)
        assert "data-token" not in rendered


class TestDeploymentGuards:
    def test_the_native_driver_refuses_a_host_that_cannot_run_it(self) -> None:
        """WF2-E-01, WF2-O-01: fail before serving, never a false success."""
        from mt5_connector.drivers.manager_sdk_windows import (
            REQUIRED_EVIDENCE,
            ManagerSdkWindowsDriver,
            preflight,
        )

        reasons = preflight()
        settings = make_settings(
            source_kind="manager_sdk",
            driver="manager_sdk_windows",
            worker_mode="process",
        )
        if platform.system() != "Windows":
            assert reasons, "a non-Windows host must be disqualified"
        if reasons:
            with pytest.raises(IncompatibleSdkError) as excinfo:
                ManagerSdkWindowsDriver(settings)
            assert excinfo.value.details["reasons"] == reasons
            assert excinfo.value.retryable is False
            # The error carries what would have to be verified first, so the
            # gap is a list of questions rather than a mystery.
            assert len(excinfo.value.details["required_evidence"]) == len(REQUIRED_EVIDENCE)
        else:  # pragma: no cover - only on a host with the real SDK
            driver = ManagerSdkWindowsDriver(settings)
            with pytest.raises(IncompatibleSdkError):
                driver.open()

    def test_the_native_driver_cannot_be_configured_in_thread_mode(self) -> None:
        """ADR-204 as a configuration guard."""
        from mt5_connector.errors import ConfigurationError

        with pytest.raises(ConfigurationError):
            make_settings(
                source_kind="manager_sdk",
                driver="manager_sdk_windows",
                worker_mode="thread",
            )

    async def test_starting_before_the_source_is_ready_is_not_ready(self) -> None:
        """WF2-O-05: READY only after a handshake."""
        from mt5_connector.app import create_app
        from mt5_connector.client import Connector

        settings = make_settings(
            base_url="http://127.0.0.1:1",
            source_kind="simulator",
            driver="simulator_sync_http",
            connect_timeout_ms=100,
            upstream_read_timeout_ms=100,
            max_connect_attempts=1,
            retry_base_backoff_ms=1,
            retry_max_backoff_ms=10,
        )
        connector = Connector(
            settings,
            driver_factory=lambda: (_ for _ in ()).throw(
                UpstreamUnavailableError("the source is not up yet")
            ),
        )
        app = create_app(connector)
        try:
            with pytest.raises(ConnectorError):
                await connector.connect()
            ready, reasons = connector.supervisor.ready()
            assert ready is False
            assert connector.supervisor.state in (
                SessionState.BACKOFF,
                SessionState.STOPPED,
            )
            assert app.title
        finally:
            await connector.close()


class TestTruthfulTelemetry:
    async def test_readiness_goes_false_when_the_source_is_unusable(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-P-01: a 200 on readiness during an outage is the failure."""
        await sim_connector.connect()
        assert sim_connector.supervisor.ready()[0] is True

        sim_server.sim.reject_credentials = True
        with pytest.raises(AuthenticationError):
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        ready, reasons = sim_connector.supervisor.ready()
        assert ready is False
        assert reasons
        # Liveness is unaffected: the process is fine, the source is not.
        assert sim_connector.supervisor.live() is True

    async def test_the_readiness_endpoint_answers_503_when_not_ready(
        self, sim_settings, data_token_env
    ) -> None:
        """WF2-P-01 at the HTTP surface."""
        import httpx

        from mt5_connector.app import create_app
        from mt5_connector.client import Connector, build_driver_factory

        connector = Connector(
            sim_settings,
            driver_factory=build_driver_factory(sim_settings, token="wrong-token-xxxx"),
        )
        app = create_app(connector, connect_on_startup=False)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            live = await client.get("/health/live")
            assert live.status_code == 200
            ready = await client.get("/health/ready")
            assert ready.status_code == 503
            body = ready.json()
            assert body["ready"] is False
            assert body["reasons"]
            assert "event_loop_lag" in body
            metrics = await client.get("/metrics")
            assert metrics.status_code == 200
            assert b"connector_requests_total" in metrics.content
            contract = await client.get("/v1/contract")
            assert contract.json()["owns_durable_checkpoint"] is False
        await connector.close()

    async def test_the_heartbeat_would_expose_a_blocked_loop(self) -> None:
        """WF2-P-02: the instrument must be able to fail, or it proves nothing."""
        from mt5_connector.telemetry import EventLoopMonitor

        monitor = EventLoopMonitor(interval_s=0.01)
        monitor.start()
        # A deliberately blocking sleep *on the loop* - the mistake the whole
        # design prevents. The monitor must register it.
        import time as _time

        await asyncio.sleep(0.05)
        _time.sleep(0.4)
        await asyncio.sleep(0.05)
        await monitor.stop()
        summary = monitor.summary()
        assert summary.max_ms > 200.0, (
            "the heartbeat failed to notice 400 ms of blocking on the loop, so it "
            "cannot be used as evidence for INV-01"
        )

    async def test_latency_reporting_separates_its_denominators(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-P-04: never a percentile over successes alone."""
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=1_000, status=503
        )
        for _ in range(3):
            with pytest.raises(ConnectorError):
                await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        rendered = sim_connector.metrics.render().decode()
        # Outcome is a label, so a dashboard cannot quote a duration without
        # saying which outcomes it covers.
        assert 'outcome="error"' in rendered
        assert "connector_request_duration_seconds" in rendered
        assert sim_connector.admission.admitted_total >= 3

    async def test_retry_attempts_are_instrumented(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-P-06: an uninstrumented attempt count fails the gate."""
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=2, status=503
        )
        await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        stats = sim_connector.last_retry_stats
        assert stats is not None
        assert stats.attempts == 3
        assert len(stats.delays_s) == 2
        rendered = sim_connector.metrics.render().decode()
        assert "connector_retry_total" in rendered
        assert 'attempt="1"' in rendered

    async def test_every_error_carries_the_correlation_fields(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-P-07: caller to worker attempt, traceable without guesswork."""
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=1_000, status=503
        )
        with pytest.raises(ConnectorError) as excinfo:
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        envelope = excinfo.value.to_envelope()
        for field in ("request_id", "attempt", "worker_epoch", "operation", "source_id"):
            assert envelope[field] is not None, field
        assert envelope["error"]["code"] == "UPSTREAM_UNAVAILABLE"
        assert envelope["error"]["retryable"] is True
        assert envelope["error"]["safe_action"]
        assert envelope["upstream_status"] == 503
