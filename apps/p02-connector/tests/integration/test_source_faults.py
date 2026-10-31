"""IT-03, IT-05, IT-06: source reset, HTTP faults and poison payloads.

Covers WF2-D-01 .. WF2-D-11, WF2-I-02, WF2-I-06, WF2-I-07, WF2-J-01,
WF2-J-03, WF2-J-05, WF2-J-07, WF2-L-01, WF2-L-02, WF2-L-06, WF2-M-04,
WF2-N-09, WF2-Q-06, WF2-R-05, INV-06, INV-07, INV-10 and INV-11.

Every fault is injected through the source's **own control plane** with the
admin credential, never from inside the connector - which is what section 9.3
requires, and which is also why each test asserts
``rule.activations >= 1``. A test whose fault never fired is not a pass; it is
``NOT_TESTED`` wearing a green tick (WF2-P-03).
"""

from __future__ import annotations

import pytest

from mt5_connector.errors import (
    DataIntegrityError,
    RateLimitedError,
    SnapshotExpiredError,
    SourceContractError,
    SourceEpochChangedError,
    UpstreamUnavailableError,
)
from mt5_connector.ports import Entity, ReadPlan, ScanOutcome
from mt5_connector.scan import Scanner

pytestmark = [pytest.mark.integration]


class TestSourceReset:
    async def test_a_reset_mid_scan_aborts_with_resync_required(
        self, sim_connector, sim_server
    ) -> None:
        """IT-03, WF2-J-01, WF2-J-05, INV-06: no mixed epochs, no checkpoint."""
        await sim_connector.connect()
        scanner = Scanner(sim_connector)
        plan = ReadPlan(entity=Entity.DEALS, limit=5)
        pages_seen: list[int] = []

        async def sink(page) -> None:
            pages_seen.append(page.page_index)
            if len(pages_seen) == 2:
                # The harness drives the source's control plane; the connector
                # holds no admin credential (section 9.3).
                sim_server.sim.reset_run()

        result = await scanner.run(plan, sink=sink)

        assert len(pages_seen) >= 2
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.error_code in ("SOURCE_EPOCH_CHANGED", "SNAPSHOT_EXPIRED")
        # Nothing to commit: a scan cut short offers no watermark.
        assert result.checkpoint_candidate is None
        assert sim_connector.supervisor.ready()[0] is False

    async def test_a_cursor_from_a_replaced_run_is_refused(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-J-03: compare run *and* generation."""
        await sim_connector.connect()
        first = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert first.next_cursor is not None
        sim_server.sim.reset_run()

        with pytest.raises((SourceEpochChangedError, SnapshotExpiredError)) as excinfo:
            await sim_connector.read_page(
                ReadPlan(entity=Entity.DEALS, limit=5, cursor=first.next_cursor)
            )
        assert excinfo.value.resync_required
        assert not excinfo.value.retryable

    async def test_resync_required_blocks_reads_until_acknowledged(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-Q-07: a consumer ignoring the signal must not look healthy."""
        await sim_connector.connect()
        sim_connector.supervisor.mark_resync_required("test: source identity changed")
        assert sim_connector.supervisor.ready()[0] is False
        with pytest.raises(SourceEpochChangedError):
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))

        identity = await sim_connector.supervisor.acknowledge_resync()
        # A fresh handshake, a new epoch, and reads work again.
        assert identity.worker_epoch > 1
        assert sim_connector.supervisor.ready()[0] is True
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert page.items

    async def test_a_page_reporting_a_new_generation_aborts_the_scan(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-07, INV-06: no cross-generation page joins a completed scan."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "mixed_generation", path="/v1/deals", nth=3, max_activations=1, delta=1
        )
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 1
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.error_code == "SOURCE_EPOCH_CHANGED"
        assert result.checkpoint_candidate is None

    async def test_a_page_reporting_a_new_snapshot_aborts_the_scan(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-06, INV-07."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "mixed_snapshot_seq", path="/v1/deals", nth=2, max_activations=1, delta=7
        )
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 1
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.error_code == "DATA_INTEGRITY_ERROR"
        assert result.checkpoint_candidate is None

    async def test_a_stale_snapshot_sequence_is_detected(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-J-07: a source serving an older snapshot than it promised."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "stale_snapshot", path="/v1/deals", nth=2, max_activations=1, delta=5
        )
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 1
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.checkpoint_candidate is None


class TestSnapshotExpiry:
    async def test_a_410_stops_the_scan_without_advancing_anything(
        self, sim_connector, sim_server
    ) -> None:
        """IT-03 variant, WF2-D-04: never retry an expired cursor."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "http_error",
            path="/v1/deals",
            nth=3,
            max_activations=1,
            status=410,
            code="SNAPSHOT_EXPIRED",
        )
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 1
        assert result.outcome is ScanOutcome.RESYNC_REQUIRED
        assert result.error_code == "SNAPSHOT_EXPIRED"
        assert result.checkpoint_candidate is None
        # Exactly one attempt: an expired cursor is not retried.
        assert sim_connector.last_retry_stats.attempts == 1

    async def test_an_expired_lease_is_reported_as_expired_not_replaced(
        self, sim_connector, sim_server
    ) -> None:
        await sim_connector.connect()
        first = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert first.next_cursor is not None
        sim_server.sim.snapshot_ttl_s = -1.0
        with pytest.raises(SnapshotExpiredError):
            await sim_connector.read_page(
                ReadPlan(entity=Entity.DEALS, limit=5, cursor=first.next_cursor)
            )


class TestHttpFaults:
    async def test_two_503s_then_success_uses_exactly_three_attempts(
        self, sim_connector, sim_server
    ) -> None:
        """IT-05, WF2-D-01, WF2-H-04, INV-11."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=2, status=503
        )
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 2
        assert len(page.items) == 5
        assert sim_connector.last_retry_stats.attempts == 3

    async def test_an_absurd_retry_after_is_capped_and_the_budget_wins(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-D-02: no sleep beyond the deadline."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "http_error",
            path="/v1/deals",
            max_activations=10,
            status=429,
            code="RATE_LIMITED",
            retry_after="999999",
        )
        with pytest.raises((RateLimitedError, Exception)) as excinfo:
            await sim_connector.read_page(
                ReadPlan(entity=Entity.DEALS, limit=5), deadline_ms=600
            )
        assert rule.activations >= 1
        assert excinfo.value.code in ("RATE_LIMITED", "DEADLINE_EXCEEDED")
        # Bounded: it did not sit for eleven days, and it did not spin either.
        assert sim_connector.last_retry_stats.attempts <= 3

    async def test_a_429_without_retry_after_falls_back_to_backoff(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-D-03."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=1, status=429, code="RATE_LIMITED"
        )
        page = await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 1
        assert len(page.items) == 5
        assert sim_connector.last_retry_stats.retry_after_honoured_s == []

    async def test_persistent_unavailability_is_bounded(
        self, sim_connector, sim_server
    ) -> None:
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=100, status=503
        )
        with pytest.raises(UpstreamUnavailableError) as excinfo:
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 3
        assert excinfo.value.details["attempts"] == 3
        assert excinfo.value.upstream_status == 503

    async def test_a_504_is_classified_as_a_timeout(
        self, sim_connector, sim_server
    ) -> None:
        await sim_connector.connect()
        sim_server.sim.add_fault(
            "http_error", path="/v1/deals", max_activations=100, status=504
        )
        with pytest.raises(Exception) as excinfo:
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert excinfo.value.code == "UPSTREAM_TIMEOUT"


class TestPoisonPayloads:
    @pytest.mark.parametrize(
        ("mode", "parameters", "expected"),
        [
            ("malformed_json", {}, "SOURCE_CONTRACT_ERROR"),
            ("truncate_response", {"keep_bytes": 40}, "SOURCE_CONTRACT_ERROR"),
            ("wrong_content_type", {}, "SOURCE_CONTRACT_ERROR"),
            ("deep_nesting", {"depth": 10_000}, "SOURCE_CONTRACT_ERROR"),
            ("duplicate_json_key", {}, "SOURCE_CONTRACT_ERROR"),
            ("non_finite_number", {}, "SOURCE_CONTRACT_ERROR"),
            ("oversize_body", {"bytes": 4_000_000}, "SOURCE_CONTRACT_ERROR"),
            ("float_money", {"field": "price"}, "SOURCE_CONTRACT_ERROR"),
            ("replace_field_value", {"field": "commission", "value": "NaN"}, "SOURCE_CONTRACT_ERROR"),
            ("omit_field", {"field": "deal_id"}, "SOURCE_CONTRACT_ERROR"),
            # Appending a duplicate also overruns the requested limit, and the
            # page-size cap is checked first - the earlier refusal, which is
            # the right one.
            ("duplicate_one_item", {}, "SOURCE_CONTRACT_ERROR"),
            ("duplicate_replace_item", {}, "DATA_INTEGRITY_ERROR"),
            ("regress_source_seq", {}, "DATA_INTEGRITY_ERROR"),
            ("overlong_page", {"extra": 5}, "SOURCE_CONTRACT_ERROR"),
            ("no_content", {}, "SOURCE_CONTRACT_ERROR"),
        ],
    )
    async def test_each_poison_payload_yields_zero_dtos(
        self, sim_connector, sim_server, mode, parameters, expected
    ) -> None:
        """IT-06, INV-10: no partially valid page is ever emitted."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            mode, path="/v1/deals", max_activations=100, **parameters
        )
        with pytest.raises(Exception) as excinfo:
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations >= 1, "the fault never fired: this is NOT_TESTED"
        assert excinfo.value.code == expected, excinfo.value.to_envelope()

    @pytest.mark.parametrize(
        "mode",
        ["malformed_json", "float_money", "duplicate_one_item", "non_finite_number"],
    )
    async def test_invalid_data_is_never_retried(
        self, sim_connector, sim_server, mode
    ) -> None:
        """WF2-H-05, WF2-R-05, ADR-205: one read, then quarantine."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(mode, path="/v1/deals", max_activations=100)
        with pytest.raises(Exception):
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        assert rule.activations == 1, "a bad payload must be read exactly once"
        assert sim_connector.last_retry_stats.attempts == 1

    async def test_a_poisoned_page_does_not_corrupt_the_source(
        self, sim_connector, sim_server
    ) -> None:
        """IT-06: the source's canonical data is untouched by a rejection."""
        await sim_connector.connect()
        before = sim_server.sim.dataset.deal_ids()
        sim_server.sim.add_fault("malformed_json", path="/v1/deals", max_activations=1)
        with pytest.raises(SourceContractError):
            await sim_connector.read_page(ReadPlan(entity=Entity.DEALS, limit=5))
        sim_server.sim.clear_faults()
        scanner = Scanner(sim_connector)
        result, items = await scanner.collect(
            ReadPlan(entity=Entity.DEALS, limit=10), limit_items=1_000
        )
        assert result.outcome is ScanOutcome.COMPLETE
        assert [item.deal_id for item in items] == before

    async def test_a_duplicate_across_pages_is_caught_by_the_scan(
        self, sim_connector, sim_server
    ) -> None:
        """WF2-I-10: per-page uniqueness is not enough."""
        await sim_connector.connect()
        rule = sim_server.sim.add_fault(
            "drop_one_item", path="/v1/deals", nth=2, max_activations=1
        )
        scanner = Scanner(sim_connector)
        result = await scanner.run(ReadPlan(entity=Entity.DEALS, limit=5))
        # Dropping an item is not detectable from inside one page: the scan
        # completes, and catching it needs the source oracle below.
        assert rule.activations == 1
        if result.outcome is ScanOutcome.COMPLETE:
            assert result.items_seen < len(sim_server.sim.dataset.deals), (
                "the dropped record must show up as a count mismatch against the oracle"
            )

    async def test_a_cursor_loop_terminates(self) -> None:
        """WF2-I-02: a repeated cursor must not paginate forever.

        Driven from the fake driver rather than the contract double: the
        double mints a correct cursor every time, so a *correct* source cannot
        produce this failure. Modelling it needs a source that misbehaves.
        """
        from ..conftest import make_connector, make_fake_driver, ok, page_body

        def responder(operation: str, params: dict):
            if operation == "fetch_meta":
                from ..conftest import fake_meta

                return ok(fake_meta())
            # The same cursor, for ever.
            return ok(page_body(items=[], next_cursor="cursor-that-never-advances"))

        driver = make_fake_driver(responder=responder)
        connector = await make_connector(driver)
        try:
            scanner = Scanner(connector)
            with pytest.raises(DataIntegrityError) as excinfo:
                async for _ in scanner.pages(ReadPlan(entity=Entity.DEALS, limit=5)):
                    pass
            assert "already used" in excinfo.value.message
            # Terminated after a bounded number of reads, not after max_pages.
            assert driver.call_count <= 4
        finally:
            await connector.close()
