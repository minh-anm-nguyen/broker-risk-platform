"""Settings fail-fast, cursor binding and the fault plan's own audit.

Grouped because all three are infrastructure for the safety rules rather than
business logic: configuration that cannot be unsafe (section 16.2), cursors
that cannot cross a build (P08-T-02), and a fault plan that cannot let a
scenario claim PASS without firing (section 11).
"""

from __future__ import annotations

import pytest

from analytics.errors import ConfigurationError, QueryError, Reason
from analytics.faults.killpoints import (
    ALL_KILL_POINTS,
    CrashError,
    FaultPlan,
    KillPoint,
    NullFaultPlan,
    kill_point_catalog,
)
from analytics.query.cursors import CursorCodec, CursorPayload, filter_digest
from analytics.settings import VERSION_PLACEHOLDER, Settings

pytestmark = pytest.mark.unit


class TestSettings:
    def test_defaults_are_bounded(self):
        settings = Settings(env="test")
        assert settings.kafka_max_batch_records <= 50_000
        assert settings.query_max_in_flight <= 4_096
        assert settings.kafka_auto_offset_reset == "none"
        assert settings.kafka_auto_offset_commit is False

    def test_auto_offset_reset_cannot_be_latest(self):
        """Section 16.2: resetting to latest hides a retention gap."""
        with pytest.raises(Exception):
            Settings(env="test", kafka_auto_offset_reset="latest")

    def test_async_insert_cannot_be_enabled_untested(self):
        with pytest.raises(ConfigurationError):
            Settings(env="test", ch_synchronous_insert_ack_required=False)

    def test_prod_requires_a_pinned_clickhouse_version(self):
        with pytest.raises(ConfigurationError, match="ch_server_version_pin"):
            Settings(
                env="prod",
                tenant_map=("broker.deals.v1:s=t",),
                reader_token="r" * 40,
                operator_token="o" * 40,
                cursor_signing_key="k" * 40,
            )

    def test_prod_requires_a_tenant_map(self):
        """There is no default tenant, so an empty map is fatal."""
        with pytest.raises(ConfigurationError, match="tenant_map"):
            Settings(
                env="prod",
                ch_server_version_pin="25.8",
                reader_token="r" * 40,
                operator_token="o" * 40,
                cursor_signing_key="k" * 40,
            )

    def test_prod_requires_distinct_tokens(self):
        with pytest.raises(ConfigurationError, match="must differ"):
            Settings(
                env="prod",
                ch_server_version_pin="25.8",
                tenant_map=("broker.deals.v1:s=t",),
                reader_token="same-token-value-padded-out-to-length",
                operator_token="same-token-value-padded-out-to-length",
                cursor_signing_key="k" * 40,
            )

    def test_prod_requires_a_cursor_key(self):
        with pytest.raises(ConfigurationError, match="cursor_signing_key"):
            Settings(
                env="prod",
                ch_server_version_pin="25.8",
                tenant_map=("broker.deals.v1:s=t",),
                reader_token="r" * 40,
                operator_token="o" * 40,
            )

    def test_non_loopback_bind_requires_a_strong_token(self):
        with pytest.raises(ConfigurationError, match="non-loopback"):
            Settings(
                env="prod",
                host="0.0.0.0",
                ch_server_version_pin="25.8",
                tenant_map=("broker.deals.v1:s=t",),
                reader_token="short",
                operator_token="o" * 40,
                cursor_signing_key="k" * 40,
            )

    def test_page_size_bounds_must_be_consistent(self):
        with pytest.raises(ConfigurationError):
            Settings(env="test", query_default_page_size=500, query_max_page_size=100)

    def test_empty_topic_allowlist_is_refused(self):
        with pytest.raises(Exception):
            Settings(env="test", kafka_topic_allowlist=())

    def test_placeholder_version_is_the_documented_sentinel(self):
        assert Settings(env="test").ch_server_version_pin == VERSION_PLACEHOLDER


class TestTenantMapping:
    def test_parses_entries(self):
        settings = Settings(
            env="test", tenant_map=("broker.deals.v1:sim-01=tenant-a,broker.deals.v1:sim-02=tenant-b",)
        )
        mapping = settings.tenant_mapping()
        assert mapping[("broker.deals.v1", "sim-01")] == "tenant-a"
        assert mapping[("broker.deals.v1", "sim-02")] == "tenant-b"

    def test_resolver_returns_none_for_unknown(self):
        settings = Settings(env="test", tenant_map=("broker.deals.v1:sim-01=tenant-a",))
        resolve = settings.tenant_resolver()
        assert resolve("broker.deals.v1", "sim-01") == "tenant-a"
        assert resolve("broker.deals.v1", "sim-99") is None
        assert resolve("broker.other.v1", "sim-01") is None

    @pytest.mark.parametrize("entry", ["no-equals", "missing-colon=tenant", "topic:=tenant", "topic:s="])
    def test_malformed_entry_is_refused(self, entry):
        with pytest.raises(ConfigurationError):
            Settings(env="test", tenant_map=(entry,)).tenant_mapping()

    def test_conflicting_mapping_is_refused(self):
        settings = Settings(
            env="test", tenant_map=("t:s=tenant-a,t:s=tenant-b",)
        )
        with pytest.raises(ConfigurationError, match="two tenants"):
            settings.tenant_mapping()


class TestCursors:
    @pytest.fixture
    def codec(self):
        return CursorCodec(signing_key="unit-test-signing-key-0123456789")

    def test_round_trips(self, codec):
        payload = CursorPayload("t", "b", "f", ("2026-10-30", "D-1"), 1)
        token = codec.encode(payload)
        decoded = codec.decode(token, expected_tenant="t", expected_build="b", expected_filter="f")
        assert decoded == payload

    def test_requires_a_signing_key(self):
        with pytest.raises(ConfigurationError):
            CursorCodec(signing_key="")

    def test_tampered_payload_fails_verification(self, codec):
        token = codec.encode(CursorPayload("t", "b", "f", ("x",), 0))
        key_id, body, signature = token.split(".")
        forged = f"{key_id}.{body[:-2]}XY.{signature}"
        with pytest.raises(QueryError) as excinfo:
            codec.decode(forged, expected_tenant="t", expected_build="b", expected_filter="f")
        assert excinfo.value.reason is Reason.CURSOR_INVALID

    def test_other_tenant_cursor_is_a_scope_violation(self, codec):
        """A valid signature over another tenant's cursor is still forbidden."""
        token = codec.encode(CursorPayload("other", "b", "f", ("x",), 0))
        with pytest.raises(QueryError) as excinfo:
            codec.decode(token, expected_tenant="t", expected_build="b", expected_filter="f")
        assert excinfo.value.reason is Reason.FORBIDDEN_SCOPE

    def test_cursor_cannot_cross_builds(self, codec):
        """P08-T-02: pagination must not drift across a pointer flip."""
        token = codec.encode(CursorPayload("t", "build-a", "f", ("x",), 0))
        with pytest.raises(QueryError) as excinfo:
            codec.decode(
                token, expected_tenant="t", expected_build="build-b", expected_filter="f"
            )
        assert excinfo.value.reason is Reason.CURSOR_BUILD_MISMATCH

    def test_cursor_cannot_cross_filters(self, codec):
        token = codec.encode(CursorPayload("t", "b", "filter-a", ("x",), 0))
        with pytest.raises(QueryError) as excinfo:
            codec.decode(
                token, expected_tenant="t", expected_build="b", expected_filter="filter-b"
            )
        assert excinfo.value.reason is Reason.CURSOR_INVALID

    @pytest.mark.parametrize("bad", ["", "a.b", "a.b.c.d", "not-a-cursor", "..", "a.!!.c"])
    def test_malformed_cursor_is_refused(self, codec, bad):
        with pytest.raises(QueryError):
            codec.decode(bad, expected_tenant="t", expected_build="b", expected_filter="f")

    def test_previous_key_verifies_during_rotation(self):
        old = CursorCodec(signing_key="old-key-padded-out-for-length-0123")
        token = old.encode(CursorPayload("t", "b", "f", ("x",), 0))
        rotated = CursorCodec(
            signing_key="new-key-padded-out-for-length-0123",
            previous_key="old-key-padded-out-for-length-0123",
        )
        assert rotated.decode(
            token, expected_tenant="t", expected_build="b", expected_filter="f"
        ).last_key == ("x",)

    def test_rotation_without_the_old_key_rejects(self):
        old = CursorCodec(signing_key="old-key-padded-out-for-length-0123")
        token = old.encode(CursorPayload("t", "b", "f", ("x",), 0))
        rotated = CursorCodec(signing_key="new-key-padded-out-for-length-0123")
        with pytest.raises(QueryError):
            rotated.decode(token, expected_tenant="t", expected_build="b", expected_filter="f")

    def test_filter_digest_ignores_none_values(self):
        assert filter_digest({"a": 1, "b": None}) == filter_digest({"a": 1})

    def test_filter_digest_detects_a_change(self):
        assert filter_digest({"a": 1}) != filter_digest({"a": 2})


class TestFaultPlan:
    def test_all_28_kill_points_are_defined(self):
        """Section 12's inventory, complete and named."""
        assert len(ALL_KILL_POINTS) == 28
        ids = {str(point) for point in ALL_KILL_POINTS}
        assert ids == {f"KP08-{n:02d}" for n in range(1, 29)}
        assert len(kill_point_catalog()) == 28

    def test_crash_is_not_catchable_as_exception(self):
        """A kill point swallowed by ``except Exception`` tests nothing."""
        plan = FaultPlan()
        plan.crash_at(KillPoint.AFTER_90PCT_FACTS)
        with pytest.raises(CrashError):
            try:
                plan.fire(KillPoint.AFTER_90PCT_FACTS)
            except Exception:  # noqa: BLE001 - the point of the test
                pytest.fail("CrashError must not be catchable as Exception")

    def test_assert_fired_fails_when_the_fault_never_triggered(self):
        """Section 11: a fault that never fired is NOT_RUN, not PASS."""
        plan = FaultPlan()
        plan.crash_at(KillPoint.AFTER_10PCT_FACTS)
        with pytest.raises(AssertionError, match="never fired"):
            plan.assert_fired(KillPoint.AFTER_10PCT_FACTS)

    def test_assert_fired_passes_after_firing(self):
        plan = FaultPlan()
        plan.raise_at(KillPoint.AFTER_10PCT_FACTS, ValueError("boom"))
        with pytest.raises(ValueError):
            plan.fire(KillPoint.AFTER_10PCT_FACTS)
        plan.assert_fired(KillPoint.AFTER_10PCT_FACTS)

    def test_times_limits_repetition(self):
        plan = FaultPlan()
        plan.raise_at(KillPoint.DURING_OFFSET_COMMIT, ValueError("once"), times=1)
        with pytest.raises(ValueError):
            plan.fire(KillPoint.DURING_OFFSET_COMMIT)
        plan.fire(KillPoint.DURING_OFFSET_COMMIT)  # retired
        assert plan.fired[str(KillPoint.DURING_OFFSET_COMMIT)] == 1

    def test_when_predicate_narrows_the_target(self):
        plan = FaultPlan()
        plan.raise_at(
            KillPoint.DURING_OFFSET_COMMIT,
            ValueError("only offset 10"),
            when=lambda ctx: ctx.get("offset") == 10,
        )
        plan.fire(KillPoint.DURING_OFFSET_COMMIT, offset=9)
        with pytest.raises(ValueError):
            plan.fire(KillPoint.DURING_OFFSET_COMMIT, offset=10)

    def test_visits_are_recorded_even_without_a_fault(self):
        plan = FaultPlan()
        plan.fire(KillPoint.AFTER_POLL_BEFORE_VALIDATE)
        assert plan.visits[str(KillPoint.AFTER_POLL_BEFORE_VALIDATE)] == 1
        assert plan.fired.get(str(KillPoint.AFTER_POLL_BEFORE_VALIDATE), 0) == 0

    def test_timeline_redacts_payloads(self):
        plan = FaultPlan()
        plan.call_at(KillPoint.AFTER_VALIDATE_BEFORE_INSERT, lambda ctx: None)
        plan.fire(KillPoint.AFTER_VALIDATE_BEFORE_INSERT, rows=[1, 2, 3], epoch=1)
        entry = plan.timeline()[0]
        assert entry["context"]["rows"] == "<3 items>"
        assert entry["context"]["epoch"] == 1

    def test_pending_reports_unfired_schedules(self):
        plan = FaultPlan()
        plan.crash_at(KillPoint.AFTER_PARTIAL_ROLLUP)
        assert plan.pending() == [str(KillPoint.AFTER_PARTIAL_ROLLUP)]

    def test_null_plan_cannot_be_armed(self):
        plan = NullFaultPlan()
        with pytest.raises(RuntimeError):
            plan.crash_at(KillPoint.AFTER_10PCT_FACTS)
        plan.fire(KillPoint.AFTER_10PCT_FACTS)  # never raises

    def test_armed_reports_pending_state(self):
        plan = FaultPlan()
        assert not plan.armed(KillPoint.AFTER_10PCT_FACTS)
        plan.crash_at(KillPoint.AFTER_10PCT_FACTS)
        assert plan.armed(KillPoint.AFTER_10PCT_FACTS)
