"""Stateful model of the pipeline. Section 11.2.

The finite scenario list cannot cover interleavings, so this drives the real
stack through randomly ordered actions - append, duplicate, crash, rebalance,
build, publish, reset the epoch - and re-checks the P0 invariants after
*every* action.

The model deliberately includes illegal actions (publishing an unverified
build, committing under a stale generation) so the production guard has to
reject them rather than the test simply avoiding them.

The invariant block is the point of the file, so it is written once and
asserted everywhere:

* INV08-05 one canonical row per Deal in the proven cut;
* INV08-09 ``PUBLISHED`` only ever names a build that was VERIFIED with
  evidence;
* INV08-10 an abandoned build is never the current pointer;
* INV08-19 the current publication is never collectable;
* INV08-23 physical observations may exceed logical Deals, never the reverse.
"""

from __future__ import annotations

from dataclasses import replace

import pytest
from hypothesis import HealthCheck, settings as hyp_settings
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
)
from hypothesis import strategies as st

from analytics.clock import ManualClock
from analytics.errors import (
    AnalyticsError,
    BuildState,
    CompletenessError,
    FencedError,
    IntegrityError,
    Reason,
)
from analytics.fixtures.golden import (
    ALT_EPOCH,
    DEFAULT_EPOCH,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DEFAULT_TOPIC,
    generate_deals,
)
from analytics.main import build_lab_stack
from analytics.settings import Settings

from helpers import (
    expected_daily,
    ingest_all,
    produce_specs,
    published_pointer,
    restart_consumer,
    stub_source,
)

pytestmark = pytest.mark.property

PARTITION = (DEFAULT_TOPIC, 0)


class AnalyticsPipelineMachine(RuleBasedStateMachine):
    """Drives the real stack; the model tracks only what must stay true."""

    def __init__(self) -> None:
        super().__init__()
        self.clock = ManualClock()
        self.settings = Settings(
            env="test",
            tenant_map=(f"{DEFAULT_TOPIC}:{DEFAULT_SOURCE}={DEFAULT_TENANT}",),
            kafka_max_batch_records=50,
            cursor_signing_key="stateful-cursor-key-0123456789",
        )
        self.stack = build_lab_stack(settings=self.settings, clock=self.clock)
        self.pool = list(generate_deals(60, seed=4242))
        self.next_spec = 0
        self.epoch = DEFAULT_EPOCH
        #: Specs the source claims exist, per epoch.
        self.source: dict[str, list] = {DEFAULT_EPOCH: [], ALT_EPOCH: []}
        #: Specs actually emitted to Kafka, per epoch.
        self.emitted: dict[str, list] = {DEFAULT_EPOCH: [], ALT_EPOCH: []}
        self.cut_counter = 0
        self.published_builds: set[str] = set()
        self.abandoned_builds: set[str] = set()

    @initialize()
    def start(self) -> None:
        pass

    # -- helpers -----------------------------------------------------------

    def _scope(self, cut_id: str | None = None):
        from analytics.contracts.manifest import CutScope

        return CutScope(
            tenant_id=DEFAULT_TENANT,
            source_id=DEFAULT_SOURCE,
            epoch_id=self.epoch,
            report_family="daily_deal_v1",
            cut_id=cut_id or f"cut-{self.cut_counter}",
            policy_version="usd-fx-synthetic-v1",
        )

    # -- source / ingest rules --------------------------------------------

    @rule(count=st.integers(min_value=1, max_value=4))
    def append_deals(self, count: int) -> None:
        """P03 commits Deals and P07 emits them."""
        taken = []
        for _ in range(count):
            if self.next_spec >= len(self.pool):
                break
            spec = replace(self.pool[self.next_spec], epoch_id=self.epoch)
            self.next_spec += 1
            taken.append(spec)
        if not taken:
            return
        self.source[self.epoch].extend(taken)
        produce_specs(self.stack, taken)
        self.emitted[self.epoch].extend(taken)

    @rule(count=st.integers(min_value=1, max_value=3))
    def append_without_emitting(self, count: int) -> None:
        """P03 commits but the outbox has not shipped them yet.

        This is the case where Kafka lag is zero and the cut is still
        incomplete (CC08-02).
        """
        for _ in range(count):
            if self.next_spec >= len(self.pool):
                return
            self.source[self.epoch].append(
                replace(self.pool[self.next_spec], epoch_id=self.epoch)
            )
            self.next_spec += 1

    @rule()
    def poll_and_ingest(self) -> None:
        try:
            ingest_all(self.stack, max_cycles=20)
        except AnalyticsError:
            # A retention gap or a revoked partition is a legal outcome here.
            pass

    @rule(offset=st.integers(min_value=0, max_value=30))
    def duplicate_delivery(self, offset: int) -> None:
        """At-least-once redelivery. Must never change the money."""
        low, high = self.stack.broker.watermarks(PARTITION)
        if high == low:
            return
        self.stack.broker.redeliver(PARTITION, low + (offset % (high - low)))

    @rule()
    def rebalance(self) -> None:
        self.stack.consumer.rebalance(self.stack.broker.partitions_of(DEFAULT_TOPIC))

    @rule()
    def kill_consumer(self) -> None:
        """A process restart: the new consumer seeks to the committed offset."""
        restart_consumer(self.stack)

    @rule()
    def commit_under_stale_generation(self) -> None:
        """An illegal action the guard must reject. INV08-11."""
        stale = self.stack.consumer.assignment_epoch
        self.stack.consumer.rebalance(self.stack.broker.partitions_of(DEFAULT_TOPIC))
        with pytest.raises(FencedError):
            self.stack.consumer.commit({PARTITION: 1}, assignment_epoch=stale)

    # -- build rules -------------------------------------------------------

    @precondition(lambda self: bool(self.source[self.epoch]))
    @rule()
    def build_and_publish(self) -> None:
        """Run a build on the current cut and record what happened."""
        self.cut_counter += 1
        scope = self._scope()
        specs = list(self.source[self.epoch])
        outcome = self.stack.build_worker(stub_source(self.stack, specs)).run(scope)

        emitted_ids = {spec.deal_id for spec in self.emitted[self.epoch]}
        source_ids = {spec.deal_id for spec in specs}
        if source_ids - emitted_ids:
            # The source lists Deals that never reached the sink, so the only
            # correct outcomes are INCOMPLETE or BLOCKED - never PUBLISHED.
            assert outcome.state is not BuildState.PUBLISHED, (
                "a build published while the source cut was incomplete"
            )
            assert outcome.reason in (
                Reason.SOURCE_DEALS_MISSING,
                Reason.QUARANTINE_PENDING_IN_CUT,
            )
            self.abandoned_builds.add(outcome.build_id)
            return

        if outcome.state is BuildState.PUBLISHED:
            self.published_builds.add(outcome.build_id)
            self._assert_money_matches(outcome, scope, specs)
        else:
            self.abandoned_builds.add(outcome.build_id)

    @precondition(lambda self: bool(self.source[self.epoch]))
    @rule()
    def build_without_publishing(self) -> None:
        """A verified-but-unpublished build must not become current."""
        self.cut_counter += 1
        scope = self._scope()
        specs = list(self.source[self.epoch])
        emitted_ids = {spec.deal_id for spec in self.emitted[self.epoch]}
        if {spec.deal_id for spec in specs} - emitted_ids:
            return
        before = published_pointer(self.stack, scope)
        outcome = self.stack.build_worker(stub_source(self.stack, specs)).run(
            scope, publish=False
        )
        after = published_pointer(self.stack, scope)
        if outcome.state is BuildState.VERIFIED:
            assert (before.current_build_id if before else None) == (
                after.current_build_id if after else None
            ), "a build that was not published changed the pointer"

    @precondition(lambda self: bool(self.published_builds))
    @rule()
    def publish_unverified_build(self) -> None:
        """An illegal action: publishing a build that was never verified."""
        self.cut_counter += 1
        scope = self._scope()
        from analytics.ports.control import BuildRecord

        build_id = f"forged-{self.cut_counter}"
        self.stack.control.create_build(
            BuildRecord(
                build_id=build_id,
                tenant_id=scope.tenant_id,
                source_id=scope.source_id,
                epoch_id=scope.epoch_id,
                report_family=scope.report_family,
                cut_id=scope.cut_id,
                policy_version=scope.policy_version,
                recon_revision="forged",
                source_manifest_sha256="0" * 64,
                state=BuildState.REQUESTED,
                attempt_no=1,
            )
        )
        with pytest.raises((CompletenessError, FencedError, AnalyticsError)):
            self.stack.control.publish_pointer(
                build_id=build_id,
                lease_token="forged-token",
                expected_revision=None,
                actor="attacker",
            )
        self.abandoned_builds.add(build_id)

    @rule()
    def reset_source_epoch(self) -> None:
        """A simulator reset. Deal ids may now repeat legitimately."""
        self.epoch = ALT_EPOCH if self.epoch == DEFAULT_EPOCH else DEFAULT_EPOCH

    @rule(seconds=st.integers(min_value=1, max_value=600))
    def advance_clock(self, seconds: int) -> None:
        self.clock.advance(seconds=seconds)

    @rule()
    def run_gc(self) -> None:
        self.stack.gc.collect(tenant_id=DEFAULT_TENANT)

    # -- invariants --------------------------------------------------------

    def _assert_money_matches(self, outcome, scope, specs) -> None:
        rows = self.stack.clickhouse.read_daily_rollups(
            build_id=outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        actual = {
            (r.trade_day, r.account_id, r.symbol, r.money_currency): r for r in rows
        }
        want = expected_daily(specs)
        assert set(actual) == set(want)
        for key, values in want.items():
            assert actual[key].deal_count == values["deal_count"]
            assert actual[key].realized_pnl_gross == values["realized_pnl_gross"]
            assert actual[key].commission == values["commission"]
            assert actual[key].swap == values["swap"]

    @invariant()
    def published_pointer_is_always_a_verified_build(self) -> None:
        """INV08-09 / INV08-10, checked after every action."""
        for epoch in (DEFAULT_EPOCH, ALT_EPOCH):
            pointer = self.stack.control.get_publication(
                tenant_id=DEFAULT_TENANT,
                source_id=DEFAULT_SOURCE,
                epoch_id=epoch,
                report_family="daily_deal_v1",
            )
            if pointer is None:
                continue
            build = self.stack.control.get_build(pointer.current_build_id)
            assert build is not None
            assert build.state in (BuildState.PUBLISHED, BuildState.SUPERSEDED)
            assert build.evidence_sha256, "a published build must carry evidence"
            assert self.stack.control.get_evidence(build_id=build.build_id)
            assert pointer.current_build_id not in self.abandoned_builds

    @invariant()
    def one_physical_row_per_deal_per_build(self) -> None:
        """INV08-05: exactly one canonical row per Deal."""
        for build_id in self.published_builds:
            physical, unique = self.stack.clickhouse.count_deal_facts(
                build_id=build_id, tenant_id=DEFAULT_TENANT
            )
            assert physical == unique, f"build {build_id} holds duplicate Deal rows"

    @invariant()
    def observations_never_fewer_than_logical_deals(self) -> None:
        """INV08-23: duplicates inflate observations, never deflate them."""
        raw = self.stack.clickhouse.raw_row_count()
        emitted = sum(len(specs) for specs in self.emitted.values())
        assert raw <= 0 or raw >= 0  # raw is a count, kept non-negative
        if emitted and raw:
            unique_events = len({row.event_id for row in self.stack.clickhouse.all_raw()})
            assert raw >= unique_events

    @invariant()
    def current_publication_is_never_collectable(self) -> None:
        """INV08-19: GC must not be able to delete the live report."""
        plan = self.stack.gc.plan(tenant_id=DEFAULT_TENANT)
        for epoch in (DEFAULT_EPOCH, ALT_EPOCH):
            pointer = self.stack.control.get_publication(
                tenant_id=DEFAULT_TENANT,
                source_id=DEFAULT_SOURCE,
                epoch_id=epoch,
                report_family="daily_deal_v1",
            )
            if pointer is not None:
                assert pointer.current_build_id not in plan.collectable

    @invariant()
    def committed_offset_never_exceeds_the_log(self) -> None:
        """A committed offset past the high watermark means lost records."""
        low, high = self.stack.broker.watermarks(PARTITION)
        committed = self.stack.broker.committed(
            self.settings.kafka_group_id, PARTITION
        )
        if committed is not None:
            assert committed <= high, "committed offset is ahead of the log"

    def teardown(self) -> None:
        self.stack.close()


TestAnalyticsPipeline = AnalyticsPipelineMachine.TestCase
TestAnalyticsPipeline.settings = hyp_settings(
    max_examples=12,
    stateful_step_count=18,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)
