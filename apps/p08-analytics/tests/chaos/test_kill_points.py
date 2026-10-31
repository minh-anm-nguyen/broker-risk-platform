"""All 28 deterministic kill points of section 12.

Every test injects at exactly one named site, asserts the fault *actually
fired* (section 11: an uninjected fault is ``NOT_RUN``, not ``PASS``), and
then asserts the recovery property from the design's "Expected recovery"
column.

The recurring assertion is negative and deliberately so: after a crash at any
point, there must be no publication that nobody verified, and no money that
moved. A crash with correct recovery semantics is an acceptable outcome; a
believable wrong total is not (section 0).
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from analytics.errors import (
    AmbiguousWriteError,
    BuildState,
    Reason,
    ReportStatus,
    RetriableError,
)
from analytics.faults.killpoints import CrashError, FaultSite, KillPoint
from analytics.fixtures.golden import DEFAULT_TOPIC, fixture, generate_deals, specs_for

from helpers import (
    assert_matches_oracle,
    restart_consumer,
    ingest_all,
    produce,
    produce_specs,
    published_pointer,
    run_pipeline,
    stub_source,
)

pytestmark = pytest.mark.chaos

PARTITION = (DEFAULT_TOPIC, 0)


def _committed(stack) -> int:
    return stack.consumer.committed([PARTITION]).get(PARTITION, 0)


# ===========================================================================
# Ingest path: KP08-01 .. KP08-10
# ===========================================================================


class TestIngestKillPoints:
    def test_kp01_after_poll_before_validate(self, stack, faults):
        """No ClickHouse effect, no offset commit."""
        produce_specs(stack, specs_for("G08-01"))
        faults.crash_at(KillPoint.AFTER_POLL_BEFORE_VALIDATE)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.AFTER_POLL_BEFORE_VALIDATE)
        assert stack.clickhouse.raw_row_count() == 0
        assert _committed(stack) == 0

    def test_kp02_after_validate_before_insert(self, stack, faults):
        """Replay, no lost ids."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.crash_at(KillPoint.AFTER_VALIDATE_BEFORE_INSERT)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.AFTER_VALIDATE_BEFORE_INSERT)
        assert stack.clickhouse.raw_row_count() == 0
        assert _committed(stack) == 0
        # A restarted process seeks to the committed offset and replays.
        restart_consumer(stack).run_until_idle()
        assert stack.clickhouse.raw_row_count() == len(specs)

    def test_kp03_insert_sent_not_acknowledged(self, stack, faults):
        """Ambiguous: replay and let the build dedup semantically."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.raise_at(FaultSite.CH_INSERT_AMBIGUOUS, AmbiguousWriteError(), times=1)
        worker = stack.ingest_worker()
        outcome = worker.run_once()
        faults.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)
        assert outcome.ambiguous
        assert outcome.committed == {}
        worker.run_until_idle()
        assert _committed(stack) == len(specs)

    def test_kp04_persisted_but_response_dropped(self, stack, scope, faults):
        """Raw may duplicate; the money must not change."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.raise_at(FaultSite.CH_INSERT_AMBIGUOUS, AmbiguousWriteError(), times=1)
        worker = stack.ingest_worker()
        worker.run_once()
        faults.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)
        assert stack.clickhouse.raw_row_count() == len(specs), "rows were durable"
        worker.run_until_idle()
        assert stack.clickhouse.raw_row_count() == 2 * len(specs), "replay duplicated them"
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        assert len(outcome.dedup.deals) == len(specs)
        assert_matches_oracle(stack, outcome, scope, specs)

    def test_kp05_after_ack_before_offset_commit(self, stack, scope, faults):
        """The canonical window: duplicate delivery must be safe."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.crash_at(KillPoint.AFTER_INSERT_ACK_BEFORE_OFFSET_COMMIT)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.AFTER_INSERT_ACK_BEFORE_OFFSET_COMMIT)
        assert stack.clickhouse.raw_row_count() == len(specs)
        assert _committed(stack) == 0, "the offset must not have advanced"

        restart_consumer(stack).run_until_idle()
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert len(outcome.dedup.deals) == len(specs)
        assert_matches_oracle(stack, outcome, scope, specs)

    def test_kp06_during_offset_commit(self, stack, faults):
        """Re-read the group state; no missing ids."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.crash_at(KillPoint.DURING_OFFSET_COMMIT)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.DURING_OFFSET_COMMIT)
        assert _committed(stack) == 0
        restart_consumer(stack).run_until_idle()
        assert _committed(stack) == len(specs)

    def test_kp07_after_offset_ack_before_local_receipt(self, stack, faults):
        """No extra local checkpoint is required to be correct."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.crash_at(KillPoint.AFTER_OFFSET_ACK_BEFORE_LOCAL_RECEIPT)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.AFTER_OFFSET_ACK_BEFORE_LOCAL_RECEIPT)
        assert _committed(stack) == len(specs), "the broker-side commit is the source of truth"
        assert restart_consumer(stack).run_once().polled == 0

    def test_kp08_poison_detected_before_receipt(self, stack, faults):
        """Pause the partition; never skip."""
        produce(stack, fixture("G08-17").envelopes)
        faults.crash_at(KillPoint.AFTER_POISON_DETECT_BEFORE_RECEIPT)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.AFTER_POISON_DETECT_BEFORE_RECEIPT)
        assert stack.control.list_quarantine() == []
        assert _committed(stack) == 0

    def test_kp09_after_quarantine_commit_before_offset_ack(self, stack, faults):
        """The receipt is idempotent, so the replay is controlled."""
        produce(stack, fixture("G08-17").envelopes)
        faults.crash_at(KillPoint.AFTER_QUARANTINE_COMMIT_BEFORE_OFFSET_ACK)
        with pytest.raises(CrashError):
            stack.ingest_worker().run_once()
        faults.assert_fired(KillPoint.AFTER_QUARANTINE_COMMIT_BEFORE_OFFSET_ACK)
        receipts = stack.control.list_quarantine()
        assert len(receipts) == 1
        assert _committed(stack) == 0

        restart_consumer(stack).run_until_idle()
        assert len(stack.control.list_quarantine()) == 1, "no duplicate receipt on replay"

    def test_kp10_rebalance_during_insert(self, stack, faults):
        """The old assignment cannot commit."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        faults.call_at(
            FaultSite.CH_INSERT_AMBIGUOUS,
            lambda ctx: stack.consumer.rebalance(stack.broker.partitions_of(DEFAULT_TOPIC)),
            times=1,
        )
        outcome = stack.ingest_worker().run_once()
        faults.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)
        assert outcome.committed == {}
        assert _committed(stack) == 0


# ===========================================================================
# Build path: KP08-11 .. KP08-26
# ===========================================================================


class TestBuildKillPoints:
    @pytest.fixture
    def ingested(self, stack):
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        return specs

    def _crash_build(self, stack, scope, specs, site):
        with pytest.raises(CrashError):
            stack.build_worker(stub_source(stack, specs)).run(scope)
        return published_pointer(stack, scope)

    def test_kp11_before_lease_claim(self, stack, scope, ingested, faults):
        """The scheduler can retry safely; nothing was created."""
        faults.crash_at(KillPoint.BUILD_REQUESTED_BEFORE_LEASE)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.BUILD_REQUESTED_BEFORE_LEASE)
        assert stack.control.list_builds(tenant_id=scope.tenant_id) == []

    def test_kp12_after_lease_before_manifest(self, stack, scope, ingested, faults):
        """A new owner can retry once the lease lapses."""
        faults.crash_at(KillPoint.AFTER_LEASE_BEFORE_MANIFEST)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_LEASE_BEFORE_MANIFEST)
        builds = stack.control.list_builds(tenant_id=scope.tenant_id)
        assert len(builds) == 1
        assert builds[0].state is BuildState.REQUESTED

        stack.clock.advance(seconds=stack.settings.build_lease_seconds + 1)
        retry = stack.build_worker(stub_source(stack, ingested)).run(scope)
        assert retry.state is BuildState.PUBLISHED
        assert retry.attempt_no == 2

    def test_kp13_after_manifest_before_raw_scan(self, stack, scope, ingested, faults):
        """The cut is pinned; retry or let the lease expire."""
        faults.crash_at(KillPoint.AFTER_MANIFEST_BEFORE_RAW_SCAN)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_MANIFEST_BEFORE_RAW_SCAN)
        assert stack.control.list_builds(tenant_id=scope.tenant_id)[0].state is BuildState.BUILDING

    def test_kp14_midway_through_conflict_scan(self, stack, scope, ingested, faults):
        """No partial publish; the candidate is incomplete."""
        faults.crash_at(KillPoint.MIDWAY_CONFLICT_SCAN)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.MIDWAY_CONFLICT_SCAN)
        physical, _ = stack.clickhouse.count_deal_facts(
            build_id=stack.control.list_builds(tenant_id=scope.tenant_id)[0].build_id,
            tenant_id=scope.tenant_id,
        )
        assert physical == 0

    def test_kp15_after_conflict_scan_before_facts(self, stack, scope, ingested, faults):
        """The pinned cut must be re-checked, so no facts exist yet."""
        faults.crash_at(KillPoint.AFTER_CONFLICT_SCAN_BEFORE_FACTS)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_CONFLICT_SCAN_BEFORE_FACTS)
        assert stack.clickhouse.storage_stats().table_rows[
            "broker_analytics.analytics_deals_build"
        ] == 0

    def test_kp16_after_ten_percent_of_facts(self, stack, scope, faults):
        """An incomplete build is never queryable as current."""
        specs = list(generate_deals(50, seed=1616))
        produce_specs(stack, specs)
        ingest_all(stack)
        faults.crash_at(KillPoint.AFTER_10PCT_FACTS)
        with pytest.raises(CrashError):
            stack.build_worker(stub_source(stack, specs)).run(scope)
        faults.assert_fired(KillPoint.AFTER_10PCT_FACTS)
        assert published_pointer(stack, scope) is None

    def test_kp17_after_ninety_percent_of_facts(self, stack, scope, faults):
        """No count-based READY: 90% written is still not verified."""
        specs = list(generate_deals(50, seed=1717))
        produce_specs(stack, specs)
        ingest_all(stack)
        faults.crash_at(KillPoint.AFTER_90PCT_FACTS)
        with pytest.raises(CrashError):
            stack.build_worker(stub_source(stack, specs)).run(scope)
        faults.assert_fired(KillPoint.AFTER_90PCT_FACTS)
        assert published_pointer(stack, scope) is None

    def test_kp18_after_facts_before_identity_proof(self, stack, scope, ingested, faults):
        """No publish without the oracle."""
        faults.crash_at(KillPoint.AFTER_FACTS_BEFORE_IDENTITY_PROOF)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_FACTS_BEFORE_IDENTITY_PROOF)
        assert published_pointer(stack, scope) is None

    def test_kp19_after_facts_verified_before_rollup(self, stack, scope, ingested, faults):
        """The old pointer stays active."""
        first, _ = run_pipeline(stack, ingested, scope)
        assert first.state is BuildState.PUBLISHED
        faults.crash_at(KillPoint.AFTER_FACTS_VERIFIED_BEFORE_ROLLUP)
        with pytest.raises(CrashError):
            stack.build_worker(stub_source(stack, ingested)).run(replace(scope, cut_id="cut-2"))
        faults.assert_fired(KillPoint.AFTER_FACTS_VERIFIED_BEFORE_ROLLUP)
        assert published_pointer(stack, scope).current_build_id == first.build_id

    def test_kp20_after_partial_rollup(self, stack, scope, ingested, faults):
        """Abandon the generation; the old report is still valid."""
        first, _ = run_pipeline(stack, ingested, scope)
        faults.crash_at(KillPoint.AFTER_PARTIAL_ROLLUP)
        with pytest.raises(CrashError):
            stack.build_worker(stub_source(stack, ingested)).run(replace(scope, cut_id="cut-2"))
        faults.assert_fired(KillPoint.AFTER_PARTIAL_ROLLUP)
        assert published_pointer(stack, scope).current_build_id == first.build_id

    def test_kp21_after_rollup_before_money_oracle(self, stack, scope, ingested, faults):
        """Cannot publish without independent totals."""
        faults.crash_at(KillPoint.AFTER_ROLLUP_BEFORE_MONEY_ORACLE)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_ROLLUP_BEFORE_MONEY_ORACLE)
        assert published_pointer(stack, scope) is None

    def test_kp22_after_oracles_before_evidence(self, stack, scope, ingested, faults):
        """No publish without durable evidence."""
        faults.crash_at(KillPoint.AFTER_ORACLES_BEFORE_EVIDENCE)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_ORACLES_BEFORE_EVIDENCE)
        build = stack.control.list_builds(tenant_id=scope.tenant_id)[0]
        assert stack.control.get_evidence(build_id=build.build_id) == []
        assert published_pointer(stack, scope) is None

    def test_kp23_after_evidence_before_verified(self, stack, scope, ingested, faults):
        """Retry the CAS under an active lease; evidence survives."""
        faults.crash_at(KillPoint.AFTER_EVIDENCE_BEFORE_VERIFIED)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_EVIDENCE_BEFORE_VERIFIED)
        build = stack.control.list_builds(tenant_id=scope.tenant_id)[0]
        assert stack.control.get_evidence(build_id=build.build_id) != []
        assert build.state is BuildState.VALIDATING
        assert published_pointer(stack, scope) is None

    def test_kp24_after_verified_before_pointer_tx(self, stack, scope, ingested, faults):
        """Safe retry, or abandon if the cut changed."""
        faults.crash_at(KillPoint.AFTER_VERIFIED_BEFORE_POINTER_TX)
        assert self._crash_build(stack, scope, ingested, None) is None
        faults.assert_fired(KillPoint.AFTER_VERIFIED_BEFORE_POINTER_TX)
        build = stack.control.list_builds(tenant_id=scope.tenant_id)[0]
        assert build.state is BuildState.VERIFIED
        assert build.evidence_sha256
        assert published_pointer(stack, scope) is None

        # A later attempt on a new cut publishes; the verified one is history.
        retry = stack.build_worker(stub_source(stack, ingested)).run(
            replace(scope, cut_id="cut-2")
        )
        assert retry.state is BuildState.PUBLISHED

    def test_kp25_pointer_commit_ack_lost(self, stack, scope, ingested, faults):
        """Read the pointer before retrying; exactly one revision."""
        faults.raise_at(FaultSite.PG_COMMIT_ACK_LOST, RuntimeError("ack lost"), times=1)
        with pytest.raises(RuntimeError):
            stack.build_worker(stub_source(stack, ingested)).run(scope)
        faults.assert_fired(FaultSite.PG_COMMIT_ACK_LOST)
        pointer = published_pointer(stack, scope)
        assert pointer is not None and pointer.publication_revision == 1
        history = stack.control.publication_history(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        )
        assert len(history) == 1

    def test_kp26_after_publish_before_cache_invalidation(self, stack, scope, ingested, faults,
                                                          reader):
        """The cache is keyed by revision, so no mixed version is served."""
        faults.crash_at(KillPoint.AFTER_PUBLISH_BEFORE_CACHE_INVALIDATION)
        with pytest.raises(CrashError):
            stack.build_worker(stub_source(stack, ingested)).run(scope)
        faults.assert_fired(KillPoint.AFTER_PUBLISH_BEFORE_CACHE_INVALIDATION)
        pointer = published_pointer(stack, scope)
        assert pointer is not None
        # A fresh reader resolves the pointer and sees the published build.
        current = stack.query.current_publication(
            reader, source_id=scope.source_id, epoch_id=scope.epoch_id
        )
        assert current["build_id"] == pointer.current_build_id


# ===========================================================================
# Read / maintenance path: KP08-27 .. KP08-28
# ===========================================================================


class TestReadAndGcKillPoints:
    def test_kp27_clickhouse_killed_during_a_read(self, stack, scope, reader, faults):
        """Retry or error; never fabricate a partial 200."""
        specs = specs_for("G08-01")
        run_pipeline(stack, specs, scope)
        faults.raise_at(
            KillPoint.CH_NODE_KILLED_DURING_READ,
            RetriableError(Reason.BACKEND_UNAVAILABLE, "node gone"),
            times=1,
        )
        from datetime import date

        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        faults.assert_fired(KillPoint.CH_NODE_KILLED_DURING_READ)
        assert report.items == ()
        assert report.envelope.status is ReportStatus.DEGRADED
        assert report.envelope.reason == str(Reason.BACKEND_UNAVAILABLE)

    def test_kp28_gc_deleting_while_a_build_is_acquired(self, stack, scope, faults):
        """GC must exclude referenced and in-use rows."""
        specs = specs_for("G08-01")
        first, _ = run_pipeline(stack, specs, scope)
        assert first.state is BuildState.PUBLISHED

        # The published build must never be collectable.
        plan = stack.gc.plan(tenant_id=scope.tenant_id)
        assert first.build_id not in plan.collectable
        assert plan.protected[first.build_id] == "CURRENT_PUBLICATION"

        faults.call_at(KillPoint.GC_DELETING_WHILE_BUILD_ACQUIRED, lambda ctx: None, times=None)
        deleted = stack.gc.collect(tenant_id=scope.tenant_id)
        assert first.build_id not in deleted
        physical, _ = stack.clickhouse.count_deal_facts(
            build_id=first.build_id, tenant_id=scope.tenant_id
        )
        assert physical == len(specs), "the published build's rows survived GC"

    def test_gc_refuses_a_build_with_an_open_reader_pin(self, stack, scope):
        """P08-T-08: a cursor mid-pagination is still reading."""
        specs = specs_for("G08-01")
        first, _ = run_pipeline(stack, specs, scope)
        second = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-2")
        )
        assert second.state is BuildState.PUBLISHED
        assert stack.control.get_build(first.build_id).state is BuildState.SUPERSEDED

        stack.control.register_reader_pin(build_id=first.build_id, pin_id="open-cursor")
        plan = stack.gc.plan(tenant_id=scope.tenant_id)
        assert plan.protected[first.build_id] == "ACTIVE_READER_PIN"
        with pytest.raises(Exception) as excinfo:
            stack.gc.assert_collectable(first.build_id)
        assert excinfo.value.reason is Reason.GC_PROTECTED
