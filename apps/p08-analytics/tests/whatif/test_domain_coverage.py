"""What-if coverage across the 44 failure domains of section 10.

**What this file is.** The design catalogues 528 designed conditions. The
other suites already execute most of the P0 cases in domains C, D, E, F, G,
H, L, M, R, S, T, U and Z; this file covers the domains those suites do not
reach, and :class:`TestCoverageAccounting` records which suite executes each
domain.

**What this file is not.** It is not a claim that all 528 cases have run.
Section 10 is explicit that a case count is not evidence, and section 18.1's
scorecard tracks execution separately.

Each test names its design IDs so the mapping stays checkable.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from analytics.contracts.manifest import ReconciliationTrust, build_source_manifest
from analytics.errors import (
    AnalyticsError,
    BuildState,
    CompletenessError,
    ConfigurationError,
    Reason,
    ReportStatus,
    RetriableError,
)
from analytics.faults.killpoints import ALL_KILL_POINTS
from analytics.fixtures.golden import (
    ALT_EPOCH,
    DEFAULT_EPOCH,
    DEFAULT_TOPIC,
    deal_envelope,
    fixture,
    fixture_ids,
    generate_deals,
    snapshot_marker_envelope,
    specs_for,
)
from analytics.observability.logging import redact
from analytics.observability.metrics import FORBIDDEN_LABELS, Metrics
from analytics.query.auth import Scope
from analytics.settings import Settings

from helpers import (
    assert_matches_oracle,
    ingest_all,
    produce,
    produce_specs,
    published_pointer,
    restart_consumer,
    run_pipeline,
    stub_source,
)

pytestmark = pytest.mark.whatif

PARTITION = (DEFAULT_TOPIC, 0)


# ===========================================================================
# A. Cross-project contract, bootstrap, source of truth
# ===========================================================================


class TestDomainA:
    def test_a01_missing_source_manifest_blocks_the_build(self, stack, scope):
        """P08-A-01 / Q-01: no manifest means no READY, ever."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        outcome = stack.build_worker(
            stub_source(stack, specs, complete=False, incomplete_reason="P03 snapshot unavailable")
        ).run(scope)
        assert outcome.status is ReportStatus.INCOMPLETE
        assert outcome.reason is Reason.SOURCE_MANIFEST_MISSING
        assert published_pointer(stack, scope) is None

    def test_a02_policy_mismatch_is_refused_before_a_build_id_exists(self, stack, scope):
        """ADR08-05: a policy mismatch would reinterpret money silently."""
        outcome = stack.build_worker(stub_source(stack, [])).run(
            replace(scope, policy_version="some-other-policy-v2")
        )
        assert outcome.reason is Reason.POLICY_VERSION_UNKNOWN
        assert outcome.build_id == ""
        assert stack.control.list_builds(tenant_id=scope.tenant_id) == []

    def test_a03_manifest_describing_another_cut_is_refused(self, stack, scope):
        """A manifest for a different cut cannot verify this one."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)

        class WrongCut:
            def capture_cut(self, _scope):
                return build_source_manifest(
                    replace(scope, cut_id="a-different-cut"),
                    [(s.deal_id, s.deal_payload_hash(), s.source_seq) for s in specs],
                    captured_at=stack.clock.now(),
                )

            def reconciliation_trust(self, requested):
                return ReconciliationTrust(revision="r1", scope=requested, verified=True)

        outcome = stack.build_worker(WrongCut()).run(scope)
        assert outcome.reason is Reason.SOURCE_MANIFEST_MISSING

    def test_a04_a_source_manifest_with_duplicate_ids_is_rejected(self, scope):
        """Set equality is meaningless if the expected set has duplicates."""
        spec = specs_for("G08-01")[0]
        with pytest.raises(CompletenessError):
            build_source_manifest(
                scope,
                [
                    (spec.deal_id, spec.deal_payload_hash(), 1),
                    (spec.deal_id, spec.deal_payload_hash(), 2),
                ],
                captured_at=datetime(2026, 10, 30, tzinfo=UTC),
            )

    def test_a05_all_twenty_golden_fixtures_are_defined(self):
        """Section 9.3 commits 20 fixtures with the repository."""
        assert set(fixture_ids()) == {f"G08-{n:02d}" for n in range(1, 21)}
        for fixture_id in fixture_ids():
            item = fixture(fixture_id)
            assert item.description
            assert item.expected_outcome in {
                "READY",
                "BLOCKED",
                "INCOMPLETE",
                "FAILED",
                "QUARANTINED",
            }


# ===========================================================================
# B. Tenant / source / epoch identity and scope fencing
# ===========================================================================


class TestDomainB:
    def test_b01_a_build_scoped_to_one_epoch_ignores_the_other(self, stack, scope):
        """INV08-04 at the storage boundary."""
        base = list(specs_for("G08-01"))
        alt = [replace(s, epoch_id=ALT_EPOCH) for s in base]
        produce_specs(stack, [*base, *alt])
        ingest_all(stack)
        rows = stack.clickhouse.scan_raw_deals(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=DEFAULT_EPOCH,
            max_source_seq=10_000,
            max_write_marker=10_000,
        )
        assert len(rows) == len(base)
        assert {row.epoch_id for row in rows} == {DEFAULT_EPOCH}

    def test_b02_scope_narrowing_never_widens(self):
        """The ``Scope`` type offers intersection only, by construction."""
        scope = Scope(tenant_id="t", account_ids=frozenset({"A-1"}))
        assert scope.narrow_accounts(["A-1"]) == frozenset({"A-1"})
        with pytest.raises(AnalyticsError):
            scope.narrow_accounts(["A-1", "A-2"])
        for name in dir(scope):
            assert not name.startswith("widen")
            assert not name.startswith("grant")

    def test_b03_cache_key_covers_every_scope_dimension(self):
        """P08-T-09: a thinner key serves one epoch's rows after a reset."""
        assert (
            Scope(tenant_id="t", epoch_ids=frozenset({"e1"})).cache_key()
            != Scope(tenant_id="t", epoch_ids=frozenset({"e2"})).cache_key()
        )
        assert Scope(tenant_id="t").cache_key() != Scope(tenant_id="u").cache_key()
        assert (
            Scope(tenant_id="t", operator=True).cache_key()
            != Scope(tenant_id="t", operator=False).cache_key()
        )


# ===========================================================================
# I. Batch sizing, async inserts, retry dedup tokens
# ===========================================================================


class TestDomainI:
    @pytest.mark.parametrize("batch", [1, 2, 7, 500])
    def test_i01_batch_size_does_not_change_the_outcome(self, batch, settings, clock, scope):
        """P08-I: splitting or recombining a batch is invisible downstream."""
        from analytics.main import build_lab_stack

        specs = list(generate_deals(15, seed=batch + 100))
        stack = build_lab_stack(
            settings=settings.model_copy(update={"kafka_max_batch_records": batch}), clock=clock
        )
        try:
            outcome, _ = run_pipeline(stack, specs, scope)
            assert outcome.state is BuildState.PUBLISHED
            assert len(outcome.dedup.deals) == len(specs)
            assert_matches_oracle(stack, outcome, scope, specs)
        finally:
            stack.close()

    def test_i02_async_insert_cannot_be_enabled_untested(self):
        """Section 6.1 step 4: durability there is version-dependent."""
        with pytest.raises(ConfigurationError):
            Settings(env="test", ch_synchronous_insert_ack_required=False)

    def test_i03_one_inflight_batch_per_partition(self, settings):
        """Bounded by configuration, with no way to raise it."""
        assert settings.kafka_max_inflight_batches_per_partition == 1
        with pytest.raises(Exception):
            Settings(env="test", kafka_max_inflight_batches_per_partition=4)


# ===========================================================================
# J / AB. Raw schema, physical identity, parts and partitions
# ===========================================================================


class TestDomainJ:
    def test_j01_raw_payload_is_stored_canonically(self, stack):
        """An auditor must be able to recompute the hash from the table."""
        from analytics.canonical import digest_of, parse_timestamp

        produce_specs(stack, specs_for("G08-01"))
        ingest_all(stack)
        for row in stack.clickhouse.all_raw():
            payload = json.loads(row.payload_json)
            normalised = dict(payload)
            for key in ("realized_pnl_gross", "commission", "swap", "volume_lots"):
                normalised[key] = Decimal(str(normalised[key]))
            normalised["executed_at"] = parse_timestamp(normalised["executed_at"])
            assert digest_of(normalised) == row.payload_sha256

    def test_j02_parts_grow_per_insert_and_collapse_on_merge(self, stack):
        """P08-AB: parts are observable and bounded by maintenance."""
        for spec in generate_deals(6, seed=55):
            produce_specs(stack, [spec])
            ingest_all(stack)
        raw_table = "broker_analytics.broker_events_raw"
        assert stack.clickhouse.storage_stats().active_parts[raw_table] >= 2
        before = stack.clickhouse.raw_row_count()
        stack.clickhouse.optimize()
        assert stack.clickhouse.storage_stats().active_parts[raw_table] == 1
        assert stack.clickhouse.raw_row_count() == before

    def test_j03_totals_are_correct_before_and_after_a_merge(self, stack, scope):
        """P08-U-06 / G08-10: correctness must not depend on a merge."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        produce_specs(stack, specs)  # duplicate delivery
        ingest_all(stack)
        before = stack.build_worker(stub_source(stack, specs)).run(scope, publish=False)
        stack.clickhouse.optimize()
        after = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-after-merge"), publish=False
        )
        assert before.canonical_digest == after.canonical_digest


# ===========================================================================
# K. Poison records, invalid payloads, quarantine
# ===========================================================================


class TestDomainK:
    @pytest.mark.parametrize(
        "body,reason",
        [
            (b"not json at all", Reason.ENVELOPE_MALFORMED),
            (b"[1,2,3]", Reason.ENVELOPE_MALFORMED),
            (b'{"event_type":"DealRecorded"}', Reason.FIELD_MISSING),
            (b"\xff\xfe\x00bad utf8", Reason.ENVELOPE_MALFORMED),
            (b'{"a": NaN}', Reason.ENVELOPE_MALFORMED),
            (b'{"a": Infinity}', Reason.ENVELOPE_MALFORMED),
        ],
    )
    def test_k01_malformed_bodies_are_quarantined_not_dropped(self, stack, body, reason):
        """Section 6.1 step 3: never skip a record because parsing failed."""
        stack.broker.produce(DEFAULT_TOPIC, body, partition=0)
        stack.ingest_worker().run_once()
        receipts = stack.control.list_quarantine()
        assert len(receipts) == 1
        assert receipts[0].reason == str(reason)

    def test_k02_a_json_float_in_a_money_field_is_poison(self, stack):
        """Section 7.2: ``12.3`` as a number, not a string."""
        envelope = deal_envelope(specs_for("G08-01")[0])
        envelope["payload"]["realized_pnl_gross"] = 12.3
        stack.broker.produce(DEFAULT_TOPIC, json.dumps(envelope).encode("utf-8"), partition=0)
        stack.ingest_worker().run_once()
        receipts = stack.control.list_quarantine()
        assert len(receipts) == 1
        assert receipts[0].reason == str(Reason.ENVELOPE_MALFORMED)

    def test_k03_an_oversized_envelope_is_poison(self, stack):
        """P08-AK: an unbounded body must not be parsed first."""
        envelope = deal_envelope(specs_for("G08-01")[0])
        envelope["padding"] = "x" * (stack.settings.kafka_max_envelope_bytes + 10)
        stack.broker.produce(DEFAULT_TOPIC, json.dumps(envelope).encode("utf-8"), partition=0)
        stack.ingest_worker().run_once()
        assert stack.control.list_quarantine()[0].reason == str(Reason.ENVELOPE_TOO_LARGE)

    def test_k04_resolution_requires_an_operator_note(self, stack):
        """Poison must not quietly disappear."""
        from analytics.ingest.quarantine import QuarantineLedger

        stack.broker.produce(DEFAULT_TOPIC, b"garbage", partition=0)
        stack.ingest_worker().run_once()
        receipt = stack.control.list_quarantine()[0]
        ledger = QuarantineLedger(stack.control, clock=stack.clock)
        with pytest.raises(CompletenessError):
            ledger.resolve(receipt.receipt_id, "   ")
        assert ledger.resolve(receipt.receipt_id, "producer bug fixed in build 42").resolved

    def test_k05_a_partition_cannot_resume_with_unresolved_poison(self, stack):
        produce(stack, fixture("G08-17").envelopes)
        worker = stack.ingest_worker()
        worker.run_once()
        stack.consumer.pause(PARTITION, reason="manual hold")
        with pytest.raises(AnalyticsError) as excinfo:
            worker.resume_after_quarantine(PARTITION)
        assert excinfo.value.reason is Reason.QUARANTINE_PENDING_IN_CUT

    def test_k06_resume_works_once_poison_is_resolved(self, stack):
        produce(stack, fixture("G08-17").envelopes)
        worker = stack.ingest_worker()
        worker.run_once()
        receipt = stack.control.list_quarantine()[0]
        stack.control.resolve_quarantine(receipt_id=receipt.receipt_id, note="reprocessed")
        stack.consumer.pause(PARTITION, reason="manual hold")
        worker.resume_after_quarantine(PARTITION)
        assert PARTITION not in stack.consumer.paused()


# ===========================================================================
# N / O. Deal semantics, fees, snapshots versus Deal facts
# ===========================================================================


class TestDomainsNAndO:
    def test_n01_signed_components_keep_their_sign(self, stack, scope):
        """P08-U-05: a fee is negative and stays negative."""
        outcome, _ = run_pipeline(stack, specs_for("G08-01"), scope)
        rows = stack.clickhouse.read_daily_rollups(
            build_id=outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        assert all(row.commission <= 0 for row in rows)
        assert any(row.swap != 0 for row in rows)

    def test_n02_unknown_fee_coverage_blocks_the_build(self, stack, scope):
        """G08-20 / INV08-15."""
        golden = fixture("G08-20")
        produce(stack, golden.envelopes)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, golden.source_deals)).run(scope)
        assert outcome.reason is Reason.FEE_COVERAGE_UNKNOWN
        assert outcome.status is ReportStatus.BLOCKED

    def test_n03_balance_is_not_inferred_from_deal_pnl(self, stack, scope):
        """Section 7.2: deposits and withdrawals are not Deal events."""
        outcome, _ = run_pipeline(stack, specs_for("G08-01"), scope)
        rows = stack.clickhouse.read_daily_rollups(
            build_id=outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        for row in rows:
            for field in ("balance", "equity", "margin", "free_margin"):
                assert not hasattr(row, field)

    def test_o01_a_marker_produces_no_deal_fact_and_no_account_metric(self, stack, scope):
        """INV08-16: the marker is not the snapshot."""
        produce(stack, [snapshot_marker_envelope()])
        ingest_all(stack)
        rows = stack.clickhouse.scan_raw_deals(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            max_source_seq=10_000,
            max_write_marker=10_000,
        )
        assert rows == []
        unsupported = stack.query.unsupported_metric(
            metric="account equity",
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            why="snapshot markers carry no holdings",
        )
        assert unsupported.envelope.status is ReportStatus.UNSUPPORTED


# ===========================================================================
# P / V. Event time, late data, backfill
# ===========================================================================


class TestDomainsPAndV:
    @pytest.mark.parametrize(
        "executed,expected_day",
        [
            ("2026-10-30T00:00:00.000000Z", "2026-10-30"),
            ("2026-10-30T23:59:59.999999Z", "2026-10-30"),
            ("2026-10-31T00:00:00.000000Z", "2026-10-31"),
            ("2026-12-31T23:59:59.999999Z", "2026-12-31"),
            ("2027-01-01T00:00:00.000000Z", "2027-01-01"),
            ("2026-03-29T01:30:00.000000Z", "2026-03-29"),
        ],
    )
    def test_p01_utc_day_boundaries(self, stack, scope, executed, expected_day):
        """INV08-14 / P08-P / P08-U-12."""
        from analytics.canonical import parse_timestamp

        spec = replace(
            specs_for("G08-01")[0], executed_at=parse_timestamp(executed), deal_id="D-BOUNDARY"
        )
        outcome, _ = run_pipeline(stack, [spec], scope)
        assert outcome.state is BuildState.PUBLISHED
        rows = stack.clickhouse.read_daily_rollups(
            build_id=outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        assert rows[0].trade_day.isoformat() == expected_day

    def test_p02_a_replay_of_a_whole_day_causes_no_economic_change(self, stack, scope):
        """P08-V-03: a rebuild over replayed data yields one digest."""
        specs = list(generate_deals(20, seed=303))
        first, _ = run_pipeline(stack, specs, scope)
        for offset in range(stack.broker.total_records()):
            stack.broker.redeliver(PARTITION, offset)
        restart_consumer(stack).run_until_idle()
        second = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-2")
        )
        assert first.canonical_digest == second.canonical_digest

    def test_v01_a_six_month_old_deal_rebuilds_into_its_own_period(self, stack, scope):
        """G08-07 / P08-V-02: raw partitions by ingest, reports by event time."""
        spec = replace(
            specs_for("G08-01")[0],
            deal_id="D-OLD",
            executed_at=datetime(2026, 4, 30, 8, tzinfo=UTC),
        )
        outcome, _ = run_pipeline(stack, [spec], scope)
        rows = stack.clickhouse.read_daily_rollups(
            build_id=outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        assert rows[0].trade_day == date(2026, 4, 30)
        raw = stack.clickhouse.all_raw()[0]
        assert raw.ingested_at.date() != raw.executed_at.date()


# ===========================================================================
# W. Corrections and reversals (P1 gated)
# ===========================================================================


class TestDomainW:
    def test_w01_a_v2_correction_event_is_refused(self, stack):
        """Section 7.3: no unapproved correction may rewrite money."""
        spec = specs_for("G08-01")[0]
        produce(stack, [deal_envelope(replace(spec, commission="-50.00"), schema_version=2)])
        stack.ingest_worker().run_once()
        assert stack.control.list_quarantine()[0].reason == str(
            Reason.SCHEMA_VERSION_UNSUPPORTED
        )

    def test_w02_a_same_key_conflicting_payload_is_quarantined(self, stack, scope):
        """P08-W: a correction smuggled in as a v1 event is a conflict."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        produce(
            stack,
            [
                deal_envelope(
                    replace(specs[0], commission="-50.00"),
                    event_id="00000000-0000-4000-8000-00000000abcd",
                )
            ],
        )
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.reason is Reason.DEAL_SEMANTIC_CONFLICT
        assert published_pointer(stack, scope) is None

    def test_w03_an_unknown_event_type_is_refused(self, stack):
        produce(stack, [deal_envelope(specs_for("G08-01")[0], event_type="DealReversed")])
        stack.ingest_worker().run_once()
        assert stack.control.list_quarantine()[0].reason == str(Reason.EVENT_TYPE_NOT_ALLOWED)


# ===========================================================================
# X / Y. Reconciliation governance and the P05/P06 handoff
# ===========================================================================


class TestDomainsXAndY:
    def test_x01_an_unverified_revision_blocks_the_build(self, stack, scope):
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs, verified=False)).run(scope)
        assert outcome.reason is Reason.RECONCILIATION_NOT_VERIFIED

    def test_x02_an_explicitly_revoked_revision_blocks_the_build(self, stack, scope):
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs, revoked=True)).run(scope)
        assert outcome.reason is Reason.RECONCILIATION_REVOKED

    def test_x03_the_cut_is_re_read_during_the_build(self, stack, scope):
        """KP08-15 / P08-R-06: the worker must not trust its first look."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        source = stub_source(stack, specs)
        stack.build_worker(source).run(scope)
        assert source.capture_calls >= 2, "the pinned cut must be re-validated"
        assert source.trust_calls >= 3, "trust must be re-read before publishing"

    def test_x04_a_manifest_that_changes_mid_build_is_refused(self, stack, scope):
        """A manifest swapped underneath a running build. P08-Q."""
        specs = list(specs_for("G08-01"))
        produce_specs(stack, specs)
        ingest_all(stack)
        mutated = [*specs, replace(specs[0], deal_id="D-APPEARED", source_seq=90)]
        outcome = stack.build_worker(
            stub_source(stack, specs, mutate_after_captures=1, mutated_specs=mutated)
        ).run(scope)
        assert outcome.reason is Reason.CUT_CHANGED
        assert published_pointer(stack, scope) is None

    def test_y01_p08_never_claims_to_be_a_live_exposure_api(self, stack, scope):
        """ADR08-07: live risk stays with P05/P06."""
        report = stack.query.unsupported_metric(
            metric="live exposure",
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            why="live risk is owned by P05/P06",
        )
        assert report.envelope.status is ReportStatus.UNSUPPORTED
        assert "P05/P06" in report.envelope.message


# ===========================================================================
# AA / AP. Query resources and bounded behaviour
# ===========================================================================


class TestDomainsAAAndAP:
    def test_aa01_a_query_row_budget_fails_closed(self, stack, scope):
        """INV08-20: overload is a controlled refusal."""
        run_pipeline(stack, list(generate_deals(30, seed=909)), scope)
        stack.clickhouse._max_query_rows = 1
        with pytest.raises(RetriableError) as excinfo:
            stack.clickhouse.scan_raw_deals(
                tenant_id=scope.tenant_id,
                source_id=scope.source_id,
                epoch_id=scope.epoch_id,
                max_source_seq=10_000,
                max_write_marker=10_000,
            )
        assert excinfo.value.reason is Reason.BUDGET_EXCEEDED

    def test_aa02_build_size_bound_is_enforced(self, stack, scope, settings):
        """A cut larger than the configured bound is refused, not attempted."""
        specs = list(generate_deals(20, seed=808))
        produce_specs(stack, specs)
        ingest_all(stack)
        worker = stack.build_worker(stub_source(stack, specs))
        worker._settings = settings.model_copy(update={"build_max_deals": 5})
        assert worker.run(scope).reason is Reason.BUDGET_EXCEEDED

    def test_ap01_repeated_builds_do_not_leak_publications(self, stack, scope):
        """P08-AP: a soak must not accumulate current pointers."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        for index in range(6):
            outcome = stack.build_worker(stub_source(stack, specs)).run(
                replace(scope, cut_id=f"cut-{index}")
            )
            assert outcome.state is BuildState.PUBLISHED
        assert published_pointer(stack, scope).publication_revision == 6
        published = [
            build
            for build in stack.control.list_builds(tenant_id=scope.tenant_id)
            if build.state is BuildState.PUBLISHED
        ]
        assert len(published) == 1, "exactly one build may be current"


# ===========================================================================
# AC / AD / AM. Retention, storage and disaster recovery
# ===========================================================================


class TestDomainsACADAM:
    def test_ac01_retention_loss_requires_a_verified_rebootstrap(self, settings, clock):
        """INV08-13 / G08-16.

        The batch size is capped so only part of the log is consumed; the
        committed offset then sits genuinely below the new low watermark,
        which is the condition that must refuse to resume. Draining the whole
        log first would leave committed == low, which is no gap at all.
        """
        from analytics.main import build_lab_stack

        stack = build_lab_stack(
            settings=settings.model_copy(update={"kafka_max_batch_records": 3}), clock=clock
        )
        try:
            produce_specs(stack, list(generate_deals(10, seed=1313)))
            stack.ingest_worker().run_once()
            committed = stack.consumer.committed([PARTITION]).get(PARTITION, 0)
            assert committed == 3
            stack.broker.truncate_to(PARTITION, committed + 3)
            with pytest.raises(RetriableError) as excinfo:
                restart_consumer(stack)
            assert excinfo.value.reason is Reason.OFFSET_OUT_OF_RANGE
        finally:
            stack.close()

    def test_ad01_a_full_disk_never_advances_past_a_stored_row(self, stack):
        """Section 8.4: stop and keep the offset."""
        produce_specs(stack, list(generate_deals(20, seed=1414)))
        stack.clickhouse.set_disk_capacity_rows(3)
        stack.ingest_worker().run_until_idle(max_cycles=4)
        stored = stack.clickhouse.raw_row_count()
        committed = stack.consumer.committed([PARTITION]).get(PARTITION, 0)
        assert committed <= stored

    def test_am01_gc_never_removes_the_current_publication(self, stack, scope):
        """INV08-19 / G08-19."""
        outcome, _ = run_pipeline(stack, specs_for("G08-01"), scope)
        plan = stack.gc.plan(tenant_id=scope.tenant_id)
        assert plan.protected[outcome.build_id] == "CURRENT_PUBLICATION"
        with pytest.raises(AnalyticsError) as excinfo:
            stack.gc.assert_collectable(outcome.build_id)
        assert excinfo.value.reason is Reason.GC_PROTECTED

    def test_am02_a_quarantined_build_is_kept_for_forensics(self, stack, scope):
        golden = fixture("G08-03")
        produce(stack, golden.envelopes)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, golden.source_deals)).run(scope)
        assert outcome.state is BuildState.QUARANTINED
        assert stack.gc.plan(tenant_id=scope.tenant_id).protected[
            outcome.build_id
        ] == "QUARANTINE_FORENSICS"

    def test_am03_a_verified_unpublished_build_is_retained_for_rollback(self, stack, scope):
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope, publish=False)
        assert stack.gc.plan(tenant_id=scope.tenant_id).protected[
            outcome.build_id
        ] == "VERIFIED_AWAITING_PUBLICATION"


# ===========================================================================
# AJ / AK. Privacy, audit, injection and denial of service
# ===========================================================================


class TestDomainsAJAndAK:
    def test_aj01_identifiers_are_hashed_in_logs(self):
        """Section 8.3 / P08-AJ: correlation without retention."""
        redacted = redact({"account_id": "A-42", "deal_id": "D-1", "offset": 7})
        assert redacted["account_id"].startswith("h:")
        assert "A-42" not in json.dumps(redacted)
        assert redacted["offset"] == 7

    def test_aj02_tokens_are_dropped_entirely(self):
        """Even a hashed token is not useful in a log."""
        redacted = redact({"reader_token": "secret", "password": "p", "note": "ok"})
        assert "reader_token" not in redacted
        assert "password" not in redacted
        assert redacted["note"] == "ok"

    def test_aj03_high_cardinality_metric_labels_are_refused(self):
        """A scrape that melts is an outage; this fails a test instead."""
        metrics = Metrics()
        for label in sorted(FORBIDDEN_LABELS):
            with pytest.raises(ValueError):
                metrics._check_labels({label: "value"})
        metrics._check_labels({"topic": "broker.deals.v1", "reason": "X"})

    def test_ak01_sql_shaped_parameters_cannot_reach_the_query(self, stack, scope, reader):
        """Section 8.2: parameters are allowlisted, never interpolated."""
        run_pipeline(stack, specs_for("G08-01"), scope)
        report = stack.query.daily(
            reader,
            source_id="sim-broker-01' OR 1=1 --",
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert report.items == ()
        assert report.envelope.status is not ReportStatus.READY

    def test_ak02_a_hostile_account_filter_is_refused(self, stack, scope):
        stack.authenticator.register(
            "scoped-token-value",
            principal="scoped@lab",
            scope=Scope(tenant_id=scope.tenant_id, account_ids=frozenset({"A-1"})),
        )
        auth = stack.authenticator.authenticate("scoped-token-value")
        run_pipeline(stack, specs_for("G08-01"), scope)
        report = stack.query.daily(
            auth,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
            accounts=["A-1", "A-2"],
        )
        assert report.envelope.reason == str(Reason.FORBIDDEN_SCOPE)

    def test_ak03_an_empty_account_filter_is_refused(self, scope):
        with pytest.raises(AnalyticsError):
            Scope(tenant_id=scope.tenant_id).narrow_accounts([])


# ===========================================================================
# AL / AN / AQ. Migration, observability and scheduling
# ===========================================================================


class TestDomainsALANAQ:
    def test_al01_the_clickhouse_version_pin_is_mandatory_in_production(self):
        """Section 13.1: a deployment must name the tested version."""
        with pytest.raises(ConfigurationError):
            Settings(
                env="prod",
                tenant_map=("broker.deals.v1:s=t",),
                reader_token="r" * 40,
                operator_token="o" * 40,
                cursor_signing_key="k" * 40,
            )

    def test_al02_the_http_adapter_refuses_the_placeholder_version(self):
        from analytics.adapters.clickhouse_http import HttpClickHouse

        with pytest.raises(ConfigurationError):
            HttpClickHouse(url="http://localhost:8123", database="broker_analytics")

    def test_an01_a_blocked_build_increments_its_alert_metric(self, stack, scope):
        """Section 8.3: any value above zero pages the owner."""
        golden = fixture("G08-03")
        produce(stack, golden.envelopes)
        ingest_all(stack)
        stack.build_worker(stub_source(stack, golden.source_deals)).run(scope)
        snapshot = stack.metrics.snapshot()
        failures = sum(
            value
            for key, value in snapshot.items()
            if key.startswith("p08_build_validation_failed_total")
        )
        assert failures >= 1

    def test_an02_a_missing_deal_sets_the_blocking_gauge(self, stack, scope):
        specs = list(generate_deals(10, seed=1515))
        produce_specs(stack, specs[:-1])
        ingest_all(stack)
        stack.build_worker(stub_source(stack, specs)).run(scope)
        snapshot = stack.metrics.snapshot()
        assert snapshot["p08_missing_deal_identity_count{report_family=daily_deal_v1}"] == 1

    def test_an03_physical_and_logical_counts_are_separate_series(self, stack):
        """INV08-23: three different numbers, three different series."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        produce_specs(stack, specs)
        ingest_all(stack)
        snapshot = stack.metrics.snapshot()
        raw = snapshot["p08_ingest_raw_rows_total{topic=broker.deals.v1}"]
        unique = snapshot["p08_ingest_unique_event_ids_total{topic=broker.deals.v1}"]
        assert raw == 2 * len(specs)
        assert unique == len(specs)
        assert raw != unique

    def test_an04_quarantine_depth_is_published(self, stack):
        stack.broker.produce(DEFAULT_TOPIC, b"garbage", partition=0)
        stack.ingest_worker().run_once()
        assert stack.metrics.snapshot()["p08_quarantine_pending"] == 1

    def test_aq01_one_concurrent_build_per_source_epoch(self, settings):
        """Section 16.2: the bound is configuration, not convention."""
        assert settings.build_max_concurrent_per_source_epoch == 1
        with pytest.raises(Exception):
            Settings(env="test", build_max_concurrent_per_source_epoch=4)

    def test_aq02_attempt_numbers_are_sequential_per_logical_build(self, stack, scope):
        """P08-S-09: a retry is a new attempt, not a reused id."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        first = stack.build_worker(stub_source(stack, specs)).run(scope, publish=False)
        second = stack.build_worker(stub_source(stack, specs)).run(scope, publish=False)
        assert (first.attempt_no, second.attempt_no) == (1, 2)
        assert first.build_id != second.build_id


# ===========================================================================
# Coverage accounting
# ===========================================================================

#: Which suite executes each of the 44 failure domains of section 10.1.
DOMAIN_COVERAGE: dict[str, str] = {
    "A": "whatif.TestDomainA",
    "B": "whatif.TestDomainB",
    "C": "unit.test_events",
    "D": "component.test_kafka_adapter",
    "E": "component.test_kafka_adapter + chaos.kill_points",
    "F": "component.test_kafka_adapter + chaos.kill_points",
    "G": "unit.test_dedup_rollup_oracle + integration.IT08-02/03/04",
    "H": "component.test_clickhouse_adapter",
    "I": "whatif.TestDomainI",
    "J": "whatif.TestDomainJ",
    "K": "whatif.TestDomainK",
    "L": "unit.test_money",
    "M": "unit.test_policy + integration.IT08-14",
    "N": "whatif.TestDomainsNAndO",
    "O": "whatif.TestDomainsNAndO + integration.IT08-09",
    "P": "whatif.TestDomainsPAndV + unit.test_canonical",
    "Q": "integration.IT08-07 + whatif.TestDomainA",
    "R": "unit.test_dedup_rollup_oracle + property.test_metamorphic",
    "S": "component.test_control_plane + chaos.kill_points",
    "T": "component.test_control_plane + chaos.CC08-05/11",
    "U": "unit.test_dedup_rollup_oracle + contract.test_http_api",
    "V": "whatif.TestDomainsPAndV + integration.IT08-08",
    "W": "whatif.TestDomainW + chaos.CC08-07",
    "X": "whatif.TestDomainsXAndY + integration.IT08-15",
    "Y": "whatif.TestDomainsXAndY",
    "Z": "contract.test_http_api",
    "AA": "whatif.TestDomainsAAAndAP",
    "AB": "whatif.TestDomainJ + component.test_clickhouse_adapter",
    "AC": "whatif.TestDomainsACADAM + chaos.CC08-04",
    "AD": "whatif.TestDomainsACADAM + integration.IT08-16",
    "AE": "component.test_clickhouse_adapter + chaos.kill_points",
    "AF": "NOT RUN - ReplicatedMergeTree/Keeper failover is P2 and needs a real cluster",
    "AG": "component.test_kafka_adapter",
    "AH": "component.test_clickhouse_adapter (timeouts, unavailability)",
    "AI": "contract.test_http_api.TestScopeIsolation",
    "AJ": "whatif.TestDomainsAJAndAK",
    "AK": "whatif.TestDomainsAJAndAK",
    "AL": "whatif.TestDomainsALANAQ",
    "AM": "whatif.TestDomainsACADAM + integration.IT08-18",
    "AN": "whatif.TestDomainsALANAQ",
    "AO": "performance.test_benchmarks",
    "AP": "whatif.TestDomainsAAAndAP",
    "AQ": "whatif.TestDomainsALANAQ",
    "AR": "chaos.test_compound_campaigns",
}


class TestCoverageAccounting:
    def test_every_kill_point_is_named_in_the_catalog(self):
        assert len(ALL_KILL_POINTS) == 28

    def test_all_forty_four_domains_are_accounted_for(self):
        """Covered is not the same as exhaustively executed.

        This asserts each domain has an executing suite, and that the one
        domain which cannot be exercised here says so. Section 10's warning
        stands: a case count is not evidence of resilience.
        """
        assert len(DOMAIN_COVERAGE) == 44
        unrun = {key for key, value in DOMAIN_COVERAGE.items() if value.startswith("NOT RUN")}
        assert unrun == {"AF"}, "a newly unrunnable domain must be declared, not hidden"
