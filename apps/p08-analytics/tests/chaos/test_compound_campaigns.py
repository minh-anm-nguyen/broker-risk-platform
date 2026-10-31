"""The 12 compound campaigns of section 12.1.

A single fault is the easy case. These combine faults that interact, because
that is where a safety argument built from individually-correct steps tends
to come apart: a duplicate *and* a rebalance *and* a delayed ACK at once.

Each campaign asserts the design's "Hard assertion" column. CC08-12 is the
odd one out and the most important: it checks that a *corrupted oracle*
blocks the release even when everything else is green and fast.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from analytics.build.oracle import verify_rollups
from analytics.build.rollup import build_daily_rollups
from analytics.errors import (
    AmbiguousWriteError,
    BuildState,
    CompletenessError,
    Reason,
    ReportStatus,
    RetriableError,
)
from analytics.faults.killpoints import FaultSite
from analytics.fixtures.golden import (
    ALT_EPOCH,
    DEFAULT_TOPIC,
    deal_envelope,
    generate_deals,
    specs_for,
)
from analytics.query.auth import Scope

from helpers import (
    assert_matches_oracle,
    expected_identities,
    ingest_all,
    produce,
    produce_specs,
    published_pointer,
    restart_consumer,
    run_pipeline,
    stub_source,
)

pytestmark = pytest.mark.chaos

PARTITION = (DEFAULT_TOPIC, 0)


class TestCC0801DuplicateRebalanceDelayedAck:
    def test_exact_amount_and_no_incorrect_offset(self, stack, scope, faults):
        """Kafka duplicate + rebalance + delayed ACK, all at once."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)

        # Delay the ACK on the first attempt and rebalance while it is in
        # flight, so the batch is both ambiguous and un-committable.
        faults.call_at(
            FaultSite.CH_INSERT_AMBIGUOUS,
            lambda ctx: stack.consumer.rebalance(stack.broker.partitions_of(DEFAULT_TOPIC)),
            times=1,
        )
        first = stack.ingest_worker().run_once()
        faults.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)
        assert first.committed == {}

        # Then duplicate every record and drain.
        for offset in range(stack.broker.total_records()):
            stack.broker.redeliver(PARTITION, offset)
        restart_consumer(stack).run_until_idle()

        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        assert len(outcome.dedup.deals) == len(specs)
        assert outcome.dedup.physical_observations > len(specs)
        assert_matches_oracle(stack, outcome, scope, specs)


class TestCC0802LagZeroIsNotCompleteness:
    def test_no_false_completeness_when_the_manifest_is_larger(self, stack, scope):
        """P07 backlog + Kafka lag zero + a changed source manifest.

        Every event in Kafka has been consumed, so lag is zero - and the
        source still lists Deals that never reached the outbox. Lag is
        progress, not proof (section 4).
        """
        source_specs = list(generate_deals(30, seed=802))
        produce_specs(stack, source_specs[:-2])
        ingest_all(stack)

        committed = stack.consumer.committed([PARTITION]).get(PARTITION, 0)
        assert stack.broker.watermarks(PARTITION)[1] - committed == 0

        outcome = stack.build_worker(stub_source(stack, source_specs)).run(scope)
        assert outcome.status is ReportStatus.INCOMPLETE
        assert outcome.reason is Reason.SOURCE_DEALS_MISSING
        assert outcome.details["comparison"]["missing_total"] == 2
        assert published_pointer(stack, scope) is None


class TestCC0803TrustRevokedDuringPublishRace:
    def test_no_ready_report_for_an_invalidated_cut(self, stack, scope, reader):
        """P04 BLOCKING + a stale reader + a pointer publish race."""
        specs = list(specs_for("G08-01"))
        first, _ = run_pipeline(stack, specs, scope)
        assert first.state is BuildState.PUBLISHED

        extra = replace(specs[0], deal_id="D-NEW", source_seq=50)
        produce_specs(stack, [extra])
        ingest_all(stack)

        blocked = stack.build_worker(
            stub_source(stack, [*specs, extra], revoke_after_trust_reads=1)
        ).run(replace(scope, cut_id="cut-2"))
        assert blocked.state in (BuildState.FAILED, BuildState.QUARANTINED)
        assert blocked.reason in (
            Reason.RECONCILIATION_REVOKED,
            Reason.RECONCILIATION_NOT_VERIFIED,
        )

        # The reader still sees the earlier verified build, not the candidate.
        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert report.envelope.build_id == first.build_id
        assert report.envelope.status is ReportStatus.READY


class TestCC0804DiskFullAndRetentionExpiry:
    def test_incomplete_and_an_explicit_rebootstrap(self, stack, scope):
        """CH disk full while Kafka retention expires underneath.

        The combination is the dangerous one: ingestion cannot proceed, and by
        the time it can, the records are gone. The only honest answer is
        ``INCOMPLETE`` plus a refusal to resume from a bogus offset.
        """
        specs = list(generate_deals(30, seed=804))
        produce_specs(stack, specs)

        stack.clickhouse.set_disk_capacity_rows(5)
        stack.ingest_worker().run_until_idle(max_cycles=3)
        assert stack.clickhouse.raw_row_count() < len(specs)

        committed = stack.consumer.committed([PARTITION]).get(PARTITION, 0)
        stack.broker.truncate_to(PARTITION, committed + 10)
        stack.clickhouse.set_disk_capacity_rows(5_000_000)

        with pytest.raises(RetriableError) as excinfo:
            restart_consumer(stack)
        assert excinfo.value.reason is Reason.OFFSET_OUT_OF_RANGE

        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.status is ReportStatus.INCOMPLETE
        assert published_pointer(stack, scope) is None


class TestCC0805PointerFailoverReplicaLagPagination:
    def test_one_build_per_response_and_no_torn_read(self, stack, scope, reader):
        """A pointer move during an open cursor must not mix builds."""
        specs = [
            replace(spec, account_id="A-1")
            for spec in generate_deals(40, seed=805, accounts=1, days=1)
        ]
        first, _ = run_pipeline(stack, specs, scope)
        assert first.state is BuildState.PUBLISHED

        page = stack.query.account_deals(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            account_id="A-1",
            limit=10,
        )
        assert page.envelope.build_id == first.build_id
        assert page.next_cursor

        second = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-2")
        )
        assert second.state is BuildState.PUBLISHED

        resumed = stack.query.account_deals(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            account_id="A-1",
            limit=10,
            cursor=page.next_cursor,
        )
        # The cursor was issued for build A and the pointer now names B, so
        # the request is refused rather than silently answered from B.
        assert resumed.envelope.reason == str(Reason.CURSOR_BUILD_MISMATCH)
        assert resumed.items == ()

        # Pinning the original build explicitly still works for an auditor.
        pinned = stack.query.account_deals(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            account_id="A-1",
            limit=10,
            cursor=page.next_cursor,
            build_id=first.build_id,
        )
        assert pinned.envelope.build_id == first.build_id
        assert len(pinned.items) == 10


class TestCC0806BackfillLateEventsEpochReset:
    def test_source_epoch_and_date_identities_stay_correct(self, stack, scope):
        """Past-month backfill + late events + an epoch reset."""
        base = list(specs_for("G08-01"))
        late = replace(
            base[0],
            deal_id="D-BACKFILL",
            executed_at=datetime(2026, 4, 2, 11, tzinfo=UTC),
            source_seq=70,
        )
        epoch2 = [replace(spec, epoch_id=ALT_EPOCH) for spec in base]

        produce_specs(stack, [*base, late, *epoch2])
        ingest_all(stack)

        first = stack.build_worker(stub_source(stack, [*base, late])).run(scope)
        assert first.state is BuildState.PUBLISHED
        assert_matches_oracle(stack, first, scope, [*base, late])

        alt_scope = replace(scope, epoch_id=ALT_EPOCH)
        second = stack.build_worker(stub_source(stack, epoch2)).run(alt_scope)
        assert second.state is BuildState.PUBLISHED
        assert_matches_oracle(stack, second, alt_scope, epoch2)

        # The backfilled day exists only in the epoch that produced it.
        alt_days = {
            row.trade_day
            for row in stack.clickhouse.read_daily_rollups(
                build_id=second.build_id,
                tenant_id=alt_scope.tenant_id,
                source_id=alt_scope.source_id,
                epoch_id=ALT_EPOCH,
            )
        }
        assert date(2026, 4, 2) not in alt_days
        assert first.canonical_digest != second.canonical_digest


class TestCC0807CorrectionFxDriftReplay:
    def test_corrections_are_gated_and_no_conversion_happens(self, stack, scope, policy):
        """A commission correction + FX drift + a replay, all refused.

        None of the three is supported in P0, and the campaign exists to prove
        the refusals compose rather than one of them quietly winning.
        """
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)

        # A v2 correction event never reaches raw storage.
        produce(stack, [deal_envelope(replace(specs[0], commission="-99.00"), schema_version=2)])
        ingest_all(stack)
        receipts = stack.control.list_quarantine()
        assert any(r.reason == str(Reason.SCHEMA_VERSION_UNSUPPORTED) for r in receipts)

        # A same-key conflicting "correction" under v1 is a semantic conflict.
        produce(
            stack,
            [
                deal_envelope(
                    replace(specs[0], commission="-99.00"),
                    event_id="00000000-0000-4000-8000-0000000000ff",
                )
            ],
        )
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.reason in (
            Reason.DEAL_SEMANTIC_CONFLICT,
            Reason.QUARANTINE_PENDING_IN_CUT,
        )
        assert published_pointer(stack, scope) is None

        # And FX conversion is still impossible.
        with pytest.raises(Exception) as excinfo:
            policy.convert(Decimal("1.00"), "EUR", "USD")
        assert excinfo.value.reason is Reason.FX_CONVERSION_UNSUPPORTED


class TestCC0808HostileTenantAuditOutageLoad:
    def test_no_cross_tenant_data_and_fail_closed_without_audit(self, stack, scope, reader):
        """A hostile tenant query + an audit outage + a load surge."""
        specs = specs_for("G08-01")
        run_pipeline(stack, specs, scope)

        stack.authenticator.register(
            "rival-tenant-token", principal="reader@rival", scope=Scope(tenant_id="rival-tenant")
        )
        rival = stack.authenticator.authenticate("rival-tenant-token")
        hostile = stack.query.daily(
            rival,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert hostile.items == (), "no row of another tenant may be returned"
        assert hostile.envelope.status is not ReportStatus.READY

        # Audit required but unavailable: reads fail closed.
        stack.query._settings = stack.settings.model_copy(
            update={"require_audit_for_reads": True}
        )
        stack.query.set_audit_available(False)
        blocked = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert blocked.envelope.reason == str(Reason.AUDIT_UNAVAILABLE)
        assert blocked.items == ()

        stack.query.set_audit_available(True)
        restored = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert restored.envelope.status is ReportStatus.READY


class TestCC0809IdCollisionAcrossScopes:
    def test_no_semantic_cross_contamination(self, stack, scope, settings):
        """Source id collision + reused Deal id + partition migration.

        Two tenants legitimately use the same ``source_id`` and the same Deal
        ids. Nothing may merge (INV08-17).
        """
        from analytics.main import build_lab_stack

        specs = specs_for("G08-01")
        run_pipeline(stack, specs, scope)
        own_digest = (
            stack.build_worker(stub_source(stack, specs))
            .run(replace(scope, cut_id="cut-probe"), publish=False)
            .canonical_digest
        )

        rival_settings = settings.model_copy(
            update={"tenant_map": (f"{DEFAULT_TOPIC}:{scope.source_id}=rival-tenant",)}
        )
        rival = build_lab_stack(
            settings=rival_settings, clock=stack.clock, tenant_id="rival-tenant"
        )
        try:
            rival_specs = [replace(s, tenant_id="rival-tenant") for s in specs]
            rival_scope = replace(scope, tenant_id="rival-tenant")
            outcome, _ = run_pipeline(rival, rival_specs, rival_scope)
            assert outcome.state is BuildState.PUBLISHED
            # Same Deal ids, different tenant: distinct canonical digests.
            assert outcome.canonical_digest != own_digest
            assert rival.control.get_publication(
                tenant_id=scope.tenant_id,
                source_id=scope.source_id,
                epoch_id=scope.epoch_id,
                report_family=scope.report_family,
            ) is None
        finally:
            rival.close()


class TestCC0810RestartAmbiguityOffsetReset:
    def test_reconstructed_sink_matches_the_source_manifest(self, stack, scope, faults):
        """CH restart + ambiguous INSERT + an operator offset reset."""
        specs = list(generate_deals(25, seed=810))
        produce_specs(stack, specs)

        faults.raise_at(FaultSite.CH_INSERT_AMBIGUOUS, AmbiguousWriteError(), times=2)
        stack.ingest_worker().run_until_idle(max_cycles=10)
        faults.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)

        # An operator rewinds the group to re-ingest everything.
        stack.broker.reset_offsets(
            stack.settings.kafka_group_id, {PARTITION: 0}, actor="oncall-engineer"
        )
        restart_consumer(stack).run_until_idle()

        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        assert expected_identities(specs) == {
            (deal.deal_id, deal.deal_payload_hash) for deal in outcome.dedup.deals
        }
        assert_matches_oracle(stack, outcome, scope, specs)


class TestCC0811GcWithActiveReaderAndRollback:
    def test_previous_verified_build_survives_until_unreferenced(self, stack, scope, reader):
        """Orphan GC + an active reader + a pointer rollback."""
        specs = specs_for("G08-01")
        first, _ = run_pipeline(stack, specs, scope)
        second = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-2")
        )
        assert second.state is BuildState.PUBLISHED

        stack.control.register_reader_pin(build_id=first.build_id, pin_id="reader-1")
        deleted = stack.gc.collect(tenant_id=scope.tenant_id)
        assert first.build_id not in deleted
        assert second.build_id not in deleted

        rolled = stack.registry.rollback(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
            target_build_id=first.build_id,
            actor="oncall",
            note="second build failed a later completeness audit",
        )
        assert rolled.build_id == first.build_id
        assert rolled.publication_revision == 3

        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert report.envelope.build_id == first.build_id
        assert_matches_oracle(stack, first, scope, specs)


class TestCC0812CorruptedOracleBlocksRelease:
    def test_a_mutated_rollup_is_always_caught(self, stack, scope):
        """CC08-12: the release gate must reject a corrupted aggregation.

        This is the campaign that protects every other test in the suite. If
        the aggregation can be wrong and the oracle still agree, the whole
        verification story is theatre - so each mutation must be caught.
        """
        specs = list(generate_deals(60, seed=812))
        produce_specs(stack, specs)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope, publish=False)
        assert outcome.state is BuildState.VERIFIED

        deals = outcome.dedup.deals
        honest = build_daily_rollups(deals, build_id=outcome.build_id)
        assert verify_rollups(deals, honest).matches

        head = honest[0]
        mutations = {
            "gross off by a cent": replace(
                head, realized_pnl_gross=head.realized_pnl_gross + Decimal("0.01")
            ),
            "commission sign flipped": replace(head, commission=-head.commission),
            "count doubled": replace(head, deal_count=head.deal_count * 2),
            "swap dropped": replace(head, swap=Decimal("0")),
            "currency relabelled": replace(head, money_currency="EUR"),
            "day shifted": replace(head, trade_day=head.trade_day + timedelta(days=1)),
            "account relabelled": replace(head, account_id="A-FORGED"),
        }
        for label, mutated in mutations.items():
            comparison = verify_rollups(deals, [mutated, *honest[1:]])
            assert not comparison.matches, f"the oracle failed to catch: {label}"
            with pytest.raises(CompletenessError):
                comparison.raise_if_mismatched()

        assert not verify_rollups(deals, honest[1:]).matches, "a dropped row must be caught"

    def test_a_fast_but_wrong_build_is_not_publishable(self, stack, scope):
        """Being quick while incorrect is a NO-GO (section 18)."""
        specs = list(generate_deals(30, seed=8122))
        produce_specs(stack, specs[:-1])
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.FAILED
        assert outcome.status is ReportStatus.INCOMPLETE
        assert published_pointer(stack, scope) is None
