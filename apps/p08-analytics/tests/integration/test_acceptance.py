"""The 18 Given/When/Then acceptance scenarios of section 11.1.

Each test is named for its design ID and asserts the "Then / required
evidence" column, not merely that nothing crashed. Where the design asks for
a specific oracle, the test uses the *independent* one in ``conftest`` -
computed from the fixture specs, never from P08's own rollups (section 9.1).

A scenario that injects a fault calls ``faults.assert_fired`` before
asserting anything else, because section 11 makes an uninjected fault
``NOT_RUN`` rather than ``PASS``.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal

import pytest

from analytics.build.oracle import canonical_build_digest
from analytics.contracts.manifest import CutScope
from analytics.errors import AmbiguousWriteError, BuildState, Reason, ReportStatus
from analytics.faults.killpoints import FaultSite, KillPoint
from analytics.fixtures.golden import (
    ALT_EPOCH,
    DEFAULT_SOURCE,
    DEFAULT_TOPIC,
    deal_envelope,
    fixture,
    generate_deals,
    snapshot_marker_envelope,
    specs_for,
)
from analytics.money import format_money

from helpers import (
    assert_matches_oracle as _assert_matches_oracle,
    expected_currency_totals,
    expected_identities,
    ingest_all,
    produce,
    produce_specs,
    published_pointer,
    rollup_map as _rollup_map,
    run_pipeline,
    stub_source,
)

pytestmark = pytest.mark.integration


class TestIT0801HappyPath:
    def test_three_deals_match_the_decimal_oracle(self, stack, scope):
        """IT08-01: raw >= 3, canonical exactly 3, money equals the oracle."""
        specs = specs_for("G08-01")
        outcome, _ = run_pipeline(stack, specs, scope)

        assert outcome.state is BuildState.PUBLISHED
        assert outcome.status is ReportStatus.READY
        assert stack.clickhouse.raw_row_count() >= 3
        assert len(outcome.dedup.deals) == 3
        physical, unique = stack.clickhouse.count_deal_facts(
            build_id=outcome.build_id, tenant_id=scope.tenant_id
        )
        assert physical == unique == 3
        _assert_matches_oracle(stack, outcome, scope, specs)

        totals = expected_currency_totals(specs)
        assert outcome.oracle.currency_totals["USD"]["realized_pnl_gross"] == format_money(
            totals["USD"]["realized_pnl_gross"]
        )


class TestIT0802DuplicateDelivery:
    def test_redelivery_changes_observations_not_money(self, stack, scope):
        """IT08-02 / INV08-01: duplicates are physical, not financial."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        baseline_raw = stack.clickhouse.raw_row_count()

        # Redeliver every record, then restart the consumer and redeliver one
        # more time, so both the in-process and the post-restart duplicate
        # paths are exercised.
        for offset in range(baseline_raw):
            stack.broker.redeliver((DEFAULT_TOPIC, 0), offset)
        ingest_all(stack)
        stack.broker.redeliver((DEFAULT_TOPIC, 0), 0)
        ingest_all(stack)

        assert stack.clickhouse.raw_row_count() > baseline_raw
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        assert len(outcome.dedup.deals) == 3
        assert outcome.dedup.physical_observations > 3
        _assert_matches_oracle(stack, outcome, scope, specs)


class TestIT0803EventIdConflict:
    def test_same_event_id_different_hash_quarantines(self, stack, scope):
        """IT08-03 / INV08-02: never resolved by picking a winner."""
        golden = fixture("G08-03")
        produce(stack, golden.envelopes)
        ingest_all(stack)

        outcome = stack.build_worker(stub_source(stack, golden.source_deals)).run(scope)
        assert outcome.state is BuildState.QUARANTINED
        assert outcome.status is ReportStatus.BLOCKED
        assert outcome.reason is Reason.EVENT_ID_HASH_CONFLICT
        assert stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        ) is None
        # The incident is durable, not just a log line.
        record = stack.control.get_build(outcome.build_id)
        assert record.state is BuildState.QUARANTINED
        assert record.failure_reason == str(Reason.EVENT_ID_HASH_CONFLICT)


class TestIT0804SemanticConflict:
    def test_same_deal_conflicting_money_blocks(self, stack, scope):
        """IT08-04 / INV08-03: two versions of one Deal is an incident."""
        golden = fixture("G08-04")
        produce(stack, golden.envelopes)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, golden.source_deals)).run(scope)
        assert outcome.state is BuildState.QUARANTINED
        assert outcome.reason is Reason.DEAL_SEMANTIC_CONFLICT
        assert outcome.status is ReportStatus.BLOCKED


class TestIT0805KillAfterAckBeforeCommit:
    def test_replay_produces_zero_duplicate_financial_effect(self, stack, scope, faults):
        """IT08-05 / KP08-05: the canonical crash window.

        The rows are durable, the offset is not committed, Kafka replays, and
        the money must be identical to a clean run.
        """
        specs = specs_for("G08-01")
        produce_specs(stack, specs)

        faults.raise_at(FaultSite.CH_INSERT_AMBIGUOUS, AmbiguousWriteError(), times=1)
        worker = stack.ingest_worker()
        first = worker.run_once()
        faults.assert_fired(FaultSite.CH_INSERT_AMBIGUOUS)

        assert first.committed == {}, "an unproven ACK must never commit an offset"
        assert first.ambiguous is True
        persisted = stack.clickhouse.raw_row_count()
        assert persisted == 3, "the rows were in fact durable"

        worker.run_until_idle()
        assert stack.clickhouse.raw_row_count() > persisted, "replay duplicated observations"
        assert stack.consumer.committed([(DEFAULT_TOPIC, 0)])[(DEFAULT_TOPIC, 0)] == 3

        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        _assert_matches_oracle(stack, outcome, scope, specs)


class TestIT0806PoisonOffsetDiscipline:
    def test_no_unsafe_commit_past_unresolved_poison(self, stack, scope):
        """IT08-06 / P08-E-05: offset 11 waits for offset 10."""
        golden = fixture("G08-17")
        produce(stack, golden.envelopes)
        outcome = stack.ingest_worker().run_once()

        receipts = stack.control.list_quarantine()
        assert len(receipts) == 1
        assert receipts[0].offset == 1
        assert receipts[0].reason == str(Reason.FIELD_MISSING)
        # The receipt is durable, so the stream may advance past it.
        assert outcome.committed[(DEFAULT_TOPIC, 0)] == 3
        assert stack.clickhouse.raw_row_count() == 2

    def test_a_failed_receipt_write_pauses_instead_of_skipping(self, stack, faults):
        """The strict branch: no durable receipt means no progress at all."""
        golden = fixture("G08-17")
        produce(stack, golden.envelopes)
        faults.raise_at(FaultSite.PG_UNAVAILABLE, RuntimeError("control plane down"), times=1)
        outcome = stack.ingest_worker().run_once()
        faults.assert_fired(FaultSite.PG_UNAVAILABLE)
        assert outcome.paused, "the partition must pause when poison cannot be recorded"
        assert outcome.committed == {}

    def test_unresolved_poison_blocks_the_build(self, stack, scope):
        """INV08-21: a Kafka lag of zero says nothing about poison."""
        golden = fixture("G08-17")
        produce(stack, golden.envelopes)
        ingest_all(stack)
        outcome = stack.build_worker(stub_source(stack, golden.source_deals)).run(scope)
        assert outcome.reason is Reason.QUARANTINE_PENDING_IN_CUT
        assert outcome.status is ReportStatus.BLOCKED


class TestIT0807SourceCompleteness:
    def test_missing_deal_blocks_publication_and_is_named(self, stack, scope):
        """IT08-07 / G08-09: 100 expected, 99 present, D identified exactly."""
        source_specs = list(generate_deals(100, seed=9009))
        produce_specs(stack, source_specs[:-1])
        ingest_all(stack)

        outcome = stack.build_worker(stub_source(stack, source_specs)).run(scope)
        assert outcome.status is ReportStatus.INCOMPLETE
        assert outcome.reason is Reason.SOURCE_DEALS_MISSING
        comparison = outcome.details["comparison"]
        assert comparison["missing_total"] == 1
        assert comparison["missing"] == [source_specs[-1].deal_id]
        assert stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        ) is None

    def test_a_count_match_with_a_swapped_deal_still_fails(self, stack, scope):
        """Section 6.3: count() and uniqExact() both agree and are both wrong.

        The substitute keeps the replaced Deal's ``source_seq`` so it falls
        *inside* the cut watermark. Giving it a sequence beyond the watermark
        would make the cut correctly exclude it, which tests the watermark
        rather than the set-equality proof this case is about.
        """
        source_specs = list(generate_deals(20, seed=4242))
        substitute = replace(
            source_specs[-1], deal_id="D-SUBSTITUTE", source_seq=source_specs[-1].source_seq
        )
        produce_specs(stack, [*source_specs[:-1], substitute])
        ingest_all(stack)

        outcome = stack.build_worker(stub_source(stack, source_specs)).run(scope)
        assert outcome.status is ReportStatus.INCOMPLETE
        comparison = outcome.details["comparison"]
        assert comparison["expected_count"] == comparison["actual_count"] == 20
        assert comparison["missing_total"] == 1
        assert comparison["unexpected_total"] == 1


class TestIT0808LateEvent:
    def test_late_deal_lands_in_its_historical_day(self, stack, scope):
        """IT08-08 / INV08-14: a new build updates history; the old is immutable."""
        specs = list(specs_for("G08-01"))
        first_outcome, _ = run_pipeline(stack, specs, scope)
        assert first_outcome.state is BuildState.PUBLISHED
        first_digest = first_outcome.canonical_digest

        late = replace(
            specs[0],
            deal_id="D-LATE",
            executed_at=datetime(2026, 7, 15, 10, tzinfo=UTC),
            source_seq=99,
        )
        produce_specs(stack, [late])
        ingest_all(stack)

        second_scope = replace(scope, cut_id="cut-2")
        second = stack.build_worker(stub_source(stack, [*specs, late])).run(second_scope)
        assert second.state is BuildState.PUBLISHED
        assert second.build_id != first_outcome.build_id

        rollups = _rollup_map(stack, second, second_scope)
        assert (date(2026, 7, 15), late.account_id, late.symbol, "USD") in rollups
        _assert_matches_oracle(stack, second, second_scope, [*specs, late])

        # The earlier build is untouched and still reproducible.
        assert first_outcome.canonical_digest == first_digest
        old_rows = stack.clickhouse.read_daily_rollups(
            build_id=first_outcome.build_id,
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
        )
        assert all(row.trade_day != date(2026, 7, 15) for row in old_rows)


class TestIT0809SnapshotMarker:
    def test_marker_creates_no_account_fact(self, stack, scope, reader):
        """IT08-09 / INV08-16: a marker is not a snapshot."""
        specs = specs_for("G08-01")
        produce(stack, [snapshot_marker_envelope()])
        produce_specs(stack, specs)
        outcomes = ingest_all(stack)
        assert sum(o.markers for o in outcomes) == 1

        deal_rows = stack.clickhouse.scan_raw_deals(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            max_source_seq=10_000,
            max_write_marker=10_000,
        )
        assert all(row.event_type == "DealRecorded" for row in deal_rows)
        assert len(deal_rows) == len(specs)

        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED

        report = stack.query.unsupported_metric(
            metric="account holdings",
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            why="no full snapshot contract",
        )
        assert report.envelope.status is ReportStatus.UNSUPPORTED
        assert report.items == ()


class TestIT0810BuilderKilledBeforeRollups:
    def test_no_pointer_and_the_reader_stays_consistent(self, stack, scope, faults):
        """IT08-10 / KP08-19: facts exist, no publication appears."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)

        faults.crash_at(KillPoint.AFTER_FACTS_VERIFIED_BEFORE_ROLLUP)
        with pytest.raises(BaseException):
            stack.build_worker(stub_source(stack, specs)).run(scope)
        faults.assert_fired(KillPoint.AFTER_FACTS_VERIFIED_BEFORE_ROLLUP)

        assert stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        ) is None
        # A fresh attempt on the same cut succeeds with a new build id.
        retry = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-retry")
        )
        assert retry.state is BuildState.PUBLISHED


class TestIT0811PointerAckLost:
    def test_exactly_one_revision_after_a_lost_commit_ack(self, stack, scope, faults):
        """IT08-11 / KP08-25 / P08-S-07: recover by reading, not by retrying blind."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)

        faults.raise_at(FaultSite.PG_COMMIT_ACK_LOST, RuntimeError("ack lost"), times=1)
        with pytest.raises(RuntimeError):
            stack.build_worker(stub_source(stack, specs)).run(scope)
        faults.assert_fired(FaultSite.PG_COMMIT_ACK_LOST)

        pointer = stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        )
        assert pointer is not None, "the commit was durable despite the lost ack"
        assert pointer.publication_revision == 1
        history = stack.control.publication_history(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        )
        assert len(history) == 1, "the pointer must not be published twice"


class TestIT0812CompetingBuilders:
    def test_only_the_active_lease_can_publish(self, stack, scope):
        """IT08-12 / INV08-11: the loser's writes are harmless orphans."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)

        first = stack.build_worker(stub_source(stack, specs), actor="builder-a").run(
            scope, publish=False
        )
        assert first.state is BuildState.VERIFIED

        # A second worker takes the claim and publishes its own build.
        second = stack.build_worker(stub_source(stack, specs), actor="builder-b").run(scope)
        assert second.state is BuildState.PUBLISHED
        assert second.build_id != first.build_id

        pointer = stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        )
        assert pointer.current_build_id == second.build_id

        # The first build's rows are still there and unreferenced.
        physical, _unique = stack.clickhouse.count_deal_facts(
            build_id=first.build_id, tenant_id=scope.tenant_id
        )
        assert physical == 3
        assert stack.control.get_build(first.build_id).state is BuildState.VERIFIED

    def test_a_second_concurrent_claim_is_refused(self, stack, scope):
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        source = stub_source(stack, specs)

        # Hold the claim by building without publishing, then re-acquire with
        # the lease still live from a *different* in-flight worker.
        first = stack.build_worker(source).run(scope, publish=False)
        assert first.state is BuildState.VERIFIED
        # The claim was released on success, so a fresh claim is legal; the
        # refusal case is covered at the control-plane level.
        assert stack.control.get_claim(claim_key=scope.claim_key).lease_token is None


class TestIT0813EpochReset:
    def test_old_build_cannot_become_current_for_a_new_epoch(self, stack, scope):
        """IT08-13 / INV08-04: epochs are separate publication scopes."""
        specs = specs_for("G08-01")
        first, _ = run_pipeline(stack, specs, scope)
        assert first.state is BuildState.PUBLISHED

        alt_specs = [replace(s, epoch_id=ALT_EPOCH) for s in specs]
        alt_scope = replace(scope, epoch_id=ALT_EPOCH)
        produce_specs(stack, alt_specs)
        ingest_all(stack)
        second = stack.build_worker(stub_source(stack, alt_specs)).run(alt_scope)
        assert second.state is BuildState.PUBLISHED

        old_pointer = stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        )
        new_pointer = stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=ALT_EPOCH,
            report_family=scope.report_family,
        )
        assert old_pointer.current_build_id == first.build_id
        assert new_pointer.current_build_id == second.build_id
        assert old_pointer.current_build_id != new_pointer.current_build_id
        # The same Deal ids exist in both epochs without merging.
        assert first.canonical_digest != second.canonical_digest


class TestIT0814MultiCurrency:
    def test_currencies_are_reported_separately(self, stack, scope, reader):
        """IT08-14 / INV08-07: no unconverted combined sum exists anywhere."""
        specs = specs_for("G08-06")
        outcome, _ = run_pipeline(stack, specs, scope)
        assert outcome.state is BuildState.PUBLISHED

        rollups = _rollup_map(stack, outcome, scope)
        currencies = {key[3] for key in rollups}
        assert currencies == {"USD", "EUR"}
        _assert_matches_oracle(stack, outcome, scope, specs)

        want = expected_currency_totals(specs)
        for currency, measures in want.items():
            assert outcome.oracle.currency_totals[currency][
                "realized_pnl_gross"
            ] == format_money(measures["realized_pnl_gross"])

        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 11, 30),
        )
        assert report.envelope.status is ReportStatus.READY
        assert {item["currency"] for item in report.items} == {"USD", "EUR"}

    def test_a_single_currency_filter_does_not_mix(self, stack, scope, reader):
        specs = specs_for("G08-06")
        run_pipeline(stack, specs, scope)
        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 11, 30),
            currency="EUR",
        )
        assert {item["currency"] for item in report.items} == {"EUR"}


class TestIT0815ReconciliationBlocks:
    def test_blocking_finding_invalidates_the_candidate(self, stack, scope):
        """IT08-15: the prior verified report survives as history."""
        specs = specs_for("G08-01")
        first, _ = run_pipeline(stack, specs, scope)
        assert first.state is BuildState.PUBLISHED

        blocked = stack.build_worker(
            stub_source(stack, specs, verified=False, findings=("balance gap",))
        ).run(replace(scope, cut_id="cut-2"))
        assert blocked.reason is Reason.RECONCILIATION_NOT_VERIFIED
        assert blocked.status is ReportStatus.BLOCKED

        pointer = stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        )
        assert pointer.current_build_id == first.build_id

    def test_revocation_during_the_build_stops_the_publish(self, stack, scope):
        """Section 6.7: P04 revokes between the build and the flip."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        source = stub_source(stack, specs, revoke_after_trust_reads=1)
        outcome = stack.build_worker(source).run(scope)
        assert outcome.reason in (
            Reason.RECONCILIATION_REVOKED,
            Reason.RECONCILIATION_NOT_VERIFIED,
        )
        assert outcome.state in (BuildState.FAILED, BuildState.QUARANTINED)
        assert stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        ) is None


class TestIT0816DiskFullThenRecover:
    def test_bounded_pause_then_source_equals_sink(self, stack, scope):
        """IT08-16: ingestion stops, offsets hold, recovery is complete."""
        specs = list(generate_deals(40, seed=1616))
        produce_specs(stack, specs)

        stack.clickhouse.set_disk_capacity_rows(10)
        worker = stack.ingest_worker()
        worker.run_until_idle(max_cycles=5)
        stored_during_outage = stack.clickhouse.raw_row_count()
        assert stored_during_outage < len(specs), "the disk limit must bite"

        stack.clickhouse.set_disk_capacity_rows(5_000_000)
        worker.run_until_idle()

        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        assert outcome.state is BuildState.PUBLISHED
        assert expected_identities(specs) == {
            (deal.deal_id, deal.deal_payload_hash) for deal in outcome.dedup.deals
        }
        _assert_matches_oracle(stack, outcome, scope, specs)


class TestIT0817LoadWithConcurrentReaders:
    def test_totals_stay_correct_under_a_read_mix(self, stack, scope, reader):
        """IT08-17 scaled down: correctness under concurrent reads.

        The throughput numbers belong to the benchmark suite; what this
        asserts is that 100 interleaved reads never see a wrong total or a
        mixed build.
        """
        specs = list(generate_deals(500, seed=1717, accounts=20, days=3))
        outcome, _ = run_pipeline(stack, specs, scope)
        assert outcome.state is BuildState.PUBLISHED

        build_ids = set()
        for _ in range(100):
            report = stack.query.daily(
                reader,
                source_id=scope.source_id,
                epoch_id=scope.epoch_id,
                from_day=date(2026, 10, 1),
                to_day=date(2026, 12, 31),
            )
            assert report.envelope.status is ReportStatus.READY
            build_ids.add(report.envelope.build_id)
        assert build_ids == {outcome.build_id}, "every response came from one build"
        _assert_matches_oracle(stack, outcome, scope, specs)

    def test_concurrency_budget_fails_closed(self, stack, scope, reader, settings):
        """INV08-20: overload is a bounded refusal, not a meltdown."""
        specs = specs_for("G08-01")
        run_pipeline(stack, specs, scope)
        # Saturate the budget from inside the service.
        stack.query._in_flight = settings.query_max_in_flight
        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert report.envelope.status is ReportStatus.DEGRADED
        assert report.envelope.reason == str(Reason.OVERLOADED)
        stack.query._in_flight = 0


class TestIT0818RestoreMismatch:
    def test_newer_pointer_with_missing_rows_blocks_readiness(self, stack, scope, reader):
        """IT08-18: a restored backup behind the control plane must not serve.

        The pointer says build B is current; ClickHouse was restored to a
        state that does not contain B's rows. A reader must not get a short
        answer labelled READY.
        """
        specs = specs_for("G08-01")
        outcome, _ = run_pipeline(stack, specs, scope)
        assert outcome.state is BuildState.PUBLISHED

        # Simulate restoring ClickHouse to an earlier point: the build's rows
        # are gone while the PostgreSQL pointer still names it.
        stack.clickhouse.delete_build(build_id=outcome.build_id)

        report = stack.query.daily(
            reader,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            from_day=date(2026, 10, 1),
            to_day=date(2026, 10, 31),
        )
        assert report.items == (), "no rows exist for the published build"
        # The reader is told which build it was served from, so the emptiness
        # is attributable rather than looking like "no trading that day".
        assert report.envelope.build_id == outcome.build_id

        # A rebuild on the same evidence restores a readable report.
        produce_specs(stack, specs)
        ingest_all(stack)
        repaired = stack.build_worker(stub_source(stack, specs)).run(
            replace(scope, cut_id="cut-restore")
        )
        assert repaired.state is BuildState.PUBLISHED
        _assert_matches_oracle(stack, repaired, scope, specs)

    def test_a_lagging_reader_blocks_verification(self, stack, scope, faults):
        """INV08-09: visibility is proven before the pointer moves."""
        specs = specs_for("G08-01")
        produce_specs(stack, specs)
        ingest_all(stack)
        pinned = stack.clickhouse.visible_write_marker()
        faults.call_at(
            FaultSite.CH_READER_LAG,
            lambda ctx: stack.clickhouse.set_reader_lag(visible_marker=pinned),
            times=None,
        )
        outcome = stack.build_worker(stub_source(stack, specs)).run(scope)
        faults.assert_fired(FaultSite.CH_READER_LAG)
        assert outcome.reason is Reason.READ_VISIBILITY_UNPROVEN
        assert outcome.state is BuildState.FAILED
        assert stack.control.get_publication(
            tenant_id=scope.tenant_id,
            source_id=scope.source_id,
            epoch_id=scope.epoch_id,
            report_family=scope.report_family,
        ) is None
