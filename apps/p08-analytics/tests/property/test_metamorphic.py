"""Metamorphic properties from section 11.2.

A metamorphic test does not need to know the right answer; it asserts that a
transformation of the *input* leaves the output unchanged (or changes it in a
stated way). That is exactly the right tool here, because the dangerous bugs
are the ones where the number looks plausible: "add any number of duplicate
deliveries and the canonical digest is invariant" catches a double-count
without anyone computing the expected total by hand.

The properties, verbatim from the design:

* any number of exact duplicate deliveries -> invariant canonical digest;
* reordering independent accounts -> invariant per-source day sums;
* splitting or recombining the ingest batch -> same outputs;
* changing only the ingestion time -> same UTC trade day;
* two tenants with the same account and deal ids -> no combined money;
* rebuilding identical pinned inputs -> identical sorted canonical digest.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import HealthCheck, given, settings as hyp_settings, strategies as st

from analytics.build.dedup import canonicalise
from analytics.build.oracle import canonical_build_digest
from analytics.build.rollup import build_daily_rollups, rollup_totals
from analytics.canonical import canonical_json
from analytics.contracts.policy import default_p0_policy
from analytics.errors import BuildState
from analytics.fixtures.golden import (
    ALT_EPOCH,
    DEFAULT_EPOCH,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DEFAULT_TOPIC,
    DealSpec,
    deal_envelope,
    generate_deals,
    specs_for,
)
from analytics.ports.clickhouse import RawObservation

from helpers import (
    assert_matches_oracle,
    expected_daily,
    ingest_all,
    produce,
    produce_specs,
    run_pipeline,
    stub_source,
)

pytestmark = pytest.mark.property

SCOPE_IDENTITY = (DEFAULT_TENANT, DEFAULT_SOURCE, DEFAULT_EPOCH)
POLICY = default_p0_policy()

PROP_SETTINGS = hyp_settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)


def _raw(spec: DealSpec, *, event_id: str, offset: int) -> RawObservation:
    return RawObservation(
        tenant_id=spec.tenant_id,
        source_id=spec.source_id,
        epoch_id=spec.epoch_id,
        event_id=event_id,
        dedupe_key=f"deal:{spec.deal_id}",
        kafka_record_key=spec.account_id,
        event_type="DealRecorded",
        schema_version=1,
        payload_sha256="a" * 64,
        deal_payload_hash=spec.deal_payload_hash(),
        deal_id=spec.deal_id,
        account_id=spec.account_id,
        source_seq=spec.source_seq,
        aggregate_version=spec.source_seq,
        event_time=spec.executed_at,
        executed_at=spec.executed_at,
        kafka_topic=DEFAULT_TOPIC,
        kafka_partition=0,
        kafka_offset=offset,
        payload_json=canonical_json(spec.payload()),
        ingested_at=datetime(2026, 10, 30, 12, tzinfo=UTC),
    )


def _uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"


def _digest_of(specs, *, duplicates: int = 0) -> str:
    rows = [_raw(spec, event_id=_uuid(i), offset=i) for i, spec in enumerate(specs)]
    for copy_index in range(duplicates):
        base = len(rows)
        rows.extend(
            _raw(spec, event_id=_uuid(i), offset=base + i + copy_index * len(specs))
            for i, spec in enumerate(specs)
        )
    result = canonicalise(rows, policy=POLICY, scope=SCOPE_IDENTITY)
    return canonical_build_digest(result.deals)


class TestDuplicateInvariance:
    @given(copies=st.integers(min_value=0, max_value=6))
    @PROP_SETTINGS
    def test_any_number_of_duplicates_leaves_the_digest_unchanged(self, copies):
        """INV08-01: at-least-once delivery cannot change the money."""
        specs = specs_for("G08-01")
        assert _digest_of(specs, duplicates=copies) == _digest_of(specs)

    @given(copies=st.integers(min_value=1, max_value=4))
    @PROP_SETTINGS
    def test_duplicates_change_only_the_observation_count(self, copies):
        specs = specs_for("G08-01")
        rows = [_raw(spec, event_id=_uuid(i), offset=i) for i, spec in enumerate(specs)]
        duplicated = list(rows)
        for copy_index in range(copies):
            duplicated.extend(
                _raw(spec, event_id=_uuid(i), offset=100 + i + copy_index * 10)
                for i, spec in enumerate(specs)
            )
        once = canonicalise(rows, policy=POLICY, scope=SCOPE_IDENTITY)
        many = canonicalise(duplicated, policy=POLICY, scope=SCOPE_IDENTITY)
        assert len(once.deals) == len(many.deals)
        assert many.physical_observations == len(specs) * (copies + 1)
        assert rollup_totals(build_daily_rollups(once.deals, build_id="b")) == rollup_totals(
            build_daily_rollups(many.deals, build_id="b")
        )


class TestOrderInvariance:
    @given(seed=st.integers(min_value=0, max_value=10_000))
    @PROP_SETTINGS
    def test_reordering_observations_leaves_the_digest_unchanged(self, seed):
        """P08-R-11: physical row order is not part of the answer."""
        specs = list(generate_deals(25, seed=seed % 997 + 1))
        rows = [_raw(spec, event_id=_uuid(i), offset=i) for i, spec in enumerate(specs)]
        forward = canonicalise(rows, policy=POLICY, scope=SCOPE_IDENTITY)
        reverse = canonicalise(list(reversed(rows)), policy=POLICY, scope=SCOPE_IDENTITY)
        assert canonical_build_digest(forward.deals) == canonical_build_digest(reverse.deals)

    @given(data=st.data())
    @PROP_SETTINGS
    def test_shuffling_accounts_leaves_day_sums_unchanged(self, data):
        specs = list(generate_deals(30, seed=7))
        shuffled = data.draw(st.permutations(specs))
        a = build_daily_rollups(
            canonicalise(
                [_raw(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)],
                policy=POLICY,
                scope=SCOPE_IDENTITY,
            ).deals,
            build_id="b",
        )
        b = build_daily_rollups(
            canonicalise(
                [_raw(s, event_id=_uuid(i), offset=i) for i, s in enumerate(shuffled)],
                policy=POLICY,
                scope=SCOPE_IDENTITY,
            ).deals,
            build_id="b",
        )
        assert rollup_totals(a) == rollup_totals(b)


class TestBatchSplitting:
    @given(batch_size=st.integers(min_value=1, max_value=20))
    @PROP_SETTINGS
    def test_batch_size_does_not_change_the_result(self, batch_size, settings, clock):
        """Splitting or recombining the ingest batch yields one answer."""
        from analytics.main import build_lab_stack

        specs = list(generate_deals(20, seed=31))
        digests = []
        for size in (batch_size, 20):
            stack = build_lab_stack(
                settings=settings.model_copy(update={"kafka_max_batch_records": size}),
                clock=clock,
            )
            try:
                produce_specs(stack, specs)
                ingest_all(stack)
                rows = stack.clickhouse.scan_raw_deals(
                    tenant_id=DEFAULT_TENANT,
                    source_id=DEFAULT_SOURCE,
                    epoch_id=DEFAULT_EPOCH,
                    max_source_seq=10_000,
                    max_write_marker=10_000,
                )
                result = canonicalise(list(rows), policy=POLICY, scope=SCOPE_IDENTITY)
                digests.append(canonical_build_digest(result.deals))
            finally:
                stack.close()
        assert digests[0] == digests[1]


class TestIngestionTimeIrrelevance:
    @given(hours=st.integers(min_value=0, max_value=5_000))
    @PROP_SETTINGS
    def test_changing_only_the_ingestion_time_keeps_the_trade_day(self, hours):
        """INV08-14: the day follows ``executed_at`` alone."""
        spec = specs_for("G08-01")[0]
        early = _raw(spec, event_id=_uuid(1), offset=0)
        late = replace(
            early, ingested_at=early.ingested_at + timedelta(hours=hours)
        )
        a = build_daily_rollups(
            canonicalise([early], policy=POLICY, scope=SCOPE_IDENTITY).deals, build_id="b"
        )
        b = build_daily_rollups(
            canonicalise([late], policy=POLICY, scope=SCOPE_IDENTITY).deals, build_id="b"
        )
        assert [row.trade_day for row in a] == [row.trade_day for row in b]
        assert canonical_build_digest(
            canonicalise([early], policy=POLICY, scope=SCOPE_IDENTITY).deals
        ) == canonical_build_digest(
            canonicalise([late], policy=POLICY, scope=SCOPE_IDENTITY).deals
        )


class TestScopeSeparation:
    @given(seed=st.integers(min_value=1, max_value=500))
    @PROP_SETTINGS
    def test_two_epochs_with_identical_ids_never_merge(self, seed):
        """INV08-04: a reused Deal id across epochs is not one Deal."""
        specs = list(generate_deals(10, seed=seed))
        alt = [replace(spec, epoch_id=ALT_EPOCH) for spec in specs]
        first = canonicalise(
            [_raw(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)],
            policy=POLICY,
            scope=SCOPE_IDENTITY,
        )
        second = canonicalise(
            [_raw(s, event_id=_uuid(100 + i), offset=100 + i) for i, s in enumerate(alt)],
            policy=POLICY,
            scope=(DEFAULT_TENANT, DEFAULT_SOURCE, ALT_EPOCH),
        )
        assert len(first.deals) == len(second.deals) == len(specs)
        assert {d.deal_id for d in first.deals} == {d.deal_id for d in second.deals}
        assert canonical_build_digest(first.deals) != canonical_build_digest(second.deals)

    @given(seed=st.integers(min_value=1, max_value=500))
    @PROP_SETTINGS
    def test_two_tenants_with_identical_ids_never_combine(self, seed):
        """INV08-17: identical account and Deal ids under two tenants."""
        specs = list(generate_deals(8, seed=seed))
        rival = [replace(spec, tenant_id="rival-tenant") for spec in specs]
        mine = canonicalise(
            [_raw(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)],
            policy=POLICY,
            scope=SCOPE_IDENTITY,
        )
        theirs = canonicalise(
            [_raw(s, event_id=_uuid(200 + i), offset=200 + i) for i, s in enumerate(rival)],
            policy=POLICY,
            scope=("rival-tenant", DEFAULT_SOURCE, DEFAULT_EPOCH),
        )
        assert canonical_build_digest(mine.deals) != canonical_build_digest(theirs.deals)
        # Neither build contains a row belonging to the other tenant.
        assert all(deal.tenant_id == DEFAULT_TENANT for deal in mine.deals)
        assert all(deal.tenant_id == "rival-tenant" for deal in theirs.deals)


class TestRebuildDeterminism:
    @given(seed=st.integers(min_value=1, max_value=400))
    @PROP_SETTINGS
    def test_rebuilding_pinned_inputs_is_byte_identical(self, seed, settings, clock, scope):
        """INV08-22: the headline reproducibility claim."""
        from analytics.main import build_lab_stack

        specs = list(generate_deals(15, seed=seed))
        digests = []
        for _ in range(2):
            stack = build_lab_stack(settings=settings, clock=clock)
            try:
                outcome, _ = run_pipeline(stack, specs, scope)
                assert outcome.state is BuildState.PUBLISHED
                digests.append(outcome.canonical_digest)
                assert_matches_oracle(stack, outcome, scope, specs)
            finally:
                stack.close()
        assert digests[0] == digests[1]

    @given(seed=st.integers(min_value=1, max_value=400))
    @PROP_SETTINGS
    def test_the_independent_oracle_agrees_on_random_corpora(
        self, seed, settings, clock, scope
    ):
        """The strongest single property: P08 agrees with plain Decimal.

        A fresh stack per example, because a function-scoped fixture is
        created once and then shared across every Hypothesis example - the
        second example would then build against a stack that already holds a
        publication for this cut, and fail for a reason unrelated to the
        property.
        """
        from analytics.main import build_lab_stack

        specs = list(generate_deals(40, seed=seed, accounts=6, days=4))
        stack = build_lab_stack(settings=settings, clock=clock)
        try:
            outcome, _ = run_pipeline(stack, specs, scope)
            assert outcome.state is BuildState.PUBLISHED
            assert_matches_oracle(stack, outcome, scope, specs)
            rows = stack.clickhouse.read_daily_rollups(
                build_id=outcome.build_id,
                tenant_id=scope.tenant_id,
                source_id=scope.source_id,
                epoch_id=scope.epoch_id,
            )
            assert sum(row.deal_count for row in rows) == len(specs)
            assert len(rows) == len(expected_daily(specs))
        finally:
            stack.close()
