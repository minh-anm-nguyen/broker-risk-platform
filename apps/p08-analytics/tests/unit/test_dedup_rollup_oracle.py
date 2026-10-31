"""Conflict detection, canonical selection, rollups and the oracles.

Section 6.2-6.6, what-if domains G, R, U. These are the pure functions behind
the build, tested without any adapter so a failure here points at the logic
rather than at the plumbing.
"""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from analytics.build.dedup import canonicalise, scan_conflicts
from analytics.build.oracle import (
    canonical_build_digest,
    compare_identities,
    expected_daily_from_facts,
    verify_rollups,
)
from analytics.build.rollup import build_daily_rollups, rollup_totals
from analytics.canonical import canonical_json
from analytics.contracts.manifest import CutScope, build_source_manifest
from analytics.contracts.policy import default_p0_policy
from analytics.errors import CompletenessError, IntegrityError, Reason
from analytics.fixtures.golden import (
    ALT_EPOCH,
    DEFAULT_EPOCH,
    DEFAULT_POLICY_VERSION,
    DEFAULT_REPORT_FAMILY,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DealSpec,
    specs_for,
)
from analytics.ports.clickhouse import RawObservation

pytestmark = pytest.mark.unit

SCOPE_IDENTITY = (DEFAULT_TENANT, DEFAULT_SOURCE, DEFAULT_EPOCH)


def raw_from_spec(
    spec: DealSpec,
    *,
    event_id: str = "00000000-0000-4000-8000-000000000001",
    offset: int = 0,
    partition: int = 0,
    payload_hash: str | None = None,
    deal_hash: str | None = None,
    account_override: str | None = None,
) -> RawObservation:
    """Build a raw observation directly, bypassing Kafka."""
    payload = spec.payload()
    return RawObservation(
        tenant_id=spec.tenant_id,
        source_id=spec.source_id,
        epoch_id=spec.epoch_id,
        event_id=event_id,
        dedupe_key=f"deal:{spec.deal_id}",
        kafka_record_key=spec.account_id,
        event_type="DealRecorded",
        schema_version=1,
        payload_sha256=payload_hash or ("a" * 64),
        deal_payload_hash=deal_hash or spec.deal_payload_hash(),
        deal_id=spec.deal_id,
        account_id=account_override or spec.account_id,
        source_seq=spec.source_seq,
        aggregate_version=spec.source_seq,
        event_time=spec.executed_at,
        executed_at=spec.executed_at,
        kafka_topic="broker.deals.v1",
        kafka_partition=partition,
        kafka_offset=offset,
        payload_json=canonical_json(payload),
        ingested_at=datetime(2026, 10, 30, 12, tzinfo=UTC),
    )


@pytest.fixture
def specs():
    return list(specs_for("G08-01"))


@pytest.fixture
def policy():
    return default_p0_policy()


@pytest.fixture
def scope():
    return CutScope(
        DEFAULT_TENANT, DEFAULT_SOURCE, DEFAULT_EPOCH, DEFAULT_REPORT_FAMILY,
        "cut-1", DEFAULT_POLICY_VERSION,
    )


class TestConflictScan:
    def test_clean_input_reports_no_conflict(self, specs):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        assert scan_conflicts(rows, expect_scope=SCOPE_IDENTITY).is_clean

    def test_identical_redelivery_is_not_a_conflict(self, specs):
        """INV08-01: at-least-once delivery is the contract, not an anomaly."""
        row = raw_from_spec(specs[0], event_id=_uuid(1), offset=0)
        twin = raw_from_spec(specs[0], event_id=_uuid(1), offset=7)
        assert scan_conflicts([row, twin], expect_scope=SCOPE_IDENTITY).is_clean

    def test_same_event_id_different_payload_hash_conflicts(self, specs):
        """INV08-02: never resolved by picking a winner."""
        a = raw_from_spec(specs[0], event_id=_uuid(1), payload_hash="a" * 64)
        b = raw_from_spec(specs[0], event_id=_uuid(1), payload_hash="b" * 64, offset=1)
        report = scan_conflicts([a, b], expect_scope=SCOPE_IDENTITY)
        assert report.event_id_conflicts
        with pytest.raises(IntegrityError) as excinfo:
            report.raise_if_dirty()
        assert excinfo.value.reason is Reason.EVENT_ID_HASH_CONFLICT

    def test_same_deal_different_content_hash_conflicts(self, specs):
        """INV08-03: two versions of one Deal's money is an incident."""
        a = raw_from_spec(specs[0], event_id=_uuid(1), deal_hash="a" * 64)
        b = raw_from_spec(specs[0], event_id=_uuid(2), deal_hash="b" * 64, offset=1)
        report = scan_conflicts([a, b], expect_scope=SCOPE_IDENTITY)
        assert report.deal_hash_conflicts
        with pytest.raises(IntegrityError) as excinfo:
            report.raise_if_dirty()
        assert excinfo.value.reason is Reason.DEAL_SEMANTIC_CONFLICT

    def test_same_deal_same_hash_under_two_event_ids_is_clean(self, specs):
        """P08-G-04: an outbox replay under a new event id is harmless."""
        a = raw_from_spec(specs[0], event_id=_uuid(1))
        b = raw_from_spec(specs[0], event_id=_uuid(2), offset=1)
        assert scan_conflicts([a, b], expect_scope=SCOPE_IDENTITY).is_clean

    def test_equal_hash_with_different_account_is_caught(self, specs):
        """P08-R-03: a hash match is not a field match.

        A producer that computed the digest over the wrong field set can ship
        the same money under a different account with a matching hash. Trusting
        the digest alone would file the Deal against the wrong account.
        """
        a = raw_from_spec(specs[0], event_id=_uuid(1))
        b = raw_from_spec(specs[0], event_id=_uuid(2), offset=1, account_override="A-999")
        report = scan_conflicts([a, b], expect_scope=SCOPE_IDENTITY)
        assert report.field_mismatches
        with pytest.raises(IntegrityError):
            report.raise_if_dirty()

    def test_row_outside_the_pinned_scope_is_an_error(self, specs):
        """A bad selection is worse than a data conflict."""
        stray = raw_from_spec(replace(specs[0], epoch_id=ALT_EPOCH))
        with pytest.raises(IntegrityError) as excinfo:
            scan_conflicts([stray], expect_scope=SCOPE_IDENTITY)
        assert excinfo.value.reason is Reason.EVENT_ID_SCOPE_COLLISION

    def test_two_deals_sharing_a_payload_hash_both_survive(self, specs):
        """P08-G-08: a hash is not an identity."""
        a = raw_from_spec(specs[0], event_id=_uuid(1), payload_hash="c" * 64)
        b = raw_from_spec(specs[1], event_id=_uuid(2), offset=1, payload_hash="c" * 64)
        assert scan_conflicts([a, b], expect_scope=SCOPE_IDENTITY).is_clean


class TestCanonicalise:
    def test_collapses_duplicates_to_one_fact(self, specs, policy):
        rows = [
            raw_from_spec(specs[0], event_id=_uuid(1), offset=0),
            raw_from_spec(specs[0], event_id=_uuid(1), offset=5),
            raw_from_spec(specs[0], event_id=_uuid(2), offset=9),
        ]
        result = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)
        assert len(result.deals) == 1
        assert result.physical_observations == 3
        assert result.deals[0].observation_count == 3

    def test_picks_a_deterministic_representative(self, specs, policy):
        """INV08-22: two rebuilds must agree on the canonical digest."""
        rows = [
            raw_from_spec(specs[0], event_id=_uuid(2), offset=9),
            raw_from_spec(specs[0], event_id=_uuid(1), offset=0),
        ]
        first = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)
        second = canonicalise(list(reversed(rows)), policy=policy, scope=SCOPE_IDENTITY)
        assert first.deals[0].event_id == second.deals[0].event_id
        assert canonical_build_digest(first.deals) == canonical_build_digest(second.deals)

    def test_distinct_epochs_stay_separate(self, specs, policy):
        """INV08-04: a reused Deal id across epochs is not one Deal."""
        rows = [raw_from_spec(specs[0], event_id=_uuid(1))]
        alt = [raw_from_spec(replace(specs[0], epoch_id=ALT_EPOCH), event_id=_uuid(2))]
        first = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)
        second = canonicalise(
            alt, policy=policy, scope=(DEFAULT_TENANT, DEFAULT_SOURCE, ALT_EPOCH)
        )
        assert first.deals[0].deal_id == second.deals[0].deal_id
        assert first.deals[0].epoch_id != second.deals[0].epoch_id
        assert canonical_build_digest(first.deals) != canonical_build_digest(second.deals)

    def test_conflict_blocks_by_default(self, specs, policy):
        rows = [
            raw_from_spec(specs[0], event_id=_uuid(1), deal_hash="a" * 64),
            raw_from_spec(specs[0], event_id=_uuid(2), deal_hash="b" * 64, offset=1),
        ]
        with pytest.raises(IntegrityError):
            canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)

    def test_forensic_mode_describes_without_producing_facts(self, specs, policy):
        rows = [
            raw_from_spec(specs[0], event_id=_uuid(1), deal_hash="a" * 64),
            raw_from_spec(specs[0], event_id=_uuid(2), deal_hash="b" * 64, offset=1),
        ]
        result = canonicalise(
            rows, policy=policy, scope=SCOPE_IDENTITY, fail_on_conflict=False
        )
        assert result.deals == ()
        assert result.policy_failures

    def test_unsupported_currency_stops_the_build(self, specs, policy):
        rows = [raw_from_spec(replace(specs[0], money_currency="JPY"), event_id=_uuid(1))]
        with pytest.raises(Exception) as excinfo:
            canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)
        assert excinfo.value.reason is Reason.CURRENCY_UNSUPPORTED

    def test_policy_version_drift_stops_the_build(self, specs, policy):
        drifted = replace(specs[0], accounting_policy_version="other-policy-v9")
        rows = [raw_from_spec(drifted, event_id=_uuid(1))]
        with pytest.raises(Exception) as excinfo:
            canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)
        assert excinfo.value.reason is Reason.POLICY_VERSION_UNKNOWN

    def test_catalog_version_drift_stops_the_build(self, specs, policy):
        drifted = replace(specs[0], symbol_catalog_version="sym-catalog-v9")
        rows = [raw_from_spec(drifted, event_id=_uuid(1))]
        with pytest.raises(Exception) as excinfo:
            canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY)
        assert excinfo.value.reason is Reason.SYMBOL_UNKNOWN


class TestRollup:
    def test_groups_by_day_account_symbol_currency(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        keys = {(r.trade_day.isoformat(), r.account_id, r.symbol, r.money_currency) for r in rollups}
        assert keys == {
            ("2026-10-30", "A-1", "EURUSD", "USD"),
            ("2026-10-30", "A-2", "GBPUSD", "USD"),
        }

    def test_sums_match_hand_arithmetic(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        by_account = {r.account_id: r for r in build_daily_rollups(deals, build_id="b1")}
        # 12.30 + (-4.75) = 7.55; -0.50 + -1.00 = -1.50; -0.10 + 0.25 = 0.15
        assert by_account["A-1"].deal_count == 2
        assert by_account["A-1"].realized_pnl_gross == Decimal("7.55")
        assert by_account["A-1"].commission == Decimal("-1.50")
        assert by_account["A-1"].swap == Decimal("0.15")
        assert by_account["A-2"].realized_pnl_gross == Decimal("100.00")

    def test_currencies_never_combine(self, policy):
        """INV08-07 is structural: the currency is part of the key."""
        base = specs_for("G08-06")
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(base)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        currencies = {r.money_currency for r in rollups}
        assert currencies == {"USD", "EUR"}
        totals = rollup_totals(rollups)
        assert set(totals) == {"USD", "EUR"}

    def test_uses_executed_at_not_ingestion(self, specs, policy):
        """INV08-14 / P08-U-04: a replayed old Deal keeps its own day."""
        old = replace(specs[0], executed_at=datetime(2024, 1, 15, 10, tzinfo=UTC))
        rows = [raw_from_spec(old, event_id=_uuid(1))]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        assert rollups[0].trade_day.isoformat() == "2024-01-15"

    def test_midnight_boundary_splits_days(self, policy):
        """G08-15: four Deals, three UTC days.

        Two of them fall either side of 01:00 on the same UTC day and share an
        account, symbol and currency, so they belong in one row with a count of
        two. The year boundary genuinely splits (P08-U-12).
        """
        boundary = specs_for("G08-15")
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(boundary)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        counts = {r.trade_day.isoformat(): r.deal_count for r in rollups}
        assert counts == {"2026-03-29": 2, "2026-12-31": 1, "2027-01-01": 1}
        assert sum(counts.values()) == len(boundary)


class TestIdentityOracle:
    def test_matching_sets_pass(self, specs, policy, scope):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        manifest = _manifest(scope, specs)
        comparison = compare_identities(manifest, deals)
        assert comparison.matches
        comparison.raise_if_mismatched()

    def test_missing_deal_is_named(self, specs, policy, scope):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs[:-1])]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        comparison = compare_identities(_manifest(scope, specs), deals)
        assert comparison.missing == (specs[-1].deal_id,)
        with pytest.raises(CompletenessError) as excinfo:
            comparison.raise_if_mismatched()
        assert excinfo.value.reason is Reason.SOURCE_DEALS_MISSING

    def test_unexpected_deal_is_named(self, specs, policy, scope):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        comparison = compare_identities(_manifest(scope, specs[:-1]), deals)
        assert comparison.unexpected == (specs[-1].deal_id,)
        with pytest.raises(CompletenessError) as excinfo:
            comparison.raise_if_mismatched()
        assert excinfo.value.reason is Reason.SOURCE_DEALS_UNEXPECTED

    def test_count_alone_cannot_hide_a_swap(self, specs, policy, scope):
        """Section 6.3: one lost Deal plus one extra satisfies count()."""
        extra = replace(specs[0], deal_id="D-SUBSTITUTE", source_seq=99)
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs[:-1])]
        rows.append(raw_from_spec(extra, event_id=_uuid(99), offset=99))
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        comparison = compare_identities(_manifest(scope, specs), deals)
        assert comparison.expected_count == comparison.actual_count
        assert not comparison.matches
        assert comparison.missing and comparison.unexpected

    def test_hash_mismatch_is_detected(self, specs, policy, scope):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        manifest = build_source_manifest(
            scope,
            [(s.deal_id, "f" * 64, s.source_seq) for s in specs],
            captured_at=datetime(2026, 10, 30, 12, tzinfo=UTC),
        )
        comparison = compare_identities(manifest, deals)
        assert len(comparison.hash_mismatches) == len(specs)
        with pytest.raises(CompletenessError) as excinfo:
            comparison.raise_if_mismatched()
        assert excinfo.value.reason is Reason.SOURCE_HASH_MISMATCH


class TestMoneyOracle:
    def test_agreement_between_the_two_paths(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        comparison = verify_rollups(deals, rollups)
        assert comparison.matches
        comparison.raise_if_mismatched()

    def test_detects_a_tampered_rollup_value(self, specs, policy):
        """A mutated aggregation must be caught by the Decimal recomputation."""
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        tampered = [replace(rollups[0], realized_pnl_gross=Decimal("999.99")), *rollups[1:]]
        comparison = verify_rollups(deals, tampered)
        assert not comparison.matches
        with pytest.raises(CompletenessError) as excinfo:
            comparison.raise_if_mismatched()
        assert excinfo.value.reason is Reason.ORACLE_MONEY_MISMATCH

    def test_detects_a_doubled_count(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        doubled = [replace(rollups[0], deal_count=rollups[0].deal_count * 2), *rollups[1:]]
        assert not verify_rollups(deals, doubled).matches

    def test_detects_a_missing_rollup_row(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        comparison = verify_rollups(deals, rollups[:-1])
        assert comparison.missing_keys
        with pytest.raises(CompletenessError) as excinfo:
            comparison.raise_if_mismatched()
        assert excinfo.value.reason is Reason.ROLLUP_SET_MISMATCH

    def test_detects_a_duplicate_rollup_key(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        with pytest.raises(CompletenessError) as excinfo:
            verify_rollups(deals, [*rollups, rollups[0]])
        assert excinfo.value.reason is Reason.ROLLUP_SET_MISMATCH

    def test_rejects_rollups_without_facts(self, specs, policy):
        """Circular-oracle guard. P08-R-09, section 9.1."""
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals
        rollups = build_daily_rollups(deals, build_id="b1")
        with pytest.raises(Exception) as excinfo:
            verify_rollups([], rollups)
        assert excinfo.value.reason is Reason.ORACLE_NOT_INDEPENDENT

    def test_duplicate_fact_identity_raises(self, specs, policy):
        rows = [raw_from_spec(s, event_id=_uuid(i), offset=i) for i, s in enumerate(specs)]
        deals = list(canonicalise(rows, policy=policy, scope=SCOPE_IDENTITY).deals)
        with pytest.raises(CompletenessError) as excinfo:
            expected_daily_from_facts([*deals, deals[0]])
        assert excinfo.value.reason is Reason.ORACLE_COUNT_MISMATCH


def _manifest(scope, specs):
    return build_source_manifest(
        scope,
        [(s.deal_id, s.deal_payload_hash(), s.source_seq) for s in specs],
        captured_at=datetime(2026, 10, 30, 12, tzinfo=UTC),
    )


def _uuid(n: int) -> str:
    return f"00000000-0000-4000-8000-{n:012d}"
