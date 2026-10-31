"""Every golden fixture must actually behave as it declares. Section 9.3.

``GoldenFixture.expected_outcome`` is a promise. Without a test that runs the
fixture and checks it, the promise is just a string in a dataclass - and the
twenty fixtures are supposed to be the corpus every other claim leans on.

Some fixtures are intentionally invalid (G08-03's duplicate hash, G08-20's
unknown fee coverage). For those, PASS means the pipeline *refused* with the
declared reason. A fixture builder that quietly repaired them would delete
the test, so this file asserts the refusal just as firmly as the success.

The register in :mod:`analytics.invariants` gets the same treatment: an entry
is checkable, or it is only a claim.
"""

from __future__ import annotations

from dataclasses import replace

import pytest

from analytics.errors import BuildState, Reason, ReportStatus
from analytics.fixtures.golden import ALT_EPOCH, fixture, fixture_ids, specs_for
from analytics.invariants import INVARIANTS, INVARIANTS_BY_ID, unproven

from helpers import (
    assert_matches_oracle,
    ingest_all,
    produce,
    published_pointer,
    stub_source,
)

pytestmark = pytest.mark.integration

#: Fixtures whose scenario needs machinery beyond "produce, ingest, build":
#: a crash, a retention truncation, a rebalance, a GC attempt, or a corpus
#: generated on demand. Each is executed by the suite named here, and listing
#: them keeps the exclusion honest rather than silent.
COVERED_ELSEWHERE = {
    "G08-10": "chaos.test_kill_points KP08-16/17 (builder killed mid-facts)",
    "G08-12": (
        "integration.IT08-15 + whatif.TestDomainsXAndY - the fixture's corpus is "
        "valid; the scenario is the injected mid-build revocation, which a plain "
        "build over these envelopes correctly publishes without"
    ),
    "G08-13": "performance.test_benchmarks (100k corpus, generated on demand)",
    "G08-14": "declared: the 1M corpus is a resource profile, not a per-commit test",
    "G08-16": "component.test_kafka_adapter.TestRetention + chaos.CC08-04",
    "G08-18": "component.test_kafka_adapter.TestGenerationFencing",
    "G08-19": "chaos.test_kill_points KP08-28 + whatif.TestDomainsACADAM",
}


def _run_fixture(stack, scope, fixture_id):
    """Produce the fixture's envelopes, ingest, and build on its source set."""
    item = fixture(fixture_id)
    produce(stack, item.envelopes)
    ingest_all(stack)
    source_specs = item.source_deals or specs_for(fixture_id)
    outcome = stack.build_worker(stub_source(stack, source_specs)).run(scope)
    return item, outcome, source_specs


class TestGoldenFixtureConformance:
    @pytest.mark.parametrize(
        "fixture_id", [fid for fid in fixture_ids() if fid not in COVERED_ELSEWHERE]
    )
    def test_fixture_behaves_as_declared(self, stack, scope, fixture_id):
        item, outcome, source_specs = _run_fixture(stack, scope, fixture_id)

        if item.expected_outcome == "READY":
            assert outcome.state is BuildState.PUBLISHED, (
                f"{fixture_id} declares READY but produced "
                f"{outcome.state} / {outcome.reason}: {outcome.message}"
            )
            assert outcome.status is ReportStatus.READY
            assert published_pointer(stack, scope) is not None
            assert_matches_oracle(stack, outcome, scope, source_specs)
            # Every expected Deal, exactly once.
            assert {deal.deal_id for deal in outcome.dedup.deals} == {
                spec.deal_id for spec in source_specs
            }
        else:
            assert outcome.state is not BuildState.PUBLISHED, (
                f"{fixture_id} declares {item.expected_outcome} but published"
            )
            assert published_pointer(stack, scope) is None
            if not item.expected_reason:
                return
            if item.expected_outcome == "QUARANTINED":
                # The declared reason is the *ingest* classification of the
                # poison record. The build then refuses for a downstream
                # reason - unresolved poison in the cut - so both halves are
                # checked rather than conflated.
                receipts = stack.control.list_quarantine(unresolved_only=False)
                assert item.expected_reason in {r.reason for r in receipts}, (
                    f"{fixture_id} declares a {item.expected_reason} receipt, "
                    f"got {[r.reason for r in receipts]}"
                )
                assert outcome.reason is Reason.QUARANTINE_PENDING_IN_CUT
            else:
                assert str(outcome.reason) == item.expected_reason, (
                    f"{fixture_id} declares reason {item.expected_reason} "
                    f"but reported {outcome.reason}"
                )

    def test_g08_02_duplicate_delivery_is_absorbed(self, stack, scope):
        """The fixture ships the same envelope twice, by design."""
        _item, outcome, source_specs = _run_fixture(stack, scope, "G08-02")
        assert outcome.state is BuildState.PUBLISHED
        assert stack.clickhouse.raw_row_count() > len(source_specs)
        assert len(outcome.dedup.deals) == len(source_specs)

    def test_g08_05_epoch_reset_builds_two_separate_reports(self, stack, scope):
        """Both epochs are valid; neither absorbs the other."""
        item = fixture("G08-05")
        produce(stack, item.envelopes)
        ingest_all(stack)

        first = stack.build_worker(stub_source(stack, item.source_deals)).run(scope)
        assert first.state is BuildState.PUBLISHED

        second_specs = item.extra["second_epoch_deals"]
        second = stack.build_worker(stub_source(stack, second_specs)).run(
            replace(scope, epoch_id=ALT_EPOCH)
        )
        assert second.state is BuildState.PUBLISHED
        assert first.canonical_digest != second.canonical_digest
        assert {d.deal_id for d in first.dedup.deals} == {
            d.deal_id for d in second.dedup.deals
        }

    def test_g08_11_identical_cut_built_twice_is_deterministic(self, stack, scope):
        """INV08-22, stated by the fixture and checked here."""
        item = fixture("G08-11")
        produce(stack, item.envelopes)
        ingest_all(stack)
        first = stack.build_worker(stub_source(stack, item.source_deals)).run(
            scope, publish=False
        )
        second = stack.build_worker(stub_source(stack, item.source_deals)).run(
            replace(scope, cut_id="cut-second"), publish=False
        )
        assert first.build_id != second.build_id
        assert first.canonical_digest == second.canonical_digest

    def test_every_fixture_is_either_run_here_or_declared_elsewhere(self):
        """No fixture may be silently unexercised."""
        covered_here = {fid for fid in fixture_ids() if fid not in COVERED_ELSEWHERE}
        assert covered_here | set(COVERED_ELSEWHERE) == set(fixture_ids())
        assert len(fixture_ids()) == 20
        for fixture_id, where in COVERED_ELSEWHERE.items():
            assert where, f"{fixture_id} is excluded without naming where it runs"


class TestInvariantRegister:
    def test_the_register_covers_all_twenty_four_invariants(self):
        assert len(INVARIANTS) == 24
        assert set(INVARIANTS_BY_ID) == {f"INV08-{n:02d}" for n in range(1, 25)}

    def test_every_entry_names_a_statement_and_a_proof(self):
        for invariant in INVARIANTS:
            assert invariant.statement.strip()
            assert invariant.proof.strip()

    def test_an_unproven_entry_explains_itself(self):
        """A gap must say why, not merely that it exists."""
        for invariant in unproven():
            assert invariant.proof.startswith("NOT PROVEN")
            # Long enough to carry a reason rather than a shrug.
            assert len(invariant.proof) > 60

    def test_only_the_replication_invariant_is_unproven(self):
        """A newly unproven invariant must be a deliberate, visible change."""
        assert {inv.invariant_id for inv in unproven()} == {"INV08-24"}
