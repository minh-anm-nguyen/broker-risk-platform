"""Tests for the what-if campaign itself (section 9, ticket T04-17).

A campaign runner can lie in two directions, and both are tested here:

* **Overstating coverage** — counting `NOT_RUN` or `INCONCLUSIVE` as passes,
  or reporting a pass rate that hides what was skipped.
* **Passing for the wrong reason** — a case that declares a fault, never
  fires it, and still reports `PASS`.

Section 9 and section 10.6 forbid both, so the honesty of the reporting is
itself under test. The structural tests are fast; actually executing cases is
marked `slow` because each case builds its own simulator.
"""

from __future__ import annotations

import json

import pytest

from reconciliation.campaign import CASES, DOMAINS, IMPLEMENTED, coverage_summary, run_campaign
from reconciliation.evidence import PASSING_RESULTS, CampaignReport, CaseResult, EvidenceManifest

pytestmark = [pytest.mark.campaign]


# ---------------------------------------------------------------------------
# Registry shape
# ---------------------------------------------------------------------------


def test_all_twenty_six_domains_are_registered() -> None:
    assert len(DOMAINS) == 26
    assert sorted(DOMAINS) == [chr(code) for code in range(ord("A"), ord("Z") + 1)]


def test_the_registry_matches_the_matrix_size() -> None:
    """Section 9 defines 312 cases: 12 per domain over 26 domains."""
    assert len(CASES) == 312
    per_domain = {letter: 0 for letter in DOMAINS}
    for spec in CASES:
        per_domain[spec.domain] += 1
    assert set(per_domain.values()) == {12}


def test_the_priority_split_matches_the_design() -> None:
    """208 P0, 78 P1, 26 P2 (8/3/1 per domain)."""
    counts = {"P0": 0, "P1": 0, "P2": 0}
    for spec in CASES:
        counts[spec.priority] += 1
    assert counts == {"P0": 208, "P1": 78, "P2": 26}


def test_case_ids_are_unique() -> None:
    ids = [spec.case_id for spec in CASES]
    assert len(set(ids)) == len(ids)


def test_every_domain_has_at_least_one_executable_case() -> None:
    """A domain with nothing implemented would be invisible in the results."""
    implemented_domains = {spec.domain for spec in CASES if spec.implemented}
    missing = sorted(set(DOMAINS) - implemented_domains)
    assert not missing, f"domains with no executable case: {missing}"


def test_every_unimplemented_case_states_why() -> None:
    """A skipped case must say what is missing, not just that it is missing."""
    for spec in CASES:
        if spec.implemented:
            continue
        assert spec.not_run_reason, f"{spec.case_id} is NOT_RUN with no stated reason"
        assert len(spec.not_run_reason) > 20, f"{spec.case_id} reason is not informative"


def test_coverage_is_reported_separately_from_pass_rate() -> None:
    """The gap between declared and implemented must be visible as data."""
    summary = coverage_summary()
    totals = summary["totals"]

    assert totals["declared"] == 312
    assert totals["p0_declared"] == 208
    assert totals["implemented"] == len(IMPLEMENTED)
    # Honest about the gap: this build does not implement all 208 P0 cases.
    assert totals["p0_implemented"] < totals["p0_declared"]
    assert totals["implemented"] >= 50, "coverage regressed below the documented level"

    for letter, bucket in summary["per_domain"].items():
        assert bucket["declared"] == 12
        assert bucket["implemented"] >= 1, letter


# ---------------------------------------------------------------------------
# Honest arithmetic
# ---------------------------------------------------------------------------


def test_only_pass_counts_as_passing() -> None:
    """Section 9: INCONCLUSIVE is never counted as a pass."""
    assert PASSING_RESULTS == frozenset({CaseResult.PASS})
    assert CaseResult.INCONCLUSIVE not in PASSING_RESULTS
    assert CaseResult.NOT_RUN not in PASSING_RESULTS
    assert CaseResult.INVALID_TEST not in PASSING_RESULTS


def test_a_declared_fault_that_did_not_fire_is_downgraded() -> None:
    """Section 10.6: a case that cannot prove its fault fired is INVALID_TEST."""
    manifest = EvidenceManifest(
        test_id="WF4-TEST-01",
        result=CaseResult.PASS,
        fault_injected="http_error 503",
        fault_activation_confirmed=False,
    )
    assert manifest.validate() is CaseResult.INVALID_TEST
    assert any("activation" in note for note in manifest.notes)


def test_a_confirmed_fault_keeps_its_pass() -> None:
    manifest = EvidenceManifest(
        test_id="WF4-TEST-02",
        result=CaseResult.PASS,
        fault_injected="http_error 503",
        fault_activation_confirmed=True,
    )
    assert manifest.validate() is CaseResult.PASS


def test_a_case_without_a_fault_is_not_downgraded() -> None:
    """Not every case needs a fault; only declared ones must be confirmed."""
    manifest = EvidenceManifest(test_id="WF4-TEST-03", result=CaseResult.PASS)
    assert manifest.validate() is CaseResult.PASS


def test_inconclusive_does_not_make_a_campaign_green_by_omission() -> None:
    report = CampaignReport(campaign_id="t")
    report.add(EvidenceManifest(test_id="a", result=CaseResult.PASS))
    report.add(EvidenceManifest(test_id="b", result=CaseResult.INCONCLUSIVE))

    assert report.passed == 1
    assert report.failed == 0
    # Green means nothing failed and nothing was invalid; the INCONCLUSIVE is
    # still visible in the tally rather than folded into the pass count.
    assert report.tally()[CaseResult.INCONCLUSIVE.value] == 1
    assert report.passed != report.executed


def test_a_failure_makes_the_campaign_not_green() -> None:
    report = CampaignReport(campaign_id="t")
    report.add(EvidenceManifest(test_id="a", result=CaseResult.PASS))
    report.add(EvidenceManifest(test_id="b", result=CaseResult.FAIL))
    assert report.green is False


def test_an_invalid_test_makes_the_campaign_not_green() -> None:
    """A case that did not test its own premise is not a neutral result."""
    report = CampaignReport(campaign_id="t")
    report.add(
        EvidenceManifest(
            test_id="a",
            result=CaseResult.PASS,
            fault_injected="something",
            fault_activation_confirmed=False,
        )
    )
    assert report.green is False


def test_not_run_reduces_coverage_without_failing_the_campaign() -> None:
    """Skipped cases lower coverage; they are not silent passes or failures."""
    report = CampaignReport(campaign_id="t")
    report.add(EvidenceManifest(test_id="a", result=CaseResult.PASS))
    report.add(EvidenceManifest(test_id="b", result=CaseResult.NOT_RUN))

    assert report.green is True
    assert report.executed == 1
    assert report.tally()[CaseResult.NOT_RUN.value] == 1


def test_manifest_records_the_engine_it_actually_ran_on(tmp_path) -> None:
    """A reader must not mistake these results for PostgreSQL measurements."""
    manifest = EvidenceManifest(test_id="WF4-TEST-04", result=CaseResult.PASS)
    body = manifest.to_dict()
    assert body["db_engine"] == "sqlite-wal"
    assert "evidence_hash" in body and len(body["evidence_hash"]) == 64

    path = manifest.write(tmp_path)
    written = json.loads(path.read_text(encoding="utf-8"))
    assert written["test_id"] == "WF4-TEST-04"


# ---------------------------------------------------------------------------
# Executing cases for real
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.parametrize(
    "case_id",
    [
        "WF4-E-01",  # missing deal detected
        "WF4-E-02",  # money conflict quarantined
        "WF4-E-04",  # W<S is not a missing deal
        "WF4-F-02",  # moved cut is not corruption
        "WF4-L-01",  # dispatch without approval refused
        "WF4-V-01",  # P04 cannot write P03 tables
    ],
)
def test_representative_cases_pass_when_executed(case_id: str, tmp_path) -> None:
    """Spot-check that the registry's cases actually run and pass.

    A registry of callables that is never invoked would report structure
    without substance, so a representative slice is executed here. The full
    set runs via ``reconctl campaign``.
    """
    report = run_campaign(tier="P0", seed=42, case_ids=[case_id])

    assert len(report.manifests) == 1
    manifest = report.manifests[0]
    assert manifest.result is CaseResult.PASS, (
        f"{case_id}: expected={manifest.expected_outcome!r} "
        f"actual={manifest.actual_outcome!r} notes={manifest.notes}"
    )
    assert manifest.expected_outcome
    assert manifest.actual_outcome


@pytest.mark.slow
def test_a_fault_injecting_case_confirms_its_own_activation(tmp_path) -> None:
    """The lost-ACK case must prove the fault fired, or be INVALID_TEST."""
    report = run_campaign(tier="P0", seed=42, case_ids=["WF4-N-01"])
    manifest = report.manifests[0]

    assert manifest.fault_injected == "lose_repair_ack"
    assert manifest.fault_activation_confirmed is True
    assert manifest.result is CaseResult.PASS, manifest.actual_outcome


@pytest.mark.slow
def test_the_oracle_case_detects_a_lying_source(tmp_path) -> None:
    """IT04-10 as a campaign case, including the oracle violation note."""
    report = run_campaign(tier="P0", seed=42, case_ids=["WF4-X-01"])
    manifest = report.manifests[0]

    assert manifest.result is CaseResult.PASS, manifest.actual_outcome
    assert manifest.fault_activation_confirmed is True
    assert any("ORACLE_VIOLATION" in note for note in manifest.notes)


@pytest.mark.slow
def test_unimplemented_cases_report_not_run_with_their_reason() -> None:
    not_run_id = next(spec.case_id for spec in CASES if not spec.implemented)
    report = run_campaign(tier="P0", seed=42, case_ids=[not_run_id])

    manifest = report.manifests[0]
    assert manifest.result is CaseResult.NOT_RUN
    assert manifest.notes and len(manifest.notes[0]) > 20
    assert report.green is True  # a skip is not a failure
    assert report.executed == 0  # but it is not coverage either
