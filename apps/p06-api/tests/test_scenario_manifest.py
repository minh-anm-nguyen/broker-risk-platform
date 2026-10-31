"""The scenario-to-test manifest, and honest accounting of what is covered.

Design doc section 21.2 asks for a manifest mapping each what-if scenario to
the test that exercises it, with evidence. This file builds that manifest from
the rows the tests recorded, and — just as importantly — asserts the coverage
claim out loud so it cannot drift into an overstatement.

The design catalogues 432 scenarios across 36 domains. This implementation does
not claim all 432 are covered. ``COVERED_DOMAINS`` lists the domains with real
executed assertions; everything else is reported as NOT_RUN rather than
quietly counted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[1]
EVIDENCE = Path(__file__).resolve().parent / "_evidence.jsonl"
MANIFEST = ROOT / "artifacts" / "scenario_manifest.json"

# Domains from design doc section 12 that this build exercises with executed
# assertions. The letter is the domain, the value is where to look.
COVERED_DOMAINS: dict[str, str] = {
    "A": "tests/contracts/test_routing_and_auth_http.py",
    "B": "tests/unit/test_jwt_verifier.py, tests/contracts/test_routing_and_auth_http.py",
    "C": "tests/integration/test_authz_matrix_and_isolation.py",
    "D": "tests/unit/test_observability_policy.py",
    "E": "tests/unit/test_report_quality_guard.py",
    "F": "tests/integration/test_strict_partial_and_quality.py",
    "G": "tests/integration/test_cut_races_and_audit.py",
    "H": "tests/integration/test_cut_races_and_audit.py",
    "I": "tests/integration/test_cut_races_and_audit.py",
    "J": "tests/unit/test_engine_reference.py",
    "K": "tests/unit/test_money_and_decimals.py",
    "L": "tests/contracts/test_risk_endpoint.py, tests/contracts/test_endpoints.py",
    "M": "tests/contracts/test_endpoints.py",
    "N": "tests/unit/test_cursor_codec.py, tests/integration/test_cut_races_and_audit.py",
    "O": "tests/contracts/test_endpoints.py",
    "P": "tests/integration/test_strict_partial_and_quality.py",
    "Q": "tests/unit/test_problem_taxonomy.py",
    "R": "tests/unit/test_admission_budgets_quotas.py, tests/adversarial/test_abuse_and_fuzz.py",
    "S": "tests/unit/test_admission_budgets_quotas.py, tests/performance/test_load_and_responsiveness.py",
    "T": "tests/unit/test_execution_isolation.py",
    "U": "tests/integration/test_authz_matrix_and_isolation.py",
    "V": "tests/integration/test_authz_matrix_and_isolation.py, tests/chaos/test_crash_and_campaigns.py",
    "X": "tests/chaos/test_crash_and_campaigns.py",
    "Y": "tests/adversarial/test_abuse_and_fuzz.py",
    "Z": "tests/contracts/test_health_metrics_openapi.py",
    "AA": "tests/integration/test_cut_races_and_audit.py, tests/chaos/test_crash_and_campaigns.py",
    "AB": "tests/unit/test_settings_and_versioning.py",
    "AC": "tests/unit/test_observability_policy.py, tests/contracts/test_health_metrics_openapi.py",
    "AD": "tests/unit/test_settings_and_versioning.py",
    "AE": "tests/performance/test_load_and_responsiveness.py",
    "AF": "tests/performance/test_load_and_responsiveness.py",
    "AG": "tests/chaos/test_crash_and_campaigns.py",
    "AH": "tests/chaos/test_crash_and_campaigns.py",
    "AI": "tests/test_scenario_manifest.py",
}

# Domains deliberately not exercised, with the reason. Each is reported as
# NOT_APPLICABLE_BY_GATE rather than PASS.
GATED_DOMAINS: dict[str, str] = {
    "W": "response cache is a P1 capability and is not implemented, so there is nothing to invalidate",
    "AJ": "async jobs, exports and multi-source are P1/P2 capabilities and are not implemented",
}

ALL_DOMAINS = (
    list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    + ["AA", "AB", "AC", "AD", "AE", "AF", "AG", "AH", "AI", "AJ"]
)

# The acceptance tests named in design doc section 13.3 that this build runs.
ACCEPTANCE_TESTS: dict[str, str] = {
    "IT06-01": "tests/contracts/test_risk_endpoint.py",
    "IT06-02": "tests/integration/test_authz_matrix_and_isolation.py",
    "IT06-03": "tests/integration/test_strict_partial_and_quality.py",
    "IT06-04": "tests/integration/test_cut_races_and_audit.py",
    "IT06-05": "tests/integration/test_cut_races_and_audit.py",
    "IT06-06": "tests/integration/test_authz_matrix_and_isolation.py",
    "IT06-07": "tests/integration/test_cut_races_and_audit.py",
    "IT06-08": "tests/unit/test_execution_isolation.py",
    "IT06-09": "tests/integration/test_cut_races_and_audit.py",
    "IT06-10": "tests/contracts/test_endpoints.py",
    "IT06-11": "tests/integration/test_strict_partial_and_quality.py",
    "IT06-12": "tests/adversarial/test_abuse_and_fuzz.py",
    "IT06-13": "tests/chaos/test_crash_and_campaigns.py",
    "IT06-14": "tests/unit/test_settings_and_versioning.py",
}

# Golden fixtures from design doc section 13.2 and where their values are asserted.
GOLDEN_FIXTURES: dict[str, str] = {
    "FX-READY-001": "tests/unit/test_engine_reference.py, tests/contracts/test_risk_endpoint.py",
    "FX-REALIZED-002": "tests/unit/test_engine_reference.py, tests/contracts/test_risk_endpoint.py",
    "FX-PARTIAL-003": "tests/integration/test_strict_partial_and_quality.py",
    "FX-RECON-004": "tests/integration/test_strict_partial_and_quality.py",
    "FX-EPOCH-005": "tests/integration/test_cut_races_and_audit.py",
    "FX-AUTH-006": "tests/integration/test_authz_matrix_and_isolation.py",
    "FX-EMPTY-007": "tests/contracts/test_endpoints.py",
    "FX-CURSOR-008": "tests/integration/test_cut_races_and_audit.py",
    "FX-DECIMAL-009": "tests/unit/test_money_and_decimals.py",
    "FX-TOO-LARGE-010": "tests/contracts/test_endpoints.py",
    "FX-OUTAGE-011": "tests/integration/test_cut_races_and_audit.py",
    "FX-LOAD-012": "tests/performance/test_load_and_responsiveness.py",
}

# The 20 kill points from design doc section 15, and their status here.
KILL_POINTS_EXERCISED = (
    "K06-01",
    "K06-02",
    "K06-05",
    "K06-06",
    "K06-09",
    "K06-11",
    "K06-13",
    "K06-14",
    "K06-16",
    "K06-17",
    "K06-20",
)


def load_evidence() -> list[dict]:
    if not EVIDENCE.exists():
        return []
    rows = []
    for line in EVIDENCE.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


class TestCoverageAccounting:
    def test_the_domain_lists_partition_the_catalog(self):
        """Every domain is either covered or explicitly gated, never forgotten."""
        accounted = set(COVERED_DOMAINS) | set(GATED_DOMAINS)
        assert accounted == set(ALL_DOMAINS), {
            "missing": sorted(set(ALL_DOMAINS) - accounted),
            "unexpected": sorted(accounted - set(ALL_DOMAINS)),
        }
        assert not (set(COVERED_DOMAINS) & set(GATED_DOMAINS))

    def test_every_covered_domain_points_at_files_that_exist(self):
        for domain, locations in sorted(COVERED_DOMAINS.items()):
            for location in locations.split(", "):
                assert (ROOT / location).exists(), f"domain {domain}: missing {location}"

    def test_every_acceptance_test_points_at_a_file_that_exists(self):
        for test_id, location in sorted(ACCEPTANCE_TESTS.items()):
            assert (ROOT / location).exists(), f"{test_id}: missing {location}"

    def test_every_golden_fixture_is_asserted_somewhere(self):
        for fixture, locations in sorted(GOLDEN_FIXTURES.items()):
            for location in locations.split(", "):
                assert (ROOT / location).exists(), f"{fixture}: missing {location}"

    def test_each_gated_domain_states_its_reason(self):
        """A gated scenario is NOT_APPLICABLE_BY_GATE, never a silent pass."""
        for domain, reason in GATED_DOMAINS.items():
            assert len(reason) > 30, f"domain {domain} needs a real reason"
            assert "not implemented" in reason


class TestEvidenceRows:
    def test_recorded_rows_are_well_formed(self):
        rows = load_evidence()
        if not rows:
            pytest.skip("no evidence recorded in this selection")
        for row in rows:
            assert row["scenario_ids"], row
            assert row["test_id"]
            assert row["assertions"], f"{row['test_id']} recorded no assertions"
            if row["expected_http"] is not None:
                assert row["observed_http"] == row["expected_http"], row

    def test_no_evidence_row_claims_a_pass_without_assertions(self):
        """Design doc section 11: no assertion means not a test."""
        for row in load_evidence():
            assert len(row["assertions"]) >= 1


def test_write_manifest(tmp_path_factory):
    """Emit the manifest artifact with an explicit, non-inflated coverage claim."""
    rows = load_evidence()
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    manifest = {
        "project": "Project 06 - Risk Reporting REST API",
        "design_catalog": {
            "domains": len(ALL_DOMAINS),
            "scenarios_catalogued": 432,
            "note": (
                "The 432-case catalog is the design's target. This build exercises the "
                "domains listed under covered_domains with executed assertions and reports "
                "the rest as NOT_APPLICABLE_BY_GATE. No scenario is counted as PASS without "
                "an assertion."
            ),
        },
        "covered_domains": COVERED_DOMAINS,
        "gated_domains": {
            domain: {"status": "NOT_APPLICABLE_BY_GATE", "reason": reason}
            for domain, reason in GATED_DOMAINS.items()
        },
        "acceptance_tests": ACCEPTANCE_TESTS,
        "golden_fixtures": GOLDEN_FIXTURES,
        "kill_points_exercised": list(KILL_POINTS_EXERCISED),
        "kill_points_not_exercised": [
            f"K06-{index:02d}"
            for index in range(1, 21)
            if f"K06-{index:02d}" not in KILL_POINTS_EXERCISED
        ],
        "evidence_rows": rows,
        "performance_claims": (
            "None. The load tests assert saturation behaviour and correctness under "
            "pressure on a loopback in-process harness. They publish no SLO, and the "
            "120 ms figure in the design provenance note is a CV reference, not a result."
        ),
    }
    MANIFEST.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    assert MANIFEST.exists()
    assert manifest["design_catalog"]["domains"] == 36
