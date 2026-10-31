"""Frozen golden fixtures (SYSTEM_DESIGN section 10.2).

The A/B determinism test proves a seed reproduces itself *within one build*.
This file proves something different and stronger: the dataset a seed produces
has not changed since it was reviewed. Without a frozen digest, a refactor that
silently alters generated data passes every other test in the suite - because
both sides of every comparison moved together.

Section 10.2 is explicit that a golden hash is only written once an
implementation has passed and is then frozen, and that no placeholder value may
be invented. The digests below were measured after the first full green suite
and reviewed as part of that change. Each was independently confirmed to
reproduce across separate processes and separate databases.

**If one of these fails, it is not a flaky test.** Either a generation
algorithm changed - in which case ``GENERATOR_VERSION`` must be bumped, because
a seed only reproduces a dataset within one generator version - or the
canonical encoding changed, in which case ``CANONICAL_VERSION`` must be bumped.
Re-freezing a digest without deciding which of those happened defeats the
purpose of having one.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator import GENERATOR_VERSION
from simulator.canonical import CANONICAL_VERSION
from simulator.main import create_app
from tests.conftest import ADMIN_TOKEN, DATA_TOKEN, make_settings, wait_for_job

pytestmark = pytest.mark.integration

ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
DATA = {"Authorization": f"Bearer {DATA_TOKEN}"}

SCENARIOS = Path(__file__).resolve().parents[2] / "scenarios"

#: Frozen for generator version 1.0 and canonical version 2.
#:
#: ``events`` is also the run's ``committed_seq``: the generator is the only
#: writer for these fixtures, so every committed event is one it produced.
GOLDEN: dict[str, dict[str, Any]] = {
    "empty.json": {
        "digest": "sha256:01526c3592111ede1c3b48b9193a1d00026b1e8abf035e53cfce49cc89b1831c",
        "counts": {"events": 1, "deals": 0, "accounts": 0, "positions": 0, "marks": 1},
    },
    "single_account_open_close.json": {
        "digest": "sha256:8c1746aca21114365bd9010a291ef8bd7eee40b3a4ad9b404360e5f5a1bd9f70",
        "counts": {"events": 8, "deals": 6, "accounts": 1, "positions": 3, "marks": 1},
    },
    "sell_profit_loss.json": {
        "digest": "sha256:c91c397858aea6a3d43d526677ff80967a01350f6fb76c302976f2ce7f7f020f",
        "counts": {"events": 33, "deals": 20, "accounts": 2, "positions": 7, "marks": 1},
    },
    "partial_close.json": {
        "digest": "sha256:55c65932e71f98c447074bb2bd2a46ac510c30717be5cfb4cb2216eb42639750",
        "counts": {"events": 84, "deals": 60, "accounts": 3, "positions": 24, "marks": 2},
    },
    "bad_payload_fault.json": {
        "digest": "sha256:e001874b2be8d2da5ecf3885e9ae51487e1f02092e621e52e377aa58d982ed2e",
        "counts": {"events": 141, "deals": 100, "accounts": 5, "positions": 34, "marks": 1},
    },
}

#: Frozen too, but slow enough to keep out of the per-commit suite.
GOLDEN_SLOW: dict[str, dict[str, Any]] = {
    "paging_10k.json": {
        "digest": "sha256:dd24518de45885e110e35d616d8da4d192febfc1327a8affd3b012bd5f7ebaff",
        "counts": {"events": 13497, "deals": 10000, "accounts": 200, "positions": 3500, "marks": 3},
    },
}

#: The remaining section 10.2 fixtures are *behavioural* rather than
#: content-hash fixtures: what they assert is a reaction, not a dataset. They
#: are listed here so the section-10.2 inventory is auditable from one place.
BEHAVIOURAL_FIXTURES = {
    "bulk-100k-seed42": "tests/performance/test_baseline_benchmark.py::test_dataset_bootstrap_is_measured_and_correct",
    "disconnect-after-page3": "tests/integration/test_fault_injection.py::test_an_nth_request_fault_hits_exactly_that_request",
    "reset-while-reading": "tests/integration/test_lifecycle_reset.py::test_old_cursors_are_rejected_after_a_reset",
    "duplicate-deal-response": "tests/integration/test_fault_injection.py::test_duplicating_an_item_produces_a_repeated_deal_id",
}


def generate_and_report(tmp_path: Path, scenario_file: str) -> dict[str, Any]:
    spec = json.loads((SCENARIOS / scenario_file).read_text(encoding="utf-8"))
    body = {key: value for key, value in spec.items() if not key.startswith("_")}

    app = create_app(make_settings(tmp_path, writer_batch_size=200), lock_process=False)
    with TestClient(app) as client:
        accepted = client.post(
            "/admin/v1/runs", json=body, headers={**ADMIN, "Idempotency-Key": "golden"}
        )
        assert accepted.status_code == 202, accepted.text
        job = wait_for_job(client, ADMIN, accepted.json()["status_url"], timeout=300)
        assert job["status"] == "SUCCEEDED", job
        return client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()


def assert_matches_golden(report: dict[str, Any], expected: dict[str, Any], name: str) -> None:
    assert report["summary"]["failed"] == 0, report["summary"]
    assert report["summary"]["storage_scope_ok"] is True
    assert report["record_counts"] == expected["counts"], (
        f"{name}: record counts changed. If a generation algorithm changed, bump "
        f"GENERATOR_VERSION (currently {GENERATOR_VERSION}) and re-freeze deliberately."
    )
    assert report["canonical_digest"] == expected["digest"], (
        f"{name}: canonical digest changed from {expected['digest']} to "
        f"{report['canonical_digest']}. This is not flakiness. Decide which "
        f"happened: a generation algorithm changed (bump GENERATOR_VERSION, "
        f"currently {GENERATOR_VERSION}) or the canonical encoding changed "
        f"(bump CANONICAL_VERSION, currently {CANONICAL_VERSION})."
    )
    # events and committed_seq must agree: the generator is the only writer here.
    assert report["committed_seq"] == expected["counts"]["events"]


@pytest.mark.parametrize("scenario_file", sorted(GOLDEN))
def test_scenario_reproduces_its_frozen_digest(tmp_path: Path, scenario_file: str) -> None:
    report = generate_and_report(tmp_path, scenario_file)
    assert_matches_golden(report, GOLDEN[scenario_file], scenario_file)


@pytest.mark.slow
@pytest.mark.parametrize("scenario_file", sorted(GOLDEN_SLOW))
def test_large_scenario_reproduces_its_frozen_digest(
    tmp_path: Path, scenario_file: str
) -> None:
    report = generate_and_report(tmp_path, scenario_file)
    assert_matches_golden(report, GOLDEN_SLOW[scenario_file], scenario_file)


def test_every_shipped_scenario_is_either_frozen_or_accounted_for() -> None:
    """No scenario may be added without deciding how it is verified.

    A fixture file that nothing checks is a fixture that drifts.
    """
    shipped = {
        path.name
        for path in SCENARIOS.glob("*.json")
        if not path.name.endswith(".rules.json")
    }
    frozen = set(GOLDEN) | set(GOLDEN_SLOW)
    # baseline_100k is covered by the benchmark rather than frozen here: a
    # three-minute generation does not belong in the integration suite, and its
    # correctness is asserted there by the invariant sweep.
    accounted = frozen | {"baseline_100k.json"}
    assert shipped <= accounted, f"unverified scenarios: {sorted(shipped - accounted)}"


def test_the_behavioural_fixture_inventory_points_at_real_tests() -> None:
    """Section 10.2's remaining fixtures must name a test that exists."""
    root = SCENARIOS.parent
    for fixture, reference in BEHAVIOURAL_FIXTURES.items():
        path, _, test_name = reference.partition("::")
        source = root / path
        assert source.exists(), f"{fixture}: {path} does not exist"
        assert f"def {test_name}(" in source.read_text(encoding="utf-8"), (
            f"{fixture}: {test_name} not found in {path}"
        )


def test_the_frozen_digests_declare_the_versions_they_belong_to() -> None:
    """A digest is only comparable within one generator and canonical version."""
    assert GENERATOR_VERSION == "1.0"
    assert CANONICAL_VERSION == "2"
    for name, entry in (GOLDEN | GOLDEN_SLOW).items():
        assert entry["digest"].startswith("sha256:"), name
        assert len(entry["digest"]) == len("sha256:") + 64, name
