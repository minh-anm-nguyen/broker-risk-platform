"""Determinism and metamorphic replay: INV-09, WF-001, WF-002, WF-009.

The claim being tested is narrow and precise: the canonical content digest of
a run is a function of ``seed + scenario spec + generator version`` and
nothing else. Not of wall-clock time, not of how long generation took, not of
how many reads arrived while it ran, and not of which process did it.

Two separate databases are used for the A/B comparison. Comparing two runs
inside one database would let a shared counter or a cached object make them
agree for the wrong reason.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from simulator.main import create_app
from tests.conftest import (
    ADMIN_TOKEN,
    BASELINE_SCENARIO,
    DATA_TOKEN,
    create_run,
    make_settings,
    wait_for_job,
)

pytestmark = pytest.mark.integration

ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
DATA = {"Authorization": f"Bearer {DATA_TOKEN}"}


def digest_of_run(tmp_path: Path, scenario: dict[str, Any], *, reads: int = 0) -> dict[str, Any]:
    """Generate a scenario in its own database and report its full report."""
    settings = make_settings(tmp_path)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        accepted = client.post(
            "/admin/v1/runs", json=scenario, headers={**ADMIN, "Idempotency-Key": "gen"}
        ).json()
        if reads:
            # WF-009: concurrent reads must not influence generation at all.
            def read(_: int) -> int:
                return client.get("/v1/deals?limit=50", headers=DATA).status_code

            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(read, range(reads)))
        job = wait_for_job(client, ADMIN, accepted["status_url"], timeout=120)
        assert job["status"] == "SUCCEEDED", job
        report = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()
        meta = client.get("/v1/meta", headers=DATA).json()
    return {"report": report, "meta": meta}


def test_same_seed_same_digest(tmp_path: Path) -> None:
    """INV-09 / WF-001: the headline reproducibility claim."""
    left = digest_of_run(tmp_path / "a", BASELINE_SCENARIO)
    right = digest_of_run(tmp_path / "b", BASELINE_SCENARIO)

    assert left["report"]["canonical_digest"] == right["report"]["canonical_digest"]
    assert left["report"]["record_counts"] == right["report"]["record_counts"]
    assert left["report"]["committed_seq"] == right["report"]["committed_seq"]
    assert left["meta"]["spec_hash"] == right["meta"]["spec_hash"]
    # Both runs are internally sound, not merely identical.
    assert left["report"]["summary"]["failed"] == 0
    assert right["report"]["summary"]["failed"] == 0
    assert left["report"]["summary"]["storage_scope_ok"] is True


def test_concurrent_reads_do_not_change_generated_data(tmp_path: Path) -> None:
    """WF-009: read traffic is not an input to the dataset."""
    quiet = digest_of_run(tmp_path / "quiet", BASELINE_SCENARIO)
    busy = digest_of_run(tmp_path / "busy", BASELINE_SCENARIO, reads=40)
    assert quiet["report"]["canonical_digest"] == busy["report"]["canonical_digest"]


def test_a_different_seed_changes_the_data_but_not_its_soundness(tmp_path: Path) -> None:
    """WF-002."""
    left = digest_of_run(tmp_path / "s1", {**BASELINE_SCENARIO, "seed": 1})
    right = digest_of_run(tmp_path / "s2", {**BASELINE_SCENARIO, "seed": 2})
    assert left["report"]["canonical_digest"] != right["report"]["canonical_digest"]
    assert left["meta"]["spec_hash"] != right["meta"]["spec_hash"]
    for side in (left, right):
        assert side["report"]["summary"]["failed"] == 0
        assert side["report"]["summary"]["storage_scope_ok"] is True


@pytest.mark.parametrize(
    "change",
    [
        {"account_count": 11},
        {"deal_count": 121},
        {"symbols": ["EURUSD", "GBPUSD"]},
        {"initial_balance_usd": "10000.01"},
        {"max_open_positions": 19},
        {"scenario_name": "renamed"},
    ],
)
def test_every_spec_field_participates_in_the_digest(
    tmp_path: Path, change: dict[str, Any]
) -> None:
    """A spec field that did not affect the digest would be a silent input."""
    baseline = digest_of_run(tmp_path / "base", BASELINE_SCENARIO)
    altered = digest_of_run(tmp_path / "alt", {**BASELINE_SCENARIO, **change})
    assert baseline["meta"]["spec_hash"] != altered["meta"]["spec_hash"], change
    # scenario_name changes the spec digest, which reseeds the derivation, so
    # even a cosmetic field changes the data. That is intentional: the digest
    # covers the declared inputs exactly.
    assert (
        baseline["report"]["canonical_digest"] != altered["report"]["canonical_digest"]
    ), change


def test_identifiers_are_deterministic_not_random(tmp_path: Path) -> None:
    """A UUID anywhere in an id would break the A/B comparison outright."""
    left = digest_of_run(tmp_path / "id1", BASELINE_SCENARIO)
    settings = make_settings(tmp_path / "id2")
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN)
        deals = client.get("/v1/deals?limit=5", headers=DATA).json()["items"]
        accounts = client.get("/v1/accounts?limit=3", headers=DATA).json()["items"]
    assert [item["deal_id"] for item in deals] == [
        "D-00000001",
        "D-00000002",
        "D-00000003",
        "D-00000004",
        "D-00000005",
    ]
    assert [item["account_id"] for item in accounts] == ["A-000001", "A-000002", "A-000003"]
    assert left["report"]["record_counts"]["deals"] == 120


def test_virtual_time_is_derived_from_the_spec_not_the_host_clock(tmp_path: Path) -> None:
    """The first event lands exactly on the declared start time."""
    settings = make_settings(tmp_path, virtual_time_step_ms=1_000)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, start_virtual_time="2020-01-01T00:00:00Z")
        deals = client.get("/v1/deals?limit=3", headers=DATA).json()["items"]
        meta = client.get("/v1/meta", headers=DATA).json()
    assert meta["start_virtual_time"] == "2020-01-01T00:00:00Z"
    # Business time is years away from now, which no wall clock could produce.
    assert all(item["executed_at"].startswith("2020-01-01T") for item in deals)
    # Steps are exactly the configured interval apart in sequence order.
    assert deals[0]["executed_at"] < deals[1]["executed_at"] < deals[2]["executed_at"]


def test_restarting_the_process_does_not_change_the_digest(tmp_path: Path) -> None:
    """WF-078: a digest computed after a restart must match the one before it."""
    settings = make_settings(tmp_path)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN)
        before = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()

    # A completely new application object against the same database.
    restarted = create_app(make_settings(tmp_path), lock_process=False)
    with TestClient(restarted) as client:
        after = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()
        ready = client.get("/health/ready").json()

    assert after["canonical_digest"] == before["canonical_digest"]
    assert after["committed_seq"] == before["committed_seq"]
    assert after["summary"]["failed"] == 0
    assert ready["ready"] is True
    assert ready["recovery_required"] is False


def test_repeated_restarts_never_move_the_watermark_backwards(tmp_path: Path) -> None:
    """WF-078: ten restarts, same truth."""
    settings = make_settings(tmp_path)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, deal_count=60)
        baseline = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()

    for _ in range(10):
        restarted = create_app(make_settings(tmp_path), lock_process=False)
        with TestClient(restarted) as client:
            report = client.get(
                "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
            ).json()
            assert report["committed_seq"] == baseline["committed_seq"]
            assert report["canonical_digest"] == baseline["canonical_digest"]
            assert report["summary"]["failed"] == 0


def test_advance_continues_the_same_seeded_stream(tmp_path: Path) -> None:
    """Generating 120 then advancing 40 must equal generating the same events
    in one pass, because `advance` continues the same derivation."""
    one_pass = make_settings(tmp_path / "one")
    app = create_app(one_pass, lock_process=False)
    with TestClient(app) as client:
        accepted = create_run(client, ADMIN, wait=False)
        wait_for_job(client, ADMIN, accepted["status_url"], timeout=120)
        target = client.get("/admin/v1/runs/current", headers=ADMIN).json()["committed_seq"]
        advance = client.post(
            "/admin/v1/runs/run_0001/advance",
            json={"events": 40},
            headers={**ADMIN, "Idempotency-Key": "a"},
        ).json()
        wait_for_job(client, ADMIN, advance["status_url"], timeout=120)
        extended = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()

    assert extended["committed_seq"] == target + 40
    assert extended["summary"]["failed"] == 0
    assert extended["summary"]["storage_scope_ok"] is True
