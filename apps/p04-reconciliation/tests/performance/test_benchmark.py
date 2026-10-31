"""Measured throughput and resource bounds (section 11.5).

Section 11 is explicit that its numbers are *proposals, not measurements*, and
section 1.2 forbids quoting an unmeasured KPI as evidence. So this module
measures what it can in this environment and records the result as data,
rather than asserting a target it did not establish.

What is asserted is only what must hold regardless of hardware:

* the audit's peak memory does not scale with the dataset -- manifests stream
  rather than materialising (section 5.2 rule 6);
* a full audit of a non-trivial dataset completes inside its configured
  deadline;
* the per-run page and item budgets are respected.

What is *recorded* rather than asserted: wall-clock duration, rows per second
and peak RSS, written to an artifact so a change in them is visible across
runs. These are SQLite-on-this-machine numbers and say nothing about the
PostgreSQL reference deployment in section 11.1.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path

import pytest

from reconciliation.canonical import payload_hash, root_hash
from reconciliation.db.engine import Role
from reconciliation.scheduler import ReconciliationScheduler

pytestmark = [pytest.mark.performance, pytest.mark.slow]

#: Kept modest so the suite stays runnable. Section 11 calls for 100k and 1M
#: deal runs; those need the dedicated rig described in 11.5, not a unit-test
#: process, and the campaign report says so rather than implying coverage.
DEAL_COUNT = 1_200
ACCOUNT_COUNT = 60


#: Benchmark evidence goes to the repository's artifacts directory, not to the
#: per-test tmp_path. Section 10.7 requires evidence to live outside what the
#: service under test can overwrite -- a throwaway tmp directory satisfies
#: that but is rotated away within a few runs, so nobody can compare this
#: run against the last one. The directory is gitignored.
ARTIFACTS = Path(__file__).resolve().parents[2] / "artifacts"


def _write_report(name: str, report: dict) -> Path:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    path = ARTIFACTS / name
    path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return path


def _peak_rss_bytes() -> int | None:
    try:
        import psutil
    except ImportError:  # pragma: no cover
        return None
    return int(psutil.Process(os.getpid()).memory_info().rss)


@pytest.fixture
def large_world(p01_admin, harness):
    run = p01_admin.create_run(
        accounts=ACCOUNT_COUNT, deals=DEAL_COUNT, max_open_positions=ACCOUNT_COUNT
    )
    meta = p01_admin.meta()
    epoch_id = harness.bootstrap_epoch()
    watermark = harness.sync_deals(epoch_id)
    harness.publish_snapshot(epoch_id, "accounts")
    harness.publish_snapshot(epoch_id, "positions")
    return {
        "run_id": run["run_id"],
        "epoch_id": epoch_id,
        "snapshot_seq": int(meta["snapshot_seq"]),
        "watermark": watermark,
        "deal_count": int(meta["deal_count"]),
    }


def test_full_deal_audit_completes_within_its_deadline_and_records_metrics(
    scheduler: ReconciliationScheduler, large_world, repository, settings, tmp_path: Path
) -> None:
    """One full audit of ~1.2k deals, measured rather than assumed."""
    before_rss = _peak_rss_bytes()
    started = time.perf_counter()

    outcome = scheduler.run_audit(entity="deals", epoch_id=large_world["epoch_id"])

    elapsed = time.perf_counter() - started
    after_rss = _peak_rss_bytes()

    assert outcome.status == "MATCHED", (outcome.reason_code, outcome.detail)
    assert outcome.source_count == large_world["deal_count"]
    assert outcome.source_count == outcome.target_count

    # The only hard assertion: it finished inside the configured budget.
    assert elapsed < settings.run_deadline_s

    rows_per_second = outcome.source_count / elapsed if elapsed > 0 else None
    report = {
        "measurement": "full_deal_audit",
        "db_engine": "sqlite-wal",
        "db_durability_mode": "synchronous=OFF (test fixture)",
        "deal_count": outcome.source_count,
        "account_count": ACCOUNT_COUNT,
        "elapsed_seconds": round(elapsed, 3),
        "rows_per_second": round(rows_per_second, 1) if rows_per_second else None,
        "pages": scheduler.metrics.histogram("recon_source_pages", entity="deals").summary()
        if scheduler.metrics.histogram("recon_source_pages", entity="deals")
        else None,
        "rss_before_bytes": before_rss,
        "rss_after_bytes": after_rss,
        "note": (
            "measured on SQLite in this environment with fsync disabled for the "
            "fixture database; it is NOT a PostgreSQL measurement and does not "
            "validate any target in section 11.4"
        ),
    }
    _write_report("benchmark_deal_audit.json", report)


def test_audit_memory_stays_bounded_rather_than_proportional(
    scheduler: ReconciliationScheduler, large_world, settings, tmp_path: Path
) -> None:
    """Manifests stream; they are not materialised in memory (section 5.2).

    Measured as an absolute bound on the RSS delta for one full audit, not as
    a ratio between a small and a large dataset. A ratio looked more rigorous
    but would have been close to meaningless here: whichever audit ran second
    reuses the first one's freed arenas, so the comparison measures allocator
    behaviour rather than this service's. A generous absolute cap still fails
    loudly if a change starts loading whole manifests into lists.

    The cap is sized for the dataset, not derived from section 11.4 — nothing
    in section 11 is validated by this test.
    """
    psutil = pytest.importorskip("psutil")
    process = psutil.Process(os.getpid())

    # Settle allocations from fixture setup before taking the baseline.
    import gc

    gc.collect()
    baseline = process.memory_info().rss

    outcome = scheduler.run_audit(entity="deals", epoch_id=large_world["epoch_id"])

    peak = process.memory_info().rss
    assert outcome.status == "MATCHED", (outcome.reason_code, outcome.detail)
    assert outcome.source_count == large_world["deal_count"]

    delta = peak - baseline
    rows = outcome.source_count or 1
    bytes_per_row = delta / rows

    # A streaming capture of ~1.2k rows should cost far less than a copy of
    # them. 64 MiB is generous for this dataset and still orders of magnitude
    # below what materialising every row and payload would take.
    assert delta < 64 * 1024 * 1024, (
        f"audit of {rows} rows grew RSS by {delta / 1024 / 1024:.1f} MiB "
        f"({bytes_per_row:.0f} bytes/row), which suggests the manifest is "
        "being materialised rather than streamed"
    )

    report = {
        "measurement": "audit_memory_delta",
        "db_engine": "sqlite-wal",
        "rows": rows,
        "rss_baseline_bytes": baseline,
        "rss_after_bytes": peak,
        "rss_delta_bytes": delta,
        "bytes_per_row": round(bytes_per_row, 1),
        "note": (
            "an absolute bound on one audit, not a cross-size ratio; "
            "validates no target in section 11.4"
        ),
    }
    _write_report("benchmark_audit_memory.json", report)


def test_manifest_matches_an_independent_oracle_at_scale(
    scheduler: ReconciliationScheduler, large_world, p01_client, db
) -> None:
    """Correctness must survive the larger dataset, not just the small one.

    A performance test that stops checking correctness measures the speed of
    something that may be wrong.
    """
    outcome = scheduler.run_audit(entity="deals", epoch_id=large_world["epoch_id"])
    assert outcome.status == "MATCHED"

    rows: list[dict] = []
    cursor = None
    while True:
        params = {"limit": 500, "after_seq": 0} if cursor is None else {"cursor": cursor}
        response = p01_client.get("/v1/deals", params=params)
        body = response.json()
        rows.extend(body["items"])
        cursor = body.get("next_cursor")
        if cursor is None:
            break

    expected = root_hash(
        "deals", [(row["deal_id"], payload_hash("deals", row)) for row in rows]
    )
    conn = db.connect(Role.RECON)
    try:
        stored = conn.execute(
            "SELECT source_root_hash, target_root_hash, source_count FROM recon_runs WHERE recon_run_id = ?",
            (outcome.recon_run_id,),
        ).fetchone()
    finally:
        conn.close()

    assert stored["source_count"] == len(rows)
    assert stored["source_root_hash"] == expected
    assert stored["target_root_hash"] == expected


def test_page_budget_bounds_a_large_scan(
    db, repository, harness, source, clock, large_world, tmp_path: Path
) -> None:
    """A dataset larger than the budget is stopped, not streamed forever."""
    from reconciliation.config import load_settings
    from reconciliation.errors import ReasonCode
    from reconciliation.scheduler import ReconciliationScheduler as Scheduler

    tight = load_settings(
        env="test",
        db_path=tmp_path / "tight.db",
        source_id="sim_local",
        max_pages_per_run=3,
        max_items_per_run=1_000_000,
        source_max_page_size=10,
    )
    repo = type(repository)(db, tight, clock)
    scheduler = Scheduler(
        db=db,
        repository=repo,
        source=source,
        read_port=harness,
        repair_port=harness,
        settings=tight,
        clock=clock,
    )

    outcome = scheduler.run_audit(entity="deals", epoch_id=large_world["epoch_id"])

    assert outcome.status == "INCONCLUSIVE"
    assert outcome.reason_code is ReasonCode.SCAN_QUOTA_EXCEEDED
    assert repo.page_receipt_count(outcome.recon_run_id) <= tight.max_pages_per_run
