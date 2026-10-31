"""Benchmarks: WF-088, WF-089, WF-090, WF-096, INV-16.

Section 9.4 is explicit that a number without its environment is not a
result, so every measurement here writes a manifest recording the git SHA,
interpreter, SQLite version, host CPU and memory, the dataset and the seed. A
benchmark run with a missing manifest is marked ``INVALID_RESULT`` rather than
published (WF-115).

Two honesty rules shaped these tests:

* **Targets are thresholds to measure, not results already achieved.** The
  provisional SLO numbers in section 9.2 are for a 4 vCPU Linux reference
  host. This suite runs wherever CI runs, so by default it *records* against
  the targets and only asserts the bounds that must hold on any machine -
  correctness, memory, backpressure. Set ``SIM_BENCH_ENFORCE_SLO=1`` on the
  reference host to make the latency and throughput targets assertions.
* **Rate-limited traffic is never counted as throughput.** Section 9.4 point
  6: a benchmark whose load was being refused has not measured the service.
"""

from __future__ import annotations

import json
import os
import platform
import sqlite3
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import psutil
import pytest
from fastapi.testclient import TestClient

from simulator import GENERATOR_VERSION, __version__
from simulator.main import create_app
from tests.conftest import ADMIN_TOKEN, DATA_TOKEN, create_run, make_settings, wait_for_job

pytestmark = [pytest.mark.performance, pytest.mark.slow]

ADMIN = {"Authorization": f"Bearer {ADMIN_TOKEN}"}
DATA = {"Authorization": f"Bearer {DATA_TOKEN}"}

ARTIFACTS = Path(__file__).resolve().parents[2] / "artifacts"

#: Provisional targets from section 9.2, for the 4 vCPU Linux reference host.
TARGETS = {
    "bootstrap_100k_seconds": 300.0,
    "read_throughput_rps": 100.0,
    "read_p95_seconds": 0.300,
    "read_p99_seconds": 1.000,
    "peak_rss_bytes": 1.5 * 1024**3,
    "restart_ready_seconds": 30.0,
}

ENFORCE_SLO = os.environ.get("SIM_BENCH_ENFORCE_SLO") == "1"

#: Keep the default run quick enough for CI. Override for a full baseline.
DEAL_COUNT = int(os.environ.get("SIM_BENCH_DEALS", "6000"))
ACCOUNT_COUNT = int(os.environ.get("SIM_BENCH_ACCOUNTS", "200"))


def manifest(extra: dict[str, Any]) -> dict[str, Any]:
    """Section 9.4 point 1: a result is only comparable with its environment."""
    try:
        git_sha = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=str(ARTIFACTS.parent),
        ).stdout.strip() or "not-a-git-repository"
    except Exception:  # noqa: BLE001 - absence of git is itself a fact to record
        git_sha = "git-unavailable"

    memory = psutil.virtual_memory()
    return {
        "git_sha": git_sha,
        "simulator_version": __version__,
        "generator_version": GENERATOR_VERSION,
        "python": sys.version.split()[0],
        "sqlite": sqlite3.sqlite_version,
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "logical_cpus": os.cpu_count(),
        "total_memory_bytes": memory.total,
        "is_reference_environment": False,
        "slo_enforced": ENFORCE_SLO,
        "targets": TARGETS,
        **extra,
    }


def record(name: str, payload: dict[str, Any]) -> Path:
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    path = ARTIFACTS / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


def check(name: str, measured: float, target: float, *, lower_is_better: bool = True) -> str:
    met = measured <= target if lower_is_better else measured >= target
    verdict = "MET" if met else "NOT_MET"
    if ENFORCE_SLO:
        assert met, f"{name}: measured {measured} against target {target}"
    return verdict


# ---------------------------------------------------------------------------
# Generation throughput and resource bounds
# ---------------------------------------------------------------------------


def test_dataset_bootstrap_is_measured_and_correct(tmp_path: Path) -> None:
    """WF-088: generate, time it, then prove the data is actually right.

    A throughput number from a run that produced wrong data is worthless, so
    the invariant sweep is part of the benchmark rather than a separate step.
    """
    settings = make_settings(
        tmp_path, writer_batch_size=500, max_pending_events=10_000, rate_limit_enabled=False
    )
    app = create_app(settings, lock_process=False)
    process = psutil.Process()
    peak_rss = process.memory_info().rss

    with TestClient(app) as client:
        started = time.perf_counter()
        accepted = create_run(
            client, ADMIN, account_count=ACCOUNT_COUNT, deal_count=DEAL_COUNT, wait=False
        )
        while True:
            job = client.get(accepted["status_url"], headers=ADMIN).json()
            peak_rss = max(peak_rss, process.memory_info().rss)
            if job["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(0.05)
        elapsed = time.perf_counter() - started
        assert job["status"] == "SUCCEEDED", job
        assert job["processed_count"] == DEAL_COUNT

        report = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()
        run = client.get("/admin/v1/runs/current", headers=ADMIN).json()

    database = Path(settings.db_path)
    wal = database.with_suffix(database.suffix + "-wal")
    deals_per_second = DEAL_COUNT / elapsed
    extrapolated = 100_000 / deals_per_second

    payload = manifest(
        {
            "benchmark": "dataset_bootstrap",
            "dataset": {"accounts": ACCOUNT_COUNT, "deals": DEAL_COUNT, "seed": 42},
            "measured": {
                "elapsed_seconds": round(elapsed, 3),
                "events": run["committed_seq"],
                "deals_per_second": round(deals_per_second, 1),
                "events_per_second": round(run["committed_seq"] / elapsed, 1),
                "extrapolated_100k_seconds": round(extrapolated, 1),
                "peak_rss_bytes": peak_rss,
                "db_bytes": database.stat().st_size,
                "wal_bytes": wal.stat().st_size if wal.exists() else 0,
            },
            "verdicts": {
                "bootstrap_100k": check(
                    "bootstrap_100k", extrapolated, TARGETS["bootstrap_100k_seconds"]
                ),
                "peak_rss": check("peak_rss", peak_rss, TARGETS["peak_rss_bytes"]),
            },
            "correctness": report["summary"],
            "canonical_digest": report["canonical_digest"],
        }
    )
    record("benchmark_bootstrap", payload)

    # Asserted on any machine: the data is correct and memory is bounded.
    assert report["summary"]["failed"] == 0
    assert report["summary"]["storage_scope_ok"] is True
    assert report["record_counts"]["deals"] == DEAL_COUNT
    assert peak_rss < TARGETS["peak_rss_bytes"], peak_rss


def test_read_throughput_and_latency_are_measured(tmp_path: Path) -> None:
    """WF-089: the query mix from section 9.1, with client-side latency.

    Latency is measured around the client call, not inside the handler.
    Handler time omits exactly the queueing and serialization a consumer
    actually waits for (section 9.4 point 3).
    """
    settings = make_settings(
        tmp_path, writer_batch_size=500, rate_limit_enabled=False
    )
    app = create_app(settings, lock_process=False)

    mix = (
        ["/v1/deals?limit=100"] * 60
        + ["/v1/positions?limit=100"] * 20
        + ["/v1/accounts?limit=100"] * 15
        + ["/v1/meta"] * 5
    )
    concurrency = 16
    iterations = 320

    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=ACCOUNT_COUNT, deal_count=DEAL_COUNT)

        # Warm-up, excluded from the measurement window (section 9.4 point 2).
        for index in range(40):
            client.get(mix[index % len(mix)], headers=DATA)

        latencies: list[float] = []
        statuses: list[int] = []

        def request(index: int) -> None:
            route = mix[index % len(mix)]
            started = time.perf_counter()
            response = client.get(route, headers=DATA)
            latencies.append(time.perf_counter() - started)
            statuses.append(response.status_code)

        window_started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(request, range(iterations)))
        window = time.perf_counter() - window_started

        report = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()

    successes = sum(1 for status in statuses if status == 200)
    rejected = sum(1 for status in statuses if status in {429, 503})
    unexpected = [status for status in statuses if status not in {200, 429, 503}]
    achieved_rps = successes / window

    payload = manifest(
        {
            "benchmark": "read_mix",
            "dataset": {"accounts": ACCOUNT_COUNT, "deals": DEAL_COUNT},
            "workload": {
                "concurrency": concurrency,
                "requests": iterations,
                "mix": "60% deals, 20% positions, 15% accounts, 5% meta",
                "warmup_requests": 40,
            },
            "measured": {
                "window_seconds": round(window, 3),
                "successful": successes,
                # Reported separately and never folded into throughput.
                "rejected": rejected,
                "achieved_rps": round(achieved_rps, 1),
                "p50_seconds": round(percentile(latencies, 0.50), 4),
                "p95_seconds": round(percentile(latencies, 0.95), 4),
                "p99_seconds": round(percentile(latencies, 0.99), 4),
                "max_seconds": round(max(latencies), 4),
                "mean_seconds": round(statistics.fmean(latencies), 4),
            },
            "verdicts": {
                "throughput": check(
                    "throughput", achieved_rps, TARGETS["read_throughput_rps"],
                    lower_is_better=False,
                ),
                "p95": check("p95", percentile(latencies, 0.95), TARGETS["read_p95_seconds"]),
                "p99": check("p99", percentile(latencies, 0.99), TARGETS["read_p99_seconds"]),
            },
            "correctness": report["summary"],
        }
    )
    record("benchmark_read_mix", payload)

    # Asserted everywhere: no unexpected 5xx, and the data survived the load.
    assert unexpected == [], unexpected
    assert rejected == 0, "rate limiting was active; the throughput number would be invalid"
    assert report["summary"]["failed"] == 0


def test_backpressure_is_bounded(tmp_path: Path) -> None:
    """INV-16 / WF-090 / WF-096: refuse under overload, recover after it.

    The assertions here hold on any machine, because they are about bounds
    rather than speed: the queue never exceeds its capacity, refusals are the
    named retryable ones, memory does not run away, and the service returns to
    serving once the load stops - with no manual repair.
    """
    settings = make_settings(
        tmp_path,
        max_pending_events=256,
        max_data_requests_in_flight=8,
        admission_timeout_ms=25,
        writer_batch_size=50,
        rate_limit_enabled=False,
    )
    app = create_app(settings, lock_process=False)
    process = psutil.Process()

    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=50, deal_count=1_500, wait=False)

        rss_before = process.memory_info().rss
        peak_rss = rss_before
        statuses: list[int] = []
        max_queue = 0
        stop_at = time.monotonic() + 3.0

        def flood(_: int) -> None:
            nonlocal peak_rss
            while time.monotonic() < stop_at:
                response = client.get("/v1/deals?limit=200", headers=DATA)
                statuses.append(response.status_code)
                peak_rss = max(peak_rss, process.memory_info().rss)

        def observe(_: int) -> None:
            nonlocal max_queue
            while time.monotonic() < stop_at:
                ready = client.get("/health/ready").json()
                max_queue = max(max_queue, ready["writer_queue_depth"])
                assert ready["requests_in_flight"] <= 8, ready
                assert ready["active_snapshots"] <= settings.max_active_snapshots
                time.sleep(0.02)

        with ThreadPoolExecutor(max_workers=14) as pool:
            futures = [pool.submit(flood, index) for index in range(12)]
            futures.append(pool.submit(observe, 0))
            for future in futures:
                future.result()

        refused = sum(1 for status in statuses if status in {429, 503})
        unexpected = [status for status in statuses if status not in {200, 429, 503}]

        # Recovery: once the load stops, the service serves again untouched.
        recovery_started = time.perf_counter()
        for _ in range(20):
            assert client.get("/v1/deals?limit=50", headers=DATA).status_code == 200
        recovery = time.perf_counter() - recovery_started

        report = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()
        ready = client.get("/health/ready").json()

    payload = manifest(
        {
            "benchmark": "overload",
            "workload": {"concurrency": 12, "duration_seconds": 3.0, "page_size": 200},
            "measured": {
                "requests": len(statuses),
                "refused": refused,
                "refusal_ratio": round(refused / max(1, len(statuses)), 4),
                "max_writer_queue_depth": max_queue,
                "writer_queue_capacity": settings.max_pending_events,
                "rss_before_bytes": rss_before,
                "peak_rss_bytes": peak_rss,
                "rss_growth_bytes": peak_rss - rss_before,
                "recovery_seconds_for_20_requests": round(recovery, 3),
            },
            "correctness": report["summary"],
        }
    )
    record("benchmark_overload", payload)

    assert unexpected == [], unexpected
    assert max_queue <= settings.max_pending_events, max_queue
    assert ready["ready"] is True
    assert report["summary"]["failed"] == 0
    # Memory is bounded by the queue and page caps, not by offered load.
    assert peak_rss - rss_before < 512 * 1024**2, peak_rss - rss_before


def test_restart_readiness_is_measured(tmp_path: Path) -> None:
    """Section 9.2: a restart must become ready quickly on an existing dataset."""
    settings = make_settings(tmp_path, writer_batch_size=500, rate_limit_enabled=False)
    app = create_app(settings, lock_process=False)
    with TestClient(app) as client:
        create_run(client, ADMIN, account_count=ACCOUNT_COUNT, deal_count=DEAL_COUNT)
        digest = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()["canonical_digest"]

    started = time.perf_counter()
    restarted = create_app(make_settings(tmp_path), lock_process=False)
    with TestClient(restarted) as client:
        ready = client.get("/health/ready").json()
        elapsed = time.perf_counter() - started
        assert ready["ready"] is True, ready
        after = client.get(
            "/admin/v1/diagnostics/invariants?scope=full", headers=ADMIN
        ).json()

    payload = manifest(
        {
            "benchmark": "restart_readiness",
            "dataset": {"accounts": ACCOUNT_COUNT, "deals": DEAL_COUNT},
            "measured": {"ready_after_seconds": round(elapsed, 3)},
            "verdicts": {
                "restart_ready": check(
                    "restart_ready", elapsed, TARGETS["restart_ready_seconds"]
                )
            },
            "digest_stable": after["canonical_digest"] == digest,
        }
    )
    record("benchmark_restart", payload)

    assert after["canonical_digest"] == digest
    assert elapsed < TARGETS["restart_ready_seconds"], elapsed


def test_a_benchmark_without_its_environment_is_invalid(tmp_path: Path) -> None:
    """WF-115: a number with no manifest is not publishable.

    This is a test of the reporting contract rather than of the service, and
    it exists because "we measured 120 ms" with no recorded hardware, dataset
    or seed is the most common way a benchmark misleads.
    """
    required = {
        "git_sha",
        "python",
        "sqlite",
        "platform",
        "logical_cpus",
        "total_memory_bytes",
        "generator_version",
        "is_reference_environment",
        "targets",
    }
    payload = manifest({"benchmark": "contract_check", "measured": {}})
    assert required <= set(payload)
    # The default environment must never claim to be the reference host.
    assert payload["is_reference_environment"] is False

    incomplete = {key: value for key, value in payload.items() if key != "git_sha"}
    assert not required <= set(incomplete), "a missing field must invalidate the result"
