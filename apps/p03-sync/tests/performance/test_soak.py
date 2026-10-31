"""Soak test: resource trends under repeated faults (design 16.2, WF3-210).

The design asks for a 60-minute soak with repeated source outages, checking
that resident memory, the connection pool and staging do not grow without
bound once the system reaches steady state.

The duration gate is the honest part. ``SYNC_SOAK_SECONDS`` defaults to a short
smoke run, and the WF3-210 case is only reported when the configured duration
actually meets the design's hour. A 60-second run that claimed the 60-minute
case would be exactly the kind of evidence inflation design 15.6 rules out, so
below the threshold the case is skipped and reported as not tested.

    SYNC_SOAK_SECONDS=3600 pytest tests/performance/test_soak.py -m slow
"""

from __future__ import annotations

import json
import os
import pathlib
import time

import psutil
import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity

pytestmark = [pytest.mark.performance, pytest.mark.integration, pytest.mark.slow]

ARTIFACTS = pathlib.Path(__file__).resolve().parents[2] / "artifacts"

#: The design's soak length. Anything shorter is a smoke run, not the case.
REQUIRED_SOAK_SECONDS = 3600
SOAK_SECONDS = int(os.environ.get("SYNC_SOAK_SECONDS", "45"))


@pytest.mark.whatif("WF3-210")
async def test_resources_stay_bounded_under_repeated_outages(
    deal_service, snapshot_service, source, sink, epoch, sessions, engine
) -> None:
    """Cycle sync and faults, and watch the trends rather than the peaks.

    What matters is the *shape*: after warm-up, resident memory and pool usage
    should be flat. A slow leak is invisible in a single scan and obvious here.
    """
    if SOAK_SECONDS < REQUIRED_SOAK_SECONDS:
        pytest.skip(
            f"SYNC_SOAK_SECONDS={SOAK_SECONDS} is below the design's "
            f"{REQUIRED_SOAK_SECONDS}s soak; this run cannot certify WF3-210"
        )

    process = psutil.Process()
    deadline = time.monotonic() + SOAK_SECONDS
    samples: list[dict[str, float]] = []
    cycle = 0

    while time.monotonic() < deadline:
        cycle += 1
        for index in range(20):
            source.add_deal(f"D{cycle}_{index}")
        source.advance_events(5)

        if cycle % 3 == 0:
            # A periodic outage: the service must recover without accumulating.
            source.inject(Fault(mode="snapshot_expired", at_page=0))
        try:
            await deal_service.run_once(source)
        except Exception:  # noqa: BLE001 - faults are the point of the soak
            pass
        finally:
            source.clear_faults()

        source.set_account("A1", balance=f"{cycle}.00")
        try:
            await snapshot_service.run_once(source, Entity.ACCOUNTS)
        except Exception:  # noqa: BLE001
            pass

        async with sessions() as session:
            staging = (
                await session.execute(text("SELECT count(*) FROM account_staging"))
            ).scalar()
        samples.append(
            {
                "cycle": cycle,
                "rss_mib": process.memory_info().rss / (1024 * 1024),
                "pool_checked_out": engine.pool.checkedout(),
                "staging_rows": int(staging or 0),
            }
        )

    assert len(samples) >= 10, "soak produced too few samples to show a trend"
    warmup = max(3, len(samples) // 5)
    steady = samples[warmup:]
    first_rss = steady[0]["rss_mib"]
    last_rss = steady[-1]["rss_mib"]

    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "benchmark_soak.json").write_text(
        json.dumps(
            {
                "benchmark": "soak",
                "soak_seconds": SOAK_SECONDS,
                "cycles": cycle,
                "rss_mib_first_steady": round(first_rss, 1),
                "rss_mib_last_steady": round(last_rss, 1),
                "rss_growth_mib": round(last_rss - first_rss, 1),
                "max_pool_checked_out": max(s["pool_checked_out"] for s in steady),
                "max_staging_rows": max(s["staging_rows"] for s in steady),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    # Growth, not absolute size: the absolute figure is machine-dependent, a
    # monotonic climb is not.
    assert last_rss - first_rss < 128, (
        f"resident memory grew {last_rss - first_rss:.1f} MiB during the soak"
    )
    assert all(sample["pool_checked_out"] == 0 for sample in steady), (
        "a connection was left checked out between cycles"
    )


@pytest.mark.whatif("WF3-216")
def test_the_soak_duration_gate_is_explicit() -> None:
    """The gate itself is asserted, so it cannot quietly stop gating.

    If the threshold were ever lowered to make the suite faster, a short run
    would start certifying an hour-long case.
    """
    assert REQUIRED_SOAK_SECONDS == 3600
