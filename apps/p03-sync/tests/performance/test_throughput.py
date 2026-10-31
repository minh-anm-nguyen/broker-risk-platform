"""Throughput, latency distribution and memory bounds (design 16, group R).

These are **measurements**, not validated SLOs. The design is explicit that its
numbers are provisional targets to be measured, never reported as achieved
(design 16.2), so this module records distributions into a manifest and asserts
only the things that are genuinely invariant regardless of hardware:

* correctness survives the load -- the oracle still matches exactly;
* memory stays bounded while streaming, because one page is held at a time;
* the connection pool is never exceeded.

Latency is reported as p50/p95/p99 with environment metadata rather than as a
mean, since a mean hides exactly the tail that matters (WF3-250).
"""

from __future__ import annotations

import json
import os
import pathlib
import platform
import statistics
import sys
import time

import psutil
import pytest
from sqlalchemy import text

from tests.support import deal_payload, deal_scan_state, make_page
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity

pytestmark = [pytest.mark.performance, pytest.mark.integration]

ARTIFACTS = pathlib.Path(__file__).resolve().parents[2] / "artifacts"

#: Default stays modest so the suite is runnable on any machine. Raise it with
#: SYNC_PERF_DEALS to approach the design's 100k reference fixture.
DEAL_COUNT = int(os.environ.get("SYNC_PERF_DEALS", "10000"))
PAGE_SIZE = 500


def _manifest(name: str, payload: dict[str, object]) -> None:
    """Persist a benchmark record with enough metadata to be reproducible.

    A latency number without the git state, the durability mode and the
    machine is not comparable to anything, which is the trap design 16.4 lists
    first.
    """
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    record = {
        "benchmark": name,
        "note": "measurement only; not a validated SLO",
        "environment": {
            "python": sys.version.split()[0],
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
        **payload,
    }
    (ARTIFACTS / f"benchmark_{name}.json").write_text(
        json.dumps(record, indent=2) + "\n", encoding="utf-8"
    )


@pytest.mark.slow
@pytest.mark.whatif("WF3-145", "WF3-205", "WF3-208")
async def test_bulk_deal_ingestion_is_exact_and_bounded(
    sink, epoch, sessions, settings
) -> None:
    """Ingest a large deal history one page at a time.

    The memory assertion is the substantive one. Streaming means resident
    memory should be flat in the number of records; a regression that buffered
    the dataset would show up here as growth proportional to DEAL_COUNT, which
    is how the 1M-deal case turns into an OOM (INV-15, WF3-145).
    """
    process = psutil.Process()
    baseline_rss = process.memory_info().rss

    lease = await sink.acquire_lease(
        epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED, ttl_seconds=600
    )
    scan = await sink.create_scan(lease, start_watermark=0)
    state = deal_scan_state(snapshot_seq=DEAL_COUNT + 1)

    page_latencies: list[float] = []
    rss_samples: list[int] = []
    pages = (DEAL_COUNT + PAGE_SIZE - 1) // PAGE_SIZE
    started = time.perf_counter()

    for index in range(pages):
        first = index * PAGE_SIZE + 1
        last = min(first + PAGE_SIZE, DEAL_COUNT + 1)
        items = [
            deal_payload(f"D{seq}", seq, commission=f"-{(seq % 97) + 1}.25")
            for seq in range(first, last)
        ]
        page = make_page(
            state,
            items,
            index=index,
            snapshot_seq=DEAL_COUNT + 1,
            terminal=index == pages - 1,
        )
        page_started = time.perf_counter()
        await sink.persist_deal_page(lease, scan, page)
        page_latencies.append((time.perf_counter() - page_started) * 1000)
        rss_samples.append(process.memory_info().rss)

    await sink.publish_deal_checkpoint(
        lease, scan, old_watermark=0, new_watermark=DEAL_COUNT + 1
    )
    elapsed = time.perf_counter() - started

    async with sessions() as session:
        stored = (await session.execute(text("SELECT count(*) FROM deals"))).scalar()
        distinct = (
            await session.execute(text("SELECT count(DISTINCT deal_id) FROM deals"))
        ).scalar()
    assert stored == DEAL_COUNT
    assert distinct == DEAL_COUNT

    ordered = sorted(page_latencies)
    percentile = lambda data, q: data[min(len(data) - 1, int(len(data) * q))]  # noqa: E731
    growth_mib = (max(rss_samples) - baseline_rss) / (1024 * 1024)

    _manifest(
        "deal_ingestion",
        {
            "deals": DEAL_COUNT,
            "pages": pages,
            "page_size": PAGE_SIZE,
            "elapsed_seconds": round(elapsed, 3),
            "deals_per_second": round(DEAL_COUNT / elapsed, 1),
            "page_commit_ms": {
                "p50": round(percentile(ordered, 0.50), 2),
                "p95": round(percentile(ordered, 0.95), 2),
                "p99": round(percentile(ordered, 0.99), 2),
                "max": round(ordered[-1], 2),
                "stdev": round(statistics.pstdev(ordered), 2),
            },
            "rss_growth_mib": round(growth_mib, 1),
            "durability": "fsync=on, synchronous_commit=on",
        },
    )

    # Generous and machine-independent: the point is to catch a regression that
    # buffers the dataset, not to pin a number to this laptop.
    assert growth_mib < 256, f"resident memory grew {growth_mib:.1f} MiB while streaming"


@pytest.mark.whatif("WF3-206")
async def test_an_idle_poll_is_cheap(sink, epoch, sessions, deal_service, source) -> None:
    """A poll finding nothing new must not scale with history size.

    A quiet source is the steady state, so if an empty poll touched every
    stored row the cost would grow forever while nothing happened.
    """
    for index in range(200):
        source.add_deal(f"D{index}")
    await deal_service.run_once(source)

    source.advance_events(5)
    started = time.perf_counter()
    outcome = await deal_service.run_once(source)
    elapsed_ms = (time.perf_counter() - started) * 1000

    assert outcome.records == 0
    assert outcome.pages == 1
    _manifest("idle_poll", {"elapsed_ms": round(elapsed_ms, 2), "history_deals": 200})


@pytest.mark.whatif("WF3-113")
async def test_the_connection_pool_is_never_exceeded(sink, epoch, engine, settings) -> None:
    """Concurrent work waits for a connection rather than opening more.

    An unbounded pool turns a load spike into "too many connections" on the
    database, which takes down every other client too (WF3-113).
    """
    import asyncio

    async def one_read() -> int:
        return await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED)

    await asyncio.gather(*(one_read() for _ in range(40)))
    pool = engine.pool
    assert pool.checkedout() == 0
    assert pool.size() <= settings.postgres.pool_size
    assert pool.overflow() <= settings.postgres.max_overflow
