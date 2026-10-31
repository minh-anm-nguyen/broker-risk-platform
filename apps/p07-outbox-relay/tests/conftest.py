"""Shared fixtures.

Two choices shape every test in this suite.

**The clock is manual.** Lease expiry, backoff and retention are time
predicates, so a test that needed real time to pass would either be slow or
flaky. :class:`~outbox.clock.ManualClock` makes "the lease expired" an
instruction rather than a wait, and it makes the ordering of a takeover race
deterministic instead of hopeful.

**The hook registry is reset around every test.** Fire counts are evidence
(oracle O8), and evidence that leaked in from a previous test is worse than no
evidence: it would let a crash test pass while reporting a fault that fired
somewhere else entirely.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from outbox.clock import ManualClock
from outbox.faults.hooks import HOOKS, HookRegistry
from outbox.fixtures import EPOCH_A, EPOCH_B
from outbox.pipeline import Pipeline, build_pipeline


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def hooks() -> Iterator[HookRegistry]:
    HOOKS.reset()
    yield HOOKS
    HOOKS.reset()


@pytest.fixture
def pipeline(tmp_path: Path, clock: ManualClock, hooks: HookRegistry) -> Iterator[Pipeline]:
    """A migrated pipeline with one ACTIVE epoch (``EPOCH_A``) already open."""
    built = build_pipeline(
        root=tmp_path / "pipeline",
        clock=clock,
        hooks=hooks,
        claim_batch_size=20,
        max_inflight=32,
        lease_seconds=30.0,
        delivery_deadline_seconds=5.0,
        # See SqliteOutboxStore: in-process crash simulation never leaves the
        # file mid-write, so FULL would buy fsync latency and no coverage.
        synchronous="NORMAL",
    )
    built.sync.open_epoch(EPOCH_A)
    yield built
    built.close()


@pytest.fixture
def epoch() -> str:
    return EPOCH_A


@pytest.fixture
def other_epoch() -> str:
    return EPOCH_B


@pytest.fixture
def pg_dsn() -> str:
    """Skip a PostgreSQL test unless a server was provided."""
    dsn = os.environ.get("OUTBOX_PG_DSN")
    if not dsn:
        pytest.skip("set OUTBOX_PG_DSN to run the PostgreSQL-backed tests")
    pytest.importorskip("psycopg", reason="the postgres extra is not installed")
    return dsn


def expire_all_leases(pipeline: Pipeline) -> int:
    """Make every outstanding lease look expired, without moving the clock.

    Used where a test needs a takeover but not the side effects of advancing
    time (backoff deadlines, retention windows). Returns the row count so a
    test can assert that there was in fact something leased to take over --
    otherwise "the standby published it" could just mean "nothing was claimed".
    """
    rows = pipeline.store.query("SELECT COUNT(*) AS n FROM event_outbox WHERE state = 'CLAIMED'")
    count = int(rows[0]["n"])
    with pipeline.store.transaction() as tx:
        tx.execute(
            "UPDATE event_outbox SET lease_until = '2000-01-01T00:00:00.000000Z' "
            "WHERE state = 'CLAIMED'"
        )
    return count


def published_event_ids(pipeline: Pipeline) -> set[str]:
    return {
        str(row["event_id"])
        for row in pipeline.store.query("SELECT event_id FROM event_outbox WHERE state = 'PUBLISHED'")
    }


def broker_event_ids(pipeline: Pipeline) -> list[str]:
    """Every event id in the broker log, duplicates included."""
    import json

    return [json.loads(record.value.decode("utf-8"))["event_id"] for record in pipeline.broker.records()]
