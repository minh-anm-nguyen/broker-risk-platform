"""Model-based test: random action sequences against the real stack.

Design 15.4 asks for an abstract model, randomised actions under a fixed seed,
and an invariant check after *every* transition. That last part is what makes
this worth more than a longer happy-path test: a violation is caught at the
transition that caused it rather than at the end, where several faults have
already composed and the cause is ambiguous.

Seeds are fixed and listed. A failure is reproducible by running the one seed,
which is the design's requirement for chaos evidence (WF3-249).

The model deliberately tracks only what must hold, not a full mirror of the
database:

``watermark``   the highest watermark ever observed, to catch regression
``deal_hashes`` first-seen content per deal id, to catch a silent rewrite
``published``   whether any snapshot has been published, per entity
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

import pytest
from sqlalchemy import text

from tests.fakes.fake_source import Fault
from trading_sync.application.killpoints import KillPointCrash
from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity
from trading_sync.errors import SyncError

pytestmark = [pytest.mark.e2e, pytest.mark.integration, pytest.mark.property]

MODEL_SEEDS = (1, 7, 42, 1337, 20261031)
STEPS_PER_SEED = 24


@dataclass
class Model:
    watermark: int = 0
    deal_hashes: dict[str, str] = field(default_factory=dict)
    published: set[str] = field(default_factory=set)
    transitions: list[str] = field(default_factory=list)


async def _db_deals(sessions) -> dict[str, str]:
    async with sessions() as session:
        rows = (await session.execute(text("SELECT deal_id, payload_hash FROM deals"))).all()
    return {row[0]: row[1] for row in rows}


async def _check_invariants(model: Model, source, sink, epoch, sessions) -> None:
    """Assert every invariant that must hold after any single transition."""
    stored = await _db_deals(sessions)

    # INV-05: a stored deal's content never changes once written.
    for deal_id, digest in stored.items():
        known = model.deal_hashes.get(deal_id)
        if known is not None:
            assert digest == known, f"deal {deal_id} content changed in place"
        else:
            model.deal_hashes[deal_id] = digest

    # INV-02: the watermark never regresses.
    current = await sink.read_watermark(epoch.epoch_id, Entity.DEALS, FILTER_UNFILTERED)
    assert current >= model.watermark, f"watermark regressed {model.watermark} -> {current}"
    model.watermark = current

    # INV-01: nothing is stored that the source never produced, and nothing
    # below the watermark is missing. Together these rule out both invention
    # and loss, which a count comparison alone would not.
    oracle_all = source.oracle_deals(after_seq=0, snapshot_seq=source.committed_seq)
    for deal_id in stored:
        assert deal_id in oracle_all, f"deal {deal_id} is not in the source at all"
    expected_upto = source.oracle_deals(after_seq=0, snapshot_seq=current)
    missing = set(expected_upto) - set(stored)
    assert not missing, f"deals at or below the watermark are missing: {sorted(missing)}"

    # INV-07: the current projection is always exactly one published scan.
    async with sessions() as session:
        for table in ("accounts_current", "positions_current"):
            scan_ids = (
                await session.execute(
                    text(f"SELECT DISTINCT published_scan_id FROM {table}")  # noqa: S608
                )
            ).scalars().all()
            assert len(scan_ids) <= 1, f"{table} holds a hybrid of {len(scan_ids)} scans"

        # Design 15.3: a checkpoint may only reference a PUBLISHED scan.
        dangling = (
            await session.execute(
                text(
                    "SELECT count(*) FROM sync_checkpoints c "
                    "JOIN sync_runs r ON r.scan_id = c.published_scan_id "
                    "WHERE r.status <> 'PUBLISHED'"
                )
            )
        ).scalar()
        assert dangling == 0

        # Design 15.3: a published scan has a gapless receipt chain with one
        # terminal page.
        broken = (
            await session.execute(
                text(
                    "WITH sums AS ("
                    " SELECT r.scan_id, count(p.page_index) AS n,"
                    "        min(p.page_index) AS lo, max(p.page_index) AS hi,"
                    "        count(*) FILTER (WHERE p.is_terminal) AS terminals"
                    " FROM sync_runs r"
                    " LEFT JOIN sync_page_receipts p ON p.scan_id = r.scan_id"
                    " WHERE r.status = 'PUBLISHED'"
                    " GROUP BY r.scan_id)"
                    " SELECT count(*) FROM sums"
                    " WHERE n = 0 OR lo <> 0 OR hi <> n - 1 OR terminals <> 1"
                )
            )
        ).scalar()
        assert broken == 0


@pytest.mark.whatif("WF3-240", "WF3-249")
@pytest.mark.parametrize("seed", MODEL_SEEDS)
async def test_random_action_sequences_preserve_every_invariant(
    seed: int, deal_service, snapshot_service, source, sink, epoch, sessions, barriers
) -> None:
    """Drive the stack through a random mix of progress and faults.

    The action set mirrors design 15.4: source mutation, scans of each entity,
    cursor expiry, and a crash at a commit boundary. Failures are swallowed on
    purpose -- a fault is a legitimate outcome, and the point is that the
    invariants hold afterwards either way.
    """
    rng = random.Random(seed)
    model = Model()
    source.page_size = rng.choice((1, 2, 5))

    actions = (
        "add_deals",
        "advance_events",
        "sync_deals",
        "sync_accounts",
        "sync_positions",
        "set_account",
        "set_position",
        "remove_position",
        "expire_cursor",
        "crash_at_commit",
    )
    deal_counter = 0

    for step in range(STEPS_PER_SEED):
        action = rng.choice(actions)
        model.transitions.append(action)
        try:
            if action == "add_deals":
                for _ in range(rng.randint(1, 4)):
                    deal_counter += 1
                    source.add_deal(
                        f"D{deal_counter}", commission=f"-{rng.randint(1, 99)}.25"
                    )
            elif action == "advance_events":
                source.advance_events(rng.randint(1, 6))
            elif action == "sync_deals":
                await deal_service.run_once(source)
            elif action == "sync_accounts":
                await snapshot_service.run_once(source, Entity.ACCOUNTS)
            elif action == "sync_positions":
                await snapshot_service.run_once(source, Entity.POSITIONS)
            elif action == "set_account":
                source.set_account(
                    f"A{rng.randint(1, 3)}", balance=f"{rng.randint(0, 9999)}.50"
                )
            elif action == "set_position":
                source.set_position(
                    f"P{rng.randint(1, 3)}", volume_lots=f"0.{rng.randint(1, 9)}0"
                )
            elif action == "remove_position":
                source.remove_position(f"P{rng.randint(1, 3)}")
            elif action == "expire_cursor":
                source.inject(
                    Fault(mode="snapshot_expired", at_page=rng.randint(0, 2))
                )
                try:
                    await deal_service.run_once(source)
                finally:
                    source.clear_faults()
            elif action == "crash_at_commit":
                point = rng.choice(
                    (
                        "K06_after_commit_before_ack",
                        "K07_after_last_page_commit",
                        "K09_after_checkpoint_update_before_commit",
                        "K13_before_snapshot_promote",
                    )
                )
                barriers.arm_crash(point)
                try:
                    if point.startswith("K13"):
                        await snapshot_service.run_once(source, Entity.ACCOUNTS)
                    else:
                        await deal_service.run_once(source)
                finally:
                    barriers.clear()
        except (SyncError, KillPointCrash):
            # A classified failure or a simulated crash is an expected outcome
            # of these actions. What must never happen is an invariant breaking.
            pass

        await _check_invariants(model, source, sink, epoch, sessions)

    # After the random walk, one clean scan must reconcile the dataset exactly.
    # This is the part that would catch a "safe" fault handler that quietly
    # dropped records: every invariant above can hold while data is missing,
    # but oracle equality after a successful scan cannot.
    source.clear_faults()
    barriers.clear()
    await deal_service.run_once(source)
    stored = await _db_deals(sessions)
    assert stored == source.oracle_deals(after_seq=0, snapshot_seq=source.committed_seq), (
        f"seed {seed} diverged from the oracle; transitions: {model.transitions}"
    )
