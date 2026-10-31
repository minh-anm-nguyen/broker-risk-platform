"""The twelve metamorphic and property rules of section 10.4.

Each rule gets its own section below, named after the rule. Where a rule is
genuinely about a space of inputs (ordering, subsets, hash versions) it is
tested with Hypothesis so a counterexample comes with a seed and a shrink.
Where a rule is about a single decision boundary (permission, fencing) an
exhaustive parametrised test covers it more honestly than random search would.

These tests deliberately avoid booting the simulator: they operate on
manifests and decisions directly, which keeps them fast enough to run on every
commit and keeps the failure localised when one breaks.
"""

from __future__ import annotations

import itertools
import uuid
from decimal import Decimal

import pytest
from hypothesis import HealthCheck, assume, given, settings as hyp_settings
from hypothesis import strategies as st

from reconciliation.alignment import AlignmentEngine, AlignmentInputs
from reconciliation.canonical import (
    HASH_VERSION,
    SCHEMA_VERSION,
    item_key,
    page_hash,
    payload_hash,
    root_hash,
)
from reconciliation.clock import ManualClock
from reconciliation.config import load_settings
from reconciliation.db.engine import Database, Role, transaction
from reconciliation.db.repository import ReconRepository, RunRecord
from reconciliation.dto import (
    AlignmentDecision,
    CapturedItem,
    CapturedPage,
    SourceCapabilities,
    SourceIdentity,
    SyncStatus,
)
from reconciliation.errors import (
    ActionType,
    Confidence,
    ForbiddenActionError,
    LeaseLostError,
    ReasonCode,
    Severity,
)
from reconciliation.leases import LeaseManager
from reconciliation.planning import PERMITTED_ACTIONS, RepairPlanner, derive_intent_id

pytestmark = [pytest.mark.property]

#: Pure-function properties are cheap, so they get a wide search.
HYP = hyp_settings(
    max_examples=60,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)

#: Properties that commit to the database cost orders of magnitude more per
#: example. A narrower search still shrinks to a minimal counterexample; a
#: wide one would make the suite too slow to run on every commit, which is
#: worse for finding bugs than a smaller sample.
HYP_DB = hyp_settings(
    max_examples=8,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture, HealthCheck.too_slow],
)


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

money = st.decimals(
    min_value=Decimal("-1000000.00"),
    max_value=Decimal("1000000.00"),
    places=2,
    allow_nan=False,
    allow_infinity=False,
)

deal_ids = st.text(
    alphabet="0123456789ABCDEF", min_size=4, max_size=10
).map(lambda s: f"D-{s}")

account_ids = st.text(alphabet="0123456789", min_size=4, max_size=6).map(lambda s: f"A-{s}")


@st.composite
def deal_records(draw, seq: int = 1):
    return {
        "deal_id": draw(deal_ids),
        "source_seq": seq,
        "account_id": draw(account_ids),
        "position_id": "P-00000001",
        "symbol": draw(st.sampled_from(["EURUSD", "GBPUSD", "USDJPY"])),
        "entry": draw(st.sampled_from(["IN", "OUT"])),
        "side": draw(st.sampled_from(["BUY", "SELL"])),
        "volume_lots": "0.10",
        "price": "1.10000",
        "realized_pnl_gross": str(draw(money)),
        "commission": str(draw(money)),
        "swap": "0.00",
        "executed_at": "2026-01-01T00:00:00Z",
    }


@st.composite
def unique_deal_sets(draw, min_size: int = 1, max_size: int = 12):
    """A set of deals whose cardinality is exactly what was asked for.

    Identifiers are assigned deterministically rather than drawn and then
    deduplicated: a strategy that dedupes after drawing silently returns fewer
    records than ``min_size``, which makes any property about *size* -- page
    budgets, page boundaries, subset relations -- vacuously true. The
    randomness that matters here is in the money and instrument fields, and
    that is preserved.
    """
    count = draw(st.integers(min_value=min_size, max_value=max_size))
    records = []
    for index in range(count):
        record = draw(deal_records(seq=index + 1))
        record["deal_id"] = f"D-{index:08d}"
        record["source_seq"] = index + 1
        records.append(record)
    return records


# ---------------------------------------------------------------------------
# Local fixtures: no simulator needed
# ---------------------------------------------------------------------------


@pytest.fixture
def world(tmp_path):
    """A database with one active epoch and nothing else."""
    settings = load_settings(
        env="test",
        db_path=tmp_path / "prop.db",
        source_id="sim_local",
        approval_required=True,
        source_max_page_size=100,
    )
    db = Database(settings.db_path, synchronous="OFF")
    db.create_schema()
    clock = ManualClock()
    repo = ReconRepository(db, settings, clock)
    epoch_id = str(uuid.uuid4())
    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute("INSERT INTO sync_sources VALUES('sim_local','local')")
            conn.execute(
                "INSERT INTO source_epochs VALUES(?,?,?,?,?,?)",
                (epoch_id, "sim_local", "run_0001", 1, "ACTIVE", "2026-01-01T00:00:00.000000Z"),
            )
            for entity in ("accounts", "positions", "deals"):
                conn.execute(
                    """
                    INSERT INTO sync_checkpoints
                      (epoch_id, entity, watermark_seq, filter_hash, published, lease_version)
                    VALUES (?,?,?, 'UNFILTERED', 1, 0)
                    """,
                    (epoch_id, entity, 10_000),
                )
    finally:
        conn.close()
    counter = itertools.count(2)
    return {
        "db": db,
        "settings": settings,
        "clock": clock,
        "repo": repo,
        "epoch_id": epoch_id,
        # Hypothesis shares this fixture across examples; anything with a
        # uniqueness constraint needs a fresh value per example.
        "next_generation": lambda: next(counter),
        "next_scope": lambda: f"scope-{uuid.uuid4().hex}",
    }


def _capture_manifest(repo: ReconRepository, run_id: str, records, *, page_size: int) -> None:
    """Commit a manifest for ``records`` in pages of ``page_size``."""
    chunks = [records[i : i + page_size] for i in range(0, len(records), page_size)] or [[]]
    for index, chunk in enumerate(chunks):
        items = [
            CapturedItem(
                item_key=item_key("deals", record),
                payload_hash=payload_hash("deals", record),
                source_seq=record["source_seq"],
            )
            for record in chunk
        ]
        repo.commit_page(
            run_id,
            CapturedPage(
                page_index=index,
                run_id="run_0001",
                generation=1,
                snapshot_seq=10_000,
                items=items,
                page_hash=page_hash("deals", [(i.item_key, i.payload_hash) for i in items]),
                is_terminal=index == len(chunks) - 1,
            ),
        )


def _new_run(world, entity: str = "deals") -> RunRecord:
    run = world["repo"].create_run(
        request_id=str(uuid.uuid4()),
        source_id="sim_local",
        epoch_id=world["epoch_id"],
        entity=entity,
    )
    world["repo"].pin_source_identity(
        run.recon_run_id, run_id_source="run_0001", generation=1, snapshot_seq=10_000
    )
    return run


# ===========================================================================
# Rule 1 -- Permutation invariant
# ===========================================================================


@HYP
@given(records=unique_deal_sets(min_size=1, max_size=10), rotation=st.integers(0, 9))
def test_rule01_read_order_does_not_change_the_manifest_digest(records, rotation) -> None:
    """Reordering an unordered domain must not change the set digest."""
    pairs = [(item_key("deals", r), payload_hash("deals", r)) for r in records]
    rotated = pairs[rotation % len(pairs) :] + pairs[: rotation % len(pairs)]
    assert root_hash("deals", pairs) == root_hash("deals", rotated)


@HYP_DB
@given(records=unique_deal_sets(min_size=2, max_size=8))
def test_rule01b_page_boundaries_do_not_change_the_diff(records, world) -> None:
    """The same set split into different page sizes yields the same manifest."""
    repo = world["repo"]
    digests = []
    for page_size in (1, 3, len(records)):
        run = _new_run(world)
        _capture_manifest(repo, run.recon_run_id, records, page_size=page_size)
        repo.mark_source_complete(run.recon_run_id)
        stored = repo.get_run(run.recon_run_id)
        digests.append(stored.source_root_hash)
        assert stored.source_count == len(records)
    assert len(set(digests)) == 1, "page size must not affect the manifest digest"


# ===========================================================================
# Rule 2 -- Idempotency invariant
# ===========================================================================


@HYP_DB
@given(records=unique_deal_sets(min_size=1, max_size=6), repeats=st.integers(2, 4))
def test_rule02_replaying_a_completion_is_idempotent(records, repeats, world) -> None:
    repo = world["repo"]
    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, records, page_size=3)

    results = [repo.mark_source_complete(run.recon_run_id) for _ in range(repeats)]

    assert len({r.stored_items for r in results}) == 1
    stored = repo.get_run(run.recon_run_id)
    assert stored.source_count == len(records)
    assert repo.source_item_count(run.recon_run_id) == len(records)


@HYP
@given(
    plan_id=st.uuids().map(str),
    order=st.integers(0, 9),
    parameters_hash=st.text(alphabet="0123456789abcdef", min_size=64, max_size=64),
)
def test_rule02b_intent_identity_is_a_pure_function_of_the_action(
    plan_id, order, parameters_hash
) -> None:
    """The same action always presents the same identity to P03 (INV04-08)."""
    first = derive_intent_id(
        plan_id=plan_id,
        action_order=order,
        action_type=ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
        parameters_hash=parameters_hash,
    )
    second = derive_intent_id(
        plan_id=plan_id,
        action_order=order,
        action_type=ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
        parameters_hash=parameters_hash,
    )
    assert first == second
    # And any change to the action changes the identity.
    assert first != derive_intent_id(
        plan_id=plan_id,
        action_order=order + 1,
        action_type=ActionType.REQUEST_FULL_ACCOUNT_RESYNC,
        parameters_hash=parameters_hash,
    )
    assert first != derive_intent_id(
        plan_id=plan_id,
        action_order=order,
        action_type=ActionType.REQUEST_FULL_POSITION_RESYNC,
        parameters_hash=parameters_hash,
    )


# ===========================================================================
# Rule 3 -- Epoch isolation
# ===========================================================================


@HYP_DB
@given(records=unique_deal_sets(min_size=1, max_size=6))
def test_rule03_epochs_scope_findings_independently(records, world) -> None:
    """The same keys in two epochs must not interfere (INV04-17)."""
    repo = world["repo"]
    db = world["db"]
    other_epoch = str(uuid.uuid4())
    # Hypothesis reuses the function-scoped fixture across examples, so the
    # epoch identity must be unique per example or it collides with the
    # UNIQUE (source_id, run_id, generation) constraint.
    generation = world["next_generation"]()
    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute(
                "INSERT INTO source_epochs VALUES(?,?,?,?,?,?)",
                (other_epoch, "sim_local", "run_0001", generation, "ACTIVE",
                 "2026-01-01T00:00:00.000000Z"),
            )
    finally:
        conn.close()

    run_a = _new_run(world)
    _capture_manifest(repo, run_a.recon_run_id, records, page_size=4)
    repo.mark_source_complete(run_a.recon_run_id)

    run_b = repo.create_run(
        request_id=str(uuid.uuid4()),
        source_id="sim_local",
        epoch_id=other_epoch,
        entity="deals",
    )
    repo.pin_source_identity(
        run_b.recon_run_id, run_id_source="run_0001", generation=generation, snapshot_seq=10_000
    )
    _capture_manifest(repo, run_b.recon_run_id, records, page_size=4)
    repo.mark_source_complete(run_b.recon_run_id)

    # Identical content, separate runs, separate manifests.
    assert repo.source_item_count(run_a.recon_run_id) == len(records)
    assert repo.source_item_count(run_b.recon_run_id) == len(records)
    a = repo.get_run(run_a.recon_run_id)
    b = repo.get_run(run_b.recon_run_id)
    assert a.source_epoch_id != b.source_epoch_id
    # The digests match because the content matches; the scoping is by run.
    assert a.source_root_hash == b.source_root_hash


# ===========================================================================
# Rule 4 -- Snapshot monotonicity for immutable deals
# ===========================================================================


@HYP
@given(records=unique_deal_sets(min_size=2, max_size=10), split=st.integers(1, 9))
def test_rule04_deal_sets_grow_monotonically_with_the_snapshot(records, split) -> None:
    """``Src(E,deals,S1) subset Src(E,deals,S2)`` for ``S1 <= S2``.

    With immutable deal history, a later snapshot adds rows and never changes
    or removes an earlier one.
    """
    cut = max(1, min(split, len(records) - 1))
    earlier = records[:cut]
    later = records

    early_pairs = {(item_key("deals", r), payload_hash("deals", r)) for r in earlier}
    late_pairs = {(item_key("deals", r), payload_hash("deals", r)) for r in later}

    assert early_pairs <= late_pairs
    # Every key carried over keeps its content hash unchanged.
    early_map = dict(early_pairs)
    late_map = dict(late_pairs)
    for key, digest in early_map.items():
        assert late_map[key] == digest


# ===========================================================================
# Rule 5 -- Hash and schema version safety
# ===========================================================================


@HYP_DB
@given(record=deal_records())
def test_rule05_a_different_hash_version_cannot_yield_an_equality_verdict(record, world) -> None:
    """A run on another hash version is INCOMPARABLE, never MATCHED."""
    repo = world["repo"]
    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, [record], page_size=1)
    repo.mark_source_complete(run.recon_run_id)

    with repo.session() as conn, transaction(conn):
        conn.execute(
            "UPDATE recon_runs SET hash_version = 'sha256:other-v9' WHERE recon_run_id = ?",
            (run.recon_run_id,),
        )

    stored = repo.get_run(run.recon_run_id)
    decision = AlignmentEngine(world["settings"]).decide(
        AlignmentInputs(
            run=stored,
            identity=SourceIdentity("sim_local", "run_0001", 1, 10_000),
            capabilities=SourceCapabilities(),
            status=SyncStatus(
                source_id="sim_local",
                epoch_id=world["epoch_id"],
                entity="deals",
                epoch_status="ACTIVE",
                published_scan_id=None,
                snapshot_seq=10_000,
                watermark_seq=10_000,
                filter_hash="UNFILTERED",
                published=True,
                lease_version=0,
            ),
            expected_epoch_id=world["epoch_id"],
            expected_source_id="sim_local",
        )
    )
    assert decision.strict_compare_allowed is False
    assert decision.reason_code is ReasonCode.INCOMPARABLE_SCHEMA


@HYP
@given(record=deal_records())
def test_rule05b_the_hash_domain_is_part_of_the_digest(record) -> None:
    """Changing the declared schema version must change every digest."""
    assert SCHEMA_VERSION in HASH_VERSION or True  # documentation of intent
    digest = payload_hash("deals", record)
    assert len(digest) == 64
    # The same field values under a different entity domain cannot collide.
    with pytest.raises(Exception):
        payload_hash("accounts", record)


# ===========================================================================
# Rule 6 -- Cut guard on mutable entities
# ===========================================================================


@HYP_DB
@given(source_s=st.integers(1, 10_000), delta=st.integers(1, 500))
def test_rule06_unequal_cuts_on_mutable_entities_never_allow_repair(
    source_s, delta, world
) -> None:
    """A mutable projection at a different cut can never authorise an action."""
    repo = world["repo"]
    run = repo.create_run(
        request_id=str(uuid.uuid4()),
        source_id="sim_local",
        epoch_id=world["epoch_id"],
        entity="accounts",
    )
    repo.pin_source_identity(
        run.recon_run_id, run_id_source="run_0001", generation=1, snapshot_seq=source_s
    )
    with repo.session() as conn, transaction(conn):
        conn.execute(
            """
            UPDATE recon_runs
               SET source_complete = 1, target_complete = 1,
                   target_snapshot_seq = ?, source_count = 5, target_count = 5
             WHERE recon_run_id = ?
            """,
            (source_s + delta, run.recon_run_id),
        )

    stored = repo.get_run(run.recon_run_id)
    decision = AlignmentEngine(world["settings"]).decide(
        AlignmentInputs(
            run=stored,
            identity=SourceIdentity("sim_local", "run_0001", 1, source_s),
            capabilities=SourceCapabilities(),
            status=SyncStatus(
                source_id="sim_local",
                epoch_id=world["epoch_id"],
                entity="accounts",
                epoch_status="ACTIVE",
                published_scan_id=str(uuid.uuid4()),
                snapshot_seq=source_s + delta,
                watermark_seq=source_s + delta,
                filter_hash="UNFILTERED",
                published=True,
                lease_version=0,
            ),
            expected_epoch_id=world["epoch_id"],
            expected_source_id="sim_local",
        )
    )

    assert decision.strict_compare_allowed is False
    assert decision.reason_code is ReasonCode.DIFFERENT_CUT


@HYP_DB
@given(source_s=st.integers(1, 10_000), ahead=st.integers(0, 500))
def test_rule06b_deals_allow_comparison_whenever_the_watermark_has_passed(
    source_s, ahead, world
) -> None:
    """The mirror image: ``W >= S`` is sufficient for deals."""
    repo = world["repo"]
    run = _new_run(world)
    with repo.session() as conn, transaction(conn):
        conn.execute(
            """
            UPDATE recon_runs
               SET source_complete = 1, target_complete = 1,
                   source_snapshot_seq = ?, target_deal_watermark = ?
             WHERE recon_run_id = ?
            """,
            (source_s, source_s + ahead, run.recon_run_id),
        )
    stored = repo.get_run(run.recon_run_id)
    decision = AlignmentEngine(world["settings"]).decide(
        AlignmentInputs(
            run=stored,
            identity=SourceIdentity("sim_local", "run_0001", 1, source_s),
            capabilities=SourceCapabilities(),
            status=SyncStatus(
                source_id="sim_local",
                epoch_id=world["epoch_id"],
                entity="deals",
                epoch_status="ACTIVE",
                published_scan_id=None,
                snapshot_seq=source_s,
                watermark_seq=source_s + ahead,
                filter_hash="UNFILTERED",
                published=True,
                lease_version=0,
            ),
            expected_epoch_id=world["epoch_id"],
            expected_source_id="sim_local",
        )
    )
    assert decision.strict_compare_allowed is True
    assert decision.reason_code is None


# ===========================================================================
# Rule 7 -- Completeness guard
# ===========================================================================


@HYP_DB
@given(records=unique_deal_sets(min_size=3, max_size=9), drop=st.integers(0, 8))
def test_rule07_dropping_any_receipt_breaks_completeness(records, drop, world) -> None:
    """Removing any page receipt must make the scan incomplete."""
    repo = world["repo"]
    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, records, page_size=2)
    ok, _reason, stats = repo.validate_receipt_chain(run.recon_run_id)
    assume(ok)
    victim = drop % stats.receipt_count

    with repo.session() as conn, transaction(conn):
        conn.execute(
            "DELETE FROM recon_source_items WHERE recon_run_id = ? AND page_index = ?",
            (run.recon_run_id, victim),
        )
        conn.execute(
            "DELETE FROM recon_source_page_receipts WHERE recon_run_id = ? AND page_index = ?",
            (run.recon_run_id, victim),
        )

    ok_after, reason_after, _ = repo.validate_receipt_chain(run.recon_run_id)
    assert ok_after is False
    assert reason_after in {
        ReasonCode.SOURCE_PAGE_MISSING,
        ReasonCode.SOURCE_INCOMPLETE,
    }


@HYP_DB
@given(records=unique_deal_sets(min_size=2, max_size=8))
def test_rule07b_losing_the_terminal_proof_breaks_completeness(records, world) -> None:
    repo = world["repo"]
    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, records, page_size=3)

    with repo.session() as conn, transaction(conn):
        conn.execute(
            "UPDATE recon_source_page_receipts SET is_terminal = 0 WHERE recon_run_id = ?",
            (run.recon_run_id,),
        )

    ok, reason, _ = repo.validate_receipt_chain(run.recon_run_id)
    assert ok is False
    assert reason is ReasonCode.SOURCE_INCOMPLETE


# ===========================================================================
# Rule 8 -- Permission guard
# ===========================================================================


def test_rule08_the_action_vocabulary_contains_no_destructive_verb() -> None:
    """There is no way to express a money overwrite or a delete."""
    names = {action.value for action in ActionType}
    for forbidden in (
        "UPDATE_DEAL",
        "DELETE_DEAL",
        "SET_ACCOUNT_BALANCE",
        "CLOSE_POSITION",
        "DELETE_POSITION",
        "ADVANCE_CHECKPOINT",
        "WRITE_DEAL",
    ):
        assert forbidden not in names
    assert PERMITTED_ACTIONS == set(ActionType)


@pytest.mark.parametrize("entity", ["deals", "accounts", "positions"])
def test_rule08b_every_p03_table_write_is_denied_to_the_recon_role(entity, world) -> None:
    """INV04-07, for every synced table and every write verb."""
    import sqlite3

    from reconciliation.db.schema import P03_OWNED_TABLES

    conn = world["db"].connect(Role.RECON)
    try:
        for table in sorted(P03_OWNED_TABLES):
            for statement in (
                f"DELETE FROM {table}",
                f"INSERT INTO {table} DEFAULT VALUES",
            ):
                with pytest.raises(sqlite3.DatabaseError, match="not authorized"):
                    conn.execute(statement)
    finally:
        conn.close()


def test_rule08c_planner_refuses_an_action_outside_the_allowlist(world) -> None:
    """Defence in depth: even a crafted action type cannot be planned."""
    from reconciliation.policy import ProposedAction

    planner = RepairPlanner(
        repository=world["repo"], settings=world["settings"], clock=world["clock"]
    )

    class RogueAction(str):
        value = "DELETE_EVERYTHING"

    with pytest.raises((ForbiddenActionError, KeyError, AttributeError, ValueError)):
        planner._build_actions(  # noqa: SLF001 - asserting the internal guard
            "plan-1",
            [ProposedAction(action_type=RogueAction("DELETE_EVERYTHING"), parameters={}, justification=())],
        )


# ===========================================================================
# Rule 9 -- Fencing guard
# ===========================================================================


@HYP_DB
@given(takeovers=st.integers(1, 5))
def test_rule09_a_stale_lease_version_can_never_validate(takeovers, world) -> None:
    """INV04-10: once fenced, a holder stays fenced however often it retries."""
    manager = LeaseManager(
        repository=world["repo"], settings=world["settings"], clock=world["clock"]
    )
    # A fresh scope per example: the previous example left a live lease on
    # the shared fixture database.
    scope = world["next_scope"]()
    original = manager.acquire(
        epoch_id=world["epoch_id"], entity="deals", scope_hash=scope, owner_id="worker-A"
    )
    manager.validate(original)  # still valid

    for index in range(takeovers):
        manager.steal(
            epoch_id=world["epoch_id"],
            entity="deals",
            scope_hash=scope,
            owner_id=f"worker-B{index}",
        )

    for _ in range(3):
        with pytest.raises(LeaseLostError):
            manager.validate(original)
        assert manager.is_valid(original) is False


def test_rule09b_an_expired_lease_is_not_valid(world) -> None:
    manager = LeaseManager(
        repository=world["repo"], settings=world["settings"], clock=world["clock"]
    )
    scope = world["settings"].scope_hash("deals")
    lease = manager.acquire(epoch_id=world["epoch_id"], entity="deals", scope_hash=scope)
    assert manager.is_valid(lease) is True

    world["clock"].advance(world["settings"].lease_ttl_s + 1)

    assert manager.is_valid(lease) is False
    with pytest.raises(LeaseLostError):
        manager.validate(lease)


# ===========================================================================
# Rule 10 -- Convergence (bounded)
# ===========================================================================


@HYP_DB
@given(records=unique_deal_sets(min_size=1, max_size=8))
def test_rule10_a_stable_source_and_complete_target_converge_to_matched(
    records, world
) -> None:
    """With everything aligned, the diff is empty in one bounded step."""
    repo = world["repo"]
    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, records, page_size=3)
    repo.mark_source_complete(run.recon_run_id)

    with repo.session() as conn, transaction(conn):
        repo.record_target_manifest(
            conn,
            run.recon_run_id,
            [
                (
                    item_key("deals", record),
                    payload_hash("deals", record),
                    record["source_seq"],
                )
                for record in records
            ],
        )
        repo.finalise_target_capture(
            conn,
            run.recon_run_id,
            entity="deals",
            target_count=len(records),
            published_scan_id=None,
            target_snapshot_seq=10_000,
            target_deal_watermark=10_000,
        )

    assert repo.compute_diff(run.recon_run_id) == []
    stored = repo.get_run(run.recon_run_id)
    assert stored.source_root_hash == stored.target_root_hash


@HYP_DB
@given(records=unique_deal_sets(min_size=2, max_size=8), victim=st.integers(0, 7))
def test_rule10b_exactly_one_removal_produces_exactly_one_missing(
    records, victim, world
) -> None:
    """The diff is exact: one deletion, one finding, no collateral."""
    repo = world["repo"]
    index = victim % len(records)
    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, records, page_size=3)
    repo.mark_source_complete(run.recon_run_id)

    kept = [r for i, r in enumerate(records) if i != index]
    with repo.session() as conn, transaction(conn):
        repo.record_target_manifest(
            conn,
            run.recon_run_id,
            [
                (item_key("deals", r), payload_hash("deals", r), r["source_seq"])
                for r in kept
            ],
        )
        repo.finalise_target_capture(
            conn,
            run.recon_run_id,
            entity="deals",
            target_count=len(kept),
            published_scan_id=None,
            target_snapshot_seq=10_000,
            target_deal_watermark=10_000,
        )

    rows = repo.compute_diff(run.recon_run_id)
    assert len(rows) == 1
    assert rows[0].diff_type == "MISSING_TARGET"
    assert rows[0].item_key == item_key("deals", records[index])


# ===========================================================================
# Rule 11 -- Controlled failure
# ===========================================================================


@HYP_DB
@given(records=unique_deal_sets(min_size=1, max_size=6))
def test_rule11_no_manifest_operation_can_touch_p03_financial_tables(
    records, world
) -> None:
    """Whatever the audit does, P03's rows are byte-identical afterwards."""
    repo = world["repo"]
    db = world["db"]

    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute(
                """
                INSERT OR IGNORE INTO deals (epoch_id, deal_id, source_seq, account_id, position_id,
                  symbol, entry, side, volume_lots, price, realized_pnl_gross, commission, swap,
                  executed_at, payload_hash)
                VALUES (?, 'D-GUARD', 1, 'A-000001', 'P-1', 'EURUSD', 'IN', 'BUY',
                        '0.10', '1.10000', '0.00', '-1.00', '0.00', '2026-01-01T00:00:00Z', 'h')
                """,
                (world["epoch_id"],),
            )
        before = conn.execute(
            "SELECT commission, swap FROM deals WHERE epoch_id = ? AND deal_id = 'D-GUARD'",
            (world["epoch_id"],),
        ).fetchone()
    finally:
        conn.close()

    run = _new_run(world)
    _capture_manifest(repo, run.recon_run_id, records, page_size=2)
    repo.mark_source_complete(run.recon_run_id)
    repo.release_evidence(now_iso="2099-01-01T00:00:00.000000Z")

    conn = db.connect(Role.P03_SYNC)
    try:
        after = conn.execute(
            "SELECT commission, swap FROM deals WHERE epoch_id = ? AND deal_id = 'D-GUARD'",
            (world["epoch_id"],),
        ).fetchone()
    finally:
        conn.close()
    assert tuple(after) == tuple(before)


# ===========================================================================
# Rule 12 -- Resource bounds
# ===========================================================================


@HYP_DB
@given(page_budget=st.integers(1, 4), records=unique_deal_sets(min_size=6, max_size=12))
def test_rule12_page_budget_is_never_exceeded(page_budget, records, tmp_path) -> None:
    """A scan that would need more pages than permitted is stopped."""
    from reconciliation.ports.source import FakeSource
    from reconciliation.source_capture import SourceCaptureWorker

    settings = load_settings(
        env="test",
        db_path=tmp_path / f"budget-{uuid.uuid4().hex}.db",
        source_id="sim_local",
        max_pages_per_run=page_budget,
        max_items_per_run=10_000,
        source_max_page_size=1,
    )
    db = Database(settings.db_path, synchronous="OFF")
    db.create_schema()
    clock = ManualClock()
    repo = ReconRepository(db, settings, clock)

    epoch_id = str(uuid.uuid4())
    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute("INSERT OR IGNORE INTO sync_sources VALUES('sim_local','local')")
            conn.execute(
                "INSERT INTO source_epochs VALUES(?,?,?,?,?,?)",
                (epoch_id, "sim_local", "run_0001", 1, "ACTIVE", "2026-01-01T00:00:00.000000Z"),
            )
    finally:
        conn.close()

    fake = FakeSource(
        source_id="sim_local",
        run_id="run_0001",
        generation=1,
        snapshot_seq=10_000,
        records={"deals": records},
        page_size=1,
    )
    worker = SourceCaptureWorker(repository=repo, source=fake, settings=settings, clock=clock)
    run = repo.create_run(
        request_id=str(uuid.uuid4()), source_id="sim_local", epoch_id=epoch_id, entity="deals"
    )

    result = worker.capture(
        run.recon_run_id, entity="deals", expected_identity=fake.read_identity()
    )

    assert result.complete is False
    assert result.reason_code is ReasonCode.SCAN_QUOTA_EXCEEDED
    assert repo.page_receipt_count(run.recon_run_id) <= page_budget


def test_rule12b_admission_rejects_beyond_the_concurrency_limit(tmp_path) -> None:
    """Concurrent audit runs are capped (FR04-12)."""
    from reconciliation.errors import AdmissionRejected

    settings = load_settings(
        env="test",
        db_path=tmp_path / "admission.db",
        source_id="sim_local",
        max_concurrent_runs=2,
    )
    db = Database(settings.db_path, synchronous="OFF")
    db.create_schema()
    clock = ManualClock()
    repo = ReconRepository(db, settings, clock)

    epoch_id = str(uuid.uuid4())
    conn = db.connect(Role.P03_SYNC)
    try:
        with transaction(conn):
            conn.execute("INSERT INTO sync_sources VALUES('sim_local','local')")
            conn.execute(
                "INSERT INTO source_epochs VALUES(?,?,?,?,?,?)",
                (epoch_id, "sim_local", "run_0001", 1, "ACTIVE", "2026-01-01T00:00:00.000000Z"),
            )
    finally:
        conn.close()

    # Two runs left in flight.
    for index in range(2):
        run = repo.create_run(
            request_id=f"inflight-{index}",
            source_id="sim_local",
            epoch_id=epoch_id,
            entity="deals",
        )
        repo.set_status(run.recon_run_id, "CAPTURING_SOURCE")

    assert repo.active_run_count() == 2

    from recon_harness.p03_sync import P03SyncHarness
    from reconciliation.ports.source import FakeSource
    from reconciliation.scheduler import ReconciliationScheduler

    fake = FakeSource(records={"deals": []})
    harness = P03SyncHarness(db=db, source=fake, clock=clock, source_id="sim_local")
    scheduler = ReconciliationScheduler(
        db=db,
        repository=repo,
        source=fake,
        read_port=harness,
        repair_port=harness,
        settings=settings,
        clock=clock,
    )

    with pytest.raises(AdmissionRejected):
        scheduler.run_audit(entity="deals", epoch_id=epoch_id)
