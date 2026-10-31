"""Contract tests: P01 wire semantics and the P03 port surface (§2.1, §2.2).

These exist so that a change in Project 01's contract breaks *here*, loudly,
with a message naming the assumption that moved — rather than surfacing later
as a mysterious reconciliation result.

Every assertion corresponds to a documented semantic this service depends on:

* every list page carries ``run_id``, ``generation``, ``snapshot_seq``;
* ``snapshot_seq`` is stable across all pages of one scan;
* ``next_cursor=null`` ends *this snapshot*, not all future events;
* ``after_seq`` is **exclusive**, so ``after_seq=0`` includes sequence 1;
* money and volumes arrive as decimal **strings**, never JSON numbers;
* ``410 SNAPSHOT_EXPIRED`` is terminal for a scan and never auto-resumed;
* cursors are opaque and are never combined with fresh filters.

The P03 section pins the two ports Project 04 is allowed to use and asserts
the properties the audit relies on: the watermark advances only after a
terminal page, publications are atomic, and intents dedup on ``intent_id``.
"""

from __future__ import annotations

import pytest

from reconciliation.errors import ReasonCode
from reconciliation.ports.source import cursor_digest

pytestmark = [pytest.mark.contract]


# ---------------------------------------------------------------------------
# P01: identity and capabilities
# ---------------------------------------------------------------------------


def test_meta_reports_identity_and_declared_limits(p01_admin, source) -> None:
    p01_admin.create_run(accounts=3, deals=12, max_open_positions=3)

    identity = source.read_identity()
    capabilities = source.read_capabilities()

    assert identity.source_id == "sim_local"
    assert identity.run_id and identity.run_id.startswith("run_")
    assert identity.generation >= 1
    assert identity.snapshot_seq > 0
    assert identity.deal_count == 12
    assert identity.account_count == 3

    # The limits this service plans its budgets around.
    assert 1 <= capabilities.max_page_size <= 500
    assert capabilities.snapshot_ttl_seconds > 0
    assert capabilities.after_seq_is_exclusive is True


def test_capabilities_do_not_claim_a_global_cross_entity_cut(source, p01_admin) -> None:
    """Section 0.4: a global Account+Position+Deal cut is never assumed."""
    p01_admin.create_run(accounts=2, deals=5, max_open_positions=2)
    assert source.read_capabilities().global_cut_across_entities is False


def test_simulator_specific_capabilities_are_present(source, p01_admin) -> None:
    """The strict-compare path depends on these three being real."""
    p01_admin.create_run(accounts=2, deals=5, max_open_positions=2)
    capabilities = source.read_capabilities()
    assert capabilities.stable_snapshot_pagination is True
    assert capabilities.durable_epoch_identity is True
    assert capabilities.complete_deal_history is True


def test_a_source_with_no_active_run_cannot_be_audited(source) -> None:
    """No run means no identity, so there is nothing to compare against."""
    from reconciliation.errors import SourceError

    with pytest.raises(SourceError) as exc:
        source.read_identity()
    assert exc.value.reason is ReasonCode.UNSUPPORTED_CAPABILITY


# ---------------------------------------------------------------------------
# P01: pagination semantics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("entity", ["accounts", "positions", "deals"])
def test_every_page_carries_provenance_and_one_stable_cut(
    p01_admin, source, entity: str
) -> None:
    p01_admin.create_run(accounts=8, deals=40, max_open_positions=8)

    pages = list(source.iter_pages(entity, page_size=3))

    assert pages, f"{entity} produced no pages"
    first = pages[0]
    for page in pages:
        assert page.run_id == first.run_id
        assert page.generation == first.generation
        # The invariant the whole manifest rests on: one cut per scan.
        assert page.snapshot_seq == first.snapshot_seq
        assert len(page.items) <= 3

    # Exactly one terminal page, and it is the last.
    assert [index for index, page in enumerate(pages) if page.is_terminal] == [len(pages) - 1]
    assert pages[-1].next_cursor is None
    for page in pages[:-1]:
        assert page.next_cursor is not None


def test_cursors_are_opaque_distinct_and_never_repeat(p01_admin, source) -> None:
    """A repeated cursor would be a cycle; the scan must make progress."""
    p01_admin.create_run(accounts=4, deals=30, max_open_positions=4)

    cursors = [
        page.next_cursor for page in source.iter_pages("deals", page_size=4) if page.next_cursor
    ]

    assert len(cursors) >= 2
    digests = [cursor_digest(cursor) for cursor in cursors]
    assert len(set(digests)) == len(digests), "a cursor was re-issued within one scan"
    for cursor in cursors:
        assert not cursor.isdigit(), "cursor must be opaque, not a constructible offset"


def test_after_seq_is_exclusive_so_zero_includes_the_first_deal(
    p01_admin, p01_client
) -> None:
    """``after_seq=0`` must include sequence 1, or the audit misses a deal."""
    p01_admin.create_run(accounts=2, deals=10, max_open_positions=2)

    response = p01_client.get("/v1/deals", params={"after_seq": 0, "limit": 100})
    assert response.status_code == 200
    everything = response.json()["items"]
    assert everything, "fixture produced no deals"
    lowest = min(int(row["source_seq"]) for row in everything)

    excluded = p01_client.get("/v1/deals", params={"after_seq": lowest, "limit": 100}).json()
    excluded_seqs = {int(row["source_seq"]) for row in excluded["items"]}

    assert lowest not in excluded_seqs, "after_seq must be exclusive"
    assert len(excluded["items"]) == len(everything) - 1


def test_deal_sequences_are_ascending_but_need_not_be_contiguous(
    p01_admin, source
) -> None:
    """Section 2.1: a gap in ``source_seq`` is normal, not missing data.

    Non-deal events consume sequence numbers too, so inferring loss from a gap
    would produce a permanent false positive. Recorded here so nobody later
    "fixes" the gap.
    """
    p01_admin.create_run(accounts=4, deals=25, max_open_positions=4)

    sequences: list[int] = []
    for page in source.iter_pages("deals", page_size=5):
        sequences.extend(int(item["source_seq"]) for item in page.items)

    assert sequences == sorted(sequences)
    assert len(set(sequences)) == len(sequences)
    assert sequences[-1] >= len(sequences)


def test_page_size_is_capped_at_the_declared_maximum(p01_admin, p01_client) -> None:
    p01_admin.create_run(accounts=4, deals=40, max_open_positions=4)
    response = p01_client.get("/v1/deals", params={"after_seq": 0, "limit": 100_000})
    # Either the request is refused or the page is clamped; both are safe.
    if response.status_code == 200:
        assert len(response.json()["items"]) <= 500
    else:
        assert response.status_code in (400, 422)


# ---------------------------------------------------------------------------
# P01: wire types
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("entity", "money_fields"),
    [
        ("accounts", ("balance", "credit")),
        ("positions", ("volume_lots", "open_price")),
        ("deals", ("realized_pnl_gross", "commission", "swap", "price", "volume_lots")),
    ],
)
def test_money_and_quantities_arrive_as_strings(
    p01_admin, p01_client, entity: str, money_fields: tuple[str, ...]
) -> None:
    """A JSON number here would reintroduce binary floating point.

    This is the one wire property the canonical layer cannot defend against on
    its own: if the source sends ``0.1`` as a double, the exact decimal is
    already lost before this service sees it.
    """
    p01_admin.create_run(accounts=3, deals=12, max_open_positions=3)

    params = {"limit": 5} if entity != "deals" else {"limit": 5, "after_seq": 0}
    items = p01_client.get(f"/v1/{entity}", params=params).json()["items"]
    assert items, f"{entity} returned no rows"

    for row in items:
        for field in money_fields:
            if row.get(field) is None:
                continue
            assert isinstance(row[field], str), (
                f"{entity}.{field} arrived as {type(row[field]).__name__}, not a string"
            )


def test_identifiers_arrive_as_strings(p01_admin, p01_client) -> None:
    """Numeric ids would lose leading zeros (F-13)."""
    p01_admin.create_run(accounts=3, deals=10, max_open_positions=3)
    items = p01_client.get("/v1/deals", params={"after_seq": 0, "limit": 5}).json()["items"]
    assert items
    for row in items:
        for field in ("deal_id", "account_id", "position_id"):
            assert isinstance(row[field], str)


def test_timestamps_are_timezone_qualified(p01_admin, p01_client) -> None:
    """A naive timestamp has no instant and cannot be compared."""
    p01_admin.create_run(accounts=2, deals=8, max_open_positions=2)
    items = p01_client.get("/v1/deals", params={"after_seq": 0, "limit": 5}).json()["items"]
    assert items
    for row in items:
        assert row["executed_at"].endswith("Z") or "+" in row["executed_at"]


# ---------------------------------------------------------------------------
# P01: error semantics
# ---------------------------------------------------------------------------


class TestSnapshotExpiry:
    """A one-second TTL makes the 410 genuine rather than simulated."""

    @pytest.fixture
    def p01_ttl_seconds(self) -> int:
        return 1

    def test_expired_cursor_returns_410_and_is_not_resumed(
        self, p01_admin, p01_client, source
    ) -> None:
        import time

        from reconciliation.errors import SnapshotExpiredError

        p01_admin.create_run(accounts=3, deals=25, max_open_positions=3)
        first = p01_client.get("/v1/deals", params={"after_seq": 0, "limit": 3}).json()
        cursor = first["next_cursor"]
        assert cursor is not None

        time.sleep(1.4)

        raw = p01_client.get("/v1/deals", params={"cursor": cursor})
        assert raw.status_code == 410, raw.text
        assert raw.json()["error"]["code"] == "SNAPSHOT_EXPIRED"

        # And the port maps it to a terminal, non-retryable error.
        pages = source.iter_pages("deals", page_size=3)
        next(pages)
        time.sleep(1.4)
        with pytest.raises(SnapshotExpiredError):
            next(pages)


def test_unauthenticated_requests_are_refused(p01_env) -> None:
    """The data plane is not open; the audit must present a token."""
    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        from starlette.testclient import TestClient

    from simulator.main import create_app

    with TestClient(create_app()) as anonymous:
        response = anonymous.get("/v1/meta")
    assert response.status_code in (401, 403)


# ---------------------------------------------------------------------------
# P03 read port (§2.2)
# ---------------------------------------------------------------------------


def test_sync_status_reports_everything_the_audit_needs(synced_world, harness) -> None:
    status = harness.sync_status("sim_local", synced_world.epoch_id, "deals")

    assert status.epoch_id == synced_world.epoch_id
    assert status.epoch_status == "ACTIVE"
    assert status.filter_hash == "UNFILTERED"
    assert status.published is True
    assert status.watermark_seq >= synced_world.snapshot_seq
    assert status.lease_version >= 0


def test_watermark_advances_only_after_a_terminal_page(
    p01_admin, harness, db, source, clock
) -> None:
    """INV04-04's precondition: a partial scan leaves W where it was."""
    from recon_harness.p03_sync import P03SyncHarness

    p01_admin.create_run(accounts=3, deals=30, max_open_positions=3)
    epoch_id = harness.bootstrap_epoch()
    assert harness.deal_watermark(epoch_id) == 0

    partial = P03SyncHarness(
        db=db, source=source, clock=clock, source_id="sim_local", page_size=5
    )
    partial.sync_deals(epoch_id, max_pages=1, advance_watermark=False)
    assert harness.deal_watermark(epoch_id) == 0, "a partial scan must not move W"

    harness.sync_deals(epoch_id)
    assert harness.deal_watermark(epoch_id) > 0


def test_publication_is_atomic_and_moves_the_published_scan_id(
    synced_world, harness, db
) -> None:
    from reconciliation.db.engine import Role

    before = harness.sync_status("sim_local", synced_world.epoch_id, "accounts")
    new_scan = harness.publish_snapshot(synced_world.epoch_id, "accounts")
    after = harness.sync_status("sim_local", synced_world.epoch_id, "accounts")

    assert new_scan != before.published_scan_id
    assert after.published_scan_id == new_scan

    # Every current row belongs to exactly one scan: no mixture survives.
    conn = db.connect(Role.RECON)
    try:
        scans = {
            row["published_scan_id"]
            for row in conn.execute(
                "SELECT DISTINCT published_scan_id FROM accounts_current WHERE epoch_id = ?",
                (synced_world.epoch_id,),
            )
        }
    finally:
        conn.close()
    assert scans == {new_scan}


def test_source_capability_profile_is_exposed(synced_world, harness) -> None:
    profile = harness.source_capability_profile("sim_local")
    assert profile["driver"] == "p01_simulator"
    assert profile["stable_snapshot_pagination"] is True
    assert profile["global_cut_across_entities"] is False


# ---------------------------------------------------------------------------
# P03 repair port (§2.2, §7.5)
# ---------------------------------------------------------------------------


def test_unknown_intent_has_no_receipt(harness) -> None:
    """``None`` is the signal that a same-id retry is safe (§7.6)."""
    assert harness.repair_intent_status("never-issued") is None


def test_intent_is_deduplicated_and_executes_once(synced_world, harness) -> None:
    """INV04-08 at the port, independent of the executor."""
    status = harness.sync_status("sim_local", synced_world.epoch_id, "accounts")
    kwargs = dict(
        intent_id="contract-intent-1",
        source_id="sim_local",
        entity="accounts",
        expected_epoch_id=synced_world.epoch_id,
        expected_revision=status.published_scan_id,
        expected_lease_version=None,
        approved_plan_hash="h" * 64,
        parameters={"entity": "accounts", "scope": "UNFILTERED"},
    )

    first = harness.request_full_resync(**kwargs)
    second = harness.request_full_resync(**kwargs)

    assert first.is_committed
    assert first.execution_count == 1
    assert second.execution_count == 1, "a replayed intent must not execute again"
    assert second.receipt_hash == first.receipt_hash


def test_intent_with_a_stale_revision_is_rejected_without_executing(
    synced_world, harness
) -> None:
    """INV04-19: preconditions are re-verified at P03's own commit boundary."""
    receipt = harness.request_full_resync(
        intent_id="contract-intent-stale",
        source_id="sim_local",
        entity="accounts",
        expected_epoch_id=synced_world.epoch_id,
        expected_revision="a-scan-that-never-existed",
        expected_lease_version=None,
        approved_plan_hash="h" * 64,
        parameters={"entity": "accounts"},
    )

    assert receipt.status == "REJECTED"
    assert receipt.reason_code == "REVISION_STALE"
    assert receipt.execution_count == 0


def test_intent_for_a_non_active_epoch_is_rejected(synced_world, harness) -> None:
    harness.quarantine_epoch(synced_world.epoch_id)
    receipt = harness.request_full_resync(
        intent_id="contract-intent-quarantined",
        source_id="sim_local",
        entity="accounts",
        expected_epoch_id=synced_world.epoch_id,
        expected_revision=None,
        expected_lease_version=None,
        approved_plan_hash="h" * 64,
        parameters={"entity": "accounts"},
    )
    assert receipt.status == "REJECTED"
    assert receipt.reason_code == "EPOCH_NOT_ACTIVE"
    assert receipt.execution_count == 0


def test_deal_replay_never_rewrites_an_existing_deal(synced_world, harness, db) -> None:
    """Deal history is immutable: a replay may add, never update (INV04-05)."""
    from reconciliation.db.engine import Role

    def snapshot() -> dict[str, tuple]:
        conn = db.connect(Role.RECON)
        try:
            return {
                row["deal_id"]: (
                    row["commission"],
                    row["swap"],
                    row["realized_pnl_gross"],
                )
                for row in conn.execute(
                    """
                    SELECT deal_id, commission, swap, realized_pnl_gross
                      FROM deals WHERE epoch_id = ?
                    """,
                    (synced_world.epoch_id,),
                )
            }
        finally:
            conn.close()

    before = snapshot()
    assert before

    harness.sync_deals(synced_world.epoch_id)  # a full replay

    assert snapshot() == before, "a replay must not alter any existing deal's money"


def test_p03_stores_its_own_hash_scheme_distinct_from_p04(synced_world, db) -> None:
    """Section 6.4: the two hashes must not be assumed interchangeable.

    The harness uses a different scheme on purpose, so any code that shortcuts
    to comparing stored hashes fails instead of passing by luck.
    """
    from reconciliation.canonical import payload_hash
    from reconciliation.db.engine import Role

    conn = db.connect(Role.RECON)
    try:
        row = conn.execute(
            """
            SELECT account_id, currency, balance, credit, payload_hash
              FROM accounts_current WHERE epoch_id = ? LIMIT 1
            """,
            (synced_world.epoch_id,),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None

    p04_hash = payload_hash(
        "accounts",
        {
            "account_id": row["account_id"],
            "currency": row["currency"],
            "balance": row["balance"],
            "credit": row["credit"],
        },
    )
    assert row["payload_hash"] != p04_hash
