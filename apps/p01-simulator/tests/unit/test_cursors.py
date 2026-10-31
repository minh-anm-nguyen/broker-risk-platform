"""Cursor signing, tamper detection, and snapshot lease behaviour.

A cursor is a capability: it decides which run, snapshot and filter a scan may
read. So the tests here are mostly adversarial - what happens when a client
edits one, replays an expired one, or brings one from another run.

The error taxonomy is as important as the rejection itself. ``400`` tells a
client to fix its request; ``410`` tells it to start a new sync. A client that
cannot tell them apart will either retry forever or resync for no reason.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from simulator.canonical import filter_digest
from simulator.domain.models import (
    InvalidCursorError,
    RunReplacedError,
    SnapshotExpiredError,
)
from simulator.generation.virtual_clock import Deadline
from simulator.persistence.cursors import (
    CursorCodec,
    CursorPayload,
    SnapshotCapacityError,
    SnapshotRegistry,
    snapshot_identity,
)
from tests.conftest import make_settings

pytestmark = pytest.mark.unit

KEYS = [("k1", "primary-signing-key")]
FILTERS = {"account_id": "A-000001", "limit": 100}


def make_payload(**overrides) -> CursorPayload:
    base = {
        "run_id": "run_0001",
        "generation": 7,
        "entity": "deals",
        "snapshot_seq": 1500,
        "filter_digest": filter_digest(FILTERS),
        "filters": dict(FILTERS),
        "last_key": [1200, "D-00001200"],
        "expires_at": datetime(2026, 10, 30, 6, 0, 0, tzinfo=UTC),
        "snapshot_id": "a" * 32,
    }
    base.update(overrides)
    return CursorPayload(**base)


def test_round_trip_preserves_every_field() -> None:
    codec = CursorCodec(KEYS)
    original = make_payload()
    decoded = codec.decode(codec.encode(original))
    assert decoded == original


def test_token_is_opaque_and_url_safe() -> None:
    """A client must not be able to read, let alone edit, the contents."""
    token = CursorCodec(KEYS).encode(make_payload())
    assert "run_0001" not in token
    assert "A-000001" not in token
    assert "=" not in token  # base64url padding stripped
    assert all(character.isalnum() or character in "-_." for character in token)


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(lambda token: token[:-1] + ("A" if token[-1] != "A" else "B"), id="signature"),
        pytest.param(lambda token: token.replace(".", "x", 1), id="structure"),
        pytest.param(lambda token: "v2" + token[2:], id="version"),
        pytest.param(lambda token: token.split(".")[0] + ".k9." + ".".join(token.split(".")[2:]), id="key-id"),
    ],
)
def test_tampered_tokens_are_rejected(mutate) -> None:
    """WF-035: one edited byte must invalidate the whole token."""
    codec = CursorCodec(KEYS)
    token = codec.encode(make_payload())
    with pytest.raises(InvalidCursorError):
        codec.decode(mutate(token))


def test_payload_edited_without_resigning_is_rejected() -> None:
    """The realistic attack: decode, change the filter, re-encode the body."""
    import base64
    import json

    codec = CursorCodec(KEYS)
    version, key_id, body, signature = codec.encode(make_payload()).split(".")
    decoded = json.loads(base64.urlsafe_b64decode(body + "=" * (-len(body) % 4)))
    decoded["q"] = {"account_id": "A-000999", "limit": 100}  # widen access
    forged = base64.urlsafe_b64encode(
        json.dumps(decoded, separators=(",", ":"), sort_keys=True).encode()
    ).rstrip(b"=").decode()
    with pytest.raises(InvalidCursorError):
        codec.decode(f"{version}.{key_id}.{forged}.{signature}")


@pytest.mark.parametrize("token", ["", "a", "a.b.c", "a.b.c.d.e", "x" * 5000])
def test_malformed_tokens_are_rejected_without_parsing(token: str) -> None:
    with pytest.raises(InvalidCursorError):
        CursorCodec(KEYS).decode(token)


def test_a_rotated_key_still_verifies_old_tokens() -> None:
    """Rotation needs an overlap of at least one snapshot TTL (section 4.2)."""
    old_codec = CursorCodec([("k1", "old-key")])
    token = old_codec.encode(make_payload())
    rotated = CursorCodec([("k2", "new-key"), ("k1", "old-key")])
    assert rotated.decode(token).run_id == "run_0001"
    # The new key is the one used for signing.
    assert rotated.encode(make_payload()).split(".")[1] == "k2"
    # Once the old key is dropped, the old token is indistinguishable from a forgery.
    with pytest.raises(InvalidCursorError):
        CursorCodec([("k2", "new-key")]).decode(token)


def test_snapshot_identity_is_derived_not_random() -> None:
    """Identical scans must share one lease, or the budget is per client."""
    arguments = {
        "run_id": "run_0001",
        "generation": 1,
        "entity": "deals",
        "snapshot_seq": 100,
        "filter_digest": "sha256:abc",
    }
    assert snapshot_identity(**arguments) == snapshot_identity(**arguments)
    assert snapshot_identity(**{**arguments, "snapshot_seq": 101}) != snapshot_identity(**arguments)
    assert snapshot_identity(**{**arguments, "generation": 2}) != snapshot_identity(**arguments)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@pytest.fixture
def registry(tmp_path: Path) -> SnapshotRegistry:
    return SnapshotRegistry(make_settings(tmp_path, max_active_snapshots=3, snapshot_ttl_sec=900))


def open_lease(registry: SnapshotRegistry, seq: int = 100, entity: str = "deals"):
    return registry.open(
        run_id="run_0001",
        generation=1,
        entity=entity,
        snapshot_seq=seq,
        filter_digest=filter_digest({"limit": 100}),
    )


def test_identical_scans_share_one_lease(registry: SnapshotRegistry) -> None:
    first = open_lease(registry)
    second = open_lease(registry)
    assert first.snapshot_id == second.snapshot_id
    assert registry.active_count == 1
    assert second.scans == 2


def test_distinct_snapshots_consume_distinct_leases(registry: SnapshotRegistry) -> None:
    open_lease(registry, seq=100)
    open_lease(registry, seq=200)
    open_lease(registry, seq=300)
    assert registry.active_count == 3


def test_exceeding_the_snapshot_budget_refuses_the_new_scan(registry: SnapshotRegistry) -> None:
    """WF-043: refuse the newcomer, never evict a scan already in progress."""
    leases = [open_lease(registry, seq=seq) for seq in (100, 200, 300)]
    with pytest.raises(SnapshotCapacityError) as excinfo:
        open_lease(registry, seq=400)
    assert excinfo.value.details["max_active_snapshots"] == 3
    # Existing leases are untouched and still usable.
    assert registry.active_count == 3
    for lease in leases:
        assert not lease.deadline.expired()


def test_expired_leases_are_collected_to_make_room(registry: SnapshotRegistry) -> None:
    """The budget counts live scans, not historical ones."""
    leases = [open_lease(registry, seq=seq) for seq in (100, 200, 300)]
    assert registry.active_count == 3
    for lease in leases:
        lease.deadline.monotonic_at = time.monotonic() - 1  # force expiry
    # A fourth scan would exceed the budget, but the three expired leases are
    # collected first, so it is admitted rather than refused.
    fresh = open_lease(registry, seq=400)
    assert fresh.snapshot_seq == 400
    assert registry.active_count == 1


def test_dropping_a_run_releases_its_leases(registry: SnapshotRegistry) -> None:
    """WF-038: a replaced generation's leases can never be used again."""
    open_lease(registry, seq=100)
    open_lease(registry, seq=200)
    assert registry.drop_run("run_0001") == 2
    assert registry.active_count == 0


def test_validate_rejects_a_different_entity(registry: SnapshotRegistry) -> None:
    lease = open_lease(registry, entity="deals")
    payload = make_payload(
        entity="deals",
        snapshot_id=lease.snapshot_id,
        filter_digest=lease.filter_digest,
        run_id="run_0001",
        generation=1,
    )
    with pytest.raises(InvalidCursorError):
        registry.validate(
            payload,
            active_run_id="run_0001",
            active_generation=1,
            entity="positions",
            filter_digest=lease.filter_digest,
        )


def test_validate_rejects_a_changed_filter(registry: SnapshotRegistry) -> None:
    """WF-036: 400, because the request is wrong, not the snapshot."""
    lease = open_lease(registry)
    payload = make_payload(
        entity="deals", snapshot_id=lease.snapshot_id, filter_digest=lease.filter_digest,
        run_id="run_0001", generation=1,
    )
    with pytest.raises(InvalidCursorError):
        registry.validate(
            payload,
            active_run_id="run_0001",
            active_generation=1,
            entity="deals",
            filter_digest="sha256:a-different-filter",
        )


def test_validate_reports_a_replaced_generation_as_410(registry: SnapshotRegistry) -> None:
    """WF-038: this snapshot is gone, so the client must start a new sync."""
    lease = open_lease(registry)
    payload = make_payload(
        entity="deals", snapshot_id=lease.snapshot_id, filter_digest=lease.filter_digest,
        run_id="run_0001", generation=1,
    )
    with pytest.raises(RunReplacedError):
        registry.validate(
            payload,
            active_run_id="run_0002",
            active_generation=2,
            entity="deals",
            filter_digest=lease.filter_digest,
        )


def test_validate_reports_an_expired_lease_as_410(registry: SnapshotRegistry) -> None:
    """WF-037."""
    lease = open_lease(registry)
    lease.deadline.monotonic_at = time.monotonic() - 1
    payload = make_payload(
        entity="deals", snapshot_id=lease.snapshot_id, filter_digest=lease.filter_digest,
        run_id="run_0001", generation=1,
    )
    with pytest.raises(SnapshotExpiredError):
        registry.validate(
            payload,
            active_run_id="run_0001",
            active_generation=1,
            entity="deals",
            filter_digest=lease.filter_digest,
        )


def test_reusing_a_cursor_extends_its_lease(registry: SnapshotRegistry) -> None:
    """An in-progress scan should not race its own TTL (WF-034, WF-044)."""
    lease = open_lease(registry)
    lease.deadline.monotonic_at = time.monotonic() + 1  # nearly expired
    payload = make_payload(
        entity="deals", snapshot_id=lease.snapshot_id, filter_digest=lease.filter_digest,
        run_id="run_0001", generation=1,
    )
    revived = registry.validate(
        payload,
        active_run_id="run_0001",
        active_generation=1,
        entity="deals",
        filter_digest=lease.filter_digest,
    )
    assert revived.deadline.remaining_seconds() > 100


def test_a_cursor_that_outlived_its_process_is_honoured_within_skew(tmp_path: Path) -> None:
    """Section 4.2 / WF-086: no in-process lease, so the wall deadline rules."""
    settings = make_settings(tmp_path, cursor_clock_skew_tolerance_sec=60)
    registry = SnapshotRegistry(settings)
    digest = filter_digest({"limit": 100})
    payload = make_payload(
        run_id="run_0001",
        generation=1,
        entity="deals",
        filter_digest=digest,
        filters={"limit": 100},
        snapshot_id="b" * 32,
        expires_at=datetime.now(UTC) + timedelta(seconds=300),
    )
    assert registry.active_count == 0
    revived = registry.validate(
        payload,
        active_run_id="run_0001",
        active_generation=1,
        entity="deals",
        filter_digest=digest,
    )
    assert revived.snapshot_id == payload.snapshot_id
    assert registry.active_count == 1


def test_a_cursor_past_its_wall_deadline_and_skew_is_expired(tmp_path: Path) -> None:
    settings = make_settings(tmp_path, cursor_clock_skew_tolerance_sec=5)
    registry = SnapshotRegistry(settings)
    digest = filter_digest({"limit": 100})
    payload = make_payload(
        filter_digest=digest,
        filters={"limit": 100},
        run_id="run_0001",
        generation=1,
        entity="deals",
        snapshot_id="c" * 32,
        expires_at=datetime.now(UTC) - timedelta(seconds=60),
    )
    with pytest.raises(SnapshotExpiredError):
        registry.validate(
            payload,
            active_run_id="run_0001",
            active_generation=1,
            entity="deals",
            filter_digest=digest,
        )


def test_deadline_uses_the_monotonic_clock() -> None:
    """A wall-clock TTL changes meaning when the host clock steps (WF-086)."""
    deadline = Deadline.in_seconds(10)
    assert not deadline.expired()
    assert 9 < deadline.remaining_seconds() <= 10
    deadline.monotonic_at = time.monotonic() - 0.001
    assert deadline.expired()
    deadline.extend_to(30)
    assert not deadline.expired()
