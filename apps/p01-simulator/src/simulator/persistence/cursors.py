"""Signed, opaque snapshot cursors and the snapshot lease registry.

A cursor is a *capability*, not a hint. It states which run, generation,
entity, snapshot sequence and filter a scan belongs to, and it is HMAC-signed
so a client cannot edit any of that. Trusting an unsigned cursor would let a
caller page across runs or widen its own filter (WF-035, WF-047, WF-104).

Snapshot identity is derived, not random: two clients scanning the same
``(run, generation, entity, snapshot_seq, filter)`` share one lease. That
matters for the active-snapshot bound - 100 clients paging the same snapshot
should consume one lease, not 100 - and it means reusing a cursor keeps the
scan alive instead of racing its own TTL (WF-034, WF-043).

TTL is measured on the monotonic clock while the issuing process lives, and
only falls back to the wall clock for a cursor that outlived its process. A
wall-clock-only TTL changes meaning when the host clock steps (WF-086).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..canonical import digest_of, format_timestamp, parse_timestamp
from ..domain.models import (
    InvalidCursorError,
    RunReplacedError,
    SimulatorError,
    SnapshotExpiredError,
)
from ..generation.virtual_clock import Deadline, wall_expired
from ..observability.metrics import Metrics, NullMetrics
from ..settings import Settings

CURSOR_VERSION = "v1"

#: Hard cap before any parsing. A megabyte of base64 should not reach json.
MAX_TOKEN_LENGTH = 4_096


class SnapshotCapacityError(SimulatorError):
    """Too many distinct scans are already in flight.

    Refusing a *new* snapshot is the correct trade: the alternative is evicting
    a snapshot someone is halfway through, which would turn a bounded resource
    problem into silently wrong pagination (WF-043).
    """

    code = "SNAPSHOT_CAPACITY_REACHED"
    retryable = True


@dataclass(frozen=True, slots=True)
class CursorPayload:
    run_id: str
    generation: int
    entity: str
    snapshot_seq: int
    filter_digest: str
    #: The filter *values*, not just their digest.
    #:
    #: Carrying only a digest would make a cursor unusable on its own: the
    #: server could verify a filter the client re-sent but could not rebuild
    #: one the client omitted. Since the token is signed, the embedded values
    #: are as trustworthy as the digest and let `?cursor=...` alone continue a
    #: scan - which is how a connector naturally paginates.
    filters: dict[str, Any]
    last_key: list[Any]
    expires_at: datetime
    snapshot_id: str


def snapshot_identity(
    *, run_id: str, generation: int, entity: str, snapshot_seq: int, filter_digest: str
) -> str:
    """Stable identity for a scan, so identical scans share one lease."""
    return digest_of(
        {
            "run_id": run_id,
            "generation": generation,
            "entity": entity,
            "snapshot_seq": snapshot_seq,
            "filter_digest": filter_digest,
        }
    )[7:39]  # strip the "sha256:" prefix, keep 32 hex characters


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    try:
        return base64.urlsafe_b64decode(text + padding)
    except (ValueError, TypeError) as exc:
        raise InvalidCursorError("cursor is not valid base64url") from exc


class CursorCodec:
    """Encodes and verifies cursor tokens.

    Several keys may be accepted for verification so a signing key can be
    rotated with an overlap of at least one snapshot TTL; only the first is
    used for signing (section 4.2).
    """

    __slots__ = ("_keys", "_active_id")

    def __init__(self, keys: list[tuple[str, str]]) -> None:
        if not keys:
            raise ValueError("at least one signing key is required")
        self._keys = {key_id: key.encode("utf-8") for key_id, key in keys}
        self._active_id = keys[0][0]

    def encode(self, payload: CursorPayload) -> str:
        body = json.dumps(
            {
                "r": payload.run_id,
                "g": payload.generation,
                "e": payload.entity,
                "s": payload.snapshot_seq,
                "f": payload.filter_digest,
                "q": payload.filters,
                "k": payload.last_key,
                "x": format_timestamp(payload.expires_at),
                "i": payload.snapshot_id,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        encoded = _b64encode(body)
        signed_part = f"{CURSOR_VERSION}.{self._active_id}.{encoded}"
        signature = hmac.new(
            self._keys[self._active_id], signed_part.encode("ascii"), hashlib.sha256
        ).digest()
        return f"{signed_part}.{_b64encode(signature)}"

    def decode(self, token: str) -> CursorPayload:
        if not token or len(token) > MAX_TOKEN_LENGTH:
            raise InvalidCursorError(
                "cursor is missing or too long", details={"max_length": MAX_TOKEN_LENGTH}
            )
        parts = token.split(".")
        if len(parts) != 4:
            raise InvalidCursorError("cursor is malformed")
        version, key_id, encoded, signature_b64 = parts
        if version != CURSOR_VERSION:
            raise InvalidCursorError(
                "unsupported cursor version", details={"supported": CURSOR_VERSION}
            )
        key = self._keys.get(key_id)
        if key is None:
            # An unknown key id is indistinguishable from a forged token, and
            # is treated the same way.
            raise InvalidCursorError("cursor was signed with an unknown key")
        expected = hmac.new(key, f"{version}.{key_id}.{encoded}".encode("ascii"), hashlib.sha256)
        if not hmac.compare_digest(expected.digest(), _b64decode(signature_b64)):
            raise InvalidCursorError("cursor signature does not verify")
        try:
            body = json.loads(_b64decode(encoded))
        except (ValueError, UnicodeDecodeError) as exc:
            raise InvalidCursorError("cursor payload is not valid JSON") from exc
        if not isinstance(body, dict):
            raise InvalidCursorError("cursor payload must be an object")
        try:
            last_key = body["k"]
            if not isinstance(last_key, list):
                raise InvalidCursorError("cursor key must be a list")
            filters = body.get("q") or {}
            if not isinstance(filters, dict):
                raise InvalidCursorError("cursor filters must be an object")
            return CursorPayload(
                run_id=str(body["r"]),
                generation=int(body["g"]),
                entity=str(body["e"]),
                snapshot_seq=int(body["s"]),
                filter_digest=str(body["f"]),
                filters=filters,
                last_key=last_key,
                expires_at=parse_timestamp(str(body["x"])),
                snapshot_id=str(body["i"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise InvalidCursorError("cursor payload is incomplete") from exc


@dataclass(slots=True)
class SnapshotLease:
    snapshot_id: str
    run_id: str
    generation: int
    entity: str
    snapshot_seq: int
    filter_digest: str
    deadline: Deadline
    scans: int = 1


class SnapshotRegistry:
    """Tracks live snapshot leases and enforces the active-snapshot bound."""

    def __init__(
        self, settings: Settings, *, metrics: Metrics | NullMetrics | None = None
    ) -> None:
        self._settings = settings
        self._metrics = metrics or NullMetrics()
        self._leases: dict[str, SnapshotLease] = {}

    @property
    def active_count(self) -> int:
        return len(self._leases)

    def collect_expired(self) -> int:
        expired = [
            snapshot_id for snapshot_id, lease in self._leases.items() if lease.deadline.expired()
        ]
        for snapshot_id in expired:
            del self._leases[snapshot_id]
            self._metrics.snapshot_expired.labels(reason="ttl").inc()
        self._metrics.snapshot_active.set(len(self._leases))
        return len(expired)

    def drop_run(self, run_id: str) -> int:
        """Release every lease for a replaced run (WF-038)."""
        stale = [
            snapshot_id
            for snapshot_id, lease in self._leases.items()
            if lease.run_id == run_id
        ]
        for snapshot_id in stale:
            del self._leases[snapshot_id]
            self._metrics.snapshot_expired.labels(reason="run_replaced").inc()
        self._metrics.snapshot_active.set(len(self._leases))
        return len(stale)

    def open(
        self,
        *,
        run_id: str,
        generation: int,
        entity: str,
        snapshot_seq: int,
        filter_digest: str,
    ) -> SnapshotLease:
        snapshot_id = snapshot_identity(
            run_id=run_id,
            generation=generation,
            entity=entity,
            snapshot_seq=snapshot_seq,
            filter_digest=filter_digest,
        )
        existing = self._leases.get(snapshot_id)
        if existing is not None and not existing.deadline.expired():
            # Same scan coordinates: keep the snapshot alive rather than let an
            # in-progress pagination race its own TTL.
            existing.deadline.extend_to(self._settings.snapshot_ttl_sec)
            existing.scans += 1
            return existing

        self.collect_expired()
        if len(self._leases) >= self._settings.max_active_snapshots:
            self._metrics.snapshot_rejected.inc()
            raise SnapshotCapacityError(
                "too many active snapshots; retry after in-flight scans finish",
                details={"max_active_snapshots": self._settings.max_active_snapshots},
            )
        lease = SnapshotLease(
            snapshot_id=snapshot_id,
            run_id=run_id,
            generation=generation,
            entity=entity,
            snapshot_seq=snapshot_seq,
            filter_digest=filter_digest,
            deadline=Deadline.in_seconds(self._settings.snapshot_ttl_sec),
        )
        self._leases[snapshot_id] = lease
        self._metrics.snapshot_active.set(len(self._leases))
        return lease

    def validate(
        self,
        payload: CursorPayload,
        *,
        active_run_id: str,
        active_generation: int,
        entity: str,
        filter_digest: str,
    ) -> SnapshotLease:
        """Check a decoded cursor against current reality.

        The error taxonomy here is the contract a connector depends on:
        ``400 INVALID_CURSOR`` means "you built this wrong, fix the request",
        while ``410`` means "this snapshot is gone, start a new sync". Blurring
        them would make a client retry something that can never succeed.
        """
        if payload.entity != entity:
            raise InvalidCursorError(
                "cursor belongs to a different entity",
                details={"cursor_entity": payload.entity, "requested_entity": entity},
            )
        if payload.filter_digest != filter_digest:
            # WF-036: changing a filter mid-scan would change what the snapshot
            # means, so the cursor is simply invalid for this request.
            raise InvalidCursorError(
                "cursor filter does not match the request filter",
                details={"hint": "start a new scan when changing any filter"},
            )
        if payload.run_id != active_run_id or payload.generation != active_generation:
            raise RunReplacedError(
                "cursor belongs to a run generation that is no longer active",
                details={
                    "cursor_run_id": payload.run_id,
                    "cursor_generation": payload.generation,
                    "active_run_id": active_run_id,
                    "active_generation": active_generation,
                },
            )

        lease = self._leases.get(payload.snapshot_id)
        if lease is not None:
            if lease.deadline.expired():
                del self._leases[payload.snapshot_id]
                self._metrics.snapshot_expired.labels(reason="ttl").inc()
                self._metrics.snapshot_active.set(len(self._leases))
                raise SnapshotExpiredError(
                    "snapshot lease expired; start a new sync",
                    details={"ttl_seconds": self._settings.snapshot_ttl_sec},
                )
            lease.deadline.extend_to(self._settings.snapshot_ttl_sec)
            return lease

        # No in-process lease: this cursor outlived the process that issued it.
        # Version history is still intact, so honour the embedded wall-clock
        # deadline with a bounded skew allowance and re-register the lease.
        if wall_expired(
            payload.expires_at,
            skew_tolerance_seconds=self._settings.cursor_clock_skew_tolerance_sec,
        ):
            self._metrics.snapshot_expired.labels(reason="ttl").inc()
            raise SnapshotExpiredError(
                "snapshot cursor expired; start a new sync",
                details={"expires_at": format_timestamp(payload.expires_at)},
            )
        self.collect_expired()
        if len(self._leases) >= self._settings.max_active_snapshots:
            self._metrics.snapshot_rejected.inc()
            raise SnapshotExpiredError(
                "snapshot can no longer be tracked; start a new sync",
                details={"reason": "snapshot_capacity"},
            )
        revived = SnapshotLease(
            snapshot_id=payload.snapshot_id,
            run_id=payload.run_id,
            generation=payload.generation,
            entity=payload.entity,
            snapshot_seq=payload.snapshot_seq,
            filter_digest=payload.filter_digest,
            deadline=Deadline.in_seconds(self._settings.snapshot_ttl_sec),
        )
        self._leases[payload.snapshot_id] = revived
        self._metrics.snapshot_active.set(len(self._leases))
        return revived

    def issue(
        self,
        codec: CursorCodec,
        lease: SnapshotLease,
        last_key: list[Any],
        filters: dict[str, Any],
    ) -> str:
        return codec.encode(
            CursorPayload(
                run_id=lease.run_id,
                generation=lease.generation,
                entity=lease.entity,
                snapshot_seq=lease.snapshot_seq,
                filter_digest=lease.filter_digest,
                filters=filters,
                last_key=last_key,
                expires_at=lease.deadline.wall_at,
                snapshot_id=lease.snapshot_id,
            )
        )
