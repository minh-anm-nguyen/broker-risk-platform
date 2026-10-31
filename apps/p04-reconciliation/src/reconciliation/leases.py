"""Scheduler leases and fencing (sections 3.3, 6.2, INV04-10).

Two reconciliation workers must not audit the same scope at once, and -- more
importantly -- a worker that *lost* its lease must not be able to finish what
it started. A paused worker that wakes up after a takeover and dispatches its
old plan is exactly the scenario IT04-04 covers.

The mechanism is a monotonic ``lease_version`` guarded by compare-and-set:

* ``acquire`` takes the lease only if it is free or expired, and bumps the
  version. The bump is what fences the previous holder.
* ``validate`` is called before any state-changing step. It compares the
  caller's ``(token, version)`` against the stored row, so a stale holder is
  told it lost rather than discovering it by corrupting something.
* ``renew`` extends the deadline only for the current holder at the current
  version.

This lease is **independent of Project 03's sync lease** (section 6.2):
holding P04's audit lease grants no right to advance P03's checkpoint, and P03
re-verifies its own fencing at its own commit boundary (INV04-19). Two
independent guards is deliberate -- a single shared lease would make an audit
pause able to stall the sync service.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

from .clock import Clock, from_iso, to_iso
from .config import Settings
from .db.engine import transaction
from .db.repository import ReconRepository
from .errors import LeaseLostError, ReasonCode


@dataclass(frozen=True, slots=True)
class Lease:
    source_epoch_id: str
    entity: str
    scope_hash: str
    token: str
    version: int
    owner_id: str
    until: str


class LeaseManager:
    """Compare-and-set fencing over ``recon_scheduler_leases``."""

    def __init__(self, *, repository: ReconRepository, settings: Settings, clock: Clock) -> None:
        self._repo = repository
        self._settings = settings
        self._clock = clock

    def acquire(self, *, epoch_id: str, entity: str, scope_hash: str, owner_id: str | None = None) -> Lease:
        """Take the lease, or raise if a live holder owns it.

        The expiry comparison uses the injected clock, so a test can expire a
        lease by moving time rather than by waiting (``ManualClock``).
        """
        owner = owner_id or self._settings.worker_id
        now = self._clock.now()
        until = now + timedelta(seconds=self._settings.lease_ttl_s)

        with self._repo.session() as conn, transaction(conn):
            conn.execute(
                """
                INSERT OR IGNORE INTO recon_scheduler_leases
                  (source_epoch_id, entity, scope_hash, lease_version)
                VALUES (?,?,?,0)
                """,
                (epoch_id, entity, scope_hash),
            )
            row = conn.execute(
                """
                SELECT lease_token, owner_id, lease_version, lease_until
                  FROM recon_scheduler_leases
                 WHERE source_epoch_id = ? AND entity = ? AND scope_hash = ?
                """,
                (epoch_id, entity, scope_hash),
            ).fetchone()

            held = row["lease_token"] is not None
            live = held and row["lease_until"] is not None and from_iso(row["lease_until"]) > now
            if live and row["owner_id"] != owner:
                raise LeaseLostError(
                    "another worker holds a live audit lease for this scope",
                    reason=ReasonCode.ADMISSION_REJECTED,
                    epoch_id=epoch_id,
                    entity=entity,
                    holder=row["owner_id"],
                    lease_version=int(row["lease_version"]),
                )

            token = str(uuid.uuid4())
            version = int(row["lease_version"]) + 1
            conn.execute(
                """
                UPDATE recon_scheduler_leases
                   SET lease_token = ?, owner_id = ?, lease_version = ?, lease_until = ?
                 WHERE source_epoch_id = ? AND entity = ? AND scope_hash = ?
                """,
                (token, owner, version, to_iso(until), epoch_id, entity, scope_hash),
            )
            return Lease(
                source_epoch_id=epoch_id,
                entity=entity,
                scope_hash=scope_hash,
                token=token,
                version=version,
                owner_id=owner,
                until=to_iso(until),
            )

    def steal(self, *, epoch_id: str, entity: str, scope_hash: str, owner_id: str) -> Lease:
        """Take the lease regardless of a live holder (takeover drill).

        Used by the two-worker tests and by the operator runbook for a worker
        confirmed dead. The version still increments, so the displaced holder
        is fenced exactly as it would be after a natural expiry.
        """
        now = self._clock.now()
        until = now + timedelta(seconds=self._settings.lease_ttl_s)
        with self._repo.session() as conn, transaction(conn):
            conn.execute(
                """
                INSERT OR IGNORE INTO recon_scheduler_leases
                  (source_epoch_id, entity, scope_hash, lease_version)
                VALUES (?,?,?,0)
                """,
                (epoch_id, entity, scope_hash),
            )
            row = conn.execute(
                "SELECT lease_version FROM recon_scheduler_leases WHERE source_epoch_id=? AND entity=? AND scope_hash=?",
                (epoch_id, entity, scope_hash),
            ).fetchone()
            token = str(uuid.uuid4())
            version = int(row["lease_version"]) + 1
            conn.execute(
                """
                UPDATE recon_scheduler_leases
                   SET lease_token = ?, owner_id = ?, lease_version = ?, lease_until = ?
                 WHERE source_epoch_id = ? AND entity = ? AND scope_hash = ?
                """,
                (token, owner_id, version, to_iso(until), epoch_id, entity, scope_hash),
            )
            return Lease(
                source_epoch_id=epoch_id,
                entity=entity,
                scope_hash=scope_hash,
                token=token,
                version=version,
                owner_id=owner_id,
                until=to_iso(until),
            )

    def validate(self, lease: Lease) -> None:
        """Raise ``LeaseLostError`` unless the caller is still the holder."""
        with self._repo.session() as conn:
            row = conn.execute(
                """
                SELECT lease_token, lease_version, owner_id, lease_until
                  FROM recon_scheduler_leases
                 WHERE source_epoch_id = ? AND entity = ? AND scope_hash = ?
                """,
                (lease.source_epoch_id, lease.entity, lease.scope_hash),
            ).fetchone()
        if row is None:
            raise LeaseLostError("audit lease row disappeared", entity=lease.entity)
        if row["lease_token"] != lease.token or int(row["lease_version"]) != lease.version:
            raise LeaseLostError(
                "audit lease was taken over by another worker",
                entity=lease.entity,
                held_version=lease.version,
                current_version=int(row["lease_version"]),
                current_owner=row["owner_id"],
            )
        if row["lease_until"] is None or from_iso(row["lease_until"]) <= self._clock.now():
            raise LeaseLostError(
                "audit lease expired",
                entity=lease.entity,
                lease_until=row["lease_until"],
            )

    def is_valid(self, lease: Lease) -> bool:
        try:
            self.validate(lease)
        except LeaseLostError:
            return False
        return True

    def renew(self, lease: Lease) -> Lease:
        """Extend the deadline for the current holder only."""
        self.validate(lease)
        until = self._clock.now() + timedelta(seconds=self._settings.lease_ttl_s)
        with self._repo.session() as conn, transaction(conn):
            conn.execute(
                """
                UPDATE recon_scheduler_leases SET lease_until = ?
                 WHERE source_epoch_id = ? AND entity = ? AND scope_hash = ?
                   AND lease_token = ? AND lease_version = ?
                """,
                (
                    to_iso(until),
                    lease.source_epoch_id,
                    lease.entity,
                    lease.scope_hash,
                    lease.token,
                    lease.version,
                ),
            )
        return Lease(
            source_epoch_id=lease.source_epoch_id,
            entity=lease.entity,
            scope_hash=lease.scope_hash,
            token=lease.token,
            version=lease.version,
            owner_id=lease.owner_id,
            until=to_iso(until),
        )

    def release(self, lease: Lease) -> None:
        """Give the lease up cleanly, leaving the version in place."""
        with self._repo.session() as conn, transaction(conn):
            conn.execute(
                """
                UPDATE recon_scheduler_leases SET lease_token = NULL, lease_until = NULL
                 WHERE source_epoch_id = ? AND entity = ? AND scope_hash = ?
                   AND lease_token = ? AND lease_version = ?
                """,
                (
                    lease.source_epoch_id,
                    lease.entity,
                    lease.scope_hash,
                    lease.token,
                    lease.version,
                ),
            )
