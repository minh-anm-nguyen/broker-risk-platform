"""The outbox writer: called *inside* someone else's transaction.

This is the ``outbox-writer`` package from design section 17.1, and its most
important property is a negative one: it cannot open a transaction, cannot
commit, and has no network client. It takes a :class:`~outbox.persistence.ports.Transaction`
that already contains the business change. An outbox writer that could begin its
own transaction, or that lived behind an HTTP call, could not give you atomicity
with the Deal insert -- design section 6.3 says so, and the way to keep that
true over time is to make the alternative unexpressible here.

Three gates run before anything is written, in this order:

1. **Tenancy.** ``source_id`` must be in the configured allowlist, and the
   ``(source_id, epoch_id)`` pair must exist in ``source_epochs``. An epoch that
   belongs to another source is a routing attack, not a typo.
2. **Epoch status.** A ``BLOCKED`` or ``RETIRED`` epoch does not get new events.
   Already-committed events are untouched -- they are history and are handled by
   the quarantine/annotation policy, never deleted (design section 7.4).
3. **Semantic key under the aggregate head lock.** The head is reserved first,
   then the dedupe key is checked, then and only then is a version allocated.
   Checking the key before taking the lock would let two concurrent writers both
   decide the event is new.

The duplicate rule is the subtle one. A second attempt with an *identical*
canonical hash is a no-op and returns the existing row: that is what makes a
retry safe (INV07-03). A second attempt with a *different* hash under the same
semantic key is an integrity incident and raises, because the two attempts
disagree about a financial fact and this code has no basis for choosing. P03's
quarantine protocol resolves it; silently keeping either version would not.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from ..canonical import payload_hash
from ..clock import Clock, RealClock, to_wire
from ..errors import EpochBlocked, IntegrityIncident, TenantForbidden, ValidationError
from ..events.envelope import Envelope
from ..events.keys import (
    AGGREGATE_ACCOUNTS,
    AGGREGATE_POSITIONS,
    AGGREGATE_RECONCILIATION,
    AGGREGATE_SOURCE_LIFECYCLE,
    EventType,
    build_dedupe_key,
)
from ..events.routing import authorize_source, topic_for
from ..events.schema import validate_payload
from ..faults.hooks import HOOKS, HookRegistry, KillPoint
from ..money import MoneyCurrency
from ..persistence.ports import OutboxRecord, Transaction


@dataclass(frozen=True, slots=True)
class DealFact:
    """An immutable Deal as P03 persisted it.

    Money arrives as decimal strings and the currency as a
    :class:`~outbox.money.MoneyCurrency`, so an unproven currency cannot be
    smuggled in as the string ``"USD"``.
    """

    deal_id: str
    account_id: str
    symbol: str
    side: str
    entry_kind: str
    volume_lots: str
    price: str
    realized_pnl_gross: str
    commission: str
    swap: str
    currency: MoneyCurrency
    executed_at: str
    source_seq: int
    position_id: str | None = None

    @property
    def business_hash(self) -> str:
        """SHA-256 over the canonical Deal fact.

        One derivation, used both as P03's immutability check and as the
        ``deal_payload_hash`` in provenance. If these were computed separately
        they could disagree, and the disagreement would look like tampering.
        """
        return payload_hash(validate_payload(EventType.DEAL_RECORDED, 1, self.to_payload()))

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "deal_id": self.deal_id,
            "account_id": self.account_id,
            "position_id": self.position_id,
            "symbol": self.symbol,
            "side": self.side,
            "entry_kind": self.entry_kind,
            "volume_lots": self.volume_lots,
            "price": self.price,
            "realized_pnl_gross": self.realized_pnl_gross,
            "commission": self.commission,
            "swap": self.swap,
            "executed_at": self.executed_at,
        }
        payload.update(self.currency.as_payload_fields())
        return payload


@dataclass(frozen=True, slots=True)
class WriteResult:
    """What the writer did. ``created=False`` means a retry was recognised."""

    created: bool
    event_id: Any
    dedupe_key: str
    aggregate_version: int
    payload_sha256: str
    envelope: Envelope | None = None
    existing: OutboxRecord | None = None


class OutboxWriter:
    """Creates outbox rows inside a caller-owned business transaction."""

    def __init__(
        self,
        *,
        source_allowlist: tuple[str, ...],
        clock: Clock | None = None,
        max_event_wire_bytes: int = 131_072,
        hooks: HookRegistry | None = None,
    ) -> None:
        self._allowlist = source_allowlist
        self._clock = clock or RealClock()
        self._max_wire = max_event_wire_bytes
        self._hooks = hooks or HOOKS

    # --- public API ---------------------------------------------------------
    def record_deal(
        self, tx: Transaction, *, source_id: str, epoch_id: str, deal: DealFact
    ) -> WriteResult:
        """Emit ``DealRecorded`` for one newly inserted immutable Deal.

        Keyed per account (ADR/decision 3 in design section 20): the ordering
        stream a consumer projects is the account's Deal stream, so the
        aggregate key is the account id, not the deal id.
        """
        return self._emit(
            tx,
            event_type=EventType.DEAL_RECORDED,
            source_id=source_id,
            epoch_id=epoch_id,
            aggregate_key=deal.account_id,
            identity=deal.deal_id,
            payload=deal.to_payload(),
            event_time=deal.executed_at,
            provenance={
                "source_seq": deal.source_seq,
                "deal_id": deal.deal_id,
                "deal_payload_hash": deal.business_hash,
            },
        )

    def record_snapshot_published(
        self,
        tx: Transaction,
        *,
        source_id: str,
        epoch_id: str,
        entity: str,
        scan_id: str,
        lease_token: str,
    ) -> WriteResult:
        """Emit a snapshot marker, but only for a scan that really published.

        The scan's status, counts and manifest are read from ``sync_runs``
        inside this transaction rather than taken as arguments. That is the
        enforcement of ADR07-03: a caller cannot describe a scan as published
        when the row says ``STAGED``, so a partial scan has no way to produce a
        marker even if the integration code is wrong.
        """
        if entity not in ("accounts", "positions"):
            raise ValidationError(f"snapshot entity {entity!r} must be 'accounts' or 'positions'")
        row = _fetch_one(
            tx,
            "SELECT entity, status, snapshot_seq, record_count, manifest_sha256, lease_token, "
            "published_at FROM sync_runs WHERE scan_id = ? AND epoch_id = ?",
            (scan_id, epoch_id),
        )
        if row is None:
            raise ValidationError(f"scan {scan_id!r} does not exist in epoch {epoch_id}")
        if row["entity"] != entity:
            raise ValidationError(f"scan {scan_id!r} is a {row['entity']!r} scan, not {entity!r}")
        if row["status"] != "PUBLISHED":
            raise IntegrityIncident(
                f"scan {scan_id!r} has status {row['status']!r}; a marker is only emitted for "
                "a scan that has been atomically published"
            )
        if row["lease_token"] != lease_token:
            # A worker whose lease was taken over must not be able to announce a
            # publication, even one that looks complete.
            raise TenantForbidden(
                f"lease token mismatch for scan {scan_id!r}: the caller does not own this scan"
            )
        published_at = row["published_at"]
        if not published_at:
            raise IntegrityIncident(f"scan {scan_id!r} is PUBLISHED but carries no published_at")

        event_type = (
            EventType.ACCOUNT_SNAPSHOT_PUBLISHED
            if entity == "accounts"
            else EventType.POSITION_SNAPSHOT_PUBLISHED
        )
        aggregate_key = AGGREGATE_ACCOUNTS if entity == "accounts" else AGGREGATE_POSITIONS
        return self._emit(
            tx,
            event_type=event_type,
            source_id=source_id,
            epoch_id=epoch_id,
            aggregate_key=aggregate_key,
            identity=scan_id,
            payload={
                "entity": entity,
                "scan_id": scan_id,
                "snapshot_seq": int(row["snapshot_seq"]),
                "record_count": int(row["record_count"]),
                "manifest_sha256": row["manifest_sha256"],
                "published_at": published_at,
                # Notification only: the marker says "re-read the P03 read
                # model", it is not the snapshot image (design decision 4).
                "notification_only": True,
            },
            event_time=published_at,
            provenance={"scan_id": scan_id},
        )

    def record_epoch_changed(
        self,
        tx: Transaction,
        *,
        source_id: str,
        epoch_id: str,
        previous_epoch_id: str | None,
        reason: str,
        changed_at: datetime | str | None = None,
    ) -> WriteResult:
        """Emit ``SourceEpochChanged`` on a controlled epoch transition."""
        moment = _wire_time(changed_at or self._clock.now())
        return self._emit(
            tx,
            event_type=EventType.SOURCE_EPOCH_CHANGED,
            source_id=source_id,
            epoch_id=epoch_id,
            aggregate_key=AGGREGATE_SOURCE_LIFECYCLE,
            identity=epoch_id,
            payload={
                "previous_epoch_id": previous_epoch_id,
                "new_epoch_id": epoch_id,
                "reason": reason,
                "changed_at": moment,
                "notification_only": True,
            },
            event_time=moment,
            provenance={},
        )

    def record_reconciliation_verified(
        self,
        tx: Transaction,
        *,
        source_id: str,
        epoch_id: str,
        reconciliation_revision: int,
        scope: str,
        evidence_id: str,
        status: str,
        verified_at: datetime | str | None = None,
    ) -> WriteResult:
        """Emit ``ReconciliationVerified`` for a durable P04 revision.

        The revision number is the identity, so one revision yields one event no
        matter how many times P04's hook runs (ADR07-05).
        """
        moment = _wire_time(verified_at or self._clock.now())
        return self._emit(
            tx,
            event_type=EventType.RECONCILIATION_VERIFIED,
            source_id=source_id,
            epoch_id=epoch_id,
            aggregate_key=AGGREGATE_RECONCILIATION,
            identity=str(reconciliation_revision),
            payload={
                "reconciliation_revision": reconciliation_revision,
                "scope": scope,
                "evidence_id": evidence_id,
                "status": status,
                "verified_at": moment,
                "notification_only": True,
            },
            event_time=moment,
            provenance={"reconciliation_revision": reconciliation_revision},
        )

    # --- core ---------------------------------------------------------------
    def _emit(
        self,
        tx: Transaction,
        *,
        event_type: EventType,
        source_id: str,
        epoch_id: str,
        aggregate_key: str,
        identity: str,
        payload: dict[str, Any],
        event_time: datetime | str,
        provenance: dict[str, Any],
    ) -> WriteResult:
        authorize_source(source_id, self._allowlist)

        # Validate and hash before touching the database, so a malformed payload
        # costs no locks (design section 6.3 step 1).
        schema_version = 1
        normalized = validate_payload(event_type, schema_version, payload)
        candidate_hash = payload_hash(normalized)
        dedupe_key = build_dedupe_key(event_type, epoch_id, identity)

        status = tx.epoch_status(source_id, epoch_id)
        if status == "UNKNOWN":
            raise TenantForbidden(
                f"epoch {epoch_id} is not registered for source {source_id!r}; "
                "the epoch/source mapping is assigned server-side and must exist"
            )
        if status != "ACTIVE":
            raise EpochBlocked(
                f"epoch {epoch_id} is {status}; new event creation is gated. "
                "Events already committed are history and are not affected."
            )

        tx.reserve_aggregate_head(source_id, epoch_id, aggregate_key)
        existing = tx.find_event_by_dedupe(source_id, epoch_id, dedupe_key)
        if existing is not None:
            if existing.payload_sha256 != candidate_hash:
                raise IntegrityIncident(
                    f"{event_type} {dedupe_key!r} already exists with payload hash "
                    f"{existing.payload_sha256} but this attempt hashes to {candidate_hash}; "
                    "the two attempts disagree about a recorded fact and neither may be "
                    "chosen automatically"
                )
            return WriteResult(
                created=False,
                event_id=existing.event_id,
                dedupe_key=existing.dedupe_key,
                aggregate_version=existing.aggregate_version,
                payload_sha256=existing.payload_sha256,
                existing=existing,
            )

        version = tx.bump_aggregate_head(source_id, epoch_id, aggregate_key)
        self._hooks.fire(KillPoint.P03_AFTER_HEAD_BUMP, dedupe_key=dedupe_key, version=version)

        now = self._clock.now()
        envelope = Envelope.build(
            event_type=event_type,
            schema_version=schema_version,
            source_id=source_id,
            epoch_id=epoch_id,
            aggregate_id=aggregate_key,
            aggregate_version=version,
            payload=normalized,
            event_time=event_time,
            created_at=now,
            provenance=provenance,
            identity=identity,
        )
        # Size is checked against the *wire* encoding, which is what the broker
        # and its max.message.bytes will see. Oversized events are refused here
        # rather than discovered by the relay after they are durable.
        wire = envelope.to_wire_bytes(max_bytes=self._max_wire)
        assert len(wire) <= self._max_wire

        topic = topic_for(event_type)
        route_key = envelope_partition_key(envelope)
        tx.insert_event(envelope, topic=topic.name, partition_key=route_key, now=now)
        self._hooks.fire(KillPoint.P03_AFTER_OUTBOX_INSERT, dedupe_key=dedupe_key, version=version)

        return WriteResult(
            created=True,
            event_id=envelope.event_id,
            dedupe_key=envelope.dedupe_key,
            aggregate_version=version,
            payload_sha256=envelope.payload_sha256,
            envelope=envelope,
        )


def envelope_partition_key(envelope: Envelope) -> str:
    from ..events.keys import partition_key

    return partition_key(envelope.source_id, envelope.epoch_id, envelope.aggregate_key)


def _wire_time(value: datetime | str) -> str:
    return to_wire(value) if isinstance(value, datetime) else value


def _fetch_one(tx: Transaction, sql: str, params: tuple[Any, ...]) -> Any:
    """Run a single-row query through whichever driver the transaction wraps.

    The two backends differ only in placeholder style, so the SQL is written
    with ``?`` and rewritten for psycopg. Keeping that here rather than in the
    store means the writer's P03-facing reads stay in one place.
    """
    cursor = tx.execute(sql if _uses_qmark(tx) else sql.replace("?", "%s"), params)
    row = cursor.fetchone()
    if row is None:
        return None
    return row


def _uses_qmark(tx: Transaction) -> bool:
    return type(tx).__name__ == "SqliteTransaction"
