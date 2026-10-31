"""Build the outbox row for a committed Deal. No I/O.

This is the half of the Project 03 -> Project 07 seam that can be reasoned
about and tested without a database. Given a ``DealRecord``, the epoch it was
committed under and an allocated aggregate version, it produces the exact
column values to insert into ``event_outbox``. The caller performs the insert,
inside its own transaction, which is the whole point (contract gap CG-08).

Three decisions worth knowing about before changing anything here.

**The currency is reported as unproven, not guessed.** ``DealRecord`` carries
no currency: Project 03 never receives one per deal. The event contract has a
required ``money_currency_status`` and an optional ``money_currency``, so the
honest payload is ``UNPROVEN`` with no code. Filling in the account's currency
would be an inference presented as a fact, and a consumer summing by currency
would believe it.

**Money that will not fit is refused, never rounded.** Project 03 stores money
at ``MONEY_SCALE = 12``; the event contract carries two decimal places and
``normalize_money`` raises rather than quantising. A deal with a commission of
``-0.005`` is valid upstream and cannot be expressed as a ``DealRecorded``
payload, so this module lets that error out and the caller's transaction rolls
back. The Deal is then not committed either, which keeps the atomicity
invariant: there is no state in which a Deal exists without its event. It is
the safe answer and not the final one - see contract gap CG-10.

**One event per newly inserted Deal.** A re-fetched page whose rows were
declined by ``ON CONFLICT DO NOTHING`` emits nothing (P07 ADR07-02). The caller
passes only the deal ids the insert actually returned.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

from platform_events import Envelope, EventType
from platform_events.clock import to_wire
from platform_events.events.routing import route
from platform_events.money import CurrencyStatus

from ..domain.canonical import canonical_decimal_str
from ..domain.entities import DealRecord
from ..errors import OutboxEmissionError

#: The schema version this producer writes. Bumping it is a contract change and
#: needs a consumer that reads both (P09 compatibility policy).
DEAL_SCHEMA_VERSION = 1

__all__ = [
    "DEAL_SCHEMA_VERSION",
    "EmittedEvent",
    "assert_emittable",
    "build_deal_event",
    "deal_payload",
    "outbox_row",
]

#: Decimal places the event contract carries for a money field. Project 03
#: itself stores twelve (``trading_sync.domain.money.MONEY_SCALE``), so a Deal
#: can be valid upstream and unrepresentable as an event (contract gap CG-10).
EVENT_MONEY_SCALE = 2

_MONEY_FIELDS = ("realized_pnl_gross", "commission", "swap")


@dataclass(frozen=True, slots=True)
class EmittedEvent:
    """One validated envelope plus the routing the relay will use."""

    envelope: Envelope
    topic: str
    partition_key: str

    @property
    def aggregate_key(self) -> str:
        return self.envelope.aggregate_id


def deal_payload(record: DealRecord) -> dict[str, Any]:
    """Project a ``DealRecord`` into a ``DealRecorded`` payload.

    Every money, price and volume field travels as a decimal string. The
    projection goes through Project 03's own ``canonical_decimal_str`` so the
    bytes in the event match the bytes Project 03 hashes, rather than being
    re-derived by a second formatter that might disagree about trailing zeros.
    """
    return {
        "deal_id": record.deal_id,
        "account_id": record.account_id,
        "position_id": record.position_id,
        "symbol": record.symbol,
        "side": str(record.side),
        "entry_kind": record.entry_kind,
        "volume_lots": canonical_decimal_str(record.volume_lots),
        "price": canonical_decimal_str(record.price),
        "realized_pnl_gross": canonical_decimal_str(record.realized_pnl_gross),
        "commission": canonical_decimal_str(record.commission),
        "swap": canonical_decimal_str(record.swap),
        # Not inferred from the account or the symbol. See the module docstring.
        "money_currency": None,
        "money_currency_status": str(CurrencyStatus.UNPROVEN),
        "executed_at": to_wire(record.executed_at),
    }


def build_deal_event(
    record: DealRecord,
    *,
    source_id: str,
    epoch_id: str,
    aggregate_version: int,
    created_at: datetime,
    source_allowlist: tuple[str, ...],
    provenance: dict[str, Any] | None = None,
) -> EmittedEvent:
    """Validate and assemble one ``DealRecorded`` event.

    ``identity`` is the deal id, so the dedupe key and the event id both follow
    from the business fact. A retry for the same deal under the same epoch
    derives the same event id and is rejected by the outbox's unique index
    instead of becoming a second event for one change.

    ``event_time`` is the deal's execution time, not now: a correction posted
    today for last week's fill carries last week's timestamp and a new
    sequence, and a consumer ordering by event time would otherwise misplace
    it.
    """
    assert_emittable(record)
    envelope = Envelope.build(
        event_type=EventType.DEAL_RECORDED,
        schema_version=DEAL_SCHEMA_VERSION,
        source_id=source_id,
        epoch_id=epoch_id,
        aggregate_id=record.account_id,
        aggregate_version=aggregate_version,
        payload=deal_payload(record),
        event_time=record.executed_at,
        created_at=created_at,
        provenance=provenance,
        identity=record.deal_id,
    )
    resolved = route(
        event_type=EventType.DEAL_RECORDED,
        source_id=source_id,
        epoch_id=envelope.epoch_id,
        aggregate_key=envelope.aggregate_id,
        allowlist=source_allowlist,
    )
    return EmittedEvent(
        envelope=envelope,
        topic=resolved.topic,
        partition_key=resolved.partition_key,
    )


def outbox_row(
    event: EmittedEvent,
    *,
    created_at: datetime,
    max_wire_bytes: int | None = None,
) -> dict[str, Any]:
    """The column values for one ``event_outbox`` insert.

    ``next_attempt_at`` equals ``created_at`` so the row is immediately
    eligible for the relay. ``state`` and the claim columns are left to their
    database defaults: they are Project 07's lifecycle, and a producer that set
    them could create a row that looks already claimed.
    """
    envelope = event.envelope
    return {
        "event_id": str(envelope.event_id),
        "source_id": envelope.source_id,
        "epoch_id": envelope.epoch_id,
        "dedupe_key": envelope.dedupe_key,
        "aggregate_type": envelope.aggregate_type,
        "aggregate_key": envelope.aggregate_id,
        "aggregate_version": envelope.aggregate_version,
        "event_type": str(envelope.event_type),
        "schema_version": envelope.schema_version,
        "topic": event.topic,
        "partition_key": event.partition_key,
        "payload": envelope.payload,
        "payload_sha256": envelope.payload_sha256,
        "wire_bytes": envelope.to_wire_bytes(max_bytes=max_wire_bytes),
        "created_at": created_at,
        "next_attempt_at": created_at,
    }


def assert_emittable(record: DealRecord) -> None:
    """Refuse a Deal the event contract cannot carry, with a usable message.

    Letting ``normalize_money`` raise would also be safe - the transaction
    rolls back either way - but the error would arrive from inside the shared
    contract saying only that a value had too many significant digits, with no
    deal id and no hint about what to do. An operator reading that at 3am
    cannot tell a code defect from an incompatible source.

    So this says which deal, which field, what the scales are, and what the
    two ways out are. The decision not to round is not reconsidered here: a
    commission silently changed from -0.005 to -0.01 is a wrong number that
    nobody was told about, which is worse than a halted scan (contract gap
    CG-10).
    """
    for field in _MONEY_FIELDS:
        value = getattr(record, field)
        exponent = value.as_tuple().exponent
        if not isinstance(exponent, int):  # NaN or Infinity
            raise OutboxEmissionError(
                f"deal {record.deal_id}: {field} is not a finite amount, "
                "so no event can describe it"
            )
        places = -exponent if exponent < 0 else 0
        if places > EVENT_MONEY_SCALE:
            raise OutboxEmissionError(
                f"deal {record.deal_id}: {field}={value} has {places} decimal "
                f"places; the DealRecorded contract carries {EVENT_MONEY_SCALE}. "
                "Rounding it would change a monetary amount without telling "
                "anyone, so the page is refused instead. Either the source must "
                "be constrained to report money at the contract's scale, or the "
                "contract and every downstream column type must be widened "
                "together (contract gap CG-10)."
            )
