"""Enumerations and page value objects shared across the sync pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping

from trading_sync.errors import SchemaError

__all__ = [
    "Entity",
    "EpochStatus",
    "Presence",
    "RawPage",
    "ScanStatus",
    "Side",
    "SourceIdentity",
    "SourceKind",
    "ValidatedPage",
    "parse_side",
]


class Entity(StrEnum):
    ACCOUNTS = "accounts"
    POSITIONS = "positions"
    DEALS = "deals"


class SourceKind(StrEnum):
    SIMULATOR = "simulator"
    MANAGER_SDK = "manager_sdk"


class ScanStatus(StrEnum):
    """Lifecycle of one scan attempt (design 3.3).

    ``STAGED`` means something different per entity: for deals, every page is
    idempotently persisted; for accounts and positions, every row sits in
    staging and nothing is visible to a reader yet.
    """

    CREATED = "CREATED"
    FETCHING = "FETCHING"
    STAGED = "STAGED"
    VALIDATING = "VALIDATING"
    PUBLISHING = "PUBLISHING"
    PUBLISHED = "PUBLISHED"
    ABORTED = "ABORTED"
    QUARANTINED = "QUARANTINED"
    BLOCKED_EPOCH = "BLOCKED_EPOCH"


class EpochStatus(StrEnum):
    DISCOVERED = "DISCOVERED"
    ACTIVE = "ACTIVE"
    DEGRADED = "DEGRADED"
    BLOCKED = "BLOCKED"
    ARCHIVED = "ARCHIVED"


class Presence(StrEnum):
    """Whether a key appeared in the most recent fully verified snapshot.

    ``ABSENT`` is a statement about our snapshots, never about the business
    state. A position missing from a complete listing is absent; whether it is
    *closed* is only knowable if the source says so (INV-08, WF3-073).
    """

    PRESENT = "PRESENT"
    ABSENT = "ABSENT"


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


def parse_side(raw: object, *, field: str = "side") -> Side:
    """Map a source side onto :class:`Side`, or raise.

    There is no default. An unrecognised side falling back to ``BUY`` would
    invert the sign of a position's exposure (WF3-080).
    """
    if isinstance(raw, bool) or not isinstance(raw, str):
        raise SchemaError(
            "side must be a string", field=field, actual_type=type(raw).__name__
        )
    try:
        return Side(raw)
    except ValueError:
        raise SchemaError("unknown side", field=field, allowed=["BUY", "SELL"]) from None


@dataclass(frozen=True, slots=True)
class SourceIdentity:
    """What the source says it is, as read from its meta endpoint.

    ``(run_id, generation)`` identifies a simulator *source epoch*. It is not
    the connector's worker epoch: a connector reconnect rotates that and must
    not reset our watermark (AS-13, WF3-016).
    """

    source_id: str
    source_kind: SourceKind
    run_id: str
    generation: int
    committed_seq: int
    supports_incremental_deals: bool = True

    @property
    def epoch_label(self) -> str:
        """Short human label for logs and audit rows."""
        return f"{self.run_id}:G{self.generation}"


@dataclass(frozen=True, slots=True)
class RawPage:
    """One page as received, before any domain validation.

    ``cursor_present`` is separate from ``next_cursor`` on purpose. A page whose
    ``next_cursor`` is explicitly ``null`` is terminal; a page *missing* the
    field entirely is a schema error. Collapsing the two would let a truncated
    response end a scan (WF3-044).
    """

    source_id: str
    entity: Entity
    run_id: str
    generation: int
    snapshot_seq: int
    page_index: int
    items: tuple[Mapping[str, Any], ...]
    cursor_present: bool
    next_cursor: str | None
    filter_scope: str
    request_cursor: str | None = None


@dataclass(frozen=True, slots=True)
class ValidatedPage:
    """A page that passed every domain rule and may be handed to the sink.

    ``is_terminal`` is necessary but not sufficient evidence of a complete
    scan: the sink still has to prove an unbroken receipt chain before any
    watermark moves (design 6.4).
    """

    entity: Entity
    run_id: str
    generation: int
    snapshot_seq: int
    page_index: int
    records: tuple[Any, ...]
    page_hash: str
    is_terminal: bool
    next_cursor: str | None
    filter_scope: str
    duplicate_count: int = 0

    @property
    def record_count(self) -> int:
        return len(self.records)
