"""Page validation: the gate between a decoded payload and a DTO.

Order matters and is the order of design section 7.2. Each step is cheaper
than the one it protects, and each failure is more specific than the generic
one it would otherwise become:

1. **Status.** A 410 is not a 500 and a 401 is not a 503. The mapping is the
   whole reason a consumer can decide between retrying, re-syncing and
   stopping (section 6.2).
2. **Envelope shape.** ``run_id``, ``generation``, ``snapshot_seq``,
   ``next_cursor`` and ``items``, each with the right type. A missing field is
   a contract error, never a default.
3. **Provenance against the scan.** The run, generation and snapshot
   sequence must equal those of the page that started the scan. Page 3
   arriving from a different snapshot is not a page of this scan, and
   stitching it on would produce a "complete" result assembled from two
   different moments (INV-06, INV-07, WF2-I-06, WF2-I-07).
4. **Cursor progress.** A repeated cursor is a loop; detection is by hash, so
   a long scan does not retain every cursor it has seen (WF2-I-02).
5. **Items.** Strict DTO construction, uniqueness inside the page, and - for
   deals - the ordering and sequence bounds the contract promises.

The rule underneath all of it: nothing is repaired. Not a float price, not a
missing field, not an out-of-order page. A connector that fixes financial data
is a connector that invents it (ADR-205, ADR-210).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from .drivers.base import DriverResponse
from .dto import build_account, build_deal, build_position
from .errors import (
    ConnectorError,
    DataIntegrityError,
    SourceContractError,
    SourceEpochChangedError,
)
from .ports import Capabilities, Entity
from .scalars import ScalarError
from .status_mapping import map_error_response

__all__ = [
    "PageFacts",
    "map_error_response",
    "parse_envelope",
    "check_provenance",
    "build_items",
    "check_deal_ordering",
    "filter_digest",
    "cursor_fingerprint",
]


@dataclass(frozen=True, slots=True)
class PageFacts:
    """The envelope of one page, after shape validation."""

    run_id: str
    generation: int
    snapshot_seq: int
    next_cursor: str | None
    raw_items: tuple[Any, ...]
    byte_count: int


def filter_digest(fields: dict[str, Any]) -> str:
    """A stable digest of a scan's filter set.

    Keyed on sorted items so two equivalent filter dicts digest identically,
    and used to scope a checkpoint candidate to the filters that produced it.
    A watermark from a scan filtered to account A says nothing about account
    B, and this digest is what makes that difference visible downstream
    (WF2-K-06, INV-21).
    """
    parts = [f"{key}={fields[key]!r}" for key in sorted(fields)]
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:32]


def cursor_fingerprint(cursor: str) -> str:
    """A short hash of a cursor, for loop detection and for logs.

    Hashed rather than stored: a cursor can be 4 KiB and a long scan sees
    thousands of them, so retaining the text would be a memory leak with a
    scan-length bound. It is also an opaque token that may be signed, and a
    hash keeps it out of log lines (WF2-I-02).
    """
    return hashlib.sha256(cursor.encode("utf-8")).hexdigest()[:24]


# ---------------------------------------------------------------------------
# Envelope
# ---------------------------------------------------------------------------


def parse_envelope(
    response: DriverResponse, *, source_id: str, operation: str, request_id: str | None = None
) -> PageFacts:
    """Validate the page envelope's shape and types."""
    payload = response.payload
    context = {
        "source_id": source_id,
        "operation": operation,
        "request_id": request_id,
        "upstream_status": response.status,
    }
    if not isinstance(payload, dict):
        raise SourceContractError(
            "the page body is not a JSON object",
            details={"observed_type": type(payload).__name__, "path": response.path_name},
            **context,
        )

    run_id = payload.get("run_id")
    if not isinstance(run_id, str) or not run_id:
        raise SourceContractError(
            "the page envelope is missing a usable run_id, so its provenance cannot "
            "be established",
            details={"field": "run_id"},
            **context,
        )
    generation = payload.get("generation")
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise SourceContractError(
            "the page envelope is missing a usable generation",
            details={"field": "generation"},
            **context,
        )
    snapshot_seq = payload.get("snapshot_seq")
    if isinstance(snapshot_seq, bool) or not isinstance(snapshot_seq, int) or snapshot_seq < 0:
        raise SourceContractError(
            "the page envelope is missing a usable snapshot_seq",
            details={"field": "snapshot_seq"},
            **context,
        )
    if "next_cursor" not in payload:
        raise SourceContractError(
            "the page envelope omits next_cursor; absence and null are different "
            "claims and only null means 'no further pages'",
            details={"field": "next_cursor"},
            **context,
        )
    next_cursor = payload.get("next_cursor")
    if next_cursor is not None and (not isinstance(next_cursor, str) or not next_cursor):
        raise SourceContractError(
            "next_cursor must be a non-empty string or null",
            details={"field": "next_cursor"},
            **context,
        )
    if isinstance(next_cursor, str) and len(next_cursor) > 4096:
        raise SourceContractError(
            "next_cursor exceeds the permitted length",
            details={"field": "next_cursor", "maximum": 4096},
            **context,
        )
    items = payload.get("items")
    if not isinstance(items, list):
        raise SourceContractError(
            "the page envelope is missing an items array",
            details={"field": "items"},
            **context,
        )

    return PageFacts(
        run_id=run_id,
        generation=generation,
        snapshot_seq=snapshot_seq,
        next_cursor=next_cursor,
        raw_items=tuple(items),
        byte_count=response.byte_count,
    )


def check_provenance(
    facts: PageFacts,
    *,
    source_id: str,
    operation: str,
    request_id: str | None,
    expected_run_id: str | None,
    expected_generation: int | None,
    expected_snapshot_seq: int | None,
    page_index: int,
) -> None:
    """Compare a page's provenance with the scan that requested it.

    ``expected_*`` is ``None`` on the first page: there is nothing to compare
    against yet, and the first page is what establishes the values every later
    page is held to.

    All three are checked, not just the run. A source can reuse a ``run_id``
    with a new ``generation`` after a reset, and comparing only the run would
    miss it entirely (WF2-J-03).
    """
    context = {"source_id": source_id, "operation": operation, "request_id": request_id}

    if expected_run_id is not None and facts.run_id != expected_run_id:
        raise SourceEpochChangedError(
            "this page belongs to a different run than the scan that requested it",
            source_generation=facts.generation,
            details={
                "field": "run_id",
                "page_index": page_index,
                "expected_run_id": expected_run_id,
                "observed_run_id": facts.run_id,
            },
            **context,
        )
    if expected_generation is not None and facts.generation != expected_generation:
        raise SourceEpochChangedError(
            "the source generation changed during this scan",
            source_generation=facts.generation,
            details={
                "field": "generation",
                "page_index": page_index,
                "expected_generation": expected_generation,
                "observed_generation": facts.generation,
            },
            **context,
        )
    if expected_snapshot_seq is not None and facts.snapshot_seq != expected_snapshot_seq:
        # Same run and generation but a different snapshot: the pages are each
        # internally valid and belong to two different moments. Joining them
        # would give a consumer a set that never existed at any instant.
        raise DataIntegrityError(
            "this page was taken at a different snapshot sequence than the scan it "
            "belongs to; the pages cannot be joined",
            source_generation=facts.generation,
            details={
                "field": "snapshot_seq",
                "page_index": page_index,
                "expected_snapshot_seq": expected_snapshot_seq,
                "observed_snapshot_seq": facts.snapshot_seq,
            },
            **context,
        )


# ---------------------------------------------------------------------------
# Items
# ---------------------------------------------------------------------------


def build_items(
    facts: PageFacts,
    *,
    entity: Entity,
    capabilities: Capabilities,
    source_id: str,
    operation: str,
    request_id: str | None,
    page_index: int,
) -> tuple[Any, ...]:
    """Strictly build every item, or emit nothing at all.

    A page is atomic: either every record validates or the page is refused.
    Emitting the valid prefix of a bad page would hand a consumer a page that
    looks complete and is not (WF2-Q-06, WF2-D-05).
    """
    context = {"source_id": source_id, "operation": operation, "request_id": request_id}
    expect_source_seq = (
        entity is Entity.DEALS and capabilities.incremental_deals_after_seq
    )
    built: list[Any] = []
    for index, raw in enumerate(facts.raw_items):
        try:
            if entity is Entity.ACCOUNTS:
                built.append(build_account(raw))
            elif entity is Entity.POSITIONS:
                built.append(build_position(raw))
            else:
                built.append(build_deal(raw, expect_source_seq=expect_source_seq))
        except ScalarError as exc:
            raise SourceContractError(
                "a record on this page failed strict validation, so the whole page "
                "was refused",
                details={
                    **exc.as_details(),
                    "entity": entity.value,
                    "page_index": page_index,
                    "item_index": index,
                    "items_on_page": len(facts.raw_items),
                },
                **context,
            ) from exc

    _check_unique_keys(
        built,
        entity=entity,
        page_index=page_index,
        context=context,
    )
    return tuple(built)


def _key_of(item: Any, entity: Entity) -> str:
    if entity is Entity.ACCOUNTS:
        return item.account_id
    if entity is Entity.POSITIONS:
        return item.position_id
    return item.deal_id


def _check_unique_keys(
    items: list[Any],
    *,
    entity: Entity,
    page_index: int,
    context: dict[str, Any],
) -> None:
    seen: set[str] = set()
    for index, item in enumerate(items):
        key = _key_of(item, entity)
        if key in seen:
            # Two records with one identity on one page. Which is current is
            # unknowable here, so neither is forwarded (WF2-L-06).
            raise DataIntegrityError(
                "the same identifier appears twice on one page",
                details={
                    "entity": entity.value,
                    "page_index": page_index,
                    "item_index": index,
                    "field": f"{entity.value}_identifier",
                },
                **context,
            )
        seen.add(key)


def check_deal_ordering(
    deals: tuple[Any, ...],
    *,
    facts: PageFacts,
    requested_after_seq: int | None,
    previous_upper_bound: int | None,
    capabilities: Capabilities,
    source_id: str,
    operation: str,
    request_id: str | None,
    page_index: int,
) -> int | None:
    """Check the sequence bounds and ordering the deal contract promises.

    Returns the highest ``source_seq`` on the page, or ``None`` when the
    source does not publish one.

    The bounds are ``after_seq < source_seq <= snapshot_seq``. Both halves
    catch a real failure:

    * A deal at or below ``after_seq`` means the source ignored an exclusive
      filter, and accepting it would re-deliver data the consumer has already
      committed while suggesting the filter works (WF2-K-07).
    * A deal above ``snapshot_seq`` is from after the snapshot this scan
      pinned - a record from the future of its own read (WF2-K-08).

    What is deliberately **not** checked: continuity. Gaps in ``source_seq``
    are expected, because the sequence counts domain events and most events
    are not deals. Alarming on a gap would fire constantly and train everyone
    to ignore it (WF2-K-01, INV-19).
    """
    if not capabilities.incremental_deals_after_seq:
        return None

    context = {"source_id": source_id, "operation": operation, "request_id": request_id}
    previous: tuple[int, str] | None = None
    highest: int | None = None

    for index, deal in enumerate(deals):
        sequence = deal.source_seq
        if sequence is None:  # pragma: no cover - build_deal enforces presence
            raise SourceContractError(
                "a deal is missing source_seq although the source declared "
                "incremental sync",
                details={"field": "source_seq", "page_index": page_index, "item_index": index},
                **context,
            )
        if requested_after_seq is not None and sequence <= requested_after_seq:
            raise DataIntegrityError(
                "a deal was returned at or below the exclusive after_seq filter",
                details={
                    "field": "source_seq",
                    "page_index": page_index,
                    "item_index": index,
                    "requested_after_seq": requested_after_seq,
                    "observed_source_seq": sequence,
                },
                **context,
            )
        if sequence > facts.snapshot_seq:
            raise DataIntegrityError(
                "a deal carries a source_seq beyond the snapshot this scan pinned",
                details={
                    "field": "source_seq",
                    "page_index": page_index,
                    "item_index": index,
                    "snapshot_seq": facts.snapshot_seq,
                    "observed_source_seq": sequence,
                },
                **context,
            )
        # Ordering is by ``(source_seq, deal_id)``: the sequence alone is not
        # guaranteed unique across deals of one event, and a tie broken
        # differently on two reads would look like disorder.
        current = (sequence, deal.deal_id)
        if previous is not None and current <= previous:
            raise DataIntegrityError(
                "deals are not in the ascending (source_seq, deal_id) order the "
                "contract promises, so a cursor scan over them cannot be trusted "
                "to be complete",
                details={
                    "field": "source_seq",
                    "page_index": page_index,
                    "item_index": index,
                    "observed_source_seq": sequence,
                    "previous_source_seq": previous[0],
                },
                **context,
            )
        previous = current
        highest = sequence if highest is None else max(highest, sequence)

    if (
        previous_upper_bound is not None
        and deals
        and previous is not None
        and previous[0] <= previous_upper_bound
        and deals[0].source_seq is not None
        and deals[0].source_seq <= previous_upper_bound
    ):
        # Ordering has to hold *across* pages too, or page 4 could repeat
        # page 3 and a scan would still look ordered within each page.
        raise DataIntegrityError(
            "this page does not continue after the previous page's highest source_seq",
            details={
                "field": "source_seq",
                "page_index": page_index,
                "previous_upper_bound": previous_upper_bound,
                "observed_first_source_seq": deals[0].source_seq,
            },
            **context,
        )
    return highest
