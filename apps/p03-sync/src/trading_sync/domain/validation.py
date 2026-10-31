"""Page validation: everything that must hold before the sink is allowed to see a page.

The source is untrusted for correctness until it passes through here (design
3.2). Validation is *stateful across the scan*, which is what makes several of
these checks possible at all -- a single page in isolation cannot reveal a
mixed snapshot, a cursor cycle or a sequence that went backwards.

What this layer can and cannot prove is worth being precise about. It can prove
that a page chain is internally consistent, ordered, in-bounds, unmixed and
terminated. It cannot prove that the source actually included every record it
should have: a source that silently omits a deal and then reports a terminal
page produces a chain that validates perfectly. Only an independent oracle
catches that (design 6.6, WF3-141), which is why the test harness never uses
the fault-injected endpoint as its own oracle.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Collection, Mapping

from trading_sync.domain.canonical import page_hash
from trading_sync.domain.entities import AccountRecord, DealRecord, PositionRecord
from trading_sync.domain.snapshots import Entity, RawPage, ValidatedPage
from trading_sync.errors import (
    CursorError,
    DuplicateKeyConflict,
    FilterScopeError,
    MixedSnapshotError,
    PageContractError,
    ScanBudgetExceededError,
    SchemaError,
    SequenceError,
    SnapshotBoundError,
)

__all__ = ["DEFAULT_MAX_PAGE_ITEMS", "ScanValidationState", "validate_page"]

#: The simulator's documented page ceiling (AS-11). A larger page is a contract
#: breach, not a windfall: accepting it would mean the response-size and
#: transaction-size budgets no longer bound anything (WF3-042).
DEFAULT_MAX_PAGE_ITEMS = 500

#: Bounds a pathological cursor chain even when every individual page is valid
#: (WF3-041, WF3-150).
DEFAULT_MAX_PAGES = 2500


@dataclass(slots=True)
class ScanValidationState:
    """Mutable per-scan validation context.

    One instance belongs to exactly one scan attempt. A restart creates a fresh
    one, because a resumed scan must re-derive provenance from the source
    rather than trust what a previous process remembered (design 6.4).
    """

    entity: Entity
    filter_scope: str
    start_watermark: int
    snapshot_seq_bound: int | None = None
    max_page_items: int = DEFAULT_MAX_PAGE_ITEMS
    max_pages: int = DEFAULT_MAX_PAGES
    allowed_currencies: Collection[str] | None = None

    run_id: str | None = None
    generation: int | None = None
    snapshot_seq: int | None = None
    expected_page_index: int = 0
    terminal_seen: bool = False
    last_source_seq: int = 0
    seen_cursors: set[str] = field(default_factory=set)
    #: key -> canonical payload hash, for cross-page duplicate classification.
    seen_keys: dict[str, str] = field(default_factory=dict)
    seen_source_seqs: dict[int, str] = field(default_factory=dict)
    duplicate_count: int = 0
    #: Records at or below the start watermark. Under an exclusive ``after_seq``
    #: these should not exist; see :func:`_check_deal_bounds`.
    boundary_anomaly_count: int = 0

    @property
    def pages_validated(self) -> int:
        return self.expected_page_index


def validate_page(raw: RawPage, state: ScanValidationState) -> ValidatedPage:
    """Validate one page in scan order and advance ``state``.

    Raises on the first violation; the caller aborts the scan and leaves the
    watermark alone. ``state`` is only mutated once a page has fully passed, so
    a rejected page cannot leave the validator half-advanced.
    """
    _check_scan_shape(raw, state)
    _check_provenance(raw, state)
    _check_cursor(raw, state)

    records, duplicates, delta = _parse_items(raw, state)

    item_hashes = tuple(record.payload_hash for record in records)
    digest = page_hash(
        entity=str(raw.entity),
        run_id=raw.run_id,
        generation=raw.generation,
        snapshot_seq=raw.snapshot_seq,
        page_index=raw.page_index,
        item_hashes=item_hashes,
    )
    is_terminal = raw.next_cursor is None

    # Commit the page's effects only now that every check has passed. Mutating
    # as we parse would leave a rejected page's partial keys and sequence
    # behind, and the next legitimate page would then look out of order.
    state.expected_page_index += 1
    state.duplicate_count += duplicates
    state.seen_keys.update(delta.keys)
    state.seen_source_seqs.update(delta.source_seqs)
    state.last_source_seq = delta.last_source_seq
    state.boundary_anomaly_count += delta.boundary_anomalies
    if is_terminal:
        state.terminal_seen = True
    if raw.next_cursor is not None:
        state.seen_cursors.add(raw.next_cursor)

    return ValidatedPage(
        entity=raw.entity,
        run_id=raw.run_id,
        generation=raw.generation,
        snapshot_seq=raw.snapshot_seq,
        page_index=raw.page_index,
        records=records,
        page_hash=digest,
        is_terminal=is_terminal,
        next_cursor=raw.next_cursor,
        filter_scope=raw.filter_scope,
        duplicate_count=duplicates,
    )


def _check_scan_shape(raw: RawPage, state: ScanValidationState) -> None:
    if raw.entity is not state.entity:
        # A cursor issued for one entity must never deliver another entity's
        # rows into this scan's staging (WF3-045).
        raise FilterScopeError(
            "page entity does not match the scan",
            expected=str(state.entity),
            actual=str(raw.entity),
        )
    if raw.filter_scope != state.filter_scope:
        # Changing scope mid-scan would make the completeness argument apply to
        # a different population than the one we started measuring (WF3-046).
        raise FilterScopeError(
            "page filter scope changed mid-scan",
            expected=state.filter_scope,
            actual=raw.filter_scope,
        )
    if state.terminal_seen:
        raise PageContractError(
            "a page arrived after the terminal page", page_index=raw.page_index
        )
    if raw.page_index != state.expected_page_index:
        raise PageContractError(
            "page arrived out of order",
            expected=state.expected_page_index,
            actual=raw.page_index,
        )
    if raw.page_index >= state.max_pages:
        raise ScanBudgetExceededError(
            "scan exceeded its page budget",
            page_index=raw.page_index,
            max_pages=state.max_pages,
        )
    if len(raw.items) > state.max_page_items:
        raise PageContractError(
            "page carries more items than the contract allows",
            item_count=len(raw.items),
            max_items=state.max_page_items,
        )
    if not raw.cursor_present:
        # Absent field, not an explicit null: do not read this as terminal.
        raise SchemaError("page is missing the next_cursor field", field="next_cursor")


def _check_provenance(raw: RawPage, state: ScanValidationState) -> None:
    if raw.generation < 0:
        raise SchemaError("generation must not be negative", field="generation")
    if raw.snapshot_seq < 0:
        raise SchemaError("snapshot_seq must not be negative", field="snapshot_seq")
    if state.run_id is None:
        state.run_id = raw.run_id
        state.generation = raw.generation
        state.snapshot_seq = raw.snapshot_seq
        return
    if (raw.run_id, raw.generation, raw.snapshot_seq) != (
        state.run_id,
        state.generation,
        state.snapshot_seq,
    ):
        raise MixedSnapshotError(
            "page provenance differs from the rest of the scan",
            expected_epoch=f"{state.run_id}:G{state.generation}",
            expected_snapshot_seq=state.snapshot_seq,
            actual_epoch=f"{raw.run_id}:G{raw.generation}",
            actual_snapshot_seq=raw.snapshot_seq,
        )


def _check_cursor(raw: RawPage, state: ScanValidationState) -> None:
    if raw.next_cursor is None:
        return
    if raw.next_cursor == "":
        raise SchemaError("next_cursor is an empty string", field="next_cursor")
    if raw.next_cursor == raw.request_cursor or raw.next_cursor in state.seen_cursors:
        # Following a repeated cursor is an unbounded loop, so stop now rather
        # than discover it through a page or wall-time budget (WF3-039).
        raise CursorError("cursor cycle detected", page_index=raw.page_index)


@dataclass(slots=True)
class _PageDelta:
    """The effects one page would have, held aside until the page passes."""

    last_source_seq: int
    keys: dict[str, str] = field(default_factory=dict)
    source_seqs: dict[int, str] = field(default_factory=dict)
    boundary_anomalies: int = 0


def _parse_items(
    raw: RawPage, state: ScanValidationState
) -> tuple[tuple[Any, ...], int, _PageDelta]:
    """Parse and de-duplicate a page's items.

    Returns the records to persist, the number of accepted duplicates, and the
    state change the page implies. A duplicate is only "accepted" when its
    canonical payload matches what the scan has already seen for that key;
    otherwise it is a conflict, because two different answers for one id cannot
    both be stored and picking one would be a silent overwrite (INV-04 versus
    INV-05).
    """
    delta = _PageDelta(last_source_seq=state.last_source_seq)
    records: list[Any] = []
    duplicates = 0
    for item in raw.items:
        record = _parse_item(raw.entity, item, state)
        previous_hash = state.seen_keys.get(record.key, delta.keys.get(record.key))
        if previous_hash is not None:
            if previous_hash != record.payload_hash:
                raise DuplicateKeyConflict(
                    "the same key appeared twice with different content",
                    entity=str(raw.entity),
                    page_index=raw.page_index,
                )
            duplicates += 1
            continue
        if raw.entity is Entity.DEALS:
            _check_deal_sequence(record, state, delta)
            _check_deal_bounds(record, state, delta)
            delta.source_seqs[record.source_seq] = record.deal_id
            delta.last_source_seq = max(delta.last_source_seq, record.source_seq)
        delta.keys[record.key] = record.payload_hash
        records.append(record)
    return tuple(records), duplicates, delta


def _parse_item(
    entity: Entity, item: Mapping[str, Any], state: ScanValidationState
) -> Any:
    if not isinstance(item, Mapping):
        raise SchemaError("page item is not an object", actual_type=type(item).__name__)
    if entity is Entity.DEALS:
        return DealRecord.from_payload(item)
    if entity is Entity.POSITIONS:
        return PositionRecord.from_payload(item)
    return AccountRecord.from_payload(
        item, allowed_currencies=state.allowed_currencies
    )


def _check_deal_sequence(
    record: DealRecord, state: ScanValidationState, delta: _PageDelta
) -> None:
    """Enforce strictly increasing ``source_seq`` within the scan.

    Gaps are fine and are not checked for: account, position and mark events
    consume sequence numbers too, so deals at 1001, 1005 and 1010 are a normal
    stream and raising a "missing data" error there would be a false alarm
    (INV-11, WF3-025, fixture G03).
    """
    existing_deal = state.seen_source_seqs.get(
        record.source_seq, delta.source_seqs.get(record.source_seq)
    )
    if existing_deal is not None:
        raise SequenceError(
            "two different deals claim the same source_seq",
            source_seq=record.source_seq,
        )
    if record.source_seq <= delta.last_source_seq:
        raise SequenceError(
            "source_seq went backwards within the scan",
            source_seq=record.source_seq,
            last_source_seq=delta.last_source_seq,
        )


def _check_deal_bounds(
    record: DealRecord, state: ScanValidationState, delta: _PageDelta
) -> None:
    """Confirm the record falls inside the window the snapshot covers.

    Above ``S`` is fatal: the page is not from the snapshot it claims, so that
    snapshot cannot justify a watermark (WF3-032).

    At or below ``W`` is a contract anomaly rather than a fatal error. It means
    the source treated ``after_seq`` as inclusive. The record is kept -- the
    sink's idempotent insert makes it a no-op -- and counted, so the mismatch
    is reported without either skipping data or blocking sync entirely
    (WF3-030, AS-04).
    """
    bound = state.snapshot_seq_bound if state.snapshot_seq_bound is not None else state.snapshot_seq
    if bound is not None and record.source_seq > bound:
        raise SnapshotBoundError(
            "deal source_seq is above the snapshot bound",
            source_seq=record.source_seq,
            snapshot_seq=bound,
        )
    if record.source_seq <= state.start_watermark:
        delta.boundary_anomalies += 1
