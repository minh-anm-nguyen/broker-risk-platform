"""Page and scan validation: what a client can prove about a page chain.

Covers design groups C (sequence and completeness) and D (cursor and
pagination). Validation is stateful across the scan, and several of these
checks are impossible on a single page in isolation -- a mixed snapshot, a
cursor cycle and a sequence that went backwards are all cross-page facts.

The last test in this file is the important one: it pins down what validation
*cannot* do, so nobody mistakes a clean chain for proof of completeness.
"""

from __future__ import annotations

import pytest

from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, RawPage
from trading_sync.domain.validation import ScanValidationState, validate_page
from trading_sync.errors import (
    CursorError,
    DuplicateKeyConflict,
    FilterScopeError,
    MixedSnapshotError,
    PageContractError,
    SchemaError,
    SequenceError,
    SnapshotBoundError,
)

pytestmark = pytest.mark.unit


def deal(deal_id: str, source_seq: int, **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "deal_id": deal_id,
        "source_seq": source_seq,
        "account_id": "A1",
        "position_id": "P1",
        "symbol": "EURUSD",
        "entry_kind": "IN",
        "side": "BUY",
        "volume_lots": "0.10",
        "price": "1.10000",
        "realized_pnl_gross": "0.00",
        "commission": "-2.50",
        "swap": "0.00",
        "executed_at": "2026-01-01T00:00:00+00:00",
    }
    payload.update(overrides)
    return payload


def page(
    items: list[dict[str, object]],
    *,
    index: int = 0,
    next_cursor: str | None = None,
    snapshot_seq: int = 100,
    run_id: str = "r1",
    generation: int = 0,
    cursor_present: bool = True,
    entity: Entity = Entity.DEALS,
    filter_scope: str = FILTER_UNFILTERED,
    request_cursor: str | None = None,
) -> RawPage:
    return RawPage(
        source_id="sim_local",
        entity=entity,
        run_id=run_id,
        generation=generation,
        snapshot_seq=snapshot_seq,
        page_index=index,
        items=tuple(items),
        cursor_present=cursor_present,
        next_cursor=next_cursor,
        filter_scope=filter_scope,
        request_cursor=request_cursor,
    )


def state(**overrides: object) -> ScanValidationState:
    kwargs: dict[str, object] = {
        "entity": Entity.DEALS,
        "filter_scope": FILTER_UNFILTERED,
        "start_watermark": 0,
        "max_page_items": 500,
    }
    kwargs.update(overrides)
    return ScanValidationState(**kwargs)  # type: ignore[arg-type]


@pytest.mark.whatif("WF3-025")
def test_sequence_gaps_are_legal() -> None:
    """Deals at 1001, 1005 and 1010 are a normal stream, not missing data.

    Account, position and mark events consume sequence numbers too, so a gap
    carries no information about deals. Raising here would be a false alarm
    that trains operators to ignore the sequence checks (INV-11, fixture G03).
    """
    validated = validate_page(
        page([deal("D1", 1001), deal("D2", 1005), deal("D3", 1010)], snapshot_seq=1012),
        state(),
    )
    assert validated.record_count == 3
    assert validated.is_terminal


@pytest.mark.whatif("WF3-026")
def test_sequence_going_backwards_is_rejected() -> None:
    scan = state()
    validate_page(page([deal("D1", 10)], index=0, next_cursor="c1"), scan)
    with pytest.raises(SequenceError):
        validate_page(page([deal("D2", 5)], index=1, request_cursor="c1"), scan)


@pytest.mark.whatif("WF3-029")
def test_two_deals_sharing_a_sequence_are_rejected() -> None:
    """One event produces one deal; two ids at one sequence breaks the contract."""
    with pytest.raises(SequenceError) as caught:
        validate_page(page([deal("D1", 10), deal("D2", 10)]), state())
    assert caught.value.safe_details["source_seq"] == 10


@pytest.mark.whatif("WF3-032")
def test_record_above_the_snapshot_bound_is_rejected() -> None:
    """A deal above ``S`` means the page is not from the snapshot it claims.

    That snapshot then proves nothing about completeness, so it must not
    justify a watermark.
    """
    with pytest.raises(SnapshotBoundError):
        validate_page(page([deal("D1", 500)], snapshot_seq=100), state())


@pytest.mark.whatif("WF3-031")
def test_first_deal_far_above_the_watermark_is_accepted() -> None:
    """A long quiet period is not a gap error."""
    validated = validate_page(
        page([deal("D1", 9_000)], snapshot_seq=10_000), state(start_watermark=100)
    )
    assert validated.record_count == 1


@pytest.mark.whatif("WF3-030")
def test_record_at_or_below_the_watermark_is_counted_as_an_anomaly() -> None:
    """An inclusive ``after_seq`` upstream is reported, not silently accepted.

    The record is kept because the sink's idempotent insert makes it a no-op,
    so sync keeps working; the counter surfaces the contract mismatch instead
    of hiding it (AS-04).
    """
    scan = state(start_watermark=100)
    validate_page(page([deal("D1", 100), deal("D2", 101)], snapshot_seq=200), scan)
    assert scan.boundary_anomaly_count == 1


@pytest.mark.whatif("WF3-038")
def test_mixed_snapshot_across_pages_is_rejected() -> None:
    """Pages from two snapshots are not a snapshot of anything (INV-03)."""
    scan = state()
    validate_page(page([deal("D1", 10)], index=0, next_cursor="c1"), scan)
    with pytest.raises(MixedSnapshotError):
        validate_page(
            page([deal("D2", 20)], index=1, snapshot_seq=101, request_cursor="c1"), scan
        )


@pytest.mark.whatif("WF3-013")
def test_changed_run_id_across_pages_is_rejected() -> None:
    scan = state()
    validate_page(page([deal("D1", 10)], index=0, next_cursor="c1"), scan)
    with pytest.raises(MixedSnapshotError):
        validate_page(
            page([deal("D2", 20)], index=1, run_id="r2", request_cursor="c1"), scan
        )


@pytest.mark.whatif("WF3-039")
def test_repeated_cursor_is_detected_as_a_cycle() -> None:
    """Stop now rather than discover the loop through a page or time budget."""
    scan = state()
    validate_page(page([deal("D1", 10)], index=0, next_cursor="c1"), scan)
    with pytest.raises(CursorError):
        validate_page(
            page([deal("D2", 20)], index=1, next_cursor="c1", request_cursor="c1"), scan
        )


@pytest.mark.whatif("WF3-039")
def test_cursor_echoing_the_request_is_a_cycle() -> None:
    with pytest.raises(CursorError):
        validate_page(
            page([deal("D1", 10)], next_cursor="same", request_cursor="same"), state()
        )


@pytest.mark.whatif("WF3-040")
def test_empty_terminal_page_is_a_valid_complete_snapshot() -> None:
    """Zero new deals is a legitimate answer, and a scan may finish on it."""
    validated = validate_page(page([]), state())
    assert validated.is_terminal
    assert validated.record_count == 0


@pytest.mark.whatif("WF3-041")
def test_empty_page_with_a_cursor_continues_without_becoming_terminal() -> None:
    scan = state()
    validated = validate_page(page([], index=0, next_cursor="c1"), scan)
    assert not validated.is_terminal
    assert not scan.terminal_seen


@pytest.mark.whatif("WF3-042")
def test_page_above_the_item_cap_is_rejected() -> None:
    """Accepting an oversized page would void the memory and transaction bounds."""
    items = [deal(f"D{i}", i + 1) for i in range(6)]
    with pytest.raises(PageContractError) as caught:
        validate_page(page(items), state(max_page_items=5))
    assert caught.value.safe_details["item_count"] == 6


@pytest.mark.whatif("WF3-044")
def test_absent_cursor_field_is_a_schema_error_not_a_terminal_page() -> None:
    """The asymmetry that stops a truncated response from ending a scan.

    An explicit ``null`` means "no more pages"; a missing field means the
    response is not the shape we agreed on, and reading it as terminal would
    turn truncation into completion.
    """
    with pytest.raises(SchemaError) as caught:
        validate_page(page([deal("D1", 10)], cursor_present=False), state())
    assert caught.value.safe_details["field"] == "next_cursor"


@pytest.mark.whatif("WF3-043")
def test_identical_duplicate_inside_one_page_collapses() -> None:
    """Same id, same content: one record, one duplicate counted (INV-04)."""
    row = deal("D1", 10)
    validated = validate_page(page([row, dict(row)]), state())
    assert validated.record_count == 1
    assert validated.duplicate_count == 1


@pytest.mark.whatif("WF3-043", "WF3-099")
def test_conflicting_duplicate_inside_one_page_is_a_conflict() -> None:
    """Two different commissions for one id cannot both be stored.

    Caught before the SQL batch, so the outcome is a classified conflict rather
    than a cardinality-violation surprise from ``ON CONFLICT`` (WF3-099).
    """
    with pytest.raises(DuplicateKeyConflict):
        validate_page(page([deal("D1", 10), deal("D1", 11, commission="-99.00")]), state())


@pytest.mark.whatif("WF3-045")
def test_page_from_another_entity_is_rejected() -> None:
    """A cursor issued for accounts must not deliver rows into a deal scan."""
    with pytest.raises(FilterScopeError):
        validate_page(page([], entity=Entity.ACCOUNTS), state())


@pytest.mark.whatif("WF3-046")
def test_filter_scope_change_mid_scan_is_rejected() -> None:
    """Changing scope mid-scan moves the goalposts of the completeness claim."""
    scan = state()
    validate_page(page([deal("D1", 10)], index=0, next_cursor="c1"), scan)
    with pytest.raises(FilterScopeError):
        validate_page(
            page([deal("D2", 20)], index=1, filter_scope="account_id=A1", request_cursor="c1"),
            scan,
        )


@pytest.mark.whatif("WF3-044")
def test_pages_must_arrive_in_order() -> None:
    with pytest.raises(PageContractError):
        validate_page(page([deal("D1", 10)], index=3), state())


def test_no_page_may_follow_the_terminal_page() -> None:
    scan = state()
    validate_page(page([deal("D1", 10)], index=0), scan)
    with pytest.raises(PageContractError):
        validate_page(page([deal("D2", 20)], index=1), scan)


def test_a_rejected_page_does_not_advance_the_validator() -> None:
    """State mutates only after a page fully passes.

    Otherwise a rejected page leaves the chain half-advanced and the next
    legitimate page looks out of order.
    """
    scan = state()
    with pytest.raises(SequenceError):
        validate_page(page([deal("D1", 10), deal("D2", 10)]), scan)
    assert scan.expected_page_index == 0
    assert scan.seen_keys == {}


@pytest.mark.whatif("WF3-141")
def test_validation_cannot_detect_a_silently_omitted_record() -> None:
    """The limit of client-side checking, written down as a test.

    A source that drops a deal and then reports a terminal page produces a
    chain that validates perfectly. No rule in this module can catch it:
    completeness is not derivable from the metadata a source supplies. Only an
    independent oracle can, which is why the e2e suite compares against ground
    truth and why this case is never a PASS on validation alone (design 6.6).
    """
    validated = validate_page(page([deal("D1", 10)], snapshot_seq=100), state())
    assert validated.is_terminal and validated.record_count == 1
