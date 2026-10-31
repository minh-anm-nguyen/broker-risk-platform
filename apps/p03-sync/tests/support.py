"""Builders shared by the database-backed suites.

Pages handed to the sink always come through :func:`validate_page`, never
constructed directly. A test that fabricated a ``ValidatedPage`` would be
asserting against a page the real pipeline could never produce, and would
quietly stop covering the validator it bypassed.
"""

from __future__ import annotations

from typing import Any, Iterable

from trading_sync.domain.identifiers import FILTER_UNFILTERED
from trading_sync.domain.snapshots import Entity, RawPage, ValidatedPage
from trading_sync.domain.validation import ScanValidationState, validate_page

__all__ = [
    "account_payload",
    "deal_payload",
    "deal_scan_state",
    "make_page",
    "position_payload",
    "snapshot_scan_state",
]


def deal_payload(deal_id: str, source_seq: int, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
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


def account_payload(account_id: str, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "account_id": account_id,
        "currency": "USD",
        "balance": "1000.00",
        "credit": "0.00",
    }
    payload.update(overrides)
    return payload


def position_payload(position_id: str, **overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "position_id": position_id,
        "account_id": "A1",
        "symbol": "EURUSD",
        "side": "BUY",
        "volume_lots": "0.10",
        "open_price": "1.10000",
        "source_status": "OPEN",
    }
    payload.update(overrides)
    return payload


def deal_scan_state(*, start_watermark: int = 0, snapshot_seq: int = 1000) -> ScanValidationState:
    return ScanValidationState(
        entity=Entity.DEALS,
        filter_scope=FILTER_UNFILTERED,
        start_watermark=start_watermark,
        snapshot_seq_bound=snapshot_seq,
    )


def snapshot_scan_state(entity: Entity) -> ScanValidationState:
    return ScanValidationState(
        entity=entity, filter_scope=FILTER_UNFILTERED, start_watermark=0
    )


def make_page(
    state: ScanValidationState,
    items: Iterable[dict[str, Any]],
    *,
    index: int,
    snapshot_seq: int,
    terminal: bool,
    run_id: str = "run_0001",
    generation: int = 0,
    filter_scope: str = FILTER_UNFILTERED,
    entity: Entity | None = None,
) -> ValidatedPage:
    """Build and validate one page in the scan's own order."""
    raw = RawPage(
        source_id="sim_local",
        entity=entity if entity is not None else state.entity,
        run_id=run_id,
        generation=generation,
        snapshot_seq=snapshot_seq,
        page_index=index,
        items=tuple(items),
        cursor_present=True,
        next_cursor=None if terminal else f"cur-{index + 1}",
        filter_scope=filter_scope,
        request_cursor=None if index == 0 else f"cur-{index}",
    )
    return validate_page(raw, state)
