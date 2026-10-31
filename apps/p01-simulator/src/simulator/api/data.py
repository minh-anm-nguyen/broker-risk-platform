"""The read-only data plane (``GET /v1/*``).

Every listing here follows the same shape, and the shape is the contract:

1. Resolve the snapshot. A request without a cursor pins ``snapshot_seq`` to
   the run's current ``committed_seq``; a request with one takes the sequence
   from the signed cursor. Either way the sequence does not move while the
   scan runs.
2. Query with keyset pagination, over-fetching by one row to learn whether a
   next page exists.
3. Issue a cursor **only if** there is a next page. Minting a lease for a
   single-page response would burn the active-snapshot budget on scans that
   are already finished (WF-043).

Nothing on this plane can mutate anything. There is no route here that writes,
resets or injects - that separation is what makes it safe to hand a data-plane
token to a downstream system under test (WF-101).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from fastapi import APIRouter, Depends, Query, Request

from .. import GENERATOR_VERSION, __version__
from ..canonical import filter_digest, format_timestamp, parse_timestamp, to_epoch_ms
from ..domain import accounting as acc
from ..domain.accounting import ACCOUNTING_POLICY_VERSION
from ..domain.models import (
    ACCOUNT_CURRENCY,
    SYMBOL_CATALOG_VERSION,
    SYMBOL_SPECS,
    InvalidCursorError,
    Mark,
    NotFoundError,
    Position,
    PositionStatus,
    ValidationError,
    symbol_spec,
)
from ..observability.logging import bind_run_id
from ..persistence import event_store as es
from ..persistence.cursors import SnapshotLease
from .deps import AppContext, get_context, require_data_plane, resolve_limit
from .schemas import (
    AccountDetailOut,
    AccountOut,
    AccountsPage,
    CapabilitiesOut,
    DealOut,
    DealsPage,
    MarkOut,
    MarksPage,
    MetaOut,
    PositionOut,
    PositionsPage,
)

router = APIRouter(prefix="/v1", tags=["data"], dependencies=[Depends(require_data_plane)])


# ---------------------------------------------------------------------------
# Scan resolution
# ---------------------------------------------------------------------------


class _Scan:
    """Resolved snapshot coordinates for one list request."""

    __slots__ = ("run", "snapshot_seq", "filters", "digest", "last_key", "lease")

    def __init__(
        self,
        run: es.RunSummary,
        snapshot_seq: int,
        filters: dict[str, Any],
        digest: str,
        last_key: list[Any] | None,
        lease: SnapshotLease | None,
    ) -> None:
        self.run = run
        self.snapshot_seq = snapshot_seq
        self.filters = filters
        self.digest = digest
        self.last_key = last_key
        self.lease = lease


def _resolve_scan(
    request: Request,
    context: AppContext,
    *,
    entity: str,
    cursor: str | None,
    filters: dict[str, Any],
) -> _Scan:
    run = context.coordinator.require_active_run()
    bind_run_id(run.run_id)
    request.state.run_id = run.run_id

    if cursor is None:
        return _Scan(run, run.committed_seq, filters, filter_digest(filters), None, None)

    payload = context.codec.decode(cursor)
    # Defence in depth: the token is signed, but a payload whose digest does
    # not match its own filters would mean the two were written inconsistently.
    if filter_digest(payload.filters) != payload.filter_digest:
        raise InvalidCursorError("cursor filter digest does not match its filters")

    # Only filters the client *explicitly* sent are compared. A client that
    # sends the cursor alone is paginating normally; one that sends a different
    # account_id or limit alongside it is changing the scan, which is an error
    # rather than a silent reinterpretation (WF-036).
    explicit = set(request.query_params.keys()) - {"cursor"}
    mismatched = sorted(
        name
        for name in explicit
        if name in filters and filters[name] != payload.filters.get(name)
    )
    if mismatched:
        raise InvalidCursorError(
            "request filters do not match the cursor",
            details={
                "mismatched_filters": mismatched,
                "hint": "start a new scan when changing any filter, including limit",
            },
        )

    lease = context.snapshots.validate(
        payload,
        active_run_id=run.run_id,
        active_generation=run.generation,
        entity=entity,
        filter_digest=payload.filter_digest,
    )
    return _Scan(
        run, payload.snapshot_seq, payload.filters, payload.filter_digest, payload.last_key, lease
    )


def _next_cursor(context: AppContext, scan: _Scan, entity: str, last_key: list[Any] | None) -> str | None:
    if last_key is None:
        return None
    lease = scan.lease or context.snapshots.open(
        run_id=scan.run.run_id,
        generation=scan.run.generation,
        entity=entity,
        snapshot_seq=scan.snapshot_seq,
        filter_digest=scan.digest,
    )
    return context.snapshots.issue(context.codec, lease, last_key, scan.filters)


def _envelope(scan: _Scan, next_cursor: str | None) -> dict[str, Any]:
    return {
        "run_id": scan.run.run_id,
        "generation": scan.run.generation,
        "snapshot_seq": scan.snapshot_seq,
        "next_cursor": next_cursor,
    }


# ---------------------------------------------------------------------------
# Marks and derived valuation
# ---------------------------------------------------------------------------


def _marks_at(context: AppContext, run_id: str, snapshot_seq: int) -> dict[str, Mark]:
    marks: dict[str, Mark] = {}
    last_key: list[Any] | None = None
    while True:
        page = context.repository.list_marks(
            run_id=run_id,
            snapshot_seq=snapshot_seq,
            last_key=last_key,
            limit=context.settings.max_page_size,
        )
        marks.update({mark.symbol: mark for mark in page.items})
        if page.last_key is None:
            return marks
        last_key = page.last_key


def _is_stale(context: AppContext, run: es.RunSummary, mark: Mark) -> bool:
    age_ms = to_epoch_ms(run.virtual_time) - to_epoch_ms(mark.market_time)
    return age_ms > context.settings.mark_stale_after_ms


def _position_out(
    context: AppContext, run: es.RunSummary, position: Position, marks: dict[str, Mark]
) -> PositionOut:
    unrealized = None
    market_time = None
    stale = None
    mark = marks.get(position.symbol)
    if mark is not None:
        market_time = format_timestamp(mark.market_time)
        stale = _is_stale(context, run, mark)
        if position.status is PositionStatus.OPEN:
            spec = symbol_spec(position.symbol)
            unrealized = acc.unrealized_pnl(
                position.side, position.open_price, position.volume_lots, spec, mark
            )
    return PositionOut(
        position_id=position.position_id,
        account_id=position.account_id,
        symbol=position.symbol,
        side=position.side.value,
        volume_lots=position.volume_lots,
        open_price=position.open_price,
        status=position.status.value,
        # Left as None when no mark exists in this snapshot. Reporting 0 would
        # be a confidently wrong number (section 3.4).
        unrealized_pnl=unrealized,
        mark_market_time=market_time,
        mark_stale=stale,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/meta", response_model=MetaOut, summary="Capabilities and active run state")
async def get_meta(request: Request) -> MetaOut:
    context = get_context(request)
    run = context.repository.active_run()
    capabilities = CapabilitiesOut(
        api_version="v1",
        generator_version=GENERATOR_VERSION,
        accounting_policy_version=ACCOUNTING_POLICY_VERSION,
        symbol_catalog_version=SYMBOL_CATALOG_VERSION,
        # The active run's symbol list when there is one, otherwise the whole
        # catalog: a connector discovering capabilities before any run exists
        # still needs to know what this build can simulate.
        supported_symbols=sorted(run.spec_payload.get("symbols", [])) if run else sorted(SYMBOL_SPECS),
        account_currency=ACCOUNT_CURRENCY,
        default_page_size=context.settings.default_page_size,
        max_page_size=context.settings.max_page_size,
        snapshot_ttl_seconds=context.settings.snapshot_ttl_sec,
    )
    if run is None:
        # A meta call must answer even before any run exists, so a connector
        # can discover capabilities and then wait.
        return MetaOut(
            run_id=None,
            generation=None,
            scenario_name=None,
            seed=None,
            spec_hash=None,
            status=None,
            snapshot_seq=0,
            virtual_time=None,
            start_virtual_time=None,
            account_count=0,
            deal_count=0,
            open_position_count=0,
            capabilities=capabilities,
        )
    request.state.run_id = run.run_id
    bind_run_id(run.run_id)
    return MetaOut(
        run_id=run.run_id,
        generation=run.generation,
        scenario_name=run.scenario_name,
        seed=run.seed,
        spec_hash=run.spec_hash,
        status=run.status.value,
        snapshot_seq=run.committed_seq,
        virtual_time=format_timestamp(run.virtual_time),
        start_virtual_time=format_timestamp(run.start_virtual_time),
        account_count=run.account_count,
        deal_count=run.deal_count,
        open_position_count=run.open_position_count,
        capabilities=capabilities,
    )


@router.get("/accounts", response_model=AccountsPage, summary="List accounts at a snapshot")
async def list_accounts(
    request: Request,
    cursor: str | None = Query(default=None, max_length=4096),
    limit: int | None = Query(default=None),
) -> AccountsPage:
    context = get_context(request)
    page_size = resolve_limit(context, limit)
    scan = _resolve_scan(
        request, context, entity="accounts", cursor=cursor, filters={"limit": page_size}
    )
    page = context.repository.list_accounts(
        run_id=scan.run.run_id,
        snapshot_seq=scan.snapshot_seq,
        last_key=scan.last_key,
        limit=int(scan.filters["limit"]),
    )
    return AccountsPage(
        **_envelope(scan, _next_cursor(context, scan, "accounts", page.last_key)),
        items=[
            AccountOut(
                account_id=item.account_id,
                currency=item.currency,
                balance=item.balance,
                credit=item.credit,
            )
            for item in page.items
        ],
    )


@router.get(
    "/accounts/{account_id}",
    response_model=AccountDetailOut,
    summary="Read one account, optionally at a historical sequence",
)
async def get_account(
    request: Request,
    account_id: str,
    as_of_seq: int | None = Query(default=None, ge=0),
) -> AccountDetailOut:
    context = get_context(request)
    run = context.coordinator.require_active_run()
    request.state.run_id = run.run_id
    bind_run_id(run.run_id)
    sequence = run.committed_seq if as_of_seq is None else as_of_seq
    if sequence > run.committed_seq:
        raise ValidationError(
            "as_of_seq is beyond the committed sequence",
            details={"field": "as_of_seq", "committed_seq": run.committed_seq},
        )
    account = context.repository.get_account(
        run_id=run.run_id, account_id=account_id, as_of_seq=sequence
    )
    if account is None:
        raise NotFoundError(
            f"account {account_id!r} does not exist at seq {sequence}",
            details={"account_id": account_id, "as_of_seq": sequence},
        )

    marks = _marks_at(context, run.run_id, sequence)
    positions: list[Position] = []
    last_key: list[Any] | None = None
    while True:
        page = context.repository.list_positions(
            run_id=run.run_id,
            snapshot_seq=sequence,
            account_id=account_id,
            status=PositionStatus.OPEN.value,
            last_key=last_key,
            limit=context.settings.max_page_size,
        )
        positions.extend(page.items)
        if page.last_key is None:
            break
        last_key = page.last_key

    total = acc.quantize_money(Decimal(0), field="unrealized_pnl")
    complete = True
    for position in positions:
        mark = marks.get(position.symbol)
        if mark is None:
            complete = False
            continue
        spec = symbol_spec(position.symbol)
        total += acc.unrealized_pnl(
            position.side, position.open_price, position.volume_lots, spec, mark
        )
    # Either every open position could be valued, or the derived figures are
    # withheld entirely. A partial total presented as "the" unrealized P&L
    # would be wrong without saying so.
    unrealized = total if complete else None
    return AccountDetailOut(
        account_id=account.account_id,
        currency=account.currency,
        balance=account.balance,
        credit=account.credit,
        as_of_seq=sequence,
        unrealized_pnl=unrealized,
        equity=acc.equity(account.balance, total) if complete else None,
        open_position_count=len(positions),
        data_quality="COMPLETE" if complete else "MARK_MISSING",
    )


@router.get("/positions", response_model=PositionsPage, summary="List positions at a snapshot")
async def list_positions(
    request: Request,
    account_id: str | None = Query(default=None, max_length=64),
    symbol: str | None = Query(default=None, max_length=32),
    status: str | None = Query(default=None, pattern="^(OPEN|CLOSED)$"),
    cursor: str | None = Query(default=None, max_length=4096),
    limit: int | None = Query(default=None),
) -> PositionsPage:
    context = get_context(request)
    page_size = resolve_limit(context, limit)
    if symbol is not None:
        symbol_spec(symbol)  # 422 for unknown, UNSUPPORTED_OPERATION for FX-converted
    filters = {
        "account_id": account_id,
        "symbol": symbol,
        "status": status,
        "limit": page_size,
    }
    scan = _resolve_scan(request, context, entity="positions", cursor=cursor, filters=filters)
    effective = scan.filters
    page = context.repository.list_positions(
        run_id=scan.run.run_id,
        snapshot_seq=scan.snapshot_seq,
        account_id=effective.get("account_id"),
        symbol=effective.get("symbol"),
        status=effective.get("status"),
        last_key=scan.last_key,
        limit=int(effective["limit"]),
    )
    marks = _marks_at(context, scan.run.run_id, scan.snapshot_seq)
    return PositionsPage(
        **_envelope(scan, _next_cursor(context, scan, "positions", page.last_key)),
        items=[_position_out(context, scan.run, item, marks) for item in page.items],
    )


@router.get("/deals", response_model=DealsPage, summary="List deals, or sync incrementally")
async def list_deals(
    request: Request,
    account_id: str | None = Query(default=None, max_length=64),
    symbol: str | None = Query(default=None, max_length=32),
    from_time: str | None = Query(default=None, max_length=64),
    to_time: str | None = Query(default=None, max_length=64),
    after_seq: int | None = Query(default=None, ge=0),
    cursor: str | None = Query(default=None, max_length=4096),
    limit: int | None = Query(default=None),
) -> DealsPage:
    context = get_context(request)
    page_size = resolve_limit(context, limit)
    if symbol is not None:
        symbol_spec(symbol)
    from_ms = _parse_time_bound(from_time, "from_time")
    to_ms = _parse_time_bound(to_time, "to_time")
    if from_ms is not None and to_ms is not None and to_ms < from_ms:
        raise ValidationError(
            "to_time must not precede from_time",
            details={"field": "to_time", "convention": "[from_time, to_time)"},
        )
    filters = {
        "account_id": account_id,
        "symbol": symbol,
        "from_time": from_time,
        "to_time": to_time,
        "after_seq": after_seq,
        "limit": page_size,
    }
    scan = _resolve_scan(request, context, entity="deals", cursor=cursor, filters=filters)
    effective = scan.filters
    page = context.repository.list_deals(
        run_id=scan.run.run_id,
        snapshot_seq=scan.snapshot_seq,
        account_id=effective.get("account_id"),
        symbol=effective.get("symbol"),
        from_time_ms=_parse_time_bound(effective.get("from_time"), "from_time"),
        to_time_ms=_parse_time_bound(effective.get("to_time"), "to_time"),
        after_seq=effective.get("after_seq"),
        last_key=scan.last_key,
        limit=int(effective["limit"]),
    )
    return DealsPage(
        **_envelope(scan, _next_cursor(context, scan, "deals", page.last_key)),
        items=[
            DealOut(
                deal_id=item.deal_id,
                source_seq=item.source_seq,
                account_id=item.account_id,
                position_id=item.position_id,
                symbol=item.symbol,
                entry=item.entry.value,
                side=item.side.value,
                volume_lots=item.volume_lots,
                price=item.price,
                realized_pnl_gross=item.realized_pnl_gross,
                commission=item.commission,
                swap=item.swap,
                executed_at=format_timestamp(item.executed_at),
            )
            for item in page.items
        ],
    )


@router.get("/marks", response_model=MarksPage, summary="List simulated market quotes")
async def list_marks(
    request: Request,
    symbols: str | None = Query(default=None, max_length=512),
    cursor: str | None = Query(default=None, max_length=4096),
    limit: int | None = Query(default=None),
) -> MarksPage:
    context = get_context(request)
    page_size = resolve_limit(context, limit)
    requested: tuple[str, ...] | None = None
    if symbols:
        parts = tuple(part.strip() for part in symbols.split(",") if part.strip())
        if not parts:
            raise ValidationError("symbols must not be empty", details={"field": "symbols"})
        if len(parts) > 32:
            raise ValidationError(
                "too many symbols requested", details={"field": "symbols", "maximum": 32}
            )
        for part in parts:
            symbol_spec(part)
        requested = parts
    filters = {"symbols": list(requested) if requested else None, "limit": page_size}
    scan = _resolve_scan(request, context, entity="marks", cursor=cursor, filters=filters)
    effective = scan.filters
    effective_symbols = effective.get("symbols")
    page = context.repository.list_marks(
        run_id=scan.run.run_id,
        snapshot_seq=scan.snapshot_seq,
        symbols=tuple(effective_symbols) if effective_symbols else None,
        last_key=scan.last_key,
        limit=int(effective["limit"]),
    )
    return MarksPage(
        **_envelope(scan, _next_cursor(context, scan, "marks", page.last_key)),
        items=[
            MarkOut(
                symbol=item.symbol,
                bid=item.bid,
                ask=item.ask,
                market_time=format_timestamp(item.market_time),
                stale=_is_stale(context, scan.run, item),
            )
            for item in page.items
        ],
    )


def _parse_time_bound(value: Any, field: str) -> int | None:
    if value is None:
        return None
    try:
        return to_epoch_ms(parse_timestamp(str(value)))
    except ValueError as exc:
        raise ValidationError(
            f"{field} must be an RFC 3339 timestamp",
            details={"field": field, "example": "2026-10-30T00:00:00Z"},
        ) from exc
