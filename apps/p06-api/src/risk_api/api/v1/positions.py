"""Position listing with scan-fenced keyset pagination.

This endpoint is a *current* listing, not a historical as-of snapshot. With
only a latest-published projection upstream, a page set cannot survive a new
publication, so the next page is refused rather than silently mixing scans.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import Response

from ...admission.budgets import Deadline
from ...app.pagination import filter_hash
from ...app.risk_service import positions_page_payload
from ...authz.principal_context import Permission, RequestedFilters
from ...authz.scope_policy import authorize_scope, require_permission
from ...contracts.problem_details import ApiProblem
from ...contracts.requests import ACCOUNT_ID_PATTERN, SYMBOL_PATTERN, enforce_query_allowlist
from ...contracts.responses import PositionsPage, ProblemResponse
from ..deps import (
    assert_accept_json,
    assert_query_budget,
    authenticate,
    get_context,
    resolve_policy,
)
from ..errors import json_response, request_id_of

__all__ = ["router"]

router = APIRouter(tags=["positions"])

_PROBLEM = {"model": ProblemResponse, "content": {"application/problem+json": {}}}
_COMMON_PROBLEMS = {code: _PROBLEM for code in (401, 403, 404, 409, 422, 429, 503)}

_QUERY_KEYS = frozenset({"source_id", "account_id", "symbol", "limit", "cursor", "epoch_id"})


@router.get(
    "/positions",
    summary="Current position listing for the authorized scope",
    response_model=PositionsPage,
    responses=_COMMON_PROBLEMS,
)
async def list_positions(request: Request) -> Response:
    import re

    context = get_context(request)
    settings = context.settings
    assert_query_budget(request, settings)
    assert_accept_json(request)
    enforce_query_allowlist(request.query_params.keys(), _QUERY_KEYS)

    source_id = request.query_params.get("source_id")
    if not source_id:
        raise ApiProblem("INVALID_REQUEST", "source_id is required")

    account_id = request.query_params.get("account_id")
    if account_id is not None and re.fullmatch(ACCOUNT_ID_PATTERN, account_id) is None:
        raise ApiProblem("INVALID_REQUEST", "the account identifier has an unsupported shape")
    symbol = request.query_params.get("symbol")
    if symbol is not None and re.fullmatch(SYMBOL_PATTERN, symbol) is None:
        raise ApiProblem("INVALID_REQUEST", "the symbol filter has an unsupported shape")

    limit = _parse_limit(request.query_params.get("limit"), settings)
    cursor_value = request.query_params.get("cursor")

    principal = authenticate(request, context)
    require_permission(
        principal, Permission.READ_POSITIONS, denial_family="ENDPOINT_NOT_PERMITTED"
    )
    policy = await resolve_policy(principal, context)

    permitted = policy.accounts_for(source_id)
    if account_id is not None:
        requested_accounts: tuple[str, ...] = (account_id,)
    else:
        if not permitted:
            raise ApiProblem(
                "RESOURCE_NOT_FOUND",
                "the requested resource does not exist or is not visible to this principal",
                reason_codes=("SOURCE_OUT_OF_SCOPE",),
            )
        # Listing "my positions" means exactly the authorized accounts, named.
        requested_accounts = tuple(sorted(permitted))
        if len(requested_accounts) > 1:
            require_permission(
                principal, Permission.READ_AGGREGATE, denial_family="AGGREGATE_NOT_PERMITTED"
            )

    scope = await authorize_scope(
        principal=principal,
        policy=policy,
        filters=RequestedFilters(
            source_id=source_id,
            account_ids=requested_accounts,
            symbol=symbol,
            epoch_id=request.query_params.get("epoch_id"),
        ),
        epochs=context.epochs(),
        allow_pinned_epoch=settings.allow_pinned_epoch_requests,
        aggregate=len(requested_accounts) > 1,
        max_accounts=settings.max_accounts_per_request,
    )

    if scope.permitted_symbols is not None and symbol is not None:
        if symbol not in scope.permitted_symbols:
            raise ApiProblem(
                "RESOURCE_NOT_FOUND",
                "the requested resource does not exist or is not visible to this principal",
                reason_codes=("SYMBOL_OUT_OF_SCOPE",),
            )

    filters = filter_hash(symbol=symbol, accounts=scope.accounts, page_size=limit)
    now = context.wall_clock()
    after_position_id: str | None = None
    cursor_payload = None
    if cursor_value is not None:
        cursor_payload = context.cursors.decode(cursor_value)
        context.cursors.authorize(cursor_payload, scope=scope, filters_hash=filters, now=now)
        after_position_id = cursor_payload.last_position_id

    deadline = Deadline.after(settings.request_deadline_seconds, clock=context.clock)
    async with context.admission.admit(
        tenant_id=scope.tenant_id, workload="cheap", deadline=deadline
    ):
        captured = await context.capturer.capture(
            scope=scope,
            components=("EXPOSURE",),
            period_start_utc=None,
            period_end_utc=None,
            symbol_catalog_version=settings.symbol_catalog_version,
            deadline=deadline,
        )
        scan_id = captured.cut.position_scan_id
        if scan_id is None:  # pragma: no cover - capture already guards this
            raise ApiProblem(
                "DATA_NOT_READY",
                "the position scan for this epoch is not published",
                reason_codes=("P03_UNPUBLISHED",),
            )
        if cursor_payload is not None:
            # The fence: a republished scan ends this page set.
            context.cursors.assert_same_scan(cursor_payload, scan_id)

        async with context.views.snapshot() as snapshot:
            rows = await snapshot.positions(
                scope.source_id,
                scope.epoch_id,
                scope.accounts,
                scan_id=scan_id,
                symbol=symbol,
                after_position_id=after_position_id,
                limit=limit + 1,
            )

        has_more = len(rows) > limit
        page = rows[:limit]
        next_cursor = None
        if has_more and page:
            next_cursor = context.cursors.encode(
                scope=scope,
                published_scan_id=scan_id,
                last_position_id=page[-1].position_id,
                filters_hash=filters,
                now=now,
            )

        payload = positions_page_payload(
            request_id=request_id_of(request),
            scope=scope,
            captured=captured,
            rows=page,
            page_size=limit,
            next_cursor=next_cursor,
            versions={
                "risk_policy": settings.risk_policy_version,
                "symbol_catalog": settings.symbol_catalog_version,
                "api_schema": settings.api_schema_version,
                "result_contract": settings.result_contract_version,
            },
        )
    return json_response(payload, status_code=200, request_id=request_id_of(request))


def _parse_limit(raw: str | None, settings) -> int:
    if raw is None:
        return settings.default_positions_page
    if not raw.isdigit():
        raise ApiProblem("INVALID_REQUEST", "limit must be a non-negative integer")
    value = int(raw)
    if value < 1:
        raise ApiProblem("INVALID_REQUEST", "limit must be at least 1")
    if value > settings.max_positions_page:
        raise ApiProblem(
            "LIMIT_EXCEEDED",
            f"limit may not exceed {settings.max_positions_page}",
        )
    return value
