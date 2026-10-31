"""Exposure aggregation across the authorized scope.

Exposure is reported in each symbol's own base-currency units. It is not a
single universal notional number: summing EUR units and JPY units would be
arithmetic without meaning, so the grouping stays per symbol and the unit
currency travels with every bucket.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import Response

from ...authz.principal_context import Permission
from ...contracts.problem_details import ApiProblem
from ...contracts.requests import ACCOUNT_ID_PATTERN, SYMBOL_PATTERN
from ...contracts.responses import ExposureResponse, ProblemResponse
from ..deps import get_context, run_report
from ..errors import json_response, request_id_of

__all__ = ["router"]

router = APIRouter(tags=["exposure"])

_PROBLEM = {"model": ProblemResponse, "content": {"application/problem+json": {}}}
_COMMON_PROBLEMS = {code: _PROBLEM for code in (401, 403, 404, 409, 422, 429, 502, 503, 504)}

_QUERY_KEYS = frozenset(
    {
        "source_id",
        "account_ids",
        "symbol",
        "allow_partial",
        "epoch_id",
        "risk_policy_version",
        "symbol_catalog_version",
    }
)


@router.get(
    "/exposure",
    summary="Exposure by symbol for the authorized scope",
    response_model=ExposureResponse,
    responses=_COMMON_PROBLEMS,
)
async def exposure(request: Request) -> Response:
    import re

    context = get_context(request)
    settings = context.settings

    source_id = request.query_params.get("source_id")
    if not source_id:
        raise ApiProblem("INVALID_REQUEST", "source_id is required")

    raw_accounts = request.query_params.get("account_ids")
    account_ids = _parse_account_ids(raw_accounts, settings)

    symbol = request.query_params.get("symbol")
    if symbol is not None and re.fullmatch(SYMBOL_PATTERN, symbol) is None:
        raise ApiProblem("INVALID_REQUEST", "the symbol filter has an unsupported shape")

    allow_partial_raw = request.query_params.get("allow_partial")
    if allow_partial_raw not in (None, "true", "false"):
        raise ApiProblem("INVALID_REQUEST", "allow_partial must be exactly true or false")
    allow_partial = (
        allow_partial_raw == "true"
        if allow_partial_raw is not None
        else not settings.strict_composite_default
    )

    if account_ids is None:
        # No explicit selection: the principal's whole authorized set, which
        # requires the aggregate permission and is still named explicitly.
        principal_accounts = await _authorized_accounts(request, context, source_id)
        account_ids = principal_accounts

    aggregate = len(account_ids) > 1

    envelope = await run_report(
        request=request,
        context=context,
        endpoint_id="exposure",
        permission=Permission.READ_AGGREGATE if aggregate else Permission.READ_ACCOUNT_RISK,
        components=("EXPOSURE",),
        allowed_query_keys=_QUERY_KEYS,
        source_id=source_id,
        account_ids=account_ids,
        allow_partial=allow_partial,
        workload="heavy",
        aggregate=aggregate,
        symbol=symbol,
        epoch_id=request.query_params.get("epoch_id"),
        requested_risk_policy=request.query_params.get("risk_policy_version"),
        requested_symbol_catalog=request.query_params.get("symbol_catalog_version"),
    )

    payload = dict(envelope.payload)
    components = payload.pop("components")
    exposure_block = components["exposure"]  # type: ignore[index]
    group_count = len(exposure_block.get("by_symbol", {}))  # type: ignore[union-attr]
    if group_count > settings.max_aggregate_symbols:
        # Never truncate a financial aggregate silently.
        raise ApiProblem(
            "RESULT_TOO_LARGE",
            f"the report groups more than {settings.max_aggregate_symbols} symbols",
        )
    payload["exposure"] = exposure_block
    payload["group_count"] = group_count
    return json_response(payload, status_code=envelope.http_status, request_id=request_id_of(request))


def _parse_account_ids(raw: str | None, settings) -> tuple[str, ...] | None:
    """Parse the comma-separated filter. An empty value is an error, not "all"."""
    import re

    if raw is None:
        return None
    if raw == "":
        raise ApiProblem(
            "INVALID_REQUEST",
            "account_ids must not be empty; omit it to use the whole authorized scope",
        )
    parts = [part.strip() for part in raw.split(",")]
    if any(part == "" for part in parts):
        raise ApiProblem("INVALID_REQUEST", "account_ids must not contain empty members")
    for part in parts:
        if re.fullmatch(ACCOUNT_ID_PATTERN, part) is None:
            raise ApiProblem("INVALID_REQUEST", "an account identifier has an unsupported shape")
    unique = tuple(sorted(set(parts)))
    if len(unique) > settings.max_accounts_per_request:
        raise ApiProblem(
            "ACCOUNT_SCOPE_TOO_LARGE",
            f"at most {settings.max_accounts_per_request} accounts may be requested",
        )
    return unique


async def _authorized_accounts(request: Request, context, source_id: str) -> tuple[str, ...]:
    """Expand to the caller's authorized accounts, or deny if there are none."""
    from ..deps import authenticate, resolve_policy

    principal = authenticate(request, context)
    policy = await resolve_policy(principal, context)
    accounts = tuple(sorted(policy.accounts_for(source_id)))
    if not accounts:
        raise ApiProblem(
            "RESOURCE_NOT_FOUND",
            "the requested resource does not exist or is not visible to this principal",
            reason_codes=("SOURCE_OUT_OF_SCOPE",),
        )
    return accounts
