"""Single-account risk and P&L endpoints.

Both routes are thin: parse, delegate, serialize. There is no arithmetic here,
and the account id from the path keeps its exact string form, so ``00042`` can
never collide with ``42``.
"""

from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Request
from fastapi.responses import Response

from ...authz.principal_context import Permission
from ...contracts.problem_details import ApiProblem
from ...contracts.requests import ACCOUNT_ID_PATTERN, parse_components, parse_rfc3339_utc
from ...contracts.responses import PnlResponse, ProblemResponse, RiskResponse
from ..deps import get_context, run_report
from ..errors import json_response, request_id_of

__all__ = ["router"]

router = APIRouter(tags=["risk"])

_RISK_QUERY_KEYS = frozenset(
    {
        "source_id",
        "components",
        "allow_partial",
        "epoch_id",
        "valuation_as_of",
        "risk_policy_version",
        "symbol_catalog_version",
    }
)
# Documented failure shapes, so a client can generate against the taxonomy
# rather than discovering codes at runtime.
_PROBLEM = {"model": ProblemResponse, "content": {"application/problem+json": {}}}
_COMMON_PROBLEMS = {
    401: _PROBLEM,
    403: _PROBLEM,
    404: _PROBLEM,
    409: _PROBLEM,
    422: _PROBLEM,
    429: _PROBLEM,
    502: _PROBLEM,
    503: _PROBLEM,
    504: _PROBLEM,
}

_PNL_QUERY_KEYS = frozenset(
    {
        "source_id",
        "period_start",
        "period_end",
        "allow_partial",
        "epoch_id",
        "risk_policy_version",
        "symbol_catalog_version",
    }
)


def _require_account_shape(account_id: str) -> str:
    import re

    if re.fullmatch(ACCOUNT_ID_PATTERN, account_id) is None:
        # A malformed id is an invalid request, not a lookup miss.
        raise ApiProblem("INVALID_REQUEST", "the account identifier has an unsupported shape")
    return account_id


def _parse_bool(raw: str | None, *, field: str, default: bool) -> bool:
    if raw is None:
        return default
    if raw in ("true", "false"):
        return raw == "true"
    raise ApiProblem("INVALID_REQUEST", f"{field} must be exactly true or false")


@router.get(
    "/accounts/{account_id}/risk",
    summary="Scoped exposure, realized and unrealized summaries",
    response_model=RiskResponse,
    responses=_COMMON_PROBLEMS,
)
async def account_risk(account_id: str, request: Request) -> Response:
    context = get_context(request)
    account = _require_account_shape(account_id)

    source_id = request.query_params.get("source_id")
    if not source_id:
        raise ApiProblem("INVALID_REQUEST", "source_id is required")
    components = parse_components(request.query_params.get("components"))
    allow_partial = _parse_bool(
        request.query_params.get("allow_partial"),
        field="allow_partial",
        default=not context.settings.strict_composite_default,
    )
    valuation_as_of = request.query_params.get("valuation_as_of")
    if valuation_as_of is not None:
        parse_rfc3339_utc(valuation_as_of, field="valuation_as_of")

    # A risk summary does not take a period: the capture anchors the realized
    # window to the publication instant of the pinned cut, and the response
    # reports the window it used.
    envelope = await run_report(
        request=request,
        context=context,
        endpoint_id="accounts.risk",
        permission=Permission.READ_ACCOUNT_RISK,
        components=components,
        allowed_query_keys=_RISK_QUERY_KEYS,
        source_id=source_id,
        account_ids=(account,),
        allow_partial=allow_partial,
        workload="heavy",
        epoch_id=request.query_params.get("epoch_id"),
        valuation_as_of=valuation_as_of,
        requested_risk_policy=request.query_params.get("risk_policy_version"),
        requested_symbol_catalog=request.query_params.get("symbol_catalog_version"),
    )
    return json_response(
        envelope.payload, status_code=envelope.http_status, request_id=request_id_of(request)
    )


@router.get(
    "/accounts/{account_id}/pnl",
    summary="Realized P&L components and fee coverage",
    response_model=PnlResponse,
    responses=_COMMON_PROBLEMS,
)
async def account_pnl(account_id: str, request: Request) -> Response:
    context = get_context(request)
    settings = context.settings
    account = _require_account_shape(account_id)

    source_id = request.query_params.get("source_id")
    if not source_id:
        raise ApiProblem("INVALID_REQUEST", "source_id is required")
    raw_start = request.query_params.get("period_start")
    raw_end = request.query_params.get("period_end")
    if not raw_start or not raw_end:
        raise ApiProblem("INVALID_REQUEST", "period_start and period_end are both required")

    start = parse_rfc3339_utc(raw_start, field="period_start")
    end = parse_rfc3339_utc(raw_end, field="period_end")
    if end <= start:
        raise ApiProblem(
            "INVALID_TIME_WINDOW", "period_end must be after period_start; the interval is half-open"
        )
    if end - start > timedelta(days=settings.max_pnl_window_days):
        raise ApiProblem(
            "HISTORICAL_WINDOW_TOO_LARGE",
            f"the realized window may not exceed {settings.max_pnl_window_days} days",
        )

    allow_partial = _parse_bool(
        request.query_params.get("allow_partial"),
        field="allow_partial",
        default=not settings.strict_composite_default,
    )

    envelope = await run_report(
        request=request,
        context=context,
        endpoint_id="accounts.pnl",
        permission=Permission.READ_ACCOUNT_PNL,
        components=("REALIZED",),
        allowed_query_keys=_PNL_QUERY_KEYS,
        source_id=source_id,
        account_ids=(account,),
        allow_partial=allow_partial,
        workload="heavy",
        epoch_id=request.query_params.get("epoch_id"),
        period_start_utc=_canonical(raw_start),
        period_end_utc=_canonical(raw_end),
        requested_risk_policy=request.query_params.get("risk_policy_version"),
        requested_symbol_catalog=request.query_params.get("symbol_catalog_version"),
    )

    payload = dict(envelope.payload)
    components = payload.pop("components")
    payload["realized"] = components["realized"]  # type: ignore[index]
    payload["period"] = {"start_utc": _canonical(raw_start), "end_utc": _canonical(raw_end)}
    return json_response(payload, status_code=envelope.http_status, request_id=request_id_of(request))


def _canonical(instant: str) -> str:
    """Normalise an accepted RFC3339 instant to the single wire form."""
    parsed = parse_rfc3339_utc(instant, field="instant")
    return parsed.isoformat().replace("+00:00", "Z")
