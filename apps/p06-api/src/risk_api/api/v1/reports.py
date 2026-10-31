"""Daily realized-deal report.

Scope is deliberately narrow: realized deal activity inside one UTC calendar
day, over the half-open interval ``[00:00Z, next 00:00Z)``. The report makes no
claim about the end-of-day position snapshot, because upstream cannot prove one.
An incomplete day is a blocker, never a zero.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Request
from fastapi.responses import Response

from ...authz.principal_context import Permission
from ...contracts.problem_details import ApiProblem
from ...contracts.requests import ACCOUNT_ID_PATTERN, parse_utc_date
from ...contracts.responses import DailyReport, ProblemResponse
from ..deps import get_context, run_report
from ..errors import json_response, request_id_of

__all__ = ["router"]

router = APIRouter(tags=["reports"])

_PROBLEM = {"model": ProblemResponse, "content": {"application/problem+json": {}}}
_COMMON_PROBLEMS = {code: _PROBLEM for code in (401, 403, 404, 409, 422, 429, 502, 503, 504)}

_QUERY_KEYS = frozenset(
    {"source_id", "date", "account_id", "allow_partial", "epoch_id", "risk_policy_version", "symbol_catalog_version"}
)


@router.get(
    "/reports/daily",
    summary="Realized deal summary for one UTC day",
    response_model=DailyReport,
    responses=_COMMON_PROBLEMS,
)
async def daily_report(request: Request) -> Response:
    import re

    context = get_context(request)
    settings = context.settings

    source_id = request.query_params.get("source_id")
    if not source_id:
        raise ApiProblem("INVALID_REQUEST", "source_id is required")

    raw_date = request.query_params.get("date")
    if not raw_date:
        raise ApiProblem("INVALID_REQUEST", "date is required and must be a UTC calendar day")
    report_date = parse_utc_date(raw_date, field="date")

    account_id = request.query_params.get("account_id")
    if account_id is None:
        raise ApiProblem(
            "INVALID_REQUEST",
            "account_id is required; a broker-wide daily report is not offered by this version",
        )
    if re.fullmatch(ACCOUNT_ID_PATTERN, account_id) is None:
        raise ApiProblem("INVALID_REQUEST", "the account identifier has an unsupported shape")

    allow_partial_raw = request.query_params.get("allow_partial")
    if allow_partial_raw not in (None, "true", "false"):
        raise ApiProblem("INVALID_REQUEST", "allow_partial must be exactly true or false")
    allow_partial = (
        allow_partial_raw == "true"
        if allow_partial_raw is not None
        else not settings.strict_composite_default
    )

    start = datetime(report_date.year, report_date.month, report_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    today = datetime.now(timezone.utc).date()
    if report_date > today:
        raise ApiProblem(
            "INVALID_TIME_WINDOW", "a future calendar day cannot be reported on"
        )

    envelope = await run_report(
        request=request,
        context=context,
        endpoint_id="reports.daily",
        permission=Permission.READ_DAILY_REPORT,
        components=("REALIZED",),
        allowed_query_keys=_QUERY_KEYS,
        source_id=source_id,
        account_ids=(account_id,),
        allow_partial=allow_partial,
        workload="heavy",
        epoch_id=request.query_params.get("epoch_id"),
        period_start_utc=_iso(start),
        period_end_utc=_iso(end),
        requested_risk_policy=request.query_params.get("risk_policy_version"),
        requested_symbol_catalog=request.query_params.get("symbol_catalog_version"),
    )

    payload = dict(envelope.payload)
    components = payload.pop("components")
    payload["realized"] = components["realized"]  # type: ignore[index]
    payload["report_date"] = report_date.isoformat()
    payload["period"] = {"start_utc": _iso(start), "end_utc": _iso(end)}
    return json_response(payload, status_code=envelope.http_status, request_id=request_id_of(request))


def _iso(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")
