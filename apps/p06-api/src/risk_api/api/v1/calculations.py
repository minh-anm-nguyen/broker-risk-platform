"""Rich read-only calculation endpoint.

A POST here computes; it changes nothing. The body is parsed under hard size,
encoding, depth and duplicate-member bounds *before* the schema runs, and the
account list is intersected with the server-side policy afterwards. Strict
composite is the default: a partial report requires ``allow_partial: true``.
"""

from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Request
from fastapi.responses import Response
from pydantic import ValidationError

from ...authz.principal_context import Permission
from ...contracts.problem_details import ApiProblem
from ...contracts.requests import RiskCalculationRequest, decode_json_body, parse_rfc3339_utc
from ...contracts.responses import ProblemResponse, RiskResponse
from ..deps import assert_accept_json, assert_json_content_type, get_context, run_report
from ..errors import json_response, request_id_of

__all__ = ["router"]

router = APIRouter(tags=["risk"])

_PROBLEM = {"model": ProblemResponse, "content": {"application/problem+json": {}}}
_COMMON_PROBLEMS = {
    code: _PROBLEM for code in (401, 403, 404, 409, 413, 415, 422, 429, 502, 503, 504)
}


@router.post(
    "/risk-calculations",
    summary="Typed read-only risk calculation",
    response_model=RiskResponse,
    responses=_COMMON_PROBLEMS,
)
async def risk_calculations(request: Request) -> Response:
    context = get_context(request)
    settings = context.settings

    assert_accept_json(request)
    assert_json_content_type(request, reject_encoding=settings.reject_content_encoding)

    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > settings.max_body_bytes:
        # Refuse before reading the body off the socket.
        raise ApiProblem("REQUEST_TOO_LARGE", "request body exceeds the configured limit")

    raw = await request.body()
    decoded = decode_json_body(
        raw, max_bytes=settings.max_body_bytes, max_depth=settings.max_json_depth
    )

    try:
        body = RiskCalculationRequest.model_validate(decoded)
    except ValidationError as exc:
        fields = sorted({".".join(str(p) for p in err["loc"]) for err in exc.errors()})
        raise ApiProblem(
            "INVALID_REQUEST",
            f"the request body failed schema validation: {', '.join(fields[:8])}",
        ) from exc

    accounts = body.canonical_accounts()
    if len(accounts) > settings.max_accounts_per_request:
        raise ApiProblem(
            "ACCOUNT_SCOPE_TOO_LARGE",
            f"at most {settings.max_accounts_per_request} accounts may be requested",
        )

    components = body.canonical_components()
    period_start = period_end = None
    if "REALIZED" in components:
        start = parse_rfc3339_utc(body.period_start_utc or "", field="period_start_utc")
        end = parse_rfc3339_utc(body.period_end_utc or "", field="period_end_utc")
        if end <= start:
            raise ApiProblem(
                "INVALID_TIME_WINDOW",
                "period_end_utc must be after period_start_utc; the interval is half-open",
            )
        if end - start > timedelta(days=settings.max_pnl_window_days):
            raise ApiProblem(
                "HISTORICAL_WINDOW_TOO_LARGE",
                f"the realized window may not exceed {settings.max_pnl_window_days} days",
            )
        period_start = start.isoformat().replace("+00:00", "Z")
        period_end = end.isoformat().replace("+00:00", "Z")

    if body.valuation_as_of is not None:
        parse_rfc3339_utc(body.valuation_as_of, field="valuation_as_of")

    aggregate = len(accounts) > 1
    envelope = await run_report(
        request=request,
        context=context,
        endpoint_id="risk_calculations",
        permission=Permission.SUBMIT_CALCULATION,
        components=components,
        # A POST carries no query parameters at all.
        allowed_query_keys=frozenset(),
        source_id=body.source_id,
        account_ids=accounts,
        allow_partial=body.allow_partial,
        workload="heavy",
        aggregate=aggregate,
        epoch_id=body.epoch_id,
        period_start_utc=period_start,
        period_end_utc=period_end,
        valuation_as_of=body.valuation_as_of,
        requested_risk_policy=body.risk_policy_version,
        requested_symbol_catalog=body.symbol_catalog_version,
    )
    return json_response(
        envelope.payload, status_code=envelope.http_status, request_id=request_id_of(request)
    )
