"""FastAPI routes for the read-only analytics API. Section 8.1.

The routes are thin: parse, delegate, serialise. All the judgement lives in
:class:`~analytics.query.service.AnalyticsQueryService`, which is also how the
integration tests reach it without an HTTP client.

Two HTTP-specific decisions:

* **A refused answer is still a 200 with a status field**, for the business
  statuses (``STALE``, ``INCOMPLETE``, ``BLOCKED``, ``UNSUPPORTED``). They are
  answers about data, not transport errors, and a dashboard that only checks
  the HTTP code must not read them as a success with no rows. Authentication,
  scope and malformed-request failures do use HTTP codes, because those are
  not answers about data at all.
* **Money is serialised by hand** as strings. ``JSONResponse`` would happily
  turn a ``Decimal`` into a float somewhere down the line, so the service
  formats money before it reaches the encoder (section 7.2).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from ..errors import AnalyticsError, AuthenticationError, AuthorizationError, QueryError
from ..observability.metrics import Metrics
from .auth import AuthContext, Authenticator
from .service import AnalyticsQueryService

#: Business statuses returned with HTTP 200 and an explicit status field.
_DATA_STATUSES = {"READY", "STALE", "INCOMPLETE", "BLOCKED", "UNSUPPORTED", "DEGRADED"}


def _bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1].strip()
    return authorization.strip()


def build_router(
    *,
    service: AnalyticsQueryService,
    authenticator: Authenticator,
    metrics: Metrics,
) -> APIRouter:
    router = APIRouter(prefix="/internal/analytics/v1", tags=["analytics"])

    def auth_dependency(authorization: str | None = Header(default=None)) -> AuthContext:
        try:
            return authenticator.authenticate(_bearer(authorization))
        except AuthenticationError as exc:
            metrics.unauthorized_access_total.labels(kind=str(exc.reason)).inc()
            raise HTTPException(status_code=401, detail=exc.to_dict()) from exc

    @router.get("/daily")
    def daily(
        auth: AuthContext = Depends(auth_dependency),
        source_id: str = Query(...),
        epoch_id: str = Query(...),
        from_: date = Query(..., alias="from"),
        to: date = Query(...),
        report_family: str = Query("daily_deal_v1"),
        account_id: list[str] | None = Query(default=None),
        currency: str | None = Query(default=None),
        build_id: str | None = Query(default=None),
    ) -> JSONResponse:
        report = service.daily(
            auth,
            source_id=source_id,
            epoch_id=epoch_id,
            from_day=from_,
            to_day=to,
            report_family=report_family,
            accounts=account_id,
            currency=currency,
            build_id=build_id,
        )
        return _respond(report.as_wire())

    @router.get("/accounts/{account_id}/deals")
    def account_deals(
        account_id: str,
        auth: AuthContext = Depends(auth_dependency),
        source_id: str = Query(...),
        epoch_id: str = Query(...),
        report_family: str = Query("daily_deal_v1"),
        limit: int | None = Query(default=None),
        cursor: str | None = Query(default=None),
        sort: str = Query("executed_at"),
        build_id: str | None = Query(default=None),
    ) -> JSONResponse:
        page = service.account_deals(
            auth,
            source_id=source_id,
            epoch_id=epoch_id,
            account_id=account_id,
            report_family=report_family,
            limit=limit,
            cursor=cursor,
            sort=sort,
            build_id=build_id,
        )
        return _respond(page.as_wire())

    @router.get("/publications/current")
    def current_publication(
        auth: AuthContext = Depends(auth_dependency),
        source_id: str = Query(...),
        epoch_id: str = Query(...),
        report_family: str = Query("daily_deal_v1"),
    ) -> JSONResponse:
        try:
            payload = service.current_publication(
                auth, source_id=source_id, epoch_id=epoch_id, report_family=report_family
            )
        except AuthorizationError as exc:
            raise HTTPException(status_code=403, detail=exc.to_dict()) from exc
        return _respond(payload)

    @router.get("/publications/{build_id}/evidence")
    def evidence(
        build_id: str, auth: AuthContext = Depends(auth_dependency)
    ) -> JSONResponse:
        try:
            payload = service.build_evidence(auth, build_id=build_id)
        except AnalyticsError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.to_dict()) from exc
        return _respond(payload)

    @router.get("/accounts/{account_id}/exposure")
    def exposure(
        account_id: str,
        auth: AuthContext = Depends(auth_dependency),
        source_id: str = Query(...),
        epoch_id: str = Query(...),
    ) -> JSONResponse:
        """Declared unsupported. INV08-16, ADR08-07.

        The route exists so the refusal is discoverable and uniform: live
        exposure belongs to P05/P06, and a snapshot marker carries no
        holdings, so there is nothing here to compute from.
        """
        auth.scope.require_source(source_id)
        report = service.unsupported_metric(
            metric="account exposure",
            source_id=source_id,
            epoch_id=epoch_id,
            why=(
                "P08 holds historical Deal facts only; account holdings require a full "
                "snapshot contract and live exposure is owned by P05/P06"
            ),
        )
        del account_id
        return _respond(report.as_wire())

    return router


def _respond(payload: dict[str, Any]) -> JSONResponse:
    status = str(payload.get("status", "READY"))
    if status not in _DATA_STATUSES:  # pragma: no cover - statuses are a closed enum
        raise HTTPException(status_code=500, detail={"reason": "UNKNOWN_STATUS"})
    return JSONResponse(status_code=200, content=payload)


def create_app(
    *,
    service: AnalyticsQueryService,
    authenticator: Authenticator,
    metrics: Metrics,
    ready_probe=None,
) -> FastAPI:
    """Assemble the read-only app.

    ``/health/ready`` consults a probe rather than returning a constant: a
    process that is up but cannot resolve a publication is not ready, and
    saying otherwise sends traffic to a service that can only fail
    (section 8.1).
    """
    app = FastAPI(title="P08 Broker Analytics (read-only)", version="1.0.0")
    app.include_router(build_router(service=service, authenticator=authenticator, metrics=metrics))

    @app.get("/health/live")
    def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready")
    def ready() -> JSONResponse:
        if ready_probe is None:
            return JSONResponse(status_code=200, content={"status": "ready"})
        detail = ready_probe()
        code = 200 if detail.get("ready") else 503
        return JSONResponse(status_code=code, content=detail)

    @app.exception_handler(AnalyticsError)
    def handle_analytics_error(_request: Request, exc: AnalyticsError) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code, content=exc.to_dict())

    @app.exception_handler(QueryError)
    def handle_query_error(_request: Request, exc: QueryError) -> JSONResponse:
        return JSONResponse(status_code=400, content=exc.to_dict())

    return app
