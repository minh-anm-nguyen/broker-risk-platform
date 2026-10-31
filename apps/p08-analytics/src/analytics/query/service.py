"""The read service. Section 8.1-8.2.

Each public method follows the same shape, and the order is the contract:

1. authorise and narrow the scope;
2. validate and bound the parameters;
3. resolve the publication pointer **once** and bind its ``build_id``;
4. take an admission slot;
5. query only that build, inside that scope;
6. answer with the status, the build id, the policy and the coverage.

Step 3 before step 5 is what makes a response internally consistent. Step 6
matters as much: a number without its ``fee_coverage`` and ``policy_version``
is not an answer a downstream consumer can use correctly, so they are not
optional fields (section 8.1).

Net P&L is included only when the pinned policy permits it, and always with
its label. ``NET_COMPONENTS_COVERED`` is not ``NET_FULL`` and the response
says which one it is (INV08-15, G08-20).
"""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from typing import Any, Iterator, Mapping, Sequence

from ..clock import Clock
from ..contracts.policy import AccountingPolicy
from ..errors import (
    AnalyticsError,
    CompletenessError,
    OverloadError,
    QueryError,
    Reason,
    ReportStatus,
)
from ..faults.killpoints import FaultPlan, KillPoint, NullFaultPlan
from ..money import checked_sum, format_money
from ..observability.metrics import Metrics
from ..ports.clickhouse import ClickHousePort
from ..publication.registry import PublicationRegistry, ResolvedPublication
from ..settings import Settings
from .auth import AuthContext
from .cursors import CursorCodec, CursorPayload, filter_digest

#: Sort keys a client may request. An allowlist, not a pass-through, because
#: an arbitrary sort expression is an injection surface (section 8.2).
ALLOWED_DEAL_SORTS: frozenset[str] = frozenset({"executed_at", "deal_id", "account_id"})


@dataclass(frozen=True, slots=True)
class ReportEnvelope:
    """Fields every answer carries, whatever the route. Section 8.1."""

    status: ReportStatus
    build_id: str | None
    source_id: str
    epoch_id: str
    policy_version: str
    reporting_cut: str | None
    freshness_seconds: int | None
    fee_coverage: str
    publication_revision: int | None = None
    reason: str | None = None
    message: str | None = None

    def as_wire(self) -> dict[str, Any]:
        return {
            "status": str(self.status),
            "build_id": self.build_id,
            "source_id": self.source_id,
            "epoch_id": self.epoch_id,
            "policy_version": self.policy_version,
            "reporting_cut": self.reporting_cut,
            "freshness_seconds": self.freshness_seconds,
            "fee_coverage": self.fee_coverage,
            "publication_revision": self.publication_revision,
            "reason": self.reason,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class DailyReport:
    envelope: ReportEnvelope
    items: tuple[Mapping[str, Any], ...] = ()
    coverage: Mapping[str, Any] = field(default_factory=dict)

    def as_wire(self) -> dict[str, Any]:
        return {**self.envelope.as_wire(), "items": [dict(i) for i in self.items],
                "coverage": dict(self.coverage)}


@dataclass(frozen=True, slots=True)
class DealPage:
    envelope: ReportEnvelope
    items: tuple[Mapping[str, Any], ...] = ()
    next_cursor: str | None = None

    def as_wire(self) -> dict[str, Any]:
        return {**self.envelope.as_wire(), "items": [dict(i) for i in self.items],
                "next_cursor": self.next_cursor}


class AnalyticsQueryService:
    """Read-only access to published analytics builds."""

    def __init__(
        self,
        *,
        clickhouse: ClickHousePort,
        registry: PublicationRegistry,
        policy: AccountingPolicy,
        settings: Settings,
        clock: Clock,
        cursors: CursorCodec | None = None,
        metrics: Metrics | None = None,
        faults: FaultPlan | None = None,
        audit_available: bool = True,
    ) -> None:
        self._ch = clickhouse
        self._registry = registry
        self._policy = policy
        self._settings = settings
        self._clock = clock
        self._cursors = cursors or CursorCodec(
            signing_key=settings.cursor_signing_key or "test-only-cursor-key",
            key_id=settings.cursor_signing_key_id,
        )
        self._metrics = metrics or Metrics()
        self._faults = faults or NullFaultPlan()
        self._in_flight = 0
        self._audit_available = audit_available

    # ------------------------------------------------------------------
    # Admission control
    # ------------------------------------------------------------------

    @contextmanager
    def _slot(self, route: str) -> Iterator[None]:
        """Bounded concurrency. Section 8.2, INV08-20.

        The slot is released even when the query fails, so a burst of
        timeouts cannot leak the budget and turn a slow period into a
        permanent outage.
        """
        if self._in_flight >= self._settings.query_max_in_flight:
            self._metrics.query_rejected_total.labels(reason=str(Reason.OVERLOADED)).inc()
            raise OverloadError(
                "query concurrency budget exhausted",
                route=route,
                limit=self._settings.query_max_in_flight,
            )
        self._in_flight += 1
        self._metrics.query_in_flight.set(self._in_flight)
        try:
            yield
        finally:
            self._in_flight -= 1
            self._metrics.query_in_flight.set(self._in_flight)

    def _require_audit(self) -> None:
        """Fail closed when audit logging is required but unavailable.

        P08-AK/CC08-08: if the deployment demands an audit trail for reads,
        serving data with the audit sink down would create exactly the
        unreviewable access the requirement exists to prevent.
        """
        if self._settings.require_audit_for_reads and not self._audit_available:
            raise CompletenessError(
                Reason.AUDIT_UNAVAILABLE,
                "read audit is required but the audit sink is unavailable",
            )

    def set_audit_available(self, available: bool) -> None:
        self._audit_available = available

    # ------------------------------------------------------------------
    # Routes
    # ------------------------------------------------------------------

    def daily(
        self,
        auth: AuthContext,
        *,
        source_id: str,
        epoch_id: str,
        from_day: date,
        to_day: date,
        report_family: str = "daily_deal_v1",
        accounts: Sequence[str] | None = None,
        currency: str | None = None,
        build_id: str | None = None,
        include_net: bool = True,
    ) -> DailyReport:
        """Daily aggregates for a bounded date range."""
        started = self._clock.now()
        scope = auth.scope
        try:
            self._require_audit()
            scope.require_source(source_id)
            scope.require_epoch(epoch_id)
            scope.require_report_family(report_family)
            account_filter = scope.narrow_accounts(accounts)
            self._validate_range(from_day, to_day)
            if currency is not None:
                self._policy.require_currency(currency)

            publication = self._resolve(
                auth, source_id=source_id, epoch_id=epoch_id,
                report_family=report_family, build_id=build_id
            )
            if not publication.is_readable:
                return DailyReport(
                    envelope=self._envelope(
                        publication.status, publication, source_id, epoch_id,
                        reason=str(Reason.BUILD_STATE_INVALID),
                        message="the current publication is not readable",
                    )
                )

            with self._slot("daily"):
                rows = list(
                    self._ch.read_daily_rollups(
                        build_id=publication.build_id,
                        tenant_id=auth.tenant_id,
                        source_id=source_id,
                        epoch_id=epoch_id,
                        from_day=from_day,
                        to_day=to_day,
                        accounts=sorted(account_filter) if account_filter else None,
                    )
                )
                self._faults.fire(
                    KillPoint.CH_NODE_KILLED_DURING_READ, route="daily", rows=len(rows)
                )

            items = []
            for row in rows:
                if currency is not None and row.money_currency != currency:
                    continue
                items.append(self._daily_item(row, include_net=include_net))

            coverage = {
                "requested_from": from_day.isoformat(),
                "requested_to": to_day.isoformat(),
                "days_requested": (to_day - from_day).days + 1,
                "days_present": len({row.trade_day for row in rows}),
                "currencies": sorted({row.money_currency for row in rows}),
                # P08-T-11: a date range is not "full-year READY" just because
                # some rows came back. The caller is told exactly which days
                # carried data so it can judge the gap itself.
                "accounts_filtered": account_filter is not None,
            }
            envelope = self._envelope(publication.status, publication, source_id, epoch_id)
            self._metrics.query_latency_seconds.labels(
                route="daily", status=str(publication.status)
            ).observe((self._clock.now() - started).total_seconds())
            return DailyReport(envelope=envelope, items=tuple(items), coverage=coverage)

        except AnalyticsError as exc:
            return DailyReport(
                envelope=self._error_envelope(exc, source_id, epoch_id, started, "daily")
            )

    def account_deals(
        self,
        auth: AuthContext,
        *,
        source_id: str,
        epoch_id: str,
        account_id: str,
        report_family: str = "daily_deal_v1",
        limit: int | None = None,
        cursor: str | None = None,
        sort: str = "executed_at",
        build_id: str | None = None,
    ) -> DealPage:
        """A page of Deal facts for one account, bound to one build."""
        started = self._clock.now()
        scope = auth.scope
        pin_id: str | None = None
        try:
            self._require_audit()
            scope.require_source(source_id)
            scope.require_epoch(epoch_id)
            scope.require_report_family(report_family)
            scope.narrow_accounts([account_id])
            if sort not in ALLOWED_DEAL_SORTS:
                raise QueryError(
                    Reason.SORT_KEY_NOT_ALLOWED,
                    f"sort key {sort!r} is not allowlisted",
                    allowed=sorted(ALLOWED_DEAL_SORTS),
                )
            page_size = self._page_size(limit)

            publication = self._resolve(
                auth, source_id=source_id, epoch_id=epoch_id,
                report_family=report_family, build_id=build_id
            )
            if not publication.is_readable:
                return DealPage(
                    envelope=self._envelope(
                        publication.status, publication, source_id, epoch_id,
                        reason=str(Reason.BUILD_STATE_INVALID),
                    )
                )

            digest = filter_digest(
                {
                    "source_id": source_id,
                    "epoch_id": epoch_id,
                    "account_id": account_id,
                    "report_family": report_family,
                    "sort": sort,
                    "page_size": page_size,
                }
            )
            position: CursorPayload | None = None
            if cursor:
                position = self._cursors.decode(
                    cursor,
                    expected_tenant=auth.tenant_id,
                    expected_build=publication.build_id,
                    expected_filter=digest,
                )

            # Pin the build so GC cannot remove it mid-pagination (P08-T-08).
            pin_id = f"pin-{uuid.uuid4()}"
            self._registry.pin_reader(build_id=publication.build_id, pin_id=pin_id)

            with self._slot("deals"):
                facts = list(
                    self._ch.read_deal_facts(
                        build_id=publication.build_id,
                        tenant_id=auth.tenant_id,
                        source_id=source_id,
                        epoch_id=epoch_id,
                    )
                )
                self._faults.fire(
                    KillPoint.CH_NODE_KILLED_DURING_READ, route="deals", rows=len(facts)
                )

            facts = [row for row in facts if row.account_id == account_id]
            facts.sort(key=lambda row: _deal_sort_key(row, sort))
            if position is not None:
                facts = [
                    row for row in facts if _deal_sort_key(row, sort) > tuple(position.last_key)
                ]

            window = facts[:page_size]
            next_cursor = None
            if len(facts) > page_size and window:
                next_cursor = self._cursors.encode(
                    CursorPayload(
                        tenant_id=auth.tenant_id,
                        build_id=publication.build_id,
                        filter_digest=digest,
                        last_key=_deal_sort_key(window[-1], sort),
                        page=(position.page + 1) if position else 1,
                    )
                )

            envelope = self._envelope(publication.status, publication, source_id, epoch_id)
            self._metrics.query_latency_seconds.labels(
                route="deals", status=str(publication.status)
            ).observe((self._clock.now() - started).total_seconds())
            return DealPage(
                envelope=envelope,
                items=tuple(self._deal_item(row) for row in window),
                next_cursor=next_cursor,
            )

        except AnalyticsError as exc:
            return DealPage(
                envelope=self._error_envelope(exc, source_id, epoch_id, started, "deals")
            )
        finally:
            if pin_id:
                self._registry.unpin_reader(pin_id=pin_id)

    def current_publication(
        self,
        auth: AuthContext,
        *,
        source_id: str,
        epoch_id: str,
        report_family: str = "daily_deal_v1",
    ) -> dict[str, Any]:
        auth.scope.require_source(source_id)
        auth.scope.require_epoch(epoch_id)
        auth.scope.require_report_family(report_family)
        try:
            publication = self._registry.resolve(
                tenant_id=auth.tenant_id,
                source_id=source_id,
                epoch_id=epoch_id,
                report_family=report_family,
            )
        except AnalyticsError as exc:
            return {
                "status": str(ReportStatus.INCOMPLETE),
                "reason": str(exc.reason),
                "message": str(exc),
                "source_id": source_id,
                "epoch_id": epoch_id,
            }
        return {
            "status": str(publication.status),
            "build_id": publication.build_id,
            "publication_revision": publication.publication_revision,
            "published_at": publication.published_at.isoformat(),
            "reporting_cut": publication.cut_id,
            "policy_version": publication.policy_version,
            "reconciliation_revision": publication.recon_revision,
            "freshness_seconds": publication.freshness_seconds,
            "evidence_sha256": publication.evidence_sha256,
            "attempt_no": publication.attempt_no,
            "source_id": source_id,
            "epoch_id": epoch_id,
        }

    def build_evidence(self, auth: AuthContext, *, build_id: str) -> dict[str, Any]:
        records = self._registry.evidence(tenant_id=auth.tenant_id, build_id=build_id)
        return {"build_id": build_id, "evidence": records}

    def unsupported_metric(
        self, *, metric: str, source_id: str, epoch_id: str, why: str
    ) -> DailyReport:
        """A declared-unsupported answer. INV08-16, section 8.1.

        Account holdings and live exposure are not derivable from Deal facts,
        and a snapshot marker does not carry them. Saying ``UNSUPPORTED`` with
        a reason is the correct answer; returning zeros would not be.
        """
        return DailyReport(
            envelope=ReportEnvelope(
                status=ReportStatus.UNSUPPORTED,
                build_id=None,
                source_id=source_id,
                epoch_id=epoch_id,
                policy_version=self._policy.version,
                reporting_cut=None,
                freshness_seconds=None,
                fee_coverage=str(self._policy.fee_coverage),
                reason=str(Reason.METRIC_UNSUPPORTED),
                message=f"{metric} is not available: {why}",
            )
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve(
        self,
        auth: AuthContext,
        *,
        source_id: str,
        epoch_id: str,
        report_family: str,
        build_id: str | None,
    ) -> ResolvedPublication:
        if build_id is not None:
            return self._registry.resolve_explicit_build(
                tenant_id=auth.tenant_id, build_id=build_id
            )
        return self._registry.resolve(
            tenant_id=auth.tenant_id,
            source_id=source_id,
            epoch_id=epoch_id,
            report_family=report_family,
        )

    def _validate_range(self, from_day: date, to_day: date) -> None:
        if to_day < from_day:
            raise QueryError(Reason.RANGE_INVALID, "to_day precedes from_day")
        span = (to_day - from_day) + timedelta(days=1)
        if span.days > self._settings.query_max_day_range:
            raise QueryError(
                Reason.RANGE_TOO_LARGE,
                f"requested {span.days} days, limit {self._settings.query_max_day_range}",
                requested_days=span.days,
            )

    def _page_size(self, limit: int | None) -> int:
        if limit is None:
            return self._settings.query_default_page_size
        if limit < 1:
            raise QueryError(Reason.FIELD_INVALID, "limit must be positive")
        if limit > self._settings.query_max_page_size:
            raise QueryError(
                Reason.RANGE_TOO_LARGE,
                f"limit {limit} exceeds {self._settings.query_max_page_size}",
            )
        return limit

    def _daily_item(self, row, *, include_net: bool) -> dict[str, Any]:
        item: dict[str, Any] = {
            "date": row.trade_day.isoformat(),
            "account_id": row.account_id,
            "symbol": row.symbol,
            "currency": row.money_currency,
            "deal_count": row.deal_count,
            # Money leaves as a string. A JSON number here would be a float on
            # the far side, which is the defect section 7.2 is about.
            "realized_pnl_gross": format_money(row.realized_pnl_gross),
            "commission": format_money(row.commission),
            "swap": format_money(row.swap),
            "volume_lots": format_money(row.volume_lots),
        }
        if include_net:
            net, label = self._policy.net_realized(
                row.realized_pnl_gross, row.commission, row.swap
            )
            item["net_realized"] = format_money(net) if net is not None else None
            item["net_label"] = label
        return item

    def _deal_item(self, row) -> dict[str, Any]:
        return {
            "deal_id": row.deal_id,
            "account_id": row.account_id,
            "symbol": row.symbol,
            "side": row.side,
            "entry_kind": row.entry_kind,
            "executed_at": row.executed_at.isoformat(),
            "currency": row.money_currency,
            "realized_pnl_gross": format_money(row.realized_pnl_gross),
            "commission": format_money(row.commission),
            "swap": format_money(row.swap),
            "volume_lots": format_money(row.volume_lots),
            "fee_coverage": row.fee_coverage,
            "deal_payload_hash": row.deal_payload_hash,
        }

    def _envelope(
        self,
        status: ReportStatus,
        publication: ResolvedPublication,
        source_id: str,
        epoch_id: str,
        *,
        reason: str | None = None,
        message: str | None = None,
    ) -> ReportEnvelope:
        return ReportEnvelope(
            status=status,
            build_id=publication.build_id,
            source_id=source_id,
            epoch_id=epoch_id,
            policy_version=publication.policy_version,
            reporting_cut=publication.cut_id,
            freshness_seconds=publication.freshness_seconds,
            fee_coverage=str(self._policy.fee_coverage),
            publication_revision=publication.publication_revision,
            reason=reason,
            message=message,
        )

    def _error_envelope(
        self, exc: AnalyticsError, source_id: str, epoch_id: str, started, route: str
    ) -> ReportEnvelope:
        status = _status_for(exc)
        self._metrics.query_rejected_total.labels(reason=str(exc.reason)).inc()
        if exc.reason in (Reason.FORBIDDEN_SCOPE, Reason.UNAUTHENTICATED):
            self._metrics.unauthorized_access_total.labels(kind=str(exc.reason)).inc()
        self._metrics.query_latency_seconds.labels(route=route, status=str(status)).observe(
            (self._clock.now() - started).total_seconds()
        )
        return ReportEnvelope(
            status=status,
            build_id=None,
            source_id=source_id,
            epoch_id=epoch_id,
            policy_version=self._policy.version,
            reporting_cut=None,
            freshness_seconds=None,
            fee_coverage=str(self._policy.fee_coverage),
            reason=str(exc.reason),
            message=str(exc),
        )


def _status_for(exc: AnalyticsError) -> ReportStatus:
    """Map a refusal to the status a reader should see. Section 8.1."""
    incomplete = {
        Reason.NO_PUBLICATION,
        Reason.SOURCE_DEALS_MISSING,
        Reason.SOURCE_MANIFEST_MISSING,
        Reason.RETENTION_GAP,
        Reason.QUARANTINE_PENDING_IN_CUT,
    }
    unsupported = {
        Reason.METRIC_UNSUPPORTED,
        Reason.FX_CONVERSION_UNSUPPORTED,
        Reason.SNAPSHOT_CONTRACT_MISSING,
        Reason.CORRECTION_UNSUPPORTED,
    }
    degraded = {Reason.OVERLOADED, Reason.DEADLINE_EXCEEDED, Reason.BACKEND_UNAVAILABLE}
    if exc.reason in incomplete:
        return ReportStatus.INCOMPLETE
    if exc.reason in unsupported:
        return ReportStatus.UNSUPPORTED
    if exc.reason in degraded:
        return ReportStatus.DEGRADED
    return ReportStatus.BLOCKED


def _deal_sort_key(row, sort: str) -> tuple[str, ...]:
    """Total order for pagination.

    The requested key is always followed by ``deal_id``, because a cursor over
    a non-unique key either repeats or skips rows at the page boundary.
    """
    if sort == "deal_id":
        return (row.deal_id,)
    if sort == "account_id":
        return (row.account_id, row.deal_id)
    return (row.executed_at.isoformat(), row.deal_id)
