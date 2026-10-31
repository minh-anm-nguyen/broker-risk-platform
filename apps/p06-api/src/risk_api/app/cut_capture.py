"""Capturing a data cut, and re-checking it before delivery.

Design doc section 5.2. Capture pins the whole vector in one consistent read:
epoch, published account/position scans, deal watermark, reconciliation
revision and quote snapshot. The engine then computes against that immutable
vector.

The final check exists because the world keeps moving. It cannot stop a repair
from landing while bytes are in flight, and this module does not pretend
otherwise: the guarantee is a result *for its declared cut*, plus a refusal if
that cut was invalidated before the result was released.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Sequence

from risk_engine import (
    AccountFact,
    CoverageDeclaration,
    DealFact,
    InstrumentSpec,
    PositionFact,
    QuoteFact,
    RiskCut,
    RiskInputBundle,
    digest,
    parse_rfc3339,
)

from ..admission.budgets import Deadline
from ..authz.principal_context import AuthorizedScope
from ..contracts.problem_details import ApiProblem
from ..faults import FaultPoint, FaultRegistry
from ..repositories.read_views import (
    CutRevisions,
    DatabaseUnavailable,
    InMemoryReadViews,
    PublishedCutRow,
    QuoteSnapshotRow,
)

__all__ = ["CapturedCut", "CutCapturer", "cut_digest"]


@dataclass(frozen=True, slots=True)
class CapturedCut:
    """One pinned cut plus the facts read under it."""

    cut: RiskCut
    revisions: CutRevisions
    coverage: CoverageDeclaration
    accounts: tuple[AccountFact, ...]
    positions: tuple[PositionFact, ...]
    deals: tuple[DealFact, ...]
    quotes: tuple[QuoteFact, ...]
    instruments: tuple[InstrumentSpec, ...]
    symbol_catalog_version: str
    recon_revision: str
    captured_at_utc: str | None
    period_start_utc: str | None = None
    period_end_utc: str | None = None

    def bundle(self) -> RiskInputBundle:
        return RiskInputBundle(
            cut=self.cut,
            accounts=self.accounts,
            positions=self.positions,
            deals=self.deals,
            quotes=self.quotes,
            instruments=self.instruments,
            coverage=self.coverage,
            symbol_catalog_version=self.symbol_catalog_version,
        )

    def digest(self) -> str:
        return cut_digest(self.cut, self.revisions)


def cut_digest(cut: RiskCut, revisions: CutRevisions) -> str:
    """A stable digest of the cut vector, for the audit row."""
    return digest(
        {
            "cut": cut.to_manifest(),
            "epoch_status": revisions.epoch_status,
            "publication_revision": revisions.publication_revision,
            "recon_revision": revisions.recon_revision,
        }
    )


class CutCapturer:
    """Reads the projections and decides whether a cut is usable."""

    def __init__(
        self,
        *,
        views: InMemoryReadViews,
        faults: FaultRegistry | None = None,
        default_realized_window_days: int = 31,
    ) -> None:
        self._views = views
        self._faults = faults or FaultRegistry()
        self._default_realized_window_days = default_realized_window_days

    async def capture(
        self,
        *,
        scope: AuthorizedScope,
        components: Sequence[str],
        period_start_utc: str | None,
        period_end_utc: str | None,
        symbol_catalog_version: str,
        deadline: Deadline,
    ) -> CapturedCut:
        """Pin a cut and read every fact the requested components need."""
        deadline.check(stage="cut capture")
        try:
            async with self._views.snapshot() as snapshot:
                epoch = await snapshot.epoch_status(scope.source_id)
                if epoch is None:
                    raise ApiProblem(
                        "RESOURCE_NOT_FOUND",
                        "the requested resource does not exist or is not visible to this principal",
                        reason_codes=("SOURCE_OUT_OF_SCOPE",),
                    )
                if epoch.epoch_id != scope.epoch_id:
                    # The epoch moved between authorization and capture.
                    raise ApiProblem(
                        "EPOCH_CHANGED",
                        "the source epoch changed while the request was being prepared",
                        reason_codes=("EPOCH_CHANGED",),
                    )
                if epoch.status != "ACTIVE":
                    raise ApiProblem(
                        "DATA_NOT_READY",
                        "the source epoch is not currently reportable",
                        reason_codes=("SOURCE_NOT_ACTIVE",),
                    )

                cut_row = await snapshot.published_cut(scope.source_id, scope.epoch_id)
                if cut_row is None:
                    raise ApiProblem(
                        "DATA_NOT_READY",
                        "no published data cut is available for this epoch",
                        reason_codes=("P03_UNPUBLISHED",),
                    )
                self._assert_publication(cut_row, components)

                recon = await snapshot.reconciliation(
                    scope.source_id, scope.epoch_id, scope.accounts
                )
                if recon is None:
                    raise ApiProblem(
                        "DATA_NOT_READY",
                        "no reconciliation evidence is available for this scope",
                        reason_codes=("RECON_BLOCKING",),
                    )
                if recon.blocking_reason_family is not None:
                    # A blocking finding is never something a report may ignore.
                    raise ApiProblem(
                        "DATA_NOT_READY",
                        "a blocking reconciliation finding covers the requested scope",
                        reason_codes=("RECON_BLOCKING",),
                    )

                quote_row: QuoteSnapshotRow | None = None
                if "UNREALIZED" in components:
                    quote_row = await snapshot.quote_snapshot(scope.source_id, scope.epoch_id)

                accounts = await snapshot.accounts(scope.source_id, scope.epoch_id, scope.accounts)
                positions: tuple[PositionFact, ...] = ()
                instruments: tuple[InstrumentSpec, ...] = ()
                quotes: tuple[QuoteFact, ...] = ()
                deals: tuple[DealFact, ...] = ()

                needs_positions = "EXPOSURE" in components or "UNREALIZED" in components
                if needs_positions and cut_row.position_scan_id is not None:
                    positions = await snapshot.positions(
                        scope.source_id,
                        scope.epoch_id,
                        scope.accounts,
                        scan_id=cut_row.position_scan_id,
                        symbol=scope.symbol,
                    )
                    symbols = sorted({row.symbol for row in positions})
                    instruments = await snapshot.instruments(symbol_catalog_version, symbols)
                    if quote_row is not None:
                        quotes = await snapshot.quotes(
                            scope.source_id,
                            scope.epoch_id,
                            symbols,
                            snapshot_id=quote_row.snapshot_id,
                        )

                resolved_start, resolved_end = period_start_utc, period_end_utc
                if "REALIZED" in components:
                    if resolved_start is None or resolved_end is None:
                        # Anchor the default window to the publication instant of
                        # the pinned cut, never to wall-clock now: the same cut
                        # must always produce the same digests.
                        resolved_start, resolved_end = self._default_window(cut_row)
                    deals = await snapshot.deals(
                        scope.source_id,
                        scope.epoch_id,
                        scope.accounts,
                        start_utc=resolved_start,
                        end_utc=resolved_end,
                    )

                coverage = self._views.coverage_for(cut_row, quote_row)
                consistency = self._consistency(cut_row, quote_row, components)
                cut = RiskCut(
                    source_id=scope.source_id,
                    epoch_id=scope.epoch_id,
                    account_scan_id=cut_row.account_scan_id,
                    position_scan_id=cut_row.position_scan_id,
                    deal_watermark_seq=cut_row.deal_watermark_seq,
                    quote_snapshot_id=None if quote_row is None else quote_row.snapshot_id,
                    fx_rate_set_id=None,
                    source_snapshot_seq=None,
                    consistency=consistency,
                )
                revisions = CutRevisions(
                    epoch_id=epoch.epoch_id,
                    epoch_status=epoch.status,
                    publication_revision=cut_row.publication_revision,
                    recon_revision=recon.recon_revision,
                    recon_blocking=recon.blocking_reason_family is not None,
                    quote_snapshot_id=None if quote_row is None else quote_row.snapshot_id,
                )
                captured = CapturedCut(
                    cut=cut,
                    revisions=revisions,
                    coverage=coverage,
                    accounts=accounts,
                    positions=positions,
                    deals=deals,
                    quotes=quotes,
                    instruments=instruments,
                    symbol_catalog_version=symbol_catalog_version,
                    recon_revision=recon.recon_revision,
                    captured_at_utc=cut_row.published_at_utc,
                    period_start_utc=resolved_start,
                    period_end_utc=resolved_end,
                )
        except DatabaseUnavailable as exc:
            raise ApiProblem(
                "DB_UNAVAILABLE",
                "the projection store could not serve this read",
                reason_codes=("DB_UNAVAILABLE",),
            ) from exc

        await self._faults.wait_if_barrier(FaultPoint.BARRIER_AFTER_CUT_CAPTURE)
        return captured

    # ----------------------------------------------------------- final check

    async def final_check(
        self, *, scope: AuthorizedScope, pinned: CutRevisions, deadline: Deadline
    ) -> CutRevisions:
        """Re-read the revision vector and refuse a result that was invalidated."""
        await self._faults.wait_if_barrier(FaultPoint.BARRIER_BEFORE_FINAL_CHECK)
        deadline.check(stage="final validity check")
        try:
            current = await self._views.current_revisions(
                scope.source_id, scope.epoch_id, scope.accounts
            )
        except DatabaseUnavailable as exc:
            # Unable to prove the cut still holds: fail closed.
            raise ApiProblem(
                "DB_UNAVAILABLE",
                "the final validity check could not be completed",
                reason_codes=("DB_UNAVAILABLE",),
            ) from exc

        changed = pinned.differs_from(current)
        if not changed:
            return current

        if "epoch" in changed:
            raise ApiProblem(
                "EPOCH_CHANGED",
                "the source epoch changed before the report could be released",
                reason_codes=("EPOCH_CHANGED",),
            )
        if "reconciliation" in changed and current.recon_blocking:
            raise ApiProblem(
                "DATA_NOT_READY",
                "a blocking reconciliation finding landed before the report was released",
                reason_codes=("RECON_BLOCKING",),
            )
        raise ApiProblem(
            "CUT_CHANGED",
            f"the pinned data cut changed before delivery ({', '.join(changed)})",
            reason_codes=("INCOMPATIBLE_CUT",),
        )

    # -------------------------------------------------------------- helpers

    def _default_window(self, cut_row: PublishedCutRow) -> tuple[str, str]:
        """The documented realized window for a cut, as a half-open interval."""
        end = parse_rfc3339(cut_row.published_at_utc, field="published_at_utc")
        start = end - timedelta(days=self._default_realized_window_days)
        return (
            start.isoformat().replace("+00:00", "Z"),
            end.isoformat().replace("+00:00", "Z"),
        )

    @staticmethod
    def _assert_publication(cut_row: PublishedCutRow, components: Sequence[str]) -> None:
        """Refuse to read from an unpublished scan."""
        if "EXPOSURE" in components or "UNREALIZED" in components:
            if not cut_row.positions_published or cut_row.position_scan_id is None:
                raise ApiProblem(
                    "DATA_NOT_READY",
                    "the position scan for this epoch is not published",
                    reason_codes=("P03_UNPUBLISHED",),
                )
        if not cut_row.accounts_published or cut_row.account_scan_id is None:
            raise ApiProblem(
                "DATA_NOT_READY",
                "the account scan for this epoch is not published",
                reason_codes=("P03_UNPUBLISHED",),
            )

    @staticmethod
    def _consistency(
        cut_row: PublishedCutRow, quote_row: QuoteSnapshotRow | None, components: Sequence[str]
    ) -> str:
        """Decide whether the components sit on one provable logical cut.

        Only a quote snapshot explicitly proven against the pinned position
        scan earns ``COMPATIBLE``. Everything else stays ``INDEPENDENT``, which
        is what stops a cross-component total from being invented.
        """
        money_components = [c for c in components if c in ("REALIZED", "UNREALIZED")]
        if len(money_components) < 2:
            return "COMPATIBLE" if cut_row.deals_complete else "UNPROVEN"
        if quote_row is None:
            return "UNPROVEN"
        if (
            quote_row.proven_against_position_scan is not None
            and quote_row.proven_against_position_scan == cut_row.position_scan_id
            and cut_row.deals_complete
        ):
            return "COMPATIBLE"
        return "INDEPENDENT"
