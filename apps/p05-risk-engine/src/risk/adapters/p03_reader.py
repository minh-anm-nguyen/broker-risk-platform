"""Read-only capture of the Project 03 projections into an immutable bundle.

The capture order is not arbitrary (section 6.4):

1. Resolve the epoch and verify it is the one the caller is entitled to.
2. Resolve the published scan pointers and the deal watermark *inside* one read
   snapshot, so a concurrent Project 03 publish cannot be observed half-way.
3. Read the facts under those same pointers, in bounded batches.
4. Record row counts and coverage flags, then release the transaction.

Step 4 matters as much as the others. The transaction is released before the
quote provider is called, because holding a database snapshot open across a
network round trip is how a 30-second budget becomes a 10-minute lock
(WF5-219).

Two things this reader will not do. It will not ``COALESCE`` a money column to
zero -- a NULL commission blocks the component. And it will not read a row
whose ``published_scan_id`` differs from the pointer it captured, because that
row belongs to a different scan than the one the result claims (WF5-026).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Mapping, Sequence

from risk.adapters.sql import SqlExecutor
from risk.domain.facts import (
    AccountFact,
    AuthorizedScope,
    CoverageReport,
    DealFact,
    PositionFact,
    ReconEvidence,
    RiskCut,
    build_money_triplet,
    parse_utc,
)
from risk.domain.money import parse_decimal
from risk.errors import AuthorizationError, ContractError, QualityGateError, ReasonCode
from risk.limits import Deadline, check_estimated_input_bytes, check_request_limits
from risk.policies.loader import RiskPolicy
from risk.status import Presence, Side

__all__ = [
    "CapturedFacts",
    "P03Reader",
    "UNFILTERED",
]

#: The sentinel Project 03 writes into ``filter_hash`` when a scan covered the
#: whole stream. Anything else means the watermark is account-scoped and cannot
#: carry a completeness claim.
UNFILTERED = "UNFILTERED"

_ACCOUNT_SQL = """
SELECT account_id, currency, presence, published_scan_id
FROM accounts_current
WHERE epoch_id = :epoch_id
  AND published_scan_id = :scan_id
  AND account_id IN (SELECT value FROM json_each(:account_ids))
ORDER BY account_id
"""

_ACCOUNT_SQL_PG = """
SELECT account_id, currency, presence, published_scan_id
FROM accounts_current
WHERE epoch_id = :epoch_id
  AND published_scan_id = :scan_id
  AND account_id = ANY(:account_ids)
ORDER BY account_id
"""

_POSITION_SQL = """
SELECT position_id, account_id, symbol, side, volume_lots, open_price,
       source_status, presence, published_scan_id
FROM positions_current
WHERE epoch_id = :epoch_id
  AND published_scan_id = :scan_id
  AND account_id IN (SELECT value FROM json_each(:account_ids))
ORDER BY position_id
"""

_POSITION_SQL_PG = """
SELECT position_id, account_id, symbol, side, volume_lots, open_price,
       source_status, presence, published_scan_id
FROM positions_current
WHERE epoch_id = :epoch_id
  AND published_scan_id = :scan_id
  AND account_id = ANY(:account_ids)
ORDER BY position_id
"""

# The watermark ceiling and the half-open window are both in the WHERE clause,
# so rows beyond the captured cut never enter the process at all.
_DEAL_SQL = """
SELECT deal_id, account_id, source_seq, entry_kind, deal_type,
       realized_pnl_gross, commission, swap, money_currency, executed_at
FROM deals
WHERE epoch_id = :epoch_id
  AND account_id IN (SELECT value FROM json_each(:account_ids))
  AND source_seq <= :watermark
  AND (:period_start IS NULL OR executed_at >= :period_start)
  AND (:period_end IS NULL OR executed_at < :period_end)
ORDER BY source_seq, deal_id
"""

_DEAL_SQL_PG = """
SELECT deal_id, account_id, source_seq, entry_kind, deal_type,
       realized_pnl_gross, commission, swap, money_currency, executed_at
FROM deals
WHERE epoch_id = :epoch_id
  AND account_id = ANY(:account_ids)
  AND source_seq <= :watermark
  AND (:period_start IS NULL OR executed_at >= :period_start)
  AND (:period_end IS NULL OR executed_at < :period_end)
ORDER BY source_seq, deal_id
"""

_DEAL_COUNT_SQL = """
SELECT COUNT(*) AS n
FROM deals
WHERE epoch_id = :epoch_id
  AND account_id IN (SELECT value FROM json_each(:account_ids))
  AND source_seq <= :watermark
"""

_DEAL_COUNT_SQL_PG = """
SELECT COUNT(*) AS n
FROM deals
WHERE epoch_id = :epoch_id
  AND account_id = ANY(:account_ids)
  AND source_seq <= :watermark
"""


@dataclass(frozen=True)
class CapturedFacts:
    """What one read snapshot produced, with its cut and coverage."""

    cut: RiskCut
    accounts: tuple[AccountFact, ...]
    positions: tuple[PositionFact, ...]
    deals: tuple[DealFact, ...]
    coverage: CoverageReport
    recon: ReconEvidence | None
    epoch_status: str
    epoch_generation: int


def _as_bool(value: Any) -> bool:
    """Normalise a driver's boolean. SQLite returns 0/1, psycopg True/False."""
    return bool(value) and value not in (0, "0", "false", "FALSE")


class P03Reader:
    """Captures a bounded, consistent read of the Project 03 read model."""

    def __init__(self, executor: SqlExecutor, policy: RiskPolicy) -> None:
        self._sql = executor
        self._policy = policy
        self._pg = executor.dialect == "postgresql"

    def _q(self, sqlite_sql: str, pg_sql: str) -> str:
        return pg_sql if self._pg else sqlite_sql

    def _account_param(self, account_ids: Sequence[str]) -> Any:
        """Bind the authorized account list as a parameter, never as text.

        PostgreSQL takes a real array; SQLite takes a JSON document expanded by
        ``json_each``. Either way the values travel as data, so a crafted
        account id is a lookup that finds nothing rather than SQL (WF5-198).
        """
        if self._pg:
            return list(account_ids)
        import json

        return json.dumps(list(account_ids))

    async def _resolve_epoch(self, scope: AuthorizedScope, epoch_id: str) -> Mapping[str, Any]:
        row = await self._sql.fetch_one(
            """
            SELECT epoch_id, source_id, run_id, generation, status
            FROM source_epochs
            WHERE epoch_id = :epoch_id
            """,
            {"epoch_id": epoch_id},
        )
        if row is None:
            raise QualityGateError(
                ReasonCode.EPOCH_IDENTITY_CONFLICT, "requested epoch does not exist"
            )
        if row["source_id"] != scope.source_id:
            # Two brokers can both have an account "00042". The epoch's source
            # is the only thing that keeps them apart (WF5-015, WF5-194).
            raise AuthorizationError(
                ReasonCode.UNAUTHORIZED_SCOPE, "epoch belongs to a different source"
            )
        return row

    async def _resolve_scan(self, epoch_id: str, entity: str) -> Mapping[str, Any] | None:
        """Latest PUBLISHED scan pointer for an entity."""
        return await self._sql.fetch_one(
            """
            SELECT scan_id, status, filter_hash, terminal_receipt, row_count,
                   cut_proof_id, source_snapshot_seq
            FROM sync_runs
            WHERE epoch_id = :epoch_id
              AND entity = :entity
              AND status = 'PUBLISHED'
            ORDER BY published_at DESC, scan_id DESC
            LIMIT 1
            """,
            {"epoch_id": epoch_id, "entity": entity},
        )

    async def _resolve_watermark(self, epoch_id: str) -> Mapping[str, Any] | None:
        return await self._sql.fetch_one(
            """
            SELECT watermark_seq, filter_hash, terminal
            FROM sync_checkpoints
            WHERE epoch_id = :epoch_id AND stream = 'DEAL'
            """,
            {"epoch_id": epoch_id},
        )

    async def _resolve_recon(self, epoch_id: str, account_ids: frozenset[str]) -> ReconEvidence | None:
        row = await self._sql.fetch_one(
            """
            SELECT evidence_id, revision, status, covered_accounts,
                   covered_deal_watermark, scope_known
            FROM recon_evidence
            WHERE epoch_id = :epoch_id
            ORDER BY revision DESC
            LIMIT 1
            """,
            {"epoch_id": epoch_id},
        )
        if row is None:
            return None

        import json

        try:
            covered = frozenset(json.loads(row["covered_accounts"]))
        except (TypeError, ValueError) as exc:
            raise ContractError(
                ReasonCode.RECON_EVIDENCE_SCOPE_UNKNOWN, "recon coverage is not a JSON array"
            ) from exc

        blocking = await self._sql.fetch_all(
            """
            SELECT account_id, symbol
            FROM recon_findings
            WHERE epoch_id = :epoch_id AND severity = 'BLOCKING'
            """,
            {"epoch_id": epoch_id},
        )
        return ReconEvidence(
            status=str(row["status"]),
            revision=int(row["revision"]),
            epoch_id=epoch_id,
            covered_accounts=covered,
            covered_deal_watermark=(
                None if row["covered_deal_watermark"] is None else int(row["covered_deal_watermark"])
            ),
            blocking_accounts=frozenset(
                str(r["account_id"]) for r in blocking if r["account_id"] is not None
            ),
            blocking_symbols=frozenset(str(r["symbol"]) for r in blocking if r["symbol"] is not None),
            scope_known=_as_bool(row["scope_known"]),
        )

    def _row_to_account(self, row: Mapping[str, Any], epoch_id: str) -> AccountFact:
        return AccountFact(
            account_id=str(row["account_id"]),
            currency=str(row["currency"] or ""),
            epoch_id=epoch_id,
            published_scan_id=str(row["published_scan_id"]),
            presence=Presence(str(row["presence"])),
        )

    def _row_to_position(self, row: Mapping[str, Any], epoch_id: str) -> PositionFact:
        try:
            side = Side(str(row["side"]))
        except ValueError:
            raise ContractError(
                ReasonCode.UNSUPPORTED_POSITION_SIDE,
                "position side outside the approved mapping",
                position_id=str(row["position_id"]),
            ) from None
        try:
            presence = Presence(str(row["presence"]))
        except ValueError:
            raise ContractError(
                ReasonCode.CONTRACT_MISSING_FIELD,
                "position presence outside the approved mapping",
                position_id=str(row["position_id"]),
            ) from None
        return PositionFact(
            position_id=str(row["position_id"]),
            account_id=str(row["account_id"]),
            symbol=str(row["symbol"]),
            side=side,
            lots=parse_decimal(row["volume_lots"], field="volume_lots"),
            open_price=parse_decimal(row["open_price"], field="open_price"),
            source_status=str(row["source_status"]),
            presence=presence,
            epoch_id=epoch_id,
            published_scan_id=str(row["published_scan_id"]),
        )

    def _row_to_deal(self, row: Mapping[str, Any], epoch_id: str, default_currency: str) -> DealFact:
        currency = row.get("money_currency") or default_currency
        gross, commission, swap = build_money_triplet(
            currency=str(currency),
            gross=row["realized_pnl_gross"],
            commission=row["commission"],
            swap=row["swap"],
        )
        return DealFact(
            deal_id=str(row["deal_id"]),
            account_id=str(row["account_id"]),
            source_seq=int(row["source_seq"]),
            entry_kind=str(row["entry_kind"]),
            deal_type=None if row["deal_type"] is None else str(row["deal_type"]),
            gross_profit=gross,
            commission=commission,
            swap=swap,
            executed_at=parse_utc(row["executed_at"], field_name="executed_at"),
            epoch_id=epoch_id,
        )

    async def capture(
        self,
        *,
        scope: AuthorizedScope,
        epoch_id: str,
        account_ids: frozenset[str],
        period_start: datetime | None,
        period_end: datetime | None,
        need_positions: bool,
        need_deals: bool,
        deadline: Deadline,
    ) -> CapturedFacts:
        """Capture facts for one calculation.

        Raises:
            AuthorizationError: the epoch belongs to another source.
            QualityGateError: the epoch is missing.
            ContractError: a row violates the field contract (a NULL money
                column, an unknown side, a malformed timestamp).
            ResourceError: the workload exceeds a declared limit.
        """
        check_request_limits(self._policy.limits, account_count=len(account_ids))
        accounts_param = self._account_param(sorted(account_ids))

        async with self._sql.read_snapshot():
            deadline.check(stage="capture_pointers")
            epoch_row = await self._resolve_epoch(scope, epoch_id)
            epoch_status = str(epoch_row["status"])
            epoch_generation = int(epoch_row["generation"])

            account_scan = await self._resolve_scan(epoch_id, "ACCOUNT")
            position_scan = await self._resolve_scan(epoch_id, "POSITION") if need_positions else None
            watermark_row = await self._resolve_watermark(epoch_id) if need_deals else None
            recon = await self._resolve_recon(epoch_id, account_ids)

            account_scan_id = None if account_scan is None else str(account_scan["scan_id"])
            position_scan_id = None if position_scan is None else str(position_scan["scan_id"])
            watermark = None if watermark_row is None else int(watermark_row["watermark_seq"])

            accounts: tuple[AccountFact, ...] = ()
            if account_scan_id is not None:
                rows = await self._sql.fetch_all(
                    self._q(_ACCOUNT_SQL, _ACCOUNT_SQL_PG),
                    {
                        "epoch_id": epoch_id,
                        "scan_id": account_scan_id,
                        "account_ids": accounts_param,
                    },
                )
                accounts = tuple(self._row_to_account(r, epoch_id) for r in rows)

            positions: tuple[PositionFact, ...] = ()
            if position_scan_id is not None:
                deadline.check(stage="capture_positions")
                collected: list[PositionFact] = []
                async for batch in self._sql.stream(
                    self._q(_POSITION_SQL, _POSITION_SQL_PG),
                    {
                        "epoch_id": epoch_id,
                        "scan_id": position_scan_id,
                        "account_ids": accounts_param,
                    },
                    batch_size=self._policy.limits.read_batch_rows,
                ):
                    for row in batch:
                        collected.append(self._row_to_position(row, epoch_id))
                    if len(collected) > self._policy.limits.max_positions_per_request:
                        check_request_limits(
                            self._policy.limits,
                            account_count=len(account_ids),
                            position_count=len(collected),
                        )
                positions = tuple(collected)

            deals: tuple[DealFact, ...] = ()
            if need_deals and watermark is not None:
                deadline.check(stage="capture_deals")
                # Count before streaming: rejecting a 10M-row window costs one
                # index scan instead of 10M object constructions (WF5-168).
                count_row = await self._sql.fetch_one(
                    self._q(_DEAL_COUNT_SQL, _DEAL_COUNT_SQL_PG),
                    {
                        "epoch_id": epoch_id,
                        "account_ids": accounts_param,
                        "watermark": watermark,
                    },
                )
                total = 0 if count_row is None else int(count_row["n"])
                check_request_limits(
                    self._policy.limits, account_count=len(account_ids), deal_count=total
                )
                # Row count and memory footprint are separate budgets: a window
                # inside the row cap can still be too large to hold.
                check_estimated_input_bytes(self._policy.limits, row_count=total)

                default_currency = next(iter({a.currency for a in accounts}), "USD")
                collected_deals: list[DealFact] = []
                async for batch in self._sql.stream(
                    self._q(_DEAL_SQL, _DEAL_SQL_PG),
                    {
                        "epoch_id": epoch_id,
                        "account_ids": accounts_param,
                        "watermark": watermark,
                        "period_start": None if period_start is None else _iso(period_start, self._pg),
                        "period_end": None if period_end is None else _iso(period_end, self._pg),
                    },
                    batch_size=self._policy.limits.read_batch_rows,
                ):
                    for row in batch:
                        collected_deals.append(self._row_to_deal(row, epoch_id, default_currency))
                deals = tuple(collected_deals)

            coverage = CoverageReport(
                account_scan_published=account_scan is not None,
                position_scan_published=position_scan is not None,
                position_scan_terminal=(
                    position_scan is not None and _as_bool(position_scan["terminal_receipt"])
                ),
                deal_watermark_terminal=(
                    watermark_row is not None and _as_bool(watermark_row["terminal"])
                ),
                deal_watermark_unfiltered=(
                    watermark_row is not None and str(watermark_row["filter_hash"]) == UNFILTERED
                ),
                epoch_active=epoch_status == "ACTIVE",
                quote_snapshot_pinned=False,  # set by the quote adapter
                fee_fields_available=False,  # Project 03 has no DEAL_FEE column
                position_row_count=len(positions),
                deal_row_count=len(deals),
            )

            # A cut is COMPATIBLE only when the position scan carries an
            # upstream freeze proof. Both scans agreeing by coincidence is not
            # a proof (section 4.1).
            cut_proof = None if position_scan is None else position_scan["cut_proof_id"]
            cut = RiskCut(
                source_id=scope.source_id,
                epoch_id=epoch_id,
                account_scan_id=account_scan_id,
                position_scan_id=position_scan_id,
                deal_watermark_seq=watermark,
                quote_snapshot_id=None,
                symbol_catalog_version=None,
                recon_revision=None if recon is None else recon.revision,
                cut_proof_id=None if cut_proof is None else str(cut_proof),
                source_snapshot_seq=(
                    None
                    if position_scan is None or position_scan["source_snapshot_seq"] is None
                    else int(position_scan["source_snapshot_seq"])
                ),
            )

        return CapturedFacts(
            cut=cut,
            accounts=accounts,
            positions=positions,
            deals=deals,
            coverage=coverage,
            recon=recon,
            epoch_status=epoch_status,
            epoch_generation=epoch_generation,
        )

    async def current_epoch_fence(self, epoch_id: str) -> tuple[str, int]:
        """Re-read epoch status and generation after computing.

        The final fence of section 6.4 step 10. A source reset that happened
        while the arithmetic was running must not be published as a current
        result (WF5-019, K05-12).
        """
        row = await self._sql.fetch_one(
            "SELECT status, generation FROM source_epochs WHERE epoch_id = :epoch_id",
            {"epoch_id": epoch_id},
        )
        if row is None:
            raise QualityGateError(ReasonCode.EPOCH_CHANGED, "epoch disappeared during calculation")
        return str(row["status"]), int(row["generation"])

    async def current_recon_revision(self, epoch_id: str) -> int | None:
        """Re-read the reconciliation revision after computing (WF5-208)."""
        row = await self._sql.fetch_one(
            "SELECT MAX(revision) AS r FROM recon_evidence WHERE epoch_id = :epoch_id",
            {"epoch_id": epoch_id},
        )
        if row is None or row["r"] is None:
            return None
        return int(row["r"])


def _iso(value: datetime, for_postgres: bool) -> Any:
    """Bind a timestamp in the form the driver compares correctly."""
    if for_postgres:
        return value.astimezone(UTC)
    # SQLite compares TEXT lexicographically, so the stored and bound forms
    # must match exactly: UTC, microseconds, explicit +00:00 offset.
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


def decimal_or_block(value: Any, *, field: str) -> Decimal:
    """Parse a money column, refusing the NULL-to-zero shortcut."""
    if value is None:
        raise ContractError(ReasonCode.MONEY_FIELD_MISSING, "required money column is NULL", field=field)
    return parse_decimal(value, field=field)
