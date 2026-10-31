"""Deterministic fixtures: the golden catalog of section 13.2 plus a builder.

Every fixture is a *named, versioned* scenario. That is a correctness
requirement, not tidiness: a test whose data was generated from an unrecorded
random seed cannot be re-run, so it can never be more than anecdote
(WF5-290).

The builder writes into a SQLite harness database shaped exactly like the
Project 03 read model, so the integration tests exercise the real reader, the
real SQL and the real coverage flags rather than a hand-built bundle.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence

from risk.adapters.quotes import build_snapshot
from risk.domain.facts import AuthorizedScope, QuoteSnapshot

__all__ = [
    "ScenarioSpec",
    "AccountSpec",
    "PositionSpec",
    "DealSpec",
    "QuoteSpec",
    "ScanSpec",
    "build_harness_db",
    "SCHEMA_PATH",
    "EPOCH_A",
    "EPOCH_B",
    "SOURCE_A",
    "SOURCE_B",
    "TENANT_A",
    "ACCOUNT_A",
    "default_scope",
    "golden_scenarios",
    "scenario",
    "T0",
]

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "database" / "migrations" / "sqlite_harness.sql"

#: Stable identifiers. Hard-coded rather than generated so a failing test names
#: the same epoch every run and the fixture digest is reproducible.
EPOCH_A = "3cecd819-fd61-420b-8e35-019c70203b4e"
EPOCH_B = "6f1d2b40-0e9c-4a52-9f0a-6a2d4c8b1e77"
SOURCE_A = "sim-broker-a"
SOURCE_B = "sim-broker-b"
TENANT_A = "tenant-a"
TENANT_B = "tenant-b"
ACCOUNT_A = "00042"
ACCOUNT_B = "00043"

#: The reference instant for every fixture. Fixed, so quote ages are exact.
T0 = datetime(2026, 10, 15, 12, 0, 0, tzinfo=UTC)

#: Seed recorded in the evidence records of every generated dataset.
DEFAULT_SEED = 4205


def _iso(value: datetime) -> str:
    """Render a timestamp in the one form the harness schema stores."""
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")


@dataclass(frozen=True)
class AccountSpec:
    account_id: str = ACCOUNT_A
    currency: str = "USD"
    presence: str = "PRESENT"
    balance: str = "10056.00"


@dataclass(frozen=True)
class PositionSpec:
    position_id: str
    symbol: str = "EURUSD"
    side: str = "BUY"
    lots: str = "0.50"
    open_price: str = "1.10000"
    source_status: str = "OPEN"
    presence: str = "PRESENT"
    account_id: str = ACCOUNT_A


@dataclass(frozen=True)
class DealSpec:
    deal_id: str
    source_seq: int
    entry_kind: str = "OUT"
    deal_type: str | None = None
    gross: str | None = "0.00"
    commission: str | None = "0.00"
    swap: str | None = "0.00"
    money_currency: str | None = "USD"
    executed_at: datetime = T0
    account_id: str = ACCOUNT_A
    symbol: str | None = "EURUSD"


@dataclass(frozen=True)
class QuoteSpec:
    symbol: str = "EURUSD"
    bid: str = "1.10200"
    ask: str = "1.10220"
    market_time: datetime = T0
    quote_id: str = "q-EURUSD-1"
    feed_status: str = "ACTIVE"


@dataclass(frozen=True)
class ScanSpec:
    """A Project 03 publication pointer.

    ``terminal_receipt`` is the flag that separates "no positions" from
    "pagination died", and ``filter_hash`` is the one that separates a global
    watermark from an account-scoped one. Both are settable because both have
    dedicated negative fixtures.
    """

    scan_id: str
    entity: str
    status: str = "PUBLISHED"
    filter_hash: str = "UNFILTERED"
    terminal_receipt: bool = True
    cut_proof_id: str | None = None
    source_snapshot_seq: int | None = None


@dataclass
class ScenarioSpec:
    """A complete, named world for one or more test cases."""

    name: str
    epoch_id: str = EPOCH_A
    source_id: str = SOURCE_A
    generation: int = 1
    epoch_status: str = "ACTIVE"
    accounts: tuple[AccountSpec, ...] = (AccountSpec(),)
    positions: tuple[PositionSpec, ...] = ()
    deals: tuple[DealSpec, ...] = ()
    quotes: tuple[QuoteSpec, ...] = (QuoteSpec(),)
    account_scan: ScanSpec | None = None
    position_scan: ScanSpec | None = None
    deal_watermark: int | None = 1050
    deal_watermark_filter: str = "UNFILTERED"
    deal_watermark_terminal: bool = True
    recon_status: str | None = "VERIFIED"
    recon_revision: int = 7
    recon_covered_accounts: tuple[str, ...] | None = None
    recon_covered_watermark: int | None = 1050
    recon_scope_known: bool = True
    blocking_findings: tuple[dict[str, Any], ...] = ()
    quote_snapshot_id: str = "quote-S1030"
    quote_bound_position_scan_id: str | None = None
    seed: int = DEFAULT_SEED
    extra_epochs: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.account_scan is None:
            self.account_scan = ScanSpec(scan_id="scan-A", entity="ACCOUNT")
        if self.position_scan is None:
            self.position_scan = ScanSpec(scan_id="scan-P", entity="POSITION")
        if self.recon_covered_accounts is None:
            self.recon_covered_accounts = tuple(a.account_id for a in self.accounts)


def default_scope(
    *,
    tenant_id: str = TENANT_A,
    source_id: str = SOURCE_A,
    account_ids: Iterable[str] = (ACCOUNT_A,),
) -> AuthorizedScope:
    return AuthorizedScope(
        tenant_id=tenant_id, source_id=source_id, account_ids=frozenset(account_ids)
    )


def build_harness_db(spec: ScenarioSpec, *, path: str = ":memory:") -> sqlite3.Connection:
    """Create and populate a harness database for one scenario."""
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))

    conn.execute(
        """
        INSERT INTO source_epochs (epoch_id, source_id, run_id, generation, status, opened_at)
        VALUES (:epoch_id, :source_id, :run_id, :generation, :status, :opened_at)
        """,
        {
            "epoch_id": spec.epoch_id,
            "source_id": spec.source_id,
            "run_id": f"run-{spec.source_id}",
            "generation": spec.generation,
            "status": spec.epoch_status,
            "opened_at": _iso(T0 - timedelta(hours=1)),
        },
    )
    for extra in spec.extra_epochs:
        conn.execute(
            """
            INSERT INTO source_epochs (epoch_id, source_id, run_id, generation, status, opened_at)
            VALUES (:epoch_id, :source_id, :run_id, :generation, :status, :opened_at)
            """,
            {
                "run_id": f"run-{extra.get('source_id', spec.source_id)}",
                "opened_at": _iso(T0 - timedelta(hours=2)),
                **extra,
            },
        )

    for scan in (spec.account_scan, spec.position_scan):
        assert scan is not None
        conn.execute(
            """
            INSERT INTO sync_runs (scan_id, epoch_id, entity, status, filter_hash,
                                   terminal_receipt, row_count, published_at,
                                   cut_proof_id, source_snapshot_seq)
            VALUES (:scan_id, :epoch_id, :entity, :status, :filter_hash,
                    :terminal_receipt, :row_count, :published_at,
                    :cut_proof_id, :source_snapshot_seq)
            """,
            {
                "scan_id": scan.scan_id,
                "epoch_id": spec.epoch_id,
                "entity": scan.entity,
                "status": scan.status,
                "filter_hash": scan.filter_hash,
                "terminal_receipt": 1 if scan.terminal_receipt else 0,
                "row_count": len(spec.positions) if scan.entity == "POSITION" else len(spec.accounts),
                "published_at": _iso(T0 - timedelta(minutes=5)),
                "cut_proof_id": scan.cut_proof_id,
                "source_snapshot_seq": scan.source_snapshot_seq,
            },
        )

    if spec.deal_watermark is not None:
        conn.execute(
            """
            INSERT INTO sync_checkpoints (epoch_id, stream, watermark_seq, filter_hash,
                                          terminal, updated_at)
            VALUES (:epoch_id, 'DEAL', :watermark, :filter_hash, :terminal, :updated_at)
            """,
            {
                "epoch_id": spec.epoch_id,
                "watermark": spec.deal_watermark,
                "filter_hash": spec.deal_watermark_filter,
                "terminal": 1 if spec.deal_watermark_terminal else 0,
                "updated_at": _iso(T0),
            },
        )

    for account in spec.accounts:
        conn.execute(
            """
            INSERT INTO accounts_current (epoch_id, account_id, currency, balance,
                                          presence, published_scan_id, payload_hash)
            VALUES (:epoch_id, :account_id, :currency, :balance, :presence, :scan_id, :hash)
            """,
            {
                "epoch_id": spec.epoch_id,
                "account_id": account.account_id,
                "currency": account.currency,
                "balance": account.balance,
                "presence": account.presence,
                "scan_id": spec.account_scan.scan_id,  # type: ignore[union-attr]
                "hash": f"acct-{account.account_id}",
            },
        )

    for position in spec.positions:
        conn.execute(
            """
            INSERT INTO positions_current (epoch_id, position_id, account_id, symbol, side,
                                           volume_lots, open_price, source_status, presence,
                                           published_scan_id, payload_hash)
            VALUES (:epoch_id, :position_id, :account_id, :symbol, :side, :lots, :open_price,
                    :source_status, :presence, :scan_id, :hash)
            """,
            {
                "epoch_id": spec.epoch_id,
                "position_id": position.position_id,
                "account_id": position.account_id,
                "symbol": position.symbol,
                "side": position.side,
                "lots": position.lots,
                "open_price": position.open_price,
                "source_status": position.source_status,
                "presence": position.presence,
                "scan_id": spec.position_scan.scan_id,  # type: ignore[union-attr]
                "hash": f"pos-{position.position_id}",
            },
        )

    for deal in spec.deals:
        conn.execute(
            """
            INSERT INTO deals (epoch_id, deal_id, account_id, source_seq, entry_kind, deal_type,
                               symbol, realized_pnl_gross, commission, swap, money_currency,
                               executed_at, payload_hash)
            VALUES (:epoch_id, :deal_id, :account_id, :source_seq, :entry_kind, :deal_type,
                    :symbol, :gross, :commission, :swap, :money_currency, :executed_at, :hash)
            """,
            {
                "epoch_id": spec.epoch_id,
                "deal_id": deal.deal_id,
                "account_id": deal.account_id,
                "source_seq": deal.source_seq,
                "entry_kind": deal.entry_kind,
                "deal_type": deal.deal_type,
                "symbol": deal.symbol,
                "gross": deal.gross,
                "commission": deal.commission,
                "swap": deal.swap,
                "money_currency": deal.money_currency,
                "executed_at": _iso(deal.executed_at),
                "hash": f"deal-{deal.deal_id}",
            },
        )

    if spec.recon_status is not None:
        evidence_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"recon:{spec.name}:{spec.recon_revision}"))
        conn.execute(
            """
            INSERT INTO recon_evidence (evidence_id, epoch_id, revision, status,
                                        covered_accounts, covered_deal_watermark,
                                        scope_known, created_at)
            VALUES (:evidence_id, :epoch_id, :revision, :status, :covered, :watermark,
                    :scope_known, :created_at)
            """,
            {
                "evidence_id": evidence_id,
                "epoch_id": spec.epoch_id,
                "revision": spec.recon_revision,
                "status": spec.recon_status,
                "covered": json.dumps(list(spec.recon_covered_accounts or ())),
                "watermark": spec.recon_covered_watermark,
                "scope_known": 1 if spec.recon_scope_known else 0,
                "created_at": _iso(T0 - timedelta(minutes=10)),
            },
        )
        for index, finding in enumerate(spec.blocking_findings):
            conn.execute(
                """
                INSERT INTO recon_findings (finding_id, evidence_id, epoch_id, severity,
                                            account_id, symbol, deal_id, finding_code, created_at)
                VALUES (:finding_id, :evidence_id, :epoch_id, :severity, :account_id,
                        :symbol, :deal_id, :code, :created_at)
                """,
                {
                    "finding_id": str(uuid.uuid5(uuid.NAMESPACE_URL, f"finding:{spec.name}:{index}")),
                    "evidence_id": evidence_id,
                    "epoch_id": spec.epoch_id,
                    "severity": finding.get("severity", "BLOCKING"),
                    "account_id": finding.get("account_id"),
                    "symbol": finding.get("symbol"),
                    "deal_id": finding.get("deal_id"),
                    "code": finding.get("code", "COMMISSION_MISMATCH"),
                    "created_at": _iso(T0 - timedelta(minutes=9)),
                },
            )

    conn.commit()
    return conn


def quote_snapshot_for(spec: ScenarioSpec) -> QuoteSnapshot:
    """Build the pinned quote snapshot a scenario describes."""
    return build_snapshot(
        spec.quote_snapshot_id,
        [
            {
                "symbol": q.symbol,
                "bid": q.bid,
                "ask": q.ask,
                "market_time": _iso(q.market_time),
                "quote_id": q.quote_id,
                "feed_status": q.feed_status,
            }
            for q in spec.quotes
        ],
        captured_at=T0,
        source_id=spec.source_id,
        bound_position_scan_id=spec.quote_bound_position_scan_id,
    )


# ---------------------------------------------------------------------------
# The golden catalog of design section 13.2.
#
# The worked example of section 5.6 is the anchor: BUY 0.5 EURUSD at 1.10000
# with commission -2.50, a close of 0.2 lots at 1.10300 realising +60.00 with
# commission -1.00 and swap -0.50, leaving 0.30 lots open against bid 1.10200.
# ---------------------------------------------------------------------------

_OPEN_DEAL = DealSpec(
    deal_id="d-open-1",
    source_seq=1001,
    entry_kind="IN",
    gross="0.00",
    commission="-2.50",
    swap="0.00",
    executed_at=T0 - timedelta(hours=3),
)
_CLOSE_DEAL = DealSpec(
    deal_id="d-close-1",
    source_seq=1002,
    entry_kind="OUT",
    gross="60.00",
    commission="-1.00",
    swap="-0.50",
    executed_at=T0 - timedelta(hours=1),
)

_ALIGNED_POSITION_SCAN = ScanSpec(
    scan_id="scan-P",
    entity="POSITION",
    cut_proof_id="freeze-S1030",
    source_snapshot_seq=1030,
)


def golden_scenarios() -> dict[str, ScenarioSpec]:
    """The named fixtures of the golden catalog, keyed by fixture id."""
    return {
        # An empty book with proof the scan completed: zero is a real answer.
        "R05-empty-complete": ScenarioSpec(
            name="R05-empty-complete",
            positions=(),
            deals=(),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        # The same empty result set without the terminal receipt: not an answer.
        "R05-empty-incomplete": ScenarioSpec(
            name="R05-empty-incomplete",
            positions=(),
            deals=(),
            position_scan=ScanSpec(
                scan_id="scan-P", entity="POSITION", terminal_receipt=False
            ),
        ),
        "R05-buy-mark": ScenarioSpec(
            name="R05-buy-mark",
            positions=(PositionSpec(position_id="p-1", side="BUY", lots="0.50"),),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        "R05-sell-mark": ScenarioSpec(
            name="R05-sell-mark",
            positions=(PositionSpec(position_id="p-1", side="SELL", lots="0.50"),),
            quotes=(QuoteSpec(bid="1.09880", ask="1.09900"),),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        "R05-signed-fees": ScenarioSpec(
            name="R05-signed-fees",
            positions=(PositionSpec(position_id="p-1", lots="0.30"),),
            deals=(_OPEN_DEAL, _CLOSE_DEAL),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        # Hedged book: net zero, gross 200k. The case that catches a netting
        # implementation that collapses gross.
        "R05-hedged-gross": ScenarioSpec(
            name="R05-hedged-gross",
            positions=(
                PositionSpec(position_id="p-long", side="BUY", lots="1.00"),
                PositionSpec(position_id="p-short", side="SELL", lots="1.00"),
            ),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        # No freeze proof anywhere: components only, composite refused.
        "R05-cut-mismatch": ScenarioSpec(
            name="R05-cut-mismatch",
            positions=(PositionSpec(position_id="p-1"),),
            deals=(_CLOSE_DEAL,),
            position_scan=ScanSpec(scan_id="scan-P", entity="POSITION", cut_proof_id=None),
            quote_bound_position_scan_id=None,
        ),
        # Same account id in a second generation of the same source.
        "R05-source-reset": ScenarioSpec(
            name="R05-source-reset",
            positions=(PositionSpec(position_id="p-1"),),
            generation=2,
            extra_epochs=(
                {
                    "epoch_id": EPOCH_B,
                    "source_id": SOURCE_A,
                    "generation": 1,
                    "status": "ARCHIVED",
                },
            ),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        # Economic time in the window, ingest sequence above the watermark.
        "R05-late-deal": ScenarioSpec(
            name="R05-late-deal",
            deals=(
                _CLOSE_DEAL,
                DealSpec(
                    deal_id="d-late-1",
                    source_seq=2000,
                    entry_kind="OUT",
                    gross="25.00",
                    commission="0.00",
                    swap="0.00",
                    executed_at=T0 - timedelta(hours=2),
                ),
            ),
            deal_watermark=1050,
            position_scan=_ALIGNED_POSITION_SCAN,
        ),
        "R05-unknown-deal-type": ScenarioSpec(
            name="R05-unknown-deal-type",
            deals=(
                DealSpec(
                    deal_id="d-balance-1",
                    source_seq=1003,
                    entry_kind="IN",
                    deal_type="DEAL_TYPE_BALANCE",
                    gross="1000.00",
                    executed_at=T0 - timedelta(hours=2),
                ),
                DealSpec(
                    deal_id="d-trade-1",
                    source_seq=1004,
                    entry_kind="OUT",
                    deal_type="DEAL_TYPE_SELL",
                    gross="30.00",
                    executed_at=T0 - timedelta(hours=2),
                ),
            ),
            position_scan=_ALIGNED_POSITION_SCAN,
        ),
        # A NULL swap. The fixture exists to prove the read path blocks rather
        # than coalescing to zero.
        "R05-invalid-decimal": ScenarioSpec(
            name="R05-invalid-decimal",
            deals=(
                DealSpec(
                    deal_id="d-null-swap",
                    source_seq=1005,
                    entry_kind="OUT",
                    gross="10.00",
                    commission="-1.00",
                    swap=None,
                    executed_at=T0 - timedelta(hours=2),
                ),
            ),
            position_scan=_ALIGNED_POSITION_SCAN,
        ),
        "R05-mixed-currency": ScenarioSpec(
            name="R05-mixed-currency",
            accounts=(
                AccountSpec(account_id=ACCOUNT_A, currency="USD"),
                AccountSpec(account_id=ACCOUNT_B, currency="EUR"),
            ),
            positions=(PositionSpec(position_id="p-1"),),
            position_scan=_ALIGNED_POSITION_SCAN,
        ),
        "R05-recon-conflict": ScenarioSpec(
            name="R05-recon-conflict",
            positions=(PositionSpec(position_id="p-1"),),
            deals=(_CLOSE_DEAL,),
            recon_status="INCONCLUSIVE",
            blocking_findings=(
                {"account_id": ACCOUNT_A, "code": "COMMISSION_MISMATCH", "deal_id": "d-close-1"},
            ),
            position_scan=_ALIGNED_POSITION_SCAN,
        ),
        # 20 seconds old against a 5-second live threshold.
        "R05-quote-stale": ScenarioSpec(
            name="R05-quote-stale",
            positions=(PositionSpec(position_id="p-1"),),
            quotes=(QuoteSpec(market_time=T0 - timedelta(seconds=20)),),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        # Mid held constant while the spread widens tenfold.
        "R05-spread-widening": ScenarioSpec(
            name="R05-spread-widening",
            positions=(
                PositionSpec(position_id="p-long", side="BUY", lots="1.00"),
                PositionSpec(position_id="p-short", side="SELL", lots="1.00"),
            ),
            quotes=(QuoteSpec(bid="1.10110", ask="1.10310"),),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        "R05-unsupported-symbol": ScenarioSpec(
            name="R05-unsupported-symbol",
            positions=(PositionSpec(position_id="p-gold", symbol="XAUUSD", lots="1.00",
                                    open_price="2400.00"),),
            quotes=(QuoteSpec(symbol="XAUUSD", bid="2410.00", ask="2410.50",
                              quote_id="q-XAUUSD-1"),),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
        # Everything aligned and healthy: the one fixture that may be READY.
        "R05-aligned-ready": ScenarioSpec(
            name="R05-aligned-ready",
            positions=(PositionSpec(position_id="p-1", lots="0.30"),),
            deals=(_OPEN_DEAL, _CLOSE_DEAL),
            position_scan=_ALIGNED_POSITION_SCAN,
            quote_bound_position_scan_id="scan-P",
        ),
    }


def scenario(name: str) -> ScenarioSpec:
    """Fetch a golden scenario by fixture id."""
    scenarios = golden_scenarios()
    if name not in scenarios:
        raise KeyError(f"unknown golden fixture {name!r}; known: {sorted(scenarios)}")
    return scenarios[name]


def synthetic_deals(
    count: int,
    *,
    seed: int = DEFAULT_SEED,
    start_seq: int = 1,
    account_id: str = ACCOUNT_A,
    base_time: datetime = T0 - timedelta(days=10),
) -> tuple[DealSpec, ...]:
    """Generate a reproducible deal set for load and property tests.

    Deterministic by construction rather than by seeding a PRNG: the amounts
    cycle through a fixed pattern, so the expected sum is computable in closed
    form and an independent oracle does not have to replay the generator
    (WF5-242, WF5-314).

    Timestamps run *backwards* from ``base_time`` rather than forwards. A
    forward-running generator put a large set past the end of the caller's
    reporting window, and the engine correctly excluded the overflow -- which
    looked like an arithmetic disagreement when it was a dataset that did not
    fit the request. Ending at ``base_time`` keeps a million rows inside a
    sane window while leaving every timestamp distinct.
    """
    import random

    rng = random.Random(seed)
    amounts = ["10.00", "-15.00", "0.00", "60.00", "-0.50", "2.25"]
    out: list[DealSpec] = []
    for index in range(count):
        out.append(
            DealSpec(
                deal_id=f"d-{seed}-{start_seq + index}",
                source_seq=start_seq + index,
                entry_kind="OUT",
                gross=amounts[index % len(amounts)],
                commission=f"-{(index % 7) * 0.5:.2f}",
                swap="0.00" if index % 3 else "-0.25",
                executed_at=base_time - timedelta(seconds=count - index),
                account_id=account_id,
            )
        )
    rng.shuffle(out)  # delivery order must not change the answer
    return tuple(out)


def synthetic_positions(
    count: int,
    *,
    seed: int = DEFAULT_SEED,
    symbols: Sequence[str] = ("EURUSD", "GBPUSD", "AUDUSD"),
    account_id: str = ACCOUNT_A,
) -> tuple[PositionSpec, ...]:
    """Generate a reproducible position book."""
    out: list[PositionSpec] = []
    for index in range(count):
        out.append(
            PositionSpec(
                position_id=f"p-{seed}-{index}",
                symbol=symbols[index % len(symbols)],
                side="BUY" if index % 2 == 0 else "SELL",
                lots=f"{Decimal('0.01') * ((index % 50) + 1):.2f}",
                open_price="1.10000",
                account_id=account_id,
            )
        )
    return tuple(out)
