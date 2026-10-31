"""Shared test harness.

Design choices the rest of the suite depends on:

* **One composition root.** Tests build the app through ``build_context`` and
  ``create_app``, the same path production uses, so a passing test cannot be
  passing against a different wiring.
* **Deterministic clocks.** ``FakeClock`` drives both the monotonic clock used
  by quotas and deadlines and the wall clock used by token validation, so
  expiry, skew and rate limits are exact rather than flaky.
* **Real signed tokens.** The local issuer mints genuine HMAC tokens. There is
  no auth bypass flag anywhere in the service, and these tests would not work
  if there were one.
* **Evidence.** ``evidence`` records what each scenario observed, and
  ``tests/test_scenario_manifest.py`` turns that into the scenario-to-test
  manifest the design doc asks for.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, AsyncIterator, Iterator, Mapping

import pytest
from httpx import ASGITransport, AsyncClient
from risk_api.api.deps import AppContext
from risk_api.authn.test_issuer import LocalTestIssuer, TokenSpec
from risk_api.authz.principal_context import ScopePolicy
from risk_api.faults import FaultRegistry
from risk_api.lifespan import build_context, default_policies
from risk_api.main import create_app
from risk_api.repositories.read_views import SourceWorld, build_demo_world
from risk_api.settings import DEV_JWT_KEYS, Settings
from risk_engine import AccountFact, DealFact, InstrumentSpec, PositionFact, QuoteFact

SOURCE_ID = "sim-broker-a"
ACCOUNT_A = "00042"
ACCOUNT_B = "00043"

EVIDENCE_PATH = Path(__file__).resolve().parent / "_evidence.jsonl"


# --------------------------------------------------------------------- clock


@dataclass(slots=True)
class FakeClock:
    """A clock the tests move by hand."""

    monotonic_seconds: float = 1_000.0
    wall_seconds: int = 1_767_225_600  # 2026-01-01T00:00:00Z

    def monotonic(self) -> float:
        return self.monotonic_seconds

    def wall(self) -> int:
        return self.wall_seconds

    def advance(self, seconds: float) -> None:
        self.monotonic_seconds += seconds
        self.wall_seconds += int(seconds)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


# ------------------------------------------------------------------ evidence


@dataclass(slots=True)
class EvidenceRecorder:
    """Collects per-scenario evidence rows for the manifest."""

    rows: list[dict[str, Any]] = field(default_factory=list)

    def record(
        self,
        *,
        scenario_ids: tuple[str, ...],
        test_id: str,
        expected_http: int | None = None,
        observed_http: int | None = None,
        public_code: str | None = None,
        assertions: tuple[str, ...] = (),
        engine_calls: int | None = None,
        source_unchanged: bool | None = None,
        audit_decisions: tuple[str, ...] = (),
        injections: tuple[str, ...] = (),
    ) -> None:
        self.rows.append(
            {
                "scenario_ids": list(scenario_ids),
                "test_id": test_id,
                "expected_http": expected_http,
                "observed_http": observed_http,
                "public_code": public_code,
                "assertions": list(assertions),
                "engine_calls": engine_calls,
                "source_unchanged": source_unchanged,
                "audit_decisions": list(audit_decisions),
                "injections": list(injections),
            }
        )


@pytest.fixture(scope="session")
def evidence_log() -> Iterator[Path]:
    """Truncate the evidence log once per session, then let tests append."""
    if EVIDENCE_PATH.exists():
        EVIDENCE_PATH.unlink()
    yield EVIDENCE_PATH


@pytest.fixture
def evidence(evidence_log: Path) -> Iterator[EvidenceRecorder]:
    recorder = EvidenceRecorder()
    yield recorder
    if recorder.rows:
        with open(evidence_log, "a", encoding="utf-8") as handle:
            for row in recorder.rows:
                handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")


# -------------------------------------------------------------------- world


def add_account(
    world: SourceWorld,
    account_id: str,
    *,
    currency: str = "USD",
    scan_id: str = "scan-A",
) -> None:
    world.accounts[account_id] = AccountFact(account_id, currency, "PRESENT", scan_id)


def add_position(
    world: SourceWorld,
    position_id: str,
    account_id: str,
    *,
    symbol: str = "EURUSD",
    side: str = "BUY",
    lots: str = "0.5",
    open_price: str = "1.1000",
    status: str = "OPEN",
    presence: str = "PRESENT",
    scan_id: str | None = None,
) -> None:
    world.positions[position_id] = PositionFact(
        position_id=position_id,
        account_id=account_id,
        symbol=symbol,
        side=side,  # type: ignore[arg-type]
        lots=Decimal(lots),
        open_price=Decimal(open_price),
        source_status=status,
        presence=presence,
        published_scan_id=scan_id or world.position_scan_id,
    )


def add_deal(
    world: SourceWorld,
    deal_id: str,
    account_id: str,
    *,
    seq: int,
    gross: str = "0.00",
    commission: str = "0.00",
    swap: str = "0.00",
    executed_at: str = "2026-10-15T10:00:00Z",
) -> None:
    world.deals[deal_id] = DealFact(
        deal_id=deal_id,
        account_id=account_id,
        source_seq=seq,
        entry_kind="OUT",
        gross_profit=Decimal(gross),
        commission=Decimal(commission),
        swap=Decimal(swap),
        executed_at_utc=executed_at,
    )


def add_instrument(
    world: SourceWorld,
    symbol: str,
    *,
    base: str = "EUR",
    quote: str = "USD",
    profit: str = "USD",
    calc_mode: str = "FOREX_LINEAR",
    contract_size: str = "100000",
) -> None:
    world.instruments[symbol] = InstrumentSpec(
        symbol=symbol,
        base_currency=base,
        quote_currency=quote,
        profit_currency=profit,
        calc_mode=calc_mode,
        contract_size=Decimal(contract_size),
        tick_size=Decimal("0.00001"),
        volume_step=Decimal("0.01"),
        spec_version="sim-usd-fx-v1",
    )


def add_quote(
    world: SourceWorld,
    symbol: str,
    *,
    bid: str,
    ask: str,
    snapshot_id: str | None = None,
) -> None:
    world.quotes[symbol] = QuoteFact(
        symbol=symbol,
        bid=Decimal(bid),
        ask=Decimal(ask),
        market_time_utc=world.quote_captured_at_utc,
        quote_id=f"q-{symbol}",
        snapshot_id=snapshot_id or world.quote_snapshot_id or "quote-S1030",
    )


@pytest.fixture
def world() -> SourceWorld:
    """A fully proven two-account world.

    Account 00042 belongs to analyst A and 00043 to analyst B. Both exist so a
    cross-tenant attempt has a real target to fail against.
    """
    built = build_demo_world(source_id=SOURCE_ID)
    add_account(built, ACCOUNT_B)
    add_position(built, "P-0002", ACCOUNT_B, side="SELL", lots="0.2", open_price="1.1010")
    add_deal(built, "D-0002", ACCOUNT_B, seq=1051, gross="10.00", commission="-1.00")
    return built


# ----------------------------------------------------------------- settings


@pytest.fixture
def settings_kwargs(tmp_path: Path) -> dict[str, Any]:
    return {
        "environment": "test",
        "faults_enabled": True,
        "audit_backend": "memory",
        "audit_path": str(tmp_path / "audit.jsonl"),
        "log_level": "CRITICAL",
    }


@pytest.fixture
def settings(settings_kwargs: dict[str, Any]) -> Settings:
    return Settings(**settings_kwargs)


@pytest.fixture
def policies() -> dict[str, ScopePolicy]:
    return default_policies(SOURCE_ID)


@pytest.fixture
def faults(settings: Settings) -> FaultRegistry:
    return FaultRegistry(enabled=settings.faults_enabled)


@pytest.fixture
def context(
    settings: Settings,
    world: SourceWorld,
    policies: dict[str, ScopePolicy],
    faults: FaultRegistry,
    clock: FakeClock,
) -> AppContext:
    return build_context(
        settings=settings,
        world=world,
        policies=policies,
        faults=faults,
        clock=clock.monotonic,
        wall_clock=clock.wall,
    )


@pytest.fixture
def app(context: AppContext, settings: Settings):
    return create_app(context=context, settings=settings)


@pytest.fixture
async def client(app, context: AppContext) -> AsyncIterator[AsyncClient]:
    """An in-process HTTP client with the worker pool genuinely running."""
    context.workers.start()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://risk-api.test") as http:
        yield http
    await context.workers.shutdown(drain_seconds=0.2)


# -------------------------------------------------------------------- tokens

_TENANTS = {
    "analyst-a": "tenant-a",
    "analyst-b": "tenant-b",
    "viewer-a": "tenant-a",
    "ops-a": "tenant-a",
}


@pytest.fixture
def issuer(settings: Settings) -> LocalTestIssuer:
    return LocalTestIssuer(
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        keys=settings.jwt_keys,
        environment=settings.environment,
    )


@dataclass(slots=True)
class TokenFactory:
    """Mints tokens for the fixture principals."""

    issuer: LocalTestIssuer
    clock: FakeClock

    def spec(
        self,
        subject: str = "analyst-a",
        *,
        roles: tuple[str, ...] = ("risk_analyst",),
        tenant_id: str | None = None,
        scopes: tuple[str, ...] = (),
        expires_in: int = 300,
        **kwargs: Any,
    ) -> TokenSpec:
        return TokenSpec(
            subject=subject,
            tenant_id=tenant_id if tenant_id is not None else _TENANTS.get(subject, "tenant-a"),
            scopes=scopes,
            expires_in=expires_in,
            token_id=f"jti-{subject}",
            extra_claims={"roles": list(roles)},
            **kwargs,
        )

    def mint(self, subject: str = "analyst-a", **kwargs: Any) -> str:
        return self.issuer.mint(self.spec(subject, **kwargs), now=self.clock.wall())

    def header(self, subject: str = "analyst-a", **kwargs: Any) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.mint(subject, **kwargs)}"}


@pytest.fixture
def tokens(issuer: LocalTestIssuer, clock: FakeClock) -> TokenFactory:
    return TokenFactory(issuer=issuer, clock=clock)


@pytest.fixture
def auth(tokens: TokenFactory) -> dict[str, str]:
    """Authorization header for analyst A, the happy-path principal."""
    return tokens.header("analyst-a")


# ------------------------------------------------------------------ helpers


@pytest.fixture
def risk_url() -> str:
    return f"/api/v1/accounts/{ACCOUNT_A}/risk"


def json_tokens(raw: str) -> Mapping[str, Any]:
    """Parse a body while keeping numbers distinguishable from strings.

    The hooks let a test prove a money field arrived as a JSON *string*, not
    merely that it compares equal to one.
    """
    floats: list[str] = []
    ints: list[str] = []

    def _float(value: str) -> float:
        floats.append(value)
        return float(value)

    def _int(value: str) -> int:
        ints.append(value)
        return int(value)

    parsed = json.loads(raw, parse_float=_float, parse_int=_int)
    return {"body": parsed, "float_tokens": floats, "int_tokens": ints}


@pytest.fixture
def no_float_money():
    """Assert no field arrived as a JSON floating-point number."""

    def _check(raw: str) -> None:
        probe = json_tokens(raw)
        assert probe["float_tokens"] == [], f"float tokens in payload: {probe['float_tokens']}"

    return _check


def dev_key_id() -> str:
    return next(iter(DEV_JWT_KEYS))


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep ambient RISK_API_* variables out of the test configuration."""
    for name in list(os.environ):
        if name.startswith("RISK_API_"):
            monkeypatch.delenv(name, raising=False)
