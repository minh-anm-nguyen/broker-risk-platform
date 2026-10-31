"""Shared fixtures and the evidence-recording plugin.

The plugin is the part worth reading. Marking a test with
``@pytest.mark.wf5("WF5-061")`` does two things: it asserts the id exists in the
360-case matrix, and it makes the test's outcome land in
``artifacts/test_matrix_results.json`` as a PASS or a FAIL against that case.

That wiring is what stops the suite from drifting into a coverage claim nobody
checked. ``risk-report`` reads the artifact and prints what is covered *and what
is not*, so an untouched domain is visible rather than absent.

A test that injects a fault must confirm the injection fired, via the
``evidence`` fixture. Without that confirmation its record is
``INVALID_TEST``, not ``PASS`` -- a chaos case whose fault never happened has
tested the happy path under a misleading name (WF5-295).
"""

from __future__ import annotations

import sqlite3
import sys
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Iterator

import pytest

SRC = Path(__file__).resolve().parents[1] / "src"
if str(SRC) not in sys.path:  # pragma: no cover - import bootstrap
    sys.path.insert(0, str(SRC))

from risk.adapters.p03_reader import P03Reader  # noqa: E402
from risk.adapters.quotes import FixtureQuoteProvider  # noqa: E402
from risk.adapters.sql import SqliteExecutor  # noqa: E402
from risk.audit import InMemoryAuditSink, SqliteAuditSink  # noqa: E402
from risk.contract import RiskRequest  # noqa: E402
from risk.observability import MetricsRegistry  # noqa: E402
from risk.policies.loader import load_catalog, load_policy  # noqa: E402
from risk.service import RiskService, ServiceDependencies  # noqa: E402
from risk.status import Component  # noqa: E402
from risk_tools.evidence import (  # noqa: E402
    EvidenceLog,
    EvidenceRecord,
    OracleKind,
    default_artifact_path,
)
from risk_tools.fixtures import (  # noqa: E402
    T0,
    ScenarioSpec,
    build_harness_db,
    default_scope,
    quote_snapshot_for,
    scenario,
)
from risk_tools.matrix import CaseStatus, case  # noqa: E402

_LOG = EvidenceLog(default_artifact_path())


# --------------------------------------------------------------------------- #
# Evidence plugin
# --------------------------------------------------------------------------- #


def pytest_configure(config: pytest.Config) -> None:
    config.stash["risk_evidence_log"] = _LOG  # type: ignore[index]


def _case_ids(item: pytest.Item) -> tuple[str, ...]:
    ids: list[str] = []
    for marker in item.iter_markers(name="wf5"):
        for value in marker.args:
            case(str(value))  # fails the collection on an unknown id
            ids.append(str(value))
    return tuple(dict.fromkeys(ids))


def _marker_kwargs(item: pytest.Item) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for marker in item.iter_markers(name="wf5"):
        merged.update(marker.kwargs)
    return merged


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]):
    outcome = yield
    report = outcome.get_result()
    if report.when != "call":
        return
    ids = _case_ids(item)
    if not ids:
        return

    kwargs = _marker_kwargs(item)
    injects = bool(kwargs.get("injects", False))
    oracle = kwargs.get("oracle", OracleKind.GOLDEN_REFERENCE)
    enrichment: dict[str, Any] = getattr(item, "_risk_evidence", {}) or {}

    if report.skipped:
        status = CaseStatus.SKIPPED_WITH_APPROVED_EXCEPTION
    elif report.passed:
        status = CaseStatus.PASS
    else:
        status = CaseStatus.FAIL

    confirmed = enrichment.get("injection_confirmed", not injects)
    if status is CaseStatus.PASS and injects and not confirmed:
        # The test asserted the system behaved, but never proved the fault
        # happened. That is not evidence of resilience.
        status = CaseStatus.INVALID_TEST

    for case_id in ids:
        _LOG.record(
            EvidenceRecord(
                case_id=case_id,
                test_status=status,
                injection_confirmed=bool(confirmed),
                oracle=OracleKind(oracle),
                elapsed_ms=int(report.duration * 1000),
                test_nodeid=item.nodeid,
                expected_status=enrichment.get("expected_status"),
                observed_status=enrichment.get("observed_status"),
                expected_reason_codes=tuple(enrichment.get("expected_reason_codes", ())),
                observed_reason_codes=tuple(enrichment.get("observed_reason_codes", ())),
                expected_values=dict(enrichment.get("expected_values", {})),
                observed_values=dict(enrichment.get("observed_values", {})),
                dataset_seed=enrichment.get("dataset_seed"),
                fixture_id=enrichment.get("fixture_id"),
                input_digest=enrichment.get("input_digest"),
                output_digest=enrichment.get("output_digest"),
                rss_peak_mib=enrichment.get("rss_peak_mib"),
                notes=tuple(enrichment.get("notes", ())),
            )
        )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _LOG.records:
        _LOG.write()


@pytest.fixture
def evidence(request: pytest.FixtureRequest) -> Callable[..., None]:
    """Attach detail (and fault confirmation) to this test's evidence record."""

    def _attach(**fields: Any) -> None:
        current = getattr(request.node, "_risk_evidence", {}) or {}
        current.update(fields)
        request.node._risk_evidence = current  # type: ignore[attr-defined]

    return _attach


# --------------------------------------------------------------------------- #
# Policy / catalog fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def policy():
    return load_policy("risk-policy-v1")


@pytest.fixture(scope="session")
def policy_v2():
    return load_policy("risk-policy-v2")


@pytest.fixture(scope="session")
def catalog():
    return load_catalog("sim-usd-fx-v1")


@pytest.fixture
def metrics() -> MetricsRegistry:
    return MetricsRegistry()


@pytest.fixture
def audit_sink() -> InMemoryAuditSink:
    return InMemoryAuditSink()


# --------------------------------------------------------------------------- #
# Scenario / service fixtures
# --------------------------------------------------------------------------- #


@pytest.fixture
def harness() -> Iterator[Callable[[ScenarioSpec], sqlite3.Connection]]:
    """Factory that builds (and closes) harness databases."""
    connections: list[sqlite3.Connection] = []

    def _build(spec: ScenarioSpec) -> sqlite3.Connection:
        conn = build_harness_db(spec)
        connections.append(conn)
        return conn

    yield _build
    for conn in connections:
        conn.close()


@pytest.fixture
def service_factory(harness, policy, metrics, audit_sink):
    """Build a fully wired service over one scenario.

    Defaults are the permissive ones that let a healthy scenario reach READY,
    so a test that wants a failure has to say which thing it is breaking.
    """

    def _build(
        spec: ScenarioSpec,
        *,
        quote_provider: Any | None = None,
        active_policy: Any | None = None,
        sink: Any | None = None,
        clock: Any | None = None,
        connection: sqlite3.Connection | None = None,
    ) -> tuple[RiskService, sqlite3.Connection]:
        conn = connection or harness(spec)
        effective_policy = active_policy or policy
        if quote_provider is None:
            snapshot = quote_snapshot_for(spec)
            quote_provider = FixtureQuoteProvider(
                {snapshot.snapshot_id: snapshot}, default_snapshot_id=snapshot.snapshot_id
            )
        # `is not None`, not `or`: an empty InMemoryAuditSink is falsy because
        # it defines __len__, and `sink or audit_sink` silently substituted the
        # shared fixture for the one the test passed in.
        deps = ServiceDependencies(
            reader=P03Reader(SqliteExecutor(conn), effective_policy),
            quote_provider=quote_provider,
            audit_sink=audit_sink if sink is None else sink,
            metrics=metrics,
        )
        return RiskService(deps, effective_policy, clock=clock or (lambda: T0)), conn

    return _build


@pytest.fixture
def request_factory():
    """Build a RiskRequest with sane, explicit defaults."""

    def _build(
        spec: ScenarioSpec,
        *,
        components: frozenset[Component] | None = None,
        strict_composite: bool = True,
        account_ids: tuple[str, ...] | None = None,
        period_days: int = 30,
        valuation_as_of: Any | None = None,
        quote_snapshot_id: str | None = "__scenario__",
        **overrides: Any,
    ) -> RiskRequest:
        return RiskRequest(
            epoch_id=spec.epoch_id,
            components=components if components is not None else frozenset(Component),
            account_ids=account_ids,
            period_start=T0 - timedelta(days=period_days),
            period_end=T0 + timedelta(days=1),
            valuation_as_of=valuation_as_of or T0,
            quote_snapshot_id=(
                spec.quote_snapshot_id if quote_snapshot_id == "__scenario__" else quote_snapshot_id
            ),
            strict_composite=strict_composite,
            **overrides,
        )

    return _build


@pytest.fixture
def sqlite_audit(harness):
    """Audit sink backed by the harness ``risk_calculation_audit`` table."""

    def _build(connection: sqlite3.Connection) -> SqliteAuditSink:
        return SqliteAuditSink(connection)

    return _build


@pytest.fixture
def golden():
    """Access a golden scenario by fixture id."""
    return scenario


@pytest.fixture
def scope():
    return default_scope
