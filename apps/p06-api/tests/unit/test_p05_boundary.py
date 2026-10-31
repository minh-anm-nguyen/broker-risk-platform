"""Which Project 05 does this API actually serve from?

There are two Project 05 surfaces in the repository and they are not API
compatible:

* ``packages/risk-engine`` - the pure Decimal domain library, package
  ``risk_engine``, about 1.2k lines. ``RiskEngine``, ``RiskRequest`` and
  ``RiskResult`` at top level.
* ``apps/p05-risk-engine`` - the full implementation, package ``risk``, about
  8.2k lines, with database adapters, policy loading, quality gates and
  tooling. ``RiskRequest`` and ``RiskResult`` live in ``risk.contract``.

This API imports the first. Contract gap CG-01 records the consequence: a
response served here is computed by the reference library and does not exercise
the full P05 policy, gate and adapter code. That is a deliberate P0 scope
decision rather than an oversight - the full engine reads Project 03's
PostgreSQL tables, and this API's read views are still the in-memory reference
implementation.

These tests exist so the decision cannot drift in either direction without
someone noticing:

* if an import of ``risk`` appears, the API has been half-wired to the full
  engine and the scope statement is no longer true;
* if ``risk_engine`` stops being imported, the API has grown its own financial
  arithmetic, which non-negotiable 1 of the design forbids outright.
"""

from __future__ import annotations

import pathlib
import re

import pytest

import risk_api

pytestmark = pytest.mark.unit

SRC = pathlib.Path(risk_api.__file__).parent

#: An import of the full P05 package. Matches ``import risk``,
#: ``from risk import x`` and ``from risk.contract import y``, but not
#: ``risk_engine`` or ``risk_api``.
_FULL_P05 = re.compile(r"^\s*(?:from|import)\s+risk(?:\.|\s|$)", re.MULTILINE)
_REFERENCE_P05 = re.compile(r"^\s*(?:from|import)\s+risk_engine(?:\.|\s|$)", re.MULTILINE)


def _sources() -> list[pathlib.Path]:
    return [p for p in sorted(SRC.rglob("*.py")) if "__pycache__" not in p.parts]


def test_the_api_does_not_import_the_full_p05_package() -> None:
    """Guards the scope statement in this module's docstring (CG-01).

    If this fails, someone has started wiring the full engine in. That may well
    be the right move - but the manifest entry, the README and this docstring
    all claim otherwise, and a half-finished migration nobody declared is how a
    financial path ends up with two sources of truth.
    """
    offenders = [
        str(path.relative_to(SRC))
        for path in _sources()
        if _FULL_P05.search(path.read_text(encoding="utf-8"))
    ]
    assert offenders == [], (
        "these modules import the full Project 05 package `risk`, but CG-01 "
        f"records this API as served by the reference library: {offenders}"
    )


def test_the_api_does_import_the_reference_engine() -> None:
    """The other direction.

    A thin HTTP layer must call Project 05, not reimplement it. If nothing here
    imports ``risk_engine`` any more, the arithmetic has moved into this
    service, which design non-negotiable 1 forbids.
    """
    importers = [
        str(path.relative_to(SRC))
        for path in _sources()
        if _REFERENCE_P05.search(path.read_text(encoding="utf-8"))
    ]
    assert importers, "no module imports risk_engine: has the arithmetic moved into the API?"


def test_the_reference_engine_is_the_one_on_the_import_path() -> None:
    """Belt and braces: the module actually loaded is the shared package, not a
    vendored copy that could drift from it."""
    import risk_engine

    resolved = pathlib.Path(risk_engine.__file__).resolve()
    assert resolved.parent.name == "risk_engine"
    assert "packages" in resolved.parts, (
        f"risk_engine resolved to {resolved}, which is not the shared package "
        "under packages/ - a vendored copy would drift from the contract"
    )
