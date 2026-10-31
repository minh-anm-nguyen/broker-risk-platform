"""Native MT5 Manager SDK driver - a guarded, deliberately unimplemented seam.

This module exists so the failure is **loud and early**. Importing it on a
platform that cannot host the native wheel, or constructing it without the
SDK present, raises
:class:`~mt5_connector.errors.IncompatibleSdkError` before a session is opened
and before readiness can report true (WF2-E-01, WF2-O-01).

Why there is no implementation here:

* The MetaQuotes ``mt5manager`` distribution publishes **Windows x86-64**
  wheels. Installing one into a Linux container and calling it does not work,
  and a driver that pretended otherwise would turn a deployment mistake into
  a runtime mystery.
* Package metadata proves nothing about thread safety, about whether a call
  can be cancelled, or about whether the API offers anything equivalent to
  the simulator's ``cursor`` / ``source_seq`` / ``snapshot_seq``. Writing a
  mapping against assumed semantics would produce a connector that reports a
  gap-free sync it cannot deliver (design section 2, 15.5).
* Connecting to a broker endpoint needs authorisation, credentials and a
  documented read-only role. None of that is a code problem.

So the capability set this driver would negotiate is **all false**, and the
honest state for the feature is ``NOT_SUPPORTED`` rather than a stub that
returns plausible-looking data. :data:`REQUIRED_EVIDENCE` lists what has to
exist before this file becomes real; each item is a question the vendor's
documentation or a conformance test must answer, not a line of code.
"""

from __future__ import annotations

import platform
import sys
from typing import Any, Final

from ..config import Settings
from ..errors import IncompatibleSdkError
from .base import DriverIdentity, DriverResponse

__all__ = ["ManagerSdkWindowsDriver", "REQUIRED_EVIDENCE", "preflight"]

#: What must be verified before a native driver is written. Published as data
#: so the scorecard can render it and nobody has to re-derive the list.
REQUIRED_EVIDENCE: Final[tuple[str, ...]] = (
    "vendor documentation for the Manager API methods this connector needs",
    "an authorised test broker endpoint and a read-only role that can reach it",
    "a wheel whose platform tag matches the host, verified by import on that host",
    "written evidence on thread safety: may two calls share one session?",
    "written evidence on cancellation: can a call be aborted, or does it hang?",
    "units, scale, sign convention and timezone for every money and time field",
    "enum translations for side, entry and position status, with test vectors",
    "whether any stable snapshot, cursor or incremental sequence exists at all",
    "hedging versus netting semantics for position identity",
    "behaviour of callbacks after logout, and which thread they arrive on",
)

_SUPPORTED_PLATFORM: Final[str] = "Windows"
_SUPPORTED_MACHINES: Final[frozenset[str]] = frozenset({"AMD64", "x86_64"})


def preflight() -> list[str]:
    """Reasons this host cannot run the native driver. Empty means "maybe".

    "Maybe" is as strong as this check gets: a passing preflight means the
    platform is not disqualifying, not that the SDK works.
    """
    reasons: list[str] = []
    if platform.system() != _SUPPORTED_PLATFORM:
        reasons.append(
            f"native Manager SDK wheels target {_SUPPORTED_PLATFORM}; this host runs "
            f"{platform.system()}"
        )
    if platform.machine() not in _SUPPORTED_MACHINES:
        reasons.append(
            f"native Manager SDK wheels target x86-64; this host reports "
            f"{platform.machine() or 'unknown'}"
        )
    if sys.maxsize <= 2**32:
        reasons.append("a 64-bit Python interpreter is required")
    try:  # pragma: no cover - depends on host
        __import__("mt5manager")
    except ImportError:
        reasons.append("the mt5manager package is not importable in this interpreter")
    return reasons


class ManagerSdkWindowsDriver:
    """Refuses to pretend. Construction fails unless the host could host it.

    Even on a qualifying host every read raises: there is no verified mapping
    to implement, and returning synthesised data would be the single worst
    outcome available - a connector that looks integrated and is not.
    """

    source_kind = "manager_sdk"

    def __init__(self, settings: Settings, *, token: str = "", **_: Any) -> None:
        reasons = preflight()
        if reasons:
            raise IncompatibleSdkError(
                "this host cannot run the native MT5 Manager SDK driver",
                source_id=settings.source_id,
                details={"reasons": reasons, "required_evidence": list(REQUIRED_EVIDENCE)},
            )
        self._settings = settings

    def open(self) -> DriverIdentity:  # pragma: no cover - unreachable without the SDK
        raise IncompatibleSdkError(
            "the native Manager SDK driver is not implemented in this build; no "
            "verified field, capability or lifecycle mapping exists yet",
            source_id=self._settings.source_id,
            details={"required_evidence": list(REQUIRED_EVIDENCE), "status": "NOT_SUPPORTED"},
        )

    def close(self) -> None:  # pragma: no cover
        return None

    def _unsupported(self, operation: str) -> IncompatibleSdkError:  # pragma: no cover
        return IncompatibleSdkError(
            f"{operation} has no verified native mapping in this build",
            source_id=self._settings.source_id,
            details={"operation": operation, "status": "NOT_SUPPORTED"},
        )

    def fetch_meta(self) -> DriverResponse:  # pragma: no cover
        raise self._unsupported("fetch_meta")

    def fetch_accounts(self, **_: Any) -> DriverResponse:  # pragma: no cover
        raise self._unsupported("fetch_accounts")

    def fetch_positions(self, **_: Any) -> DriverResponse:  # pragma: no cover
        raise self._unsupported("fetch_positions")

    def fetch_deals(self, **_: Any) -> DriverResponse:  # pragma: no cover
        raise self._unsupported("fetch_deals")
