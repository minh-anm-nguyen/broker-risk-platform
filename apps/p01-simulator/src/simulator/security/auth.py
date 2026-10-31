"""Bearer authentication and plane separation.

Three rules, each with a failure it prevents:

* **Constant-time comparison.** ``==`` on a secret leaks its prefix through
  timing. ``hmac.compare_digest`` does not.
* **401 says nothing about the token.** "Unknown token" and "wrong plane for
  this token" must not be distinguishable to an unauthenticated caller, or the
  error becomes an oracle for guessing (WF-102).
* **Only a token id is ever logged.** A short digest is enough to correlate
  requests from one caller; the token itself must never reach a log line, a
  metric label or a trace (WF-107).

A data-plane token presenting itself at an admin route gets 403, not 404:
hiding the route's existence from an *authenticated* caller buys nothing and
makes the failure harder to diagnose (WF-101).
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from enum import StrEnum

from ..domain.models import SimulatorError
from ..settings import Settings


class Plane(StrEnum):
    DATA = "data"
    ADMIN = "admin"


class UnauthorizedError(SimulatorError):
    code = "UNAUTHORIZED"


class ForbiddenError(SimulatorError):
    code = "FORBIDDEN"


@dataclass(frozen=True, slots=True)
class Principal:
    plane: Plane
    #: Short digest of the presented token. Safe to log and to use as a rate
    #: limit key; it is not reversible to the token.
    token_id: str

    @property
    def is_admin(self) -> bool:
        return self.plane is Plane.ADMIN


def token_id_for(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


class Authenticator:
    def __init__(self, settings: Settings) -> None:
        self._data_token = settings.effective_data_token
        self._admin_token = settings.effective_admin_token
        self._data_token_id = token_id_for(self._data_token)
        self._admin_token_id = token_id_for(self._admin_token)

    def authenticate(self, authorization: str | None, *, required: Plane) -> Principal:
        token = self._extract(authorization)
        if token is None:
            raise UnauthorizedError(
                "a bearer token is required",
                details={"scheme": "Bearer"},
            )
        if hmac.compare_digest(token, self._admin_token):
            # The admin plane is a superset of the data plane: an operator
            # debugging a dataset should not need a second token to read it.
            return Principal(plane=Plane.ADMIN, token_id=self._admin_token_id)
        if hmac.compare_digest(token, self._data_token):
            if required is Plane.ADMIN:
                raise ForbiddenError(
                    "this token is not permitted on the control plane",
                    details={"required_plane": Plane.ADMIN.value},
                )
            return Principal(plane=Plane.DATA, token_id=self._data_token_id)
        raise UnauthorizedError("the presented token is not recognized")

    @staticmethod
    def _extract(authorization: str | None) -> str | None:
        if not authorization:
            return None
        parts = authorization.split(None, 1)
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        token = parts[1].strip()
        # An absurdly long header is rejected before any comparison work.
        if not token or len(token) > 512:
            return None
        return token
