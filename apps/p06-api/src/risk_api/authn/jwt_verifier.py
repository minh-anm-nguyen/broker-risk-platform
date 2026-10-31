"""Bearer token verification.

This build verifies HMAC-signed tokens from a configured local issuer. The
properties that matter are the ones an attacker probes:

* the algorithm comes from an allowlist, never from the token header alone;
* ``alg: none`` and any unsigned token are rejected outright;
* ``kid`` is a lookup key into configured material, never a URL to fetch;
* ``iss``, ``aud``, ``exp`` and ``nbf`` are all checked, with bounded skew;
* signature comparison is constant time.

Replacing this with a real OIDC/JWKS verifier (P1) must preserve every one of
those properties.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any, Final, Mapping

__all__ = [
    "VerifiedToken",
    "TokenError",
    "JwtVerifier",
    "b64url_decode",
    "b64url_encode",
]

_SUPPORTED_HMAC: Final[Mapping[str, Any]] = {
    "HS256": hashlib.sha256,
    "HS384": hashlib.sha384,
    "HS512": hashlib.sha512,
}

# A JWT is three dot-separated parts. Anything else is not a token we parse.
_EXPECTED_SEGMENTS: Final = 3
_MAX_TOKEN_BYTES: Final = 8 * 1024


class TokenError(Exception):
    """Authentication failed.

    ``reason`` is for logs and metrics only. The HTTP layer returns a single
    opaque 401 so a prober cannot tell a bad signature from a bad audience.
    """

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass(frozen=True, slots=True)
class VerifiedToken:
    """Claims that survived verification. Nothing here is client-controlled."""

    subject: str
    issuer: str
    audience: str
    key_id: str
    algorithm: str
    issued_at: int | None
    expires_at: int
    not_before: int | None
    scopes: tuple[str, ...]
    tenant_id: str | None
    token_id: str | None
    raw_claims: Mapping[str, Any]


def b64url_decode(segment: str) -> bytes:
    """Decode a base64url segment, rejecting non-canonical padding."""
    if not isinstance(segment, str) or segment == "":
        raise TokenError("empty token segment")
    if "=" in segment or "+" in segment or "/" in segment:
        raise TokenError("token segment is not base64url without padding")
    padding = "=" * (-len(segment) % 4)
    try:
        return base64.urlsafe_b64decode(segment + padding)
    except (binascii.Error, ValueError) as exc:
        raise TokenError("undecodable token segment") from exc


def b64url_encode(raw: bytes) -> str:
    """Encode to base64url without padding."""
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


class JwtVerifier:
    """Verify a compact JWS token against configured local key material."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        keys: Mapping[str, str],
        allowed_algorithms: tuple[str, ...],
        clock_skew_seconds: int = 30,
        max_lifetime_seconds: int = 3600,
    ) -> None:
        if not keys:
            raise ValueError("at least one signing key must be configured")
        bad = [algorithm for algorithm in allowed_algorithms if algorithm not in _SUPPORTED_HMAC]
        if bad:
            raise ValueError(f"unsupported algorithms configured: {bad}")
        self._issuer = issuer
        self._audience = audience
        self._keys = dict(keys)
        self._allowed_algorithms = frozenset(allowed_algorithms)
        self._skew = max(0, int(clock_skew_seconds))
        self._max_lifetime = max_lifetime_seconds
        self._denied_token_ids: set[str] = set()
        self._denied_key_ids: set[str] = set()

    # ------------------------------------------------------------- lifecycle

    def deny_token_id(self, token_id: str) -> None:
        """Emergency revocation of a single token."""
        self._denied_token_ids.add(token_id)

    def deny_key_id(self, key_id: str) -> None:
        """Emergency invalidation of a compromised signing key."""
        self._denied_key_ids.add(key_id)

    def rotate_keys(self, keys: Mapping[str, str]) -> None:
        """Replace the key set, for example after a rotation window closes."""
        if not keys:
            raise ValueError("cannot rotate to an empty key set")
        self._keys = dict(keys)

    @property
    def key_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._keys))

    # ---------------------------------------------------------------- verify

    def verify(self, token: str, *, now: int) -> VerifiedToken:
        """Verify a token at instant ``now`` (epoch seconds)."""
        if not isinstance(token, str) or not token:
            raise TokenError("missing token")
        if len(token.encode("utf-8")) > _MAX_TOKEN_BYTES:
            raise TokenError("token exceeds the accepted size")

        segments = token.split(".")
        if len(segments) != _EXPECTED_SEGMENTS:
            # Two segments means an unsigned JWT; there is no mode for that.
            raise TokenError("token is not a three-segment compact JWS")
        header_segment, payload_segment, signature_segment = segments
        if signature_segment == "":
            raise TokenError("token carries an empty signature")

        header = self._decode_json(header_segment, what="header")
        algorithm = header.get("alg")
        if not isinstance(algorithm, str):
            raise TokenError("token header has no string alg")
        if algorithm.lower() == "none":
            raise TokenError("alg none is never accepted")
        if algorithm not in self._allowed_algorithms:
            # Algorithm confusion is prevented here: the token does not choose.
            raise TokenError(f"algorithm {algorithm} is not allowlisted")
        if header.get("typ") not in (None, "JWT"):
            raise TokenError("unexpected token type")
        if "crit" in header:
            raise TokenError("unsupported critical header parameters")

        key_id = header.get("kid")
        if not isinstance(key_id, str) or not key_id:
            raise TokenError("token header has no kid")
        if "://" in key_id or key_id.startswith("/"):
            # A kid is an identifier, not a location to dereference.
            raise TokenError("kid must not look like a URL")
        if key_id in self._denied_key_ids:
            raise TokenError("signing key is denylisted")
        secret = self._keys.get(key_id)
        if secret is None:
            raise TokenError("unknown signing key")

        expected = self._sign(f"{header_segment}.{payload_segment}", secret, algorithm)
        provided = b64url_decode(signature_segment)
        if not hmac.compare_digest(expected, provided):
            raise TokenError("signature mismatch")

        claims = self._decode_json(payload_segment, what="payload")
        return self._validate_claims(claims, key_id=key_id, algorithm=algorithm, now=now)

    # -------------------------------------------------------------- internals

    @staticmethod
    def _sign(signing_input: str, secret: str, algorithm: str) -> bytes:
        return hmac.new(
            secret.encode("utf-8"), signing_input.encode("ascii"), _SUPPORTED_HMAC[algorithm]
        ).digest()

    @staticmethod
    def _decode_json(segment: str, *, what: str) -> Mapping[str, Any]:
        try:
            decoded = json.loads(b64url_decode(segment).decode("utf-8"))
        except TokenError:
            raise
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TokenError(f"token {what} is not valid JSON") from exc
        if not isinstance(decoded, dict):
            raise TokenError(f"token {what} is not a JSON object")
        return decoded

    def _validate_claims(
        self, claims: Mapping[str, Any], *, key_id: str, algorithm: str, now: int
    ) -> VerifiedToken:
        issuer = claims.get("iss")
        if issuer != self._issuer:
            raise TokenError("issuer is not allowlisted")

        audience = claims.get("aud")
        if isinstance(audience, str):
            audiences = (audience,)
        elif isinstance(audience, list) and all(isinstance(item, str) for item in audience):
            audiences = tuple(audience)
        else:
            raise TokenError("audience claim is missing or malformed")
        if self._audience not in audiences:
            # A token minted for the P04 admin API is not a credential here.
            raise TokenError("audience does not include this API")

        subject = claims.get("sub")
        if not isinstance(subject, str) or not subject:
            raise TokenError("subject claim is missing")

        expires_at = claims.get("exp")
        if not isinstance(expires_at, int) or isinstance(expires_at, bool):
            raise TokenError("exp claim is missing or not an integer")
        if now > expires_at + self._skew:
            raise TokenError("token expired")

        not_before = claims.get("nbf")
        if not_before is not None:
            if not isinstance(not_before, int) or isinstance(not_before, bool):
                raise TokenError("nbf claim is not an integer")
            if now + self._skew < not_before:
                raise TokenError("token not yet valid")

        issued_at = claims.get("iat")
        if issued_at is not None:
            if not isinstance(issued_at, int) or isinstance(issued_at, bool):
                raise TokenError("iat claim is not an integer")
            if issued_at - self._skew > now:
                raise TokenError("token issued in the future")
            if expires_at - issued_at > self._max_lifetime:
                raise TokenError("token lifetime exceeds policy")

        token_id = claims.get("jti")
        if token_id is not None and not isinstance(token_id, str):
            raise TokenError("jti claim is not a string")
        if token_id is not None and token_id in self._denied_token_ids:
            raise TokenError("token is revoked")

        raw_scopes = claims.get("scope", "")
        if isinstance(raw_scopes, str):
            scopes = tuple(part for part in raw_scopes.split() if part)
        elif isinstance(raw_scopes, list) and all(isinstance(item, str) for item in raw_scopes):
            scopes = tuple(raw_scopes)
        else:
            raise TokenError("scope claim is malformed")

        tenant_id = claims.get("tenant_id")
        if tenant_id is not None and not isinstance(tenant_id, str):
            raise TokenError("tenant_id claim is not a string")

        return VerifiedToken(
            subject=subject,
            issuer=issuer,
            audience=self._audience,
            key_id=key_id,
            algorithm=algorithm,
            issued_at=issued_at,
            expires_at=expires_at,
            not_before=not_before,
            scopes=scopes,
            tenant_id=tenant_id,
            token_id=token_id,
            raw_claims=dict(claims),
        )
