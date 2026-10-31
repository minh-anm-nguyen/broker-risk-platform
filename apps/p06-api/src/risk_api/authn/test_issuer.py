"""A deterministic local token issuer for development and tests.

This exists so the test suite can mint *genuinely signed* tokens instead of
the service growing a debug bypass. It is the only place that holds a signing
secret, and it refuses to run outside local/test configuration.

The ``mint_*`` helpers deliberately expose the ability to produce invalid
tokens (wrong algorithm, no signature, future ``nbf``) because the negative
authentication tests need real adversarial inputs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Mapping

from .jwt_verifier import b64url_encode

__all__ = ["LocalTestIssuer", "TokenSpec"]


def _segment(payload: Mapping[str, Any]) -> str:
    return b64url_encode(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    )


@dataclass(frozen=True, slots=True)
class TokenSpec:
    """What to put in a minted token."""

    subject: str
    scopes: tuple[str, ...] = ()
    tenant_id: str | None = None
    token_id: str | None = None
    issued_at: int | None = None
    expires_in: int = 300
    not_before_offset: int | None = None
    audience: str | list[str] | None = None
    issuer: str | None = None
    extra_claims: Mapping[str, Any] | None = None


class LocalTestIssuer:
    """Mint HMAC tokens for the configured local issuer."""

    def __init__(
        self,
        *,
        issuer: str,
        audience: str,
        keys: Mapping[str, str],
        default_key_id: str | None = None,
        environment: str = "local",
    ) -> None:
        if environment not in ("local", "test"):
            raise RuntimeError("the local test issuer must never run outside local/test")
        if not keys:
            raise ValueError("at least one signing key is required")
        self._issuer = issuer
        self._audience = audience
        self._keys = dict(keys)
        self._default_key_id = default_key_id or sorted(self._keys)[0]

    # ----------------------------------------------------------------- valid

    def mint(self, spec: TokenSpec, *, now: int, key_id: str | None = None, algorithm: str = "HS256") -> str:
        """Mint a correctly signed token."""
        kid = key_id or self._default_key_id
        secret = self._keys.get(kid)
        if secret is None:
            raise KeyError(f"unknown key id {kid!r}")
        header = {"alg": algorithm, "typ": "JWT", "kid": kid}
        claims: dict[str, Any] = {
            "iss": spec.issuer or self._issuer,
            "aud": spec.audience if spec.audience is not None else self._audience,
            "sub": spec.subject,
            "iat": spec.issued_at if spec.issued_at is not None else now,
            "exp": (spec.issued_at if spec.issued_at is not None else now) + spec.expires_in,
            "scope": " ".join(spec.scopes),
        }
        if spec.not_before_offset is not None:
            claims["nbf"] = now + spec.not_before_offset
        if spec.tenant_id is not None:
            claims["tenant_id"] = spec.tenant_id
        if spec.token_id is not None:
            claims["jti"] = spec.token_id
        if spec.extra_claims:
            claims.update(spec.extra_claims)
        return self._assemble(header, claims, secret, algorithm)

    # -------------------------------------------------------------- invalid

    def mint_unsigned(self, spec: TokenSpec, *, now: int) -> str:
        """Produce an ``alg: none`` token with an empty signature segment."""
        header = {"alg": "none", "typ": "JWT", "kid": self._default_key_id}
        claims = self._claims_for(spec, now)
        return f"{_segment(header)}.{_segment(claims)}."

    def mint_with_tampered_payload(
        self, spec: TokenSpec, *, now: int, overrides: Mapping[str, Any]
    ) -> str:
        """Sign one payload, then swap in a different one, keeping the signature."""
        token = self.mint(spec, now=now)
        header_segment, _, signature_segment = token.split(".")
        claims = dict(self._claims_for(spec, now))
        claims.update(overrides)
        return f"{header_segment}.{_segment(claims)}.{signature_segment}"

    def mint_with_unknown_key(self, spec: TokenSpec, *, now: int, key_id: str = "attacker-kid") -> str:
        """Sign with a key the verifier does not know, under a plausible kid."""
        header = {"alg": "HS256", "typ": "JWT", "kid": key_id}
        claims = self._claims_for(spec, now)
        return self._assemble(header, claims, "attacker-controlled-secret", "HS256")

    def mint_with_raw_header(self, spec: TokenSpec, *, now: int, header: Mapping[str, Any]) -> str:
        """Sign with a caller-supplied header, for header-validation tests."""
        kid = header.get("kid", self._default_key_id)
        secret = self._keys.get(kid, "attacker-controlled-secret")
        algorithm = header.get("alg", "HS256")
        claims = self._claims_for(spec, now)
        if algorithm not in ("HS256", "HS384", "HS512"):
            # Cannot really sign with an unsupported algorithm; emit HS256 bytes
            # under the declared header so the verifier must reject on `alg`.
            signing_algorithm = "HS256"
        else:
            signing_algorithm = algorithm
        return self._assemble(dict(header), claims, secret, signing_algorithm)

    # ------------------------------------------------------------- internals

    def _claims_for(self, spec: TokenSpec, now: int) -> dict[str, Any]:
        issued = spec.issued_at if spec.issued_at is not None else now
        claims: dict[str, Any] = {
            "iss": spec.issuer or self._issuer,
            "aud": spec.audience if spec.audience is not None else self._audience,
            "sub": spec.subject,
            "iat": issued,
            "exp": issued + spec.expires_in,
            "scope": " ".join(spec.scopes),
        }
        if spec.not_before_offset is not None:
            claims["nbf"] = now + spec.not_before_offset
        if spec.tenant_id is not None:
            claims["tenant_id"] = spec.tenant_id
        if spec.token_id is not None:
            claims["jti"] = spec.token_id
        if spec.extra_claims:
            claims.update(spec.extra_claims)
        return claims

    @staticmethod
    def _assemble(
        header: Mapping[str, Any], claims: Mapping[str, Any], secret: str, algorithm: str
    ) -> str:
        import hashlib
        import hmac

        digestmod = {"HS256": hashlib.sha256, "HS384": hashlib.sha384, "HS512": hashlib.sha512}[
            algorithm
        ]
        header_segment = _segment(header)
        payload_segment = _segment(claims)
        signature = hmac.new(
            secret.encode("utf-8"), f"{header_segment}.{payload_segment}".encode("ascii"), digestmod
        ).digest()
        return f"{header_segment}.{payload_segment}.{b64url_encode(signature)}"
