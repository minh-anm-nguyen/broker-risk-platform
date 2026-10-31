"""Token verification, probed the way an attacker would.

WF06-B. Every case here is a real forged or malformed token built by the local
issuer, not a mocked verifier result.
"""

from __future__ import annotations

import json

import pytest
from risk_api.authn.jwt_verifier import JwtVerifier, TokenError, b64url_encode
from risk_api.authn.test_issuer import LocalTestIssuer, TokenSpec

pytestmark = pytest.mark.unit

ISSUER = "https://issuer.local.test/risk"
AUDIENCE = "risk-reporting-api"
KEYS = {"k1": "key-one-secret-material", "k2": "key-two-secret-material"}
NOW = 1_767_225_600


@pytest.fixture
def verifier() -> JwtVerifier:
    return JwtVerifier(
        issuer=ISSUER,
        audience=AUDIENCE,
        keys=KEYS,
        allowed_algorithms=("HS256",),
        clock_skew_seconds=30,
        max_lifetime_seconds=3600,
    )


@pytest.fixture
def issuer() -> LocalTestIssuer:
    return LocalTestIssuer(
        issuer=ISSUER, audience=AUDIENCE, keys=KEYS, default_key_id="k1", environment="test"
    )


def spec(**kwargs) -> TokenSpec:
    base = {"subject": "analyst-a", "tenant_id": "tenant-a", "scopes": ()}
    base.update(kwargs)
    return TokenSpec(**base)  # type: ignore[arg-type]


class TestHappyPath:
    def test_a_correctly_signed_token_verifies(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW)
        verified = verifier.verify(token, now=NOW)
        assert verified.subject == "analyst-a"
        assert verified.tenant_id == "tenant-a"
        assert verified.issuer == ISSUER
        assert verified.audience == AUDIENCE
        assert verified.key_id == "k1"
        assert verified.algorithm == "HS256"

    def test_scope_claim_is_split_on_whitespace(self, verifier, issuer):
        token = issuer.mint(spec(scopes=("risk:read_account", "risk:read_pnl")), now=NOW)
        assert verifier.verify(token, now=NOW).scopes == ("risk:read_account", "risk:read_pnl")

    def test_a_list_audience_containing_this_api_is_accepted(self, verifier, issuer):
        token = issuer.mint(spec(audience=[AUDIENCE, "other-api"]), now=NOW)
        assert verifier.verify(token, now=NOW).audience == AUDIENCE

    def test_either_configured_key_works(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW, key_id="k2")
        assert verifier.verify(token, now=NOW).key_id == "k2"


class TestSignature:
    def test_tampered_payload_with_original_signature_is_refused(self, verifier, issuer):
        """WF06-B-02: editing claims must not survive verification."""
        forged = issuer.mint_with_tampered_payload(
            spec(), now=NOW, overrides={"sub": "analyst-b", "tenant_id": "tenant-b"}
        )
        with pytest.raises(TokenError, match="signature mismatch"):
            verifier.verify(forged, now=NOW)

    def test_token_signed_with_an_unknown_key_is_refused(self, verifier, issuer):
        with pytest.raises(TokenError, match="unknown signing key"):
            verifier.verify(issuer.mint_with_unknown_key(spec(), now=NOW), now=NOW)

    def test_empty_signature_segment_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW)
        header, payload, _ = token.split(".")
        with pytest.raises(TokenError, match="empty signature"):
            verifier.verify(f"{header}.{payload}.", now=NOW)

    def test_truncated_signature_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW)
        header, payload, signature = token.split(".")
        with pytest.raises(TokenError):
            verifier.verify(f"{header}.{payload}.{signature[:-4]}", now=NOW)


class TestAlgorithm:
    def test_unsigned_token_is_refused(self, verifier, issuer):
        """WF06-B-03: an alg-none token has no signature and gets no pass."""
        with pytest.raises(TokenError, match="empty signature"):
            verifier.verify(issuer.mint_unsigned(spec(), now=NOW), now=NOW)

    def test_alg_none_with_a_plausible_signature_is_still_refused(self, verifier, issuer):
        """An attacker can attach bytes; the algorithm is still not acceptable."""
        forged = issuer.mint_with_raw_header(
            spec(), now=NOW, header={"alg": "none", "typ": "JWT", "kid": "k1"}
        )
        with pytest.raises(TokenError, match="alg none"):
            verifier.verify(forged, now=NOW)

    def test_two_segment_token_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW)
        header, payload, _ = token.split(".")
        with pytest.raises(TokenError, match="three-segment"):
            verifier.verify(f"{header}.{payload}", now=NOW)

    def test_non_allowlisted_algorithm_is_refused(self, verifier, issuer):
        """WF06-B-08: the token header does not get to choose the algorithm."""
        forged = issuer.mint_with_raw_header(
            spec(), now=NOW, header={"alg": "HS512", "typ": "JWT", "kid": "k1"}
        )
        with pytest.raises(TokenError, match="not allowlisted"):
            verifier.verify(forged, now=NOW)

    def test_asymmetric_algorithm_claim_is_refused(self, verifier, issuer):
        forged = issuer.mint_with_raw_header(
            spec(), now=NOW, header={"alg": "RS256", "typ": "JWT", "kid": "k1"}
        )
        with pytest.raises(TokenError, match="not allowlisted"):
            verifier.verify(forged, now=NOW)

    def test_unsupported_algorithm_cannot_be_configured(self):
        with pytest.raises(ValueError):
            JwtVerifier(
                issuer=ISSUER,
                audience=AUDIENCE,
                keys=KEYS,
                allowed_algorithms=("RS256",),
            )


class TestKeyId:
    def test_kid_that_looks_like_a_url_is_refused(self, verifier, issuer):
        """A kid is an identifier, never a location to fetch keys from."""
        forged = issuer.mint_with_raw_header(
            spec(),
            now=NOW,
            header={"alg": "HS256", "typ": "JWT", "kid": "https://attacker.test/jwks.json"},
        )
        with pytest.raises(TokenError, match="kid must not look like a URL"):
            verifier.verify(forged, now=NOW)

    def test_missing_kid_is_refused(self, verifier, issuer):
        forged = issuer.mint_with_raw_header(spec(), now=NOW, header={"alg": "HS256", "typ": "JWT"})
        with pytest.raises(TokenError, match="no kid"):
            verifier.verify(forged, now=NOW)

    def test_denylisted_key_is_refused_even_with_a_valid_signature(self, verifier, issuer):
        """WF06-B-12: an emergency key invalidation drill."""
        token = issuer.mint(spec(), now=NOW)
        assert verifier.verify(token, now=NOW).key_id == "k1"
        verifier.deny_key_id("k1")
        with pytest.raises(TokenError, match="denylisted"):
            verifier.verify(token, now=NOW)

    def test_rotation_retires_the_old_key(self, verifier, issuer):
        """WF06-B-09: after the overlap window the old signature stops working."""
        old = issuer.mint(spec(), now=NOW, key_id="k1")
        new = issuer.mint(spec(), now=NOW, key_id="k2")
        verifier.rotate_keys({"k2": KEYS["k2"]})
        assert verifier.verify(new, now=NOW).key_id == "k2"
        with pytest.raises(TokenError, match="unknown signing key"):
            verifier.verify(old, now=NOW)

    def test_rotating_to_an_empty_key_set_is_refused(self, verifier):
        with pytest.raises(ValueError):
            verifier.rotate_keys({})


class TestClaims:
    def test_untrusted_issuer_is_refused(self, verifier, issuer):
        """WF06-B-04: a plausible audience does not excuse a foreign issuer."""
        forged = issuer.mint(spec(issuer="https://attacker.test/"), now=NOW)
        with pytest.raises(TokenError, match="issuer"):
            verifier.verify(forged, now=NOW)

    def test_wrong_audience_is_refused(self, verifier, issuer):
        """WF06-B-05: a token for the P04 admin API is not a credential here."""
        forged = issuer.mint(spec(audience="p04-reconciliation-admin"), now=NOW)
        with pytest.raises(TokenError, match="audience"):
            verifier.verify(forged, now=NOW)

    def test_expired_token_is_refused_beyond_the_skew(self, verifier, issuer):
        """WF06-B-06: a valid signature does not extend a lifetime."""
        token = issuer.mint(spec(expires_in=60), now=NOW)
        assert verifier.verify(token, now=NOW + 80).subject == "analyst-a"  # inside skew
        with pytest.raises(TokenError, match="expired"):
            verifier.verify(token, now=NOW + 120)

    def test_future_not_before_is_refused(self, verifier, issuer):
        """WF06-B-07: nbf ten minutes out is not yet valid."""
        token = issuer.mint(spec(not_before_offset=600, expires_in=1800), now=NOW)
        with pytest.raises(TokenError, match="not yet valid"):
            verifier.verify(token, now=NOW)
        assert verifier.verify(token, now=NOW + 600).subject == "analyst-a"

    def test_overlong_lifetime_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(expires_in=7200), now=NOW)
        with pytest.raises(TokenError, match="lifetime exceeds policy"):
            verifier.verify(token, now=NOW)

    def test_token_issued_in_the_future_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(issued_at=NOW + 600), now=NOW)
        with pytest.raises(TokenError, match="issued in the future"):
            verifier.verify(token, now=NOW)

    def test_missing_subject_is_refused(self, verifier, issuer):
        forged = issuer.mint(spec(extra_claims={"sub": ""}), now=NOW)
        with pytest.raises(TokenError, match="subject"):
            verifier.verify(forged, now=NOW)

    def test_missing_expiry_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW)
        header_segment, _, signature = token.split(".")
        claims = {"iss": ISSUER, "aud": AUDIENCE, "sub": "analyst-a"}
        payload = b64url_encode(json.dumps(claims, sort_keys=True).encode("utf-8"))
        with pytest.raises(TokenError):
            verifier.verify(f"{header_segment}.{payload}.{signature}", now=NOW)

    def test_revoked_token_id_is_refused(self, verifier, issuer):
        """WF06-B-11: a revocation applies even to an unexpired token."""
        token = issuer.mint(spec(token_id="jti-1"), now=NOW)
        assert verifier.verify(token, now=NOW).token_id == "jti-1"
        verifier.deny_token_id("jti-1")
        with pytest.raises(TokenError, match="revoked"):
            verifier.verify(token, now=NOW)


class TestMalformedInput:
    @pytest.mark.parametrize(
        "token",
        ["", "   ", "a.b", "a.b.c.d", "....", "not-a-token", "a." * 50, "ey.ey.ey"],
    )
    def test_garbage_is_refused_without_raising_anything_else(self, verifier, token: str):
        with pytest.raises(TokenError):
            verifier.verify(token, now=NOW)

    def test_padded_base64_is_refused(self, verifier):
        with pytest.raises(TokenError):
            verifier.verify("eyJ=.eyJ=.sig", now=NOW)

    def test_oversized_token_is_refused_before_parsing(self, verifier):
        with pytest.raises(TokenError, match="size"):
            verifier.verify("a" * 9000 + ".b.c", now=NOW)

    def test_non_object_payload_is_refused(self, verifier, issuer):
        token = issuer.mint(spec(), now=NOW)
        header, _, signature = token.split(".")
        payload = b64url_encode(b"[1,2,3]")
        with pytest.raises(TokenError):
            verifier.verify(f"{header}.{payload}.{signature}", now=NOW)

    def test_critical_header_parameters_are_refused(self, verifier, issuer):
        forged = issuer.mint_with_raw_header(
            spec(), now=NOW, header={"alg": "HS256", "typ": "JWT", "kid": "k1", "crit": ["exp"]}
        )
        with pytest.raises(TokenError, match="critical header"):
            verifier.verify(forged, now=NOW)


def test_issuer_refuses_to_run_outside_local_or_test():
    """The signing helper must not be constructible in a real environment."""
    with pytest.raises(RuntimeError):
        LocalTestIssuer(issuer=ISSUER, audience=AUDIENCE, keys=KEYS, environment="production")
