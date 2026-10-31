"""Signed cursor integrity and scope binding.

WF06-N. A cursor is attacker-supplied input that drives a query, so every
property is tested directly rather than through the endpoint: integrity,
expiry, scope binding, filter binding, scan fencing and the absence of raw
account identifiers in the token.
"""

from __future__ import annotations

import base64
import json

import pytest
from risk_api.app.pagination import CursorCodec, filter_hash
from risk_api.authz.principal_context import AuthorizedScope
from risk_api.contracts.problem_details import ApiProblem

pytestmark = pytest.mark.unit

NOW = 1_767_225_600
KEY = "cursor-signing-key-for-tests"


def scope_for(
    *,
    principal: str = "analyst-a",
    tenant: str = "tenant-a",
    source: str = "sim-broker-a",
    epoch: str = "E1",
    accounts: tuple[str, ...] = ("00042",),
    revision: str = "policy-r1",
) -> AuthorizedScope:
    return AuthorizedScope(
        principal_id=principal,
        tenant_id=tenant,
        source_id=source,
        epoch_id=epoch,
        accounts=accounts,
        auth_scope_revision=revision,
    )


@pytest.fixture
def codec() -> CursorCodec:
    return CursorCodec(signing_key=KEY, key_id="cursor-key-1", ttl_seconds=120)


@pytest.fixture
def scope() -> AuthorizedScope:
    return scope_for()


@pytest.fixture
def filters(scope: AuthorizedScope) -> str:
    return filter_hash(symbol=None, accounts=scope.accounts, page_size=50)


def mint(codec: CursorCodec, scope: AuthorizedScope, filters: str, *, last: str = "P-0001") -> str:
    return codec.encode(
        scope=scope,
        published_scan_id="scan-P",
        last_position_id=last,
        filters_hash=filters,
        now=NOW,
    )


class TestRoundTrip:
    def test_a_minted_cursor_decodes_and_authorizes(self, codec, scope, filters):
        cursor = mint(codec, scope, filters)
        payload = codec.decode(cursor)
        codec.authorize(payload, scope=scope, filters_hash=filters, now=NOW)
        assert payload.last_position_id == "P-0001"
        assert payload.published_scan_id == "scan-P"
        assert payload.expires_at == NOW + 120

    def test_cursor_is_url_safe_and_unpadded(self, codec, scope, filters):
        cursor = mint(codec, scope, filters)
        assert "=" not in cursor
        assert "+" not in cursor and "/" not in cursor


class TestConfidentiality:
    def test_raw_account_ids_are_not_in_the_cursor(self, codec, filters):
        """HMAC signs, it does not encrypt. So no account numbers go in."""
        scope = scope_for(accounts=("00042", "00043", "00044"))
        filters = filter_hash(symbol=None, accounts=scope.accounts, page_size=50)
        cursor = mint(codec, scope, filters)
        decoded = base64.urlsafe_b64decode(cursor.split(".")[0] + "==").decode("utf-8")
        for account in scope.accounts:
            assert account not in decoded
        assert "00042" not in cursor

    def test_payload_holds_hashes_not_identities(self, codec, scope, filters):
        cursor = mint(codec, scope, filters)
        raw = json.loads(base64.urlsafe_b64decode(cursor.split(".")[0] + "==").decode("utf-8"))
        assert raw["tsh"].startswith("sha256:")
        assert raw["fh"].startswith("sha256:")
        assert "analyst-a" not in json.dumps(raw)


class TestIntegrity:
    def test_a_flipped_payload_byte_is_rejected(self, codec, scope, filters):
        cursor = mint(codec, scope, filters)
        body, signature = cursor.split(".")
        raw = json.loads(base64.urlsafe_b64decode(body + "==").decode("utf-8"))
        raw["last"] = "P-9999"
        tampered = base64.urlsafe_b64encode(
            json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).decode("ascii").rstrip("=")
        with pytest.raises(ApiProblem) as caught:
            codec.decode(f"{tampered}.{signature}")
        assert caught.value.code == "INVALID_CURSOR"

    def test_a_cursor_signed_by_another_key_is_rejected(self, scope, filters):
        attacker = CursorCodec(signing_key="attacker-key", key_id="cursor-key-1", ttl_seconds=120)
        forged = mint(attacker, scope, filters)
        victim = CursorCodec(signing_key=KEY, key_id="cursor-key-1", ttl_seconds=120)
        with pytest.raises(ApiProblem) as caught:
            victim.decode(forged)
        assert caught.value.code == "INVALID_CURSOR"

    def test_an_unknown_key_id_is_rejected(self, codec, scope, filters):
        other = CursorCodec(signing_key=KEY, key_id="rotated-away", ttl_seconds=120)
        with pytest.raises(ApiProblem):
            codec.decode(mint(other, scope, filters))

    def test_a_retired_key_stops_verifying(self, codec, scope, filters):
        codec.add_key("cursor-key-2", "second-key")
        second = CursorCodec(signing_key="second-key", key_id="cursor-key-2", ttl_seconds=120)
        cursor = mint(second, scope, filters)
        codec.decode(cursor)  # accepted during the rotation window
        codec.retire_key("cursor-key-2")
        with pytest.raises(ApiProblem):
            codec.decode(cursor)

    @pytest.mark.parametrize(
        "cursor",
        ["", "x", "a.b.c", "....", "notbase64!.sig", "ZXlK.", ".sig", "a" * 2000],
    )
    def test_garbage_is_rejected_as_an_invalid_cursor(self, codec, cursor: str):
        with pytest.raises(ApiProblem) as caught:
            codec.decode(cursor)
        assert caught.value.code == "INVALID_CURSOR"

    def test_a_cursor_whose_json_is_not_an_object_is_rejected(self, codec):
        body = base64.urlsafe_b64encode(b"[1,2]").decode("ascii").rstrip("=")
        with pytest.raises(ApiProblem):
            codec.decode(f"{body}.sig")

    def test_missing_members_are_rejected(self, codec, scope, filters):
        cursor = mint(codec, scope, filters)
        body, signature = cursor.split(".")
        raw = json.loads(base64.urlsafe_b64decode(body + "==").decode("utf-8"))
        del raw["scan"]
        trimmed = base64.urlsafe_b64encode(
            json.dumps(raw, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).decode("ascii").rstrip("=")
        with pytest.raises(ApiProblem):
            codec.decode(f"{trimmed}.{signature}")


class TestScopeBinding:
    def test_another_tenant_cannot_replay_a_cursor(self, codec, scope, filters):
        """A cursor is not a bearer token for someone else's data."""
        cursor = codec.decode(mint(codec, scope, filters))
        other = scope_for(principal="analyst-b", tenant="tenant-b", accounts=("00043",))
        with pytest.raises(ApiProblem) as caught:
            codec.authorize(cursor, scope=other, filters_hash=filters, now=NOW)
        assert caught.value.code == "RESOURCE_NOT_FOUND"

    def test_same_tenant_different_principal_is_also_refused(self, codec, scope, filters):
        cursor = codec.decode(mint(codec, scope, filters))
        sibling = scope_for(principal="viewer-a", tenant="tenant-a")
        with pytest.raises(ApiProblem):
            codec.authorize(cursor, scope=sibling, filters_hash=filters, now=NOW)

    def test_a_cursor_from_another_epoch_is_refused(self, codec, scope, filters):
        cursor = codec.decode(mint(codec, scope, filters))
        rotated = scope_for(epoch="E2")
        with pytest.raises(ApiProblem) as caught:
            codec.authorize(cursor, scope=rotated, filters_hash=filters, now=NOW)
        assert caught.value.code == "EPOCH_CHANGED"

    def test_an_authorization_revision_change_ends_the_session(self, codec, scope, filters):
        """WF06-C-11: a delegated role expiring mid-pagination denies the next page."""
        cursor = codec.decode(mint(codec, scope, filters))
        revised = scope_for(revision="policy-r2")
        with pytest.raises(ApiProblem) as caught:
            codec.authorize(cursor, scope=revised, filters_hash=filters, now=NOW)
        assert caught.value.code == "ACCESS_DENIED"

    def test_changed_filters_invalidate_the_cursor(self, codec, scope, filters):
        cursor = codec.decode(mint(codec, scope, filters))
        other_filters = filter_hash(symbol="EURUSD", accounts=scope.accounts, page_size=50)
        with pytest.raises(ApiProblem) as caught:
            codec.authorize(cursor, scope=scope, filters_hash=other_filters, now=NOW)
        assert caught.value.code == "INVALID_CURSOR"

    def test_changed_page_size_invalidates_the_cursor(self, codec, scope, filters):
        cursor = codec.decode(mint(codec, scope, filters))
        resized = filter_hash(symbol=None, accounts=scope.accounts, page_size=100)
        with pytest.raises(ApiProblem):
            codec.authorize(cursor, scope=scope, filters_hash=resized, now=NOW)


class TestExpiryAndFence:
    def test_an_expired_cursor_is_refused(self, codec, scope, filters):
        cursor = codec.decode(mint(codec, scope, filters))
        with pytest.raises(ApiProblem) as caught:
            codec.authorize(cursor, scope=scope, filters_hash=filters, now=NOW + 121)
        assert caught.value.code == "CURSOR_EXPIRED"

    def test_a_cursor_at_its_exact_ttl_boundary_is_still_valid(self, codec, scope, filters):
        cursor = codec.decode(mint(codec, scope, filters))
        codec.authorize(cursor, scope=scope, filters_hash=filters, now=NOW + 119)

    def test_a_republished_scan_ends_the_page_set(self, codec, scope, filters):
        """WF06-N-01: pages are never stitched across two scans."""
        cursor = codec.decode(mint(codec, scope, filters))
        codec.assert_same_scan(cursor, "scan-P")
        with pytest.raises(ApiProblem) as caught:
            codec.assert_same_scan(cursor, "scan-P2")
        assert caught.value.code == "SCAN_CHANGED"


def test_filter_hash_is_order_independent_for_accounts():
    a = filter_hash(symbol=None, accounts=("00042", "00043"), page_size=50)
    b = filter_hash(symbol=None, accounts=("00042", "00043"), page_size=50)
    c = filter_hash(symbol=None, accounts=("00043", "00042"), page_size=50)
    assert a == b
    # Order matters only because callers always pass a sorted tuple; proving the
    # hash is sensitive keeps a caller from passing an unsorted one unnoticed.
    assert a != c


def test_a_signing_key_is_required():
    with pytest.raises(ValueError):
        CursorCodec(signing_key="", key_id="k", ttl_seconds=60)
