"""Pre-parse body guards: size, type, depth, duplicate keys, non-finite.

Covers WF2-D-05, WF2-D-06, WF2-D-07, WF2-D-08, WF2-D-09, WF2-D-12, WF2-M-04,
WF2-N-09 and WF2-E-10.

The point of every check here is that it runs *before* the object graph is
built. A body that is refused after parsing has already cost the allocation it
was supposed to be refused for.
"""

from __future__ import annotations

import json

import pytest

from mt5_connector.drivers.base import decode_json_body, guard_json_depth
from mt5_connector.errors import SourceContractError

pytestmark = pytest.mark.unit


def decode(body: bytes, **overrides):
    kwargs = {
        "content_type": "application/json",
        "max_bytes": 1_048_576,
        "max_depth": 64,
        "status": 200,
        "path_name": "deals",
    }
    kwargs.update(overrides)
    return decode_json_body(body, **kwargs)


class TestSizeAndType:
    def test_a_valid_body_decodes(self) -> None:
        assert decode(b'{"items":[1,2,3]}') == {"items": [1, 2, 3]}

    def test_a_body_above_the_cap_is_refused(self) -> None:
        """WF2-D-07, WF2-M-04."""
        with pytest.raises(SourceContractError) as excinfo:
            decode(b'{"a":"' + b"x" * 2_000 + b'"}', max_bytes=1_000)
        assert excinfo.value.details["max_response_bytes"] == 1_000
        assert excinfo.value.details["observed_bytes"] > 1_000

    @pytest.mark.parametrize(
        "content_type",
        ["text/html", "text/plain", "application/octet-stream", "", None, "text/html; charset=utf-8"],
    )
    def test_a_non_json_content_type_is_refused(self, content_type) -> None:
        """WF2-D-06: a proxy's HTML arrives with status 200."""
        with pytest.raises(SourceContractError) as excinfo:
            decode(b'{"items":[]}', content_type=content_type)
        assert "accepted" in excinfo.value.details

    def test_a_charset_parameter_is_tolerated(self) -> None:
        assert decode(b'{"a":1}', content_type="application/json; charset=utf-8") == {"a": 1}

    def test_problem_json_is_accepted(self) -> None:
        assert decode(b'{"error":{}}', content_type="application/problem+json") == {"error": {}}

    def test_an_empty_body_is_refused(self) -> None:
        for body in (b"", b"   ", b"\n"):
            with pytest.raises(SourceContractError) as excinfo:
                decode(body)
            assert excinfo.value.details["reason"] == "empty_body"


class TestMalformed:
    def test_a_truncated_body_is_refused(self) -> None:
        """WF2-D-05: zero DTOs from a half-written page."""
        full = json.dumps({"items": [{"deal_id": "d1"}]}).encode()
        with pytest.raises(SourceContractError) as excinfo:
            decode(full[: len(full) // 2])
        assert excinfo.value.details["reason"] == "malformed_json"
        assert "error_position" in excinfo.value.details

    def test_the_error_does_not_echo_the_payload(self) -> None:
        """INV-15: the body may be a financial record."""
        with pytest.raises(SourceContractError) as excinfo:
            decode(b'{"balance":"987654.32"')
        assert "987654" not in str(excinfo.value.details)

    def test_invalid_utf8_is_refused(self) -> None:
        """WF2-E-10: no silent replacement of bytes in a key."""
        with pytest.raises(SourceContractError) as excinfo:
            decode(b'{"a":"\xff\xfe"}')
        assert excinfo.value.details["reason"] == "encoding"


class TestDepth:
    def test_deep_nesting_is_refused_before_parsing(self) -> None:
        """WF2-D-08: json.loads has no depth limit; 10,000 levels is a crash."""
        body = (b"[" * 10_000) + (b"]" * 10_000)
        with pytest.raises(SourceContractError) as excinfo:
            decode(body, max_depth=64)
        assert excinfo.value.details["reason"] == "nesting_depth"
        assert excinfo.value.details["max_json_depth"] == 64

    def test_depth_at_the_limit_is_allowed(self) -> None:
        body = (b"[" * 10) + (b"]" * 10)
        assert decode(body, max_depth=10) == [[[[[[[[[[]]]]]]]]]]

    def test_brackets_inside_strings_are_not_counted(self) -> None:
        # A symbol or message containing a bracket must not eat the budget.
        body = b'{"symbol":"EUR[USD]{{{{{{{{{{"}'
        assert decode(body, max_depth=4)["symbol"] == "EUR[USD]{{{{{{{{{{"

    def test_an_escaped_quote_does_not_end_the_string(self) -> None:
        body = b'{"note":"a \\" [[[[[[[[[[ b"}'
        assert decode(body, max_depth=4)["note"] == 'a " [[[[[[[[[[ b'

    def test_guard_is_callable_on_its_own(self) -> None:
        guard_json_depth(b'{"a":[1]}', max_depth=4)
        with pytest.raises(SourceContractError):
            guard_json_depth(b"[[[[[]]]]]", max_depth=2)


class TestAmbiguousPayloads:
    def test_a_duplicate_object_key_is_refused(self) -> None:
        """WF2-D-09: "last one wins" is a silent choice between two documents."""
        with pytest.raises(SourceContractError) as excinfo:
            decode(b'{"run_id":"a","run_id":"b"}')
        assert excinfo.value.details["reason"] == "duplicate_key"
        assert excinfo.value.details["field"] == "run_id"

    def test_a_duplicate_key_nested_in_an_item_is_refused(self) -> None:
        with pytest.raises(SourceContractError):
            decode(b'{"items":[{"price":"1.0","price":"2.0"}]}')

    @pytest.mark.parametrize("literal", [b"NaN", b"Infinity", b"-Infinity"])
    def test_non_finite_literals_are_refused(self, literal: bytes) -> None:
        """WF2-D-09, WF2-L-02: json.loads accepts all three by default."""
        with pytest.raises(SourceContractError) as excinfo:
            decode(b'{"commission":' + literal + b"}")
        assert excinfo.value.details["reason"] == "non_finite"

    def test_the_string_nan_is_not_confused_with_the_literal(self) -> None:
        # A *string* "NaN" is still rejected, but by the money parser, not
        # here: this layer only refuses the JSON literal.
        assert decode(b'{"commission":"NaN"}') == {"commission": "NaN"}
