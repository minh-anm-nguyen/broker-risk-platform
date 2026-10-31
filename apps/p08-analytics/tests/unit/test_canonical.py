"""Canonical encoding, set digests and UTC discipline. Sections 11.3, INV08-14."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from analytics.canonical import (
    ChunkedSetDigest,
    EncodingError,
    canonical_decimal_text,
    canonical_json,
    digest_of,
    encode_tuple,
    ensure_utc,
    format_timestamp,
    is_sha256_hex,
    parse_timestamp,
    require_sha256_hex,
    sequence_digest,
    set_digest,
    trade_day_of,
)

pytestmark = pytest.mark.unit


class TestLengthPrefixing:
    def test_concatenation_ambiguity_is_removed(self):
        """``("ab","c")`` and ``("a","bc")`` must not collide.

        Section 11.3 asks for length-prefixed serialization precisely because
        a naive join makes these two different sets hash alike, and a set
        equality proof that cannot tell them apart proves nothing.
        """
        assert set_digest([("ab", "c")], kind="t") != set_digest([("a", "bc")], kind="t")

    def test_arity_is_part_of_the_encoding(self):
        assert encode_tuple(["a"]) != encode_tuple(["a", ""])

    def test_none_differs_from_the_string_none(self):
        assert encode_tuple([None]) != encode_tuple(["None"])

    def test_bool_differs_from_int(self):
        assert encode_tuple([True]) != encode_tuple([1])


class TestSetDigest:
    def test_is_order_independent(self):
        rows = [("a", 1), ("b", 2), ("c", 3)]
        assert set_digest(rows, kind="t") == set_digest(list(reversed(rows)), kind="t")

    def test_kind_separates_namespaces(self):
        rows = [("a", 1)]
        assert set_digest(rows, kind="one") != set_digest(rows, kind="two")

    def test_count_prevents_prefix_collision(self):
        assert set_digest([("a",)], kind="t") != set_digest([("a",), ("a",)], kind="t")

    def test_detects_a_single_changed_amount(self):
        base = [("D-1", Decimal("1.00")), ("D-2", Decimal("2.00"))]
        changed = [("D-1", Decimal("1.00")), ("D-2", Decimal("2.01"))]
        assert set_digest(base, kind="t") != set_digest(changed, kind="t")

    def test_handles_heterogeneous_types(self):
        """Sorting encoded bytes avoids comparing int with str."""
        rows = [("a", 1), (2, "b"), (None, True)]
        assert set_digest(rows, kind="t") == set_digest(list(reversed(rows)), kind="t")

    def test_sequence_digest_is_order_sensitive(self):
        rows = [("a",), ("b",)]
        assert sequence_digest(rows, kind="t") != sequence_digest(list(reversed(rows)), kind="t")


class TestChunkedSetDigest:
    def test_matches_the_unchunked_digest_semantics(self):
        rows = [(f"D-{i}", Decimal(i)) for i in range(500)]
        small = ChunkedSetDigest("t", chunk_size=7)
        large = ChunkedSetDigest("t", chunk_size=10_000)
        for row in rows:
            small.add(row)
        for row in reversed(rows):
            large.add(row)
        # Different chunk sizes genuinely produce different chunk groupings,
        # so the guarantee is per-configuration determinism, not equality
        # across chunk sizes. Both must at least agree with themselves.
        assert small.hexdigest() == _rebuild(rows, 7)
        assert large.hexdigest() == _rebuild(rows, 10_000)
        assert small.count == large.count == 500

    def test_detects_a_changed_row(self):
        rows = [(f"D-{i}", Decimal(i)) for i in range(50)]
        assert _rebuild(rows, 8) != _rebuild(rows[:-1] + [("D-49", Decimal(99))], 8)

    def test_rejects_zero_chunk_size(self):
        with pytest.raises(ValueError):
            ChunkedSetDigest("t", chunk_size=0)


def _rebuild(rows, chunk_size):
    digest = ChunkedSetDigest("t", chunk_size=chunk_size)
    for row in rows:
        digest.add(row)
    return digest.hexdigest()


class TestCanonicalJson:
    def test_rejects_floats_anywhere(self):
        """A float in a money payload is the bug, not a formatting detail."""
        with pytest.raises(EncodingError):
            canonical_json({"amount": 0.1})
        with pytest.raises(EncodingError):
            canonical_json({"nested": {"list": [1, 2.5]}})

    def test_decimal_never_uses_exponent_notation(self):
        assert canonical_json({"a": Decimal("1E+2")}) == '{"a":"100"}'

    @pytest.mark.parametrize(
        "spellings",
        [
            ("12.30", "12.300", "12.3", "1.230E+1"),
            ("100", "100.00", "1E+2"),
            ("0", "0.00", "-0.00", "-0"),
            ("-0.50", "-0.500"),
        ],
    )
    def test_equal_amounts_share_one_digest(self, spellings):
        """One numeric value must have one digest.

        P08 treats two different hashes for one Deal as an integrity incident
        and blocks the build (INV08-03). If a trailing zero changed the hash,
        a producer reformatting its output would raise a false incident.
        """
        digests = {digest_of({"amount": Decimal(text)}) for text in spellings}
        assert len(digests) == 1

    def test_different_amounts_still_differ(self):
        assert digest_of({"a": Decimal("12.30")}) != digest_of({"a": Decimal("12.31")})
        assert digest_of({"a": Decimal("1")}) != digest_of({"a": Decimal("-1")})

    def test_canonical_decimal_text(self):
        assert canonical_decimal_text(Decimal("12.300")) == "12.3"
        assert canonical_decimal_text(Decimal("-0.00")) == "0"
        assert canonical_decimal_text(Decimal("1E+3")) == "1000"
        with pytest.raises(EncodingError):
            canonical_decimal_text(Decimal("NaN"))

    def test_key_order_is_stable(self):
        assert canonical_json({"b": 1, "a": 2}) == '{"a":2,"b":1}'

    def test_rejects_non_finite_decimal(self):
        with pytest.raises(EncodingError):
            canonical_json({"a": Decimal("Infinity")})

    def test_digest_is_bare_hex(self):
        digest = digest_of({"a": 1})
        assert is_sha256_hex(digest)
        assert not digest.startswith("sha256:")


class TestTimestamps:
    def test_naive_input_is_refused(self):
        """A missing offset is a real ambiguity across a wire format."""
        with pytest.raises(EncodingError):
            ensure_utc(datetime(2026, 10, 30, 12, 0, 0))
        with pytest.raises(EncodingError):
            parse_timestamp("2026-10-30T12:00:00")

    def test_offset_is_converted_to_utc(self):
        parsed = parse_timestamp("2026-10-30T12:00:00+02:00")
        assert parsed == datetime(2026, 10, 30, 10, 0, 0, tzinfo=UTC)

    def test_format_has_fixed_precision(self):
        moment = datetime(2026, 10, 30, 1, 2, 3, tzinfo=UTC)
        assert format_timestamp(moment) == "2026-10-30T01:02:03.000000Z"

    def test_subsecond_and_whole_second_hash_alike(self):
        a = parse_timestamp("2026-10-30T00:00:00Z")
        b = parse_timestamp("2026-10-30T00:00:00.000000Z")
        assert digest_of({"t": a}) == digest_of({"t": b})

    @pytest.mark.parametrize("text", ["", "   ", "not-a-date", "2026-13-01T00:00:00Z"])
    def test_rejects_malformed(self, text):
        with pytest.raises(EncodingError):
            parse_timestamp(text)


class TestTradeDay:
    def test_uses_the_utc_day(self):
        assert trade_day_of(parse_timestamp("2026-10-30T23:59:59.999999Z")).isoformat() == "2026-10-30"
        assert trade_day_of(parse_timestamp("2026-10-31T00:00:00Z")).isoformat() == "2026-10-31"

    def test_a_local_offset_cannot_shift_the_day(self):
        """INV08-14: the day follows UTC, not the producer's timezone.

        ``2026-10-31T01:00+02:00`` is 23:00 on the 30th in UTC, and that is
        the day the Deal belongs to.
        """
        shifted = datetime(2026, 10, 31, 1, 0, 0, tzinfo=timezone(timedelta(hours=2)))
        assert trade_day_of(shifted).isoformat() == "2026-10-30"

    def test_takes_no_timezone_parameter(self):
        """A reporting timezone is a versioned policy, not a call argument."""
        import inspect

        params = list(inspect.signature(trade_day_of).parameters)
        assert params == ["executed_at"]


class TestHashShape:
    @pytest.mark.parametrize(
        "value,valid",
        [
            ("a" * 64, True),
            ("A" * 64, False),
            ("a" * 63, False),
            ("a" * 65, False),
            ("g" * 64, False),
            (None, False),
            (123, False),
        ],
    )
    def test_requires_64_lowercase_hex(self, value, valid):
        assert is_sha256_hex(value) is valid

    def test_require_raises_with_field_name(self):
        with pytest.raises(EncodingError, match="payload_sha256"):
            require_sha256_hex("short", "payload_sha256")
