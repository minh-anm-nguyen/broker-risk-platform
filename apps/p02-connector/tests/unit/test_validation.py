"""Status mapping, envelope shape, provenance fencing and deal ordering.

Covers WF2-D-01, WF2-D-04 .. WF2-D-11, WF2-I-06, WF2-I-07, WF2-I-09,
WF2-J-02, WF2-J-03, WF2-K-01, WF2-K-07, WF2-K-08, WF2-L-06, WF2-Q-06,
INV-06, INV-10, INV-18 and INV-19.
"""

from __future__ import annotations

import pytest

from mt5_connector.capability import map_simulator_capabilities
from mt5_connector.drivers.base import DriverResponse
from mt5_connector.errors import (
    AuthenticationError,
    DataIntegrityError,
    PermissionDeniedError,
    RateLimitedError,
    SnapshotExpiredError,
    SourceContractError,
    SourceEpochChangedError,
    UpstreamTimeoutError,
    UpstreamUnavailableError,
)
from mt5_connector.ports import Entity
from mt5_connector.validation import (
    build_items,
    check_deal_ordering,
    check_provenance,
    filter_digest,
    map_error_response,
    parse_envelope,
)

pytestmark = pytest.mark.unit

CONTEXT = {"source_id": "broker-sim-01", "operation": "fetch_deals", "request_id": "req-1"}


def capabilities(*, incremental: bool = True, cursor: bool = True):
    return map_simulator_capabilities(
        {
            "capabilities": {
                "api_version": "v1",
                "max_page_size": 500,
                "after_seq_is_exclusive": incremental,
                "cursor_is_opaque_and_signed": cursor,
                "simulator_specific_fields": ["source_seq"] if incremental else [],
            }
        },
        declared_max_page_size=500,
    )


def response(payload, *, status: int = 200, retry_after: str | None = None) -> DriverResponse:
    return DriverResponse(
        status=status,
        payload=payload,
        byte_count=256,
        content_type="application/json",
        retry_after=retry_after,
        path_name="deals",
    )


def envelope(items=None, *, run_id="run_0001", generation=7, snapshot_seq=130, next_cursor=None):
    return {
        "run_id": run_id,
        "generation": generation,
        "snapshot_seq": snapshot_seq,
        "next_cursor": next_cursor,
        "items": items if items is not None else [],
    }


def deal(index: int, *, source_seq: int, deal_id: str | None = None) -> dict:
    return {
        "deal_id": deal_id or f"deal-{index:06d}",
        "source_seq": source_seq,
        "account_id": "acct-0001",
        "position_id": "pos-00001",
        "symbol": "EURUSD",
        "entry": "IN",
        "side": "BUY",
        "volume_lots": "0.10",
        "price": "1.08500",
        "realized_pnl_gross": "0.00",
        "commission": "-0.70",
        "swap": "0.00",
        "executed_at": "2026-10-01T00:00:00.000Z",
    }


class TestStatusMapping:
    @pytest.mark.parametrize(
        ("status", "code", "expected"),
        [
            (401, "UNAUTHORIZED", AuthenticationError),
            (403, "FORBIDDEN", PermissionDeniedError),
            (404, "NOT_FOUND", SourceContractError),
            (400, "INVALID_CURSOR", SourceContractError),
            (409, "RUN_CONFLICT", SourceContractError),
            (413, "PAYLOAD_TOO_LARGE", SourceContractError),
            (422, "VALIDATION_FAILED", SourceContractError),
            (429, "RATE_LIMITED", RateLimitedError),
            (500, "INTERNAL_ERROR", UpstreamUnavailableError),
            (502, "SERVICE_NOT_READY", UpstreamUnavailableError),
            (503, "STORAGE_ERROR", UpstreamUnavailableError),
            (504, "SIMULATED_UPSTREAM_TIMEOUT", UpstreamTimeoutError),
        ],
    )
    def test_each_status_maps_to_its_family(self, status, code, expected) -> None:
        """INV-18: the mapping is what lets a consumer decide what to do."""
        error = map_error_response(
            response({"error": {"code": code, "message": "x"}}, status=status),
            source_id="broker-sim-01",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert isinstance(error, expected)
        assert error.upstream_status == status

    def test_the_410_pair_is_distinguished(self) -> None:
        """WF2-D-04, WF2-J-03: two very different remedies behind one status."""
        expired = map_error_response(
            response({"error": {"code": "SNAPSHOT_EXPIRED"}}, status=410),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        replaced = map_error_response(
            response({"error": {"code": "RUN_REPLACED"}}, status=410),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert isinstance(expired, SnapshotExpiredError)
        assert isinstance(replaced, SourceEpochChangedError)
        assert expired.resync_required and replaced.resync_required
        assert not expired.retryable and not replaced.retryable

    def test_retryable_flags_follow_the_family_not_the_source_claim(self) -> None:
        """A source claiming a 401 is retryable does not make it so."""
        error = map_error_response(
            response(
                {"error": {"code": "UNAUTHORIZED", "retryable": True}}, status=401
            ),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert error.retryable is False

    def test_rate_limited_carries_a_capped_retry_after(self) -> None:
        error = map_error_response(
            response(
                {"error": {"code": "RATE_LIMITED"}}, status=429, retry_after="999999"
            ),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert isinstance(error, RateLimitedError)
        assert error.retry_after_s == 2.0

    def test_rate_limited_without_a_header_leaves_retry_after_unset(self) -> None:
        """WF2-D-03: None means "use your own backoff", not zero."""
        error = map_error_response(
            response({"error": {"code": "RATE_LIMITED"}}, status=429),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert error.retry_after_s is None
        assert error.details["retry_after_header_present"] is False

    def test_204_is_a_contract_error_not_an_empty_dataset(self) -> None:
        """WF2-D-10: an empty page has an envelope; 204 does not."""
        error = map_error_response(
            response(None, status=204),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert isinstance(error, SourceContractError)
        assert "must not advance a checkpoint" in error.message

    def test_an_html_error_page_does_not_masquerade_as_a_source_code(self) -> None:
        """WF2-D-06."""
        error = map_error_response(
            DriverResponse(
                status=503,
                payload="<html>gateway</html>",
                byte_count=20,
                content_type="text/html",
            ),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert error.details["source_code"] == "<absent>"

    def test_an_unclassified_status_is_a_contract_error(self) -> None:
        error = map_error_response(
            response({}, status=418),
            source_id="s",
            operation="fetch_deals",
            retry_after_cap_s=2.0,
        )
        assert isinstance(error, SourceContractError)


class TestEnvelope:
    def test_a_valid_envelope_parses(self) -> None:
        facts = parse_envelope(response(envelope()), **CONTEXT)
        assert facts.run_id == "run_0001"
        assert facts.generation == 7
        assert facts.snapshot_seq == 130
        assert facts.next_cursor is None

    @pytest.mark.parametrize(
        "field", ["run_id", "generation", "snapshot_seq", "next_cursor", "items"]
    )
    def test_a_missing_envelope_field_is_a_contract_error(self, field: str) -> None:
        payload = envelope()
        del payload[field]
        with pytest.raises(SourceContractError) as excinfo:
            parse_envelope(response(payload), **CONTEXT)
        assert excinfo.value.details["field"] == field

    def test_an_absent_next_cursor_is_not_the_same_as_null(self) -> None:
        """Absence is "we do not know"; null is "no further pages"."""
        payload = envelope()
        del payload["next_cursor"]
        with pytest.raises(SourceContractError) as excinfo:
            parse_envelope(response(payload), **CONTEXT)
        assert "only null means" in excinfo.value.message

    @pytest.mark.parametrize("value", [True, "7", 7.0, -1, None])
    def test_a_malformed_generation_is_refused(self, value) -> None:
        with pytest.raises(SourceContractError):
            parse_envelope(response(envelope(generation=value)), **CONTEXT)

    def test_an_empty_cursor_string_is_refused(self) -> None:
        with pytest.raises(SourceContractError):
            parse_envelope(response(envelope(next_cursor="")), **CONTEXT)

    def test_an_overlong_cursor_is_refused(self) -> None:
        with pytest.raises(SourceContractError):
            parse_envelope(response(envelope(next_cursor="c" * 5_000)), **CONTEXT)

    def test_a_non_object_body_is_refused(self) -> None:
        """WF2-D-06."""
        for payload in ("<html>", [], 7, None):
            with pytest.raises(SourceContractError):
                parse_envelope(response(payload), **CONTEXT)

    def test_items_must_be_an_array(self) -> None:
        with pytest.raises(SourceContractError):
            parse_envelope(response(envelope(items={"0": {}})), **CONTEXT)


class TestProvenance:
    def test_the_first_page_establishes_rather_than_compares(self) -> None:
        facts = parse_envelope(response(envelope()), **CONTEXT)
        check_provenance(
            facts,
            **CONTEXT,
            expected_run_id=None,
            expected_generation=None,
            expected_snapshot_seq=None,
            page_index=0,
        )

    def test_a_changed_run_aborts_the_scan(self) -> None:
        """WF2-J-02: meta says A, pages say B."""
        facts = parse_envelope(response(envelope(run_id="run_0002")), **CONTEXT)
        with pytest.raises(SourceEpochChangedError) as excinfo:
            check_provenance(
                facts,
                **CONTEXT,
                expected_run_id="run_0001",
                expected_generation=7,
                expected_snapshot_seq=130,
                page_index=3,
            )
        assert excinfo.value.details["field"] == "run_id"
        assert excinfo.value.resync_required

    def test_a_reused_run_id_with_a_new_generation_is_still_detected(self) -> None:
        """WF2-J-03: comparing only the run would miss a reset entirely."""
        facts = parse_envelope(response(envelope(generation=8)), **CONTEXT)
        with pytest.raises(SourceEpochChangedError) as excinfo:
            check_provenance(
                facts,
                **CONTEXT,
                expected_run_id="run_0001",
                expected_generation=7,
                expected_snapshot_seq=130,
                page_index=2,
            )
        assert excinfo.value.details["field"] == "generation"

    def test_a_different_snapshot_in_the_same_generation_is_an_integrity_error(self) -> None:
        """WF2-I-06, INV-07: two valid pages from two different moments."""
        facts = parse_envelope(response(envelope(snapshot_seq=137)), **CONTEXT)
        with pytest.raises(DataIntegrityError) as excinfo:
            check_provenance(
                facts,
                **CONTEXT,
                expected_run_id="run_0001",
                expected_generation=7,
                expected_snapshot_seq=130,
                page_index=3,
            )
        assert excinfo.value.details["observed_snapshot_seq"] == 137
        assert "cannot be joined" in excinfo.value.message


class TestItems:
    def test_a_valid_page_builds_every_item(self) -> None:
        facts = parse_envelope(
            response(envelope([deal(1, source_seq=10), deal(2, source_seq=14)])), **CONTEXT
        )
        items = build_items(
            facts,
            entity=Entity.DEALS,
            capabilities=capabilities(),
            page_index=0,
            **CONTEXT,
        )
        assert len(items) == 2
        assert items[0].deal_id == "deal-000001"

    def test_one_bad_record_refuses_the_whole_page(self) -> None:
        """WF2-Q-06, WF2-D-05: no valid prefix of a bad page is emitted."""
        bad = deal(2, source_seq=14)
        bad["price"] = 1.085  # a JSON float
        facts = parse_envelope(response(envelope([deal(1, source_seq=10), bad])), **CONTEXT)
        with pytest.raises(SourceContractError) as excinfo:
            build_items(
                facts,
                entity=Entity.DEALS,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
        assert excinfo.value.details["field"] == "price"
        assert excinfo.value.details["item_index"] == 1
        assert excinfo.value.details["items_on_page"] == 2

    def test_a_duplicate_id_on_one_page_is_an_integrity_error(self) -> None:
        """WF2-L-06."""
        facts = parse_envelope(
            response(
                envelope([deal(1, source_seq=10), deal(1, source_seq=12, deal_id="deal-000001")])
            ),
            **CONTEXT,
        )
        with pytest.raises(DataIntegrityError) as excinfo:
            build_items(
                facts,
                entity=Entity.DEALS,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
        assert excinfo.value.details["item_index"] == 1

    def test_validation_details_never_include_a_field_value(self) -> None:
        """INV-15: these details are logged."""
        bad = deal(1, source_seq=10)
        bad["commission"] = "NaN"
        facts = parse_envelope(response(envelope([bad])), **CONTEXT)
        with pytest.raises(SourceContractError) as excinfo:
            build_items(
                facts,
                entity=Entity.DEALS,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
        rendered = str(excinfo.value.details)
        assert "NaN" not in rendered
        assert excinfo.value.details["field"] == "commission"


class TestDealOrdering:
    def test_a_gap_in_source_seq_is_not_an_error(self) -> None:
        """WF2-K-01, INV-19: the sequence counts events, most of which are not deals."""
        facts = parse_envelope(
            response(envelope([deal(1, source_seq=100), deal(2, source_seq=104)])), **CONTEXT
        )
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=capabilities(), page_index=0, **CONTEXT
        )
        highest = check_deal_ordering(
            items,
            facts=facts,
            requested_after_seq=None,
            previous_upper_bound=None,
            capabilities=capabilities(),
            page_index=0,
            **CONTEXT,
        )
        assert highest == 104

    def test_a_deal_at_or_below_an_exclusive_after_seq_is_refused(self) -> None:
        """WF2-K-07: the source ignored the filter; accepting it re-delivers."""
        facts = parse_envelope(response(envelope([deal(1, source_seq=100)])), **CONTEXT)
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=capabilities(), page_index=0, **CONTEXT
        )
        with pytest.raises(DataIntegrityError) as excinfo:
            check_deal_ordering(
                items,
                facts=facts,
                requested_after_seq=100,
                previous_upper_bound=None,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
        assert excinfo.value.details["requested_after_seq"] == 100

    def test_a_deal_above_the_snapshot_is_refused(self) -> None:
        """WF2-K-08: a record from after its own read."""
        facts = parse_envelope(
            response(envelope([deal(1, source_seq=999)], snapshot_seq=130)), **CONTEXT
        )
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=capabilities(), page_index=0, **CONTEXT
        )
        with pytest.raises(DataIntegrityError) as excinfo:
            check_deal_ordering(
                items,
                facts=facts,
                requested_after_seq=None,
                previous_upper_bound=None,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
        assert excinfo.value.details["snapshot_seq"] == 130

    def test_out_of_order_deals_are_refused(self) -> None:
        """WF2-I-09: a cursor scan over disordered rows cannot be complete."""
        facts = parse_envelope(
            response(envelope([deal(2, source_seq=120), deal(1, source_seq=100)])), **CONTEXT
        )
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=capabilities(), page_index=0, **CONTEXT
        )
        with pytest.raises(DataIntegrityError) as excinfo:
            check_deal_ordering(
                items,
                facts=facts,
                requested_after_seq=None,
                previous_upper_bound=None,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
        assert "ascending" in excinfo.value.message

    def test_a_tie_on_source_seq_is_broken_by_deal_id(self) -> None:
        facts = parse_envelope(
            response(
                envelope(
                    [
                        deal(1, source_seq=100, deal_id="deal-000001"),
                        deal(2, source_seq=100, deal_id="deal-000002"),
                    ]
                )
            ),
            **CONTEXT,
        )
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=capabilities(), page_index=0, **CONTEXT
        )
        assert (
            check_deal_ordering(
                items,
                facts=facts,
                requested_after_seq=None,
                previous_upper_bound=None,
                capabilities=capabilities(),
                page_index=0,
                **CONTEXT,
            )
            == 100
        )

    def test_a_page_that_repeats_the_previous_page_is_refused(self) -> None:
        """Ordering has to hold across pages, not only inside one."""
        facts = parse_envelope(
            response(envelope([deal(1, source_seq=100), deal(2, source_seq=104)])), **CONTEXT
        )
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=capabilities(), page_index=1, **CONTEXT
        )
        with pytest.raises(DataIntegrityError) as excinfo:
            check_deal_ordering(
                items,
                facts=facts,
                requested_after_seq=None,
                previous_upper_bound=200,
                capabilities=capabilities(),
                page_index=1,
                **CONTEXT,
            )
        assert excinfo.value.details["previous_upper_bound"] == 200

    def test_ordering_is_skipped_when_the_source_declares_no_sequence(self) -> None:
        """WF2-R-07: no fabricated bound for a source without sequences."""
        caps = capabilities(incremental=False)
        payload = deal(1, source_seq=100)
        del payload["source_seq"]
        facts = parse_envelope(response(envelope([payload])), **CONTEXT)
        items = build_items(
            facts, entity=Entity.DEALS, capabilities=caps, page_index=0, **CONTEXT
        )
        assert (
            check_deal_ordering(
                items,
                facts=facts,
                requested_after_seq=None,
                previous_upper_bound=None,
                capabilities=caps,
                page_index=0,
                **CONTEXT,
            )
            is None
        )


class TestFilterDigest:
    def test_the_digest_is_stable_and_order_independent(self) -> None:
        assert filter_digest({"a": 1, "b": 2}) == filter_digest({"b": 2, "a": 1})

    def test_different_filters_produce_different_digests(self) -> None:
        """WF2-K-06, INV-21: a watermark is scoped to its filters."""
        assert filter_digest({"account_id": "A"}) != filter_digest({"account_id": "B"})
        assert filter_digest({"after_seq": None}) != filter_digest({"after_seq": 0})
