"""Capability negotiation: absent means unknown means false.

Covers WF2-A-04, WF2-A-09, WF2-J-06, WF2-J-10, WF2-Q-05, WF2-R-07 and
WF2-K-12.
"""

from __future__ import annotations

import pytest

from mt5_connector.capability import SIMULATOR_V1_ENTITIES, map_simulator_capabilities
from mt5_connector.errors import IncompatibleSdkError, SourceContractError
from mt5_connector.ports import Entity

pytestmark = pytest.mark.unit


def meta(**capability_overrides) -> dict:
    capabilities = {
        "api_version": "v1",
        "default_page_size": 100,
        "max_page_size": 500,
        "snapshot_ttl_seconds": 300,
        "after_seq_is_exclusive": True,
        "cursor_is_opaque_and_signed": True,
        "simulator_specific_fields": ["run_id", "generation", "snapshot_seq", "source_seq"],
        "wire_compatible_with_mt5_manager_api": False,
    }
    capabilities.update(capability_overrides)
    return {"run_id": "run_0001", "generation": 7, "capabilities": capabilities}


class TestVersionGate:
    def test_v1_is_accepted(self) -> None:
        mapped = map_simulator_capabilities(meta(), declared_max_page_size=500)
        assert mapped.contract_version == "v1"
        assert mapped.supported_entities == SIMULATOR_V1_ENTITIES

    @pytest.mark.parametrize("version", ["v2", "V1", "1", "", None, 1, True])
    def test_an_unknown_contract_version_is_fatal(self, version) -> None:
        """WF2-A-04: unknown field meanings cannot be parsed defensively."""
        with pytest.raises(IncompatibleSdkError) as excinfo:
            map_simulator_capabilities(
                meta(api_version=version), declared_max_page_size=500
            )
        assert excinfo.value.details["supported"] == ["v1"]
        assert not excinfo.value.retryable

    def test_a_missing_capabilities_object_is_a_contract_error(self) -> None:
        with pytest.raises(SourceContractError):
            map_simulator_capabilities({"run_id": "r"}, declared_max_page_size=500)

    def test_a_non_object_meta_is_a_contract_error(self) -> None:
        for payload in ("meta", [], 7, None):
            with pytest.raises(SourceContractError):
                map_simulator_capabilities(payload, declared_max_page_size=500)


class TestConservativeDefaults:
    def test_an_absent_flag_is_false_not_true(self) -> None:
        """WF2-J-10, WF2-R-07: unknown must never collapse into supported."""
        mapped = map_simulator_capabilities(
            {"capabilities": {"api_version": "v1"}}, declared_max_page_size=500
        )
        assert mapped.snapshot_cursor is False
        assert mapped.incremental_deals_after_seq is False
        assert mapped.stable_snapshot_across_entities is False
        assert mapped.after_seq_is_exclusive is False

    @pytest.mark.parametrize("value", ["true", 1, "yes", [], {}, "false"])
    def test_a_truthy_non_boolean_is_not_read_as_true(self, value) -> None:
        # Trusting "false" or 1 would be trusting a claim the source did not
        # make in the form the contract specifies.
        mapped = map_simulator_capabilities(
            meta(cursor_is_opaque_and_signed=value), declared_max_page_size=500
        )
        assert mapped.snapshot_cursor is False

    def test_cross_entity_atomicity_is_never_inferred(self) -> None:
        """WF2-J-06, WF2-Q-05: Project 01 pins a snapshot per scan, not globally."""
        mapped = map_simulator_capabilities(meta(), declared_max_page_size=500)
        assert mapped.stable_snapshot_across_entities is False

    def test_cross_entity_atomicity_is_honoured_when_actually_declared(self) -> None:
        mapped = map_simulator_capabilities(
            meta(stable_snapshot_across_entities=True), declared_max_page_size=500
        )
        assert mapped.stable_snapshot_across_entities is True


class TestIncrementalSync:
    def test_both_halves_are_required(self) -> None:
        """An exclusive filter with nothing to anchor it is useless."""
        assert (
            map_simulator_capabilities(
                meta(), declared_max_page_size=500
            ).incremental_deals_after_seq
            is True
        )
        # The filter is declared but no per-deal sequence is published.
        assert (
            map_simulator_capabilities(
                meta(simulator_specific_fields=["run_id", "generation", "snapshot_seq"]),
                declared_max_page_size=500,
            ).incremental_deals_after_seq
            is False
        )
        # The sequence is published but the filter is not declared exclusive.
        assert (
            map_simulator_capabilities(
                meta(after_seq_is_exclusive=False), declared_max_page_size=500
            ).incremental_deals_after_seq
            is False
        )

    def test_a_malformed_field_list_does_not_grant_the_capability(self) -> None:
        """WF2-K-12."""
        for value in ("source_seq", {"source_seq": True}, None, 7):
            mapped = map_simulator_capabilities(
                meta(simulator_specific_fields=value), declared_max_page_size=500
            )
            assert mapped.incremental_deals_after_seq is False


class TestPageSize:
    def test_the_effective_cap_is_the_lower_of_the_two(self) -> None:
        assert (
            map_simulator_capabilities(
                meta(max_page_size=500), declared_max_page_size=100
            ).max_page_size
            == 100
        )
        assert (
            map_simulator_capabilities(
                meta(max_page_size=50), declared_max_page_size=500
            ).max_page_size
            == 50
        )

    @pytest.mark.parametrize("value", [0, -1, "500", True, None])
    def test_a_malformed_page_size_falls_back_to_our_own_cap(self, value) -> None:
        mapped = map_simulator_capabilities(
            meta(max_page_size=value), declared_max_page_size=200
        )
        assert mapped.max_page_size == 200


class TestSupportsHelper:
    def test_supports_reflects_the_negotiated_entity_set(self) -> None:
        mapped = map_simulator_capabilities(meta(), declared_max_page_size=500)
        assert all(mapped.supports(entity) for entity in Entity)

    def test_raw_is_kept_for_evidence_but_not_branched_on(self) -> None:
        mapped = map_simulator_capabilities(meta(), declared_max_page_size=500)
        assert mapped.raw["api_version"] == "v1"
        assert mapped.raw["wire_compatible_with_mt5_manager_api"] is False
