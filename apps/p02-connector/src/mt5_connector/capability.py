"""Capability negotiation: what the source said, mapped to what we rely on.

The connector's :class:`~mt5_connector.ports.Capabilities` is **our** schema,
not the source's. A source publishes whatever fields it publishes, and this
module translates. Two rules govern the translation:

* **An absent field is ``False``, never ``True``.** "Unknown" and "supported"
  must not collapse into the same value. A connector that assumes a snapshot
  cursor exists because nothing said otherwise will happily report a complete,
  gap-free scan built on pages that were never from one snapshot
  (WF2-J-10, WF2-R-07).
* **A wrong contract version is fatal, not a warning.** If ``/v1/meta``
  declares a version this build was not written against, the field meanings
  are unknown and no amount of defensive parsing recovers them. The source is
  marked incompatible and nothing is read from it (WF2-A-04, WF2-A-09).

The Project 01 mapping is written out field by field below rather than copied
from a dict, because each line is a claim about what a published field means -
and that is exactly the kind of claim a contract test has to pin down.
"""

from __future__ import annotations

from typing import Any, Final

from .errors import IncompatibleSdkError, SourceContractError
from .ports import Capabilities, Entity

__all__ = [
    "SUPPORTED_SIMULATOR_CONTRACTS",
    "SIMULATOR_V1_ENTITIES",
    "map_simulator_capabilities",
    "map_fake_capabilities",
]

#: Source contract versions this build understands.
SUPPORTED_SIMULATOR_CONTRACTS: Final[frozenset[str]] = frozenset({"v1"})

#: The entity set belonging to the Project 01 ``v1`` data plane.
#:
#: Derived from the *contract version*, not probed. ``/v1/meta`` does not
#: enumerate entities, so the alternative to a version-keyed constant is
#: issuing a request per entity to see which ones answer - which is
#: discovering the contract in production rather than negotiating against it.
#: If the version matches, these three exist; if it does not, nothing is read.
SIMULATOR_V1_ENTITIES: Final[frozenset[Entity]] = frozenset(
    {Entity.ACCOUNTS, Entity.POSITIONS, Entity.DEALS}
)


def _require_dict(value: Any, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise SourceContractError(
            f"{field} is not a JSON object",
            details={"field": field, "observed_type": type(value).__name__},
        )
    return value


def _flag(payload: dict[str, Any], name: str) -> bool:
    """Read a boolean capability. Absent or non-boolean means ``False``.

    A truthy non-boolean - the string ``"false"``, say - is *not* read as
    ``True``. Coercing it would mean trusting a capability claim the source
    did not actually make in the form the contract specifies.
    """
    value = payload.get(name)
    return value is True


def _positive_int(payload: dict[str, Any], name: str, *, fallback: int) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        return fallback
    return value


def map_simulator_capabilities(
    meta: Any, *, declared_max_page_size: int
) -> Capabilities:
    """Translate a Project 01 ``GET /v1/meta`` body.

    ``declared_max_page_size`` is the connector's own cap; the effective
    maximum is the lower of the two, so neither side can be talked into a
    larger page than it agreed to.
    """
    body = _require_dict(meta, field="meta")
    published = _require_dict(body.get("capabilities"), field="meta.capabilities")

    api_version = published.get("api_version")
    if not isinstance(api_version, str) or api_version not in SUPPORTED_SIMULATOR_CONTRACTS:
        raise IncompatibleSdkError(
            "the source declares a data-plane contract version this build was not "
            "written against; its field meanings are unknown and nothing will be read "
            "from it",
            details={
                "declared_contract_version": api_version
                if isinstance(api_version, str)
                else "<absent>",
                "supported": sorted(SUPPORTED_SIMULATOR_CONTRACTS),
            },
        )

    simulator_fields = published.get("simulator_specific_fields")
    simulator_field_names = (
        frozenset(name for name in simulator_fields if isinstance(name, str))
        if isinstance(simulator_fields, list)
        else frozenset()
    )

    # A signed, opaque cursor is what pins a snapshot across pages in this
    # contract. Without that flag there is no claim that page 2 belongs to the
    # same snapshot as page 1, so the connector will not run a multi-page scan.
    snapshot_cursor = _flag(published, "cursor_is_opaque_and_signed")

    # Incremental deal sync needs both halves: an exclusive ``after_seq``
    # filter *and* a per-deal ``source_seq`` to compare against it. Either one
    # alone is useless, and treating the pair as present because one is would
    # produce a watermark with nothing to anchor it.
    after_seq_exclusive = _flag(published, "after_seq_is_exclusive")
    incremental = after_seq_exclusive and "source_seq" in simulator_field_names

    source_max = _positive_int(published, "max_page_size", fallback=declared_max_page_size)

    return Capabilities(
        contract_version=api_version,
        source_kind="simulator",
        supported_entities=SIMULATOR_V1_ENTITIES,
        snapshot_cursor=snapshot_cursor,
        incremental_deals_after_seq=incremental,
        # Never inferred. Project 01 pins a snapshot per *scan*, and two scans
        # of two entities are two snapshots. A consumer that needs one cut
        # across accounts, positions and deals has to get it from Project 04
        # (WF2-J-06, WF2-Q-05).
        stable_snapshot_across_entities=_flag(
            published, "stable_snapshot_across_entities"
        ),
        after_seq_is_exclusive=after_seq_exclusive,
        cursor_is_opaque=_flag(published, "cursor_is_opaque_and_signed"),
        max_page_size=min(source_max, declared_max_page_size),
        raw=published,
    )


def map_fake_capabilities(
    meta: Any, *, declared_max_page_size: int, source_kind: str = "fake"
) -> Capabilities:
    """Capabilities for the in-package fake driver.

    Shares the Project 01 mapping so a test cannot accidentally exercise a
    more generous capability set than the real contract offers.
    """
    capabilities = map_simulator_capabilities(
        meta, declared_max_page_size=declared_max_page_size
    )
    return Capabilities(
        contract_version=capabilities.contract_version,
        source_kind="fake" if source_kind == "fake" else capabilities.source_kind,
        supported_entities=capabilities.supported_entities,
        snapshot_cursor=capabilities.snapshot_cursor,
        incremental_deals_after_seq=capabilities.incremental_deals_after_seq,
        stable_snapshot_across_entities=capabilities.stable_snapshot_across_entities,
        after_seq_is_exclusive=capabilities.after_seq_is_exclusive,
        cursor_is_opaque=capabilities.cursor_is_opaque,
        max_page_size=capabilities.max_page_size,
        raw=capabilities.raw,
    )
