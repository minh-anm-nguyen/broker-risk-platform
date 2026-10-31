"""The shared contract, tested in isolation.

The point of extracting this package was that a producer should be able to
build an event without importing the relay, and a stranger's consumer should be
able to validate one without importing either. These tests assert that
property rather than assuming it: if anything here grows a dependency on
``outbox``, ``trading_sync`` or ``analytics``, the import check fails.
"""

from __future__ import annotations

import sys

import pytest

import platform_events as pe

pytestmark = pytest.mark.unit

_COMPONENT_PACKAGES = ("outbox", "trading_sync", "analytics", "risk", "risk_api", "simulator")


def test_the_contract_imports_without_any_component_package() -> None:
    """The whole reason this package exists.

    ``platform_events`` is imported at module scope above. If importing it drags
    in a component, the extraction has failed and the dependency direction is
    back where it started.
    """
    leaked = [name for name in _COMPONENT_PACKAGES if name in sys.modules]
    assert leaked == [], f"importing platform_events pulled in {leaked}"


def test_no_module_in_the_package_imports_a_component() -> None:
    """Checked by reading the source, so a lazily imported component is caught
    too - an import inside a function would not show up in sys.modules."""
    import pathlib

    root = pathlib.Path(pe.__file__).parent
    offenders = []
    for path in sorted(root.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for name in _COMPONENT_PACKAGES:
            if f"import {name}" in text:
                offenders.append((path.name, name))
    assert offenders == [], offenders


def test_the_declared_schema_version_matches_what_the_registry_serves() -> None:
    """A producer stamps ``EVENT_SCHEMA_VERSION``; the registry is what
    validates. If they disagree, every event fails validation at runtime for a
    reason no error message would explain."""
    for event_type in pe.EventType:
        spec = pe.get_spec(event_type, pe.EVENT_SCHEMA_VERSION)
        assert spec.schema_version == pe.EVENT_SCHEMA_VERSION


def test_every_event_type_has_a_topic() -> None:
    """An event with no route cannot be published, and the failure would appear
    at relay time rather than at build time."""
    for event_type in pe.EventType:
        assert pe.topic_for(event_type).name


def test_consumable_topics_name_real_topics_and_exclude_the_poison_lane() -> None:
    """``ALL_TOPICS`` holds specs, ``CONSUMABLE_TOPICS`` holds names, so the two
    are compared by name.

    The property that matters is the exclusion: ``broker.poison.v1`` carries
    records that already failed, and a consumer that treated it as ordinary
    input would reprocess poison in a loop.
    """
    all_names = {spec.name for spec in pe.ALL_TOPICS}
    consumable = set(pe.CONSUMABLE_TOPICS)

    assert consumable <= all_names
    assert "broker.poison.v1" in all_names
    assert "broker.poison.v1" not in consumable


def test_the_public_surface_is_declared_and_complete() -> None:
    """``__all__`` is the contract's advertised surface. A name in it that does
    not exist breaks ``from platform_events import *`` for every consumer."""
    missing = [name for name in pe.__all__ if not hasattr(pe, name)]
    assert missing == []


def test_identity_derivation_is_stable_across_processes() -> None:
    """Event ids are derived, not generated, so two processes handed the same
    business fact must agree. A random uuid4 here would make the outbox unique
    index useless as a duplicate guard.
    """
    epoch = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
    key = pe.build_dedupe_key(pe.EventType.DEAL_RECORDED, epoch, "D-1")
    assert pe.derive_event_id("sim_local", key) == pe.derive_event_id("sim_local", key)
    # A different source with the same business key is a different event.
    assert pe.derive_event_id("other", key) != pe.derive_event_id("sim_local", key)
