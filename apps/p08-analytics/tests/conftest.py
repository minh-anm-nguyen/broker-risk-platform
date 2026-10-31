"""Pytest fixtures for the P08 suite.

The shared helpers and the independent oracle live in :mod:`helpers` so test
modules can import them by name; this file holds only fixtures.
"""

from __future__ import annotations

import pytest

from analytics.clock import ManualClock
from analytics.contracts.manifest import CutScope
from analytics.contracts.policy import default_p0_policy
from analytics.faults.killpoints import FaultPlan
from analytics.fixtures.golden import (
    DEFAULT_EPOCH,
    DEFAULT_POLICY_VERSION,
    DEFAULT_REPORT_FAMILY,
    DEFAULT_SOURCE,
    DEFAULT_TENANT,
    DEFAULT_TOPIC,
)
from analytics.main import Stack, build_lab_stack
from analytics.query.auth import AuthContext
from analytics.settings import Settings

from helpers import OPERATOR_TOKEN, READER_TOKEN


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock()


@pytest.fixture
def faults() -> FaultPlan:
    return FaultPlan()


@pytest.fixture
def settings() -> Settings:
    return Settings(
        env="test",
        tenant_map=(f"{DEFAULT_TOPIC}:{DEFAULT_SOURCE}={DEFAULT_TENANT}",),
        kafka_max_batch_records=500,
        cursor_signing_key="test-cursor-signing-key-0123456789",
    )


@pytest.fixture
def scope() -> CutScope:
    return CutScope(
        tenant_id=DEFAULT_TENANT,
        source_id=DEFAULT_SOURCE,
        epoch_id=DEFAULT_EPOCH,
        report_family=DEFAULT_REPORT_FAMILY,
        cut_id="cut-1",
        policy_version=DEFAULT_POLICY_VERSION,
    )


@pytest.fixture
def policy():
    return default_p0_policy()


@pytest.fixture
def stack(settings: Settings, clock: ManualClock, faults: FaultPlan):
    built = build_lab_stack(settings=settings, clock=clock, faults=faults)
    yield built
    built.close()


@pytest.fixture
def multi_partition_stack(settings: Settings, clock: ManualClock, faults: FaultPlan):
    """A six-partition stack for the rebalance and routing scenarios."""
    built = build_lab_stack(settings=settings, clock=clock, faults=faults, partitions=6)
    yield built
    built.close()


@pytest.fixture
def reader(stack: Stack) -> AuthContext:
    return stack.authenticator.authenticate(READER_TOKEN)


@pytest.fixture
def operator(stack: Stack) -> AuthContext:
    return stack.authenticator.authenticate(OPERATOR_TOKEN)
