"""The Project 01 contract double used across the suite.

Written against the published Project 01 ``v1`` contract. It is a **double**,
not that service: evidence collected here is evidence about the connector's
behaviour against the contract, and the scorecard says so rather than
implying the two services have been run together (section 14.1).
"""

from __future__ import annotations

from .app import ADMIN_TOKEN, DATA_TOKEN, FakeSimulator, create_fake_simulator
from .dataset import Dataset, DatasetSpec, build_dataset
from .server import FakeSimulatorServer, free_port

__all__ = [
    "ADMIN_TOKEN",
    "DATA_TOKEN",
    "FakeSimulator",
    "create_fake_simulator",
    "Dataset",
    "DatasetSpec",
    "build_dataset",
    "FakeSimulatorServer",
    "free_port",
]
