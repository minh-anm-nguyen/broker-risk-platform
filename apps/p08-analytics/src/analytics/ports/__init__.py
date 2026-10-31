"""Protocols for the three external systems P08 depends on.

The seams are here because the hard parts of this project - ACK ambiguity, a
lost partition assignment, a lease expiring mid-aggregation, a retention
truncation - are *timing* properties. Reproducing them against real brokers
means sleeping and hoping; reproducing them against an in-process model that
implements the same contract means asserting them (section 9.2).

The deterministic adapters are therefore not mocks of P08's own code. They
model Kafka, ClickHouse and the control plane, and the production adapters
satisfy the same protocols, so a test that passes proves something about the
logic under test rather than about a stub returning what it was told to.
"""

from .clickhouse import ClickHousePort, InsertResult, RawObservation
from .control import ControlPlanePort, BuildRecord, ClaimRecord, PublicationRecord, QuarantineRecord
from .kafka import ConsumerPort, ConsumerRecord, PartitionKey, PolledBatch

__all__ = [
    "BuildRecord",
    "ClaimRecord",
    "ClickHousePort",
    "ConsumerPort",
    "ConsumerRecord",
    "ControlPlanePort",
    "InsertResult",
    "PartitionKey",
    "PolledBatch",
    "PublicationRecord",
    "QuarantineRecord",
    "RawObservation",
]
