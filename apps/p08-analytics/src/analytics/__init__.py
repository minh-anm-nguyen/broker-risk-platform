"""Project 08 - ClickHouse Broker Analytics.

Kafka ``DealRecorded.v1`` events become raw append-only observations, raw
observations become immutable verified analytics builds, and exactly one
verified build at a time is reachable by readers through a fenced PostgreSQL
publication pointer.

The package is organised around the one asymmetry that drives the whole
design (SYSTEM_DESIGN section 0): a slow, ``STALE`` or rejected answer is
acceptable, a believable wrong monetary total is not.
"""

__version__ = "1.0.0"
