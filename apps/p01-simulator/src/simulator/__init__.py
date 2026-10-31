"""Project 01 - MT5 Broker Simulator.

A deterministic, stateful synthetic broker data source used as a test double for
Project 02 (MT5 Integration Connector) and downstream mini-projects.

This package deliberately does NOT implement the real MT5 Manager API wire
protocol. The REST surface is a *simulated test contract* only; fields such as
``run_id``, ``generation`` and ``source_seq`` are simulator semantics and must
not be assumed to exist in a real MT5 Manager SDK.
"""

__version__ = "1.0.0"

# Bumping GENERATOR_VERSION is a breaking change for reproducibility: a seed is
# only guaranteed to replay to the same canonical digest within one generator
# version. Never change a generation algorithm without bumping this.
GENERATOR_VERSION = "1.0"

# Storage schema version, asserted at startup against the migrated database.
SCHEMA_VERSION = 1
