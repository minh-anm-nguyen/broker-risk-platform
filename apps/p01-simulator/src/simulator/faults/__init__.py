"""Deliberate fault injection.

The hard rule for this package: a response-layer fault mutates a *copy* of the
outgoing payload and never the canonical store (ADR-006, INV-12). If a fault
could change stored truth, the simulator would stop being a usable oracle for
the downstream system it is meant to test.
"""
