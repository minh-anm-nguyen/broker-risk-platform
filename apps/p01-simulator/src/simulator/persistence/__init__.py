"""Persistence layer: SQLite schema, the single writer, and snapshot reads.

The event log and the versioned projections are written in one transaction and
are therefore never observed out of step with each other (section 2.2). Reads
never open a transaction: snapshot consistency comes from the ``seq`` predicate
in the query, so holding one would only stall WAL checkpointing.
"""
