"""Deterministic scenario generation: virtual clock, PRNG, generator, coordinator.

Determinism here is a function of ``seed + scenario spec + generator version``
and nothing else. In particular it does not depend on wall-clock time, thread
scheduling, or how many read requests arrive while generation runs (WF-009).
"""
