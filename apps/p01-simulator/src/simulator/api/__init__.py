"""HTTP surface: the read-only data plane, the admin control plane, and health.

Every financial value crosses this boundary as a JSON **string** (ADR-005), and
every list response carries ``run_id``, ``generation`` and ``snapshot_seq`` so a
consumer can detect that it is talking to a different run than it started with.
"""
