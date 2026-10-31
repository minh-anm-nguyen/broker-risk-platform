"""Project 03 - Trading Data Synchronization.

Ingests MT5 account, position and deal data from the Project 02 connector into
PostgreSQL. The service owns every durable decision: the connector never writes
a checkpoint, and a successful HTTP response from it is not evidence that
anything was persisted (INV-16).

Layering, outermost last:

``domain``
    Pure value validation -- identifiers, money, timestamps, canonical hashes,
    page and provenance rules. No I/O, no database, no clock.
``application``
    Scan orchestration -- epoch guard, lease lifecycle, deal replay, snapshot
    staging and publication. Talks only to the ports in ``application.ports``.
``infrastructure``
    PostgreSQL sink and HTTP source adapter, metrics, audit, quarantine.
``status``
    Read-only health and freshness surface.
"""

from __future__ import annotations

__all__ = ["__version__"]

__version__ = "0.1.0"
