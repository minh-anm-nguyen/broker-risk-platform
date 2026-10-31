"""Publication resolution, rollback and garbage collection.

The reader never names a build. It resolves the pointer, binds the returned
``build_id`` for the whole request, and queries only that build (INV08-10,
P08-T-02). GC, correspondingly, must never remove a build the pointer or an
open cursor still references (INV08-19, P08-T-08).
"""

from .gc import GarbageCollector, GcPlan
from .registry import PublicationRegistry, ResolvedPublication

__all__ = ["GarbageCollector", "GcPlan", "PublicationRegistry", "ResolvedPublication"]
