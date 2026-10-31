"""Project 03 call-site stand-in.

Not a reimplementation of Project 03. It exists so the outbox writer has a real
transaction to join and the kill points in design section 15 have somewhere to
fire. See :mod:`outbox.p03.sync_service` for what it does and does not model.
"""

from .sync_service import DealInsertOutcome, SyncService, new_epoch_id

__all__ = ["DealInsertOutcome", "SyncService", "new_epoch_id"]
