"""The read-only analytics query service. Section 8.

Three rules the whole package exists to enforce:

* A query's scope comes from the authenticated context; parameters may narrow
  it and never widen it (INV08-17).
* Every response names the ``build_id`` it was served from, and every row in
  one response comes from that single build (P08-T-02, P08-T-03).
* A cursor is bound to one build and one filter, so pagination cannot drift
  across a pointer flip (P08-T-08).
"""

from .auth import AuthContext, Authenticator, Scope
from .cursors import CursorCodec, CursorPayload
from .service import AnalyticsQueryService, DailyReport, DealPage

__all__ = [
    "AnalyticsQueryService",
    "AuthContext",
    "Authenticator",
    "CursorCodec",
    "CursorPayload",
    "DailyReport",
    "DealPage",
    "Scope",
]
