"""Independent oracle (P09 tickets T09-07 and T09-08).

The rule this package exists to obey: **never use the system under test as its
own oracle**. Nothing here imports `simulator`, `mt5_connector`,
`trading_sync`, `reconciliation`, `risk`, `risk_engine`, `risk_api`, `outbox`
or `analytics`. It re-derives what the answer should be from the source and
compares, with its own `Decimal` arithmetic and its own set algebra.

Why that matters more than it sounds: every component in this platform passes
its own suite, and three of the eight seams between them turned out to be
stand-ins anyway (contract gaps CG-08, CG-12, CG-13). An oracle that shared a
library with the thing it checks would have agreed with all three.

Two checks, both deliberately set-based rather than count-based:

* :mod:`identity` compares *which* records exist, not how many. `pg_count ==
  source_count` is the check that passes while both sides are missing the same
  deal, and P09 section 2.3 names it as insufficient.
* :mod:`money` recomputes totals from the source payloads and compares them to
  what was persisted. Equal as `Decimal`, not as text: `"0.00"` and `"0E-12"`
  are the same amount and different strings, and a comparison that confused
  those two would report a failure that is not there.
"""

from .identity import IdentityVerdict, compare_identity_sets
from .money import MoneyVerdict, compare_money

__all__ = [
    "IdentityVerdict",
    "MoneyVerdict",
    "compare_identity_sets",
    "compare_money",
]
