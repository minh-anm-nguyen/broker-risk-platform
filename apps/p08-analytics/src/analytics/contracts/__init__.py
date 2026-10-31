"""Frozen contracts with P03, P04, P05 and P07.

Nothing in this package may be relaxed to make a test pass. Each module
encodes an agreement owned by *another* project (section 2.2), so a change
here is a cross-project negotiation, and code that needs a field the contract
does not promise must report ``UNSUPPORTED`` rather than invent it.
"""

from .events import (
    ALLOWED_EVENT_TYPES,
    DEAL_RECORDED,
    DealPayload,
    EventEnvelope,
    SUPPORTED_SCHEMA_VERSIONS,
    canonical_deal_fingerprint,
    parse_envelope,
)
from .manifest import (
    BuildManifest,
    CutScope,
    ReconciliationTrust,
    SourceCutManifest,
    SourceDealIdentity,
)
from .policy import (
    AccountingPolicy,
    FeeCoverage,
    SymbolCatalog,
    SymbolSpec,
)

__all__ = [
    "ALLOWED_EVENT_TYPES",
    "DEAL_RECORDED",
    "SUPPORTED_SCHEMA_VERSIONS",
    "AccountingPolicy",
    "BuildManifest",
    "CutScope",
    "DealPayload",
    "EventEnvelope",
    "FeeCoverage",
    "ReconciliationTrust",
    "SourceCutManifest",
    "SourceDealIdentity",
    "SymbolCatalog",
    "SymbolSpec",
    "canonical_deal_fingerprint",
    "parse_envelope",
]
