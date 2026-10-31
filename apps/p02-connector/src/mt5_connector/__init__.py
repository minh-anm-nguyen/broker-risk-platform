"""Project 02 - MT5 Integration Connector.

An async read port (:mod:`mt5_connector.ports`) over a **blocking** broker
driver. The blocking call never runs on the event loop: it runs in a dedicated
worker that owns the session, and the async side holds nothing but a waiter.

Three properties the rest of the package exists to protect:

* A caller's deadline expiring does **not** cancel the native call and does
  **not** free the session slot (:mod:`mt5_connector.actor`).
* A result that arrives after its session was replaced is discarded, never
  delivered (:mod:`mt5_connector.supervisor` epoch fencing).
* A page is emitted only when its provenance matches the scan that asked for
  it (:mod:`mt5_connector.validation`, :mod:`mt5_connector.scan`).

This connector is read-only and owns no durable checkpoint. It reports a
*checkpoint candidate*; committing one belongs to Project 03 (ADR-207).
"""

from __future__ import annotations

__version__ = "1.0.0"

#: Version of the internal handoff contract (ReadPlan / Page / SourceIdentity /
#: typed errors) consumed by Project 03. Bumped on any breaking change.
CONTRACT_VERSION = "connector-v1"

#: The Project 01 data-plane contract this build negotiates against.
SUPPORTED_SOURCE_API_VERSION = "v1"
