"""Drivers: the blocking edge of the connector.

Everything in this package runs on a worker thread or in a worker process,
never on the event loop. A driver is written as ordinary **synchronous** code
on purpose - that is what makes the isolation claim testable. An async HTTP
client would keep the event loop responsive for free and would therefore
prove nothing about whether a blocking SDK call can be contained
(design section 3.2).
"""

from __future__ import annotations

from .base import (
    BlockingDriverProtocol,
    DriverIdentity,
    DriverResponse,
    decode_json_body,
    guard_json_depth,
)

__all__ = [
    "BlockingDriverProtocol",
    "DriverIdentity",
    "DriverResponse",
    "decode_json_body",
    "guard_json_depth",
]
