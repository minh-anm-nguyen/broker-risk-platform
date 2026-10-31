"""Length-framed JSON IPC between the gateway and a worker process.

Framing is explicit - an 8-byte big-endian length followed by that many bytes
of UTF-8 JSON - because the failure this protocol has to survive is a worker
that dies **mid-write**. A newline-delimited stream cannot tell a truncated
record from a complete one, and a partially written JSON object that happens
to parse is the worst possible outcome: a DTO assembled from half a message
(WF2-F-11, WF2-O-07).

Two more properties worth stating:

* **Every frame carries the worker epoch.** The gateway rejects a frame whose
  epoch is not the current one, so a message still in the pipe buffer when a
  worker is replaced cannot be answered as if it belonged to the new one
  (WF2-R-09).
* **A frame is size-capped before it is read.** A declared length above the
  cap is refused without allocating for it, which keeps a corrupt or hostile
  length prefix from becoming an allocation of several gigabytes.
"""

from __future__ import annotations

import json
import struct
from dataclasses import dataclass
from typing import Any, BinaryIO, Final

__all__ = [
    "PROTOCOL_VERSION",
    "MAX_FRAME_BYTES",
    "FrameError",
    "Frame",
    "write_frame",
    "read_frame",
]

#: Bumped on any incompatible change. The gateway and the worker exchange it
#: during the handshake and refuse to proceed on a mismatch, rather than
#: decoding a payload whose shape they disagree about (WF2-O-07).
PROTOCOL_VERSION: Final[str] = "worker-ipc-1"

MAX_FRAME_BYTES: Final[int] = 32 * 1024 * 1024

_HEADER = struct.Struct(">Q")


class FrameError(Exception):
    """The stream did not yield a usable frame."""


@dataclass(frozen=True, slots=True)
class Frame:
    kind: str
    worker_epoch: int
    request_id: str
    payload: dict[str, Any]

    def to_obj(self) -> dict[str, Any]:
        return {
            "protocol": PROTOCOL_VERSION,
            "kind": self.kind,
            "worker_epoch": self.worker_epoch,
            "request_id": self.request_id,
            "payload": self.payload,
        }


def write_frame(stream: BinaryIO, frame: Frame) -> None:
    body = json.dumps(frame.to_obj(), separators=(",", ":"), default=str).encode("utf-8")
    if len(body) > MAX_FRAME_BYTES:
        raise FrameError(f"frame of {len(body)} bytes exceeds the {MAX_FRAME_BYTES} cap")
    stream.write(_HEADER.pack(len(body)))
    stream.write(body)
    # Flushed every time: a buffered frame is a frame the peer is waiting for
    # while both sides believe the other is busy.
    stream.flush()


def _read_exactly(stream: BinaryIO, count: int) -> bytes:
    chunks: list[bytes] = []
    remaining = count
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            # EOF part-way through a frame. This is the worker-died-mid-write
            # case, and it is an error rather than a short frame.
            raise FrameError(
                f"the stream ended after {count - remaining} of {count} bytes"
            )
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def read_frame(stream: BinaryIO) -> Frame:
    header = stream.read(_HEADER.size)
    if not header:
        raise FrameError("the stream is closed")
    if len(header) < _HEADER.size:
        raise FrameError("the stream ended inside a frame header")
    (length,) = _HEADER.unpack(header)
    if length == 0:
        raise FrameError("a frame declared zero length")
    if length > MAX_FRAME_BYTES:
        raise FrameError(f"a frame declared {length} bytes, above the {MAX_FRAME_BYTES} cap")
    body = _read_exactly(stream, length)
    try:
        obj = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameError("a frame body was not valid JSON") from exc
    if not isinstance(obj, dict):
        raise FrameError("a frame body was not an object")
    if obj.get("protocol") != PROTOCOL_VERSION:
        raise FrameError(
            f"frame protocol {obj.get('protocol')!r} does not match {PROTOCOL_VERSION!r}"
        )
    kind = obj.get("kind")
    request_id = obj.get("request_id")
    epoch = obj.get("worker_epoch")
    payload = obj.get("payload")
    if not isinstance(kind, str) or not isinstance(request_id, str):
        raise FrameError("a frame is missing kind or request_id")
    if isinstance(epoch, bool) or not isinstance(epoch, int):
        raise FrameError("a frame is missing a worker epoch")
    if not isinstance(payload, dict):
        raise FrameError("a frame payload is not an object")
    return Frame(kind=kind, worker_epoch=epoch, request_id=request_id, payload=payload)
