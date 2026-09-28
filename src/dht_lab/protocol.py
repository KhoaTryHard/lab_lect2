"""Length-prefixed JSON protocol used by all DHT TCP connections."""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

MAX_FRAME_SIZE = 1024 * 1024
HEADER_SIZE = 4


class ProtocolError(ValueError):
    """Raised when a peer sends an invalid or oversized frame."""


def encode_message(message: dict[str, Any]) -> bytes:
    """Encode one JSON object as a 4-byte big-endian length-prefixed frame."""

    if not isinstance(message, dict):
        raise ProtocolError("message must be an object")
    payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    if len(payload) > MAX_FRAME_SIZE:
        raise ProtocolError("message exceeds 1 MiB frame limit")
    return struct.pack(">I", len(payload)) + payload


async def read_message(reader: asyncio.StreamReader) -> dict[str, Any]:
    """Read exactly one frame and decode it."""

    header = await reader.readexactly(HEADER_SIZE)
    (size,) = struct.unpack(">I", header)
    if size > MAX_FRAME_SIZE:
        raise ProtocolError(f"frame size {size} exceeds limit")
    payload = await reader.readexactly(size)
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError("invalid UTF-8 JSON payload") from exc
    if not isinstance(value, dict):
        raise ProtocolError("JSON payload must be an object")
    return value


async def write_message(writer: asyncio.StreamWriter, message: dict[str, Any]) -> None:
    """Write one framed message and wait until it is flushed to the socket."""

    writer.write(encode_message(message))
    await writer.drain()


def ok_response(request_id: str | None, **payload: Any) -> dict[str, Any]:
    return {"ok": True, "request_id": request_id, **payload}


def error_response(request_id: str | None, code: str, message: str) -> dict[str, Any]:
    return {"ok": False, "request_id": request_id, "error": {"code": code, "message": message}}
