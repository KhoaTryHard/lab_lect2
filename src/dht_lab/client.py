# Cung cap client ket noi seed node de goi RPC DHT.
"""External client helpers for the Mini-DHT.

The client is deliberately not a Chord participant. It has no listening
socket, storage database, predecessor or successor state; it sends RPCs to
one of the configured seed endpoints and lets the node perform the lookup.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from typing import Any, Iterable

from .chord import NodeRef
from .node import REQUEST_TIMEOUT, RemoteError
from .protocol import read_message, write_message


def endpoint_ref(value: str, default_port: int = 7000) -> NodeRef:
    """Parse ``host:port`` or ``name@host:port`` into a seed endpoint."""

    value = value.strip()
    if not value:
        raise ValueError("empty endpoint")
    if "@" in value:
        name, endpoint = value.split("@", 1)
    else:
        name, endpoint = value, value
    if endpoint.startswith("[") and "]" in endpoint:
        host, _, port_text = endpoint[1:].partition("]")
        port = int(port_text.removeprefix(":")) if port_text else default_port
    elif ":" in endpoint and endpoint.rsplit(":", 1)[1].isdigit():
        host, port_text = endpoint.rsplit(":", 1)
        port = int(port_text)
    else:
        host, port = endpoint, default_port
    return NodeRef(name=name, node_id=-1, host=host, port=port)


def parse_endpoints(value: str | None, default_port: int = 7000) -> list[NodeRef]:
    if not value:
        return []
    return [endpoint_ref(item, default_port) for item in value.split(",") if item.strip()]


class DhtClient:
    """A seed-based client that never joins the Chord ring."""

    def __init__(self, seeds: Iterable[NodeRef], bits: int = 32) -> None:
        self.seeds = list(seeds)
        self.bits = bits
        if not self.seeds:
            raise ValueError("at least one seed is required")
        self._started = False

    async def start(self) -> None:
        if self._started:
            return
        deadline = time.monotonic() + REQUEST_TIMEOUT
        last_error: Exception | None = None
        for seed in self.seeds:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                await self.rpc(seed, {"op": "PING"}, timeout=remaining)
                self._started = True
                return
            except Exception as exc:
                last_error = exc
        raise RemoteError("NO_BOOTSTRAP", f"none of the seed endpoints responded: {last_error}")

    async def stop(self) -> None:
        self._started = False

    async def rpc(
        self, ref: NodeRef, request: dict[str, Any], timeout: float = REQUEST_TIMEOUT
    ) -> dict[str, Any]:
        request = dict(request)
        request["request_id"] = request.get("request_id") or uuid.uuid4().hex
        deadline = time.monotonic() + timeout
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(ref.host, ref.port), timeout=max(0.001, deadline - time.monotonic())
        )
        try:
            await asyncio.wait_for(
                write_message(writer, request), timeout=max(0.001, deadline - time.monotonic())
            )
            response = await asyncio.wait_for(
                read_message(reader), timeout=max(0.001, deadline - time.monotonic())
            )
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        if not response.get("ok", False):
            details = response.get("error") or {}
            raise RemoteError(
                str(details.get("code", "REMOTE_ERROR")),
                str(details.get("message", "unknown error")),
            )
        if response.get("request_id") != request["request_id"]:
            raise RemoteError("PROTOCOL_ERROR", "response request_id does not match request")
        return response

    async def _call_seeds(self, request: dict[str, Any], deadline: float = REQUEST_TIMEOUT) -> dict[str, Any]:
        end = time.monotonic() + deadline
        last_error: Exception | None = None
        for seed in self.seeds:
            remaining = end - time.monotonic()
            if remaining <= 0:
                break
            try:
                return await self.rpc(seed, request, timeout=min(REQUEST_TIMEOUT, remaining))
            except (OSError, asyncio.TimeoutError, RemoteError) as exc:
                last_error = exc
        raise RemoteError("REQUEST_FAILED", str(last_error or "request deadline exceeded"))

    async def lookup(self, name: str) -> dict[str, Any]:
        return await self._call_seeds({"op": "RESOLVE", "name": name})

    async def register(
        self, name: str, host: str, port: int, node_id: int, revision: int = 1
    ) -> dict[str, Any]:
        record = {
            "name": name,
            "node_id": int(node_id),
            "host": host,
            "port": int(port),
            "revision": int(revision),
        }
        return await self._call_seeds({"op": "REGISTER", "record": record})

    async def send(
        self,
        target_name: str,
        sender: str,
        body: str,
        message_id: str | None = None,
        deadline: float = 30.0,
    ) -> dict[str, Any]:
        message_id = message_id or uuid.uuid4().hex
        end = time.monotonic() + deadline
        last_error: Exception | None = None
        attempts = 0
        while time.monotonic() < end:
            attempts += 1
            remaining = end - time.monotonic()
            request = {
                "op": "SEND",
                "recipient": target_name,
                "sender": sender,
                "body": body,
                "message_id": message_id,
                "deadline": remaining,
            }
            try:
                response = await self._call_seeds(request, deadline=remaining)
                response.setdefault("message_id", message_id)
                response["client_attempts"] = attempts
                return response
            except (OSError, asyncio.TimeoutError, RemoteError) as exc:
                last_error = exc
                remaining = end - time.monotonic()
                if remaining <= 0:
                    break
                await asyncio.sleep(min(0.25 * attempts, 1.0, remaining))
        raise RemoteError(
            "DELIVERY_TIMEOUT",
            f"message_id={message_id} was not acknowledged before deadline: {last_error}",
        )

    async def status(self) -> dict[str, Any]:
        return await self._call_seeds({"op": "STATUS"})

    async def messages(self) -> dict[str, Any]:
        return await self._call_seeds({"op": "LIST_MESSAGES"})


async def run_client(operation: str, seeds: Iterable[NodeRef], **kwargs: Any) -> dict[str, Any]:
    client = DhtClient(seeds)
    await client.start()
    try:
        if operation == "lookup":
            return await client.lookup(kwargs["name"])
        if operation == "register":
            return await client.register(**kwargs)
        if operation == "send":
            return await client.send(**kwargs)
        if operation == "status":
            return await client.status()
        if operation == "messages":
            return await client.messages()
        raise ValueError(f"unsupported client operation {operation!r}")
    finally:
        await client.stop()
