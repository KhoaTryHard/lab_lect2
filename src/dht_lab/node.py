"""Async TCP node implementing a small Chord ring, registry and inbox."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Iterable

from .chord import NodeRef, closest_preceding, hash_id, in_open_closed, in_open_open
from .protocol import ProtocolError, error_response, ok_response, read_message, write_message
from .storage import MessageConflict, RecordConflict, Storage

LOG = logging.getLogger("dht_lab.node")
REQUEST_TIMEOUT = 2.0
MAX_LOOKUP_HOPS = 64


class RemoteError(ConnectionError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class Node:
    """One Chord participant.

    ``bootstrap`` contains endpoints only; after joining, all routing state is
    learned from peers. The node never asks a central registry for the full
    membership list.
    """

    def __init__(
        self,
        name: str,
        host: str = "0.0.0.0",
        port: int = 7000,
        advertised_host: str | None = None,
        storage_dir: str | Path = "data",
        bootstrap: Iterable[NodeRef] = (),
        bits: int = 32,
        successor_replicas: int = 3,
    ) -> None:
        if not name:
            raise ValueError("node name cannot be empty")
        self.name = name
        self.bind_host = host
        self.port = int(port)
        self.advertised_host = advertised_host or ("127.0.0.1" if host in {"0.0.0.0", ""} else host)
        self.bits = bits
        self.ring_size = 1 << bits
        self.max_successor_replicas = max(1, int(successor_replicas))
        self.node_id = hash_id(f"node:{name}", bits)
        self.storage = Storage(storage_dir, name, self.node_id)
        stored_name = self.storage.get_meta("node_name")
        stored_id = int(self.storage.get_meta("node_id", str(self.node_id)) or self.node_id)
        if stored_name != name or stored_id != self.node_id:
            raise ValueError("storage directory belongs to a different node identity")

        self.endpoint_revision = int(self.storage.get_meta("revision", "0") or "0")
        self.self_ref = NodeRef(
            name, self.node_id, self.advertised_host, self.port, self.endpoint_revision
        )
        self.predecessor: NodeRef | None = None
        self.successor: NodeRef = self.self_ref
        self.successor_list: list[NodeRef] = [self.self_ref]
        self.fingers: list[dict[str, Any]] = []
        self.known_refs: dict[tuple[str, int], NodeRef] = {(name, self.node_id): self.self_ref}
        self.bootstrap = list(bootstrap)
        self.server: asyncio.AbstractServer | None = None
        self._tasks: list[asyncio.Task[Any]] = []
        self._finger_index = 0
        self._started = False
        self._ready = asyncio.Event()
        self._state_lock = asyncio.Lock()
        self._announcement_ids: set[str] = set()
        self._last_register_payload: dict[str, Any] | None = None
        self._storage_closed = False
        self._connection_tasks: set[asyncio.Task[Any]] = set()
        self.replica_hints: dict[str, list[NodeRef]] = {}

    @property
    def ring_descriptor(self) -> NodeRef:
        return self.self_ref

    async def start(self, join: bool = True, register: bool = True) -> None:
        if self._started:
            return
        self.server = await asyncio.start_server(
            self._handle_connection, self.bind_host, self.port
        )
        socket = self.server.sockets[0]
        actual_port = int(socket.getsockname()[1])
        self.port = actual_port
        self.self_ref = NodeRef(
            self.name, self.node_id, self.advertised_host, actual_port, self.endpoint_revision
        )
        self.known_refs[(self.name, self.node_id)] = self.self_ref
        self.successor = self.self_ref
        self.successor_list = [self.self_ref]
        self._started = True
        try:
            if join and self.bootstrap:
                await self.join_with_retries()
            else:
                await self._initialize_ring()
            if register:
                await self._register_self_with_retries()
            self._tasks = [
                asyncio.create_task(self._maintenance_loop(), name=f"{self.name}-maintenance")
            ]
            self._ready.set()
            LOG.info(
                "node ready name=%s id=%s endpoint=%s:%s successor=%s",
                self.name,
                self.node_id,
                self.advertised_host,
                self.port,
                self.successor.name,
            )
        except Exception:
            await self.stop()
            raise

    async def wait_ready(self) -> None:
        await self._ready.wait()

    async def stop(self) -> None:
        if self._storage_closed:
            return
        if not self._started and self.server is None:
            self.storage.close()
            self._storage_closed = True
            return
        self._started = False
        self._ready.clear()
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        if self._connection_tasks:
            for task in self._connection_tasks:
                task.cancel()
            await asyncio.gather(*self._connection_tasks, return_exceptions=True)
            self._connection_tasks.clear()
        self.storage.close()
        self._storage_closed = True

    async def join_with_retries(self, attempts: int = 60) -> None:
        last_error: Exception | None = None
        for attempt in range(attempts):
            for bootstrap in self.bootstrap:
                if bootstrap.name == self.name and bootstrap.port == self.port:
                    continue
                try:
                    await self._join(bootstrap)
                    await self._announce_self()
                    return
                except RemoteError as exc:
                    if exc.code == "ID_COLLISION":
                        raise
                    last_error = exc
                    LOG.warning(
                        "join retry node=%s attempt=%d peer=%s error=%s",
                        self.name,
                        attempt + 1,
                        bootstrap.name,
                        exc,
                    )
                except (OSError, asyncio.TimeoutError, LookupError) as exc:
                    last_error = exc
                    LOG.warning(
                        "join retry node=%s attempt=%d peer=%s error=%s",
                        self.name,
                        attempt + 1,
                        bootstrap.name,
                        exc,
                    )
            await asyncio.sleep(min(0.25 + attempt * 0.05, 2.0))
        raise RuntimeError(f"node {self.name} could not join the ring: {last_error}")

    async def _initialize_ring(self) -> None:
        async with self._state_lock:
            self.predecessor = None
            self.successor = self.self_ref
            self.successor_list = [self.self_ref]
            self._rebuild_local_fingers()

    async def _join(self, bootstrap: NodeRef) -> None:
        response = await self.rpc(
            bootstrap,
            {
                "op": "FIND_SUCCESSOR",
                "key": self.node_id,
                "hop": 0,
                "trace": [],
            },
        )
        successor = NodeRef.from_dict(response["node"])
        if successor.node_id == self.node_id and successor.name != self.name:
            raise RemoteError("ID_COLLISION", "another node has the same Chord identifier")
        async with self._state_lock:
            self.predecessor = None
            self.successor = successor
            self.successor_list = [successor]
            self._remember(successor)
            self._rebuild_local_fingers()
        await self.rpc(successor, {"op": "NOTIFY", "node": self.self_ref.to_dict()})

    async def rpc(
        self, ref: NodeRef, request: dict[str, Any], timeout: float = REQUEST_TIMEOUT
    ) -> dict[str, Any]:
        request = dict(request)
        request["request_id"] = request.get("request_id") or uuid.uuid4().hex
        if self._is_self(ref):
            try:
                response = await self._dispatch(request)
            except RemoteError:
                raise
            except RecordConflict as exc:
                raise RemoteError("RECORD_CONFLICT", str(exc)) from exc
            except MessageConflict as exc:
                raise RemoteError("MESSAGE_CONFLICT", str(exc)) from exc
            except (KeyError, TypeError, ValueError) as exc:
                raise RemoteError("BAD_REQUEST", str(exc)) from exc
        else:
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
            raise RemoteError(str(details.get("code", "REMOTE_ERROR")), str(details.get("message", "unknown error")))
        if response.get("request_id") != request["request_id"]:
            raise RemoteError("PROTOCOL_ERROR", "response request_id does not match request")
        return response

    def _is_self(self, ref: NodeRef) -> bool:
        return ref.name == self.name and ref.node_id == self.node_id

    def _remember(self, ref: NodeRef) -> None:
        for (known_name, known_id), known in self.known_refs.items():
            if known_id == ref.node_id and known_name != ref.name:
                raise RemoteError(
                    "ID_COLLISION",
                    f"node_id {ref.node_id} is already used by {known.name!r}",
                )
        key = (ref.name, ref.node_id)
        old = self.known_refs.get(key)
        if old is not None and ref.revision < old.revision:
            return
        self.known_refs[key] = ref
        if old is None or old == ref:
            return
        self.predecessor = self._replace_in_ref(self.predecessor, old, ref)
        self.successor = self._replace_in_ref(self.successor, old, ref) or self.successor
        self.successor_list = self._replace_in_refs(self.successor_list, old, ref)
        for finger in self.fingers:
            value = finger.get("node")
            if isinstance(value, NodeRef) and (value.name, value.node_id) == (old.name, old.node_id):
                finger["node"] = ref

    @staticmethod
    def _replace_in_ref(value: NodeRef | None, old: NodeRef, new: NodeRef) -> NodeRef | None:
        if value is None:
            return None
        return new if (value.name, value.node_id) == (old.name, old.node_id) else value

    @staticmethod
    def _replace_in_refs(values: list[NodeRef], old: NodeRef, new: NodeRef) -> list[NodeRef]:
        return [new if (value.name, value.node_id) == (old.name, old.node_id) else value for value in values]

    def _rebuild_local_fingers(self) -> None:
        self.fingers = [
            {
                "index": index,
                "start": (self.node_id + (1 << index)) % self.ring_size,
                "node": self.successor,
            }
            for index in range(self.bits)
        ]

    async def refresh_one_finger(self) -> None:
        if not self.fingers:
            self._rebuild_local_fingers()
        index = self._finger_index % self.bits
        self._finger_index += 1
        key = (self.node_id + (1 << index)) % self.ring_size
        try:
            result = await self.find_successor(key)
        except Exception as exc:
            LOG.debug("finger refresh failed node=%s index=%d: %s", self.name, index, exc)
            return
        ref = NodeRef.from_dict(result["node"])
        self._remember(ref)
        self.fingers[index] = {"index": index, "start": key, "node": ref}

    async def stabilize(self) -> None:
        successor = self.successor
        if self._is_self(successor):
            if self.predecessor and self.predecessor.node_id != self.node_id:
                candidate = self.predecessor
                try:
                    await self.rpc(candidate, {"op": "PING"})
                    self.successor = candidate
                    self.successor_list = [candidate, self.self_ref]
                except Exception:
                    self.predecessor = None
            return
        try:
            response = await self.rpc(successor, {"op": "GET_PREDECESSOR"})
            candidate_data = response.get("node")
            if candidate_data:
                candidate = NodeRef.from_dict(candidate_data)
                self._remember(candidate)
                if candidate.node_id != self.node_id and in_open_closed(
                    candidate.node_id, self.node_id, successor.node_id, self.ring_size
                ):
                    self.successor = candidate
                    successor = candidate
            await self.rpc(successor, {"op": "NOTIFY", "node": self.self_ref.to_dict()})
            await self.refresh_successor_list()
        except Exception as exc:
            LOG.debug("stabilize failure node=%s successor=%s: %s", self.name, successor.name, exc)
            self._mark_failed(successor)

    async def refresh_successor_list(self) -> None:
        successor = self.successor
        refs: list[NodeRef] = []
        if not self._is_self(successor):
            try:
                response = await self.rpc(successor, {"op": "GET_SUCCESSOR_LIST"})
                refs = [NodeRef.from_dict(value) for value in response.get("nodes", [])]
            except Exception:
                self._mark_failed(successor)
        candidates = [successor, *refs, *self.successor_list, self.self_ref]
        unique: list[NodeRef] = []
        seen: set[tuple[str, int]] = set()
        for ref in candidates:
            if (ref.name, ref.node_id) in seen:
                continue
            seen.add((ref.name, ref.node_id))
            if self._is_self(ref):
                unique.append(self.self_ref)
                continue
            try:
                await self.rpc(ref, {"op": "PING"}, timeout=0.5)
            except Exception:
                continue
            self._remember(ref)
            unique.append(ref)
            if len(unique) >= self.max_successor_replicas:
                break
        if not unique:
            unique = [self.self_ref]
        self.successor = unique[0]
        self.successor_list = unique

    def _mark_failed(self, ref: NodeRef) -> None:
        key = (ref.name, ref.node_id)
        self.known_refs.pop(key, None)
        self.successor_list = [node for node in self.successor_list if (node.name, node.node_id) != key]
        for finger in self.fingers:
            value = finger.get("node")
            if isinstance(value, NodeRef) and (value.name, value.node_id) == key:
                finger["node"] = self.self_ref
        if (self.successor.name, self.successor.node_id) == key:
            self.successor = self.successor_list[0] if self.successor_list else self.self_ref

    async def check_predecessor(self) -> None:
        predecessor = self.predecessor
        if predecessor is None or self._is_self(predecessor):
            return
        try:
            await self.rpc(predecessor, {"op": "PING"}, timeout=0.5)
        except Exception:
            self.predecessor = None

    async def _maintenance_loop(self) -> None:
        last_stabilize = 0.0
        last_health = 0.0
        while self._started:
            try:
                now = time.monotonic()
                if now - last_stabilize >= 1.0:
                    await self.stabilize()
                    last_stabilize = time.monotonic()
                await self.refresh_one_finger()
                if now - last_health >= 1.0:
                    await self.check_predecessor()
                    await self.sync_replica_records()
                    last_health = time.monotonic()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.debug("maintenance failure node=%s: %s", self.name, exc)
            await asyncio.sleep(0.25)

    async def find_successor(
        self, key: int, hop: int = 0, trace: list[str] | None = None
    ) -> dict[str, Any]:
        key %= self.ring_size
        trace = list(trace or [])
        if hop > MAX_LOOKUP_HOPS:
            raise RemoteError("HOP_LIMIT", "lookup exceeded 64 hops")
        if self.name in trace:
            raise RemoteError("ROUTING_LOOP", "lookup encountered a repeated node")
        trace.append(self.name)
        if key == self.node_id:
            return {"node": self.self_ref.to_dict(), "trace": trace, "hops": len(trace) - 1}
        successor = self.successor
        if self._is_self(successor) or in_open_closed(
            key, self.node_id, successor.node_id, self.ring_size
        ):
            return {"node": successor.to_dict(), "trace": trace, "hops": len(trace) - 1}

        candidates = self._routing_candidates(key)
        for candidate in candidates:
            if candidate.name in trace or self._is_self(candidate):
                continue
            try:
                response = await self.rpc(
                    candidate,
                    {"op": "FIND_SUCCESSOR", "key": key, "hop": hop + 1, "trace": trace},
                )
                self._remember(NodeRef.from_dict(response["node"]))
                response_trace = response.get("trace", trace)
                return {
                    "node": response["node"],
                    "trace": response_trace,
                    "hops": int(response.get("hops", len(response_trace) - 1)),
                }
            except (OSError, asyncio.TimeoutError, RemoteError) as exc:
                LOG.debug("route failure from=%s to=%s: %s", self.name, candidate.name, exc)
                self._mark_failed(candidate)
        if not self._is_self(successor):
            try:
                response = await self.rpc(
                    successor,
                    {"op": "FIND_SUCCESSOR", "key": key, "hop": hop + 1, "trace": trace},
                )
                self._remember(NodeRef.from_dict(response["node"]))
                return response
            except Exception:
                self._mark_failed(successor)
        if self.successor_list and not self._is_self(self.successor_list[0]):
            for candidate in self.successor_list:
                if candidate.name in trace:
                    continue
                try:
                    return await self.rpc(
                        candidate,
                        {"op": "FIND_SUCCESSOR", "key": key, "hop": hop + 1, "trace": trace},
                    )
                except Exception:
                    self._mark_failed(candidate)
        raise RemoteError("NO_ROUTE", "no live successor is available")

    async def find_successor_linear(
        self, key: int, hop: int = 0, trace: list[str] | None = None
    ) -> dict[str, Any]:
        """Successor-only baseline used by the benchmark and teaching demo."""

        key %= self.ring_size
        trace = list(trace or [])
        if hop > MAX_LOOKUP_HOPS:
            raise RemoteError("HOP_LIMIT", "linear lookup exceeded 64 hops")
        if self.name in trace:
            raise RemoteError("ROUTING_LOOP", "linear lookup encountered a repeated node")
        trace.append(self.name)
        successor = self.successor
        if key == self.node_id:
            return {"node": self.self_ref.to_dict(), "trace": trace, "hops": len(trace) - 1}
        if self._is_self(successor) or in_open_closed(
            key, self.node_id, successor.node_id, self.ring_size
        ):
            return {"node": successor.to_dict(), "trace": trace, "hops": len(trace) - 1}
        response = await self.rpc(
            successor,
            {
                "op": "FIND_SUCCESSOR_LINEAR",
                "key": key,
                "hop": hop + 1,
                "trace": trace,
            },
        )
        return response

    def _routing_candidates(self, target: int) -> list[NodeRef]:
        values: list[NodeRef] = []
        for finger in reversed(self.fingers):
            data = finger.get("node")
            if data:
                try:
                    values.append(data if isinstance(data, NodeRef) else NodeRef.from_dict(data))
                except (KeyError, TypeError, ValueError):
                    continue
        values.extend(self.successor_list)
        chosen = closest_preceding(self.node_id, target, values, self.bits)
        ordered = [chosen] if chosen else []
        ordered.extend(values)
        unique: list[NodeRef] = []
        seen: set[tuple[str, int]] = set()
        for value in ordered:
            key = (value.name, value.node_id)
            if key not in seen:
                seen.add(key)
                unique.append(value)
        return unique

    async def register_self(self) -> dict[str, Any]:
        current = self.storage.get_record(self.name)
        if (
            current
            and current.get("node_id") == self.node_id
            and current.get("host") == self.advertised_host
            and int(current.get("port", -1)) == self.port
        ):
            self.endpoint_revision = int(current.get("revision", self.endpoint_revision))
            self.self_ref = NodeRef(
                self.name,
                self.node_id,
                self.advertised_host,
                self.port,
                self.endpoint_revision,
            )
            self.known_refs[(self.name, self.node_id)] = self.self_ref
            self._last_register_payload = current
            return current
        pending = self._last_register_payload
        if (
            pending
            and pending.get("node_id") == self.node_id
            and pending.get("host") == self.advertised_host
            and int(pending.get("port", -1)) == self.port
        ):
            record = dict(pending)
        else:
            record = {
                "name": self.name,
                "node_id": self.node_id,
                "host": self.advertised_host,
                "port": self.port,
                "revision": self.storage.next_revision(),
            }
        self.endpoint_revision = int(record["revision"])
        self.self_ref = NodeRef(
            self.name,
            self.node_id,
            self.advertised_host,
            self.port,
            self.endpoint_revision,
        )
        self.known_refs[(self.name, self.node_id)] = self.self_ref
        self._last_register_payload = record
        await self.register_record(record)
        return record

    async def _register_self_with_retries(self, attempts: int = 30) -> dict[str, Any]:
        """Register identity after join, allowing the new ring to converge."""

        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return await self.register_self()
            except RemoteError as exc:
                if exc.code not in {"OWNER_UNAVAILABLE", "REPLICATION_FAILED", "NO_ROUTE"}:
                    raise
                last_error = exc
                await asyncio.sleep(min(0.1 + attempt * 0.05, 1.0))
        raise RuntimeError(f"node {self.name} could not register its endpoint: {last_error}")

    async def register_record(self, record: dict[str, Any]) -> dict[str, Any]:
        self._validate_record(record)
        key = hash_id(f"service:{record['name']}", self.bits)
        owner_result = await self.find_successor(key)
        owner = NodeRef.from_dict(owner_result["node"])
        targets_response = await self.rpc(owner, {"op": "GET_REPLICATION_TARGETS"})
        targets = [NodeRef.from_dict(value) for value in targets_response.get("nodes", [])]
        if not targets:
            targets = [owner]
        targets = self._unique_refs(targets)[: self.max_successor_replicas]
        results = await asyncio.gather(
            *(self.rpc(target, {"op": "STORE_RECORD", "record": record}) for target in targets),
            return_exceptions=True,
        )
        acknowledgements = [
            (target, result)
            for target, result in zip(targets, results)
            if isinstance(result, dict)
        ]
        owner_ack = any(ref.name == owner.name and ref.node_id == owner.node_id for ref, _ in acknowledgements)
        if not owner_ack:
            raise RemoteError("OWNER_UNAVAILABLE", "registry owner did not acknowledge the record")
        minimum = 1 if len(targets) == 1 else 2
        if len(acknowledgements) < minimum:
            raise RemoteError("REPLICATION_FAILED", "owner and one replica did not acknowledge")
        self.replica_hints[record["name"]] = targets
        return {
            "record": record,
            "owner": owner.to_dict(),
            "replicas": [target.to_dict() for target in targets],
            "acknowledgements": len(acknowledgements),
        }

    def _validate_record(self, record: dict[str, Any]) -> None:
        required = {"name", "node_id", "host", "port", "revision"}
        missing = required.difference(record)
        if missing:
            raise ValueError(f"record requires {sorted(missing)}")
        record["name"] = str(record["name"])
        record["host"] = str(record["host"])
        record["node_id"] = int(record["node_id"])
        record["port"] = int(record["port"])
        record["revision"] = int(record["revision"])
        if not record["name"].strip():
            raise ValueError("record name cannot be empty")
        if not record["host"].strip():
            raise ValueError("record host cannot be empty")
        node_id = record["node_id"]
        if not 0 <= node_id < self.ring_size:
            raise ValueError("record node_id is outside the identifier space")
        port = record["port"]
        if not 1 <= port <= 65535:
            raise ValueError("record port must be between 1 and 65535")
        if record["revision"] < 0:
            raise ValueError("record revision cannot be negative")

    async def resolve_name(self, name: str) -> dict[str, Any]:
        key = hash_id(f"service:{name}", self.bits)
        owner_result = await self.find_successor(key)
        owner = NodeRef.from_dict(owner_result["node"])
        record = None
        candidates = self._unique_refs(
            [owner, *self.replica_hints.get(name, []), *self.successor_list, *self.known_refs.values()]
        )
        for target in candidates:
            try:
                response = await self.rpc(target, {"op": "GET_RECORD", "name": name})
                candidate_record = response.get("record")
                if candidate_record is None:
                    continue
                if record is None or int(candidate_record.get("revision", 0)) > int(record.get("revision", 0)):
                    record = candidate_record
            except Exception:
                self._mark_failed(target)
        if record is None:
            raise RemoteError("NOT_FOUND", f"service {name!r} is not registered")
        return {
            "name": name,
            "key": key,
            "record": record,
            "owner": owner.to_dict(),
            "trace": owner_result.get("trace", []),
            "hops": owner_result.get("hops", 0),
        }

    async def send_message(
        self,
        target_name: str,
        sender: str,
        body: str,
        message_id: str | None = None,
        deadline: float = 30.0,
    ) -> dict[str, Any]:
        message_id = message_id or uuid.uuid4().hex
        end = time.monotonic() + deadline
        attempts = 0
        last_error: Exception | None = None
        while time.monotonic() < end:
            attempts += 1
            try:
                remaining = max(0.001, end - time.monotonic())
                response, resolution = await asyncio.wait_for(
                    self._send_once(target_name, sender, body, message_id),
                    timeout=remaining,
                )
                return {
                    "delivered": True,
                    "message_id": message_id,
                    "attempts": attempts,
                    "resolution": resolution,
                    "ack": response,
                }
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

    async def _send_once(
        self, target_name: str, sender: str, body: str, message_id: str
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        resolution = await self.resolve_name(target_name)
        record = resolution["record"]
        target = NodeRef(
            target_name,
            int(record["node_id"]),
            str(record["host"]),
            int(record["port"]),
            int(record.get("revision", 0)),
        )
        response = await self.rpc(
            target,
            {
                "op": "DELIVER",
                "message_id": message_id,
                "sender": sender,
                "recipient": target_name,
                "recipient_node_id": target.node_id,
                "body": body,
            },
        )
        return response, resolution

    async def sync_replica_records(self) -> None:
        records = self.storage.list_records()
        if not records or self._is_self(self.successor):
            return
        for record in records:
            with contextlib.suppress(Exception):
                await self.register_record(record)

    async def _announce_self(self) -> None:
        announcement_id = uuid.uuid4().hex
        self._announcement_ids.add(announcement_id)
        targets = self._unique_refs([self.successor, self.predecessor, *self.bootstrap])
        for target in targets:
            if self._is_self(target):
                continue
            with contextlib.suppress(Exception):
                await self.rpc(
                    target,
                    {
                        "op": "ANNOUNCE",
                        "announcement_id": announcement_id,
                        "node": self.self_ref.to_dict(),
                        "ttl": 4,
                    },
                )

    async def _forward_announcement(
        self, announcement_id: str, ref: NodeRef, ttl: int
    ) -> None:
        if ttl <= 0:
            return
        targets = self._unique_refs(
            [self.successor, self.predecessor, *self.successor_list, *self._finger_refs()]
        )
        for target in targets:
            if self._is_self(target) or target.name == ref.name:
                continue
            with contextlib.suppress(Exception):
                await self.rpc(
                    target,
                    {
                        "op": "ANNOUNCE",
                        "announcement_id": announcement_id,
                        "node": ref.to_dict(),
                        "ttl": ttl - 1,
                    },
                    timeout=0.5,
                )

    def _finger_refs(self) -> list[NodeRef]:
        refs: list[NodeRef] = []
        for finger in self.fingers:
            data = finger.get("node")
            if data:
                with contextlib.suppress(Exception):
                    refs.append(data if isinstance(data, NodeRef) else NodeRef.from_dict(data))
        return refs

    def _unique_refs(self, values: Iterable[NodeRef | None]) -> list[NodeRef]:
        result: list[NodeRef] = []
        seen: set[tuple[str, int]] = set()
        for ref in values:
            if ref is None:
                continue
            key = (ref.name, ref.node_id)
            if key not in seen:
                seen.add(key)
                result.append(ref)
        return result

    async def _handle_connection(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        current_task = asyncio.current_task()
        if current_task is not None:
            self._connection_tasks.add(current_task)
        peer = writer.get_extra_info("peername")
        try:
            while True:
                try:
                    request = await read_message(reader)
                except asyncio.IncompleteReadError:
                    break
                except ProtocolError as exc:
                    await write_message(writer, error_response(None, "BAD_FRAME", str(exc)))
                    break
                try:
                    response = await self._dispatch(request)
                except RemoteError as exc:
                    response = error_response(request.get("request_id"), exc.code, exc.message)
                except RecordConflict as exc:
                    response = error_response(request.get("request_id"), "RECORD_CONFLICT", str(exc))
                except MessageConflict as exc:
                    response = error_response(request.get("request_id"), "MESSAGE_CONFLICT", str(exc))
                except (KeyError, TypeError, ValueError) as exc:
                    response = error_response(request.get("request_id"), "BAD_REQUEST", str(exc))
                except Exception as exc:
                    LOG.exception("request failed node=%s peer=%s", self.name, peer)
                    response = error_response(request.get("request_id"), "INTERNAL", str(exc))
                await write_message(writer, response)
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.CancelledError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            if current_task is not None:
                self._connection_tasks.discard(current_task)

    async def _dispatch(self, request: dict[str, Any]) -> dict[str, Any]:
        request_id = request.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            raise RemoteError("BAD_REQUEST", "request_id must be a non-empty string")
        op = str(request.get("op", ""))
        if op == "PING":
            return ok_response(request_id, node=self.self_ref.to_dict())
        if op == "GET_PREDECESSOR":
            return ok_response(request_id, node=self.predecessor.to_dict() if self.predecessor else None)
        if op == "GET_SUCCESSOR_LIST":
            return ok_response(request_id, nodes=[node.to_dict() for node in self.successor_list])
        if op == "GET_REPLICATION_TARGETS":
            known = sorted(
                self.known_refs.values(),
                key=lambda ref: (ref.node_id - self.node_id) % self.ring_size,
            )
            return ok_response(
                request_id,
                nodes=[
                    node.to_dict()
                    for node in self._unique_refs([self.self_ref, *self.successor_list, *known])
                    if node.name != self.name or node.node_id == self.node_id
                ][: self.max_successor_replicas],
            )
        if op == "FIND_SUCCESSOR":
            result = await self.find_successor(
                int(request["key"]), int(request.get("hop", 0)), list(request.get("trace", []))
            )
            return ok_response(request_id, **result)
        if op == "FIND_SUCCESSOR_LINEAR":
            result = await self.find_successor_linear(
                int(request["key"]), int(request.get("hop", 0)), list(request.get("trace", []))
            )
            result = {key: value for key, value in result.items() if key not in {"ok", "request_id"}}
            return ok_response(request_id, **result)
        if op == "GET_SUCCESSOR":
            return ok_response(request_id, node=self.successor.to_dict())
        if op == "NOTIFY":
            ref = NodeRef.from_dict(request["node"])
            self._remember(ref)
            async with self._state_lock:
                if (
                    self.predecessor is None
                    or self._is_self(self.predecessor)
                    or in_open_open(ref.node_id, self.predecessor.node_id, self.node_id, self.ring_size)
                ):
                    self.predecessor = ref
            return ok_response(request_id, accepted=True)
        if op == "ANNOUNCE":
            ref = NodeRef.from_dict(request["node"])
            announcement_id = str(request.get("announcement_id", uuid.uuid4().hex))
            first_seen = announcement_id not in self._announcement_ids
            self._announcement_ids.add(announcement_id)
            self._remember(ref)
            if first_seen:
                self._tasks.append(
                    asyncio.create_task(
                        self._forward_announcement(announcement_id, ref, int(request.get("ttl", 0)))
                    )
                )
            return ok_response(request_id, accepted=True)
        if op == "STORE_RECORD":
            record = dict(request["record"])
            self._validate_record(record)
            applied = self.storage.put_record(record)
            return ok_response(request_id, applied=applied, record=record)
        if op == "GET_RECORD":
            return ok_response(request_id, record=self.storage.get_record(str(request["name"])))
        if op == "RESOLVE":
            return ok_response(request_id, **(await self.resolve_name(str(request["name"]))))
        if op == "REGISTER":
            record = dict(request["record"])
            return ok_response(request_id, **(await self.register_record(record)))
        if op == "SEND":
            result = await self.send_message(
                str(request["recipient"]),
                str(request.get("sender", "unknown")),
                str(request.get("body", "")),
                message_id=str(request.get("message_id") or uuid.uuid4().hex),
                deadline=float(request.get("deadline", 30.0)),
            )
            return ok_response(request_id, **result)
        if op == "DELIVER":
            message_id = str(request["message_id"])
            recipient = str(request["recipient"])
            if recipient != self.name:
                raise RemoteError("RECIPIENT_MISMATCH", f"node {self.name!r} cannot receive for {recipient!r}")
            requested_node_id = request.get("recipient_node_id")
            if requested_node_id is not None and int(requested_node_id) != self.node_id:
                raise RemoteError("RECIPIENT_MISMATCH", "recipient node_id does not match this node")
            inserted, duplicate = self.storage.deliver_once(
                message_id,
                str(request.get("sender", "unknown")),
                recipient,
                str(request.get("body", "")),
            )
            return ok_response(
                request_id,
                delivered=True,
                inserted=inserted,
                duplicate=duplicate,
                node=self.name,
                message_id=message_id,
            )
        if op == "STATUS":
            return ok_response(request_id, **self.status())
        if op == "LIST_MESSAGES":
            return ok_response(request_id, messages=self.storage.list_messages())
        raise RemoteError("UNKNOWN_OPERATION", f"unknown operation {op!r}")

    def status(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "node_id": self.node_id,
            "endpoint": {"host": self.advertised_host, "port": self.port},
            "predecessor": self.predecessor.to_dict() if self.predecessor else None,
            "successor": self.successor.to_dict(),
            "successor_list": [node.to_dict() for node in self.successor_list],
            "fingers": [
                {
                    **finger,
                    "node": (
                        finger["node"].to_dict()
                        if isinstance(finger.get("node"), NodeRef)
                        else finger.get("node")
                    ),
                }
                for finger in self.fingers
            ],
            "records": self.storage.list_records(),
            "messages": self.storage.list_messages(),
        }
