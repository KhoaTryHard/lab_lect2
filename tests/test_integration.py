# Kiem thu ket hop node, registry, message va kha nang phuc hoi.
from __future__ import annotations

import asyncio
import sys
import tempfile
import unittest
from pathlib import Path

from dht_lab.chord import NodeRef, hash_id
from dht_lab.client import DhtClient
from dht_lab.node import Node, RemoteError

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


class NodeIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="mini-dht-test-")
        self.nodes: list[Node] = []

    async def asyncTearDown(self) -> None:
        for node in reversed(self.nodes):
            await node.stop()
        self.temp_dir.cleanup()

    def path_for(self, name: str) -> Path:
        path = Path(self.temp_dir.name) / name
        path.mkdir(parents=True, exist_ok=True)
        return path

    async def add_node(self, name: str, bootstrap: list[NodeRef] | None = None) -> Node:
        node = Node(
            name=name,
            host="127.0.0.1",
            port=0,
            advertised_host="127.0.0.1",
            storage_dir=self.path_for(name),
            bootstrap=bootstrap or [],
            bits=8,
            successor_replicas=3,
        )
        await node.start()
        self.nodes.append(node)
        return node

    async def test_join_lookup_registry_and_duplicate_delivery(self) -> None:
        first = await self.add_node("node-a")
        second = await self.add_node("node-b", [first.ring_descriptor])
        third = await self.add_node("node-c", [first.ring_descriptor])
        await asyncio.sleep(2.5)

        record = {
            "name": "node-c",
            "node_id": third.node_id,
            "host": third.advertised_host,
            "port": third.port,
            "revision": 1,
        }
        result = await first.register_record(record)
        self.assertGreaterEqual(result["acknowledgements"], 2)
        resolved = await second.resolve_name("node-c")
        self.assertEqual(resolved["record"]["port"], third.port)
        self.assertLessEqual(resolved["hops"], 8)

        delivered = await first.send_message("node-c", "node-a", "hello", message_id="m-1", deadline=5)
        self.assertTrue(delivered["delivered"])
        duplicate = await first.send_message("node-c", "node-a", "hello", message_id="m-1", deadline=5)
        self.assertTrue(duplicate["ack"]["duplicate"])
        self.assertEqual(len(third.storage.list_messages()), 1)

        client = DhtClient([first.ring_descriptor])
        try:
            await client.start()
            client_result = await client.send("node-c", "external-client", "through gateway", deadline=5)
            self.assertTrue(client_result["delivered"])
        finally:
            await client.stop()

    async def test_stale_revision_cannot_restore_old_endpoint(self) -> None:
        first = await self.add_node("node-a")
        second = await self.add_node("node-b", [first.ring_descriptor])
        await asyncio.sleep(1.5)
        newer = {
            "name": "moving-service",
            "node_id": hash_id("node:moving-service", 8),
            "host": "10.0.0.99",
            "port": 7999,
            "revision": 2,
        }
        older = {**newer, "host": "10.0.0.1", "revision": 1}
        await first.register_record(newer)
        await first.register_record(older)
        resolved = await second.resolve_name("moving-service")
        self.assertEqual(resolved["record"]["revision"], 2)
        self.assertEqual(resolved["record"]["host"], "10.0.0.99")

    async def test_failed_successor_is_removed_from_routing_candidates(self) -> None:
        first = await self.add_node("node-a")
        second = await self.add_node("node-b", [first.ring_descriptor])
        third = await self.add_node("node-c", [first.ring_descriptor])
        await asyncio.sleep(2)
        old_successor = second.successor
        await third.stop()
        await second.stabilize()
        self.assertTrue(second.successor.name in {first.name, second.name, old_successor.name})
        # A failed endpoint must not remain as the first live successor after
        # an explicit health refresh.
        await second.refresh_successor_list()
        self.assertNotIn(third.name, [ref.name for ref in second.successor_list])

    async def test_same_node_id_survives_endpoint_change(self) -> None:
        first = await self.add_node("node-a")
        moving = await self.add_node("moving", [first.ring_descriptor])
        await asyncio.sleep(1.5)
        old_id = moving.node_id
        moving_path = self.path_for("moving")
        await moving.stop()
        replacement = Node(
            name="moving",
            host="0.0.0.0",
            port=0,
            advertised_host="127.0.0.2",
            storage_dir=moving_path,
            bootstrap=[first.ring_descriptor],
            bits=8,
            successor_replicas=3,
        )
        await replacement.start()
        self.nodes.append(replacement)
        await asyncio.sleep(2.0)
        resolved = await first.resolve_name("moving")
        self.assertEqual(replacement.node_id, old_id)
        self.assertEqual(resolved["record"]["node_id"], old_id)
        self.assertEqual(resolved["record"]["host"], "127.0.0.2")
        self.assertGreater(resolved["record"]["revision"], 1)
        delivered = await first.send_message("moving", "node-a", "new endpoint", deadline=5)
        self.assertTrue(delivered["delivered"])

    async def test_external_client_never_joins_ring(self) -> None:
        first = await self.add_node("node-a")
        existing_client_data = set(Path.cwd().glob(".client-data-*"))
        client = DhtClient([first.ring_descriptor])
        await client.start()
        try:
            self.assertIsNone(first.predecessor)
            self.assertFalse(hasattr(client, "gateway"))
            self.assertEqual(existing_client_data, set(Path.cwd().glob(".client-data-*")))
        finally:
            await client.stop()

    async def test_delivery_rejects_wrong_recipient_and_conflicting_message_id(self) -> None:
        node = await self.add_node("node-a")
        with self.assertRaises(RemoteError) as wrong_recipient:
            await node.rpc(
                node.self_ref,
                {
                    "op": "DELIVER",
                    "recipient": "other-node",
                    "recipient_node_id": node.node_id,
                    "message_id": "m-wrong",
                    "sender": "client",
                    "body": "hello",
                },
            )
        self.assertEqual(wrong_recipient.exception.code, "RECIPIENT_MISMATCH")
        await node.rpc(
            node.self_ref,
            {
                "op": "DELIVER",
                "recipient": node.name,
                "recipient_node_id": node.node_id,
                "message_id": "m-conflict",
                "sender": "client",
                "body": "first",
            },
        )
        with self.assertRaises(RemoteError) as conflict:
            await node.rpc(
                node.self_ref,
                {
                    "op": "DELIVER",
                    "recipient": node.name,
                    "recipient_node_id": node.node_id,
                    "message_id": "m-conflict",
                    "sender": "client",
                    "body": "second",
                },
            )
        self.assertEqual(conflict.exception.code, "MESSAGE_CONFLICT")

    async def test_registry_replica_answers_after_owner_failure(self) -> None:
        first = await self.add_node("node-a")
        await self.add_node("node-b", [first.ring_descriptor])
        await self.add_node("node-c", [first.ring_descriptor])
        await self.add_node("node-d", [first.ring_descriptor])
        await asyncio.sleep(3)
        record = {
            "name": "replicated-service",
            "node_id": hash_id("node:replicated-service", 8),
            "host": "127.0.0.1",
            "port": 7799,
            "revision": 1,
        }
        registration = await first.register_record(record)
        self.assertGreaterEqual(registration["acknowledgements"], 2)
        owner_name = registration["owner"]["name"]
        owner = next(node for node in self.nodes if node.name == owner_name)
        await owner.stop()
        await asyncio.sleep(1)
        live = next(node for node in self.nodes if node is not owner)
        resolved = await live.resolve_name("replicated-service")
        self.assertEqual(resolved["record"]["revision"], 1)


if __name__ == "__main__":
    unittest.main()
