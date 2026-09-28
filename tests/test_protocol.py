# Kiem thu ma hoa, doc khung JSON va xu ly frame loi.
from __future__ import annotations

import asyncio
import tempfile
import unittest

from dht_lab.protocol import MAX_FRAME_SIZE, ProtocolError, encode_message, read_message


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_multiple_frames_and_unicode(self) -> None:
        first = encode_message({"text": "Xin chào", "number": 1})
        second = encode_message({"text": "second", "number": 2})
        reader = asyncio.StreamReader()
        reader.feed_data(first + second)
        reader.feed_eof()
        self.assertEqual((await read_message(reader))["text"], "Xin chào")
        self.assertEqual((await read_message(reader))["number"], 2)

    async def test_oversized_frame_is_rejected(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data((MAX_FRAME_SIZE + 1).to_bytes(4, "big"))
        reader.feed_eof()
        with self.assertRaises(ProtocolError):
            await read_message(reader)

    async def test_partial_frame_is_reassembled(self) -> None:
        frame = encode_message({"message": "partial"})
        reader = asyncio.StreamReader()
        for byte in frame:
            reader.feed_data(bytes([byte]))
        reader.feed_eof()
        self.assertEqual((await read_message(reader))["message"], "partial")

    async def test_request_id_is_required_by_node_dispatch(self) -> None:
        from dht_lab.node import Node, RemoteError

        with tempfile.TemporaryDirectory(prefix="protocol-node-") as directory:
            node = Node("protocol-node", storage_dir=directory, bits=8)
            try:
                with self.assertRaises(RemoteError) as context:
                    await node._dispatch({"op": "PING"})
                self.assertEqual(context.exception.code, "BAD_REQUEST")
            finally:
                node.storage.close()


if __name__ == "__main__":
    unittest.main()
