from __future__ import annotations

import unittest

from dht_lab.chord import NodeRef, build_finger_table, hash_id, in_open_closed, successor_for


class ChordMathTests(unittest.TestCase):
    def setUp(self) -> None:
        self.nodes = [
            NodeRef("n4", 4, "127.0.0.1", 7004),
            NodeRef("n12", 12, "127.0.0.1", 7012),
            NodeRef("n20", 20, "127.0.0.1", 7020),
            NodeRef("n28", 28, "127.0.0.1", 7028),
        ]

    def test_wrapped_open_closed_interval(self) -> None:
        self.assertTrue(in_open_closed(2, 28, 4, 32))
        self.assertTrue(in_open_closed(4, 28, 4, 32))
        self.assertFalse(in_open_closed(28, 28, 4, 32))
        self.assertFalse(in_open_closed(20, 28, 4, 32))

    def test_successor_including_exact_boundary_and_wrap(self) -> None:
        self.assertEqual(successor_for(12, self.nodes, 5).name, "n12")
        self.assertEqual(successor_for(13, self.nodes, 5).name, "n20")
        self.assertEqual(successor_for(31, self.nodes, 5).name, "n4")

    def test_finger_table_has_m_entries(self) -> None:
        fingers = build_finger_table(self.nodes[0], self.nodes, bits=5)
        self.assertEqual(len(fingers), 5)
        self.assertEqual(fingers[0]["node"]["name"], "n12")
        self.assertEqual(fingers[3]["node"]["name"], "n12")

    def test_hash_is_stable(self) -> None:
        self.assertEqual(hash_id("node:alpha", 32), hash_id("node:alpha", 32))
        self.assertNotEqual(hash_id("node:alpha", 32), hash_id("node:beta", 32))


if __name__ == "__main__":
    unittest.main()
