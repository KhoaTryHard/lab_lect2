# Do hieu nang dinh tuyen finger so voi duong successor tuan tu.
"""Measure finger routing against successor-by-successor routing locally.

This is a reproducible experiment for the report. It creates real TCP nodes in
one event loop; the network protocol remains the same as in Docker Compose.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import random
import statistics
import tempfile
import time
from pathlib import Path

from dht_lab.chord import NodeRef, hash_id, successor_for
from dht_lab.node import Node, RemoteError


def _same_identity(left: NodeRef | None, right: NodeRef | None) -> bool:
    return left is not None and right is not None and left.name == right.name and left.node_id == right.node_id


def ring_is_stable(nodes: list[Node]) -> bool:
    refs = [node.ring_descriptor for node in nodes]
    if len({ref.node_id for ref in refs}) != len(refs):
        return False
    ordered = sorted(refs, key=lambda ref: ref.node_id)
    for index, node in enumerate(nodes):
        expected_successor = successor_for(node.node_id + 1, refs, node.bits)
        expected_predecessor = ordered[(ordered.index(node.ring_descriptor) - 1) % len(ordered)]
        if not _same_identity(node.successor, expected_successor):
            return False
        if not _same_identity(node.predecessor, expected_predecessor):
            return False
        if len(node.fingers) != node.bits:
            return False
        for finger in node.fingers:
            expected = successor_for(int(finger["start"]), refs, node.bits)
            actual = finger.get("node")
            if isinstance(actual, dict):
                actual = NodeRef.from_dict(actual)
            if not _same_identity(actual, expected):
                return False
    return True


async def prime_stable_ring(nodes: list[Node]) -> None:
    """Create a deterministic post-convergence snapshot for the measurement.

    Join/stabilize traffic is intentionally excluded from the routing benchmark.
    After every TCP node has started, this routine applies the same successor,
    predecessor and finger definitions used by the oracle, then pauses the
    maintenance loops while queries are measured. The lookup itself still uses
    the real TCP RPC path.
    """

    refs = [node.ring_descriptor for node in nodes]
    ordered = sorted(refs, key=lambda ref: ref.node_id)
    for node in nodes:
        node._started = False
    maintenance_tasks = [task for node in nodes for task in node._tasks]
    for node in nodes:
        node._tasks.clear()
    for task in maintenance_tasks:
        task.cancel()
    if maintenance_tasks:
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                asyncio.gather(*maintenance_tasks, return_exceptions=True), timeout=2.0
            )
    for node in nodes:
        node.known_refs = {(ref.name, ref.node_id): ref for ref in refs}
        index = ordered.index(node.ring_descriptor)
        node.predecessor = ordered[(index - 1) % len(ordered)]
        node.successor = successor_for(node.node_id + 1, refs, node.bits)
        node.successor_list = [
            ordered[(index + offset) % len(ordered)]
            for offset in range(1, min(node.max_successor_replicas, len(refs) - 1) + 1)
        ]
        if not node.successor_list:
            node.successor_list = [node.self_ref]
        node.fingers = [
            {
                "index": finger_index,
                "start": (node.node_id + (1 << finger_index)) % node.ring_size,
                "node": successor_for(node.node_id + (1 << finger_index), refs, node.bits),
            }
            for finger_index in range(node.bits)
        ]
        node._started = True
    if not ring_is_stable(nodes):
        raise AssertionError("benchmark oracle did not produce a stable ring")


async def wait_for_ring(nodes: list[Node], seconds: float = 30.0) -> None:
    deadline = time.monotonic() + seconds
    stable_rounds = 0
    while time.monotonic() < deadline:
        for node in nodes:
            await node.stabilize()
            await node.refresh_successor_list()
            for _ in range(min(8, node.bits)):
                await node.refresh_one_finger()
        if ring_is_stable(nodes):
            stable_rounds += 1
            if stable_rounds >= 2:
                await asyncio.sleep(0.2)
                if ring_is_stable(nodes):
                    return
        else:
            stable_rounds = 0
        await asyncio.sleep(0.1)
    raise TimeoutError("ring did not stabilize to the oracle state")


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    position = max(0, min(len(ordered) - 1, int(len(ordered) * fraction) - 1))
    return ordered[position]


async def run(node_count: int, queries: int, bits: int) -> dict[str, object]:
    planned_ids = [hash_id(f"node:node-{index}", bits) for index in range(node_count)]
    if len(set(planned_ids)) != len(planned_ids):
        raise ValueError(
            f"node names collide in a {bits}-bit identifier space; use a larger --bits value"
        )
    with tempfile.TemporaryDirectory(prefix="mini-dht-benchmark-") as temp:
        root = Path(temp)
        nodes: list[Node] = []
        try:
            for index in range(node_count):
                bootstrap = [nodes[0].ring_descriptor] if nodes else []
                node = Node(
                    f"node-{index}",
                    host="127.0.0.1",
                    port=0,
                    advertised_host="127.0.0.1",
                    storage_dir=root / f"node-{index}",
                    bootstrap=bootstrap,
                    bits=bits,
                    successor_replicas=min(3, node_count),
                )
                await node.start(register=False)
                nodes.append(node)
            await prime_stable_ring(nodes)
            rng = random.Random(20260920)
            keys = [rng.randrange(1 << bits) for _ in range(queries)]
            finger_times: list[float] = []
            linear_times: list[float] = []
            finger_hops: list[int] = []
            linear_hops: list[int] = []
            errors = 0
            for key in keys:
                source = nodes[key % len(nodes)]
                try:
                    started = time.perf_counter()
                    finger_result = await source.find_successor(key)
                    expected = successor_for(key, [node.ring_descriptor for node in nodes], bits)
                    actual = NodeRef.from_dict(finger_result["node"])
                    if not _same_identity(actual, expected):
                        raise AssertionError(
                            f"finger lookup returned {actual.name} for key {key}, expected {expected.name}"
                        )
                    finger_times.append((time.perf_counter() - started) * 1000)
                    finger_hops.append(int(finger_result["hops"]))
                    started = time.perf_counter()
                    linear_result = await source.rpc(
                        source.self_ref,
                        {"op": "FIND_SUCCESSOR_LINEAR", "key": key, "hop": 0, "trace": []},
                    )
                    linear_times.append((time.perf_counter() - started) * 1000)
                    linear_hops.append(int(linear_result["hops"]))
                except (OSError, asyncio.TimeoutError, RemoteError):
                    errors += 1
            if not finger_times or not linear_times:
                raise RuntimeError("benchmark produced no successful lookup samples")
            return {
                "nodes": node_count,
                "queries": queries,
                "errors": errors,
                "error_rate": errors / queries,
                "finger": {
                    "avg_ms": statistics.mean(finger_times),
                    "p95_ms": percentile(finger_times, 0.95),
                    "avg_hops": statistics.mean(finger_hops),
                    "max_hops": max(finger_hops),
                },
                "successor_baseline": {
                    "avg_ms": statistics.mean(linear_times),
                    "p95_ms": percentile(linear_times, 0.95),
                    "avg_hops": statistics.mean(linear_hops),
                    "max_hops": max(linear_hops),
                },
            }
        finally:
            for node in reversed(nodes):
                await node.stop()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, nargs="+", default=[4, 8, 16, 32])
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--bits", type=int, default=32)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    results = [
        {"run": run_index, **asyncio.run(run(count, args.queries, args.bits))}
        for run_index in range(1, args.runs + 1)
        for count in args.nodes
    ]
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
