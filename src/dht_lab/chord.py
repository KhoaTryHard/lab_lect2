"""Pure Chord identifier and finger-table helpers.

The network-facing node implementation is in :mod:`dht_lab.node`; keeping the
ring mathematics here makes the boundary cases easy to test independently.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Iterable, Sequence


@dataclass(frozen=True, slots=True)
class NodeRef:
    """The stable identity and current network endpoint of a Chord node."""

    name: str
    node_id: int
    host: str
    port: int
    revision: int = 0

    def to_dict(self) -> dict[str, str | int]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict[str, object]) -> "NodeRef":
        return cls(
            name=str(value["name"]),
            node_id=int(value["node_id"]),
            host=str(value["host"]),
            port=int(value["port"]),
            revision=int(value.get("revision", 0)),
        )


def in_open_closed(value: int, start: int, end: int, ring_size: int) -> bool:
    """Return whether value is in the clockwise interval ``(start, end]``."""

    value %= ring_size
    start %= ring_size
    end %= ring_size
    if start < end:
        return start < value <= end
    if start > end:
        return value > start or value <= end
    return True


def in_open_open(value: int, start: int, end: int, ring_size: int) -> bool:
    """Return whether value is in the clockwise interval ``(start, end)``."""

    if value == end:
        return False
    return in_open_closed(value, start, end, ring_size)


def hash_id(value: str, bits: int = 32) -> int:
    """Hash a logical name into the Chord identifier space."""

    import hashlib

    if bits <= 0 or bits > 256:
        raise ValueError("bits must be in the range 1..256")
    digest = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(digest, "big") % (1 << bits)


def successor_for(key: int, nodes: Iterable[NodeRef], bits: int = 32) -> NodeRef:
    """Return the first node clockwise from key, using a sorted oracle list."""

    ring_size = 1 << bits
    ordered = sorted(nodes, key=lambda node: node.node_id)
    if not ordered:
        raise LookupError("cannot resolve a key on an empty ring")
    key %= ring_size
    return next((node for node in ordered if node.node_id >= key), ordered[0])


def build_finger_table(
    node: NodeRef, nodes: Sequence[NodeRef], bits: int = 32
) -> list[dict[str, int | str]]:
    """Build the m-entry finger table from a known stable ring.

    The live node only asks the network for each successor. This helper is the
    deterministic oracle used by unit tests and documentation examples.
    """

    ring_size = 1 << bits
    return [
        {
            "start": (node.node_id + (1 << index)) % ring_size,
            "node": successor_for(node.node_id + (1 << index), nodes, bits).to_dict(),
        }
        for index in range(bits)
    ]


def closest_preceding(
    node_id: int, target: int, candidates: Iterable[NodeRef], bits: int = 32
) -> NodeRef | None:
    """Choose the closest candidate strictly before target clockwise."""

    ring_size = 1 << bits
    eligible = [
        candidate
        for candidate in candidates
        if candidate.node_id != node_id
        and in_open_open(candidate.node_id, node_id, target, ring_size)
    ]
    if not eligible:
        return None
    return max(eligible, key=lambda candidate: (candidate.node_id - node_id) % ring_size)
