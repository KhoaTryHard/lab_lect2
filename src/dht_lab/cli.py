"""Command-line entry point for nodes, clients and local diagnostics."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any

from .chord import hash_id
from .client import DhtClient, parse_endpoints
from .node import Node


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Mini-DHT registry and messaging lab")
    parser.add_argument("--verbose", action="store_true", help="enable debug logging")
    subparsers = parser.add_subparsers(dest="command", required=True)

    node = subparsers.add_parser("node", help="run or inspect a DHT node")
    node_sub = node.add_subparsers(dest="node_command", required=True)
    start = node_sub.add_parser("start")
    start.add_argument("--name", default=os.getenv("NODE_NAME"))
    start.add_argument("--host", default=os.getenv("BIND_HOST", "0.0.0.0"))
    start.add_argument("--advertised-host", default=os.getenv("ADVERTISED_HOST"))
    start.add_argument("--port", type=int, default=int(os.getenv("NODE_PORT", "7000")))
    start.add_argument("--data-dir", default=os.getenv("DATA_DIR", "data"))
    start.add_argument("--bootstrap", default=os.getenv("BOOTSTRAP", ""))
    start.add_argument("--bits", type=int, default=int(os.getenv("DHT_BITS", "32")))
    start.add_argument("--replicas", type=int, default=int(os.getenv("DHT_REPLICAS", "3")))

    inspect = node_sub.add_parser("inspect")
    inspect.add_argument("--seed", required=True, help="node endpoint or comma-separated endpoints")
    inspect.add_argument("--messages", action="store_true", help="show inbox messages")

    client = subparsers.add_parser("client", help="use the DHT from outside the node ring")
    client_sub = client.add_subparsers(dest="client_command", required=True)
    for command in ("lookup", "send", "register", "status"):
        command_parser = client_sub.add_parser(command)
        command_parser.add_argument("--seed", required=True, help="comma-separated host:port endpoints")
        if command in {"lookup", "send"}:
            command_parser.add_argument("name")
        if command == "send":
            command_parser.add_argument("--from", dest="sender", required=True)
            command_parser.add_argument("--message", required=True)
            command_parser.add_argument("--message-id")
            command_parser.add_argument("--timeout", type=float, default=30.0)
        if command == "register":
            command_parser.add_argument("name")
            command_parser.add_argument("--host", required=True)
            command_parser.add_argument("--port", type=int, required=True)
            command_parser.add_argument("--node-id", type=int, required=True)
            command_parser.add_argument("--revision", type=int, default=1)
    return parser


async def run_node(args: argparse.Namespace) -> None:
    if not args.name:
        raise SystemExit("node start requires --name or NODE_NAME")
    node = Node(
        name=args.name,
        host=args.host,
        port=args.port,
        advertised_host=args.advertised_host,
        storage_dir=args.data_dir,
        bootstrap=parse_endpoints(args.bootstrap, args.port),
        bits=args.bits,
        successor_replicas=args.replicas,
    )
    await node.start()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    print(json.dumps({"ready": True, **node.status()}, ensure_ascii=False), flush=True)
    try:
        await stop_event.wait()
    finally:
        await node.stop()


async def run_client_command(args: argparse.Namespace) -> None:
    seeds = parse_endpoints(args.seed)
    client = DhtClient(seeds)
    try:
        await client.start()
        if args.client_command == "lookup":
            result = await client.lookup(args.name)
        elif args.client_command == "send":
            result = await client.send(
                args.name,
                args.sender,
                args.message,
                message_id=args.message_id,
                deadline=args.timeout,
            )
        elif args.client_command == "register":
            result = await client.register(
                args.name,
                args.host,
                args.port,
                args.node_id,
                revision=args.revision,
            )
        else:
            result = await client.status()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        await client.stop()


async def run_inspect(args: argparse.Namespace) -> None:
    seeds = parse_endpoints(args.seed)
    client = DhtClient(seeds)
    try:
        await client.start()
        if args.messages:
            result = await client.messages()
        else:
            result = await client.status()
        print(json.dumps(result, ensure_ascii=False, indent=2))
    finally:
        await client.stop()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        if args.command == "node" and args.node_command == "start":
            asyncio.run(run_node(args))
        elif args.command == "node" and args.node_command == "inspect":
            asyncio.run(run_inspect(args))
        elif args.command == "client":
            asyncio.run(run_client_command(args))
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
