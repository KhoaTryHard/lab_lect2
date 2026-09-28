"""Wait until all configured seed nodes answer and report a non-trivial ring."""

from __future__ import annotations

import argparse
import asyncio
import time

from dht_lab.client import DhtClient, parse_endpoints


async def wait_for_ring(seeds_text: str, timeout: float, stable_rounds: int) -> None:
    seeds = parse_endpoints(seeds_text)
    client = DhtClient(seeds)
    await client.start()
    try:
        deadline = time.monotonic() + timeout
        consecutive = 0
        while time.monotonic() < deadline:
            statuses = []
            for seed in seeds:
                try:
                    statuses.append(await client.rpc(seed, {"op": "STATUS"}, timeout=1.0))
                except Exception:
                    statuses = []
                    break
            names = {status.get("name") for status in statuses}
            ready = (
                len(statuses) == len(seeds)
                and len(names) == len(seeds)
                and all(status.get("successor") for status in statuses)
                and all(status.get("fingers") for status in statuses)
            )
            if ready:
                consecutive += 1
                if consecutive >= stable_rounds:
                    return
            else:
                consecutive = 0
            await asyncio.sleep(0.5)
    finally:
        await client.stop()
    raise TimeoutError(f"ring did not become ready within {timeout:g}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, help="comma-separated seed endpoints")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--stable-rounds", type=int, default=2)
    args = parser.parse_args()
    asyncio.run(wait_for_ring(args.seed, args.timeout, args.stable_rounds))
    print("ring-ready")


if __name__ == "__main__":
    main()
