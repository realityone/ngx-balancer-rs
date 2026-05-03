#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["aiohttp>=3.9"]
# ///
"""Concurrent randomized HTTP client for the balancer_rs fuzz harness.

Drives `FUZZ_CLIENTS` asyncio tasks against a target nginx for
`FUZZ_DURATION` seconds. Each task picks a random method, path
(under `/lc/` or `/ewma/`), body size, and Connection header per
request. All exceptions are caught and bucketed — no failure mode
should kill a worker.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import string
import sys
import time
from collections import Counter
from dataclasses import dataclass

import aiohttp


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--target", default="127.0.0.1:9080",
                   help="host:port of the nginx server")
    p.add_argument("--duration", type=float, default=60.0,
                   help="seconds to drive the load")
    p.add_argument("--clients", type=int, default=32,
                   help="concurrent worker tasks")
    p.add_argument("--seed", type=int, default=0,
                   help="rng seed (0 = time-based)")
    p.add_argument("--timeout", type=float, default=8.0,
                   help="per-request read timeout (seconds)")
    return p.parse_args()


POLICIES = ("lc", "ewma", "rr")


@dataclass
class Stats:
    counters: dict[str, Counter]
    deadline: float


def random_policy_path(rng: random.Random) -> tuple[str, str]:
    policy = rng.choice(POLICIES)
    n = rng.randint(0, 24)
    tail = "".join(rng.choice(string.ascii_letters + string.digits + "/-_")
                   for _ in range(n))
    return policy, f"/{policy}/{tail}"


def random_method(rng: random.Random) -> str:
    r = rng.random()
    if r < 0.80:
        return "GET"
    if r < 0.95:
        return "POST"
    return "HEAD"


def random_body(rng: random.Random) -> bytes:
    n = rng.randint(0, 4096)
    return bytes(rng.getrandbits(8) for _ in range(n))


def classify_status(code: int) -> str:
    if 200 <= code < 300:
        return "2xx"
    if 300 <= code < 400:
        return "3xx"
    if 400 <= code < 500:
        return "4xx"
    if 500 <= code < 600:
        return "5xx"
    return "protocol_error"


async def one_request(session: aiohttp.ClientSession, base: str,
                      rng: random.Random, timeout: float) -> tuple[str, str]:
    policy, path = random_policy_path(rng)
    method = random_method(rng)
    keepalive = rng.random() < 0.30
    headers = {
        "User-Agent": "balancer_rs-fuzz/1",
        "Connection": "keep-alive" if keepalive else "close",
    }
    data = random_body(rng) if method == "POST" else None
    try:
        async with session.request(
            method,
            base + path,
            data=data,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=timeout),
        ) as resp:
            outcome = classify_status(resp.status)
            # Drain — some bugs only show up after nginx finishes streaming.
            await resp.read()
            return policy, outcome
    except asyncio.TimeoutError:
        return policy, "timeout"
    except aiohttp.ClientConnectorError:
        return policy, "connect_error"
    except aiohttp.ClientError:
        return policy, "protocol_error"


async def worker(idx: int, base: str, base_seed: int,
                 stats: Stats, timeout: float,
                 session: aiohttp.ClientSession) -> None:
    rng = random.Random(base_seed ^ (idx * 0x9E37_79B9))
    while time.monotonic() < stats.deadline:
        try:
            policy, outcome = await one_request(session, base, rng, timeout)
        except Exception as e:  # noqa: BLE001 — swallow everything
            stats.counters[POLICIES[0]][f"unexpected:{type(e).__name__}"] += 1
            continue
        stats.counters[policy][outcome] += 1


async def main_async(args: argparse.Namespace) -> int:
    target = args.target
    if target.startswith("http://"):
        target = target[len("http://"):]
    base = f"http://{target}"

    base_seed = args.seed if args.seed else int(time.time() * 1000) & 0xFFFFFFFF
    stats = Stats(
        counters={p: Counter() for p in POLICIES},
        deadline=time.monotonic() + args.duration,
    )

    print(f"fuzz_client: target={target} clients={args.clients} "
          f"duration={args.duration}s seed={base_seed}", flush=True)

    async with aiohttp.ClientSession() as session:
        workers = [
            asyncio.create_task(
                worker(i, base, base_seed, stats, args.timeout, session))
            for i in range(args.clients)
        ]
        await asyncio.gather(*workers, return_exceptions=True)

    grand_total = sum(sum(c.values()) for c in stats.counters.values())
    print("fuzz_client: summary")
    if grand_total == 0:
        print("  (no requests completed)")
        return 1

    print_summary(stats.counters, grand_total)
    return 0


def print_summary(counters: dict[str, Counter], grand_total: int) -> None:
    all_keys = sorted(set().union(*(c.keys() for c in counters.values())))
    label_w = max(len(k) for k in all_keys + ["total"])
    col_w = 10  # right-aligned width for each numeric column

    header = " " * (label_w + 2) + "".join(f"{p:>{col_w}}" for p in POLICIES) \
        + f"{'total':>{col_w}}"
    print(header)

    for key in all_keys:
        row = f"  {key:<{label_w}}"
        row_total = 0
        for p in POLICIES:
            n = counters[p].get(key, 0)
            row_total += n
            row += f"{n:>{col_w}}"
        row += f"{row_total:>{col_w}}"
        print(row)

    totals = f"  {'total':<{label_w}}"
    for p in POLICIES:
        totals += f"{sum(counters[p].values()):>{col_w}}"
    totals += f"{grand_total:>{col_w}}"
    print(totals)


def main() -> int:
    args = parse_args()
    try:
        return asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
