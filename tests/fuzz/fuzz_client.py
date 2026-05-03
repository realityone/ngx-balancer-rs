#!/usr/bin/env python3
"""Concurrent randomized HTTP client for the balancer_rs fuzz harness.

Drives `FUZZ_CLIENTS` asyncio tasks against a target nginx for
`FUZZ_DURATION` seconds. Each task picks a random method, path
(under `/lc/` or `/ewma/`), body size, and Connection header per
request. All exceptions are caught and bucketed — no failure mode
should kill a worker.

Stdlib only: `asyncio`, `argparse`, `random`, plus `socket` /
plain BSD streams (no `aiohttp`, which isn't installed).
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


POLICIES = ("lc", "ewma")


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
    # 0-4 KiB of opaque bytes.
    n = rng.randint(0, 4096)
    return bytes(rng.getrandbits(8) for _ in range(n))


def build_request(host: str, port: int, path: str, rng: random.Random) -> bytes:
    method = random_method(rng)
    keepalive = rng.random() < 0.30
    body = random_body(rng) if method == "POST" else b""

    lines = [
        f"{method} {path} HTTP/1.1".encode(),
        f"Host: {host}:{port}".encode(),
        f"Connection: {'keep-alive' if keepalive else 'close'}".encode(),
        b"User-Agent: balancer_rs-fuzz/1",
    ]
    if method == "POST":
        lines.append(f"Content-Length: {len(body)}".encode())
        lines.append(b"Content-Type: application/octet-stream")
    return b"\r\n".join(lines) + b"\r\n\r\n" + body


def classify_status(line: bytes) -> str:
    # b"HTTP/1.1 200 OK\r\n"
    parts = line.split(b" ", 2)
    if len(parts) < 2 or not parts[1].isdigit():
        return "protocol_error"
    code = int(parts[1])
    if 200 <= code < 300:
        return "2xx"
    if 300 <= code < 400:
        return "3xx"
    if 400 <= code < 500:
        return "4xx"
    if 500 <= code < 600:
        return "5xx"
    return "protocol_error"


async def one_request(host: str, port: int, path: str, rng: random.Random,
                      timeout: float) -> str:
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout)
    except (asyncio.TimeoutError, ConnectionError, OSError):
        return "connect_error"

    try:
        writer.write(build_request(host, port, path, rng))
        await asyncio.wait_for(writer.drain(), timeout=timeout)
        try:
            status_line = await asyncio.wait_for(
                reader.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            return "timeout"
        if not status_line:
            return "protocol_error"

        outcome = classify_status(status_line)

        # Drain the body — some bugs only show up when nginx finishes
        # streaming. Cap the drain at `timeout` total.
        try:
            await asyncio.wait_for(reader.read(-1), timeout=timeout)
        except asyncio.TimeoutError:
            return "timeout"
        return outcome
    except (ConnectionError, OSError):
        return "protocol_error"
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except (ConnectionError, OSError):
            pass


async def worker(idx: int, host: str, port: int, base_seed: int,
                 stats: Stats, timeout: float) -> None:
    rng = random.Random(base_seed ^ (idx * 0x9E37_79B9))
    while time.monotonic() < stats.deadline:
        policy, path = random_policy_path(rng)
        try:
            outcome = await one_request(host, port, path, rng, timeout)
        except Exception as e:  # noqa: BLE001 — swallow everything
            stats.counters[policy][f"unexpected:{type(e).__name__}"] += 1
            continue
        stats.counters[policy][outcome] += 1


async def main_async(args: argparse.Namespace) -> int:
    target = args.target
    if target.startswith("http://"):
        target = target[len("http://"):]
    if ":" in target:
        host, port_str = target.rsplit(":", 1)
        port = int(port_str)
    else:
        host, port = target, 80

    base_seed = args.seed if args.seed else int(time.time() * 1000) & 0xFFFFFFFF
    stats = Stats(
        counters={p: Counter() for p in POLICIES},
        deadline=time.monotonic() + args.duration,
    )

    print(f"fuzz_client: host={host} port={port} clients={args.clients} "
          f"duration={args.duration}s seed={base_seed}", flush=True)

    workers = [
        asyncio.create_task(worker(i, host, port, base_seed, stats, args.timeout))
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
