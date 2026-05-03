#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# dependencies = ["aiohttp>=3.9"]
# ///
"""Chaos HTTP backend for the balancer_rs fuzz harness.

Listens on --port; on each request, rolls a die against the
weights below and replies (or misbehaves) accordingly. Mischief
is reproducible via --seed. With --mode=stable, the backend
always returns an instant 200 OK — useful as a control peer in
the upstream pool so the policies have a known-good endpoint to
prefer.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys

from aiohttp import web


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--mode", choices=("chaos", "stable"), default="chaos",
                   help="chaos: dice-rolled misbehavior (default); "
                        "stable: always instant 200 OK")
    return p.parse_args()


def make_stable_handler(port: int):
    headers_stable = {"X-Port": str(port), "X-Backend": "stable"}

    async def handle(request: web.Request) -> web.Response:
        # Drain the request body so nginx isn't blocked sending it.
        await request.read()
        await asyncio.sleep(0.180)
        return web.Response(text="OK", headers=headers_stable)

    return handle


def make_chaos_handler(port: int, rng: random.Random):
    headers_ok = {"X-Port": str(port), "X-Backend": "ok"}
    headers_slow = {"X-Port": str(port), "X-Backend": "ok-slow"}
    headers_502 = {"X-Port": str(port), "X-Backend": "502"}

    async def handle(request: web.Request) -> web.Response:
        # Roll synchronously at handler entry so the seed → outcome
        # mapping is fixed by request-arrival order.
        r = rng.random()
        slow_jitter = rng.random() if 0.73 <= r < 0.88 else 0.0

        # Drain the request body so nginx isn't blocked sending it.
        await request.read()

        if r < 0.73:
            return web.Response(text="OK", headers=headers_ok)
        if r < 0.88:
            await asyncio.sleep(0.05 + slow_jitter * 0.45)
            return web.Response(text="OK", headers=headers_slow)
        if r < 0.98:
            return web.Response(status=502, text="bad-gtwy", headers=headers_502)
        # Hang past nginx's proxy_read_timeout (5s in the fuzz config).
        await asyncio.sleep(30)
        return web.Response(status=504, text="hung")

    return handle


def main() -> int:
    args = parse_args()
    if args.mode == "stable":
        handler = make_stable_handler(args.port)
    else:
        # XOR the global seed with the port so each chaos backend
        # gets a distinct but reproducible stream of decisions.
        rng = random.Random(args.seed ^ args.port)
        handler = make_chaos_handler(args.port, rng)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)

    print(
        f"chaos_backend: listening on 127.0.0.1:{args.port} "
        f"(mode={args.mode}, seed={args.seed})",
        file=sys.stderr,
        flush=True,
    )
    try:
        web.run_app(
            app,
            host="127.0.0.1",
            port=args.port,
            print=None,
            access_log=None,
            backlog=64,
        )
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
