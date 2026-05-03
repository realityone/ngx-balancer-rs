#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""Chaos HTTP backend for the balancer_rs fuzz harness.

Listens on --port; on each request, rolls a die against the
weights below and replies (or misbehaves) accordingly. Mischief
is reproducible via --seed. With --mode=stable, the backend
always returns 200 OK after a fixed small latency — useful as a
control peer in the upstream pool so the policies have a
known-good endpoint to prefer.

Stays on raw `asyncio.start_server` rather than aiohttp because
two of the chaos branches (partial-header close, partial-body
close) require sending deliberately malformed HTTP framing,
which aiohttp's response writer is designed to prevent.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--port", type=int, required=True)
    p.add_argument("--seed", type=int, default=0)
    p.add_argument("--mode", choices=("chaos", "stable"), default="chaos",
                   help="chaos: dice-rolled misbehavior (default); "
                        "stable: always 200 OK after a fixed small latency")
    return p.parse_args()


async def read_headers(reader: asyncio.StreamReader) -> None:
    while True:
        line = await reader.readline()
        if not line or line in (b"\r\n", b"\n"):
            return


def ok_response(port: int) -> bytes:
    body = b"OK"
    return (
        b"HTTP/1.1 200 OK\r\n"
        b"Connection: close\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"X-Port: " + str(port).encode() + b"\r\n"
        b"X-Backend: ok\r\n\r\n" + body
    )


def bad_gateway_response(port: int) -> bytes:
    body = b"bad-gtwy"
    return (
        b"HTTP/1.1 502 Bad Gateway\r\n"
        b"Connection: close\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"X-Port: " + str(port).encode() + b"\r\n"
        b"X-Backend: 502\r\n\r\n" + body
    )


def stable_response(port: int) -> bytes:
    body = b"OK"
    return (
        b"HTTP/1.1 200 OK\r\n"
        b"Connection: close\r\n"
        b"Content-Length: " + str(len(body)).encode() + b"\r\n"
        b"X-Port: " + str(port).encode() + b"\r\n"
        b"X-Backend: stable\r\n\r\n" + body
    )


def make_stable_handler(port: int):
    response = stable_response(port)

    async def handle(reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter) -> None:
        try:
            try:
                await read_headers(reader)
            except (ConnectionError, OSError):
                return
            try:
                # Fixed small latency so EWMA can see the stable peer
                # as "fast" relative to the slower chaos peers without
                # also being instant — closer to a real backend.
                await asyncio.sleep(0.180)
                writer.write(response)
                await writer.drain()
            except (ConnectionError, OSError, BrokenPipeError):
                return
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    return handle


def make_chaos_handler(port: int, rng: random.Random):
    async def handle(reader: asyncio.StreamReader,
                     writer: asyncio.StreamWriter) -> None:
        # Roll synchronously at handler entry so the seed → outcome
        # mapping is fixed by connection-arrival order, matching the
        # serial-accept Perl original even though we now handle
        # connections concurrently.
        r = rng.random()
        slow_jitter = rng.random() if 0.60 <= r < 0.75 else 0.0
        try:
            try:
                await read_headers(reader)
            except (ConnectionError, OSError):
                return

            try:
                if r < 0.60:
                    writer.write(ok_response(port))
                elif r < 0.75:
                    await asyncio.sleep(0.05 + slow_jitter * 0.45)
                    writer.write(ok_response(port))
                elif r < 0.85:
                    writer.write(bad_gateway_response(port))
                elif r < 0.93:
                    # Partial header send, then close.
                    writer.write(b"HTTP/1.1 200 OK\r\nServer: chaos\r\nX-Po")
                elif r < 0.98:
                    # Full headers, partial body, then close.
                    writer.write(
                        b"HTTP/1.1 200 OK\r\n"
                        b"Connection: close\r\n"
                        b"Content-Length: 4096\r\n"
                        b"X-Port: " + str(port).encode() + b"\r\n"
                        b"X-Backend: trunc\r\n\r\n"
                        + b"x" * 12
                    )
                else:
                    # Hang past nginx's proxy_read_timeout (5s in fuzz config).
                    await asyncio.sleep(30)
                await writer.drain()
            except (ConnectionError, OSError, BrokenPipeError):
                return
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    return handle


async def main_async(args: argparse.Namespace) -> None:
    if args.mode == "stable":
        handler = make_stable_handler(args.port)
    else:
        # XOR the global seed with the port so each chaos backend
        # gets a distinct but reproducible stream of decisions.
        rng = random.Random(args.seed ^ args.port)
        handler = make_chaos_handler(args.port, rng)

    server = await asyncio.start_server(
        handler,
        host="127.0.0.1",
        port=args.port,
        backlog=64,
        reuse_address=True,
    )
    print(
        f"chaos_backend: listening on 127.0.0.1:{args.port} "
        f"(mode={args.mode}, seed={args.seed})",
        file=sys.stderr,
        flush=True,
    )
    async with server:
        await server.serve_forever()


def main() -> int:
    args = parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
