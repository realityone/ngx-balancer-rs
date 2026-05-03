# `balancer_rs` chaos / fuzz harness

Long-running stress test: live nginx loaded with both
`balancer_rs least_conn` and `balancer_rs ewma`, hammered with
concurrent randomized HTTP requests while the upstream backends
misbehave on purpose. Watches for crashes, sanitizer hits, and
fatal markers in the nginx error log.

This isn't coverage-guided fuzzing of nginx itself — that would
require rebuilding nginx with AFL/honggfuzz instrumentation. It
*is* effective at finding bugs in the policy code paths that only
appear under concurrent load + chaotic upstream behavior:
crashes in `peer.get` / `peer.free`, deadlocks in the backup
fallback, and inconsistent `peer.refs` / `peer.conns` accounting.

## Run

```bash
tests/fuzz/run.sh                                  # default 60s, 32 clients
FUZZ_DURATION=10 tests/fuzz/run.sh                 # quick smoke
FUZZ_DURATION=600 FUZZ_CLIENTS=64 tests/fuzz/run.sh   # soak run
FUZZ_SEED=12345 tests/fuzz/run.sh                  # reproducible
FUZZ_KEEP=1 tests/fuzz/run.sh                      # don't rm the prefix on success
```

| env var          | default       | meaning                                   |
| ---------------- | ------------- | ----------------------------------------- |
| `FUZZ_DURATION`  | `60`          | seconds to drive the load                 |
| `FUZZ_CLIENTS`   | `32`          | concurrent client coroutines              |
| `FUZZ_SEED`      | `$(date +%s)` | rng seed (reproducible chaos when fixed)  |
| `FUZZ_KEEP`      | unset         | keep `target/fuzz-prefix` on success      |

The harness writes everything to `target/fuzz-prefix/` (nginx
prefix, pid file, error/access logs, backend stderr).

## Prerequisites

[`uv`](https://docs.astral.sh/uv/) is required — `chaos_backend.py`
and `fuzz_client.py` declare aiohttp via PEP 723 inline metadata
and `run.sh` invokes them with `uv run --script`, which provisions
a transient env on first run. Install with:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

`run.sh` looks for `uv` on `$PATH` and falls back to
`~/.local/bin/uv` (where the installer puts it).

## Pieces

- **`run.sh`** — entry point. Builds the module, locates the
  vendored nginx, spawns 5 chaos backends (ports 9081–9085), starts
  nginx, drives the fuzz client, then checks the error log and
  reports pass/fail. Reuses the build/discovery logic from
  `tests/run.sh`.
- **`nginx.conf`** — three upstreams sharing the same peer set
  (4 primaries + 1 backup): `u_lc` with `balancer_rs least_conn`,
  `u_ewma` with `balancer_rs ewma`, and `u_rr` with no
  `balancer_rs` directive (stock nginx round-robin, the control
  column in the summary). Path-routed via `/lc/`, `/ewma/`, and
  `/rr/`. Aggressive `proxy_*_timeout` so chaos hangs surface
  quickly. `proxy_next_upstream` set so failures exercise the
  retry path.
- **`chaos_backend.py`** — raw-asyncio randomized HTTP listener
  seeded per port. Each request rolls a die: 60% instant 200, 15%
  slow 200 (50–500 ms), 10% 502, 8% partial-header close, 5%
  partial-body close, 2% sleep 30 s (forces nginx upstream
  timeout). Stays on `asyncio.start_server` rather than aiohttp so
  it can emit deliberately malformed framing — aiohttp's response
  writer is designed to prevent that.
- **`fuzz_client.py`** — aiohttp client. Spawns `FUZZ_CLIENTS`
  workers; each loops, picking random method (GET/POST/HEAD),
  random path under `/lc/` or `/ewma/`, random body, random
  Connection header, with a per-request timeout. Catches every
  exception; prints a counter summary at exit.

## Pass criteria

`run.sh` exits 0 iff:

- nginx master was alive at the end of the run,
- the error log contains no fatal markers
  (`[alert]`/`[emerg]`/`SIGSEGV`/`SIGABRT`/`panic`/`sanitizer`/
  `assertion failed`/`core dumped`),
- and the fuzz client made at least one request (`fuzz_client.py`
  exits 0).

`[error]` lines are *expected* — they're how nginx reports the
upstream timeouts / connection-refused / 502s our chaos backends
produce on purpose.

## A note on EWMA results in this harness

It's tempting to read the per-policy summary as a benchmark and
conclude "EWMA returned more 5xx than least_conn, so EWMA is worse."
That's not a fair read of this test. Three things conspire against
EWMA here:

1. **Sample size is small.** A 10–60 s smoke run with 32 clients
   produces a few hundred completed requests per policy; the
   variance on the 5xx / timeout counts is large enough that
   per-policy gaps flip on a different seed. Run a longer soak
   (`FUZZ_DURATION=120 FUZZ_CLIENTS=64`) and the numbers converge.

2. **The chaos backends are stateless misbehavers.** Each
   backend rolls the same dice on every request, independently.
   A backend that just returned an instant 200 has the same
   probability of failing on the next request — there is no
   signal in past behavior for EWMA to learn from. EWMA wins
   when peers have *persistent* characteristics (one pod under
   GC, one node slower than the others, one zone further away);
   under uniform chaos, uniform random distribution is optimal,
   which is closer to what least_conn produces with idle peers.

3. **We deliberately skip EWMA updates on `NGX_PEER_FAILED`**
   (`src/ewma.rs` `free_peer`). The reasoning is defensible: a
   connection-refused completes in ~1 ms, and folding 1 ms into
   the score would make a *dead* peer look like the *fastest*
   peer. The flip side, exposed here, is that a backend that
   returned a fast 200 once and then fails most of the time
   keeps its good-looking EWMA score forever. P2C keeps
   preferring it, each attempt eats one of the
   `proxy_next_upstream_tries` budget, and the client sees a 5xx
   once that budget runs out. Least_conn doesn't carry a quality
   score, so it doesn't have this failure mode.

What still protects both policies is the round_robin
`max_fails=1 fail_timeout=10s` quarantine — both policies honor
it via `peer_available` in `src/peer.rs`, so a peer that's
failed at least once is excluded for 10 s.

Two ways to make the harness more representative of the
workloads EWMA is *designed* for:

- **Differentiate the backends** (give 9081–9082 mostly-good
  rolls and 9084–9085 mostly-bad rolls). With persistent
  per-peer skew, EWMA's 2xx rate pulls ahead of least_conn
  because P2C learns to avoid the bad peers.
- **Fold failure RTTs into EWMA** with a constant penalty (e.g.
  `proxy_read_timeout`-worth of "RTT" on failure) instead of
  skipping the update. Closer to how ingress-nginx's Lua
  balancer works (it reads `$upstream_response_time` in
  `log_phase`, which is populated even on failed responses).

Both are deferred — the harness is honest about what it tests,
which is robustness under chaos rather than relative throughput.

## Out of scope

- AFL/honggfuzz coverage-guided fuzzing of nginx itself.
- AddressSanitizer / valgrind run modes (the vendored nginx is
  built with `--with-compat`, no sanitizer flags). To add ASAN
  coverage you'd need to rebuild nginx with `-fsanitize=address`
  and rebuild the `.so` with `RUSTFLAGS=-Zsanitizer=address`.
- TLS / HTTP/2 fuzzing — the policies don't touch protocol details.
