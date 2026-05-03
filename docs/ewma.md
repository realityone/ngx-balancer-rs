# How ingress-nginx's EWMA balancer works

It's a **Peak-EWMA + Power-of-Two-Choices** balancer, ported in spirit from Twitter Finagle's `PeakEwma`. Each endpoint carries a single floating-point "score" — an exponentially-weighted moving average of recent round-trip time — and on every request the balancer samples 2 random endpoints and routes to the lower-scored one.

## The score: time-decayed RTT EWMA

`decay_ewma` (line 56) is the heart of it:

```
td     = now - last_touched_at
weight = exp(-td / DECAY_TIME)         -- DECAY_TIME = 10s
ewma   = ewma * weight + rtt * (1 - weight)
```

This is a continuous-time EWMA with a 10-second time constant:

- If a sample arrives **immediately** after the last one (`td≈0`), `weight≈1` → the old EWMA dominates and the new RTT barely moves it.
- If a sample arrives **long after** (`td >> 10s`), `weight≈0` → the EWMA snaps to the latest RTT, forgetting old history.
- An endpoint that hasn't been hit in a while has its score *automatically decay toward the latest sample*, even without explicit ticks. Decay happens lazily, on read.

The "peak" character comes from the fact that one slow request immediately pulls the score up; subsequent fast requests pull it back down only as fast as the decay allows. So a misbehaving backend gets penalized quickly and rehabilitated slowly — exactly what you want for shedding load away from a struggling instance.

## Shared state

Two `lua_shared_dict`s, keyed by `address:port`:

- `balancer_ewma` — current EWMA score.
- `balancer_ewma_last_touched_at` — `ngx.now()` of the last update, needed to compute `td`.

A `resty.lock` (`balancer_ewma_locks`) serializes RMW updates per upstream. Crucially the lock is configured with `timeout = 0` (line 29) — workers that can't grab it instantly **don't block**; they fall through and use the (possibly stale) read value. The `exptime = 0.1` is a safety net so a crashed holder can't wedge the lock for long. Picking a victim is read-mostly, so contention is rare and brief; the only writer per request is `after_balance`.

## Pick path: `_M.balance` (line 177)

1. Pull `tried_endpoints` from `ngx.ctx` (per-request retry memory) and filter the candidate list, so a `proxy_next_upstream` retry won't re-pick a peer that already failed this request. If everything's been tried, fall back to the full set with a warning (line 203).
2. Run **power-of-two-choices**:
   - `shuffle_peers(filtered_peers, k)` — partial Fisher-Yates, just enough to randomize the first `k` slots (`PICK_SET_SIZE = 2`).
   - `pick_and_score` — read scores for those `k` candidates and return the lowest.
3. Mark the winner tried and return `address:port`. Stash the score in `$balancer_ewma_score` for logging.

P2C with EWMA is the standard combination: random sampling avoids the herd effect of "everyone picks the globally best endpoint at once" while still strongly preferring fast peers.

The reads in `score()` call `get_or_update_ewma(name, 0, false)` — `update=false` means it computes the decayed value (so a stale endpoint isn't unfairly punished by a long-ago score) but doesn't take the lock or persist. The `rtt=0` argument is harmless because `update=false` short-circuits before the value is stored.

## Update path: `_M.after_balance` (line 222)

Runs in NGINX's `log_phase` after the upstream finishes. Reads `$upstream_connect_time` + `$upstream_response_time` (using `get_last_value` because those vars are comma-joined across retries — only the last attempt's number is the real one), sums them into `rtt`, and calls `get_or_update_ewma(upstream, rtt, true)`:

1. `lock(upstream)` — non-blocking; if it fails, the function still returns the read-only decayed value but skips the write. No retry; one missed update per worker per upstream is acceptable.
2. Recompute decayed EWMA with the new `rtt`.
3. `store_stats` writes both shared dicts.
4. `unlock`.

`forcible` warnings (lines 70, 78) fire when the shared dict evicts a valid entry under memory pressure — operator's signal to size `lua_shared_dict balancer_ewma{,_last_touched_at}` larger.

## Endpoint churn: slow start (`_M.sync`, line 235)

When the controller re-syncs and adds new endpoints, they have no history. Seeding them with `0` would make P2C *always* pick the new ones (lowest score wins), instantly hammering them. Instead, `calculate_slow_start_ewma` averages the existing endpoints' EWMAs and seeds new ones with that mean. New endpoints start "average," not "free," and their real score takes over after a few requests.

Removed endpoints have their entries explicitly deleted so the dict doesn't accumulate stale keys.

## Quick mental model

- Score = "recent latency, with a 10-second memory."
- Pick = "randomly look at 2 peers, take the faster one."
- Update = "after the request completes, fold the observed RTT into the chosen peer's score."
- New peers inherit the fleet average, not zero.
- Retries within a request avoid already-tried peers.
- All RMW is best-effort under a non-blocking per-upstream lock — losing an update is preferred over blocking the worker.

Sources of complexity worth knowing if you're porting this: the lazy decay (you must store *and* read `last_touched_at`), the `update=false` read path that computes but doesn't persist, the per-request `tried_endpoints` table, and the slow-start seeding for new endpoints.
