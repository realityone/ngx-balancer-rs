# ngx-balancer-rs

> Built end-to-end by coding agents as an AI-assisted experiment, and used for my own service. No warranties — use at your own risk.

NGINX HTTP upstream balancer written in Rust, packaged as a dynamic
module. Provides a single `balancer_rs <policy>;` directive inside an
`upstream { … }` block.

Currently supported policies:

| Policy       | Behavior |
| ------------ | -------- |
| `least_conn` | Picks the available peer with the fewest active connections, weighted. Matches stock nginx's `ngx_http_upstream_least_conn_module` (tie-break by weighted round-robin, primary→backup fallback, `down` / `max_fails` / `max_conns` honored). |
| `ewma`       | Peak-EWMA + Power-of-Two-Choices, modeled on ingress-nginx's Lua implementation. Each peer's score is an exponentially-weighted moving average of its observed RTT (10s decay constant); on every request we sample two random eligible peers and route to the lower score. Failed attempts (`NGX_PEER_FAILED`) are skipped from the update so a connection-refused doesn't make a dead peer look fast. See [`docs/ewma.md`](docs/ewma.md) for the algorithm in detail. |

## Build

```bash
cargo build --release
```

This builds:

- `target/release/libngx_balancer_rs.so` — the dynamic module
- A vendored nginx under `target/release/build/nginx-sys-*/out/objs/nginx`
  (downloaded and compiled by `nginx-sys`'s build script on first run)

The vendored nginx is built `--with-compat`, which is what lets the
externally-built `.so` load.

## Usage

Load the module and select the policy on an upstream block:

```nginx
load_module /path/to/libngx_balancer_rs.so;

events {}

http {
    upstream backend {
        balancer_rs least_conn;     # or: balancer_rs ewma;

        server 10.0.0.1:8080;
        server 10.0.0.2:8080 weight=2;
        server 10.0.0.3:8080 max_conns=100;
        server 10.0.0.4:8080 backup;
    }

    server {
        listen 80;
        location / {
            proxy_pass http://backend;
        }
    }
}
```

Both policies accept the same per-`server` parameters as stock
nginx's `least_conn`: `weight=`, `max_conns=`, `max_fails=`,
`fail_timeout=`, `down`, `backup`.

`balancer_rs` must appear **before** the `server` lines it should
govern, since flag parsing for `server` depends on the active
load-balancer.

## Test

```bash
tests/run.sh                            # full Test::Nginx suite
tests/run.sh tests/t/balancer_rs.t      # one .t file (extra args pass through)
```

The harness self-fetches the nginx-tests Perl library on first run and
injects `load_module` for the freshly-built `.so` automatically.
