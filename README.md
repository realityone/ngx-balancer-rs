# ngx-balancer-rs

NGINX HTTP upstream balancer written in Rust, packaged as a dynamic
module. Provides a single `balancer_rs <policy>;` directive inside an
`upstream { … }` block.

Currently supported policies:

| Policy       | Behavior |
| ------------ | -------- |
| `least_conn` | Picks the available peer with the fewest active connections, weighted. Matches stock nginx's `ngx_http_upstream_least_conn_module` (tie-break by weighted round-robin, primary→backup fallback, `down` / `max_fails` / `max_conns` honored). |

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
        balancer_rs least_conn;

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

The `balancer_rs least_conn` directive accepts the same per-`server`
parameters as the stock module: `weight=`, `max_conns=`, `max_fails=`,
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
