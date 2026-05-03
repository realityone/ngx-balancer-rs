# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust-implemented NGINX HTTP **upstream** module, packaged as a dynamic
module (`cdylib`). Adds an `upstream { ... }`-context directive
`balancer_rs <policy>;` — `least_conn` mirrors stock nginx's
`ngx_http_upstream_least_conn_module.c`, and `ewma` implements
Peak-EWMA + Power-of-Two-Choices in the spirit of ingress-nginx's
Lua module (see `docs/ewma.md`). Built against
[`ngx-rust`](https://github.com/nginx/ngx-rust) (`main` branch, `vendored`
feature — the build script downloads and compiles a full nginx from source).

## Common commands

```bash
cargo build                 # builds .so + vendored nginx (first run is slow)
cargo clippy --all-targets  # all + pedantic lints; project must stay clean
tests/run.sh                # full end-to-end: build + Test::Nginx prove run
tests/run.sh -v             # verbose prove output
tests/run.sh tests/t/balancer_rs.t  # single .t file (any prove args pass through)
```

`tests/run.sh` self-fetches `nginx-tests/lib` via sparse clone on first
invocation and globs for the vendored nginx binary at
`target/debug/build/nginx-sys-*/out/objs/nginx` (the hash changes when
cargo redoes dependency resolution).

## Architecture

The shape mirrors `ngx-rust`'s `examples/upstream.rs` — when in
doubt, that's the reference. A local checkout of ngx-rust sits at
`~/.cargo/git/checkouts/ngx-rust-*/` if you need to read its source.

- **`src/lib.rs`** — module plumbing: `ngx_module_t`, the
  `NGX_HTTP_BALANCER_RS_CTX` `ngx_http_module_t`, the `balancer_rs`
  command, and `ngx_http_balancer_rs_commands_set` (the directive
  callback). The callback parses the policy argument, sets
  `uscf.flags` to the accepted-server-parameter set (`weight=`,
  `max_conns=`, `down`, `backup`, …), and installs the policy's
  `init_upstream` on `uscf.peer`. `BalancerConfig.policy` is a
  `PolicyImpl` enum whose variants carry their own state — adding a
  new policy means adding a variant, not a side channel field.
- **`src/policy.rs`** — `BalancingPolicy` trait. One method:
  `init_upstream() -> ngx_http_upstream_init_pt`. Each policy impl
  returns its `peer.init_upstream` entry point.
- **`src/peer.rs`** — round-robin helpers shared by every policy
  (`PTR_BITS`, `peer_available`, `select_peer`, `peers_wlock` /
  `peers_wunlock`, `busy_with_primary_name`). Direct ports of
  fragments from `ngx_http_upstream_round_robin.c` /
  `ngx_http_upstream_least_conn_module.c`.
- **`src/least_conn.rs`** — first policy. `init_upstream` delegates
  to `ngx_http_upstream_init_round_robin` then patches `peer.init`;
  that per-request `init_peer` chains to
  `ngx_http_upstream_init_round_robin_peer` and only overrides
  `peer.get` (round_robin's `peer.free` keeps doing connection
  bookkeeping). Selector matches stock nginx including weighted-RR
  tie-break, primary→backup fallback, peers wlock for zone mode,
  config-generation staleness check, and `peer.refs++`.
- **`src/ewma.rs`** — second policy. Same `init_upstream` →
  `init_peer` shape, but **wraps** round_robin's per-request
  `rr_peer_data_t` in our own `EwmaPeerData` (the wrap-data pattern
  from `ngx-rust/examples/upstream.rs`) so we can override
  *both* `peer.get` and `peer.free`. Per-cycle `EwmaSlot` tables
  hung off `Ewma::config` (allocated from `cf->pool` in
  `init_upstream`); per-request avail-buffer + pick record sit in
  `EwmaPeerData` (allocated from `r->pool`). `peer.get` does P2C —
  collect available indices, pick `i = ngx_random() % count` and a
  distinct `j`, compare decayed scores, take the lower. `peer.free`
  folds the attempt's RTT into the slot, **skipping** on
  `NGX_PEER_FAILED` (a connection-refused must not pull the score
  down) and on `peers->config` mismatch (zone-driven peer-list
  shift between get and free could mis-attribute the RTT). EWMA
  history is per-worker and does not survive a config reload —
  out of scope for v1.

`Balancer` (ZST in `lib.rs`) implements `HttpModule` and
`HttpModuleServerConf` so ngx-rust generates the boilerplate
`create_srv_conf` / `merge_srv_conf` shims that
`NGX_HTTP_BALANCER_RS_CTX` references.

The `ngx::ngx_modules!(...)` invocation at the top level of `lib.rs`
is **load-bearing** — it emits the `ngx_modules` symbol that nginx's
`dlsym` looks up at `load_module` time. Removing it (or gating it
behind a cfg) breaks dynamic loading with `undefined symbol: ngx_modules`.

### Adding a new policy

1. New `mod foo` in `src/lib.rs`, file `src/foo.rs`. `pub struct Foo;`
   that `impl BalancingPolicy`. If the policy carries per-upstream
   state (see `Ewma`), make it a struct with a `pub(crate) fn new()`
   that returns the empty/null state. Pick which
   `peer.{init_upstream,init,get,free}` slots to override and which
   to chain through to round_robin.
2. Add a `Foo(Foo)` variant to `PolicyImpl` in `lib.rs`.
3. Extend `ngx_http_balancer_rs_commands_set`: match the new policy
   name, set `ccf.policy = PolicyImpl::Foo(Foo::new())`, install
   `Foo::init_upstream()`. Add a match arm in the dispatch below.

Stock nginx's modules under `nginx/src/http/modules/` are the
canonical reference for any algorithm we mirror; ingress-nginx's
Lua balancers under `ingress-nginx/rootfs/etc/nginx/lua/balancer/`
are the reference for anything Lua-derived (e.g. `ewma`).

## Test::Nginx harness gotchas

- `tests/run.sh` injects `load_module <abs-path-to-.so>;` via the
  `TEST_NGINX_GLOBALS` env var, which Test::Nginx splices into
  `%%TEST_GLOBALS%%`. Don't hard-code paths in `.t` files.
- The vendored nginx is built `--with-compat`, which is what allows the
  externally-built `.so` to load. Don't change build flags lightly.
- Test backends should respond with `return 200 "ok\n";` rather than
  serving files. Test::Nginx's temp prefix is `0700` and owned by the
  invoking user; nginx workers drop privileges to `nobody` and can't
  read those files when tests run as root. Multi-backend tests can
  spin custom Perl `http_daemon` listeners under the test user — see
  `tests/t/balancer_rs_least_conn.t` (ported from upstream
  `nginx-tests/upstream_least_conn.t`) for the canonical pattern.
  The ewma `.t` files use the same harness:
  `balancer_rs_ewma.t` (asymmetric-latency discriminator),
  `balancer_rs_ewma_distribution.t` (port of ingress-nginx's
  `test/e2e/loadbalance/ewma.go`), and `balancer_rs_ewma_retry.t`
  (port of the "skip tried endpoint" Lua unit test).
- Each `.t` file reports +2 auto-injected sub-tests (`no alerts`, `no
  sanitizer errors`) on top of whatever you `plan(...)`.

## Editor

`.vscode/settings.json` routes `rust-analyzer.check.command` through
clippy so warnings show up inline. After any cargo-feature or build-script
change, restart the rust-analyzer server.

rust-analyzer is stricter than rustc about chained coercions (fn-item →
fn-pointer + safe → unsafe). If r-a flags an FFI assignment that rustc
accepts, replicate the explicit-`Some(...)` pattern already used in the
file rather than introducing `as _` casts.

Rust 2024 enforces `unsafe_op_in_unsafe_fn` — every unsafe op inside
an `unsafe extern "C" fn` or `unsafe fn` needs an explicit `unsafe { … }`
block. The policy FFI code is verbose for this reason.
