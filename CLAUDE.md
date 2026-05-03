# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust-implemented NGINX HTTP **upstream** module, packaged as a dynamic
module (`cdylib`). Adds an `upstream { ... }`-context directive
`balancer_rs <policy>;` — currently `least_conn` is the only accepted
value, and it is fully wired (init_upstream → init_peer → get_peer)
with the same selection algorithm as stock nginx's
`ngx_http_upstream_least_conn_module.c`. Built against
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

Three source files; the shape mirrors `ngx-rust`'s
`examples/upstream.rs` — when in doubt, that's the reference. A local
checkout of ngx-rust sits at `~/.cargo/git/checkouts/ngx-rust-*/` if
you need to read its source.

- **`src/lib.rs`** — module plumbing: `ngx_module_t`, the
  `NGX_HTTP_BALANCER_RS_CTX` `ngx_http_module_t`, the `balancer_rs`
  command, and `ngx_http_balancer_rs_commands_set` (the directive
  callback). The callback parses the policy argument, sets
  `uscf.flags` to the matching policy's accepted-server-parameter set
  (e.g. `weight=`, `max_conns=`, `down`, `backup`), and installs the
  policy's `init_upstream` on `uscf.peer`.
- **`src/policy.rs`** — `BalancingPolicy` trait. One method:
  `init_upstream() -> ngx_http_upstream_init_pt`. Each policy impl
  returns its `peer.init_upstream` entry point.
- **`src/least_conn.rs`** — the only policy today. `init_upstream`
  delegates to `ngx_http_upstream_init_round_robin` then patches
  `peer.init`; that per-request `init_peer` chains to
  `ngx_http_upstream_init_round_robin_peer` and only overrides
  `peer.get` (round_robin's `peer.free` keeps doing connection
  bookkeeping). The selector matches stock nginx including: tie-break
  by weighted round-robin, primary→backup fallback with `tried[]`
  reset, peers wlock when `peers->shpool` is set, config-generation
  staleness check, and `peer.refs++` for zone mode.

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
   that `impl BalancingPolicy`. Pick which `peer.{init_upstream,init,get,free}`
   slots to override and which to chain through to round_robin.
2. Add a variant to the `Policy` enum in `lib.rs`.
3. Extend `ngx_http_balancer_rs_commands_set`: parse the new policy
   name, set the appropriate `uscf.flags`, install
   `Foo::init_upstream()`.

Stock nginx's modules under `nginx/src/http/modules/` are the
canonical reference for any algorithm we mirror.

## Test::Nginx harness gotchas

- `tests/run.sh` injects `load_module <abs-path-to-.so>;` via the
  `TEST_NGINX_GLOBALS` env var, which Test::Nginx splices into
  `%%TEST_GLOBALS%%`. Don't hard-code paths in `.t` files.
- The vendored nginx is built `--with-compat`, which is what allows the
  externally-built `.so` to load. Don't change build flags lightly.
- Test backends should respond with `return 200 "ok\n";` rather than
  serving files. Test::Nginx's temp prefix is `0700` and owned by the
  invoking user; nginx workers drop privileges to `nobody` and can't
  read those files when tests run as root. (Multi-backend tests can
  spin custom Perl `http_daemon` listeners under the test user — see
  `tests/t/balancer_rs_least_conn.t`, ported from upstream
  `nginx-tests/upstream_least_conn.t`, for the pattern.)
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
block. The least_conn FFI code is verbose for this reason.
