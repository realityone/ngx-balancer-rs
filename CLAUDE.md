# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Rust-implemented NGINX HTTP **upstream** module, packaged as a dynamic
module (`cdylib`). Adds an `upstream { ... }`-context directive
`balancer_rs <policy>;` — currently `least_conn` is the only accepted
value. The directive is **inert today**: the policy is parsed and
stored in `BalancerConfig`, but no `peer.init_upstream` hook is
installed, so nginx falls back to its default round-robin path without
any of our code running on the data path. Wiring per-policy behavior is
the next step. Built against
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

Everything lives in `src/lib.rs`. The shape mirrors `ngx-rust`'s
`examples/upstream.rs` — when in doubt, that's the reference. A local
checkout of ngx-rust sits at `~/.cargo/git/checkouts/ngx-rust-*/` if you
need to read its source.

Today there is one FFI callback:
`ngx_http_balancer_rs_commands_set` runs at config-parse time when
nginx encounters `balancer_rs <policy>;`. It validates the argument
and stores the resulting `Policy` in `BalancerConfig` (the
per-`upstream {}` server conf). Returning `NGX_CONF_ERROR` aborts
startup with the logged message.

To make the directive actually do something, add an
`unsafe extern "C" fn` matching `ngx_http_upstream_init_pt`, install it
via `uscf.peer.init_upstream = Some(...)` inside the commands callback
(re-importing `NgxHttpUpstreamModule::server_conf_mut`), and inside it
either call `ngx_http_upstream_init_round_robin` then patch
`peer.init` for per-request routing, or set up a custom peer init from
scratch. The earlier git history (`git log -- src/lib.rs`) has the
wrapper shape we used before.

`Balancer` (ZST) implements `HttpModule` and `HttpModuleServerConf` so
ngx-rust generates the boilerplate `create_srv_conf` / `merge_srv_conf`
shims that the static `NGX_HTTP_BALANCER_RS_CTX` references.

The `ngx::ngx_modules!(...)` invocation at the top level is
**load-bearing** — it emits the `ngx_modules` symbol that nginx's
`dlsym` looks up at `load_module` time. Removing it (or gating it
behind a cfg) breaks dynamic loading with `undefined symbol: ngx_modules`.

## Test::Nginx harness gotchas

- `tests/run.sh` injects `load_module <abs-path-to-.so>;` via the
  `TEST_NGINX_GLOBALS` env var, which Test::Nginx splices into
  `%%TEST_GLOBALS%%`. Don't hard-code paths in `.t` files.
- The vendored nginx is built `--with-compat`, which is what allows the
  externally-built `.so` to load. Don't change build flags lightly.
- Test backends should respond with `return 200 "ok\n";` rather than
  serving files. Test::Nginx's temp prefix is `0700` and owned by the
  invoking user; nginx workers drop privileges to `nobody` and can't
  read those files when tests run as root.
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
