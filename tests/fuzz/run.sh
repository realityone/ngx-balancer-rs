#!/usr/bin/env bash
# Chaos / fuzz harness for live nginx loaded with balancer_rs.
#
# Builds the module, spawns a handful of misbehaving HTTP backends,
# starts an nginx that uses both `balancer_rs least_conn` and
# `balancer_rs ewma` upstreams, drives concurrent randomized
# requests at it for FUZZ_DURATION seconds, then checks the error
# log for fatal markers and prints a summary.
#
# Usage:
#   tests/fuzz/run.sh
#   FUZZ_DURATION=10 tests/fuzz/run.sh
#   FUZZ_DURATION=600 FUZZ_CLIENTS=64 tests/fuzz/run.sh
#   FUZZ_SEED=12345 tests/fuzz/run.sh
#   FUZZ_KEEP=1 tests/fuzz/run.sh   # keep the temp prefix on success
set -euo pipefail

cd "$(dirname "$0")/../.."

FUZZ_DURATION=${FUZZ_DURATION:-60}
FUZZ_CLIENTS=${FUZZ_CLIENTS:-32}
FUZZ_SEED=${FUZZ_SEED:-$(date +%s)}
FUZZ_KEEP=${FUZZ_KEEP:-}

TEST_NGINX_NOFILE="${TEST_NGINX_NOFILE:-1024}"
NOFILE_HARD=$(ulimit -Hn)
if [[ "$NOFILE_HARD" != "unlimited" && "$TEST_NGINX_NOFILE" -gt "$NOFILE_HARD" ]]; then
    TEST_NGINX_NOFILE=$NOFILE_HARD
fi
ulimit -Sn "$TEST_NGINX_NOFILE"

cargo build

# `chaos_backend.py` and `fuzz_client.py` declare aiohttp via PEP 723
# inline metadata; `uv run --script` provisions a transient env on
# first run. Pull uv from $PATH or its standard install location.
if ! command -v uv >/dev/null 2>&1; then
    if [[ -x "$HOME/.local/bin/uv" ]]; then
        PATH="$HOME/.local/bin:$PATH"
    else
        echo "fuzz: uv not found. Install with: curl -LsSf https://astral.sh/uv/install.sh | sh" >&2
        exit 1
    fi
fi

# stat -c (GNU) vs stat -f (BSD) — same probe as tests/run.sh.
if stat -c '%Y' /dev/null >/dev/null 2>&1; then
    stat_mtime() { stat -c '%Y' "$1"; }
else
    stat_mtime() { stat -f '%m' "$1"; }
fi

NGINX_BIN=
NGINX_MTIME=0
while IFS= read -r -d '' candidate; do
    mtime=$(stat_mtime "$candidate")
    if (( mtime > NGINX_MTIME )); then
        NGINX_BIN=$candidate
        NGINX_MTIME=$mtime
    fi
done < <(find "$PWD/target/debug/build" -path '*/nginx-sys-*/out/objs/nginx' -type f -print0)
if [[ -z "${NGINX_BIN:-}" ]]; then
    echo "fuzz: vendored nginx binary not found under target/debug/build/" >&2
    exit 1
fi

MODULE_SO=
for candidate in "$PWD"/target/debug/libngx_balancer_rs.{so,dylib}; do
    if [[ -f "$candidate" ]]; then
        MODULE_SO=$candidate
        break
    fi
done
if [[ -z "${MODULE_SO:-}" ]]; then
    echo "fuzz: module not found at target/debug/libngx_balancer_rs.{so,dylib}" >&2
    exit 1
fi

PREFIX="$PWD/target/fuzz-prefix"
LOGS="$PREFIX/logs"
rm -rf "$PREFIX"
mkdir -p "$LOGS" "$PREFIX/html"
cp "$PWD/tests/fuzz/nginx.conf" "$PREFIX/nginx.conf"

BACKEND_PIDS=()
NGINX_PID=

cleanup() {
    if [[ -n "$NGINX_PID" ]] && kill -0 "$NGINX_PID" 2>/dev/null; then
        kill -TERM "$NGINX_PID" 2>/dev/null || true
        wait "$NGINX_PID" 2>/dev/null || true
    fi
    for pid in "${BACKEND_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            kill -TERM "$pid" 2>/dev/null || true
        fi
    done
}
trap cleanup EXIT

echo "fuzz: spawning chaos backends (seed=$FUZZ_SEED)"
for port in 9081 9082 9083 9084 9085; do
    uv run --script "$PWD/tests/fuzz/chaos_backend.py" --port="$port" --seed="$FUZZ_SEED" \
        >>"$LOGS/backend.log" 2>&1 &
    BACKEND_PIDS+=("$!")
done

# Wait for each chaos backend to start accepting.
for port in 9081 9082 9083 9084 9085; do
    for _ in {1..50}; do
        if (echo > /dev/tcp/127.0.0.1/$port) 2>/dev/null; then
            break
        fi
        sleep 0.05
    done
done

echo "fuzz: starting nginx ($NGINX_BIN)"
"$NGINX_BIN" \
    -p "$PREFIX" \
    -c "$PREFIX/nginx.conf" \
    -g "worker_rlimit_nofile $TEST_NGINX_NOFILE; load_module $MODULE_SO;" \
    >>"$LOGS/nginx.stdout" 2>&1 &
NGINX_PID=$!

# Wait for the listener.
for _ in {1..100}; do
    if (echo > /dev/tcp/127.0.0.1/9080) 2>/dev/null; then
        break
    fi
    sleep 0.05
done
if ! (echo > /dev/tcp/127.0.0.1/9080) 2>/dev/null; then
    echo "fuzz: nginx failed to start listening on 127.0.0.1:9080" >&2
    echo "--- error.log tail ---" >&2
    tail -200 "$LOGS/error.log" >&2 || true
    exit 1
fi

echo "fuzz: driving load for ${FUZZ_DURATION}s with $FUZZ_CLIENTS clients"
uv run --script "$PWD/tests/fuzz/fuzz_client.py" \
    --target 127.0.0.1:9080 \
    --duration "$FUZZ_DURATION" \
    --clients "$FUZZ_CLIENTS" \
    --seed "$FUZZ_SEED"
CLIENT_RC=$?

if ! kill -0 "$NGINX_PID" 2>/dev/null; then
    echo "fuzz: nginx died during the run" >&2
    echo "--- error.log tail ---" >&2
    tail -200 "$LOGS/error.log" >&2 || true
    exit 1
fi

echo "fuzz: stopping nginx"
kill -TERM "$NGINX_PID" 2>/dev/null || true
wait "$NGINX_PID" 2>/dev/null || true
NGINX_PID=

# Pattern matches obvious hard failures. Plain `[error]` is *not*
# included — connection refused / upstream timeout messages from
# our chaos backends are expected.
FATAL_RE='\[(alert|emerg)\]|SIGSEGV|SIGABRT|panic|sanitizer|stack[ -]overflow|use[-_]after[-_]free|assertion failed|core dumped'

if grep -E "$FATAL_RE" "$LOGS/error.log" >/dev/null 2>&1; then
    echo "fuzz: FAIL — fatal markers in error.log:" >&2
    grep -nE "$FATAL_RE" "$LOGS/error.log" >&2
    echo "--- error.log tail ---" >&2
    tail -100 "$LOGS/error.log" >&2 || true
    exit 1
fi

if [[ "$CLIENT_RC" -ne 0 ]]; then
    echo "fuzz: FAIL — fuzz_client exited $CLIENT_RC" >&2
    exit 1
fi

ACCESS_LINES=$(wc -l < "$LOGS/access.log" 2>/dev/null || echo 0)
ERR_LINES=$(wc -l < "$LOGS/error.log" 2>/dev/null || echo 0)
echo "fuzz: PASS — access_log=$ACCESS_LINES lines, error_log=$ERR_LINES lines, prefix=$PREFIX"

if [[ -z "$FUZZ_KEEP" ]]; then
    rm -rf "$PREFIX"
fi
