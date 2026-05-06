#!/usr/bin/env bash
# Build the module + vendored nginx, then run the Test::Nginx (.t) suite.
#
# Usage: tests/run.sh [extra prove args...]
#   TEST_NGINX_VERBOSE=1   show nginx error log output (default on)
#   TEST_NGINX_LEAVE=1     keep the temp prefix dir after a failed test
#   TEST_NGINX_NOFILE=1024 open-file limit for nginx workers
set -euo pipefail

cd "$(dirname "$0")/.."

TEST_NGINX_NOFILE="${TEST_NGINX_NOFILE:-1024}"
NOFILE_HARD=$(ulimit -Hn)
if [[ "$NOFILE_HARD" != "unlimited" && "$TEST_NGINX_NOFILE" -gt "$NOFILE_HARD" ]]; then
    TEST_NGINX_NOFILE=$NOFILE_HARD
fi
ulimit -Sn "$TEST_NGINX_NOFILE"

cargo build

# `stat -c` is GNU (Linux), `stat -f` is BSD (macOS). Detect once —
# if we leave both forms in the per-iteration fallback, GNU stat's
# `-f` doesn't fail on Linux but means `--file-system`, so it
# prints filesystem info to stdout and the `||` never fires.
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
    echo "tests: vendored nginx binary not found under target/debug/build/nginx-sys-*/out/objs/" >&2
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
    echo "tests: module not found under target/debug/libngx_balancer_rs.{so,dylib}" >&2
    exit 1
fi

if [[ ! -d tests/nginx-tests/lib ]]; then
    echo "tests: fetching nginx-tests harness..."
    (cd tests && git clone --depth 1 --filter=blob:none --sparse \
        https://github.com/nginx/nginx-tests.git)
    (cd tests/nginx-tests && git sparse-checkout set lib)
fi

export PERL5LIB="$PWD/tests/nginx-tests/lib"
export TEST_NGINX_BINARY="$NGINX_BIN"
export TEST_NGINX_GLOBALS="worker_rlimit_nofile $TEST_NGINX_NOFILE;
load_module $MODULE_SO;"
export TEST_NGINX_VERBOSE="${TEST_NGINX_VERBOSE:-1}"

if (($#)); then
    exec prove "$@"
else
    exec prove tests/t/
fi
