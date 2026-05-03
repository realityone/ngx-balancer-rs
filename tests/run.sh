#!/usr/bin/env bash
# Build the module + vendored nginx, then run the Test::Nginx (.t) suite.
#
# Usage: tests/run.sh [extra prove args...]
#   TEST_NGINX_VERBOSE=1   show nginx error log output (default on)
#   TEST_NGINX_LEAVE=1     keep the temp prefix dir after a failed test
set -euo pipefail

cd "$(dirname "$0")/.."

cargo build

NGINX_BIN=
NGINX_MTIME=0
while IFS= read -r -d '' candidate; do
    mtime=$(stat -f '%m' "$candidate" 2>/dev/null || stat -c '%Y' "$candidate")
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
export TEST_NGINX_GLOBALS="load_module $MODULE_SO;"
export TEST_NGINX_VERBOSE="${TEST_NGINX_VERBOSE:-1}"

exec prove "$@" tests/t/
