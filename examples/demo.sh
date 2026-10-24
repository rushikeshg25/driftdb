#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

BIN=${BIN:-./bin/driftdb}
if [[ ! -x "$BIN" ]]; then
  echo "building $BIN"
  go build -o "$BIN" ./cmd/driftdb
fi

TMP=$(mktemp -d "${TMPDIR:-/tmp}/driftdb-demo.XXXXXX")
PID=""
ADDR=""

cleanup() {
  if [[ -n "$PID" ]]; then
    kill "$PID" 2>/dev/null || true
    wait "$PID" 2>/dev/null || true
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

a() { "$BIN" --data "$TMP/a" --node alice "$@"; }
b() { "$BIN" --data "$TMP/b" --node bob "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

start_bob() {
  ADDR=""
  b serve --listen "127.0.0.1:0" >"$TMP/bob.log" 2>&1 &
  PID=$!
  for _ in $(seq 1 50); do
    ADDR=$(sed -n 's/.*listening on \([^ ]*\).*/\1/p' "$TMP/bob.log" | head -n 1)
    if [[ -n "$ADDR" ]]; then
      return 0
    fi
    sleep 0.1
  done
  echo "bob failed to start:" >&2
  cat "$TMP/bob.log" >&2
  return 1
}

stop_bob() {
  kill "$PID" 2>/dev/null || true
  wait "$PID" 2>/dev/null || true
  PID=""
}

step "alice writes while offline"
a put note title "draft"
a sadd note tags urgent
a get note

step "first exchange: bob catches up, then goes offline"
start_bob
a sync "$ADDR"
stop_bob

step "both replicas edit the same document offline"
a put note title "alice's title"
b put note title "bob's title"
a srem note tags urgent   # alice removes the tag she has seen
b sadd note tags urgent   # bob concurrently re-adds it

step "merge over one TCP exchange"
start_bob
a sync "$ADDR"
stop_bob

step "alice's view"
a get note
step "bob's view"
b get note

if diff -u <(a get note) <(b get note) >/dev/null; then
  printf '\n\033[32mreplicas converged; concurrent add survived the remove\033[0m\n'
else
  printf '\n\033[31mreplicas diverged\033[0m\n' >&2
  exit 1
fi
