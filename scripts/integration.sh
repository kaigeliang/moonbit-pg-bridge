#!/bin/sh
set -eu
# Creates an isolated, trust-authenticated cluster bound only to loopback.
# Override PG_BRIDGE_PORT if this port is already in use.
port=${PG_BRIDGE_PORT:-55438}
target=${PG_BRIDGE_TARGET:-native}
case "$target" in native|wasm) ;; *) echo "Unsupported target: $target" >&2; exit 2 ;; esac
case "$port" in ''|*[!0-9]*) echo 'Port must be numeric.' >&2; exit 2 ;; esac
if [ "$port" -lt 1024 ] || [ "$port" -gt 65535 ]; then
  echo 'Port must be between 1024 and 65535.' >&2; exit 2
fi
if pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1; then
  echo "Port $port is already serving PostgreSQL; refusing to use it." >&2
  exit 2
fi
dir=$(mktemp -d "${TMPDIR:-/tmp}/pg-bridge.XXXXXX")
trap 'pg_ctl -D "$dir/data" -m immediate stop >/dev/null 2>&1 || true' EXIT HUP INT TERM
initdb -D "$dir/data" -U bridge_test --auth=trust --no-locale >/dev/null
pg_ctl -D "$dir/data" -l "$dir/log" -o "-h 127.0.0.1 -p $port -k $dir" start >/dev/null
PG_BRIDGE_PORT="$port" PG_BRIDGE_INTEGRATION=1 moon test --target "$target" -p kaigeliang/moonbit-pg-bridge/integration
# Copy the consumer and current source into a fresh workspace. This exercises
# the public interface through a separate module, not a registry installation.
mkdir -p "$dir/workspace/bridge" "$dir/workspace/consumer"
cp moon.mod moon.pkg bridge.mbt "$dir/workspace/bridge/"
cp consumer/moon.mod consumer/moon.pkg consumer/consumer_test.mbt "$dir/workspace/consumer/"
printf 'members = ["bridge", "consumer"]\n' > "$dir/workspace/moon.work"
PG_BRIDGE_PORT="$port" moon -C "$dir/workspace/consumer" test --target native
printf 'Temporary cluster files retained at %s (server stops on exit)\n' "$dir"
