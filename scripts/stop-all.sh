#!/usr/bin/env bash
# Stops everything scripts/start-all.sh started. Docker infra (Kafka/Postgres)
# is left running by default so the topic/lag state survives between runs;
# pass --with-infra to also bring that down.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PID_DIR="$ROOT/pids"
log() { printf '\033[1;34m[stop-all]\033[0m %s\n' "$1"; }

if [ -d "$PID_DIR" ]; then
  for pid_file in "$PID_DIR"/*.pid; do
    [ -e "$pid_file" ] || continue
    name="$(basename "$pid_file" .pid)"
    pid="$(cat "$pid_file")"
    # start-all.sh launches each service with setsid, so pid doubles as the
    # process group id — signal the whole group (-pid), not just that one
    # pid, otherwise npm's/poetry's actual child (the real server/interpreter)
    # is left running as an orphan.
    if kill -0 "$pid" 2>/dev/null; then
      kill -TERM -- "-$pid" 2>/dev/null || true
      for _ in $(seq 1 10); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
      done
      if kill -0 "$pid" 2>/dev/null; then
        kill -KILL -- "-$pid" 2>/dev/null || true
        log "force-killed $name (pid $pid, didn't stop within 5s)"
      else
        log "stopped $name (pid $pid)"
      fi
    fi
    rm -f "$pid_file"
  done
fi

if [ "${1:-}" = "--with-infra" ]; then
  log "bringing down docker compose infra..."
  docker compose down
fi
