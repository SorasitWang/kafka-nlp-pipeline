#!/usr/bin/env bash
# Starts the whole pipeline: infra (docker compose), producer, all three
# consumers, the results dashboard, and the ops visualizer.
#
# Everything except docker compose runs as a background process; logs go to
# logs/*.log and pids to pids/*.pid. Use scripts/stop-all.sh to tear it down.
set -euo pipefail
set -m  # job control: gives each backgrounded job below its own process group

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

LOG_DIR="$ROOT/logs"
PID_DIR="$ROOT/pids"
mkdir -p "$LOG_DIR" "$PID_DIR"

# The Go producer reads plain OS env vars (no dotenv support), so a root
# .env has to be exported by hand for it to see any overrides.
if [ -f .env ]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

log()  { printf '\033[1;34m[start-all]\033[0m %s\n' "$1"; }
warn() { printf '\033[1;33m[start-all]\033[0m %s\n' "$1"; }

start_bg() {
  local name="$1" dir="$2" cmd="$3"
  local pid_file="$PID_DIR/$name.pid"
  if [ -f "$pid_file" ] && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    warn "$name already running (pid $(cat "$pid_file")), skipping"
    return
  fi
  # `set -m` above makes this backgrounded subshell its own process group,
  # with pgid == its own pid ($!). npm/poetry run the real server/interpreter
  # as a separate child pid underneath it, but since nothing here calls
  # setpgid, every descendant inherits that same pgid — so stop-all.sh can
  # kill the whole tree at once via `kill -- -$pid` instead of just this one
  # pid, which would otherwise orphan the real child still running.
  (cd "$dir" && exec nohup bash -c "$cmd" >"$LOG_DIR/$name.log" 2>&1) &
  local pid=$!
  disown "$pid" 2>/dev/null || true
  echo "$pid" >"$pid_file"
  log "$name started (pid $pid, log: logs/$name.log)"
}

log "bringing up Kafka + Postgres + Kafka UI..."
docker compose up -d

log "waiting for Kafka to accept admin requests..."
for _ in $(seq 1 30); do
  if docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

TOPIC_NAME="${TOPIC_NAME:-text-stream}"
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC_NAME" --partitions 2 --replication-factor 1 --if-not-exists >/dev/null
log "topic \"$TOPIC_NAME\" ready"

log "waiting for Postgres to accept connections..."
for _ in $(seq 1 30); do
  if docker exec postgres pg_isready -U "${POSTGRES_USER:-textanalysis}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if [ ! -x producer-go/producer ]; then
  log "producer binary missing, building it..."
  (cd producer-go && go build -o producer ./cmd/producer)
fi

if [ ! -d consumers-python/.venv ] && ! poetry -C consumers-python env info -q >/dev/null 2>&1; then
  log "python deps not installed yet, running poetry install (this can take a while the first time)..."
  (cd consumers-python && poetry install)
fi

if [ ! -d dashboard-node/node_modules ]; then
  log "dashboard-node deps missing, running npm install..."
  (cd dashboard-node && npm install)
fi

if [ ! -d visualizer-node/node_modules ]; then
  log "visualizer-node deps missing, running npm install..."
  (cd visualizer-node && npm install)
fi

# Each pretrained-model consumer defaults to using most/all CPU cores for a
# single inference call (OpenMP/MKL intra-op threads). Run more than a couple
# of them at once uncapped and they oversubscribe the machine and slow each
# other down instead of running in parallel. Cap per-process threads so
# CONSUMER_THREADS * (number of consumer processes) stays near `nproc`.
CONSUMER_THREADS="${CONSUMER_THREADS:-4}"
CONSUMER_ENV="OMP_NUM_THREADS=$CONSUMER_THREADS MKL_NUM_THREADS=$CONSUMER_THREADS"

# How many topic_service instances to run. Since they all share the same
# Kafka consumer group (topic-service), Kafka splits the topic's partitions
# across however many are running — this is how topic_service scales
# horizontally when it's the slowest stage of the pipeline.
TOPIC_INSTANCES="${TOPIC_INSTANCES:-1}"

start_bg producer      producer-go       "./producer"
start_bg sentiment     consumers-python  "$CONSUMER_ENV poetry run python -m sentiment_service.main"
if [ "$TOPIC_INSTANCES" -le 1 ]; then
  start_bg topic consumers-python "$CONSUMER_ENV poetry run python -m topic_service.main"
else
  for i in $(seq 1 "$TOPIC_INSTANCES"); do
    start_bg "topic-$i" consumers-python "$CONSUMER_ENV poetry run python -m topic_service.main"
  done
fi
start_bg toxicity      consumers-python  "$CONSUMER_ENV poetry run python -m toxicity_service.main"
start_bg ner           consumers-python  "$CONSUMER_ENV poetry run python -m ner_service.main"
start_bg dashboard     dashboard-node    "npm start"
start_bg visualizer    visualizer-node   "npm start"

cat <<EOF

Pipeline is up.
  Dashboard:   http://localhost:3000
  Visualizer:  http://localhost:3001
  Kafka UI:    http://localhost:8080

Tail all logs:  tail -f logs/*.log
Stop it all:    scripts/stop-all.sh
EOF
