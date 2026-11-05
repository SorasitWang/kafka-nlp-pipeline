# Kafka + NLP Text Analysis Pipeline

A static-data streaming pipeline: a Go producer replays a sampled HuggingFace
dataset into Kafka; independent Python consumers run NLP models (sentiment,
zero-shot topic classification, toxicity — all pretrained/off-the-shelf, and
NER, which runs a model fine-tuned locally on CoNLL-2003) and write results
to Postgres; a Node.js dashboard displays them.

```
data/export_dataset.py --> data/dataset.jsonl
                                  |
                                  v
                    producer-go (Kafka producer)
                                  |
                     topic: text-stream (2 partitions)
                /          |            |            \
   sentiment_service  topic_service  toxicity_service  ner_service
     (pretrained)      (pretrained)    (pretrained)   (fine-tuned locally)
                \          |            |            /
                          Postgres (results)
                                  |
                          dashboard-node (Express + vanilla JS)

Kafka admin API + Postgres also feed visualizer-node (Express + vanilla JS),
a separate ops view of consumer lag / busy-idle-down status per service.
```

## Prerequisites

- Docker + Docker Compose plugin (for Kafka + Postgres)
- Go 1.23+
- Python 3.12+ with Poetry
- Node.js 20+ (installed here via `nvm`)

## Setup

1. Copy env file: `cp .env.example .env` (required: Postgres credentials have no built-in defaults)
2. Start infra: `docker compose up -d`
3. Install Python deps: `cd consumers-python && poetry install`
4. Export the dataset (one-time): `poetry run python ../data/export_dataset.py`
5. Fine-tune the NER model (one-time, CPU-only, ~30-45 min on a modern
   multi-core machine — hardware-dependent):
   `cd consumers-python && poetry run python -m ner_service.training.train_ner`
   — downloads CoNLL-2003 and fine-tunes `distilbert-base-cased` for token
   classification, saving weights to `consumers-python/ner_service/artifacts/ner-bert/`.
   `ner_service` refuses to start without this artifact. Safe to run in the
   background (e.g. `nohup ... &`); it prints entity-level precision/recall/F1
   per epoch and a final test-set report when done.
6. Install Node deps: `cd dashboard-node && npm install`, and `cd visualizer-node && npm install`
7. Build the Go producer: `cd producer-go && go build -o producer ./cmd/producer`

## Running the pipeline

Fastest path — one script starts infra, producer, all four consumers,
the dashboard, and the visualizer (each backgrounded, logging to `logs/`):

```bash
./scripts/start-all.sh   # brings everything up
tail -f logs/*.log       # optional: watch all logs together
./scripts/stop-all.sh    # stops the app processes (leaves Kafka/Postgres running)
./scripts/stop-all.sh --with-infra   # also runs `docker compose down`
```

Or run each piece by hand, in separate terminals:

```bash
# 1. Producer - streams data/dataset.jsonl into Kafka
cd producer-go && ./producer

# 2. Consumers - each is an independent process/consumer group
cd consumers-python && poetry run python -m sentiment_service.main
cd consumers-python && poetry run python -m topic_service.main
cd consumers-python && poetry run python -m toxicity_service.main
cd consumers-python && poetry run python -m ner_service.main   # requires the training step above

# 3. Dashboard
cd dashboard-node && npm start
# open http://localhost:3000

# 4. Ops visualizer (queue depth + busy/idle/down per service)
cd visualizer-node && npm start
# open http://localhost:3001
```

Kafka UI (topic/consumer group inspection) is available at http://localhost:8080.

## Notes

- Dataset labels are intentionally ignored; sentiment/topic/toxicity results
  come from pretrained/off-the-shelf models, not models trained on this data.
  `ner_service` is the exception: it runs a model fine-tuned locally on
  CoNLL-2003 (see Setup step 5), not a model trained on the review data itself.
- Each consumer writes `messages` once (idempotent upsert) plus its own
  result row (`sentiment_results` / `topic_results` / `toxicity_results` /
  `ner_results`), keyed by `message_id` with "latest result wins" semantics.
- `ner_results.entities` is a JSONB array of `{text, label, score, start, end}`
  per message. Amazon reviews are more entity-sparse than CoNLL-2003's
  news-domain training data, so many messages will show no entities — that's
  expected, not a bug.
- `db/init/001_init.sql` only runs on a fresh Postgres volume (Docker's
  `docker-entrypoint-initdb.d` behavior). If you already have a running
  Postgres volume from before `ner_results` was added, either recreate it
  (`docker compose down -v && docker compose up -d`) or apply the new
  `CREATE TABLE ner_results` / `CREATE INDEX idx_ner_processed_at` statements
  by hand via `docker exec -i postgres psql -U textanalysis -d textanalysis`.
- `PRODUCER_LOOP=true` in `.env` makes the producer replay the dataset
  indefinitely, useful for a longer-running dashboard demo.
