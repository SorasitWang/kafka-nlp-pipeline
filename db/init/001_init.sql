CREATE TABLE messages (
    id             TEXT PRIMARY KEY,
    text           TEXT NOT NULL,
    source_dataset TEXT,
    produced_at    TIMESTAMPTZ,
    inserted_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sentiment_results (
    id            BIGSERIAL PRIMARY KEY,
    message_id    TEXT NOT NULL REFERENCES messages(id),
    label         TEXT NOT NULL,
    score         DOUBLE PRECISION NOT NULL,
    model_name    TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id)
);

CREATE TABLE topic_results (
    id             BIGSERIAL PRIMARY KEY,
    message_id     TEXT NOT NULL REFERENCES messages(id),
    top_label      TEXT NOT NULL,
    top_score      DOUBLE PRECISION NOT NULL,
    all_labels     JSONB NOT NULL,
    model_name     TEXT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id)
);

CREATE TABLE toxicity_results (
    id             BIGSERIAL PRIMARY KEY,
    message_id     TEXT NOT NULL REFERENCES messages(id),
    is_toxic       BOOLEAN NOT NULL,
    toxicity_score DOUBLE PRECISION NOT NULL,
    label          TEXT NOT NULL,
    model_name     TEXT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id)
);

CREATE TABLE ner_results (
    id             BIGSERIAL PRIMARY KEY,
    message_id     TEXT NOT NULL REFERENCES messages(id),
    entities       JSONB NOT NULL,
    entity_count   INT NOT NULL,
    model_name     TEXT NOT NULL,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (message_id)
);

CREATE INDEX idx_sentiment_processed_at ON sentiment_results (processed_at);
CREATE INDEX idx_topic_processed_at     ON topic_results (processed_at);
CREATE INDEX idx_toxicity_processed_at  ON toxicity_results (processed_at);
CREATE INDEX idx_ner_processed_at       ON ner_results (processed_at);
