import logging

from common.db import get_connection, upsert_message
from common.kafka_consumer import run_consumer_loop

from .model import MODEL_NAME, classify_sentiment

logger = logging.getLogger("sentiment_service")


def main():
    logger.info("loading sentiment model %s...", MODEL_NAME)
    classify_sentiment("warmup")
    conn = get_connection()

    def handle_message(record):
        message_id = record["id"]
        text = record["text"]
        upsert_message(conn, message_id, text, record.get("source_dataset"), record.get("produced_at"))

        label, score = classify_sentiment(text)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO sentiment_results (message_id, label, score, model_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE
                SET label = EXCLUDED.label, score = EXCLUDED.score,
                    model_name = EXCLUDED.model_name, processed_at = now()
                """,
                (message_id, label, score, MODEL_NAME),
            )
        conn.commit()
        logger.info("processed %s -> %s (%.3f)", message_id, label, score)

    run_consumer_loop("sentiment-service", handle_message)


if __name__ == "__main__":
    main()
