import logging

from common.db import get_connection, upsert_message
from common.kafka_consumer import run_consumer_loop

from .model import MODEL_NAME, classify_toxicity

logger = logging.getLogger("toxicity_service")


def main():
    logger.info("loading toxicity model %s...", MODEL_NAME)
    classify_toxicity("warmup")
    conn = get_connection()

    def handle_message(record):
        message_id = record["id"]
        text = record["text"]
        upsert_message(conn, message_id, text, record.get("source_dataset"), record.get("produced_at"))

        label, score, is_toxic = classify_toxicity(text)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO toxicity_results (message_id, is_toxic, toxicity_score, label, model_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE
                SET is_toxic = EXCLUDED.is_toxic, toxicity_score = EXCLUDED.toxicity_score,
                    label = EXCLUDED.label, model_name = EXCLUDED.model_name,
                    processed_at = now()
                """,
                (message_id, is_toxic, score, label, MODEL_NAME),
            )
        conn.commit()
        logger.info("processed %s -> %s toxic=%s (%.3f)", message_id, label, is_toxic, score)

    run_consumer_loop("toxicity-service", handle_message)


if __name__ == "__main__":
    main()
