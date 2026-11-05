import json
import logging

from common.db import get_connection, upsert_message
from common.kafka_consumer import run_consumer_loop

from .model import MODEL_NAME, classify_topic

logger = logging.getLogger("topic_service")


def main():
    logger.info("loading zero-shot topic model %s...", MODEL_NAME)
    classify_topic("warmup")
    conn = get_connection()

    def handle_message(record):
        message_id = record["id"]
        text = record["text"]
        upsert_message(conn, message_id, text, record.get("source_dataset"), record.get("produced_at"))

        top_label, top_score, all_labels = classify_topic(text)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO topic_results (message_id, top_label, top_score, all_labels, model_name)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE
                SET top_label = EXCLUDED.top_label, top_score = EXCLUDED.top_score,
                    all_labels = EXCLUDED.all_labels, model_name = EXCLUDED.model_name,
                    processed_at = now()
                """,
                (message_id, top_label, top_score, json.dumps(all_labels), MODEL_NAME),
            )
        conn.commit()
        logger.info("processed %s -> %s (%.3f)", message_id, top_label, top_score)

    run_consumer_loop("topic-service", handle_message)


if __name__ == "__main__":
    main()
