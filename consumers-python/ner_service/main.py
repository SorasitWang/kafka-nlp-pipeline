import json
import logging

from common.db import transaction, upsert_message
from common.kafka_consumer import run_consumer_loop

from .model import MODEL_NAME, extract_entities

logger = logging.getLogger("ner_service")


def main():
    logger.info("loading NER model %s...", MODEL_NAME)
    extract_entities("warmup")

    def handle_message(record):
        message_id = record["id"]
        text = record["text"]
        entities = extract_entities(text)
        # The message row and this service's result commit together, or not at all.
        with transaction() as cur:
            upsert_message(cur, message_id, text, record.get("source_dataset"), record.get("produced_at"))
            cur.execute(
                """
                INSERT INTO ner_results (message_id, entities, entity_count, model_name)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (message_id) DO UPDATE
                SET entities = EXCLUDED.entities, entity_count = EXCLUDED.entity_count,
                    model_name = EXCLUDED.model_name, processed_at = now()
                """,
                (message_id, json.dumps(entities), len(entities), MODEL_NAME),
            )
        logger.info("processed %s -> %d entities", message_id, len(entities))

    run_consumer_loop("ner-service", handle_message)


if __name__ == "__main__":
    main()
