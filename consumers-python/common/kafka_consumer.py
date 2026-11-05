import json
import logging

from confluent_kafka import Consumer

from . import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def make_consumer(group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.KAFKA_BROKERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )


def run_consumer_loop(group_id: str, handle_message):
    """Subscribes to the shared text-stream topic and calls handle_message(dict) for each record."""
    logger = logging.getLogger(group_id)
    consumer = make_consumer(group_id)
    consumer.subscribe([config.TOPIC_NAME])
    logger.info("subscribed to %s as group %s", config.TOPIC_NAME, group_id)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("consumer error: %s", msg.error())
                continue
            try:
                record = json.loads(msg.value())
            except json.JSONDecodeError:
                logger.exception("failed to decode message value")
                continue
            try:
                handle_message(record)
            except Exception:
                logger.exception("failed to process message id=%s", record.get("id"))
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        consumer.close()
