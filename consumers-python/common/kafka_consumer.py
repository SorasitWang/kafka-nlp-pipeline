import json
import logging
import time

from confluent_kafka import Consumer, Producer

from . import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def make_consumer(group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": config.KAFKA_BROKERS,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # Commit offsets ourselves, only after a message is fully handled. With auto-commit,
            # offsets are committed in the background whether or not processing succeeded, so a
            # message that failed (or was in flight during a crash) would be skipped for good.
            "enable.auto.commit": False,
        }
    )


def make_dlq_producer() -> Producer:
    return Producer({"bootstrap.servers": config.KAFKA_BROKERS})


def send_to_dlq(producer: Producer, msg, group_id: str, error: str) -> None:
    """Copies a message that can't be processed to the dead-letter topic, with the reason.

    Raises if the broker doesn't confirm the write, so the caller can avoid committing
    an offset for a message that was neither processed nor parked.
    """
    delivery_errors = []

    def on_delivery(err, _msg):
        if err is not None:
            delivery_errors.append(err)

    producer.produce(
        config.DLQ_TOPIC_NAME,
        key=msg.key(),
        value=msg.value(),
        headers=[
            ("source_topic", msg.topic().encode()),
            ("source_partition", str(msg.partition()).encode()),
            ("source_offset", str(msg.offset()).encode()),
            ("consumer_group", group_id.encode()),
            ("error", error[:1000].encode()),
        ],
        on_delivery=on_delivery,
    )
    remaining = producer.flush(10)
    if remaining or delivery_errors:
        raise RuntimeError(f"DLQ write not confirmed: {delivery_errors or 'flush timed out'}")


def process_with_retries(handle_message, record, max_attempts: int, backoff_seconds: float, logger):
    """Calls handle_message(record), retrying with linear backoff. Returns None on success,
    or the last error message once every attempt has failed."""
    for attempt in range(1, max_attempts + 1):
        try:
            handle_message(record)
            return None
        except Exception as exc:
            logger.exception(
                "attempt %d/%d failed for message id=%s", attempt, max_attempts, record.get("id")
            )
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                time.sleep(backoff_seconds * attempt)
    return last_error


def run_consumer_loop(group_id: str, handle_message):
    """Subscribes to the shared text-stream topic and calls handle_message(dict) for each record.

    Delivery is at-least-once: an offset is committed only after the record was handled or,
    when it keeps failing, parked on the dead-letter topic. Handlers must therefore be
    idempotent (the services upsert by message_id).
    """
    logger = logging.getLogger(group_id)
    consumer = make_consumer(group_id)
    dlq_producer = make_dlq_producer()
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
                error = None
            except json.JSONDecodeError as exc:
                logger.exception("failed to decode message at offset %d", msg.offset())
                record, error = None, f"JSONDecodeError: {exc}"

            if record is not None:
                error = process_with_retries(
                    handle_message, record, config.MAX_ATTEMPTS, config.RETRY_BACKOFF_SECONDS, logger
                )

            if error is not None:
                # If this raises, the offset stays uncommitted and the process exits, so the
                # message is redelivered on restart instead of being lost.
                send_to_dlq(dlq_producer, msg, group_id, error)
                logger.error("sent offset %d to %s: %s", msg.offset(), config.DLQ_TOPIC_NAME, error)

            consumer.commit(message=msg, asynchronous=False)
    except KeyboardInterrupt:
        logger.info("shutting down")
    finally:
        consumer.close()
        dlq_producer.flush(5)
