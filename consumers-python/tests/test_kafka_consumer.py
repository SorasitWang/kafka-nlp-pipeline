"""Delivery-semantics tests for run_consumer_loop, using in-memory fakes instead of a broker."""

import json

import pytest

from common import config, kafka_consumer


class FakeMessage:
    def __init__(self, value, offset):
        self._value = value
        self._offset = offset

    def value(self):
        return self._value

    def key(self):
        return b"key"

    def topic(self):
        return config.TOPIC_NAME

    def partition(self):
        return 0

    def offset(self):
        return self._offset

    def error(self):
        return None


class FakeConsumer:
    """Returns the queued messages, then raises KeyboardInterrupt to end the loop."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.committed = []
        self.closed = False

    def subscribe(self, topics):
        pass

    def poll(self, timeout):
        if not self.messages:
            raise KeyboardInterrupt
        return self.messages.pop(0)

    def commit(self, message, asynchronous):
        self.committed.append(message.offset())

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self, fail=False):
        self.fail = fail
        self.produced = []

    def produce(self, topic, key, value, headers, on_delivery):
        self.produced.append({"topic": topic, "value": value, "headers": dict(headers)})
        on_delivery(RuntimeError("broker down") if self.fail else None, None)

    def flush(self, timeout):
        return 0


def record(message_id):
    return json.dumps({"id": message_id, "text": "hello"}).encode()


@pytest.fixture
def run(monkeypatch):
    monkeypatch.setattr(kafka_consumer.time, "sleep", lambda _s: None)

    def _run(messages, handler, producer=None):
        consumer = FakeConsumer(messages)
        producer = producer or FakeProducer()
        monkeypatch.setattr(kafka_consumer, "make_consumer", lambda _g: consumer)
        monkeypatch.setattr(kafka_consumer, "make_dlq_producer", lambda: producer)
        kafka_consumer.run_consumer_loop("test-group", handler)
        return consumer, producer

    return _run


def test_commits_each_offset_after_handling(run):
    handled = []
    consumer, producer = run([FakeMessage(record("a"), 0), FakeMessage(record("b"), 1)], handled.append)

    assert [r["id"] for r in handled] == ["a", "b"]
    assert consumer.committed == [0, 1]
    assert producer.produced == []
    assert consumer.closed


def test_transient_failure_is_retried_then_committed(run):
    calls = []

    def flaky(rec):
        calls.append(rec["id"])
        if len(calls) < 2:
            raise ConnectionError("db blip")

    consumer, producer = run([FakeMessage(record("a"), 7)], flaky)

    assert calls == ["a", "a"]
    assert consumer.committed == [7]
    assert producer.produced == []


def test_persistent_failure_goes_to_dlq_and_loop_continues(run):
    def handler(rec):
        if rec["id"] == "bad":
            raise ValueError("model exploded")

    consumer, producer = run([FakeMessage(record("bad"), 0), FakeMessage(record("ok"), 1)], handler)

    assert len(producer.produced) == 1
    dead = producer.produced[0]
    assert dead["topic"] == config.DLQ_TOPIC_NAME
    assert dead["headers"]["source_offset"] == b"0"
    assert dead["headers"]["consumer_group"] == b"test-group"
    assert b"model exploded" in dead["headers"]["error"]
    assert consumer.committed == [0, 1]


def test_undecodable_message_goes_to_dlq_without_calling_handler(run):
    handled = []
    consumer, producer = run([FakeMessage(b"not json", 3)], handled.append)

    assert handled == []
    assert b"JSONDecodeError" in producer.produced[0]["headers"]["error"]
    assert consumer.committed == [3]


def test_offset_not_committed_when_dlq_write_fails(run):
    def always_fails(_rec):
        raise ValueError("boom")

    with pytest.raises(RuntimeError, match="DLQ write not confirmed"):
        run([FakeMessage(record("a"), 5)], always_fails, producer=FakeProducer(fail=True))
