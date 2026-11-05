import os

from dotenv import load_dotenv

load_dotenv()


def _get(key: str, default: str) -> str:
    val = os.environ.get(key)
    return val if val else default


KAFKA_BROKERS = _get("KAFKA_BROKERS", "localhost:9092")
TOPIC_NAME = _get("TOPIC_NAME", "text-stream")

POSTGRES_HOST = _get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = _get("POSTGRES_PORT", "5432")
POSTGRES_DB = _get("POSTGRES_DB", "textanalysis")
POSTGRES_USER = _get("POSTGRES_USER", "textanalysis")
POSTGRES_PASSWORD = _get("POSTGRES_PASSWORD", "textanalysis")
