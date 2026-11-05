import os

from dotenv import load_dotenv

load_dotenv()


def _get(key: str, default: str) -> str:
    val = os.environ.get(key)
    return val if val else default


def _require(key: str) -> str:
    val = os.environ.get(key)
    if not val:
        raise RuntimeError(f"{key} is not set; copy .env.example to .env")
    return val


KAFKA_BROKERS = _get("KAFKA_BROKERS", "localhost:9092")
TOPIC_NAME = _get("TOPIC_NAME", "text-stream")

POSTGRES_HOST = _get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = _get("POSTGRES_PORT", "5432")
POSTGRES_DB = _require("POSTGRES_DB")
POSTGRES_USER = _require("POSTGRES_USER")
POSTGRES_PASSWORD = _require("POSTGRES_PASSWORD")
