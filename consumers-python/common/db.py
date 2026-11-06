import logging
from contextlib import contextmanager

import psycopg2

from . import config

logger = logging.getLogger(__name__)

_conn = None


def get_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def _ensure_connection():
    """Returns the shared connection, reconnecting if it is missing or broken."""
    global _conn
    if _conn is None or _conn.closed:
        if _conn is not None:
            logger.warning("database connection lost, reconnecting")
        _conn = get_connection()
    return _conn


@contextmanager
def transaction():
    """Yields a cursor inside one transaction: commits on success, rolls back on any error.

    Without the rollback, a single failed statement would leave psycopg2's connection in an
    aborted-transaction state and every later message would fail too. A connection that broke
    (e.g. Postgres restarted) is detected via `conn.closed` and replaced on the next call.
    """
    conn = _ensure_connection()
    try:
        with conn.cursor() as cur:
            yield cur
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except psycopg2.Error:
            # The connection itself is unusable; _ensure_connection() replaces it next time.
            pass
        raise


def upsert_message(cur, message_id: str, text: str, source_dataset: str, produced_at: str):
    """Inserts the source message if it isn't stored yet. Runs on the caller's transaction."""
    cur.execute(
        """
        INSERT INTO messages (id, text, source_dataset, produced_at)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        """,
        (message_id, text, source_dataset, produced_at),
    )
