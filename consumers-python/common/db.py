import psycopg2

from . import config


def get_connection():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def upsert_message(conn, message_id: str, text: str, source_dataset: str, produced_at: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO messages (id, text, source_dataset, produced_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
            """,
            (message_id, text, source_dataset, produced_at),
        )
    conn.commit()
