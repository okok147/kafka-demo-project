import psycopg2
from psycopg2.extras import RealDictCursor

from kafka_demo.common.config import settings



def get_conn():
    if settings.postgres_dsn:
        return psycopg2.connect(settings.postgres_dsn)
    return psycopg2.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password,
    )



def fetch_one(query: str, params: tuple = ()):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchone()



def fetch_all(query: str, params: tuple = ()):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query, params)
            return cur.fetchall()
