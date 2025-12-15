from contextlib import contextmanager
import psycopg
from psycopg.rows import dict_row

from .config import get_database_url

@contextmanager
def get_conn():
    dsn = get_database_url()
    conn = psycopg.connect(dsn, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()
