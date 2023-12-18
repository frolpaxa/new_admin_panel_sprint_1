import sqlite3
from contextlib import closing, contextmanager
from dataclasses import astuple, fields

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

SLICE_LENGTH = 50


@contextmanager
def sqlite_context(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    except Exception as ex:
        conn.rollback()
        raise ex
    finally:
        conn.close()


@contextmanager
def pg_context(dsn: dict):
    conn = psycopg2.connect(**dsn, cursor_factory=DictCursor)
    try:
        yield conn
    except Exception as ex:
        conn.rollback()
        raise ex
    finally:
        conn.close()


def get_slice(slice, data):
    for i in range(0, len(data), slice):
        yield data[i : i + slice]


class PostgresSaver:
    def __init__(self, pg_conn: _connection) -> None:
        self.pg_conn = pg_conn

    def save_data(self, raw_data, table):
        data_sets = list(get_slice(SLICE_LENGTH, raw_data))

        with self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            for data in data_sets:
                column_names = [field.name for field in fields(data[0])]
                column_names_str = ",".join(column_names)

                col_count = ", ".join(["%s"] * len(column_names))

                query = (
                    f"INSERT INTO content.{table} ({column_names_str}) VALUES ({col_count}) "
                    f" ON CONFLICT (id) DO NOTHING"
                )

                psycopg2.extras.execute_batch(cur, query, [astuple(x) for x in data])

            self.pg_conn.commit()


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def extract_data(self, table, limit, offset):
        with closing(self.connection.cursor()) as cur:
            cur.execute(f"SELECT * FROM {table} LIMIT {limit} OFFSET {offset};")
            return [dict(x) for x in cur.fetchall()]
