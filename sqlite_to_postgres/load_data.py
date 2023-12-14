import sqlite3
import uuid
from dataclasses import dataclass, fields, astuple
import sys
from contextlib import closing
from datetime import datetime

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

PG_DSN = {
    "dbname": "movies_database",
    "user": "app",
    "password": "123qwe",
    "host": "127.0.0.1",
    "port": 5432,
}

SQLITE_DB_PATH = "db.sqlite"


@dataclass
class Movie:
    id: uuid.UUID
    title: str
    description: str
    creation_date: datetime
    rating: float
    type: str
    created_at: datetime
    updated_at: datetime
    file_path: str


@dataclass
class Genre:
    id: uuid.UUID
    name: str
    description: str
    created_at: datetime
    updated_at: datetime


@dataclass
class Person:
    id: uuid.UUID
    full_name: str
    created_at: datetime
    updated_at: datetime


@dataclass
class GenreFilmWork:
    id: uuid.UUID
    film_work_id: str
    genre_id: str
    created_at: datetime


@dataclass
class PersonFilmWork:
    id: uuid.UUID
    film_work_id: str
    person_id: str
    role: str
    created_at: datetime


def get_slice(slice, data):
    for i in range(0, len(data), slice):
        yield data[i : i + slice]


class PostgresSaver:
    def __init__(self, pg_conn: _connection) -> None:
        self.pg_conn = pg_conn

    def save_data(self, data, table):
        # data = list(get_slice(50, data))

        with self.pg_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            column_names = [field.name for field in fields(data[0])]
            column_names_str = ",".join(column_names)
            col_count = ", ".join(["%s"] * len(column_names))
            bind_values = ",".join(
                cur.mogrify(f"({col_count})", astuple(x)).decode("utf-8") for x in data
            )

            query = (
                f"INSERT INTO content.{table} ({column_names_str}) VALUES {bind_values} "
                f" ON CONFLICT (id) DO NOTHING"
            )
            cur.execute(query)


class SQLiteExtractor:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        self.connection.row_factory = sqlite3.Row

    def extract_data(self, table):
        with closing(self.connection.cursor()) as cur:
            cur.execute(f"SELECT * FROM {table};")
            return [dict(x) for x in cur.fetchall()]


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""

    tables = {
        "film_work": Movie,
        "genre": Genre,
        "person": Person,
        "genre_film_work": GenreFilmWork,
        "person_film_work": PersonFilmWork,
    }

    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    for table, mapper in tables.items():
        data = [mapper(**x) for x in sqlite_extractor.extract_data(table)]
        postgres_saver.save_data(data, table)


if __name__ == "__main__":
    try:
        with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, psycopg2.connect(
            **PG_DSN, cursor_factory=DictCursor
        ) as pg_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
    except (sqlite3.DatabaseError, psycopg2.DatabaseError) as ex:
        sys.exit(f"Database error: {ex}")
