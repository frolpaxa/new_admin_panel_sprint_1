import os
import sqlite3
import sys

import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import connection as _connection

sys.path.append("..")

from sqlite_to_postgres.loaders import (
    PostgresSaver,
    SQLiteExtractor,
    pg_context,
    sqlite_context,
)
from sqlite_to_postgres.models import (
    Genre,
    GenreFilmWork,
    Movie,
    Person,
    PersonFilmWork,
)

load_dotenv()

PG_DSN = {
    "dbname": os.environ.get("DB_NAME"),
    "user": os.environ.get("DB_USER"),
    "password": os.environ.get("DB_PASSWORD"),
    "host": "127.0.0.1",
    "port": 5432,
}

SQLITE_DB_PATH = "db.sqlite"


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
        limit, offset = 100, 0

        while batch_data := sqlite_extractor.extract_data(table, limit, offset):
            data = [mapper(**x) for x in batch_data]
            postgres_saver.save_data(data, table)
            offset = limit
            limit *= 2


if __name__ == "__main__":
    with sqlite_context(SQLITE_DB_PATH) as sqlite_conn, pg_context(PG_DSN) as pg_conn:
        try:
            load_from_sqlite(sqlite_conn, pg_conn)
        except (sqlite3.DatabaseError, psycopg2.DatabaseError) as ex:
            sys.exit(f"Database error: {ex}")
