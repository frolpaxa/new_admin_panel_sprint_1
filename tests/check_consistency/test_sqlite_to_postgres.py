import os
import sqlite3
import sys
import unittest
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extras import DictCursor

ROOT_DIR = os.path.join(os.path.dirname(__file__), "..", "..")

sys.path.append(ROOT_DIR)

print(ROOT_DIR)
from sqlite_to_postgres.load_data import (
    Genre,
    GenreFilmWork,
    Movie,
    Person,
    PersonFilmWork,
)


@dataclass
class TimeStampedMixin:
    created_at: datetime
    updated_at: datetime

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = datetime.strftime(self.created_at, "%Y-%m-%d %H:%M:%S")
        else:
            self.created_at = self.created_at[:19]

        if isinstance(self.updated_at, datetime):
            self.updated_at = datetime.strftime(self.updated_at, "%Y-%m-%d %H:%M:%S")
        else:
            self.updated_at = self.updated_at[:19]


@dataclass
class CreatedAt:
    created_at: datetime

    def __post_init__(self):
        if isinstance(self.created_at, datetime):
            self.created_at = datetime.strftime(self.created_at, "%Y-%m-%d %H:%M:%S")
        else:
            self.created_at = self.created_at[:19]


@dataclass
class TestGenre(TimeStampedMixin, Genre):
    ...


@dataclass
class TestMovie(TimeStampedMixin, Movie):
    ...


@dataclass
class TestPerson(TimeStampedMixin, Person):
    ...


@dataclass
class TestGenreFilmWork(CreatedAt, GenreFilmWork):
    ...


@dataclass
class TestPersonFilmWork(CreatedAt, PersonFilmWork):
    ...


PG_DSN = {
    "dbname": "movies_database",
    "user": "app",
    "password": "123qwe",
    "host": "127.0.0.1",
    "port": 5432,
}

SQLITE_DB_PATH = os.path.abspath(
    os.path.join(ROOT_DIR, "sqlite_to_postgres", "db.sqlite")
)


class TestMirgation(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, psycopg2.connect(
            **PG_DSN
        ) as pg_conn:
            self.sqlite_conn = sqlite_conn
            self.sqlite_conn.row_factory = sqlite3.Row
            self.pg_conn = pg_conn

    def get_data(self, table):
        with self.pg_conn.cursor(
            cursor_factory=psycopg2.extras.DictCursor
        ) as pg_cur, closing(self.sqlite_conn.cursor()) as sqlite_cur:
            sqlite_data = sqlite_cur.execute(
                f"SELECT * FROM {table} ORDER BY ID"
            ).fetchall()
            pg_cur.execute(f"SELECT * FROM CONTENT.{table} ORDER BY ID")
            pg_data = pg_cur.fetchall()
            return sqlite_data, pg_data

    def test_genre(self):
        sqlite_data, pg_data = self.get_data("GENRE")

        assert len(sqlite_data) == len(pg_data)

        for k, v in enumerate(sqlite_data):
            assert TestGenre(**v) == TestGenre(**pg_data[k])

    def test_person(self):
        sqlite_data, pg_data = self.get_data("PERSON")

        assert len(sqlite_data) == len(pg_data)

        for k, v in enumerate(sqlite_data):
            assert TestPerson(**v) == TestPerson(**pg_data[k])

    def test_film_work(self):
        sqlite_data, pg_data = self.get_data("FILM_WORK")

        assert len(sqlite_data) == len(pg_data)

        for k, v in enumerate(sqlite_data):
            assert TestMovie(**v) == TestMovie(**pg_data[k])

    def test_person_film_work(self):
        sqlite_data, pg_data = self.get_data("PERSON_FILM_WORK")

        assert len(sqlite_data) == len(pg_data)

        for k, v in enumerate(sqlite_data):
            assert TestPersonFilmWork(**v) == TestPersonFilmWork(**pg_data[k])

    def test_genre_film_work(self):
        sqlite_data, pg_data = self.get_data("GENRE_FILM_WORK")

        assert len(sqlite_data) == len(pg_data)

        for k, v in enumerate(sqlite_data):
            assert TestGenreFilmWork(**v) == TestGenreFilmWork(**pg_data[k])


if __name__ == "__main__":
    unittest.main()
