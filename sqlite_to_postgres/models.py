import uuid
from dataclasses import dataclass
from datetime import datetime


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
