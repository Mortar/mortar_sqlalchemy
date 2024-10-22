from typing import Iterable

import pytest
from sqlalchemy import text
from sqlalchemy.future import Connection
from sqlalchemy.orm import declarative_base
from testservices.collection import Collection
from testservices.provider import Provider
from testservices.services.databases import PostgresContainer, Database, DatabaseFromEnvironment

from mortar_sqlalchemy.testing import connection_in_transaction

collection = Collection(PostgresContainer())

database_provider = Provider[Database](
    DatabaseFromEnvironment(timeout=300),
    PostgresContainer(),
)


@pytest.fixture(scope='session')
def database() -> Iterable[Database]:
    with database_provider as database_:
        yield database_


@pytest.fixture()
def connection(database: Database) -> Iterable[Connection]:
    # Use psycopg3:
    database.driver = 'psycopg'
    with connection_in_transaction(database.url) as conn:
        conn.execute(text('CREATE EXTENSION IF NOT EXISTS btree_gist'))
        yield conn


@pytest.fixture
def base():
    return declarative_base()
