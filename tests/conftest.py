import pytest
from sqlalchemy import text
from sqlalchemy.orm import declarative_base
from testservices.provider import Provider
from testservices.services.databases import PostgresContainer, Database, DatabaseFromEnvironment

from mortar_sqlalchemy.testing import connection_in_transaction


@pytest.fixture(scope='session')
def db():
    provider = Provider(
        Database,
        DatabaseFromEnvironment(timeout=300),
        PostgresContainer(),
    )
    with provider as database:
        with connection_in_transaction(database.url) as conn:
            conn.execute(text('CREATE EXTENSION IF NOT EXISTS btree_gist'))
            yield conn


@pytest.fixture
def base():
    return declarative_base()
