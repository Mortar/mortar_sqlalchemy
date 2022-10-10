from os import environ

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base


@pytest.fixture(scope='session')
def db():
    engine = create_engine(environ['DB_URL'], future=True)
    conn = engine.connect()
    transaction = conn.begin()
    try:
        yield conn
    finally:
        transaction.rollback()


@pytest.fixture
def base():
    return declarative_base()
