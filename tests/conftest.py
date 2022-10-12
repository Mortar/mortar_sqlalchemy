import pytest
from sqlalchemy.orm import declarative_base

from mortar_mixins.testing import connection_in_transaction


@pytest.fixture(scope='session')
def db():
    with connection_in_transaction() as conn:
        yield conn


@pytest.fixture
def base():
    return declarative_base()
