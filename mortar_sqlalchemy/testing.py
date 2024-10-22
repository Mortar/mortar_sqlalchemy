from contextlib import contextmanager
from os import environ
from typing import Iterable

from sqlalchemy import MetaData, create_engine
from sqlalchemy.future import Connection
from sqlalchemy.orm import Session


def drop_tables(conn):
    # https://github.com/sqlalchemy/sqlalchemy/wiki/DropEverything
    metadata = MetaData()
    metadata.reflect(conn)
    metadata.drop_all(conn)


@contextmanager
def connection_in_transaction(url: str = None) -> Iterable[Connection]:
    engine = create_engine(url or environ['DB_URL'], future=True)
    connection = engine.connect()
    transaction = connection.begin()
    try:
        yield connection
    finally:
        transaction.rollback()


@contextmanager
def create_tables_and_session(connection, base):
    base.metadata.create_all(bind=connection, checkfirst=False)
    # https://docs.sqlalchemy.org/en/20/orm/session_transaction.html#joining-a-session-into-an-external-transaction-such-as-for-test-suites
    yield Session(connection, join_transaction_mode="create_savepoint")
